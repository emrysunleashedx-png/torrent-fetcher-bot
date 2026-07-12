"""
torrent_engine.py
---------------------------------------------------------------------------
Thin wrapper around libtorrent for the Torrent Fetcher bot. Kept as its
own isolated module (rather than inlined into the bot's message handlers)
specifically so that if something misbehaves, the libtorrent-specific
surface area is easy to find and reason about on its own.

IMPORTANT CAVEAT: this is the least battle-tested part of this whole
project. libtorrent's Python bindings only gained properly distributed
manylinux/macOS wheels on PyPI as of the 2.0.13 release (mid-2026) -- this
code is written against that documented API, but has not been run
end-to-end in the actual deployment environment. Watch the bot's logs
closely on first real use, and expect this module specifically to need
iteration based on what actually happens on Render.

libtorrent's API is synchronous/blocking (it's a C++ library with Python
bindings, not asyncio-native) -- every function here is written as a
plain blocking function and MUST be called via asyncio.to_thread() from
the bot's async handlers, never awaited directly.
"""

import os
import time
import logging

import libtorrent as lt

logger = logging.getLogger("torrent_engine")

# How long to wait for the initial metadata (file list, torrent name) to
# arrive after adding a magnet link, before giving up. Magnets with no
# metadata after this long almost always mean "no seeders" or "dead link".
METADATA_TIMEOUT_SECONDS = 60

# How often to check/report download progress.
PROGRESS_POLL_INTERVAL_SECONDS = 3

# Hard ceiling on total download time, regardless of progress, so a
# slow/stalled torrent can't tie up the bot (and Render's memory/disk)
# indefinitely. Configurable via env var since "reasonable" depends a lot
# on your actual torrent sizes and connection speed.
DOWNLOAD_TIMEOUT_SECONDS = int(os.getenv("TORRENT_DOWNLOAD_TIMEOUT_SECONDS", str(60 * 60)))  # 1 hour default


class TorrentSession:
    """Wraps a single libtorrent session + handle for one magnet download.
    One instance per active download -- not reused across downloads, so
    there's no shared mutable state between concurrent user requests
    (though in practice this bot processes one download at a time, per
    the "safer, slower" design decision made earlier).
    """

    def __init__(self, save_path: str):
        self.save_path = save_path
        self.session = None
        self.handle = None

    def start(self, magnet_uri: str):
        """Blocking. Creates the session and adds the magnet. Does NOT
        wait for metadata -- call wait_for_metadata() separately so the
        caller can report progress/timeouts distinctly for each phase.
        """
        os.makedirs(self.save_path, exist_ok=True)

        settings = {
            "listen_interfaces": "0.0.0.0:6881",
            "user_agent": "NovaFlixTorrentFetcher/1.0",
        }
        self.session = lt.session(settings)

        # DHT + well-known bootstrap routers -- needed since magnet links
        # often have few/no trackers listed and rely on DHT for peer
        # discovery.
        try:
            self.session.add_dht_router("router.utorrent.com", 6881)
            self.session.add_dht_router("router.bittorrent.com", 6881)
            self.session.add_dht_router("dht.transmissionbt.com", 6881)
            self.session.start_dht()
        except Exception as e:
            # DHT setup failing isn't fatal -- trackers alone may still
            # work -- but worth logging since it explains a slow/failed
            # download later.
            logger.warning("DHT setup failed (continuing without it): %s", e)

        params = {
            "save_path": self.save_path,
            "storage_mode": lt.storage_mode_t(2),  # sparse allocation
        }
        self.handle = lt.add_magnet_uri(self.session, magnet_uri, params)
        logger.info("Torrent added, waiting for metadata (save_path=%s)", self.save_path)

    def wait_for_metadata(self, timeout_seconds: int = METADATA_TIMEOUT_SECONDS) -> bool:
        """Blocking. Polls until the torrent's metadata (file list, name)
        is available, or the timeout elapses. Returns True/False.
        """
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            if self.handle.has_metadata():
                return True
            time.sleep(1)
        return False

    def get_files(self) -> list:
        """Returns [{"index": int, "path": str, "size": int}, ...] for
        every file in the torrent. Only valid after wait_for_metadata()
        returns True.
        """
        info = self.handle.get_torrent_info()
        files = info.files()
        result = []
        for i in range(files.num_files()):
            result.append({
                "index": i,
                "path": files.file_path(i),
                "size": files.file_size(i),
            })
        return result

    def select_file(self, file_index: int, total_files: int):
        """Deprioritize every file except the chosen one, so only that
        file actually gets downloaded -- important for multi-file
        torrents (season packs, torrents bundled with extras/samples/nfo
        files we don't want) to avoid wasting bandwidth/disk on files the
        admin didn't pick.
        """
        priorities = [0] * total_files
        priorities[file_index] = 4  # normal priority (0 = skip, 1-7 = priority levels)
        self.handle.prioritize_files(priorities)
        self.handle.set_sequential_download(True)

    def get_progress(self) -> dict:
        """Blocking, cheap. Returns current status as a plain dict rather
        than libtorrent's own status object, so callers don't need to
        import/understand libtorrent types themselves.
        """
        status = self.handle.status()
        return {
            "progress": status.progress,  # 0.0 - 1.0
            "download_rate_bps": status.download_rate,
            "num_peers": status.num_peers,
            "state": str(status.state),
            "total_wanted": status.total_wanted,
            "total_wanted_done": status.total_wanted_done,
        }

    def is_download_complete(self) -> bool:
        status = self.handle.status()
        # total_wanted_done reaching total_wanted accounts for
        # single-file-selected downloads correctly (progress alone can
        # read confusingly for partial/prioritized multi-file torrents).
        return status.total_wanted > 0 and status.total_wanted_done >= status.total_wanted

    def get_downloaded_file_path(self, relative_path: str) -> str:
        return os.path.join(self.save_path, relative_path)

    def shutdown(self):
        """Blocking. Removes the torrent from the session to free up
        resources. Does NOT delete downloaded files -- caller is
        responsible for cleanup after the upload step reads them.
        """
        try:
            if self.session and self.handle:
                self.session.remove_torrent(self.handle)
        except Exception as e:
            logger.warning("Error during torrent session shutdown (non-fatal): %s", e)


def validate_magnet_uri(text: str) -> bool:
    """Basic sanity check before handing something to libtorrent -- catches
    obviously-not-a-magnet-link input early with a clear error, rather
    than letting libtorrent fail deeper in the process with a less
    helpful message.
    """
    return text.strip().lower().startswith("magnet:?")

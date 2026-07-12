import os
import time
import shutil
import asyncio
import logging

import httpx
from pyrogram import Client, filters
from pyrogram.types import Message

import torrent_engine
import doodstream_upload

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("torrent_fetcher_bot")

API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "")
BOT_TOKEN = os.getenv("TORRENT_BOT_TOKEN", "")

# The shared group this bot hands finished uploads off to, and the fixed
# marker line NovaFlix's bot looks for to recognize a trusted handoff.
PIPELINE_GROUP_ID = int(os.getenv("PIPELINE_GROUP_ID", "-1004319667086"))
DOODSTREAM_LINK_MARKER = "DOODSTREAM_LINK"

DOWNLOAD_DIR = os.getenv("TORRENT_DOWNLOAD_DIR", "/tmp/torrent_downloads")

app = Client("torrent_fetcher_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# One active session per chat -- deliberately single-download-at-a-time
# per the earlier "safer, slower" decision, to avoid stacking multiple
# large in-progress downloads in memory/disk simultaneously.
ACTIVE_SESSIONS = {}


@app.on_message(filters.private & filters.command("start"))
async def handle_start(client: Client, message: Message):
    await message.reply_text(
        "📡 **NovaFlix Torrent Fetcher**\n\n"
        "Send me a magnet link and I'll download it, upload the result to "
        "Doodstream, and hand the finished link off automatically.\n\n"
        "If the torrent has multiple files, I'll ask you which one to grab."
    )


@app.on_message(filters.private & filters.text & ~filters.command(["start", "pickfile", "cancel"]))
async def handle_magnet(client: Client, message: Message):
    chat_id = message.chat.id
    text = (message.text or "").strip()

    if chat_id in ACTIVE_SESSIONS:
        await message.reply_text(
            "A download is already in progress in this chat. `/cancel` it first "
            "if you want to start a different one."
        )
        return

    if not torrent_engine.validate_magnet_uri(text):
        await message.reply_text(
            "That doesn't look like a magnet link (should start with `magnet:?`). "
            "Send `/start` for instructions."
        )
        return

    status_msg = await message.reply_text("🔗 Magnet link received. Connecting to peers...")

    save_path = os.path.join(DOWNLOAD_DIR, str(chat_id), str(int(time.time())))
    session = torrent_engine.TorrentSession(save_path)

    try:
        await asyncio.to_thread(session.start, text)
    except Exception as e:
        logger.exception("Failed to start torrent session")
        await status_msg.edit_text(f"❌ Couldn't start this torrent: {type(e).__name__}: {e}")
        _cleanup_session(session)
        return

    await status_msg.edit_text("⏳ Fetching torrent metadata (file list)... this can take up to a minute.")

    try:
        got_metadata = await asyncio.to_thread(session.wait_for_metadata)
    except Exception as e:
        logger.exception("Error while waiting for metadata")
        await status_msg.edit_text(f"❌ Error fetching metadata: {type(e).__name__}: {e}")
        _cleanup_session(session)
        return

    if not got_metadata:
        await status_msg.edit_text(
            "❌ Couldn't get torrent metadata within the timeout. This usually means "
            "the magnet link has no active seeders, or is invalid. Try a different link."
        )
        _cleanup_session(session)
        return

    try:
        files = await asyncio.to_thread(session.get_files)
    except Exception as e:
        logger.exception("Failed to read file list")
        await status_msg.edit_text(f"❌ Couldn't read the file list: {type(e).__name__}: {e}")
        _cleanup_session(session)
        return

    if not files:
        await status_msg.edit_text("❌ This torrent has no files. Nothing to download.")
        _cleanup_session(session)
        return

    ACTIVE_SESSIONS[chat_id] = {"session": session, "files": files, "status_msg": status_msg}

    if len(files) == 1:
        await status_msg.edit_text(f"📄 Single file: `{files[0]['path']}`. Starting download...")
        await _start_download(client, message.chat.id, file_index=0)
        return

    lines = ["📄 **Multiple files found** — reply `/pickfile <number>`:", ""]
    for f in files:
        size_mb = f["size"] / (1024 ** 2)
        lines.append(f"{f['index'] + 1}. `{f['path']}` ({size_mb:.1f} MB)")
    await status_msg.edit_text("\n".join(lines))


@app.on_message(filters.private & filters.command("pickfile"))
async def handle_pickfile(client: Client, message: Message):
    chat_id = message.chat.id
    entry = ACTIVE_SESSIONS.get(chat_id)
    if not entry:
        await message.reply_text("No active torrent waiting for a file pick.")
        return

    raw = message.text.split(None, 1)
    arg = raw[1].strip() if len(raw) > 1 else ""
    if not arg.isdigit():
        await message.reply_text("Usage: `/pickfile <number>`")
        return

    index = int(arg) - 1
    if index < 0 or index >= len(entry["files"]):
        await message.reply_text(f"Pick a number between 1 and {len(entry['files'])}.")
        return

    await message.reply_text(f"Starting download of file {index + 1}...")
    await _start_download(client, chat_id, file_index=index)


@app.on_message(filters.private & filters.command("cancel"))
async def handle_cancel(client: Client, message: Message):
    chat_id = message.chat.id
    entry = ACTIVE_SESSIONS.pop(chat_id, None)
    if not entry:
        await message.reply_text("Nothing active to cancel.")
        return
    _cleanup_session(entry["session"])
    await message.reply_text("🛑 Cancelled and cleaned up.")


async def _start_download(client: Client, chat_id: int, file_index: int):
    entry = ACTIVE_SESSIONS[chat_id]
    session = entry["session"]
    files = entry["files"]
    status_msg = entry["status_msg"]
    chosen_file = files[file_index]

    try:
        await asyncio.to_thread(session.select_file, file_index, len(files))
    except Exception as e:
        logger.exception("Failed to select file for download")
        await status_msg.edit_text(f"❌ Couldn't select that file: {type(e).__name__}: {e}")
        ACTIVE_SESSIONS.pop(chat_id, None)
        _cleanup_session(session)
        return

    deadline = time.time() + torrent_engine.DOWNLOAD_TIMEOUT_SECONDS
    last_edit_time = 0

    while True:
        try:
            progress = await asyncio.to_thread(session.get_progress)
            complete = await asyncio.to_thread(session.is_download_complete)
        except Exception as e:
            logger.exception("Error polling torrent progress")
            await status_msg.edit_text(f"❌ Error during download: {type(e).__name__}: {e}")
            ACTIVE_SESSIONS.pop(chat_id, None)
            _cleanup_session(session)
            return

        if complete:
            break

        if time.time() > deadline:
            await status_msg.edit_text(
                f"❌ Download timed out after {torrent_engine.DOWNLOAD_TIMEOUT_SECONDS // 60} minutes "
                f"({progress['progress'] * 100:.1f}% done, {progress['num_peers']} peers). Giving up."
            )
            ACTIVE_SESSIONS.pop(chat_id, None)
            _cleanup_session(session)
            return

        # Only actually edit the Telegram message every few seconds --
        # editing on every poll would hit Telegram's rate limits fast.
        now = time.time()
        if now - last_edit_time >= torrent_engine.PROGRESS_POLL_INTERVAL_SECONDS:
            speed_kbps = progress["download_rate_bps"] / 1024
            try:
                await status_msg.edit_text(
                    f"⬇️ Downloading `{chosen_file['path']}`\n"
                    f"{progress['progress'] * 100:.1f}% — {speed_kbps:.0f} KB/s — "
                    f"{progress['num_peers']} peer(s) — {progress['state']}"
                )
            except Exception:
                pass  # message-not-modified errors etc -- not worth crashing over
            last_edit_time = now

        await asyncio.sleep(1)

    await status_msg.edit_text("✅ Download complete. Uploading to Doodstream...")

    file_path = session.get_downloaded_file_path(chosen_file["path"])
    if not os.path.exists(file_path):
        await status_msg.edit_text(
            f"❌ Download reported complete but the file isn't on disk at the expected "
            f"path ({file_path}). This may be a libtorrent path-handling issue -- check logs."
        )
        ACTIVE_SESSIONS.pop(chat_id, None)
        _cleanup_session(session)
        return

    try:
        async with httpx.AsyncClient(follow_redirects=True) as http_client:
            dood_url = await doodstream_upload.upload_to_hoster(http_client, "Doodstream", file_path)
    except Exception as e:
        logger.exception("Unexpected error during Doodstream upload")
        await status_msg.edit_text(f"❌ Upload failed unexpectedly: {type(e).__name__}: {e}")
        ACTIVE_SESSIONS.pop(chat_id, None)
        _cleanup_session(session)
        return

    ACTIVE_SESSIONS.pop(chat_id, None)
    _cleanup_session(session)

    if not dood_url.startswith("http"):
        await status_msg.edit_text(f"❌ Doodstream upload failed: {dood_url}")
        return

    await status_msg.edit_text(f"✅ Uploaded! {dood_url}\n\nSending to the pipeline group...")

    original_name = os.path.basename(chosen_file["path"])
    handoff_text = f"{DOODSTREAM_LINK_MARKER}\n{dood_url}\n{original_name}"

    try:
        await client.send_message(PIPELINE_GROUP_ID, handoff_text)
        await status_msg.edit_text(
            f"✅ **All done!** Uploaded and sent to the pipeline group:\n{dood_url}"
        )
    except Exception as e:
        logger.exception("Failed to post handoff message to pipeline group")
        await status_msg.edit_text(
            f"⚠️ Uploaded successfully ({dood_url}) but couldn't post it to the pipeline "
            f"group automatically ({type(e).__name__}: {e}). You can forward this link to "
            "the NovaFlix bot manually."
        )


def _cleanup_session(session: "torrent_engine.TorrentSession"):
    """Tears down the libtorrent session/handle AND removes any
    downloaded files on disk. Combined into one function (rather than two
    separate calls every caller has to remember) specifically because an
    earlier version of this file had call sites that cleaned up the
    session but forgot the disk directory, which would have slowly
    leaked disk space on repeated failed/cancelled downloads.
    """
    try:
        session.shutdown()
    except Exception as e:
        logger.warning("Session cleanup raised (non-fatal): %s", e)

    try:
        if os.path.exists(session.save_path):
            shutil.rmtree(session.save_path)
    except OSError as e:
        logger.warning("Failed to remove download dir %s: %s", session.save_path, e)


if __name__ == "__main__":
    import http.server
    import socketserver
    import threading

    def run_dummy_server():
        port = int(os.getenv("PORT", "8000"))
        with socketserver.TCPServer(("", port), http.server.SimpleHTTPRequestHandler) as httpd:
            httpd.serve_forever()

    threading.Thread(target=run_dummy_server, daemon=True).start()
    app.run()

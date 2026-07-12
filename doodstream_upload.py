"""
doodstream_upload.py
---------------------------------------------------------------------------
Doodstream upload logic for the Torrent Fetcher bot. This is copied
verbatim from the NovaFlix Media Router bot (main.py there) rather than
imported across repos, since the two bots are deployed as separate
services with separate codebases. If you fix a bug in one, mirror the fix
in the other -- see NovaFlix's main.py for the canonical version and its
test history.
"""

import os
import asyncio
import logging

import httpx

logger = logging.getLogger("doodstream_upload")

HOSTERS = {
    "Doodstream": {
        "api_key": os.getenv("DOODSTREAM_API_KEY", ""),
        "server_url": "https://doodapi.com/api/upload/server",
        "download_url_fmt": "https://dood.to/d/{code}",
    },
}

UPLOAD_TIMEOUT = httpx.Timeout(connect=30.0, read=600.0, write=600.0, pool=30.0)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


def _extract_filecode_from_entry(entry):
    if isinstance(entry, dict):
        return entry.get("filecode") or entry.get("file_code")
    return None


def _extract_filecode(upload_data):
    result = upload_data.get("result")
    if isinstance(result, list) and result:
        result = result[0]
    return _extract_filecode_from_entry(result)


async def upload_to_hoster(client: httpx.AsyncClient, hoster_name: str, file_path: str,
                            max_retries: int = 2) -> str:
    """Generic uploader used for any hoster following the doodapi-style
    upload-server protocol (fetch server -> POST file -> parse filecode).
    Retries transient failures; permanent failures return immediately.
    """
    config = HOSTERS[hoster_name]
    api_key = config["api_key"]

    if not api_key:
        return "Key Missing ⚠️"

    filename = os.path.basename(file_path)
    last_transient_error = None

    for attempt in range(1, max_retries + 2):
        if attempt > 1:
            backoff = 2 ** (attempt - 1)
            logger.info("%s: retrying upload (attempt %d/%d) after %ds backoff",
                        hoster_name, attempt, max_retries + 1, backoff)
            await asyncio.sleep(backoff)

        result = await _attempt_upload(client, hoster_name, config, api_key, filename, file_path)

        if result.startswith("http"):
            return result
        if result.startswith("TRANSIENT:"):
            last_transient_error = result[len("TRANSIENT:"):]
            continue
        return result

    return (f"Error: upload failed after {max_retries + 1} attempts "
            f"(last error: {last_transient_error})")


async def _attempt_upload(client: httpx.AsyncClient, hoster_name: str, config: dict,
                           api_key: str, filename: str, file_path: str) -> str:
    try:
        server_resp = await client.get(
            config["server_url"],
            params={"key": api_key},
            headers=HEADERS,
            timeout=UPLOAD_TIMEOUT,
        )
    except httpx.TimeoutException:
        logger.warning("%s: timed out fetching upload server", hoster_name)
        return "TRANSIENT:request to fetch upload server timed out"
    except httpx.HTTPError as e:
        logger.warning("%s: network error fetching upload server: %s", hoster_name, e)
        return f"TRANSIENT:network issue contacting {hoster_name} ({type(e).__name__})"

    try:
        server_data = server_resp.json()
    except ValueError:
        logger.warning("%s: non-JSON server response (HTTP %s): %r",
                        hoster_name, server_resp.status_code, server_resp.text[:200])
        if server_resp.status_code >= 500:
            return f"TRANSIENT:server error (HTTP {server_resp.status_code})"
        return f"API Error (HTTP {server_resp.status_code})"

    upload_url = server_data.get("result")
    if not upload_url:
        msg = server_data.get("msg", "No Upload URL")
        if isinstance(msg, str) and any(w in msg.lower() for w in ("busy", "try again", "unavailable", "overload")):
            return f"TRANSIENT:server reported: {msg}"
        return f"Server Error: {msg}"

    extra_data = {"api_key": api_key}

    try:
        async def _post():
            with open(file_path, "rb") as f:
                files = {"file": (filename, f, "application/octet-stream")}
                return await client.post(
                    upload_url,
                    files=files,
                    data=extra_data,
                    headers=HEADERS,
                    timeout=UPLOAD_TIMEOUT,
                )

        upload_resp = await _post()
    except httpx.TimeoutException:
        logger.warning("%s: upload timed out for %s", hoster_name, filename)
        return "TRANSIENT:upload timed out"
    except httpx.HTTPError as e:
        logger.warning("%s: network error during upload: %s", hoster_name, e)
        return f"TRANSIENT:network issue during upload ({type(e).__name__})"
    except OSError as e:
        logger.error("%s: could not read local file %s: %s", hoster_name, file_path, e)
        return "Error: could not read downloaded file"

    try:
        upload_data = upload_resp.json()
    except ValueError:
        logger.warning("%s: invalid upload response: %r", hoster_name, upload_resp.text[:200])
        if upload_resp.status_code >= 500:
            return f"TRANSIENT:server error during upload (HTTP {upload_resp.status_code})"
        return f"Response Parse Error: {upload_resp.text[:100]}"

    file_code = _extract_filecode(upload_data)
    if file_code:
        return config["download_url_fmt"].format(code=file_code)

    logger.warning("%s: upload failed, response=%r", hoster_name, upload_data)
    return f"Upload Failed: no filecode in response ({upload_data})"

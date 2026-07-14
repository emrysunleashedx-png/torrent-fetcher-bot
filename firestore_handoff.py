"""
firestore_handoff.py
---------------------------------------------------------------------------
Writes finished Doodstream uploads directly to Firestore for the Torrent
Fetcher bot, instead of posting them as a Telegram message to a shared
group. This replaces the original group-message handoff design, which
turned out to be unreliable in production for reasons never fully
resolved (Telegram/Pyrogram silently failing to dispatch group updates to
Media Router despite every other check -- membership, permissions, peer
resolution, dispatch priority -- checking out fine).

Firestore is used here purely as a message queue between the two bots:
Torrent Fetcher writes a small doc into `pending_torrent_uploads`, and
Media Router polls that collection periodically (see main.py's
poll_pending_torrent_uploads background task) and processes each one
through its normal parse -> dup-check -> confirm flow, exactly as if the
admin had forwarded a file directly.

Uses the same Firebase Admin SDK approach as Media Router's
firestore_publish.py -- see that file's docstring for full setup details.
This module intentionally only implements the narrow slice needed here
(write one doc), not the full publish logic that module has.
"""

import os
import time
import logging

logger = logging.getLogger("firestore_handoff")

PENDING_COLLECTION = "pending_torrent_uploads"

_app = None
_db = None
_firebase_admin = None
_credentials = None
_firestore = None


def _import_firebase_admin():
    global _firebase_admin, _credentials, _firestore
    if _firebase_admin is not None:
        return
    try:
        import firebase_admin as fa
        from firebase_admin import credentials as cr, firestore as fs
    except ImportError as e:
        raise RuntimeError(
            "firebase-admin is not installed. Run: "
            "pip install firebase-admin --break-system-packages"
        ) from e
    _firebase_admin = fa
    _credentials = cr
    _firestore = fs


def init_firebase():
    """Initialize the Firebase Admin app once. Call this at bot startup."""
    global _app, _db
    if _app is not None:
        return _db

    _import_firebase_admin()

    cred_path = os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH", "")
    if not cred_path or not os.path.exists(cred_path):
        raise RuntimeError(
            "FIREBASE_SERVICE_ACCOUNT_PATH is not set or the file doesn't exist. "
            "Download a service account key from Firebase Console -> Project "
            "Settings -> Service Accounts, and point this env var at it. This "
            "must be the SAME Firebase project Media Router uses (emryshub-a1e8d), "
            "since both bots need to see the same pending_torrent_uploads collection."
        )

    cred = _credentials.Certificate(cred_path)
    _app = _firebase_admin.initialize_app(cred)
    _db = _firestore.client()
    logger.info("Firebase Admin initialized (project: %s)", cred.project_id)
    return _db


def write_pending_upload(dood_url: str, original_filename: str, requested_by_chat_id: int) -> str:
    """Write a finished Doodstream upload into the pending queue for
    Media Router to pick up. Returns the new doc's ID.
    """
    db = _db or init_firebase()

    payload = {
        "doodstreamUrl": dood_url,
        "originalFilename": original_filename,
        "requestedByChatId": requested_by_chat_id,
        "createdAt": _firestore.SERVER_TIMESTAMP,
        "processed": False,
    }
    _, doc_ref = db.collection(PENDING_COLLECTION).add(payload)
    logger.info("Wrote pending upload doc %s: url=%s filename=%r",
                doc_ref.id, dood_url, original_filename)
    return doc_ref.id

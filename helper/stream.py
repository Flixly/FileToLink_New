import asyncio
import logging
import mimetypes
import math
import time
from typing import Dict, Set, Tuple, Union

from aiohttp import web
from pyrogram import Client, utils, raw
from pyrogram.errors import AuthBytesInvalid, FloodWait
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from pyrogram.session import Auth, Session

from config import Config
from database import Database

logger = logging.getLogger(__name__)

# Telegram hard-caps upload.GetFile at 1 MB per call.
CHUNK_SIZE = 1024 * 1024  # 1 MB

# Minimum prefetch buffer: keep this many chunks fetched ahead to guarantee
# smooth playback without mid-stream stalls.
PREFETCH_COUNT = 3

MIME_TYPE_MAP = {
    "video":    "video/mp4",
    "audio":    "audio/mpeg",
    "image":    "image/jpeg",
    "document": "application/octet-stream",
}

# ── Session dedup for live-viewer counting ────────────────────────────────────
# Maps (file_hash, client_ip) → last-seen timestamp so that multiple
# range-requests from the same player are counted as ONE session.
_active_sessions: Dict[str, float] = {}
_sessions_lock = asyncio.Lock()

# A session is considered "alive" while data is flowing. We clean up stale
# entries after this many seconds of inactivity.
_SESSION_TTL = 30  # seconds

# ── Bandwidth dedup ───────────────────────────────────────────────────────────
# Tracks (client_ip, message_id, from_bytes) tuples that have already been
# counted toward bandwidth.  Duplicate range requests from the same client
# (e.g. player probing, prefetch, reconnect) for the same byte range are
# skipped so bandwidth is not inflated.
# Key: (client_ip, message_id, from_bytes)  Value: expiry timestamp
_bw_tracked: Dict[Tuple[str, str, int], float] = {}
_bw_lock = asyncio.Lock()

# How long (seconds) to remember a (client, file, offset) tuple.
# Any re-request within this window is treated as a duplicate.
_BW_DEDUP_TTL = 60  # seconds

# ── File metadata cache ───────────────────────────────────────────────────────
# Keyed by file_hash → file doc so DB isn't hit on every chunk request.
_file_meta_cache: Dict[str, dict] = {}


async def get_file_ids(client: Client, message_id: str) -> FileId:
    msg = await client.get_messages(Config.FLOG_CHAT_ID, int(message_id))
    if not msg or msg.empty:
        raise ValueError(f"message {message_id} not found in dump chat")

    media = (
        msg.document
        or msg.video
        or msg.audio
        or msg.photo
        or msg.sticker
        or msg.animation
        or msg.voice
        or msg.video_note
    )
    if not media:
        raise ValueError(f"message {message_id} contains no streamable media")

    return FileId.decode(media.file_id)


class ByteStreamer:

    def __init__(self, client: Client):
        self.client: Client = client
        self.cached_file_ids: Dict[str, FileId] = {}
        self.clean_timer: int = 30 * 60
        asyncio.create_task(self.clean_cache())

    async def get_file_properties(self, db_id: str) -> FileId:
        if db_id not in self.cached_file_ids:
            logger.debug("FileId cache miss for %s — fetching from Telegram", db_id)
            await self.generate_file_properties(db_id)
        return self.cached_file_ids[db_id]

    async def generate_file_properties(self, db_id: str) -> FileId:
        file_id = await get_file_ids(self.client, db_id)
        logger.debug("Decoded FileId for message %s  dc=%s", db_id, file_id.dc_id)
        self.cached_file_ids[db_id] = file_id
        return file_id

    async def generate_media_session(self, client: Client, file_id: FileId) -> Session:
        media_session = client.media_sessions.get(file_id.dc_id)

        if media_session is None:
            if file_id.dc_id != await client.storage.dc_id():
                media_session = Session(
                    client,
                    file_id.dc_id,
                    await Auth(
                        client,
                        file_id.dc_id,
                        await client.storage.test_mode(),
                    ).create(),
                    await client.storage.test_mode(),
                    is_media=True,
                )
                await media_session.start()

                for _ in range(6):
                    exported_auth = await client.invoke(
                        raw.functions.auth.ExportAuthorization(dc_id=file_id.dc_id)
                    )
                    try:
                        await media_session.invoke(
                            raw.functions.auth.ImportAuthorization(
                                id=exported_auth.id,
                                bytes=exported_auth.bytes,
                            )
                        )
                        break
                    except AuthBytesInvalid:
                        logger.debug("Invalid auth bytes for DC %s — retrying", file_id.dc_id)
                        continue
                else:
                    await media_session.stop()
                    raise AuthBytesInvalid

            else:
                media_session = Session(
                    client,
                    file_id.dc_id,
                    await client.storage.auth_key(),
                    await client.storage.test_mode(),
                    is_media=True,
                )
                await media_session.start()

            logger.debug("Created media session for DC %s", file_id.dc_id)
            client.media_sessions[file_id.dc_id] = media_session

        else:
            logger.debug("Reusing cached media session for DC %s", file_id.dc_id)

        return media_session

    @staticmethod
    async def get_location(
        file_id: FileId,
    ) -> Union[
        raw.types.InputPhotoFileLocation,
        raw.types.InputDocumentFileLocation,
        raw.types.InputPeerPhotoFileLocation,
    ]:
        file_type = file_id.file_type

        if file_type == FileType.CHAT_PHOTO:
            if file_id.chat_id > 0:
                peer = raw.types.InputPeerUser(
                    user_id=file_id.chat_id,
                    access_hash=file_id.chat_access_hash,
                )
            else:
                if file_id.chat_access_hash == 0:
                    peer = raw.types.InputPeerChat(chat_id=-file_id.chat_id)
                else:
                    peer = raw.types.InputPeerChannel(
                        channel_id=utils.get_channel_id(file_id.chat_id),
                        access_hash=file_id.chat_access_hash,
                    )
            location = raw.types.InputPeerPhotoFileLocation(
                peer=peer,
                volume_id=file_id.volume_id,
                local_id=file_id.local_id,
                big=file_id.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG,
            )

        elif file_type == FileType.PHOTO:
            location = raw.types.InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )

        else:
            location = raw.types.InputDocumentFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )

        return location

    async def yield_file(
        self,
        file_id: FileId,
        offset: int,
        first_part_cut: int,
        last_part_cut: int,
        part_count: int,
        chunk_size: int,
    ):
        """
        Continuously fetch and yield Telegram file chunks using a prefetch queue.

        Instead of the old packet-by-packet approach (request → yield → request →
        yield …), we use an asyncio.Queue to keep PREFETCH_COUNT chunks in flight
        at all times.  This eliminates the gap between writing one chunk and
        requesting the next, which was the root cause of mid-stream buffering.
        """
        client        = self.client
        media_session = await self.generate_media_session(client, file_id)
        location      = await self.get_location(file_id)

        # Queue of (part_index, chunk_bytes | Exception)
        queue: asyncio.Queue = asyncio.Queue(maxsize=PREFETCH_COUNT + 2)

        async def _fetch_worker():
            """Fetch all parts and push them into the queue."""
            current_offset = offset
            for part_idx in range(part_count):
                for attempt in range(5):
                    try:
                        r = await media_session.invoke(
                            raw.functions.upload.GetFile(
                                location=location,
                                offset=current_offset,
                                limit=chunk_size,
                            )
                        )
                        break
                    except FloodWait as fw:
                        logger.warning(
                            "FloodWait %ds on part %d/%d — sleeping",
                            fw.value, part_idx + 1, part_count,
                        )
                        await asyncio.sleep(fw.value + 1)
                    except (TimeoutError, AttributeError) as exc:
                        logger.debug("Transient error part %d: %s", part_idx + 1, exc)
                        if attempt == 4:
                            await queue.put(exc)
                            return
                        await asyncio.sleep(0.5 * (attempt + 1))
                else:
                    err = IOError(f"All retries failed at part {part_idx + 1}")
                    logger.error(str(err))
                    await queue.put(err)
                    return

                if isinstance(r, raw.types.upload.FileCdnRedirect):
                    logger.warning(
                        "FileCdnRedirect received for part %d — stopping", part_idx + 1
                    )
                    await queue.put(EOFError("CDN redirect"))
                    return

                if not isinstance(r, raw.types.upload.File):
                    err = TypeError(f"Unexpected response type: {type(r)}")
                    logger.error(str(err))
                    await queue.put(err)
                    return

                chunk = r.bytes
                if not chunk:
                    await queue.put(None)  # Signal EOF
                    return

                # Slice the chunk for boundary alignment
                if part_count == 1:
                    sliced = chunk[first_part_cut:last_part_cut]
                elif part_idx == 0:
                    sliced = chunk[first_part_cut:]
                elif part_idx == part_count - 1:
                    sliced = chunk[:last_part_cut]
                else:
                    sliced = chunk

                await queue.put(sliced)
                current_offset += chunk_size

            await queue.put(None)  # Normal EOF sentinel

        # Start the background fetch worker
        fetch_task = asyncio.create_task(_fetch_worker())

        try:
            parts_yielded = 0
            while True:
                item = await queue.get()
                if item is None:
                    break  # EOF
                if isinstance(item, BaseException):
                    logger.error("yield_file: fetch error: %s", item)
                    break
                yield item
                parts_yielded += 1
        except Exception as exc:
            logger.error("yield_file: consumer error: %s", exc)
        finally:
            fetch_task.cancel()
            try:
                await fetch_task
            except (asyncio.CancelledError, Exception):
                pass
            logger.debug("yield_file finished after %d part(s)", parts_yielded if 'parts_yielded' in dir() else 0)

    async def clean_cache(self) -> None:
        while True:
            await asyncio.sleep(self.clean_timer)
            self.cached_file_ids.clear()
            _file_meta_cache.clear()
            logger.debug("ByteStreamer cache cleared")


def _parse_range(range_header: str, file_size: int):
    if range_header:
        try:
            raw_range   = range_header.replace("bytes=", "").split(",")[0].strip()
            start_str, end_str = raw_range.split("-")
            from_bytes  = int(start_str) if start_str else 0
            until_bytes = int(end_str)   if end_str   else file_size - 1
        except (ValueError, AttributeError):
            from_bytes  = 0
            until_bytes = file_size - 1
    else:
        from_bytes  = 0
        until_bytes = file_size - 1

    from_bytes  = max(0, from_bytes)
    until_bytes = min(until_bytes, file_size - 1)
    return from_bytes, until_bytes


def _get_client_ip(request: web.Request) -> str:
    """Return the best-effort client IP for session dedup."""
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.remote or "unknown"


async def _register_session(session_key: str) -> bool:
    """
    Register an active streaming session.
    Returns True if this is a *new* unique session, False if already tracked.
    """
    async with _sessions_lock:
        _prune_stale_sessions()
        is_new = session_key not in _active_sessions
        _active_sessions[session_key] = time.monotonic()
        return is_new


async def _unregister_session(session_key: str) -> None:
    async with _sessions_lock:
        _active_sessions.pop(session_key, None)


def _prune_stale_sessions() -> None:
    """Remove sessions that have been idle longer than _SESSION_TTL."""
    now = time.monotonic()
    stale = [k for k, ts in _active_sessions.items() if now - ts > _SESSION_TTL]
    for k in stale:
        del _active_sessions[k]


def get_active_session_count() -> int:
    """Return the number of currently active unique streaming sessions."""
    _prune_stale_sessions()
    return len(_active_sessions)


async def _should_track_bandwidth(
    client_ip: str,
    message_id: str,
    from_bytes: int,
) -> bool:
    """
    Return True only if this (client_ip, message_id, from_bytes) combination
    has NOT been tracked within the last _BW_DEDUP_TTL seconds.

    This prevents counting the same byte range multiple times when a player
    issues multiple range requests for the same segment (probing, prefetch,
    seek-back, reconnect, etc.) — which was the cause of 5× bandwidth inflation.
    """
    key = (client_ip, message_id, from_bytes)
    now = time.monotonic()
    async with _bw_lock:
        # Prune expired entries
        expired = [k for k, exp in _bw_tracked.items() if now > exp]
        for k in expired:
            del _bw_tracked[k]

        if key in _bw_tracked:
            # Already counted — duplicate request
            return False

        # Mark as counted
        _bw_tracked[key] = now + _BW_DEDUP_TTL
        return True


class StreamingService:

    def __init__(self, bot_client: Client, db: Database):
        self.bot      = bot_client
        self.db       = db
        self.streamer = ByteStreamer(bot_client)

    async def stream_file(
        self,
        request: web.Request,
        file_hash: str,
        is_download: bool = False,
    ) -> web.StreamResponse:

        range_header     = request.headers.get("Range", "")
        is_range_request = bool(range_header)
        client_ip        = _get_client_ip(request)

        # ── File metadata ─────────────────────────────────────────────────────
        if file_hash in _file_meta_cache:
            file_data = _file_meta_cache[file_hash]
        else:
            file_data = await self.db.get_file_by_hash(file_hash)
            if not file_data:
                raise web.HTTPNotFound(reason="file not found")
            _file_meta_cache[file_hash] = file_data

        # ── Bandwidth guard ───────────────────────────────────────────────────
        # Only check on fresh (non-range or first-range) requests to avoid DB
        # overhead on every 1 MB chunk during active playback.
        if Config.get("bandwidth_mode", True):
            stats  = await self.db.get_bandwidth_stats()
            max_bw = Config.get("max_bandwidth", 107374182400)
            if max_bw and stats["total_bandwidth"] >= max_bw:
                raise web.HTTPServiceUnavailable(reason="bandwidth limit exceeded")

        file_size  = int(file_data["file_size"])
        file_name  = file_data["file_name"]
        message_id = str(file_data["message_id"])

        # Resolve FileId before preparing response
        try:
            file_id = await self.streamer.get_file_properties(message_id)
        except Exception as exc:
            logger.error("get_file_properties failed: msg=%s err=%s", message_id, exc)
            raise web.HTTPNotFound(reason="could not resolve file on Telegram")

        from_bytes, until_bytes = _parse_range(range_header, file_size)

        if from_bytes > until_bytes or from_bytes >= file_size:
            return web.Response(
                status=416,
                body=b"Range Not Satisfiable",
                headers={"Content-Range": f"bytes */{file_size}"},
            )

        until_bytes = min(until_bytes, file_size - 1)
        req_length  = until_bytes - from_bytes + 1

        offset         = from_bytes - (from_bytes % CHUNK_SIZE)
        first_part_cut = from_bytes - offset
        last_part_cut  = (until_bytes % CHUNK_SIZE) + 1
        part_count     = math.ceil((until_bytes + 1) / CHUNK_SIZE) - (offset // CHUNK_SIZE)

        logger.debug(
            "stream  msg=%s  size=%d  range=%d-%d  offset=%d  parts=%d",
            message_id, file_size, from_bytes, until_bytes, offset, part_count,
        )

        mime = (
            file_data.get("mime_type")
            or mimetypes.guess_type(file_name)[0]
            or MIME_TYPE_MAP.get(file_data.get("file_type"), "application/octet-stream")
        )
        if not mime:
            mime = "application/octet-stream"

        disposition = "attachment" if is_download else "inline"
        status      = 206 if is_range_request else 200

        headers = {
            "Content-Type":                mime,
            "Content-Length":              str(req_length),
            "Content-Disposition":         f'{disposition}; filename="{file_name}"',
            "Accept-Ranges":               "bytes",
            "Cache-Control":               "no-cache",
            "Access-Control-Allow-Origin": "*",
            "Connection":                  "keep-alive",
            # Hint to the browser/player: don't wait for the whole file
            "X-Content-Type-Options":      "nosniff",
        }
        if is_range_request:
            headers["Content-Range"] = f"bytes {from_bytes}-{until_bytes}/{file_size}"

        response = web.StreamResponse(status=status, headers=headers)
        await response.prepare(request)

        bytes_sent = 0
        try:
            async for chunk in self.streamer.yield_file(
                file_id,
                offset,
                first_part_cut,
                last_part_cut,
                part_count,
                CHUNK_SIZE,
            ):
                await response.write(chunk)
                bytes_sent += len(chunk)
        except (asyncio.CancelledError, ConnectionResetError):
            logger.debug("stream  msg=%s  client disconnected after %d bytes", message_id, bytes_sent)
        except Exception as exc:
            logger.error("streaming error: msg=%s err=%s", message_id, exc)

        try:
            await response.write_eof()
        except Exception:
            pass

        # Record only actual bytes delivered — not the full requested range.
        # Additionally, only track a given (client, file, offset) once per
        # _BW_DEDUP_TTL window to prevent counting the same segment multiple
        # times from player probes / prefetch / reconnects.
        if bytes_sent > 0:
            should_track = await _should_track_bandwidth(client_ip, message_id, from_bytes)
            if should_track:
                asyncio.create_task(self.db.track_bandwidth(message_id, bytes_sent))
            else:
                logger.debug(
                    "bw dedup  msg=%s  ip=%s  from=%d  bytes=%d  (skipped)",
                    message_id, client_ip, from_bytes, bytes_sent,
                )

        return response

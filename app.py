import hashlib
import hmac
import json
import logging
import time
import asyncio
from pathlib import Path
from urllib.parse import parse_qs, unquote

import psutil
from aiohttp import web
import aiohttp_jinja2
import jinja2

from bot import Bot
from config import Config
from database import Database
from helper import (
    StreamingService,
    check_bandwidth_limit,
    check_user_bandwidth_limit,
    format_size,
    format_reset_countdown,
)
from helper.stream import (
    get_active_session_count,
    _register_session,
    _unregister_session,
    _get_client_ip,
    _mime_for_filename,
    is_browser_playable,
    MIME_TYPE_MAP,
)

logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"


def _bot_info(bot: Bot) -> dict:
    me = getattr(bot, "me", None)
    return {
        "bot_name":     (me.first_name if me else None) or "FLiX Bot",
        "bot_username": (me.username   if me else None) or "FLiX_Bot",
        "bot_id":       str(me.id)    if me else "N/A",
        "bot_dc":       str(me.dc_id) if me else "N/A",
    }


def _verify_telegram_init_data(init_data: str, bot_token: str) -> dict | None:
    """
    Verify Telegram Web App initData against the bot token.
    Returns parsed user dict on success, None on failure.
    """
    try:
        parsed    = parse_qs(init_data)
        hash_val  = parsed.pop("hash", [None])[0]
        if not hash_val:
            return None

        # Build the data-check string
        data_pairs = sorted(
            (k, v[0]) for k, v in parsed.items()
        )
        data_check = "\n".join(f"{k}={v}" for k, v in data_pairs)

        # HMAC-SHA256 with key = HMAC-SHA256("WebAppData", bot_token)
        secret_key = hmac.new(
            b"WebAppData",
            bot_token.encode(),
            hashlib.sha256,
        ).digest()
        expected = hmac.new(secret_key, data_check.encode(), hashlib.sha256).hexdigest()

        if not hmac.compare_digest(expected, hash_val):
            return None

        # Parse user JSON
        user_raw = parsed.get("user", [None])[0]
        if user_raw:
            import json as _json
            return _json.loads(unquote(user_raw))
        return {}
    except Exception as e:
        logger.warning("Telegram initData verification failed: %s", e)
        return None


def build_app(bot: Bot, database) -> web.Application:
    streaming_service = StreamingService(bot, database)

    @web.middleware
    async def not_found_middleware(request: web.Request, handler):
        try:
            return await handler(request)
        except web.HTTPNotFound:
            return await _render_not_found(request)
        except web.HTTPServiceUnavailable:
            return await _render_bandwidth_exceeded(request)

    async def _render_not_found(request: web.Request) -> web.Response:
        try:
            info = _bot_info(bot)
            return aiohttp_jinja2.render_template(
                "not_found.html",
                request,
                {"bot_name": info["bot_name"], "bot_username": info["bot_username"]},
            )
        except Exception as exc:
            logger.error("not_found template error: %s", exc)
            return web.Response(status=404, text="404 — File not found", content_type="text/plain")

    async def _render_bandwidth_exceeded(request: web.Request) -> web.Response:
        try:
            info = _bot_info(bot)
            return aiohttp_jinja2.render_template(
                "bandwidth_exceeded.html",
                request,
                {
                    "bot_name":       info["bot_name"],
                    "bot_username":   info["bot_username"],
                    "owner_username": "FLiX_LY",
                },
            )
        except Exception as exc:
            logger.error("bandwidth_exceeded template error: %s", exc)
            return web.Response(
                status=503,
                text="Bandwidth limit exceeded",
                content_type="text/plain",
            )

    app = web.Application(middlewares=[not_found_middleware])
    aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader(str(TEMPLATES_DIR)))

    # ── Home ─────────────────────────────────────────────────────────────────

    @aiohttp_jinja2.template("home.html")
    async def home(request: web.Request):
        info = _bot_info(bot)
        return {
            "bot_name":       info["bot_name"],
            "bot_username":   info["bot_username"],
            "owner_username": "FLiX_LY",
        }

    # ── Stream / Download ─────────────────────────────────────────────────────

    async def _tracked_stream(request: web.Request, file_hash: str, is_download: bool):
        client_ip   = _get_client_ip(request)
        session_key = f"{file_hash}:{client_ip}"
        await _register_session(session_key)
        try:
            return await streaming_service.stream_file(request, file_hash, is_download=is_download)
        finally:
            await _unregister_session(session_key)

    async def stream_page(request: web.Request):
        file_hash = request.match_info["file_hash"]
        accept    = request.headers.get("Accept", "")
        range_h   = request.headers.get("Range", "")

        if range_h or "text/html" not in accept:
            return await _tracked_stream(request, file_hash, is_download=False)

        file_data = await database.get_file_by_hash(file_hash)
        if not file_data:
            raise web.HTTPNotFound(reason="File not found")

        try:
            from helper.stream import get_file_ids
            await get_file_ids(bot, str(file_data["message_id"]))
        except web.HTTPNotFound:
            raise
        except Exception as exc:
            logger.warning(
                "stream_page Flog verification failed: hash=%s err=%s", file_hash, exc
            )
            raise web.HTTPNotFound(reason="File no longer available on Telegram")

        # Check global bandwidth
        allowed, _ = await check_bandwidth_limit(database)
        if not allowed:
            raise web.HTTPServiceUnavailable(reason="bandwidth limit exceeded")

        base      = str(request.url.origin())
        file_type = (
            "video"   if file_data["file_type"] == Config.FILE_TYPE_VIDEO
            else "audio" if file_data["file_type"] == Config.FILE_TYPE_AUDIO
            else "document"
        )

        mime = (
            file_data.get("mime_type")
            or _mime_for_filename(
                file_data["file_name"],
                MIME_TYPE_MAP.get(file_data.get("file_type"), "application/octet-stream"),
            )
            or "application/octet-stream"
        )
        playable = is_browser_playable(mime)

        info = _bot_info(bot)
        thumbnail_url = f"{base}/stream/{file_hash}"
        context = {
            "bot_name":         info["bot_name"],
            "bot_username":     info["bot_username"],
            "owner_username":   "FLiX_LY",
            "file_name":        file_data["file_name"],
            "file_size":        format_size(file_data["file_size"]),
            "file_type":        file_type,
            "mime_type":        mime,
            "browser_playable": playable,
            "stream_url":       f"{base}/stream/{file_hash}",
            "download_url":     f"{base}/dl/{file_hash}",
            "thumbnail_url":    thumbnail_url,
            "telegram_url":     f"https://t.me/{info['bot_username']}?start={file_hash}",
        }
        return aiohttp_jinja2.render_template("stream.html", request, context)

    async def download_file(request: web.Request):
        file_hash = request.match_info["file_hash"]
        return await _tracked_stream(request, file_hash, is_download=True)

    # ── Admin Panel Data ──────────────────────────────────────────────────────

    async def _collect_panel_data():
        try:
            stats    = await database.get_stats()
            bw_stats = await database.get_bandwidth_stats()
        except Exception:
            stats    = {"total_users": 0, "total_files": 0}
            bw_stats = {"total_bandwidth": 0, "today_bandwidth": 0, "reset_info": {}}

        max_bw    = Config.get("max_bandwidth", 107374182400)
        bw_mode   = Config.get("bandwidth_mode", True)
        bw_used   = bw_stats["total_bandwidth"]
        bw_today  = bw_stats["today_bandwidth"]
        remaining = max(0, max_bw - bw_used)
        bw_pct    = round((bw_used / max_bw * 100) if max_bw else 0, 1)

        # Bandwidth reset info
        reset_info    = bw_stats.get("reset_info", {})
        secs_left     = reset_info.get("seconds_until_reset", 0)
        reset_in_str  = format_reset_countdown(secs_left)

        try:
            ram          = psutil.virtual_memory()
            ram_pct      = ram.percent
            ram_used_fmt = format_size(ram.used)
            cpu_pct      = psutil.cpu_percent(interval=None)
        except Exception:
            ram_pct      = 0
            ram_used_fmt = "N/A"
            cpu_pct      = 0

        uptime_seconds = time.time() - Config.UPTIME if Config.UPTIME else 0
        uptime_str     = _format_uptime(uptime_seconds)

        info = _bot_info(bot)

        return {
            **info,
            "total_users":       stats.get("total_users",  0),
            "total_chats":       stats.get("total_users",  0),
            "total_files":       stats.get("total_files",  0),
            "ram_used":          ram_used_fmt,
            "ram_pct":           ram_pct,
            "cpu_pct":           cpu_pct,
            "uptime":            uptime_str,
            "bw_mode":           bw_mode,
            "bw_limit":          format_size(max_bw),
            "bw_used":           format_size(bw_used),
            "bw_today":          format_size(bw_today),
            "bw_remaining":      format_size(remaining),
            "bw_pct":            bw_pct,
            "bw_reset_in":       reset_in_str,
            "bw_seconds_left":   secs_left,
            "bot_status":        "running" if getattr(bot, "me", None) else "initializing",
            "active_conns":      get_active_session_count(),
        }

    def _format_uptime(seconds: float) -> str:
        seconds = int(seconds)
        d, seconds = divmod(seconds, 86400)
        h, seconds = divmod(seconds, 3600)
        m, s       = divmod(seconds, 60)
        parts = []
        if d: parts.append(f"{d}d")
        if h: parts.append(f"{h}h")
        if m: parts.append(f"{m}m")
        parts.append(f"{s}s")
        return " ".join(parts)

    async def bot_settings_page(request: web.Request):
        try:
            ctx = await _collect_panel_data()
            return aiohttp_jinja2.render_template("bot_settings.html", request, ctx)
        except Exception as exc:
            logger.error("bot_settings page error: %s", exc)
            return web.Response(status=500, text="Internal server error")

    # ── API Endpoints ─────────────────────────────────────────────────────────

    async def api_stats(request: web.Request):
        try:
            stats    = await database.get_stats()
            bw_stats = await database.get_bandwidth_stats()
            max_bw   = Config.get("max_bandwidth", 107374182400)
            bw_used  = bw_stats["total_bandwidth"]
            bw_today = bw_stats["today_bandwidth"]
            bw_pct   = round((bw_used / max_bw * 100) if max_bw else 0, 1)
            reset_info = bw_stats.get("reset_info", {})

            try:
                ram          = psutil.virtual_memory()
                cpu_pct      = psutil.cpu_percent(interval=None)
                ram_used_fmt = format_size(ram.used)
            except Exception:
                cpu_pct      = 0
                ram_used_fmt = "N/A"

            uptime_str = _format_uptime(time.time() - Config.UPTIME if Config.UPTIME else 0)

            payload = {
                "total_users": stats.get("total_users", 0),
                "total_chats": stats.get("total_users", 0),
                "total_files": stats.get("total_files", 0),
                "ram_used":    ram_used_fmt,
                "cpu_pct":     cpu_pct,
                "uptime":      uptime_str,
                "bw_pct":      bw_pct,
                "bw_used":     format_size(bw_used),
                "bw_today":    format_size(bw_today),
                "bw_limit":    format_size(max_bw),
                "bw_reset_in": format_reset_countdown(reset_info.get("seconds_until_reset", 0)),
                "bw_seconds_left": reset_info.get("seconds_until_reset", 0),
            }
            return web.Response(text=json.dumps(payload), content_type="application/json")
        except Exception as exc:
            logger.error("api_stats error: %s", exc)
            return web.json_response({"error": str(exc)}, status=500)

    async def api_bandwidth(request: web.Request):
        try:
            stats     = await database.get_bandwidth_stats()
            max_bw    = Config.get("max_bandwidth", 107374182400)
            bw_mode   = Config.get("bandwidth_mode", True)
            used      = stats["total_bandwidth"]
            today     = stats["today_bandwidth"]
            remaining = max(0, max_bw - used)
            pct       = round((used / max_bw * 100) if max_bw else 0, 1)
            reset_info = stats.get("reset_info", {})
            secs_left  = reset_info.get("seconds_until_reset", 0)

            payload = {
                **{k: v for k, v in stats.items() if k != "reset_info"},
                "limit":               max_bw,
                "remaining":           remaining,
                "percentage":          pct,
                "bandwidth_mode":      bw_mode,
                "reset_info":          reset_info,
                "reset_countdown":     format_reset_countdown(secs_left),
                "seconds_until_reset": secs_left,
                "formatted": {
                    "total_bandwidth": format_size(used),
                    "today_bandwidth": format_size(today),
                    "limit":           format_size(max_bw),
                    "remaining":       format_size(remaining),
                    "reset_in":        format_reset_countdown(secs_left),
                },
            }
            return web.Response(text=json.dumps(payload), content_type="application/json")
        except Exception as exc:
            logger.error("api_bandwidth error: %s", exc)
            return web.json_response({"error": str(exc)}, status=500)

    async def api_health(request: web.Request):
        try:
            info = _bot_info(bot)
            payload = {
                "status":       "ok",
                "bot_status":   "running" if getattr(bot, "me", None) else "initializing",
                "bot_name":     info["bot_name"],
                "bot_username": info["bot_username"],
                "bot_id":       info["bot_id"],
                "bot_dc":       info["bot_dc"],
                "active_conns": get_active_session_count(),
            }
            return web.Response(text=json.dumps(payload), content_type="application/json")
        except Exception as exc:
            logger.error("api_health error: %s", exc)
            return web.json_response({"error": str(exc)}, status=500)

    async def api_user_check(request: web.Request):
        """Check per-user bandwidth/limit status."""
        try:
            user_id = request.query.get("user_id", "")
            if not user_id:
                return web.json_response({"error": "user_id required"}, status=400)

            allowed, reason, info = await check_user_bandwidth_limit(database, user_id)
            ubw = await database.get_user_bandwidth(user_id)
            return web.json_response({
                "user_id":    user_id,
                "allowed":    allowed,
                "reason":     reason,
                "bw_used":    ubw["total_bytes"],
                "bw_used_fmt":format_size(ubw["total_bytes"]),
                "bw_limit":   info.get("bw_limit", 0),
                "bw_limit_fmt": format_size(info.get("bw_limit", 0)) if info.get("bw_limit", 0) else "Unlimited",
                "files_used": info.get("files_used", 0),
                "files_limit": info.get("files_limit", 0),
                "is_blocked": info.get("blocked", False),
                "warn_sent":  info.get("warn_sent", False),
            })
        except Exception as exc:
            logger.error("api_user_check error: %s", exc)
            return web.json_response({"error": str(exc)}, status=500)

    # ── Telegram Web App ──────────────────────────────────────────────────────

    async def twa_page(request: web.Request):
        """Serve the Telegram Web App main page."""
        try:
            info = _bot_info(bot)
            return aiohttp_jinja2.render_template(
                "twa.html",
                request,
                {
                    "bot_name":     info["bot_name"],
                    "bot_username": info["bot_username"],
                    "bot_token_provided": bool(Config.BOT_TOKEN),
                },
            )
        except Exception as exc:
            logger.error("twa_page error: %s", exc)
            return web.Response(status=500, text="Internal server error")

    async def api_twa_auth(request: web.Request):
        """
        Validate Telegram Web App initData and return user session info.
        POST body: { "initData": "<telegram initData string>" }
        """
        try:
            body     = await request.json()
            init_data = body.get("initData", "")
            if not init_data:
                return web.json_response({"ok": False, "error": "initData missing"}, status=400)

            user = _verify_telegram_init_data(init_data, Config.BOT_TOKEN)
            if user is None:
                return web.json_response({"ok": False, "error": "Invalid initData"}, status=401)

            user_id = str(user.get("id", ""))

            # Load user data from DB
            db_user  = await database.get_user(user_id) or {}
            ubw      = await database.get_user_bandwidth(user_id)
            lim_info = await database.check_user_limit(user_id)

            # Global bandwidth
            bw_stats  = await database.get_bandwidth_stats()
            max_bw    = Config.get("max_bandwidth", 107374182400)
            bw_used   = bw_stats["total_bandwidth"]
            bw_today  = bw_stats["today_bandwidth"]
            bw_pct    = round((bw_used / max_bw * 100) if max_bw else 0, 1)
            reset_info = bw_stats.get("reset_info", {})

            # Check global bandwidth limit
            global_ok, _ = await check_bandwidth_limit(database)

            return web.json_response({
                "ok":           True,
                "user": {
                    "id":         user_id,
                    "first_name": user.get("first_name", ""),
                    "last_name":  user.get("last_name", ""),
                    "username":   user.get("username", ""),
                    "photo_url":  user.get("photo_url", ""),
                    "is_blocked": db_user.get("is_blocked", False),
                    "is_owner":   int(user_id) in Config.OWNER_ID if user_id.isdigit() else False,
                    "is_sudo":    await database.is_sudo_user(user_id),
                },
                "limits": {
                    "allowed":         lim_info.get("allowed", True),
                    "global_bw_ok":    global_ok,
                    "bw_used":         ubw["total_bytes"],
                    "bw_used_fmt":     format_size(ubw["total_bytes"]),
                    "bw_limit":        lim_info.get("bw_limit", 0),
                    "bw_limit_fmt":    format_size(lim_info.get("bw_limit", 0)) if lim_info.get("bw_limit", 0) else "Unlimited",
                    "files_used":      lim_info.get("files_used", 0),
                    "files_limit":     lim_info.get("files_limit", 0),
                },
                "global_bandwidth": {
                    "used":       format_size(bw_used),
                    "today":      format_size(bw_today),
                    "limit":      format_size(max_bw),
                    "remaining":  format_size(max(0, max_bw - bw_used)),
                    "pct":        bw_pct,
                    "reset_in":   format_reset_countdown(reset_info.get("seconds_until_reset", 0)),
                    "seconds_until_reset": reset_info.get("seconds_until_reset", 0),
                },
            })
        except json.JSONDecodeError:
            return web.json_response({"ok": False, "error": "Invalid JSON"}, status=400)
        except Exception as exc:
            logger.error("api_twa_auth error: %s", exc)
            return web.json_response({"ok": False, "error": str(exc)}, status=500)

    async def api_twa_files(request: web.Request):
        """
        Return paginated file list for a verified TWA user.
        Requires header: X-TWA-InitData: <initData>
        """
        try:
            init_data = request.headers.get("X-TWA-InitData", "")
            user      = _verify_telegram_init_data(init_data, Config.BOT_TOKEN)
            if user is None:
                return web.json_response({"ok": False, "error": "Unauthorized"}, status=401)

            user_id = str(user.get("id", ""))

            # Check user access
            allowed, reason, _info = await check_user_bandwidth_limit(database, user_id)
            global_ok, _ = await check_bandwidth_limit(database)
            if not global_ok or not allowed:
                return web.json_response({
                    "ok": False,
                    "error": "Access restricted",
                    "reason": reason or "global_bw_exceeded",
                }, status=403)

            page  = int(request.query.get("page", 1))
            limit = min(int(request.query.get("limit", 20)), 50)
            skip  = (page - 1) * limit

            cursor, total = await database.find_files(user_id, [skip + 1, limit])
            files = []
            async for f in cursor:
                files.append({
                    "file_id":   f.get("file_id", ""),
                    "file_name": f.get("file_name", ""),
                    "file_size": f.get("file_size", 0),
                    "file_size_fmt": format_size(f.get("file_size", 0)),
                    "file_type": f.get("file_type", "document"),
                    "mime_type": f.get("mime_type", ""),
                    "created_at": f["created_at"].isoformat() if f.get("created_at") else "",
                    "stream_url":   f"/stream/{f.get('file_id', '')}",
                    "download_url": f"/dl/{f.get('file_id', '')}",
                })

            return web.json_response({
                "ok":    True,
                "files": files,
                "total": total,
                "page":  page,
                "pages": max(1, -(-total // limit)),
            })
        except Exception as exc:
            logger.error("api_twa_files error: %s", exc)
            return web.json_response({"ok": False, "error": str(exc)}, status=500)

    async def api_twa_file(request: web.Request):
        """Return single file info for TWA."""
        try:
            init_data = request.headers.get("X-TWA-InitData", "")
            user      = _verify_telegram_init_data(init_data, Config.BOT_TOKEN)
            if user is None:
                return web.json_response({"ok": False, "error": "Unauthorized"}, status=401)

            file_hash = request.match_info["file_hash"]
            file_data = await database.get_file_by_hash(file_hash)
            if not file_data:
                return web.json_response({"ok": False, "error": "File not found"}, status=404)

            info = _bot_info(bot)
            base = str(request.url.origin())
            mime = (
                file_data.get("mime_type")
                or _mime_for_filename(file_data["file_name"], "application/octet-stream")
                or "application/octet-stream"
            )
            return web.json_response({
                "ok": True,
                "file": {
                    "file_id":     file_data.get("file_id", ""),
                    "file_name":   file_data.get("file_name", ""),
                    "file_size":   file_data.get("file_size", 0),
                    "file_size_fmt": format_size(file_data.get("file_size", 0)),
                    "file_type":   file_data.get("file_type", "document"),
                    "mime_type":   mime,
                    "playable":    is_browser_playable(mime),
                    "stream_url":  f"{base}/stream/{file_hash}",
                    "download_url": f"{base}/dl/{file_hash}",
                    "telegram_url": f"https://t.me/{info['bot_username']}?start={file_hash}",
                },
            })
        except Exception as exc:
            logger.error("api_twa_file error: %s", exc)
            return web.json_response({"ok": False, "error": str(exc)}, status=500)

    # ── Legacy redirect endpoints ─────────────────────────────────────────────

    async def stats_endpoint(request: web.Request):
        if "application/json" in request.headers.get("Accept", ""):
            return await api_stats(request)
        raise web.HTTPFound("/bot_settings")

    async def bandwidth_endpoint(request: web.Request):
        if "application/json" in request.headers.get("Accept", ""):
            return await api_bandwidth(request)
        raise web.HTTPFound("/bot_settings")

    async def health_endpoint(request: web.Request):
        if "application/json" in request.headers.get("Accept", ""):
            return await api_health(request)
        raise web.HTTPFound("/bot_settings")

    # ── Router ────────────────────────────────────────────────────────────────

    app.router.add_get("/",                         home)
    app.router.add_get("/stream/{file_hash}",       stream_page)
    app.router.add_get("/dl/{file_hash}",           download_file)
    app.router.add_get("/bot_settings",             bot_settings_page)
    app.router.add_get("/twa",                      twa_page)
    app.router.add_get("/api/stats",                api_stats)
    app.router.add_get("/api/bandwidth",            api_bandwidth)
    app.router.add_get("/api/health",               api_health)
    app.router.add_get("/api/user/check",           api_user_check)
    app.router.add_get("/stats",                    stats_endpoint)
    app.router.add_get("/bandwidth",                bandwidth_endpoint)
    app.router.add_get("/health",                   health_endpoint)
    # TWA API
    app.router.add_post("/api/twa/auth",            api_twa_auth)
    app.router.add_get("/api/twa/files",            api_twa_files)
    app.router.add_get("/api/twa/file/{file_hash}", api_twa_file)

    return app

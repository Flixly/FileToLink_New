import json
import logging
import time
import asyncio
from pathlib import Path

import psutil
from aiohttp import web
import aiohttp_jinja2
import jinja2

from bot import Bot
from config import Config
from database import Database
from helper import StreamingService, check_bandwidth_limit, format_size
from helper.stream import (
    get_active_session_count,
    _register_session,
    _unregister_session,
    _get_client_ip,
)

logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"

# Active connection count is now managed by stream.py's dedup session tracker.
# _active_connections kept for backward-compat references only — read via get_active_session_count().


def _bot_info(bot: Bot) -> dict:
    me = getattr(bot, "me", None)
    return {
        "bot_name":     (me.first_name if me else None) or DEFAULT_BOT_NAME,
        "bot_username": (me.username   if me else None) or DEFAULT_BOT_USERNAME,
        "bot_id":       str(me.id)    if me else "N/A",
        "bot_dc":       str(me.dc_id) if me else "N/A",
    }


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

    @aiohttp_jinja2.template("home.html")
    async def home(request: web.Request):
        info = _bot_info(bot)
        return {
            "bot_name":       info["bot_name"],
            "bot_username":   info["bot_username"],
            "owner_username": "FLiX_LY",
        }

    async def _tracked_stream(request: web.Request, file_hash: str, is_download: bool):
        """
        Wrap stream_file with accurate unique-session tracking.
        
        A session key is (file_hash, client_ip).  Multiple range-requests from
        the same IP playing the same file count as ONE session, eliminating the
        inflated viewer count that occurred when a single player issued 3-5
        probe/prefetch requests before starting playback.
        """
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

        allowed, _ = await check_bandwidth_limit(database)
        if not allowed:
            raise web.HTTPServiceUnavailable(reason="bandwidth limit exceeded")

        base      = str(request.url.origin())
        file_type = (
            "video"   if file_data["file_type"] == Config.FILE_TYPE_VIDEO
            else "audio" if file_data["file_type"] == Config.FILE_TYPE_AUDIO
            else "document"
        )
        info = _bot_info(bot)
        context = {
            "bot_name":       info["bot_name"],
            "bot_username":   info["bot_username"],
            "owner_username": "FLiX_LY",
            "file_name":      file_data["file_name"],
            "file_size":      format_size(file_data["file_size"]),
            "file_type":      file_type,
            "stream_url":     f"{base}/stream/{file_hash}",
            "download_url":   f"{base}/dl/{file_hash}",
            "telegram_url":   f"https://t.me/{info['bot_username']}?start={file_hash}",
        }
        return aiohttp_jinja2.render_template("stream.html", request, context)

    async def download_file(request: web.Request):
        file_hash = request.match_info["file_hash"]
        return await _tracked_stream(request, file_hash, is_download=True)

    async def _collect_panel_data():
        try:
            stats    = await database.get_stats()
            bw_stats = await database.get_bandwidth_stats()
        except Exception:
            stats    = {"total_users": 0, "total_files": 0}
            bw_stats = {"total_bandwidth": 0, "today_bandwidth": 0}

        max_bw    = Config.get("max_bandwidth", 107374182400)
        bw_mode   = Config.get("bandwidth_mode", True)
        bw_used   = bw_stats["total_bandwidth"]
        bw_today  = bw_stats["today_bandwidth"]
        remaining = max(0, max_bw - bw_used)
        bw_pct    = round((bw_used / max_bw * 100) if max_bw else 0, 1)

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
            "total_users":  stats.get("total_users",  0),
            "total_chats":  stats.get("total_users",  0),
            "total_files":  stats.get("total_files",  0),
            "ram_used":     ram_used_fmt,
            "ram_pct":      ram_pct,
            "cpu_pct":      cpu_pct,
            "uptime":       uptime_str,
            "bw_mode":      bw_mode,
            "bw_limit":     format_size(max_bw),
            "bw_used":      format_size(bw_used),
            "bw_today":     format_size(bw_today),
            "bw_remaining": format_size(remaining),
            "bw_pct":       bw_pct,
            "bot_status":   "running" if getattr(bot, "me", None) else "initializing",
            "active_conns": get_active_session_count(),
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

    async def api_stats(request: web.Request):
        try:
            stats    = await database.get_stats()
            bw_stats = await database.get_bandwidth_stats()
            max_bw   = Config.get("max_bandwidth", 107374182400)
            bw_used  = bw_stats["total_bandwidth"]
            bw_today = bw_stats["today_bandwidth"]
            bw_pct   = round((bw_used / max_bw * 100) if max_bw else 0, 1)

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
            payload = {
                **stats,
                "limit":          max_bw,
                "remaining":      remaining,
                "percentage":     pct,
                "bandwidth_mode": bw_mode,
                "formatted": {
                    "total_bandwidth": format_size(used),
                    "today_bandwidth": format_size(today),
                    "limit":           format_size(max_bw),
                    "remaining":       format_size(remaining),
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
                "status":                  "ok",
                "bot_status":              "running" if getattr(bot, "me", None) else "initializing",
                "bot_name":                info["bot_name"],
                "bot_username":            info["bot_username"],
                "bot_id":                  info["bot_id"],
                "bot_dc":                  info["bot_dc"],
                "active_conns":            get_active_session_count(),
                "active_conns_description": "Live streaming/download sessions currently transferring bytes",
            }
            return web.Response(text=json.dumps(payload), content_type="application/json")
        except Exception as exc:
            logger.error("api_health error: %s", exc)
            return web.json_response({"error": str(exc)}, status=500)

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

    # ── Inline-query thumbnail icons served locally ───────────
    # These SVG icons are served directly from our web server so
    # Telegram inline results can always load a thumbnail with minimal
    # latency and zero dependency on external CDNs.
    # Designed to be clean, modern, and visually attractive.
    _ICON_SVGS = {
        # 🎬 Video icon — gradient purple/blue with play button + film strips
        "media": (
            b'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100">'
            b'<defs>'
            b'<linearGradient id="vg" x1="0" y1="0" x2="1" y2="1">'
            b'<stop offset="0%" stop-color="#667eea"/>'
            b'<stop offset="100%" stop-color="#764ba2"/>'
            b'</linearGradient>'
            b'<filter id="vs"><feDropShadow dx="0" dy="3" stdDeviation="3" flood-opacity="0.35"/></filter>'
            b'</defs>'
            b'<rect width="100" height="100" rx="22" fill="url(#vg)"/>'
            b'<rect x="14" y="22" width="72" height="56" rx="10" fill="rgba(255,255,255,0.12)" stroke="rgba(255,255,255,0.25)" stroke-width="1.5"/>'
            b'<polygon points="40,32 40,68 70,50" fill="white" filter="url(#vs)"/>'
            b'<rect x="14" y="22" width="72" height="10" rx="5" fill="rgba(0,0,0,0.2)"/>'
            b'<rect x="14" y="68" width="72" height="10" rx="5" fill="rgba(0,0,0,0.2)"/>'
            b'<rect x="22" y="22" width="6" height="10" rx="2" fill="rgba(255,255,255,0.5)"/>'
            b'<rect x="36" y="22" width="6" height="10" rx="2" fill="rgba(255,255,255,0.5)"/>'
            b'<rect x="58" y="22" width="6" height="10" rx="2" fill="rgba(255,255,255,0.5)"/>'
            b'<rect x="72" y="22" width="6" height="10" rx="2" fill="rgba(255,255,255,0.5)"/>'
            b'<rect x="22" y="68" width="6" height="10" rx="2" fill="rgba(255,255,255,0.5)"/>'
            b'<rect x="36" y="68" width="6" height="10" rx="2" fill="rgba(255,255,255,0.5)"/>'
            b'<rect x="58" y="68" width="6" height="10" rx="2" fill="rgba(255,255,255,0.5)"/>'
            b'<rect x="72" y="68" width="6" height="10" rx="2" fill="rgba(255,255,255,0.5)"/>'
            b'</svg>'
        ),
        # 🎵 Audio icon — gradient purple with headphone design
        "audio": (
            b'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100">'
            b'<defs>'
            b'<linearGradient id="ag" x1="0" y1="0" x2="1" y2="1">'
            b'<stop offset="0%" stop-color="#a855f7"/>'
            b'<stop offset="100%" stop-color="#6366f1"/>'
            b'</linearGradient>'
            b'<filter id="as"><feDropShadow dx="0" dy="3" stdDeviation="3" flood-opacity="0.3"/></filter>'
            b'</defs>'
            b'<rect width="100" height="100" rx="22" fill="url(#ag)"/>'
            b'<path d="M28,50 C28,35 36,22 50,22 C64,22 72,35 72,50" fill="none" stroke="white" stroke-width="5" stroke-linecap="round" filter="url(#as)"/>'
            b'<rect x="20" y="48" width="14" height="20" rx="7" fill="white"/>'
            b'<rect x="66" y="48" width="14" height="20" rx="7" fill="white"/>'
            b'<circle cx="50" cy="58" r="10" fill="rgba(255,255,255,0.15)" stroke="rgba(255,255,255,0.4)" stroke-width="2"/>'
            b'<circle cx="50" cy="58" r="4" fill="white"/>'
            b'</svg>'
        ),
        # 🖼️ Photo icon — gradient cyan/teal with camera frame
        "photo": (
            b'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100">'
            b'<defs>'
            b'<linearGradient id="pg" x1="0" y1="0" x2="1" y2="1">'
            b'<stop offset="0%" stop-color="#06b6d4"/>'
            b'<stop offset="100%" stop-color="#0ea5e9"/>'
            b'</linearGradient>'
            b'<filter id="ps"><feDropShadow dx="0" dy="3" stdDeviation="3" flood-opacity="0.3"/></filter>'
            b'</defs>'
            b'<rect width="100" height="100" rx="22" fill="url(#pg)"/>'
            b'<rect x="14" y="28" width="72" height="52" rx="10" fill="rgba(255,255,255,0.15)" stroke="rgba(255,255,255,0.35)" stroke-width="2"/>'
            b'<path d="M36,28 L42,18 L58,18 L64,28" fill="rgba(255,255,255,0.2)" stroke="rgba(255,255,255,0.35)" stroke-width="2" stroke-linejoin="round"/>'
            b'<circle cx="50" cy="54" r="14" fill="rgba(255,255,255,0.0)" stroke="white" stroke-width="3" filter="url(#ps)"/>'
            b'<circle cx="50" cy="54" r="8" fill="white"/>'
            b'<circle cx="72" cy="38" r="4" fill="white" opacity="0.7"/>'
            b'</svg>'
        ),
        # 📄 Document icon — gradient green with lined page
        "document": (
            b'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100">'
            b'<defs>'
            b'<linearGradient id="dg" x1="0" y1="0" x2="1" y2="1">'
            b'<stop offset="0%" stop-color="#10b981"/>'
            b'<stop offset="100%" stop-color="#059669"/>'
            b'</linearGradient>'
            b'<filter id="ds"><feDropShadow dx="0" dy="3" stdDeviation="3" flood-opacity="0.3"/></filter>'
            b'</defs>'
            b'<rect width="100" height="100" rx="22" fill="url(#dg)"/>'
            b'<g filter="url(#ds)">'
            b'<path d="M26,16 L62,16 L76,30 L76,84 L26,84 Z" fill="white" rx="4"/>'
            b'<path d="M62,16 L62,30 L76,30 Z" fill="rgba(16,185,129,0.3)"/>'
            b'</g>'
            b'<line x1="36" y1="44" x2="66" y2="44" stroke="#10b981" stroke-width="4" stroke-linecap="round"/>'
            b'<line x1="36" y1="54" x2="66" y2="54" stroke="#10b981" stroke-width="4" stroke-linecap="round"/>'
            b'<line x1="36" y1="64" x2="54" y2="64" stroke="#10b981" stroke-width="4" stroke-linecap="round"/>'
            b'</svg>'
        ),
        # 📁 Default / folder icon — gradient orange/amber
        "folder": (
            b'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100">'
            b'<defs>'
            b'<linearGradient id="fg" x1="0" y1="0" x2="1" y2="1">'
            b'<stop offset="0%" stop-color="#f59e0b"/>'
            b'<stop offset="100%" stop-color="#d97706"/>'
            b'</linearGradient>'
            b'</defs>'
            b'<rect width="100" height="100" rx="22" fill="url(#fg)"/>'
            b'<path d="M16,38 L16,72 Q16,78 22,78 L78,78 Q84,78 84,72 L84,42 Q84,36 78,36 L50,36 L44,26 L22,26 Q16,26 16,32 Z" fill="white"/>'
            b'<path d="M16,38 L84,38 L84,72 Q84,78 78,78 L22,78 Q16,78 16,72 Z" fill="rgba(255,255,255,0.85)"/>'
            b'</svg>'
        ),
    }

    async def serve_icon(request: web.Request):
        name = request.match_info["name"]
        svg  = _ICON_SVGS.get(name)
        if svg is None:
            raise web.HTTPNotFound()
        return web.Response(
            body=svg,
            content_type="image/svg+xml",
            headers={
                "Cache-Control": "public, max-age=86400",
                "Access-Control-Allow-Origin": "*",
            },
        )

    app.router.add_get("/",                   home)
    app.router.add_get("/stream/{file_hash}", stream_page)
    app.router.add_get("/dl/{file_hash}",     download_file)
    app.router.add_get("/bot_settings",       bot_settings_page)
    app.router.add_get("/api/stats",          api_stats)
    app.router.add_get("/api/bandwidth",      api_bandwidth)
    app.router.add_get("/api/health",         api_health)
    app.router.add_get("/stats",              stats_endpoint)
    app.router.add_get("/bandwidth",          bandwidth_endpoint)
    app.router.add_get("/health",             health_endpoint)
    app.router.add_get("/icons/{name}",       serve_icon)

    return app

import logging
from config import Config

logger = logging.getLogger(__name__)


async def check_bandwidth_limit(db):
    """Check global bandwidth limit. Returns (allowed, stats)."""
    try:
        stats  = await db.get_bandwidth_stats()
        max_bw = Config.get("max_bandwidth", 107374182400)
        if Config.get("bandwidth_mode", True) and stats["total_bandwidth"] >= max_bw:
            return False, stats
        return True, stats
    except Exception as e:
        logger.error("bandwidth check error: %s", e)
        return True, {}


async def check_user_bandwidth_limit(db, user_id: str):
    """
    Check per-user bandwidth & file limit.
    Returns (allowed, reason, limit_info_dict)
    """
    try:
        info = await db.check_user_limit(str(user_id))
        if not info["allowed"]:
            if info["blocked"]:
                reason = "blocked_by_admin"
            elif not info["bw_ok"]:
                reason = "bandwidth_exceeded"
            elif not info["files_ok"]:
                reason = "file_limit_exceeded"
            else:
                reason = "unknown"
            return False, reason, info
        return True, None, info
    except Exception as e:
        logger.error("check_user_bandwidth_limit error: %s", e)
        return True, None, {}


def format_reset_countdown(seconds: int) -> str:
    """Format seconds into human-readable countdown string."""
    if seconds <= 0:
        return "Overdue"
    days,    rem  = divmod(int(seconds), 86400)
    hours,   rem  = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    parts = []
    if days:    parts.append(f"{days}d")
    if hours:   parts.append(f"{hours}h")
    if minutes: parts.append(f"{minutes}m")
    if not parts:
        parts.append(f"{secs}s")
    return " ".join(parts[:2])  # show max 2 units for brevity

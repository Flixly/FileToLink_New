import logging
from config import Config

logger = logging.getLogger(__name__)


async def check_bandwidth_limit(db):
    """
    Check global bot-wide bandwidth limit.
    Returns (allowed: bool, stats: dict).
    Uses the legacy daily-stats total for backward compat with the stream
    pipeline, AND also checks the monthly cycle.
    """
    try:
        if not Config.get("bandwidth_mode", True):
            return True, {}

        # Primary check: monthly global cycle
        cycle_stats = await db.get_global_bw_cycle()
        max_bw      = Config.get("max_bandwidth", 107374182400)
        if max_bw > 0 and cycle_stats["used"] >= max_bw:
            return False, cycle_stats

        return True, cycle_stats
    except Exception as e:
        logger.error("bandwidth check error: %s", e)
        return True, {}


async def check_user_bandwidth_limit(db, user_id: str) -> tuple:
    """
    Check per-user monthly bandwidth limit.
    Returns (allowed: bool, stats: dict).
    """
    try:
        if not Config.get("user_bw_mode", True):
            return True, {}

        max_ubw = Config.get("max_user_bandwidth", 10737418240)
        if max_ubw <= 0:
            return True, {}

        allowed, stats = await db.check_user_bw_limit(str(user_id))
        return allowed, stats
    except Exception as e:
        logger.error("check_user_bandwidth_limit error: %s", e)
        return True, {}


async def should_warn_global_bw(db) -> bool:
    """Return True if the global bandwidth warning threshold has been crossed."""
    try:
        warn_pct = Config.get("bw_warn_pct", 80)
        if warn_pct <= 0:
            return False
        stats = await db.get_global_bw_cycle()
        return stats["pct"] >= warn_pct
    except Exception as e:
        logger.error("should_warn_global_bw error: %s", e)
        return False


async def should_warn_user_bw(db, user_id: str) -> bool:
    """Return True if the user's bandwidth warning threshold has been crossed."""
    try:
        warn_pct = Config.get("user_bw_warn_pct", 80)
        if warn_pct <= 0:
            return False
        stats = await db.get_user_bw(str(user_id))
        return stats["pct"] >= warn_pct
    except Exception as e:
        logger.error("should_warn_user_bw error: %s", e)
        return False


async def track_bandwidth_usage(db, message_id: str, size: int, user_id: str):
    """
    Unified bandwidth tracking:
    - legacy daily bandwidth collection
    - global monthly cycle
    - per-user monthly cycle
    """
    try:
        # Legacy per-file + daily tracking
        await db.track_bandwidth(message_id, size)
        # Monthly global cycle
        await db.record_global_bw(size)
        # Monthly per-user cycle
        if user_id:
            await db.record_user_bw(str(user_id), size)
    except Exception as e:
        logger.error("track_bandwidth_usage error: %s", e)

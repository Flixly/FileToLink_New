import logging
from config import Config

logger = logging.getLogger(__name__)

BW_WARN_THRESHOLD = 0.85


async def check_bandwidth_limit(db):
    try:
        stats  = await db.get_bandwidth_stats()
        max_bw = Config.get("max_bandwidth", 107374182400)
        if not Config.get("bandwidth_mode", True):
            return True, stats
        if max_bw and stats["total_bandwidth"] >= max_bw:
            return False, stats
        return True, stats
    except Exception as e:
        logger.error("bandwidth check error: %s", e)
        return True, {}


async def check_user_bandwidth(db, user_id: str, is_privileged: bool = False) -> tuple:
    if is_privileged:
        return True, {}
    try:
        max_user_bw = Config.get("max_user_bandwidth", 0)
        if not max_user_bw or not Config.get("bandwidth_mode", True):
            return True, {}
        user_bw = await db.get_user_bandwidth(user_id)
        used    = user_bw.get("bw_used", 0)
        if used >= max_user_bw:
            return False, user_bw
        return True, user_bw
    except Exception as e:
        logger.error("user bandwidth check error: %s", e)
        return True, {}


def should_warn_global(stats: dict) -> bool:
    max_bw = Config.get("max_bandwidth", 107374182400)
    if not max_bw:
        return False
    used = stats.get("total_bandwidth", 0)
    return (used / max_bw) >= BW_WARN_THRESHOLD


def should_warn_user(user_bw: dict) -> bool:
    max_user_bw = Config.get("max_user_bandwidth", 0)
    if not max_user_bw:
        return False
    used = user_bw.get("bw_used", 0)
    return (used / max_user_bw) >= BW_WARN_THRESHOLD

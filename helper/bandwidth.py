import logging
from config import Config

logger = logging.getLogger(__name__)


def _is_privileged_user(user_id: str) -> bool:
    """
    Return True when the user is an owner or has elevated privileges
    that exempt them from per-user bandwidth limits.

    Owner IDs are sourced from Config.OWNER_ID (static list).
    Sudo users are checked via the database when db is provided.
    This helper is a fast synchronous check for owners only.
    """
    try:
        uid_int = int(user_id)
        return uid_int in Config.OWNER_ID
    except (ValueError, TypeError):
        return False


async def is_exempt_from_user_bw(db, user_id: str) -> bool:
    """
    Return True if this user is exempt from per-user bandwidth limits.
    Exempt roles:
      - Owner (in Config.OWNER_ID)
      - Sudo users (stored in DB)
    """
    try:
        # Fast path: owner check (no DB call)
        if _is_privileged_user(user_id):
            return True
        # Sudo check
        return await db.is_sudo_user(str(user_id))
    except Exception as e:
        logger.error("is_exempt_from_user_bw error: %s", e)
        return False


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
    Exempt users (owner / sudo) always pass.
    Returns (allowed: bool, stats: dict).
    """
    try:
        if not Config.get("user_bw_mode", True):
            return True, {}

        max_ubw = Config.get("max_user_bandwidth", 10737418240)
        if max_ubw <= 0:
            return True, {}

        # Privileged users bypass the limit entirely
        if await is_exempt_from_user_bw(db, user_id):
            return True, {"exempt": True}

        allowed, stats = await db.check_user_bw_limit(str(user_id))
        return allowed, stats
    except Exception as e:
        logger.error("check_user_bandwidth_limit error: %s", e)
        return True, {}


async def should_warn_global_bw(db) -> bool:
    """
    Return True if the global bandwidth warning threshold has been crossed
    AND the warning has NOT yet been sent this cycle.
    Sends only once per cycle (de-duplicated via DB flag).
    """
    try:
        warn_pct = Config.get("bw_warn_pct", 80)
        if warn_pct <= 0:
            return False
        stats = await db.get_global_bw_cycle()
        if stats["pct"] < warn_pct:
            return False
        # Already warned this cycle?
        already_warned = await db.get_global_bw_warned()
        if already_warned:
            return False
        # Mark as warned so we don't fire again
        await db.mark_global_bw_warned()
        return True
    except Exception as e:
        logger.error("should_warn_global_bw error: %s", e)
        return False


async def should_warn_user_bw(db, user_id: str) -> bool:
    """
    Return True if the user's bandwidth warning threshold has been crossed
    AND the warning has NOT yet been sent this cycle.
    Sends only once per cycle (de-duplicated via DB flag).
    Exempt users are never warned.
    """
    try:
        # Never warn privileged users
        if await is_exempt_from_user_bw(db, user_id):
            return False

        warn_pct = Config.get("user_bw_warn_pct", 80)
        if warn_pct <= 0:
            return False
        stats = await db.get_user_bw(str(user_id), lazy=True)
        if stats.get("no_usage_yet"):
            return False
        if stats["pct"] < warn_pct:
            return False
        # Already warned this cycle?
        already_warned = await db.get_user_bw_warned(str(user_id), "warned_pct")
        if already_warned:
            return False
        # Mark as warned
        await db.mark_user_bw_warned(str(user_id), "warned_pct")
        return True
    except Exception as e:
        logger.error("should_warn_user_bw error: %s", e)
        return False


async def should_warn_user_limit_exceeded(db, user_id: str) -> bool:
    """
    Return True if user has just hit their hard limit AND the limit-exceeded
    warning has NOT yet been sent this cycle.
    Sends only once per cycle.
    Exempt users are never warned.
    """
    try:
        if await is_exempt_from_user_bw(db, user_id):
            return False
        already_warned = await db.get_user_bw_warned(str(user_id), "warned_limit")
        if already_warned:
            return False
        # Mark as warned now (before send to avoid race)
        await db.mark_user_bw_warned(str(user_id), "warned_limit")
        return True
    except Exception as e:
        logger.error("should_warn_user_limit_exceeded error: %s", e)
        return False


async def track_bandwidth_usage(db, message_id: str, size: int, user_id: str):
    """
    Unified bandwidth tracking:
    - legacy daily bandwidth collection
    - global monthly cycle
    - per-user monthly cycle (only for non-exempt users)
    """
    try:
        # Legacy per-file + daily tracking
        await db.track_bandwidth(message_id, size)
        # Monthly global cycle
        await db.record_global_bw(size)
        # Monthly per-user cycle — only track for normal users
        if user_id:
            exempt = await is_exempt_from_user_bw(db, user_id)
            if not exempt:
                await db.record_user_bw(str(user_id), size)
    except Exception as e:
        logger.error("track_bandwidth_usage error: %s", e)

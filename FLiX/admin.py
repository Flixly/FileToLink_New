import asyncio
import logging
import os
import time

from pyrogram import Client, filters, StopPropagation
from pyrogram.enums import ChatMemberStatus
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from config import Config
from database import db
from helper import (
    small_caps, format_size, escape_markdown,
    format_uptime, human_size, check_owner,
)

logger = logging.getLogger(__name__)

# ── Ban reason presets ────────────────────────────────────────────────────────
BAN_REASONS = {
    "abuse":     "🚫 Service Abuse — misuse of the bot's features or API.",
    "content":   "🔞 Restricted / Prohibited Content — sharing illegal or harmful content.",
    "spam":      "📨 Spam — excessive unsolicited requests.",
}


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS — display / formatting
# ══════════════════════════════════════════════════════════════════════════════

def _mention(user_id: str, first_name: str = "", username: str = "") -> str:
    """Return a Telegram inline mention string for a user."""
    display = first_name or username or str(user_id)
    display = display[:30]
    return f"[{display}](tg://user?id={user_id})"


def _user_line(doc: dict) -> str:
    uid   = doc.get("user_id", "?")
    fname = doc.get("first_name", "")
    uname = doc.get("username", "")
    return f"• {_mention(uid, fname, uname)} `{uid}`"


# ══════════════════════════════════════════════════════════════════════════════
# PANEL BUILDER — central show_panel function
# ══════════════════════════════════════════════════════════════════════════════

async def show_panel(client: Client, source, panel_type: str, extra: dict = None):
    config  = Config.all()
    extra   = extra or {}
    msg     = source.message if isinstance(source, CallbackQuery) else source

    # ── Main settings panel ────────────────────────────────────────────────
    if panel_type == "main_panel":
        max_bw     = Config.get("max_bandwidth", 107374182400)
        bw_toggle  = Config.get("bandwidth_mode", True)
        ubw_toggle = Config.get("user_bw_mode", True)
        max_ubw    = Config.get("max_user_bandwidth", 10737418240)
        text = (
            f"✨ **{small_caps('bot settings panel')}** ✨\n\n"
            f"📡 **{small_caps('global bw')}**   : {'🟢 ᴀᴄᴛɪᴠᴇ' if bw_toggle else '🔴 ɪɴᴀᴄᴛɪᴠᴇ'} | `{format_size(max_bw)}`\n"
            f"👤 **{small_caps('user bw')}**     : {'🟢 ᴀᴄᴛɪᴠᴇ' if ubw_toggle else '🔴 ɪɴᴀᴄᴛɪᴠᴇ'} | `{format_size(max_ubw)}`\n"
            f"👥 **{small_caps('sudo users')}**  : ᴍᴀɴᴀɢᴇ ᴀᴄᴄᴇꜱꜱ\n"
            f"🚫 **{small_caps('ban system')}**  : ᴍᴀɴᴀɢᴇ ʙᴀɴꜱ\n"
            f"🤖 **{small_caps('bot mode')}**    : {'🟢 ᴘᴜʙʟɪᴄ' if config.get('public_bot') else '🔴 ᴘʀɪᴠᴀᴛᴇ'}\n"
            f"📢 **{small_caps('force sub')}**   : {'🟢 ᴀᴄᴛɪᴠᴇ' if config.get('fsub_mode') else '🔴 ɪɴᴀᴄᴛɪᴠᴇ'}\n\n"
            "👇 ᴄʜᴏᴏꜱᴇ ᴀ ᴄᴀᴛᴇɢᴏʀʏ ᴛᴏ ᴄᴏɴꜰɪɢᴜʀᴇ."
        )
        buttons = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📡 ʙᴀɴᴅᴡɪᴅᴛʜ",   callback_data="settings_bandwidth"),
                InlineKeyboardButton("👥 ꜱᴜᴅᴏ ᴜꜱᴇʀꜱ",  callback_data="settings_sudo"),
            ],
            [
                InlineKeyboardButton("🚫 ʙᴀɴ ꜱʏꜱᴛᴇᴍ",  callback_data="settings_ban"),
                InlineKeyboardButton("🤖 ʙᴏᴛ ᴍᴏᴅᴇ",    callback_data="settings_botmode"),
            ],
            [
                InlineKeyboardButton("📢 ꜰᴏʀᴄᴇ ꜱᴜʙ",   callback_data="settings_fsub"),
            ],
            [InlineKeyboardButton("❌ ᴄʟᴏꜱᴇ",          callback_data="settings_close")],
        ])

    # ── Bandwidth panel ────────────────────────────────────────────────────
    elif panel_type == "bandwidth_panel":
        max_bw     = Config.get("max_bandwidth", 107374182400)
        bw_toggle  = Config.get("bandwidth_mode", True)
        ubw_toggle = Config.get("user_bw_mode", True)
        max_ubw    = Config.get("max_user_bandwidth", 10737418240)
        bw_warn    = Config.get("bw_warn_pct", 80)
        ubw_warn   = Config.get("user_bw_warn_pct", 80)

        cycle      = await db.get_global_bw_cycle()
        bw_used    = cycle.get("used", 0)
        bw_pct     = cycle.get("pct", 0)
        days_r     = cycle.get("days_remaining", 30)
        bw_today   = (await db.get_bandwidth_stats())["today_bandwidth"]

        text = (
            f"💠 **{small_caps('bandwidth settings')}** 💠\n\n"
            f"📡 **{small_caps('global bw mode')}** : {'🟢 ᴀᴄᴛɪᴠᴇ' if bw_toggle else '🔴 ɪɴᴀᴄᴛɪᴠᴇ'}\n"
            f"📊 **{small_caps('global limit')}**   : `{format_size(max_bw)}`\n"
            f"📤 **{small_caps('global used')}**    : `{format_size(bw_used)}` ({bw_pct:.1f}%)\n"
            f"📅 **{small_caps('today')}**          : `{format_size(bw_today)}`\n"
            f"🔄 **{small_caps('resets in')}**      : `{days_r}` ᴅᴀʏꜱ\n"
            f"⚠️ **{small_caps('warn at')}**        : `{bw_warn}%`\n\n"
            f"👤 **{small_caps('per-user bw mode')}**: {'🟢 ᴀᴄᴛɪᴠᴇ' if ubw_toggle else '🔴 ɪɴᴀᴄᴛɪᴠᴇ'}\n"
            f"👤 **{small_caps('user limit')}**     : `{format_size(max_ubw)}`\n"
            f"⚠️ **{small_caps('user warn at')}**   : `{ubw_warn}%`"
        )
        buttons = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("⚡ ᴛᴏɢɢʟᴇ ɢʟᴏʙᴀʟ",  callback_data="toggle_bandwidth"),
                InlineKeyboardButton("⚡ ᴛᴏɢɢʟᴇ ᴜꜱᴇʀ",    callback_data="toggle_user_bw"),
            ],
            [
                InlineKeyboardButton("✏️ ɢʟᴏʙᴀʟ ʟɪᴍɪᴛ",  callback_data="set_bandwidth_limit"),
                InlineKeyboardButton("✏️ ᴜꜱᴇʀ ʟɪᴍɪᴛ",    callback_data="set_user_bw_limit"),
            ],
            [
                InlineKeyboardButton("⚠️ ᴡᴀʀɴ %",         callback_data="set_bw_warn_pct"),
                InlineKeyboardButton("⚠️ ᴜꜱᴇʀ ᴡᴀʀɴ %",   callback_data="set_user_bw_warn_pct"),
            ],
            [InlineKeyboardButton("🔄 ʀᴇꜱᴇᴛ ᴀʟʟ ᴜꜱᴀɢᴇ",  callback_data="reset_bandwidth")],
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ",              callback_data="settings_back")],
        ])

    # ── Sudo panel ─────────────────────────────────────────────────────────
    elif panel_type == "sudo_panel":
        sudo_users  = await db.get_sudo_users()
        count       = len(sudo_users)
        # Show only 5 most recent
        recent      = sudo_users[:5]
        recent_text = "\n".join(_user_line(u) for u in recent) if recent else "  ɴᴏɴᴇ"
        more        = count - len(recent)
        text = (
            f"💠 **{small_caps('sudo users')}** 💠\n\n"
            f"👥 **{small_caps('count')}** : `{count}`\n\n"
            f"**{small_caps('recent')}:**\n{recent_text}"
            + (f"\n\n_…ᴀɴᴅ {more} ᴍᴏʀᴇ_" if more > 0 else "")
        )
        btns = [
            [
                InlineKeyboardButton("➕ ᴀᴅᴅ",        callback_data="sudo_add"),
                InlineKeyboardButton("➖ ʀᴇᴍᴏᴠᴇ",     callback_data="sudo_remove"),
            ],
            [
                InlineKeyboardButton("📋 ᴜꜱᴇʀꜱ ʟɪꜱᴛ", callback_data="sudo_list_page_1"),
                InlineKeyboardButton("📜 ʜɪꜱᴛᴏʀʏ",    callback_data="sudo_history"),
            ],
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ",          callback_data="settings_back")],
        ]
        buttons = InlineKeyboardMarkup(btns)

    # ── Sudo full list ─────────────────────────────────────────────────────
    elif panel_type == "sudo_list":
        page       = extra.get("page", 1)
        page_size  = 10
        sudo_users = await db.get_sudo_users()
        total      = len(sudo_users)
        start      = (page - 1) * page_size
        end        = start + page_size
        chunk      = sudo_users[start:end]
        lines      = "\n".join(_user_line(u) for u in chunk) if chunk else "ɴᴏɴᴇ"
        total_p    = max(1, (total + page_size - 1) // page_size)
        text = (
            f"👥 **{small_caps('sudo users — full list')}**\n\n"
            f"**{small_caps('count')}:** `{total}` | "
            f"**{small_caps('page')}:** `{page}/{total_p}`\n\n"
            f"{lines}"
        )
        nav = []
        if page > 1:
            nav.append(InlineKeyboardButton("◄", callback_data=f"sudo_list_page_{page-1}"))
        nav.append(InlineKeyboardButton(f"{page}/{total_p}", callback_data="N/A"))
        if page < total_p:
            nav.append(InlineKeyboardButton("►", callback_data=f"sudo_list_page_{page+1}"))
        buttons = InlineKeyboardMarkup([
            nav,
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="settings_sudo")],
        ])

    # ── Sudo history ───────────────────────────────────────────────────────
    elif panel_type == "sudo_history":
        history = await db.get_sudo_history(limit=15)
        lines   = []
        for h in history:
            ts     = h["timestamp"].strftime("%Y-%m-%d %H:%M")
            action = "➕ ᴀᴅᴅᴇᴅ" if h["action"] == "add" else "➖ ʀᴇᴍᴏᴠᴇᴅ"
            uid    = h["user_id"]
            fname  = h.get("first_name", "")
            uname  = h.get("username", "")
            actor  = h.get("actor_id", "?")
            lines.append(
                f"{action} {_mention(uid, fname, uname)} `{uid}`\n"
                f"  ᴀᴄᴛᴏʀ: `{actor}` · {ts}"
            )
        history_text = "\n\n".join(lines) if lines else "ɴᴏ ᴀᴄᴛɪᴏɴꜱ ʏᴇᴛ."
        text = (
            f"📜 **{small_caps('sudo history')}**\n\n"
            f"{history_text}"
        )
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="settings_sudo")],
        ])

    # ── Ban panel ──────────────────────────────────────────────────────────
    elif panel_type == "ban_panel":
        banned_users = await db.get_banned_users()
        count        = len(banned_users)
        recent       = banned_users[:5]
        recent_text  = ""
        for u in recent:
            uid   = u.get("user_id", "?")
            fname = u.get("first_name", "")
            uname = u.get("username", "")
            reason= u.get("reason", "")[:40]
            recent_text += f"• {_mention(uid, fname, uname)} `{uid}`\n  _{reason}_\n"
        if not recent_text:
            recent_text = "  ɴᴏɴᴇ"
        more = count - len(recent)
        text = (
            f"🚫 **{small_caps('ban system')}** 🚫\n\n"
            f"🔒 **{small_caps('banned count')}** : `{count}`\n\n"
            f"**{small_caps('recently banned')}:**\n{recent_text}"
            + (f"\n_…ᴀɴᴅ {more} ᴍᴏʀᴇ_" if more > 0 else "")
        )
        btns = [
            [
                InlineKeyboardButton("🔨 ʙᴀɴ ᴜꜱᴇʀ",   callback_data="ban_user"),
                InlineKeyboardButton("🔓 ᴜɴʙᴀɴ ᴜꜱᴇʀ", callback_data="unban_user"),
            ],
            [
                InlineKeyboardButton("📋 ʙᴀɴ ʟɪꜱᴛ",   callback_data="ban_list_page_1"),
                InlineKeyboardButton("📜 ʜɪꜱᴛᴏʀʏ",   callback_data="ban_history"),
            ],
            [
                InlineKeyboardButton("🔍 ᴄʜᴇᴄᴋ ʙᴀɴ",  callback_data="ban_check"),
            ],
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ",           callback_data="settings_back")],
        ]
        buttons = InlineKeyboardMarkup(btns)

    # ── Ban full list ──────────────────────────────────────────────────────
    elif panel_type == "ban_list":
        page          = extra.get("page", 1)
        page_size     = 10
        banned_users  = await db.get_banned_users()
        total         = len(banned_users)
        start         = (page - 1) * page_size
        end           = start + page_size
        chunk         = banned_users[start:end]
        lines         = []
        for u in chunk:
            uid   = u.get("user_id", "?")
            fname = u.get("first_name", "")
            uname = u.get("username", "")
            reason= u.get("reason", "")[:40]
            lines.append(
                f"• {_mention(uid, fname, uname)} `{uid}`\n  _{reason}_"
            )
        list_text = "\n\n".join(lines) if lines else "ɴᴏ ʙᴀɴɴᴇᴅ ᴜꜱᴇʀꜱ."
        total_p   = max(1, (total + page_size - 1) // page_size)
        text = (
            f"🚫 **{small_caps('banned users — full list')}**\n\n"
            f"**{small_caps('count')}:** `{total}` | "
            f"**{small_caps('page')}:** `{page}/{total_p}`\n\n"
            f"{list_text}"
        )
        nav = []
        if page > 1:
            nav.append(InlineKeyboardButton("◄", callback_data=f"ban_list_page_{page-1}"))
        nav.append(InlineKeyboardButton(f"{page}/{total_p}", callback_data="N/A"))
        if page < total_p:
            nav.append(InlineKeyboardButton("►", callback_data=f"ban_list_page_{page+1}"))
        buttons = InlineKeyboardMarkup([
            nav,
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="settings_ban")],
        ])

    # ── Ban history ────────────────────────────────────────────────────────
    elif panel_type == "ban_history":
        history = await db.get_ban_history(limit=15)
        lines   = []
        for h in history:
            ts     = h["timestamp"].strftime("%Y-%m-%d %H:%M")
            action = "🔨 ʙᴀɴɴᴇᴅ" if h["action"] == "ban" else "🔓 ᴜɴʙᴀɴɴᴇᴅ"
            uid    = h["user_id"]
            fname  = h.get("first_name", "")
            uname  = h.get("username", "")
            actor  = h.get("actor_id", "?")
            reason = h.get("reason", "")
            entry  = (
                f"{action} {_mention(uid, fname, uname)} `{uid}`\n"
                f"  ᴀᴄᴛᴏʀ: `{actor}` · {ts}"
            )
            if reason:
                entry += f"\n  _{reason[:50]}_"
            lines.append(entry)
        history_text = "\n\n".join(lines) if lines else "ɴᴏ ᴀᴄᴛɪᴏɴꜱ ʏᴇᴛ."
        text = (
            f"📜 **{small_caps('ban history')}**\n\n"
            f"{history_text}"
        )
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="settings_ban")],
        ])

    # ── Bot mode panel ─────────────────────────────────────────────────────
    elif panel_type == "botmode_panel":
        public = config.get("public_bot", False)
        text = (
            f"💠 **{small_caps('bot mode settings')}** 💠\n\n"
            f"⚡ **{small_caps('current mode')}** : {'🌍 ᴘᴜʙʟɪᴄ' if public else '🔒 ᴘʀɪᴠᴀᴛᴇ'}\n\n"
            f"🌍 **{small_caps('public')}** — ᴀɴʏᴏɴᴇ ᴄᴀɴ ᴜꜱᴇ ᴛʜᴇ ʙᴏᴛ\n"
            f"🔒 **{small_caps('private')}** — ᴏɴʟʏ ꜱᴜᴅᴏ/ᴏᴡɴᴇʀ ᴄᴀɴ ᴜꜱᴇ"
        )
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                f"🔓 {small_caps('set public')}" if not public else f"🔒 {small_caps('set private')}",
                callback_data="toggle_botmode",
            )],
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="settings_back")],
        ])

    # ── Force sub panel ────────────────────────────────────────────────────
    elif panel_type == "fsub_panel":
        fsub_id   = config.get("fsub_chat_id", 0)
        fsub_name = "ɴᴏᴛ ꜱᴇᴛ"
        if fsub_id:
            try:
                fsub_name = (await client.get_chat(fsub_id)).title
            except Exception:
                fsub_name = "❓ ᴜɴᴋɴᴏᴡɴ"

        text = (
            f"💠 **{small_caps('force sub settings')}** 💠\n\n"
            f"⚡ **{small_caps('mode')}**          : {'🟢 ᴀᴄᴛɪᴠᴇ' if config.get('fsub_mode') else '🔴 ɪɴᴀᴄᴛɪᴠᴇ'}\n"
            f"🆔 **{small_caps('channel id')}**   : `{fsub_id or 'ɴᴏᴛ ꜱᴇᴛ'}`\n"
            f"📛 **{small_caps('channel name')}** : `{fsub_name}`\n"
            f"🔗 **{small_caps('invite link')}**  : `{config.get('fsub_inv_link') or 'ɴᴏᴛ ꜱᴇᴛ'}`"
        )
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("⚡ ᴛᴏɢɢʟᴇ", callback_data="toggle_fsub")],
            [
                InlineKeyboardButton(f"🆔 {small_caps('channel id')}", callback_data="set_fsub_id"),
                InlineKeyboardButton(f"🔗 {small_caps('invite link')}",  callback_data="set_fsub_link"),
            ],
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="settings_back")],
        ])

    else:
        return

    # ── Send / edit the message ────────────────────────────────────────────
    if isinstance(source, CallbackQuery):
        try:
            await source.message.edit_text(text, reply_markup=buttons)
        except Exception:
            await client.send_message(
                chat_id=source.message.chat.id,
                text=text,
                reply_markup=buttons,
            )
    else:
        await client.send_message(
            chat_id=source.chat.id,
            text=text,
            reply_to_message_id=source.id,
            reply_markup=buttons,
        )


# ══════════════════════════════════════════════════════════════════════════════
# ASK INPUT helper
# ══════════════════════════════════════════════════════════════════════════════

_pending: dict[int, asyncio.Future] = {}


@Client.on_message(filters.text & filters.private, group=99)
async def _catch_pending(client: Client, message: Message):
    uid = message.from_user.id
    if uid in _pending and not _pending[uid].done():
        _pending[uid].set_result(message)
        raise StopPropagation


async def ask_input(
    client: Client, user_id: int, prompt: str, timeout: int = 60
) -> str | None:
    loop   = asyncio.get_event_loop()
    future = loop.create_future()
    _pending[user_id] = future

    ask_msg = None
    reply   = None
    try:
        ask_msg = await client.send_message(user_id, prompt)
        reply   = await asyncio.wait_for(future, timeout=timeout)
        return reply.text.strip() if reply and reply.text else None
    except asyncio.TimeoutError:
        return None
    except Exception as exc:
        logger.debug("ask_input error for user %s: %s", user_id, exc)
        return None
    finally:
        _pending.pop(user_id, None)
        for m in (ask_msg, reply):
            if m:
                try:
                    await m.delete()
                except Exception:
                    pass


# ══════════════════════════════════════════════════════════════════════════════
# /bot_settings  —  main entry
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.command("bot_settings") & filters.private, group=2)
async def open_settings(client: Client, message: Message):
    if not await check_owner(client, message):
        return
    await show_panel(client, message, "main_panel")


# ══════════════════════════════════════════════════════════════════════════════
# SETTINGS CALLBACK ROUTER
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_callback_query(
    filters.regex(
        r"^(settings_|toggle_|set_|sudo_|reset_|ban_|unban_)"
    ),
    group=2,
)
async def settings_callback(client: Client, callback: CallbackQuery):
    data   = callback.data
    config = Config.all()

    if not await check_owner(client, callback):
        return

    # ── Simple panel navigation ────────────────────────────────────────────
    panel_nav = {
        "settings_bandwidth": ("bandwidth_panel", f"📡 {small_caps('bandwidth settings')}"),
        "settings_sudo":      ("sudo_panel",      f"👥 {small_caps('sudo users')}"),
        "settings_ban":       ("ban_panel",        f"🚫 {small_caps('ban system')}"),
        "settings_botmode":   ("botmode_panel",    f"🤖 {small_caps('bot mode settings')}"),
        "settings_fsub":      ("fsub_panel",       f"📌 {small_caps('force sub settings')}"),
        "settings_back":      ("main_panel",       f"⬅️ {small_caps('back to main menu')}"),
        "sudo_history":       ("sudo_history",     f"📜 {small_caps('sudo history')}"),
        "ban_history":        ("ban_history",      f"📜 {small_caps('ban history')}"),
    }
    if data in panel_nav:
        panel, toast = panel_nav[data]
        await callback.answer(toast, show_alert=False)
        return await show_panel(client, callback, panel)

    # ── Paginated lists ────────────────────────────────────────────────────
    if data.startswith("sudo_list_page_"):
        page = int(data.split("_")[-1])
        await callback.answer()
        return await show_panel(client, callback, "sudo_list", {"page": page})

    if data.startswith("ban_list_page_"):
        page = int(data.split("_")[-1])
        await callback.answer()
        return await show_panel(client, callback, "ban_list", {"page": page})

    # ── Close ──────────────────────────────────────────────────────────────
    if data == "settings_close":
        await callback.answer(f"❌ {small_caps('closing')}", show_alert=True)
        try:
            await callback.message.delete()
        except Exception:
            pass
        return

    # ══════════════════════════════════════════════════════════════════════
    # BANDWIDTH ACTIONS
    # ══════════════════════════════════════════════════════════════════════

    if data == "toggle_bandwidth":
        new_val = not config.get("bandwidth_mode", True)
        await Config.update(db.db, {"bandwidth_mode": new_val})
        await callback.answer(f"✅ {small_caps('global bandwidth toggled')}!", show_alert=True)
        return await show_panel(client, callback, "bandwidth_panel")

    if data == "toggle_user_bw":
        new_val = not config.get("user_bw_mode", True)
        await Config.update(db.db, {"user_bw_mode": new_val})
        await callback.answer(f"✅ {small_caps('per-user bandwidth toggled')}!", show_alert=True)
        return await show_panel(client, callback, "bandwidth_panel")

    if data == "set_bandwidth_limit":
        text = await ask_input(
            client, callback.from_user.id,
            f"📡 **{small_caps('set global monthly bandwidth limit (bytes)')}**\n\n"
            f"{small_caps('examples')}:\n"
            "`107374182400` — 100 GB\n"
            "`53687091200`  — 50 GB\n"
            "`10737418240`  — 10 GB\n\n"
            f"{small_caps('send')} `0` {small_caps('for 100 gb default')}.",
        )
        if text is None:
            return
        if not text.isdigit():
            return await callback.answer(f"❌ {small_caps('invalid number')}!", show_alert=True)
        new_limit = int(text) or 107374182400
        await Config.update(db.db, {"max_bandwidth": new_limit})
        await callback.answer(f"✅ {small_caps('global limit set to')} {format_size(new_limit)}!", show_alert=True)
        return await show_panel(client, callback, "bandwidth_panel")

    if data == "set_user_bw_limit":
        text = await ask_input(
            client, callback.from_user.id,
            f"👤 **{small_caps('set per-user monthly bandwidth limit (bytes)')}**\n\n"
            f"{small_caps('examples')}:\n"
            "`10737418240`  — 10 GB\n"
            "`5368709120`   — 5 GB\n"
            "`2147483648`   — 2 GB\n\n"
            f"{small_caps('send')} `0` {small_caps('to disable per-user limit')}.",
        )
        if text is None:
            return
        if not text.isdigit():
            return await callback.answer(f"❌ {small_caps('invalid number')}!", show_alert=True)
        new_limit = int(text)
        await Config.update(db.db, {"max_user_bandwidth": new_limit})
        lbl = format_size(new_limit) if new_limit else small_caps("unlimited")
        await callback.answer(f"✅ {small_caps('user limit set to')} {lbl}!", show_alert=True)
        return await show_panel(client, callback, "bandwidth_panel")

    if data == "set_bw_warn_pct":
        text = await ask_input(
            client, callback.from_user.id,
            f"⚠️ **{small_caps('set global bw warning threshold (%)')}**\n\n"
            f"{small_caps('enter a number 1-99. send 0 to disable.')}",
        )
        if text is None:
            return
        if not text.isdigit() or not 0 <= int(text) <= 99:
            return await callback.answer(f"❌ {small_caps('enter a number 0-99')}!", show_alert=True)
        await Config.update(db.db, {"bw_warn_pct": int(text)})
        await callback.answer(f"✅ {small_caps('warning set to')} {text}%!", show_alert=True)
        return await show_panel(client, callback, "bandwidth_panel")

    if data == "set_user_bw_warn_pct":
        text = await ask_input(
            client, callback.from_user.id,
            f"⚠️ **{small_caps('set per-user bw warning threshold (%)')}**\n\n"
            f"{small_caps('enter a number 1-99. send 0 to disable.')}",
        )
        if text is None:
            return
        if not text.isdigit() or not 0 <= int(text) <= 99:
            return await callback.answer(f"❌ {small_caps('enter a number 0-99')}!", show_alert=True)
        await Config.update(db.db, {"user_bw_warn_pct": int(text)})
        await callback.answer(f"✅ {small_caps('user warning set to')} {text}%!", show_alert=True)
        return await show_panel(client, callback, "bandwidth_panel")

    if data == "reset_bandwidth":
        await callback.answer(f"🔄 {small_caps('resetting all bandwidth usage')}…", show_alert=False)
        ok = await db.reset_bandwidth()
        if ok:
            await callback.answer(f"✅ {small_caps('all bandwidth usage reset')}!", show_alert=True)
        else:
            await callback.answer(f"❌ {small_caps('reset failed')}.", show_alert=True)
        return await show_panel(client, callback, "bandwidth_panel")

    # ══════════════════════════════════════════════════════════════════════
    # SUDO ACTIONS
    # ══════════════════════════════════════════════════════════════════════

    if data == "sudo_add":
        text = await ask_input(
            client, callback.from_user.id,
            f"👥 **{small_caps('send the user id to add as sudo')}**",
        )
        if text is None:
            return
        if not text.lstrip("-").isdigit():
            return await callback.answer(f"❌ {small_caps('invalid user id')}!", show_alert=True)

        # Try to fetch user info for richer records
        fname, uname = "", ""
        try:
            tg_user = await client.get_users(int(text))
            fname   = tg_user.first_name or ""
            uname   = tg_user.username or ""
        except Exception:
            pass

        await db.add_sudo_user(text, str(callback.from_user.id), uname, fname)
        display = _mention(text, fname, uname)
        await callback.answer(f"✅ {small_caps('added as sudo')}!", show_alert=True)

        # Notify newly-added sudo user if possible
        try:
            await client.send_message(
                int(text),
                f"🎉 **{small_caps('you have been granted sudo access')}**\n\n"
                f"ʏᴏᴜ ᴄᴀɴ ɴᴏᴡ ᴜꜱᴇ ᴛʜᴇ ʙᴏᴛ ᴡɪᴛʜ ᴇʟᴇᴠᴀᴛᴇᴅ ᴘᴇʀᴍɪꜱꜱɪᴏɴꜱ.",
            )
        except Exception:
            pass
        return await show_panel(client, callback, "sudo_panel")

    if data == "sudo_remove":
        text = await ask_input(
            client, callback.from_user.id,
            f"👥 **{small_caps('send the user id to remove from sudo')}**",
        )
        if text is None:
            return
        result = await db.remove_sudo_user(text, str(callback.from_user.id))
        if result:
            await callback.answer(f"✅ `{text}` {small_caps('removed from sudo')}!", show_alert=True)
            # Notify the removed user if possible
            try:
                await client.send_message(
                    int(text),
                    f"ℹ️ **{small_caps('your sudo access has been revoked')}**.",
                )
            except Exception:
                pass
        else:
            await callback.answer(f"❌ `{text}` {small_caps('not found in sudo list')}.", show_alert=True)
        return await show_panel(client, callback, "sudo_panel")

    # ══════════════════════════════════════════════════════════════════════
    # BAN ACTIONS
    # ══════════════════════════════════════════════════════════════════════

    if data == "ban_user":
        # Step 1: get user ID
        uid_text = await ask_input(
            client, callback.from_user.id,
            f"🔨 **{small_caps('send the user id to ban')}**",
        )
        if uid_text is None:
            return
        if not uid_text.lstrip("-").isdigit():
            return await callback.answer(f"❌ {small_caps('invalid user id')}!", show_alert=True)

        target_id = uid_text
        # Check if already banned
        if await db.is_banned(target_id):
            return await callback.answer(
                f"⚠️ {small_caps('user')} `{target_id}` {small_caps('is already banned')}.",
                show_alert=True,
            )

        # Prevent banning owner or self
        if int(target_id) in Config.OWNER_ID:
            return await callback.answer(
                f"❌ {small_caps('cannot ban the bot owner')}.", show_alert=True
            )

        # Step 2: show reason selection
        await callback.message.edit_text(
            f"🔨 **{small_caps('select ban reason')}**\n\n"
            f"👤 **{small_caps('target')}:** `{target_id}`\n\n"
            "ᴄʜᴏᴏꜱᴇ ᴀ ʀᴇᴀꜱᴏɴ ᴏʀ ᴇɴᴛᴇʀ ᴀ ᴄᴜꜱᴛᴏᴍ ᴍᴇꜱꜱᴀɢᴇ:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(
                    "🚫 ꜱᴇʀᴠɪᴄᴇ ᴀʙᴜꜱᴇ",
                    callback_data=f"banreason_{target_id}_abuse",
                )],
                [InlineKeyboardButton(
                    "🔞 ᴘʀᴏʜɪʙɪᴛᴇᴅ ᴄᴏɴᴛᴇɴᴛ",
                    callback_data=f"banreason_{target_id}_content",
                )],
                [InlineKeyboardButton(
                    "📨 ꜱᴘᴀᴍ",
                    callback_data=f"banreason_{target_id}_spam",
                )],
                [InlineKeyboardButton(
                    "✏️ ᴄᴜꜱᴛᴏᴍ ᴍᴇꜱꜱᴀɢᴇ",
                    callback_data=f"banreason_{target_id}_custom",
                )],
                [InlineKeyboardButton("❌ ᴄᴀɴᴄᴇʟ", callback_data="settings_ban")],
            ]),
        )
        await callback.answer()
        return

    if data == "unban_user":
        uid_text = await ask_input(
            client, callback.from_user.id,
            f"🔓 **{small_caps('send the user id to unban')}**",
        )
        if uid_text is None:
            return
        result = await db.unban_user(uid_text, str(callback.from_user.id))
        if result:
            await callback.answer(f"✅ `{uid_text}` {small_caps('has been unbanned')}!", show_alert=True)
            try:
                await client.send_message(
                    int(uid_text),
                    f"✅ **{small_caps('your ban has been lifted')}**\n\n"
                    "ʏᴏᴜ ᴄᴀɴ ɴᴏᴡ ᴜꜱᴇ ᴛʜᴇ ʙᴏᴛ ᴀɢᴀɪɴ.",
                )
            except Exception:
                pass
        else:
            await callback.answer(
                f"❌ `{uid_text}` {small_caps('is not banned')}.", show_alert=True
            )
        return await show_panel(client, callback, "ban_panel")

    if data == "ban_check":
        uid_text = await ask_input(
            client, callback.from_user.id,
            f"🔍 **{small_caps('send the user id to check ban status')}**",
        )
        if uid_text is None:
            return
        ban_info = await db.get_ban_info(uid_text)
        if ban_info:
            ts = ban_info.get("banned_at", "?")
            ts_str = ts.strftime("%Y-%m-%d %H:%M") if hasattr(ts, "strftime") else str(ts)
            reason = ban_info.get("reason", "N/A")
            await callback.answer(
                f"🔒 {small_caps('user is banned')}\n"
                f"ʀᴇᴀꜱᴏɴ: {reason[:50]}\n"
                f"ᴅᴀᴛᴇ: {ts_str}",
                show_alert=True,
            )
        else:
            await callback.answer(
                f"✅ {small_caps('user')} `{uid_text}` {small_caps('is not banned')}.",
                show_alert=True,
            )
        return await show_panel(client, callback, "ban_panel")

    # ── Force sub toggles / setters ────────────────────────────────────────
    if data == "toggle_botmode":
        new_val = not config.get("public_bot", False)
        await Config.update(db.db, {"public_bot": new_val})
        mode = small_caps("public") if new_val else small_caps("private")
        await callback.answer(f"✅ {small_caps('bot set to')} {mode}!", show_alert=True)
        return await show_panel(client, callback, "botmode_panel")

    if data == "toggle_fsub":
        new_val = not config.get("fsub_mode", False)
        await Config.update(db.db, {"fsub_mode": new_val})
        await callback.answer(f"✅ {small_caps('force sub toggled')}!", show_alert=True)
        return await show_panel(client, callback, "fsub_panel")

    if data == "set_fsub_id":
        text = await ask_input(
            client, callback.from_user.id,
            f"📢 **{small_caps('send the channel id')}**\n\n"
            f"📌 {small_caps('format')}: `-100xxxxxxxxxx`\n"
            f"➡️ {small_caps('send')} `0` {small_caps('to unset')}.",
        )
        if text is None:
            return

        value = int(text) if text != "0" and text.lstrip("-").isdigit() else 0

        if value == 0:
            await Config.update(db.db, {"fsub_chat_id": 0, "fsub_inv_link": ""})
            await callback.answer(f"✅ {small_caps('force sub channel unset')}!", show_alert=True)
            return await show_panel(client, callback, "fsub_panel")

        if not str(value).startswith("-100"):
            return await callback.answer(
                f"❌ {small_caps('invalid id')}!\n\n📌 {small_caps('channel id must start with')} `-100`",
                show_alert=True,
            )

        try:
            me     = await client.get_me()
            member = await client.get_chat_member(value, me.id)

            if member.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
                return await callback.answer(
                    f"❌ {small_caps('no admin rights')}!\n\n⚡ {small_caps('i must be admin in that channel')}.",
                    show_alert=True,
                )

            rights = getattr(member, "privileges", None)
            if rights and not rights.can_invite_users:
                return await callback.answer(
                    f"❌ {small_caps('missing permission')}!\n\n"
                    f"👤 {small_caps('please grant')}: 🔑 `{small_caps('add subscribers')}` {small_caps('right')}",
                    show_alert=True,
                )

            try:
                inv = await client.export_chat_invite_link(value)
            except Exception:
                inv = ""

            await Config.update(db.db, {"fsub_chat_id": value, "fsub_inv_link": inv})
            await callback.answer(
                f"✅ {small_caps('force sub channel saved')}!\n\n🆔 {small_caps('id')} + 🔗 {small_caps('invite link added')}.",
                show_alert=True,
            )

        except Exception as exc:
            return await callback.answer(f"❌ {small_caps('error')}:\n`{exc}`", show_alert=True)

        return await show_panel(client, callback, "fsub_panel")

    if data == "set_fsub_link":
        text = await ask_input(
            client, callback.from_user.id,
            f"🔗 **{small_caps('send invite link')}**\n\n{small_caps('send')} `0` {small_caps('to unset')}.",
        )
        if text is not None:
            await Config.update(db.db, {"fsub_inv_link": "" if text == "0" else text})
            await callback.answer(f"✅ {small_caps('force sub invite link updated')}!", show_alert=True)
            return await show_panel(client, callback, "fsub_panel")
        return


# ══════════════════════════════════════════════════════════════════════════════
# BAN REASON SELECTION CALLBACK
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_callback_query(filters.regex(r"^banreason_"), group=2)
async def ban_reason_callback(client: Client, callback: CallbackQuery):
    if not await check_owner(client, callback):
        return

    # Format: banreason_<user_id>_<reason_key>
    raw        = callback.data[len("banreason_"):]
    parts      = raw.rsplit("_", 1)
    target_id  = parts[0]
    reason_key = parts[1] if len(parts) > 1 else "custom"

    if reason_key == "custom":
        reason_text = await ask_input(
            client, callback.from_user.id,
            f"✏️ **{small_caps('enter the custom ban reason / message')}**\n\n"
            f"{small_caps('this message will be shown to the user.')}",
        )
        if reason_text is None:
            await callback.answer(f"❌ {small_caps('cancelled')}.", show_alert=False)
            return await show_panel(client, callback, "ban_panel")
    else:
        reason_text = BAN_REASONS.get(reason_key, reason_key)

    # Fetch user info
    fname, uname = "", ""
    try:
        tg_user = await client.get_users(int(target_id))
        fname   = tg_user.first_name or ""
        uname   = tg_user.username or ""
    except Exception:
        pass

    actor_id = str(callback.from_user.id)
    ok = await db.ban_user(target_id, actor_id, reason_text, uname, fname)

    if ok:
        # Notify the banned user
        try:
            await client.send_message(
                int(target_id),
                f"🚫 **{small_caps('you have been banned from this bot')}**\n\n"
                f"**{small_caps('reason')}:**\n{reason_text}\n\n"
                f"ɪꜰ ʏᴏᴜ ʙᴇʟɪᴇᴠᴇ ᴛʜɪꜱ ɪꜱ ᴀ ᴍɪꜱᴛᴀᴋᴇ, ᴄᴏɴᴛᴀᴄᴛ ᴛʜᴇ ᴀᴅᴍɪɴɪꜱᴛʀᴀᴛᴏʀ.",
            )
        except Exception:
            pass

        await callback.answer(f"✅ {small_caps('user banned')}!", show_alert=True)
    else:
        await callback.answer(f"❌ {small_caps('ban failed')}.", show_alert=True)

    return await show_panel(client, callback, "ban_panel")


# ══════════════════════════════════════════════════════════════════════════════
# /ban  /unban  /checkban  commands
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.command("ban") & filters.private, group=2)
async def ban_command(client: Client, message: Message):
    if not await check_owner(client, message):
        return

    if len(message.command) < 2:
        await client.send_message(
            chat_id=message.chat.id,
            text=(
                f"❌ **{small_caps('usage')}**\n\n"
                f"`/ban <user_id> [reason]`"
            ),
            reply_to_message_id=message.id,
        )
        return

    target_id   = message.command[1]
    reason_text = " ".join(message.command[2:]) if len(message.command) > 2 else BAN_REASONS["abuse"]

    if not target_id.lstrip("-").isdigit():
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('invalid user id')}**",
            reply_to_message_id=message.id,
        )
        return

    if int(target_id) in Config.OWNER_ID:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('cannot ban the bot owner')}**",
            reply_to_message_id=message.id,
        )
        return

    fname, uname = "", ""
    try:
        tg_user = await client.get_users(int(target_id))
        fname   = tg_user.first_name or ""
        uname   = tg_user.username or ""
    except Exception:
        pass

    ok = await db.ban_user(target_id, str(message.from_user.id), reason_text, uname, fname)
    if ok:
        try:
            await client.send_message(
                int(target_id),
                f"🚫 **{small_caps('you have been banned from this bot')}**\n\n"
                f"**{small_caps('reason')}:**\n{reason_text}\n\n"
                "ɪꜰ ʏᴏᴜ ʙᴇʟɪᴇᴠᴇ ᴛʜɪꜱ ɪꜱ ᴀ ᴍɪꜱᴛᴀᴋᴇ, ᴄᴏɴᴛᴀᴄᴛ ᴛʜᴇ ᴀᴅᴍɪɴɪꜱᴛʀᴀᴛᴏʀ.",
            )
        except Exception:
            pass
        await client.send_message(
            chat_id=message.chat.id,
            text=(
                f"🔨 **{small_caps('user banned')}**\n\n"
                f"👤 {_mention(target_id, fname, uname)} `{target_id}`\n"
                f"📝 **{small_caps('reason')}:** {reason_text}"
            ),
            reply_to_message_id=message.id,
        )
    else:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('ban failed or user already banned')}**",
            reply_to_message_id=message.id,
        )


@Client.on_message(filters.command("unban") & filters.private, group=2)
async def unban_command(client: Client, message: Message):
    if not await check_owner(client, message):
        return

    if len(message.command) < 2:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('usage')}**\n\n`/unban <user_id>`",
            reply_to_message_id=message.id,
        )
        return

    target_id = message.command[1]
    ok = await db.unban_user(target_id, str(message.from_user.id))
    if ok:
        try:
            await client.send_message(
                int(target_id),
                f"✅ **{small_caps('your ban has been lifted')}**\n\n"
                "ʏᴏᴜ ᴄᴀɴ ɴᴏᴡ ᴜꜱᴇ ᴛʜᴇ ʙᴏᴛ ᴀɢᴀɪɴ.",
            )
        except Exception:
            pass
        await client.send_message(
            chat_id=message.chat.id,
            text=f"✅ **{small_caps('user')}** `{target_id}` **{small_caps('has been unbanned')}**",
            reply_to_message_id=message.id,
        )
    else:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('user not found in ban list')}**",
            reply_to_message_id=message.id,
        )


@Client.on_message(filters.command("checkban") & filters.private, group=2)
async def checkban_command(client: Client, message: Message):
    if not await check_owner(client, message):
        return

    if len(message.command) < 2:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('usage')}**\n\n`/checkban <user_id>`",
            reply_to_message_id=message.id,
        )
        return

    target_id = message.command[1]
    ban_info  = await db.get_ban_info(target_id)
    if ban_info:
        ts     = ban_info.get("banned_at", "?")
        ts_str = ts.strftime("%Y-%m-%d %H:%M UTC") if hasattr(ts, "strftime") else str(ts)
        reason = ban_info.get("reason", "N/A")
        actor  = ban_info.get("banned_by", "?")
        fname  = ban_info.get("first_name", "")
        uname  = ban_info.get("username", "")
        await client.send_message(
            chat_id=message.chat.id,
            text=(
                f"🔒 **{small_caps('user is banned')}**\n\n"
                f"👤 {_mention(target_id, fname, uname)} `{target_id}`\n"
                f"📝 **{small_caps('reason')}:** {reason}\n"
                f"🕒 **{small_caps('banned on')}:** `{ts_str}`\n"
                f"👮 **{small_caps('banned by')}:** `{actor}`"
            ),
            reply_to_message_id=message.id,
        )
    else:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"✅ **{small_caps('user')}** `{target_id}` **{small_caps('is not banned')}**",
            reply_to_message_id=message.id,
        )


# ══════════════════════════════════════════════════════════════════════════════
# /adminstats
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.command("adminstats") & filters.private, group=2)
async def adminstats_command(client: Client, message: Message):
    if not await check_owner(client, message):
        return

    uptime_str  = format_uptime(time.time() - Config.UPTIME)
    stats       = await db.get_stats()
    cycle       = await db.get_global_bw_cycle()
    bw_used     = cycle.get("used", 0)
    max_bw      = Config.get("max_bandwidth", 107374182400)
    bw_pct      = cycle.get("pct", 0)
    days_r      = cycle.get("days_remaining", 30)
    bw_mode     = f"🟢 {small_caps('active')}" if Config.get("bandwidth_mode", True) else f"🔴 {small_caps('inactive')}"
    ubw_mode    = f"🟢 {small_caps('active')}" if Config.get("user_bw_mode", True) else f"🔴 {small_caps('inactive')}"
    max_ubw     = Config.get("max_user_bandwidth", 10737418240)
    banned_count= len(await db.get_banned_users())
    sudo_count  = len(await db.get_sudo_users())

    text = (
        f"📊 **{small_caps('admin statistics')}**\n\n"
        f"⏱️ **{small_caps('uptime')}:**           `{uptime_str}`\n\n"
        f"👥 **{small_caps('total users')}:**       `{stats['total_users']}`\n"
        f"📂 **{small_caps('total files')}:**       `{stats['total_files']}`\n"
        f"🔑 **{small_caps('sudo users')}:**        `{sudo_count}`\n"
        f"🚫 **{small_caps('banned users')}:**      `{banned_count}`\n\n"
        f"📡 **{small_caps('global bw mode')}:**    {bw_mode}\n"
        f"📶 **{small_caps('global limit')}:**      `{format_size(max_bw)}`\n"
        f"📤 **{small_caps('global used')}:**       `{format_size(bw_used)}` ({bw_pct:.1f}%)\n"
        f"🔄 **{small_caps('resets in')}:**         `{days_r}` ᴅᴀʏꜱ\n\n"
        f"👤 **{small_caps('user bw mode')}:**      {ubw_mode}\n"
        f"👤 **{small_caps('user limit')}:**        `{format_size(max_ubw)}`"
    )

    await client.send_message(
        chat_id=message.chat.id,
        text=text,
        reply_to_message_id=message.id,
    )


# ══════════════════════════════════════════════════════════════════════════════
# /revoke  /revokeall  — unchanged (original logic kept)
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.command("revoke") & filters.private, group=0)
async def revoke_command(client: Client, message: Message):
    if not await check_owner(client, message):
        return

    if len(message.command) < 2:
        await client.send_message(
            chat_id=message.chat.id,
            text=(
                f"❌ **{small_caps('usage')}**\n\n"
                f"`/revoke <file_hash>`"
            ),
            reply_to_message_id=message.id,
        )
        return

    file_hash = message.command[1]
    file_data = await db.get_file_by_hash(file_hash)

    if not file_data:
        await client.send_message(
            chat_id=message.chat.id,
            text=(
                f"❌ **{small_caps('file not found')}**\n\n"
                "ᴛʜᴇ ꜰɪʟᴇ ᴅᴏᴇꜱɴ'ᴛ ᴇxɪꜱᴛ ᴏʀ ʜᴀꜱ ᴀʟʀᴇᴀᴅʏ ʙᴇᴇɴ ᴅᴇʟᴇᴛᴇᴅ."
            ),
            reply_to_message_id=message.id,
        )
        return

    safe_name = escape_markdown(file_data["file_name"])
    await client.send_message(
        chat_id=message.chat.id,
        text=(
            f"⚠️ **{small_caps('confirm revoke')}**\n\n"
            f"🚫 ᴀʀᴇ ʏᴏᴜ ꜱᴜʀᴇ ʏᴏᴜ ᴡᴀɴᴛ ᴛᴏ **ᴘᴇʀᴍᴀɴᴇɴᴛʟʏ ʀᴇᴠᴏᴋᴇ ᴀᴄᴄᴇꜱꜱ** ᴛᴏ ᴛʜɪꜱ ꜰɪʟᴇ?\n\n"
            f"📂 **{small_caps('file')}:** `{safe_name}`\n\n"
            "⚠️ **ᴛʜɪꜱ ᴀᴄᴛɪᴏɴ ᴄᴀɴɴᴏᴛ ʙᴇ ᴜɴᴅᴏɴᴇ.**\n"
            "ᴀʟʟ ꜱᴛʀᴇᴀᴍ ᴀɴᴅ ᴅᴏᴡɴʟᴏᴀᴅ ʟɪɴᴋꜱ ꜰᴏʀ ᴛʜɪꜱ ꜰɪʟᴇ ᴡɪʟʟ ʙᴇᴄᴏᴍᴇ ɪɴᴠᴀʟɪᴅ ɪᴍᴍᴇᴅɪᴀᴛᴇʟʏ."
        ),
        reply_to_message_id=message.id,
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton(f"✅ {small_caps('yes, revoke')}", callback_data=f"revoke_{file_hash}"),
                InlineKeyboardButton(f"❌ {small_caps('cancel')}",      callback_data="revoke_no_1"),
            ]
        ]),
    )


@Client.on_message(filters.command("revokeall") & filters.private, group=2)
async def revokeall_command(client: Client, message: Message):
    if not await check_owner(client, message):
        return

    if len(message.command) > 1:
        raw = message.command[1]
        if not raw.lstrip("-").isdigit():
            await client.send_message(
                chat_id=message.chat.id,
                text=(
                    f"❌ **{small_caps('invalid user id')}**\n\n"
                    f"`/revokeall <user_id>`"
                ),
                reply_to_message_id=message.id,
            )
            return

        target_id = raw
        files     = await db.get_user_files(target_id, limit=0)
        count     = len(files)

        if count == 0:
            await client.send_message(
                chat_id=message.chat.id,
                text=f"📂 **{small_caps('no files found')}** {small_caps('for user')} `{target_id}`.",
                reply_to_message_id=message.id,
            )
            return

        await client.send_message(
            chat_id=message.chat.id,
            text=(
                f"⚠️ **{small_caps('confirm revokeall')}**\n\n"
                f"ᴛʜɪꜱ ᴡɪʟʟ ᴘᴇʀᴍᴀɴᴇɴᴛʟʏ ᴅᴇʟᴇᴛᴇ **{count}** ꜰɪʟᴇꜱ "
                f"ʙᴇʟᴏɴɢɪɴɢ ᴛᴏ ᴜꜱᴇʀ `{target_id}`.\n"
                "ᴀʟʟ ꜱᴛʀᴇᴀᴍ/ᴅᴏᴡɴʟᴏᴀᴅ ʟɪɴᴋꜱ ᴡɪʟʟ ʙᴇᴄᴏᴍᴇ ɪɴᴠᴀʟɪᴅ.\n\n"
                "ᴀʀᴇ ʏᴏᴜ ꜱᴜʀᴇ?"
            ),
            reply_to_message_id=message.id,
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton(
                        f"✅ {small_caps('confirm')}",
                        callback_data=f"revokeuser_confirm_{target_id}",
                    ),
                    InlineKeyboardButton(
                        f"❌ {small_caps('cancel')}",
                        callback_data="revokeall_cancel",
                    ),
                ]
            ]),
        )
        return

    stats       = await db.get_stats()
    total_files = stats["total_files"]

    if total_files == 0:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"📂 **{small_caps('no files to delete')}**.",
            reply_to_message_id=message.id,
        )
        return

    await client.send_message(
        chat_id=message.chat.id,
        text=(
            f"⚠️ **{small_caps('confirm revokeall')}**\n\n"
            f"ᴛʜɪꜱ ᴡɪʟʟ ᴘᴇʀᴍᴀɴᴇɴᴛʟʏ ᴅᴇʟᴇᴛᴇ **{total_files}** ꜰɪʟᴇꜱ ꜰʀᴏᴍ ᴛʜᴇ ᴅᴀᴛᴀʙᴀꜱᴇ.\n"
            "ᴀʟʟ ꜱᴛʀᴇᴀᴍ/ᴅᴏᴡɴʟᴏᴀᴅ ʟɪɴᴋꜱ ᴡɪʟʟ ʙᴇᴄᴏᴍᴇ ɪɴᴠᴀʟɪᴅ.\n\n"
            "ᴀʀᴇ ʏᴏᴜ ꜱᴜʀᴇ?"
        ),
        reply_to_message_id=message.id,
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton(f"✅ {small_caps('confirm')}", callback_data="revokeall_confirm"),
                InlineKeyboardButton(f"❌ {small_caps('cancel')}",  callback_data="revokeall_cancel"),
            ]
        ]),
    )


@Client.on_callback_query(filters.regex(r"^revokeall_(confirm|cancel)$"), group=2)
async def revokeall_callback(client: Client, callback: CallbackQuery):
    if not await check_owner(client, callback):
        return

    if callback.data == "revokeall_cancel":
        await callback.answer(f"❌ {small_caps('cancelled')}.", show_alert=False)
        try:
            await callback.message.edit_text(f"❌ **{small_caps('revokeall cancelled')}.**")
        except Exception:
            pass
        return

    await callback.answer(f"🗑️ {small_caps('deleting all files')}…", show_alert=False)
    try:
        await callback.message.edit_text(f"🗑️ {small_caps('deleting all files')}…")
    except Exception:
        pass

    deleted_count = await db.delete_all_files()
    try:
        await callback.message.edit_text(
            f"🗑️ **{small_caps('all files deleted')}!**\n\n"
            f"{small_caps('deleted')} `{deleted_count}` {small_caps('files successfully')}."
        )
    except Exception:
        pass


@Client.on_callback_query(filters.regex(r"^revokeuser_confirm_"), group=2)
async def revokeuser_confirm_callback(client: Client, callback: CallbackQuery):
    if not await check_owner(client, callback):
        return

    target_id = callback.data.replace("revokeuser_confirm_", "", 1)

    await callback.answer(f"🗑️ {small_caps('deleting')}…", show_alert=False)
    try:
        await callback.message.edit_text(
            f"🗑️ {small_caps('deleting all files for user')} `{target_id}`…"
        )
    except Exception:
        pass

    deleted_count = await db.delete_user_files(target_id)
    try:
        await callback.message.edit_text(
            f"🗑️ **{small_caps('done')}!**\n\n"
            f"{small_caps('deleted')} `{deleted_count}` {small_caps('files for user')} `{target_id}`."
        )
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# /logs
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.command("logs") & filters.private, group=2)
async def logs_command(client: Client, message: Message):
    if not await check_owner(client, message):
        return

    log_file = "bot.log"

    if not os.path.isfile(log_file) or os.path.getsize(log_file) == 0:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('log file not found or empty')}.**",
            reply_to_message_id=message.id,
        )
        return

    try:
        await client.send_document(
            chat_id=message.chat.id,
            document=log_file,
            file_name="bot.log",
            caption=(
                f"📋 **{small_caps('bot logs')}**\n\n"
                f"📁 **{small_caps('file')}:** `bot.log`\n"
                f"📦 **{small_caps('size')}:** `{human_size(os.path.getsize(log_file))}`"
            ),
            reply_to_message_id=message.id,
        )
    except Exception as exc:
        logger.error("logs_command send document error: %s", exc)
        try:
            with open(log_file, "r", encoding="utf-8", errors="replace") as fh:
                tail = fh.read()[-4000:]
            await client.send_message(
                chat_id=message.chat.id,
                text=f"📋 **{small_caps('bot logs')}** *({small_caps('last 4000 chars')})*\n\n```\n{tail}\n```",
                reply_to_message_id=message.id,
            )
        except Exception as exc2:
            logger.error("logs_command fallback error: %s", exc2)
            await client.send_message(
                chat_id=message.chat.id,
                text=f"❌ **{small_caps('error reading logs')}:** `{exc2}`",
                reply_to_message_id=message.id,
            )

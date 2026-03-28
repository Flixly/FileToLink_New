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
from helper import small_caps, format_size, escape_markdown, format_uptime, human_size, check_owner

logger = logging.getLogger(__name__)

BAN_REASONS = {
    "abuse":     "🚫 Service Abuse",
    "content":   "⛔ Restricted / Prohibited Content",
    "custom":    "✏️ Custom Message",
}


async def _user_mention(user_id: str, first_name: str = "", username: str = "") -> str:
    name = first_name or username or user_id
    return f"[{name}](tg://user?id={user_id})"


async def show_panel(client: Client, source, panel_type: str, extra: dict = None):
    config = Config.all()
    msg    = source.message if isinstance(source, CallbackQuery) else source
    extra  = extra or {}

    if panel_type == "main_panel":
        max_bw    = Config.get("max_bandwidth", 107374182400)
        bw_toggle = Config.get("bandwidth_mode", True)
        text = (
            f"✨ **{small_caps('bot settings panel')}** ✨\n\n"
            f"📡 **{small_caps('bandwidth')}**  : {'🟢 ᴀᴄᴛɪᴠᴇ' if bw_toggle else '🔴 ɪɴᴀᴄᴛɪᴠᴇ'} | `{format_size(max_bw)}`\n"
            f"👥 **{small_caps('sudo users')}** : ᴍᴀɴᴀɢᴇ ᴀᴄᴄᴇꜱꜱ\n"
            f"🚫 **{small_caps('ban system')}**  : ᴍᴀɴᴀɢᴇ ʙᴀɴꜱ\n"
            f"🤖 **{small_caps('bot mode')}**  : {'🟢 ᴘᴜʙʟɪᴄ' if config.get('public_bot') else '🔴 ᴘʀɪᴠᴀᴛᴇ'}\n"
            f"📢 **{small_caps('force sub')}** : {'🟢 ᴀᴄᴛɪᴠᴇ' if config.get('fsub_mode') else '🔴 ɪɴᴀᴄᴛɪᴠᴇ'}\n\n"
            "👇 ᴄʜᴏᴏꜱᴇ ᴀ ᴄᴀᴛᴇɢᴏʀʏ ᴛᴏ ᴄᴏɴꜰɪɢᴜʀᴇ."
        )
        buttons = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📡 ʙᴀɴᴅᴡɪᴅᴛʜ",  callback_data="settings_bandwidth"),
                InlineKeyboardButton("👥 ꜱᴜᴅᴏ ᴜꜱᴇʀꜱ", callback_data="settings_sudo"),
            ],
            [
                InlineKeyboardButton("🚫 ʙᴀɴ ꜱʏꜱᴛᴇᴍ",  callback_data="settings_ban"),
                InlineKeyboardButton("🤖 ʙᴏᴛ ᴍᴏᴅᴇ",   callback_data="settings_botmode"),
            ],
            [InlineKeyboardButton("📢 ꜰᴏʀᴄᴇ ꜱᴜʙ",  callback_data="settings_fsub")],
            [InlineKeyboardButton("❌ ᴄʟᴏꜱᴇ", callback_data="settings_close")],
        ])

    elif panel_type == "bandwidth_panel":
        max_bw    = Config.get("max_bandwidth", 107374182400)
        max_user_bw = Config.get("max_user_bandwidth", 0)
        bw_toggle = Config.get("bandwidth_mode", True)
        bw_stats  = await db.get_bandwidth_stats()
        bw_used   = bw_stats["total_bandwidth"]
        days_left = bw_stats.get("days_left", 30)
        bw_pct    = (bw_used / max_bw * 100) if max_bw else 0
        text = (
            f"💠 **{small_caps('bandwidth settings')}** 💠\n\n"
            f"⚡ **{small_caps('mode')}**           : {'🟢 ᴀᴄᴛɪᴠᴇ' if bw_toggle else '🔴 ɪɴᴀᴄᴛɪᴠᴇ'}\n"
            f"📊 **{small_caps('global limit')}**   : `{format_size(max_bw)}`\n"
            f"📤 **{small_caps('used (cycle)')}**   : `{format_size(bw_used)}` ({bw_pct:.1f}%)\n"
            f"🔄 **{small_caps('reset in')}**       : `{days_left}` ᴅᴀʏꜱ\n"
            f"👤 **{small_caps('per-user limit')}** : `{format_size(max_user_bw) if max_user_bw else 'ᴜɴʟɪᴍɪᴛᴇᴅ'}`"
        )
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("⚡ ᴛᴏɢɢʟᴇ",               callback_data="toggle_bandwidth")],
            [
                InlineKeyboardButton("✏️ ɢʟᴏʙᴀʟ ʟɪᴍɪᴛ",      callback_data="set_bandwidth_limit"),
                InlineKeyboardButton("👤 ᴜꜱᴇʀ ʟɪᴍɪᴛ",         callback_data="set_user_bandwidth_limit"),
            ],
            [InlineKeyboardButton("🔄 ʀᴇꜱᴇᴛ ᴜꜱᴀɢᴇ",          callback_data="reset_bandwidth")],
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ",                 callback_data="settings_back")],
        ])

    elif panel_type == "sudo_panel":
        sudo_users = await db.get_sudo_users()
        count      = len(sudo_users)
        recent     = sudo_users[:5]
        lines = []
        for u in recent:
            name = u.get("first_name") or u.get("username") or u["user_id"]
            lines.append(f"  • [{name}](tg://user?id={u['user_id']}) `{u['user_id']}`")
        list_text = "\n".join(lines) if lines else "  ɴᴏɴᴇ"
        text = (
            f"💠 **{small_caps('sudo users')}** 💠\n\n"
            f"👥 **{small_caps('count')}** : `{count}`\n\n"
            f"**{small_caps('recent')}:**\n{list_text}"
        )
        buttons = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("➕ ᴀᴅᴅ",           callback_data="sudo_add"),
                InlineKeyboardButton("➖ ʀᴇᴍᴏᴠᴇ",        callback_data="sudo_remove"),
            ],
            [
                InlineKeyboardButton("📋 ᴜꜱᴇʀꜱ ʟɪꜱᴛ",   callback_data="sudo_list_full"),
                InlineKeyboardButton("📜 ʜɪꜱᴛᴏʀʏ",       callback_data="sudo_history"),
            ],
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ",            callback_data="settings_back")],
        ])

    elif panel_type == "sudo_list_full":
        sudo_users = await db.get_sudo_users()
        lines = []
        for u in sudo_users:
            name = u.get("first_name") or u.get("username") or u["user_id"]
            added = u.get("added_at")
            date_str = added.strftime("%Y-%m-%d") if added else "—"
            lines.append(f"  • [{name}](tg://user?id={u['user_id']}) — `{date_str}`")
        list_text = "\n".join(lines) if lines else "  ɴᴏɴᴇ"
        text = (
            f"💠 **{small_caps('sudo users — full list')}** 💠\n\n"
            f"👥 **{small_caps('total')}**: `{len(sudo_users)}`\n\n"
            f"{list_text}"
        )
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="settings_sudo")],
        ])

    elif panel_type == "sudo_history":
        logs = await db.get_audit_log(action=None, limit=15)
        sudo_logs = [l for l in logs if l.get("action") in ("sudo_add", "sudo_remove")][:10]
        lines = []
        for l in sudo_logs:
            action_icon = "➕" if l["action"] == "sudo_add" else "➖"
            name = l.get("first_name") or l.get("username") or l["target"]
            date_str = l["created_at"].strftime("%m-%d %H:%M") if l.get("created_at") else "—"
            lines.append(f"  {action_icon} [{name}](tg://user?id={l['target']}) by `{l['actor']}` — `{date_str}`")
        list_text = "\n".join(lines) if lines else "  ɴᴏ ʜɪꜱᴛᴏʀʏ"
        text = (
            f"📜 **{small_caps('sudo action history')}** 📜\n\n"
            f"{list_text}"
        )
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="settings_sudo")],
        ])

    elif panel_type == "ban_panel":
        banned_users = await db.get_banned_users()
        count        = len(banned_users)
        recent       = banned_users[:5]
        lines = []
        for u in recent:
            name = u.get("first_name") or u.get("username") or u["user_id"]
            reason_short = u.get("reason", "—")[:30]
            lines.append(f"  • [{name}](tg://user?id={u['user_id']}) — `{reason_short}`")
        list_text = "\n".join(lines) if lines else "  ɴᴏɴᴇ"
        text = (
            f"🚫 **{small_caps('ban system')}** 🚫\n\n"
            f"🔒 **{small_caps('banned')}** : `{count}`\n\n"
            f"**{small_caps('recent')}:**\n{list_text}"
        )
        buttons = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🔨 ʙᴀɴ",          callback_data="ban_user"),
                InlineKeyboardButton("✅ ᴜɴʙᴀɴ",        callback_data="unban_user"),
            ],
            [
                InlineKeyboardButton("📋 ʙᴀɴɴᴇᴅ ʟɪꜱᴛ",  callback_data="ban_list_full"),
                InlineKeyboardButton("📜 ʜɪꜱᴛᴏʀʏ",      callback_data="ban_history"),
            ],
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ",            callback_data="settings_back")],
        ])

    elif panel_type == "ban_list_full":
        banned_users = await db.get_banned_users()
        lines = []
        for u in banned_users:
            name = u.get("first_name") or u.get("username") or u["user_id"]
            date_str = u.get("banned_at", "").strftime("%Y-%m-%d") if u.get("banned_at") else "—"
            reason_short = u.get("reason", "—")[:25]
            lines.append(f"  • [{name}](tg://user?id={u['user_id']}) — `{reason_short}` — `{date_str}`")
        list_text = "\n".join(lines) if lines else "  ɴᴏ ʙᴀɴɴᴇᴅ ᴜꜱᴇʀꜱ"
        text = (
            f"🚫 **{small_caps('banned users — full list')}** 🚫\n\n"
            f"🔒 **{small_caps('total')}**: `{len(banned_users)}`\n\n"
            f"{list_text}"
        )
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="settings_ban")],
        ])

    elif panel_type == "ban_history":
        logs = await db.get_audit_log(action=None, limit=20)
        ban_logs = [l for l in logs if l.get("action") in ("ban", "unban")][:10]
        lines = []
        for l in ban_logs:
            action_icon = "🔨" if l["action"] == "ban" else "✅"
            name = l.get("first_name") or l.get("username") or l["target"]
            date_str = l["created_at"].strftime("%m-%d %H:%M") if l.get("created_at") else "—"
            reason_short = l.get("reason", "")[:20]
            lines.append(f"  {action_icon} [{name}](tg://user?id={l['target']}) by `{l['actor']}` — `{date_str}`")
            if reason_short:
                lines[-1] += f"\n    ↳ _{reason_short}_"
        list_text = "\n".join(lines) if lines else "  ɴᴏ ʜɪꜱᴛᴏʀʏ"
        text = (
            f"📜 **{small_caps('ban action history')}** 📜\n\n"
            f"{list_text}"
        )
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="settings_ban")],
        ])

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

    elif panel_type == "ban_reason_panel":
        target_id = extra.get("target_id", "")
        text = (
            f"🚫 **{small_caps('select ban reason')}**\n\n"
            f"👤 **{small_caps('user')}:** `{target_id}`\n\n"
            "ᴄʜᴏᴏꜱᴇ ᴀ ʀᴇᴀꜱᴏɴ ꜰᴏʀ ᴛʜɪꜱ ʙᴀɴ:"
        )
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("🚫 ꜱᴇʀᴠɪᴄᴇ ᴀʙᴜꜱᴇ",       callback_data=f"ban_do_{target_id}_abuse")],
            [InlineKeyboardButton("⛔ ᴘʀᴏʜɪʙɪᴛᴇᴅ ᴄᴏɴᴛᴇɴᴛ",  callback_data=f"ban_do_{target_id}_content")],
            [InlineKeyboardButton("✏️ ᴄᴜꜱᴛᴏᴍ ᴍᴇꜱꜱᴀɢᴇ",      callback_data=f"ban_do_{target_id}_custom")],
            [InlineKeyboardButton("❌ ᴄᴀɴᴄᴇʟ",               callback_data="settings_ban")],
        ])

    else:
        return

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


_pending: dict[int, asyncio.Future] = {}


@Client.on_message(filters.text & filters.private, group=99)
async def _catch_pending(client: Client, message: Message):
    uid = message.from_user.id
    if uid in _pending and not _pending[uid].done():
        _pending[uid].set_result(message)
        raise StopPropagation


async def ask_input(client: Client, user_id: int, prompt: str, timeout: int = 60) -> str | None:
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


@Client.on_message(filters.command("bot_settings") & filters.private, group=2)
async def open_settings(client: Client, message: Message):
    if not await check_owner(client, message):
        return
    await show_panel(client, message, "main_panel")


@Client.on_callback_query(
    filters.regex(r"^(settings_|toggle_|set_|sudo_|reset_|ban_|unban_).+"),
    group=2,
)
async def settings_callback(client: Client, callback: CallbackQuery):
    data   = callback.data
    config = Config.all()

    if not await check_owner(client, callback):
        return

    panel_nav = {
        "settings_bandwidth": ("bandwidth_panel", f"📡 {small_caps('bandwidth settings')}"),
        "settings_sudo":      ("sudo_panel",      f"👥 {small_caps('sudo users')}"),
        "settings_ban":       ("ban_panel",        f"🚫 {small_caps('ban system')}"),
        "settings_botmode":   ("botmode_panel",   f"🤖 {small_caps('bot mode settings')}"),
        "settings_fsub":      ("fsub_panel",      f"📌 {small_caps('force sub settings')}"),
        "settings_back":      ("main_panel",      f"⬅️ {small_caps('back to main menu')}"),
        "sudo_list_full":     ("sudo_list_full",  f"📋 {small_caps('full sudo list')}"),
        "sudo_history":       ("sudo_history",    f"📜 {small_caps('sudo history')}"),
        "ban_list_full":      ("ban_list_full",   f"📋 {small_caps('full ban list')}"),
        "ban_history":        ("ban_history",     f"📜 {small_caps('ban history')}"),
    }
    if data in panel_nav:
        panel, toast = panel_nav[data]
        await callback.answer(toast, show_alert=False)
        return await show_panel(client, callback, panel)

    if data == "settings_close":
        try:
            await callback.answer(f"❌ {small_caps('closing')}", show_alert=True)
            await callback.message.delete()
        except Exception:
            pass
        return

    if data == "toggle_bandwidth":
        new_val = not config.get("bandwidth_mode", True)
        await Config.update(db.db, {"bandwidth_mode": new_val})
        await callback.answer(f"✅ {small_caps('bandwidth mode toggled')}!", show_alert=True)
        return await show_panel(client, callback, "bandwidth_panel")

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

    if data == "set_bandwidth_limit":
        text = await ask_input(
            client, callback.from_user.id,
            f"📡 **{small_caps('send global bandwidth limit in bytes')}**\n\n"
            f"{small_caps('examples')}:\n"
            "`107374182400` — 100 GB\n"
            "`53687091200`  — 50 GB\n"
            "`10737418240`  — 10 GB\n\n"
            f"{small_caps('send')} `0` {small_caps('to reset to 100 gb')}.",
        )
        if text is None:
            return
        if not text.isdigit():
            await callback.answer(f"❌ {small_caps('invalid number')}!", show_alert=True)
            return
        new_limit = int(text) or 107374182400
        await Config.update(db.db, {"max_bandwidth": new_limit})
        await callback.answer(f"✅ {small_caps('global limit set to')} {format_size(new_limit)}!", show_alert=True)
        return await show_panel(client, callback, "bandwidth_panel")

    if data == "set_user_bandwidth_limit":
        text = await ask_input(
            client, callback.from_user.id,
            f"👤 **{small_caps('send per-user bandwidth limit in bytes')}**\n\n"
            f"{small_caps('examples')}:\n"
            "`10737418240` — 10 GB\n"
            "`5368709120`  — 5 GB\n"
            "`1073741824`  — 1 GB\n\n"
            f"{small_caps('send')} `0` {small_caps('to disable per-user limit')}.",
        )
        if text is None:
            return
        if not text.isdigit():
            await callback.answer(f"❌ {small_caps('invalid number')}!", show_alert=True)
            return
        new_limit = int(text)
        await Config.update(db.db, {"max_user_bandwidth": new_limit})
        label = format_size(new_limit) if new_limit else "ᴜɴʟɪᴍɪᴛᴇᴅ"
        await callback.answer(f"✅ {small_caps('per-user limit set to')} {label}!", show_alert=True)
        return await show_panel(client, callback, "bandwidth_panel")

    if data == "reset_bandwidth":
        await callback.answer(f"🔄 {small_caps('resetting bandwidth usage')}…", show_alert=False)
        ok = await db.reset_bandwidth()
        if ok:
            await callback.answer(f"✅ {small_caps('bandwidth usage reset')}!", show_alert=True)
        else:
            await callback.answer(f"❌ {small_caps('failed to reset bandwidth')}.", show_alert=True)
        return await show_panel(client, callback, "bandwidth_panel")

    if data == "sudo_add":
        text = await ask_input(
            client, callback.from_user.id,
            f"👥 **{small_caps('send user id to add as sudo')}**",
        )
        if text is None:
            return
        if not text.lstrip("-").isdigit():
            await callback.answer(f"❌ {small_caps('invalid user id')}!", show_alert=True)
            return
        username = ""
        first_name = ""
        try:
            target_user = await client.get_users(int(text))
            username = target_user.username or ""
            first_name = target_user.first_name or ""
        except Exception:
            pass
        await db.add_sudo_user(text, str(callback.from_user.id), username=username, first_name=first_name)
        await callback.answer(f"✅ `{text}` {small_caps('added as sudo')}!", show_alert=True)
        return await show_panel(client, callback, "sudo_panel")

    if data == "sudo_remove":
        text = await ask_input(
            client, callback.from_user.id,
            f"👥 **{small_caps('send user id to remove from sudo')}**",
        )
        if text is None:
            return
        result = await db.remove_sudo_user(text, removed_by=str(callback.from_user.id))
        if result:
            await callback.answer(f"✅ `{text}` {small_caps('removed from sudo')}!", show_alert=True)
        else:
            await callback.answer(f"❌ `{text}` {small_caps('not found in sudo list')}.", show_alert=True)
        return await show_panel(client, callback, "sudo_panel")

    if data == "ban_user":
        text = await ask_input(
            client, callback.from_user.id,
            f"🚫 **{small_caps('send user id to ban')}**",
        )
        if text is None:
            return
        if not text.lstrip("-").isdigit():
            await callback.answer(f"❌ {small_caps('invalid user id')}!", show_alert=True)
            return
        await callback.answer(f"🚫 {small_caps('choose ban reason')}…", show_alert=False)
        return await show_panel(client, callback, "ban_reason_panel", extra={"target_id": text})

    if data == "unban_user":
        text = await ask_input(
            client, callback.from_user.id,
            f"✅ **{small_caps('send user id to unban')}**",
        )
        if text is None:
            return
        if not text.lstrip("-").isdigit():
            await callback.answer(f"❌ {small_caps('invalid user id')}!", show_alert=True)
            return
        result = await db.unban_user(text, unbanned_by=str(callback.from_user.id))
        if result:
            await callback.answer(f"✅ `{text}` {small_caps('unbanned')}!", show_alert=True)
            try:
                await client.send_message(
                    chat_id=int(text),
                    text=(
                        f"✅ **{small_caps('ban lifted')}**\n\n"
                        "ʏᴏᴜʀ ʙᴀɴ ʜᴀꜱ ʙᴇᴇɴ ʀᴇᴍᴏᴠᴇᴅ. ʏᴏᴜ ᴄᴀɴ ᴜꜱᴇ ᴛʜᴇ ʙᴏᴛ ᴀɢᴀɪɴ."
                    ),
                )
            except Exception:
                pass
        else:
            await callback.answer(f"❌ `{text}` {small_caps('not found in ban list')}.", show_alert=True)
        return await show_panel(client, callback, "ban_panel")

    if data.startswith("ban_do_"):
        parts      = data[len("ban_do_"):].rsplit("_", 1)
        target_id  = parts[0]
        reason_key = parts[1] if len(parts) > 1 else "abuse"

        if reason_key == "custom":
            reason_text = await ask_input(
                client, callback.from_user.id,
                f"✏️ **{small_caps('send custom ban reason')}**",
            )
            if reason_text is None:
                return
        else:
            reason_text = BAN_REASONS.get(reason_key, "Service Abuse")

        username   = ""
        first_name = ""
        try:
            target_user = await client.get_users(int(target_id))
            username    = target_user.username or ""
            first_name  = target_user.first_name or ""
        except Exception:
            pass

        await db.ban_user(
            target_id,
            banned_by=str(callback.from_user.id),
            reason=reason_text,
            username=username,
            first_name=first_name,
        )
        await callback.answer(f"🚫 `{target_id}` {small_caps('has been banned')}!", show_alert=True)

        try:
            await client.send_message(
                chat_id=int(target_id),
                text=(
                    f"🚫 **{small_caps('you have been banned')}**\n\n"
                    f"**{small_caps('reason')}:** {reason_text}\n\n"
                    "ᴄᴏɴᴛᴀᴄᴛ ꜱᴜᴘᴘᴏʀᴛ ɪꜰ ʏᴏᴜ ʙᴇʟɪᴇᴠᴇ ᴛʜɪꜱ ɪꜱ ᴀɴ ᴇʀʀᴏʀ."
                ),
            )
        except Exception:
            pass

        return await show_panel(client, callback, "ban_panel")

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
                f"✅ {small_caps('force sub channel saved')}!",
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


@Client.on_message(filters.command("ban") & filters.private, group=2)
async def ban_command(client: Client, message: Message):
    if not await check_owner(client, message):
        return

    if len(message.command) < 2:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('usage')}**\n\n`/ban <user_id> [reason]`",
            reply_to_message_id=message.id,
        )
        return

    target_id  = message.command[1]
    reason_text = " ".join(message.command[2:]) if len(message.command) > 2 else BAN_REASONS["abuse"]

    if not target_id.lstrip("-").isdigit():
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('invalid user id')}**",
            reply_to_message_id=message.id,
        )
        return

    username   = ""
    first_name = ""
    try:
        target_user = await client.get_users(int(target_id))
        username    = target_user.username or ""
        first_name  = target_user.first_name or ""
    except Exception:
        pass

    await db.ban_user(
        target_id,
        banned_by=str(message.from_user.id),
        reason=reason_text,
        username=username,
        first_name=first_name,
    )

    try:
        await client.send_message(
            chat_id=int(target_id),
            text=(
                f"🚫 **{small_caps('you have been banned')}**\n\n"
                f"**{small_caps('reason')}:** {reason_text}\n\n"
                "ᴄᴏɴᴛᴀᴄᴛ ꜱᴜᴘᴘᴏʀᴛ ɪꜰ ʏᴏᴜ ʙᴇʟɪᴇᴠᴇ ᴛʜɪꜱ ɪꜱ ᴀɴ ᴇʀʀᴏʀ."
            ),
        )
    except Exception:
        pass

    await client.send_message(
        chat_id=message.chat.id,
        text=(
            f"🚫 **{small_caps('user banned')}**\n\n"
            f"👤 **{small_caps('id')}:** `{target_id}`\n"
            f"📋 **{small_caps('reason')}:** {reason_text}"
        ),
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
    if not target_id.lstrip("-").isdigit():
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('invalid user id')}**",
            reply_to_message_id=message.id,
        )
        return

    result = await db.unban_user(target_id, unbanned_by=str(message.from_user.id))
    if result:
        try:
            await client.send_message(
                chat_id=int(target_id),
                text=(
                    f"✅ **{small_caps('ban lifted')}**\n\n"
                    "ʏᴏᴜʀ ʙᴀɴ ʜᴀꜱ ʙᴇᴇɴ ʀᴇᴍᴏᴠᴇᴅ. ʏᴏᴜ ᴄᴀɴ ᴜꜱᴇ ᴛʜᴇ ʙᴏᴛ ᴀɢᴀɪɴ."
                ),
            )
        except Exception:
            pass
        await client.send_message(
            chat_id=message.chat.id,
            text=f"✅ **{small_caps('user unbanned')}:** `{target_id}`",
            reply_to_message_id=message.id,
        )
    else:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('user not found in ban list')}:** `{target_id}`",
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

    if not ban_info:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"✅ **{small_caps('user is not banned')}:** `{target_id}`",
            reply_to_message_id=message.id,
        )
        return

    banned_at = ban_info.get("banned_at", "")
    date_str  = banned_at.strftime("%Y-%m-%d %H:%M") if banned_at else "—"

    await client.send_message(
        chat_id=message.chat.id,
        text=(
            f"🚫 **{small_caps('user is banned')}**\n\n"
            f"👤 **{small_caps('id')}:** `{target_id}`\n"
            f"📋 **{small_caps('reason')}:** {ban_info.get('reason', '—')}\n"
            f"👮 **{small_caps('banned by')}:** `{ban_info.get('banned_by', '—')}`\n"
            f"📅 **{small_caps('date')}:** `{date_str}`"
        ),
        reply_to_message_id=message.id,
    )


@Client.on_message(filters.command("adminstats") & filters.private, group=2)
async def adminstats_command(client: Client, message: Message):
    if not await check_owner(client, message):
        return

    uptime_str = format_uptime(time.time() - Config.UPTIME)
    stats      = await db.get_stats()
    bw_stats   = await db.get_bandwidth_stats()

    max_bw    = Config.get("max_bandwidth", 107374182400)
    bw_used   = bw_stats["total_bandwidth"]
    days_left = bw_stats.get("days_left", 30)
    bw_pct    = (bw_used / max_bw * 100) if max_bw else 0
    bw_mode   = f"🟢 {small_caps('active')}" if Config.get("bandwidth_mode", True) else f"🔴 {small_caps('inactive')}"

    banned_count = len(await db.get_banned_users())
    sudo_count   = len(await db.get_sudo_users())

    text = (
        f"📊 **{small_caps('admin statistics')}**\n\n"
        f"⏱️ **{small_caps('uptime')}:**         `{uptime_str}`\n\n"
        f"👥 **{small_caps('total users')}:**     `{stats['total_users']}`\n"
        f"📂 **{small_caps('total files')}:**     `{stats['total_files']}`\n"
        f"🛡️ **{small_caps('sudo users')}:**      `{sudo_count}`\n"
        f"🚫 **{small_caps('banned users')}:**    `{banned_count}`\n\n"
        f"📡 **{small_caps('bandwidth mode')}:**  {bw_mode}\n"
        f"📶 **{small_caps('bw limit')}:**        `{format_size(max_bw)}`\n"
        f"📤 **{small_caps('bw used')}:**         `{format_size(bw_used)}` ({bw_pct:.1f}%)\n"
        f"🔄 **{small_caps('reset in')}:**        `{days_left}` ᴅᴀʏꜱ"
    )

    await client.send_message(
        chat_id=message.chat.id,
        text=text,
        reply_to_message_id=message.id,
    )


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
            "⚠️ **ᴛʜɪꜱ ᴀᴄᴛɪᴏɴ ᴄᴀɴɴᴏᴛ ʙᴇ ᴜɴᴅᴏɴᴇ.**"
        ),
        reply_to_message_id=message.id,
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton(f"✅ {small_caps('yes, revoke')}", callback_data=f"revoke_{file_hash}"),
                InlineKeyboardButton(f"❌ {small_caps('cancel')}",  callback_data="revoke_no_1"),
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
            await client.send_message(
                chat_id=message.chat.id,
                text=f"❌ **{small_caps('error reading logs')}:** `{exc2}`",
                reply_to_message_id=message.id,
            )

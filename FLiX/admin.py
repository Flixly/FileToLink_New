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


async def show_panel(client: Client, source, panel_type: str):
    config = Config.all()
    msg    = source.message if isinstance(source, CallbackQuery) else source

    if panel_type == "main_panel":
        max_bw    = Config.get("max_bandwidth", 107374182400)
        bw_toggle = Config.get("bandwidth_mode", True)
        text = (
            f"✨ **{small_caps('bot settings panel')}** ✨\n\n"
            f"📡 **{small_caps('bandwidth')}**  : {'🟢 ᴀᴄᴛɪᴠᴇ' if bw_toggle else '🔴 ɪɴᴀᴄᴛɪᴠᴇ'} | `{format_size(max_bw)}`\n"
            f"👥 **{small_caps('sudo users')}** : ᴍᴀɴᴀɢᴇ ᴀᴄᴄᴇꜱꜱ\n"
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
                InlineKeyboardButton("🤖 ʙᴏᴛ ᴍᴏᴅᴇ",   callback_data="settings_botmode"),
                InlineKeyboardButton("📢 ꜰᴏʀᴄᴇ ꜱᴜʙ",  callback_data="settings_fsub"),
            ],
            [InlineKeyboardButton("❌ ᴄʟᴏꜱᴇ", callback_data="settings_close")],
        ])

    elif panel_type == "bandwidth_panel":
        max_bw    = Config.get("max_bandwidth", 107374182400)
        bw_toggle = Config.get("bandwidth_mode", True)
        bw_stats  = await db.get_bandwidth_stats()
        bw_used   = bw_stats["total_bandwidth"]
        bw_today  = bw_stats["today_bandwidth"]
        bw_pct    = (bw_used / max_bw * 100) if max_bw else 0
        text = (
            f"💠 **{small_caps('bandwidth settings')}** 💠\n\n"
            f"⚡ **{small_caps('mode')}**       : {'🟢 ᴀᴄᴛɪᴠᴇ' if bw_toggle else '🔴 ɪɴᴀᴄᴛɪᴠᴇ'}\n"
            f"📊 **{small_caps('limit')}**      : `{format_size(max_bw)}`\n"
            f"📤 **{small_caps('used (total)')}**: `{format_size(bw_used)}` ({bw_pct:.1f}%)\n"
            f"📅 **{small_caps('used today')}** : `{format_size(bw_today)}`"
        )
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("⚡ ᴛᴏɢɢʟᴇ",         callback_data="toggle_bandwidth")],
            [
                InlineKeyboardButton("✏️ ꜱᴇᴛ ʟɪᴍɪᴛ",     callback_data="set_bandwidth_limit"),
                InlineKeyboardButton("🔄 ʀᴇꜱᴇᴛ ᴜꜱᴀɢᴇ",   callback_data="reset_bandwidth"),
            ],
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ",           callback_data="settings_back")],
        ])

    elif panel_type == "sudo_panel":
        sudo_users = await db.get_sudo_users()
        count = len(sudo_users)
        lines = "\n".join(f"  • `{u['user_id']}`" for u in sudo_users) if sudo_users else "  ɴᴏɴᴇ"
        text = (
            f"💠 **{small_caps('sudo users')}** 💠\n\n"
            f"👥 **{small_caps('count')}** : `{count}`\n\n"
            f"**{small_caps('list')}:**\n{lines}"
        )
        buttons = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("➕ ᴀᴅᴅ",    callback_data="sudo_add"),
                InlineKeyboardButton("➖ ʀᴇᴍᴏᴠᴇ", callback_data="sudo_remove"),
            ],
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="settings_back")],
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

    else:
        return

    if isinstance(source, CallbackQuery):
        try:
            await source.message.edit_text(
                text,
                reply_markup=buttons,
            )
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
        logger.debug("ask_input timed out for user %s", user_id)
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
    filters.regex(r"^(settings_|toggle_|set_|sudo_|reset_).+"),
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
        "settings_botmode":   ("botmode_panel",   f"🤖 {small_caps('bot mode settings')}"),
        "settings_fsub":      ("fsub_panel",      f"📌 {small_caps('force sub settings')}"),
        "settings_back":      ("main_panel",      f"⬅️ {small_caps('back to main menu')}"),
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
            f"📡 **{small_caps('send bandwidth limit in bytes')}**\n\n"
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
        await callback.answer(f"✅ {small_caps('limit set to')} {format_size(new_limit)}!", show_alert=True)
        return await show_panel(client, callback, "bandwidth_panel")

    if data == "reset_bandwidth":
        await callback.answer(f"🔄 {small_caps('resetting bandwidth usage')}…", show_alert=False)
        ok = await db.reset_bandwidth()
        if ok:
            await callback.answer(f"✅ {small_caps('bandwidth usage reset to zero')}!", show_alert=True)
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
        await db.add_sudo_user(text, str(callback.from_user.id))
        await callback.answer(f"✅ `{text}` {small_caps('added as sudo')}!", show_alert=True)
        return await show_panel(client, callback, "sudo_panel")

    if data == "sudo_remove":
        text = await ask_input(
            client, callback.from_user.id,
            f"👥 **{small_caps('send user id to remove from sudo')}**",
        )
        if text is None:
            return
        result = await db.remove_sudo_user(text)
        if result:
            await callback.answer(f"✅ `{text}` {small_caps('removed from sudo')}!", show_alert=True)
        else:
            await callback.answer(f"❌ `{text}` {small_caps('not found in sudo list')}.", show_alert=True)
        return await show_panel(client, callback, "sudo_panel")

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


@Client.on_message(filters.command("adminstats") & filters.private, group=2)
async def adminstats_command(client: Client, message: Message):
    if not await check_owner(client, message):
        return

    uptime_str = format_uptime(time.time() - Config.UPTIME)
    stats      = await db.get_stats()
    bw_stats   = await db.get_bandwidth_stats()

    max_bw  = Config.get("max_bandwidth", 107374182400)
    bw_used = bw_stats["total_bandwidth"]
    bw_pct  = (bw_used / max_bw * 100) if max_bw else 0
    bw_mode = f"🟢 {small_caps('active')}" if Config.get("bandwidth_mode", True) else f"🔴 {small_caps('inactive')}"

    text = (
        f"📊 **{small_caps('admin statistics')}**\n\n"
        f"⏱️ **{small_caps('uptime')}:**         `{uptime_str}`\n\n"
        f"👥 **{small_caps('total users')}:**     `{stats['total_users']}`\n"
        f"📂 **{small_caps('total files')}:**     `{stats['total_files']}`\n\n"
        f"📡 **{small_caps('bandwidth mode')}:**  {bw_mode}\n"
        f"📶 **{small_caps('bw limit')}:**        `{format_size(max_bw)}`\n"
        f"📤 **{small_caps('bw used total')}:**   `{format_size(bw_used)}` ({bw_pct:.1f}%)\n"
        f"📅 **{small_caps('bw used today')}:**   `{format_size(bw_stats['today_bandwidth'])}`"
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
    # Routes to gen.py's cb_revoke_confirm handler via the shared "revoke_<hash>" pattern.
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
            await callback.message.edit_text(
                f"❌ **{small_caps('revokeall cancelled')}.**"
            )
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
            logger.error("logs_command fallback error: %s", exc2)
            await client.send_message(
                chat_id=message.chat.id,
                text=f"❌ **{small_caps('error reading logs')}:** `{exc2}`",
                reply_to_message_id=message.id,
            )


# ═══════════════════════════════════════════════════════════════
#   USER LIMIT / BLOCK MANAGEMENT COMMANDS
# ═══════════════════════════════════════════════════════════════

@Client.on_message(filters.command("block_user") & filters.private, group=2)
async def block_user_command(client: Client, message: Message):
    """Block a user's access to streaming and downloading."""
    if not await check_owner(client, message):
        return

    if len(message.command) < 2:
        await client.send_message(
            chat_id=message.chat.id,
            text=(
                f"❌ **{small_caps('usage')}**\n\n"
                f"`/block_user <user_id> [reason]`\n\n"
                f"{small_caps('example')}: `/block_user 123456789 spam`"
            ),
            reply_to_message_id=message.id,
        )
        return

    target_id = message.command[1]
    reason    = " ".join(message.command[2:]) if len(message.command) > 2 else "admin_decision"

    if not target_id.lstrip("-").isdigit():
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('invalid user id')}**",
            reply_to_message_id=message.id,
        )
        return

    ok = await db.set_user_blocked(target_id, True, reason)
    if ok:
        text = (
            f"🚫 **{small_caps('user blocked')}**\n\n"
            f"👤 **{small_caps('user id')}:** `{target_id}`\n"
            f"📝 **{small_caps('reason')}:** `{reason}`\n\n"
            f"⚡ {small_caps('streaming and downloading blocked immediately.')}"
        )
        # Notify the user
        try:
            await client.send_message(
                chat_id=int(target_id),
                text=(
                    f"🚫 **{small_caps('access restricted')}**\n\n"
                    "ʏᴏᴜʀ ᴀᴄᴄᴇꜱꜱ ʜᴀꜱ ʙᴇᴇɴ **ʀᴇꜱᴛʀɪᴄᴛᴇᴅ** ʙʏ ᴀɴ ᴀᴅᴍɪɴɪꜱᴛʀᴀᴛᴏʀ.\n"
                    "📩 ᴄᴏɴᴛᴀᴄᴛ ꜱᴜᴘᴘᴏʀᴛ ɪꜰ ʏᴏᴜ ʙᴇʟɪᴇᴠᴇ ᴛʜɪꜱ ɪꜱ ᴀ ᴍɪꜱᴛᴀᴋᴇ."
                ),
            )
        except Exception:
            pass  # user may have blocked the bot
    else:
        text = f"❌ **{small_caps('failed to block user')}** `{target_id}`"

    await client.send_message(
        chat_id=message.chat.id,
        text=text,
        reply_to_message_id=message.id,
    )


@Client.on_message(filters.command("unblock_user") & filters.private, group=2)
async def unblock_user_command(client: Client, message: Message):
    """Unblock a user and restore access."""
    if not await check_owner(client, message):
        return

    if len(message.command) < 2:
        await client.send_message(
            chat_id=message.chat.id,
            text=(
                f"❌ **{small_caps('usage')}**\n\n"
                f"`/unblock_user <user_id>`"
            ),
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

    ok = await db.set_user_blocked(target_id, False)
    # Also reset bandwidth block state
    await db.set_user_warn_sent(target_id, False)
    if ok:
        text = (
            f"✅ **{small_caps('user unblocked')}**\n\n"
            f"👤 **{small_caps('user id')}:** `{target_id}`\n"
            f"⚡ {small_caps('access restored immediately.')}"
        )
        try:
            await client.send_message(
                chat_id=int(target_id),
                text=(
                    f"✅ **{small_caps('access restored')}**\n\n"
                    "ʏᴏᴜʀ ᴀᴄᴄᴇꜱꜱ ʜᴀꜱ ʙᴇᴇɴ **ʀᴇꜱᴛᴏʀᴇᴅ** ʙʏ ᴀɴ ᴀᴅᴍɪɴɪꜱᴛʀᴀᴛᴏʀ.\n"
                    "ʏᴏᴜ ᴄᴀɴ ɴᴏᴡ ꜱᴛʀᴇᴀᴍ ᴀɴᴅ ᴅᴏᴡɴʟᴏᴀᴅ ᴀɢᴀɪɴ. 🎉"
                ),
            )
        except Exception:
            pass
    else:
        text = f"❌ **{small_caps('failed to unblock user')}** `{target_id}`"

    await client.send_message(
        chat_id=message.chat.id,
        text=text,
        reply_to_message_id=message.id,
    )


@Client.on_message(filters.command("set_user_limit") & filters.private, group=2)
async def set_user_limit_command(client: Client, message: Message):
    """Set per-user bandwidth/file limit. Usage: /set_user_limit <user_id> bw=<bytes> files=<count>"""
    if not await check_owner(client, message):
        return

    if len(message.command) < 2:
        await client.send_message(
            chat_id=message.chat.id,
            text=(
                f"❌ **{small_caps('usage')}**\n\n"
                f"`/set_user_limit <user_id> [bw=<bytes>] [files=<count>]`\n\n"
                f"**{small_caps('examples')}:**\n"
                f"`/set_user_limit 123 bw=10737418240`  — 10 GB bandwidth\n"
                f"`/set_user_limit 123 files=100`        — 100 files max\n"
                f"`/set_user_limit 123 bw=0 files=0`     — reset to unlimited"
            ),
            reply_to_message_id=message.id,
        )
        return

    target_id   = message.command[1]
    max_bw      = 0
    max_files   = 0

    for arg in message.command[2:]:
        if arg.startswith("bw="):
            try: max_bw = int(arg[3:])
            except: pass
        elif arg.startswith("files="):
            try: max_files = int(arg[6:])
            except: pass

    ok = await db.set_user_limit(target_id, max_bw, max_files)
    if ok:
        bw_str    = format_size(max_bw)    if max_bw    else "Unlimited"
        files_str = str(max_files)         if max_files else "Unlimited"
        text = (
            f"✅ **{small_caps('user limit set')}**\n\n"
            f"👤 **{small_caps('user id')}:**     `{target_id}`\n"
            f"📡 **{small_caps('bw limit')}:**    `{bw_str}`\n"
            f"📂 **{small_caps('file limit')}:**  `{files_str}`"
        )
    else:
        text = f"❌ **{small_caps('failed to set limit')}** for `{target_id}`"

    await client.send_message(
        chat_id=message.chat.id,
        text=text,
        reply_to_message_id=message.id,
    )


@Client.on_message(filters.command("user_info") & filters.private, group=2)
async def user_info_command(client: Client, message: Message):
    """Show a user's bandwidth and limit status."""
    if not await check_owner(client, message):
        return

    if len(message.command) < 2:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('usage')}**: `/user_info <user_id>`",
            reply_to_message_id=message.id,
        )
        return

    target_id = message.command[1]
    user      = await db.get_user(target_id)
    lim       = await db.check_user_limit(target_id)
    ubw       = await db.get_user_bandwidth(target_id)

    if not user:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('user not found')}**: `{target_id}`",
            reply_to_message_id=message.id,
        )
        return

    blocked    = user.get("is_blocked", False)
    bw_limit   = lim.get("bw_limit",   0)
    files_limit = lim.get("files_limit", 0)
    bw_used    = ubw["total_bytes"]
    files_used = lim.get("files_used", 0)
    bw_pct     = (bw_used / bw_limit * 100) if bw_limit else 0

    status_icon = "🚫" if blocked else "✅"
    text = (
        f"👤 **{small_caps('user info')}**\n\n"
        f"🆔 **{small_caps('user id')}:**       `{target_id}`\n"
        f"📛 **{small_caps('name')}:**          `{user.get('first_name', '')} {user.get('last_name', '')}`\n"
        f"🔗 **{small_caps('username')}:**      `@{user.get('username', 'N/A')}`\n\n"
        f"{status_icon} **{small_caps('status')}:**       {'🚫 ʙʟᴏᴄᴋᴇᴅ' if blocked else '✅ ᴀᴄᴛɪᴠᴇ'}\n"
        f"📡 **{small_caps('bw used')}:**      `{format_size(bw_used)}`\n"
        f"🔒 **{small_caps('bw limit')}:**     `{format_size(bw_limit) if bw_limit else 'Unlimited'}`\n"
        f"📊 **{small_caps('bw usage')}:**     `{bw_pct:.1f}%`\n"
        f"📂 **{small_caps('files')}:**        `{files_used}` / `{files_limit if files_limit else 'Unlimited'}`\n"
    )

    buttons = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                f"{'✅ Unblock' if blocked else '🚫 Block'}",
                callback_data=f"{'unblock' if blocked else 'block'}_{target_id}",
            ),
            InlineKeyboardButton("♻️ Reset BW", callback_data=f"reset_ubw_{target_id}"),
        ],
    ])

    await client.send_message(
        chat_id=message.chat.id,
        text=text,
        reply_to_message_id=message.id,
        reply_markup=buttons,
    )


@Client.on_callback_query(filters.regex(r"^(block|unblock)_(\d+)$"), group=2)
async def block_unblock_callback(client: Client, callback: CallbackQuery):
    if not await check_owner(client, callback):
        return
    parts     = callback.data.split("_", 1)
    action    = parts[0]
    target_id = parts[1]
    blocked   = action == "block"
    reason    = "admin_button" if blocked else ""
    await db.set_user_blocked(target_id, blocked, reason)
    await callback.answer(
        f"{'🚫 Blocked' if blocked else '✅ Unblocked'} user {target_id}",
        show_alert=True,
    )
    # Notify user
    try:
        msg = (
            "🚫 **ʏᴏᴜʀ ᴀᴄᴄᴇꜱꜱ ʜᴀꜱ ʙᴇᴇɴ ʀᴇꜱᴛʀɪᴄᴛᴇᴅ ʙʏ ᴀɴ ᴀᴅᴍɪɴ.**"
            if blocked else
            "✅ **ʏᴏᴜʀ ᴀᴄᴄᴇꜱꜱ ʜᴀꜱ ʙᴇᴇɴ ʀᴇꜱᴛᴏʀᴇᴅ ʙʏ ᴀɴ ᴀᴅᴍɪɴ.**"
        )
        await client.send_message(int(target_id), msg)
    except Exception:
        pass


@Client.on_callback_query(filters.regex(r"^reset_ubw_(\d+)$"), group=2)
async def reset_user_bw_callback(client: Client, callback: CallbackQuery):
    if not await check_owner(client, callback):
        return
    target_id = callback.data.replace("reset_ubw_", "")
    ok = await db.reset_user_bandwidth(target_id)
    # Also unblock if they were bandwidth-blocked
    user = await db.get_user(target_id)
    if user and user.get("blocked_reason", "") in ("bandwidth_exceeded",):
        await db.set_user_blocked(target_id, False)
    await callback.answer(
        f"✅ Bandwidth reset for {target_id}" if ok else f"❌ Failed to reset",
        show_alert=True,
    )

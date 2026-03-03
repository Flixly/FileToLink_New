import logging

from pyrogram import Client, filters
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from config import Config
from database import db
from helper import small_caps, format_size, escape_markdown, check_fsub

logger = logging.getLogger(__name__)


def show_nav(page: str, user=None, bot_username: str = "") -> tuple[str, InlineKeyboardMarkup]:

    mention = getattr(user, "mention", "user") if user else "user"

    if page == "start":
        text = (
            f"👋 **ʜᴇʟʟᴏ {mention}**,\n\n"
            "ɪ ᴀᴍ ᴀ ᴘʀᴇᴍɪᴜᴍ ꜰɪʟᴇ ꜱᴛʀᴇᴀᴍ ʙᴏᴛ.\n\n"
            "📂 **ꜱᴇɴᴅ ᴍᴇ ᴀɴʏ ꜰɪʟᴇ** "
            "(ᴠɪᴅᴇᴏ, ᴀᴜᴅɪᴏ, ᴅᴏᴄᴜᴍᴇɴᴛ) ᴀɴᴅ ɪ ᴡɪʟʟ ɢᴇɴᴇʀᴀᴛᴇ ᴀ ᴅɪʀᴇᴄᴛ "
            "ᴅᴏᴡɴʟᴏᴀᴅ ᴀɴᴅ ꜱᴛʀᴇᴀᴍɪɴɢ ʟɪɴᴋ ꜰᴏʀ ʏᴏᴜ."
        )

        buttons = [[
            InlineKeyboardButton("📚 ʜᴇʟᴘ", callback_data="help"),
            InlineKeyboardButton("ℹ️ ᴀʙᴏᴜᴛ", callback_data="about"),
        ]]

    elif page == "help":
        text = (
            "📚 **ʜᴇʟᴘ & ɢᴜɪᴅᴇ**\n\n"
            "**ʜᴏᴡ ᴛᴏ ᴜꜱᴇ:**\n"
            "1️⃣ ꜱᴇɴᴅ ᴀɴʏ ꜰɪʟᴇ ᴛᴏ ᴛʜᴇ ʙᴏᴛ\n"
            "2️⃣ ɢᴇᴛ ɪɴꜱᴛᴀɴᴛ ꜱᴛʀᴇᴀᴍ & ᴅᴏᴡɴʟᴏᴀᴅ ʟɪɴᴋꜱ\n"
            "3️⃣ ꜱʜᴀʀᴇ ʟɪɴᴋꜱ ᴀɴʏᴡʜᴇʀᴇ!\n\n"
            "**ꜱᴜᴘᴘᴏʀᴛᴇᴅ ꜰɪʟᴇꜱ:**\n"
            "🎬 ᴠɪᴅᴇᴏꜱ\n"
            "🎵 ᴀᴜᴅɪᴏ\n"
            "📄 ᴅᴏᴄᴜᴍᴇɴᴛꜱ\n"
            "🖼️ ɪᴍᴀɢᴇꜱ"
        )

        buttons = [[
            InlineKeyboardButton("🏠 ʜᴏᴍᴇ", callback_data="start")
        ]]

    elif page == "about":
        text = (
            "ℹ️ **ᴀʙᴏᴜᴛ ꜰɪʟᴇꜱᴛʀᴇᴀᴍ ʙᴏᴛ**\n\n"
            f"🤖 **ʙᴏᴛ:** @{bot_username}\n\n"
            "💻 **ᴅᴇᴠᴇʟᴏᴘᴇʀ:** @FLiX_LY\n"
            "⚡ **ᴠᴇʀꜱɪᴏɴ:** 2.1"
        )

        buttons = [[
            InlineKeyboardButton("🏠 ʜᴏᴍᴇ", callback_data="start")
        ]]

    else:
        text = "ɪɴᴠᴀʟɪᴅ ᴘᴀɢᴇ"
        buttons = []

    return text, InlineKeyboardMarkup(buttons)


@Client.on_message(filters.command("start") & filters.private, group=1)
async def start_command(client: Client, message: Message):
    user    = message.from_user
    user_id = user.id
    _me     = await client.get_me()

    is_new = await db.register_user_on_start({
        "user_id":    str(user_id),
        "username":   user.username   or "",
        "first_name": user.first_name or "",
        "last_name":  user.last_name  or "",
    })

    if is_new and Config.LOGS_CHAT_ID:
        try:
            full_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
            await client.send_message(
                chat_id=Config.LOGS_CHAT_ID,
                text=(
                    "#NewUser\n\n"
                    f"👤 **User:** {user.mention}\n"
                    f"🆔 **ID:** `{user_id}`\n"
                    f"👤 **Username:** @{user.username or 'N/A'}\n"
                    f"📛 **Name:** `{full_name}`"
                ),
                disable_web_page_preview=True,
            )
        except Exception as exc:
            logger.error("failed to log new user: %s", exc)

    if len(message.command) > 1:
        arg       = message.command[1]
        # Support both plain hash and the "file_<hash>" share format
        file_hash = arg[5:] if arg.startswith("file_") else arg

        if Config.get("fsub_mode", False):
            if not await check_fsub(client, message):
                return

        try:
            file_data = await db.get_file_by_hash(file_hash)
            if not file_data:
                await client.send_message(
                    chat_id=message.chat.id,
                    text=(
                        f"❌ **{small_caps('file not found')}**\n\n"
                        "ᴛʜᴇ ꜰɪʟᴇ ʟɪɴᴋ ɪꜱ ɪɴᴠᴀʟɪᴅ ᴏʀ ʜᴀꜱ ʙᴇᴇɴ ᴅᴇʟᴇᴛᴇᴅ."
                    ),
                    reply_to_message_id=message.id,
                    disable_web_page_preview=True,
                )
                return

            base_url      = Config.URL or f"http://localhost:{Config.PORT}"
            stream_link   = f"{base_url}/stream/{file_hash}"
            download_link = f"{base_url}/dl/{file_hash}"

            file_type     = file_data.get("file_type", "document")
            is_streamable = file_type in ("video", "audio")
            safe_name     = escape_markdown(file_data["file_name"])
            fmt_size      = format_size(file_data["file_size"])

            text = (
                f"✅ **{small_caps('file found')}!**\n\n"
                f"📂 **{small_caps('name')}:** `{safe_name}`\n"
                f"💾 **{small_caps('size')}:** `{fmt_size}`\n"
                f"📊 **{small_caps('type')}:** `{file_type}`\n\n"
            )

            btn_rows = []
            if is_streamable:
                text += f"🎬 **{small_caps('stream link')}:**\n`{stream_link}`"
                btn_rows.append([
                    InlineKeyboardButton(f"🎬 {small_caps('stream')}",   url=stream_link),
                    InlineKeyboardButton(f"📥 {small_caps('download')}", url=download_link),
                ])
            else:
                text += f"🔗 **{small_caps('download link')}:**\n`{download_link}`"
                btn_rows.append([
                    InlineKeyboardButton(f"📥 {small_caps('download')}", url=download_link),
                ])

            await client.send_message(
                chat_id=message.chat.id,
                text=text,
                reply_to_message_id=message.id,
                reply_markup=InlineKeyboardMarkup(btn_rows),
                disable_web_page_preview=True,
            )

        except Exception as exc:
            logger.error("deep-link error: user=%s hash=%s err=%s", user_id, file_hash, exc)
            await client.send_message(
                chat_id=message.chat.id,
                text=f"❌ `{small_caps('error')}`: ɪɴᴠᴀʟɪᴅ ᴏʀ ᴇxᴘɪʀᴇᴅ ʟɪɴᴋ",
                reply_to_message_id=message.id,
                disable_web_page_preview=True,
            )
        return

    text, buttons = show_nav("start", message.from_user, bot_username=_me.username or "")

    if Config.Start_IMG:
        try:
            await client.send_photo(
                chat_id=message.chat.id,
                photo=Config.Start_IMG,
                caption=text,
                reply_to_message_id=message.id,
                reply_markup=buttons,
            )
            return
        except Exception as exc:
            logger.warning("failed to send start photo: user=%s err=%s", user_id, exc)

    await client.send_message(
        chat_id=message.chat.id,
        text=text,
        reply_to_message_id=message.id,
        reply_markup=buttons,
        disable_web_page_preview=True,
    )


@Client.on_message(filters.command("help") & filters.private, group=1)
async def help_command(client: Client, message: Message):
    text, buttons = show_nav("help", message.from_user)
    await client.send_message(
        chat_id=message.chat.id,
        text=text,
        reply_to_message_id=message.id,
        reply_markup=buttons,
        disable_web_page_preview=True,
    )


@Client.on_message(filters.command("about") & filters.private, group=1)
async def about_command(client: Client, message: Message):
    _me = await client.get_me()
    text, buttons = show_nav("about", message.from_user, bot_username=_me.username or "")
    await client.send_message(
        chat_id=message.chat.id,
        text=text,
        reply_to_message_id=message.id,
        reply_markup=buttons,
        disable_web_page_preview=True,
    )


@Client.on_callback_query(filters.regex(r"^(start|help|about)$"), group=1)
async def cb_info(client: Client, callback: CallbackQuery):
    _me = await client.get_me()
    text, markup = show_nav(callback.data, callback.from_user, bot_username=_me.username or "")
    msg = callback.message

    try:
        if msg.photo or msg.video or msg.document or msg.animation:
            await msg.edit_caption(
                caption=text,
                reply_markup=markup
            )
        else:
            await msg.edit_text(
                text=text,
                reply_markup=markup
            )

    except Exception:
        await msg.reply(
            text=text,
            reply_markup=markup
        )

    await callback.answer()
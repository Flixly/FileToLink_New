import time
from pyrogram import Client
from pyrogram.types import BotCommand, BotCommandScopeChat
from config import Config
import logging

logger = logging.getLogger(__name__)


class Bot(Client):
    def __init__(self):
        super().__init__(
            name="FileStreamBot",
            api_id=Config.API_ID,
            api_hash=Config.API_HASH,
            bot_token=Config.BOT_TOKEN,
            plugins=dict(root="FLiX"),
            workers=50,
            sleep_threshold=10,
        )

    async def start(self):
        await super().start()
        me = await self.get_me()
        self.me       = me
        Config.UPTIME = time.time()
        logger.info("bot: @%s | name: %s | id: %s | workers: 50",
                    me.username, me.first_name, me.id)
        await self._resolve_log_channel()
        await self._set_commands()
        return me

    async def _resolve_log_channel(self):
        if not Config.FLOG_CHAT_ID:
            return
        try:
            chat = await self.get_chat(Config.FLOG_CHAT_ID)
            logger.info(
                "log channel resolved | id: %s | name: %s",
                Config.FLOG_CHAT_ID,
                getattr(chat, "title", None) or getattr(chat, "first_name", "?"),
            )
        except Exception as exc:
            logger.warning(
                "could not resolve FLOG_CHAT_ID %s — bot must be a member/admin: %s",
                Config.FLOG_CHAT_ID, exc,
            )

    async def stop(self, *args):
        await super().stop()
        logger.info("bot stopped")

    async def _set_commands(self):
        user_commands = [
            BotCommand("start",     "🚀 Start the bot"),
            BotCommand("help",      "📚 Get help info"),
            BotCommand("about",     "ℹ️ About this bot"),
            BotCommand("files",     "📂 View your files"),
        ]

        owner_commands = user_commands + [
            BotCommand("adminstats",   "📊 Admin stats"),
            BotCommand("bot_settings", "⚙️ Bot settings panel"),
            BotCommand("ban",          "🚫 Ban a user"),
            BotCommand("unban",        "✅ Unban a user"),
            BotCommand("checkban",     "🔍 Check ban status"),
            BotCommand("broadcast",    "📢 Broadcast message"),
            BotCommand("revoke",       "🗑️ Revoke file by hash"),
            BotCommand("revokeall",    "🗑️ Bulk revoke files"),
            BotCommand("logs",         "📄 Get bot logs"),
        ]
        try:
            await self.set_bot_commands(user_commands)

            for owner_id in Config.OWNER_ID:
                try:
                    await self.set_bot_commands(
                        owner_commands,
                        scope=BotCommandScopeChat(chat_id=owner_id),
                    )
                except Exception as e:
                    logger.warning("could not set owner commands for %s: %s", owner_id, e)

            logger.info("bot commands registered")
        except Exception as e:
            logger.error("failed to register commands: %s", e)

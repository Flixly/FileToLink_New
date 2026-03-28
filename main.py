import asyncio
import logging
import sys

from aiohttp import web

from bot import Bot
from app import build_app
from config import Config
from database import Database, db_instance


class LoggingFormatter(logging.Formatter):
    RESET  = "\033[0m"
    BOLD   = "\033[1m"
    GREY   = "\033[38;5;245m"
    CYAN   = "\033[38;5;51m"
    GREEN  = "\033[38;5;82m"
    YELLOW = "\033[38;5;220m"
    RED    = "\033[38;5;196m"
    PURPLE = "\033[38;5;135m"

    LEVEL_STYLES = {
        logging.DEBUG:    (GREY,   "DEBUG  "),
        logging.INFO:     (CYAN,   "INFO   "),
        logging.WARNING:  (YELLOW, "WARN   "),
        logging.ERROR:    (RED,    "ERROR  "),
        logging.CRITICAL: (RED,    "CRITIC "),
    }

    def format(self, record: logging.LogRecord) -> str:
        color, label = self.LEVEL_STYLES.get(record.levelno, (self.GREY, "?      "))
        ts   = self.formatTime(record, "%H:%M:%S")
        name = record.name.split(".")[-1][:16].ljust(16)
        msg  = record.getMessage()
        return (
            f"{self.GREY}{ts}{self.RESET} "
            f"{self.BOLD}{color}{label}{self.RESET} "
            f"{self.PURPLE}{name}{self.RESET}  "
            f"{msg}"
        )


def setup_logging() -> None:
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(LoggingFormatter())
    root.addHandler(console)

    file_h = logging.FileHandler("bot.log", encoding="utf-8")
    file_h.setLevel(logging.DEBUG)
    file_h.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
    )
    root.addHandler(file_h)

    for noisy in ("pyrogram", "aiohttp", "aiohttp.access", "aiohttp.server", "motor", "pymongo"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


setup_logging()
logger = logging.getLogger(__name__)


async def main() -> None:
    logger.info("🎬 FLiX File Stream Bot — booting up…")

    logger.info("🔍 Validating configuration…")
    try:
        Config.validate()
    except ValueError as exc:
        logger.critical("❌ Config error: %s", exc)
        raise SystemExit(1) from exc

    logger.info("🗄️  Connecting to database…")
    database = Database(Config.DB_URI, Config.DATABASE_NAME)
    await database.init_db()
    db_instance.set(database)
    await Config.load(database.db)
    logger.info("✅ Database ready")

    logger.info("🤖 Connecting bot to Telegram…")
    bot = Bot()
    await bot.start()
    bot_info = bot.me
    logger.info(
        "✅ Bot connected | @%s | %s | id: %s | dc: %s",
        bot_info.username,
        bot_info.first_name,
        bot_info.id,
        bot_info.dc_id,
    )

    logger.info("🌐 Starting web server…")
    web_app = build_app(bot, database)
    runner  = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, Config.BIND_ADDRESS, Config.PORT)
    await site.start()

    public_url = Config.URL or f"http://{Config.BIND_ADDRESS}:{Config.PORT}"
    logger.info("✅ Web server live: %s", public_url)
    logger.info("🚀 All services ready | bot: %s (@%s)", bot_info.first_name, bot_info.username)

    try:
        await asyncio.Event().wait()
    finally:
        logger.info("🛑 Shutting down…")
        await runner.cleanup()
        await database.close()
        await bot.stop()
        logger.info("✅ Shutdown complete")


asyncio.run(main())

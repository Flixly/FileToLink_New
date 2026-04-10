from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, mongo_uri: str, database_name: str):
        self.client = AsyncIOMotorClient(
            mongo_uri,
            maxPoolSize=50,
            minPoolSize=10,
            maxIdleTimeMS=45000,
            waitQueueTimeoutMS=5000,
            serverSelectionTimeoutMS=5000,
        )
        self.db            = self.client[database_name]
        self.files         = self.db.files
        self.users         = self.db.users
        self.bandwidth     = self.db.bandwidth
        self.user_bandwidth= self.db.user_bandwidth
        self.sudo_users    = self.db.sudo_users
        self.config        = self.db.config
        self.bw_resets     = self.db.bw_resets   # tracks reset timestamps

    async def init_db(self):
        try:
            async def _existing(col):
                info = await col.index_information()
                return {v['key'][0][0] for v in info.values() if v.get('key')}

            files_idx = await _existing(self.files)
            if 'file_id'    not in files_idx:
                await self.files.create_index('file_id',    unique=True)
            if 'message_id' not in files_idx:
                await self.files.create_index('message_id', unique=True)
            if 'user_id'    not in files_idx:
                await self.files.create_index('user_id')
            if 'created_at' not in files_idx:
                await self.files.create_index('created_at')

            users_idx = await _existing(self.users)
            if 'user_id'       not in users_idx:
                await self.users.create_index('user_id',      unique=True)
            if 'last_activity' not in users_idx:
                await self.users.create_index('last_activity')

            bw_idx = await _existing(self.bandwidth)
            if 'date' not in bw_idx:
                await self.bandwidth.create_index('date')

            # user_bandwidth index
            ubw_idx = await _existing(self.user_bandwidth)
            if 'user_id' not in ubw_idx:
                await self.user_bandwidth.create_index('user_id', unique=True)

            sudo_idx = await _existing(self.sudo_users)
            if 'user_id' not in sudo_idx:
                await self.sudo_users.create_index('user_id', unique=True)

            # Ensure reset tracker doc exists
            await self.bw_resets.update_one(
                {"_id": "global"},
                {"$setOnInsert": {"last_reset": datetime.utcnow(), "reset_interval_days": 30}},
                upsert=True,
            )

            logger.info("✅ ᴅʙ ɪɴᴅᴇxᴇꜱ ʀᴇᴀᴅˏ ᴀʟʟ ɪɴꜱᴛᴀɴᴛ — ꜱᴄɪᴘᴘᴇᴅ ɴᴇᴡ ᴄʀᴇᴀᴛɪᴏɴ ᴏɴʟˏ")
            return True
        except Exception as e:
            logger.error("❌ ᴅʙ ɪɴɪᴛ ᴇʀʀᴏʀ: %s", e)
            return False

    # ── Bandwidth Reset Tracker ──────────────────────────────────────────────

    async def get_bandwidth_reset_info(self) -> Dict:
        """Return last reset time and seconds until next reset."""
        try:
            doc = await self.bw_resets.find_one({"_id": "global"})
            if not doc:
                now = datetime.utcnow()
                doc = {"last_reset": now, "reset_interval_days": 30}
                await self.bw_resets.insert_one({"_id": "global", **doc})

            last_reset     = doc.get("last_reset", datetime.utcnow())
            interval_days  = doc.get("reset_interval_days", 30)
            next_reset     = last_reset + timedelta(days=interval_days)
            now            = datetime.utcnow()
            seconds_left   = max(0, int((next_reset - now).total_seconds()))
            return {
                "last_reset":          last_reset.isoformat(),
                "next_reset":          next_reset.isoformat(),
                "reset_interval_days": interval_days,
                "seconds_until_reset": seconds_left,
            }
        except Exception as e:
            logger.error("get_bandwidth_reset_info error: %s", e)
            return {
                "last_reset": datetime.utcnow().isoformat(),
                "next_reset": (datetime.utcnow() + timedelta(days=30)).isoformat(),
                "reset_interval_days": 30,
                "seconds_until_reset": 30 * 86400,
            }

    async def set_bandwidth_reset_interval(self, days: int) -> bool:
        try:
            await self.bw_resets.update_one(
                {"_id": "global"},
                {"$set": {"reset_interval_days": days}},
                upsert=True,
            )
            return True
        except Exception as e:
            logger.error("set_bandwidth_reset_interval error: %s", e)
            return False

    # ── File Operations ──────────────────────────────────────────────────────

    async def add_file(self, file_data: Dict) -> bool:
        try:
            doc = {
                "file_id":          file_data["file_id"],
                "message_id":       file_data["message_id"],
                "telegram_file_id": file_data.get("telegram_file_id", ""),
                "user_id":          file_data["user_id"],
                "username":         file_data.get("username", ""),
                "file_name":        file_data["file_name"],
                "file_size":        file_data["file_size"],
                "file_type":        file_data["file_type"],
                "mime_type":        file_data.get("mime_type", ""),
                "created_at":       datetime.utcnow(),
                "bandwidth_used":   0,
            }
            await self.files.insert_one(doc)
            return True
        except Exception as e:
            logger.error("add file error: %s", e)
            return False

    async def get_file(self, message_id: str) -> Optional[Dict]:
        try:
            return await self.files.find_one({"message_id": message_id})
        except Exception as e:
            logger.error("get file error: %s", e)
            return None

    async def get_file_by_hash(self, file_hash: str) -> Optional[Dict]:
        try:
            return await self.files.find_one({"file_id": file_hash})
        except Exception as e:
            logger.error("get file by hash error: %s", e)
            return None

    async def delete_file(self, message_id: str) -> bool:
        try:
            result = await self.files.delete_one({"message_id": message_id})
            return result.deleted_count > 0
        except Exception as e:
            logger.error("delete file error: %s", e)
            return False

    async def delete_all_files(self) -> int:
        try:
            result = await self.files.delete_many({})
            return result.deleted_count
        except Exception as e:
            logger.error("delete all files error: %s", e)
            return 0

    async def get_user_files(self, user_id: str, limit: int = 50) -> List[Dict]:
        try:
            cursor = self.files.find({"user_id": user_id}).sort("created_at", -1)
            if limit and limit > 0:
                cursor = cursor.limit(limit)
                return await cursor.to_list(length=limit)
            return await cursor.to_list(length=None)
        except Exception as e:
            logger.error("get user files error: %s", e)
            return []

    async def find_files(self, user_id, page_range: list) -> tuple:
        try:
            skip  = page_range[0] - 1 if page_range[0] > 0 else 0
            limit = page_range[1]
            total = await self.files.count_documents({"user_id": str(user_id)})
            cursor = (
                self.files.find({"user_id": str(user_id)})
                .sort("created_at", -1)
                .skip(skip)
                .limit(limit)
            )
            return cursor, total
        except Exception as e:
            logger.error("find_files error: %s", e)
            return self.files.find({"user_id": str(user_id)}).limit(0), 0

    async def delete_user_files(self, user_id: str) -> int:
        try:
            result = await self.files.delete_many({"user_id": str(user_id)})
            return result.deleted_count
        except Exception as e:
            logger.error("delete user files error: %s", e)
            return 0

    # ── Global Bandwidth ─────────────────────────────────────────────────────

    async def update_bandwidth(self, size: int, user_id: str = None) -> bool:
        try:
            today = datetime.utcnow().date().isoformat()
            await self.bandwidth.update_one(
                {"date": today},
                {
                    "$inc": {"total_bytes": size},
                    "$set": {"last_updated": datetime.utcnow()},
                },
                upsert=True,
            )
            # Also track per-user bandwidth
            if user_id:
                await self.update_user_bandwidth(str(user_id), size)
            return True
        except Exception as e:
            logger.error("update bandwidth error: %s", e)
            return False

    async def track_bandwidth(self, message_id: str, size: int, user_id: str = None) -> bool:
        try:
            await self.files.update_one(
                {"message_id": message_id},
                {"$inc": {"bandwidth_used": size}},
            )
            await self.update_bandwidth(size, user_id=user_id)
            return True
        except Exception as e:
            logger.error("track bandwidth error: %s", e)
            return False

    async def reset_bandwidth(self) -> bool:
        try:
            await self.bandwidth.delete_many({})
            await self.files.update_many({}, {"$set": {"bandwidth_used": 0}})
            # Reset all user bandwidth too
            await self.user_bandwidth.update_many({}, {"$set": {"total_bytes": 0}})
            # Record reset timestamp
            await self.bw_resets.update_one(
                {"_id": "global"},
                {"$set": {"last_reset": datetime.utcnow()}},
                upsert=True,
            )
            return True
        except Exception as e:
            logger.error("reset bandwidth error: %s", e)
            return False

    # ── Per-User Bandwidth ───────────────────────────────────────────────────

    async def update_user_bandwidth(self, user_id: str, size: int) -> bool:
        try:
            today = datetime.utcnow().date().isoformat()
            await self.user_bandwidth.update_one(
                {"user_id": user_id},
                {
                    "$inc": {"total_bytes": size, f"daily.{today}": size},
                    "$set": {"last_updated": datetime.utcnow()},
                },
                upsert=True,
            )
            return True
        except Exception as e:
            logger.error("update_user_bandwidth error: %s", e)
            return False

    async def get_user_bandwidth(self, user_id: str) -> Dict:
        try:
            doc   = await self.user_bandwidth.find_one({"user_id": str(user_id)})
            today = datetime.utcnow().date().isoformat()
            if not doc:
                return {"total_bytes": 0, "today_bytes": 0}
            daily = doc.get("daily", {})
            return {
                "total_bytes": doc.get("total_bytes", 0),
                "today_bytes": daily.get(today, 0),
            }
        except Exception as e:
            logger.error("get_user_bandwidth error: %s", e)
            return {"total_bytes": 0, "today_bytes": 0}

    async def reset_user_bandwidth(self, user_id: str) -> bool:
        try:
            await self.user_bandwidth.update_one(
                {"user_id": str(user_id)},
                {"$set": {"total_bytes": 0, "daily": {}}},
                upsert=True,
            )
            return True
        except Exception as e:
            logger.error("reset_user_bandwidth error: %s", e)
            return False

    async def get_total_bandwidth(self) -> int:
        try:
            pipeline = [{"$group": {"_id": None, "total": {"$sum": "$total_bytes"}}}]
            result   = await self.bandwidth.aggregate(pipeline).to_list(length=1)
            return result[0]["total"] if result else 0
        except Exception as e:
            logger.error("get total bandwidth error: %s", e)
            return 0

    async def get_bandwidth_stats(self) -> Dict:
        try:
            total       = await self.get_total_bandwidth()
            today       = datetime.utcnow().date().isoformat()
            today_stats = await self.bandwidth.find_one({"date": today})
            reset_info  = await self.get_bandwidth_reset_info()
            return {
                "total_bandwidth":    total,
                "today_bandwidth":    today_stats.get("total_bytes", 0) if today_stats else 0,
                "reset_info":         reset_info,
            }
        except Exception as e:
            logger.error("get bandwidth stats error: %s", e)
            return {
                "total_bandwidth": 0, "today_bandwidth": 0,
                "reset_info": {"seconds_until_reset": 0},
            }

    # ── User Management ──────────────────────────────────────────────────────

    async def register_user_on_start(self, user_data: Dict) -> bool:
        try:
            existing = await self.users.find_one({"user_id": user_data["user_id"]})
            if existing:
                await self.users.update_one(
                    {"user_id": user_data["user_id"]},
                    {"$set": {"last_activity": datetime.utcnow()}},
                )
                return False  # not new

            await self.users.insert_one({
                "user_id":          user_data["user_id"],
                "username":         user_data.get("username", ""),
                "first_name":       user_data.get("first_name", ""),
                "last_name":        user_data.get("last_name", ""),
                "first_used":       datetime.utcnow(),
                "last_activity":    datetime.utcnow(),
                # Limit fields
                "is_blocked":       False,
                "blocked_reason":   "",
                "max_bandwidth":    0,     # 0 = use global limit
                "max_files":        0,     # 0 = unlimited
                "warn_sent":        False,
            })
            logger.info("👤 ɴᴇᴡ ᴜꜱᴇʀ ʀᴇɢɪꜱᴛᴇʀᴇᴅ: %s", user_data["user_id"])
            return True  # new user
        except Exception as e:
            logger.error("❌ ʀᴇɢɪꜱᴛᴇʀ_ᴜꜱᴇʀ_ᴏɴ_ꜱᴛᴀʀᴛ ᴇʀʀᴏʀ: %s", e)
            return False

    async def get_user(self, user_id: str) -> Optional[Dict]:
        try:
            return await self.users.find_one({"user_id": str(user_id)})
        except Exception as e:
            logger.error("get user error: %s", e)
            return None

    async def set_user_blocked(self, user_id: str, blocked: bool, reason: str = "") -> bool:
        """Block or unblock a user's access to streaming/downloading."""
        try:
            await self.users.update_one(
                {"user_id": str(user_id)},
                {"$set": {
                    "is_blocked":     blocked,
                    "blocked_reason": reason,
                    "warn_sent":      blocked,
                }},
                upsert=True,
            )
            return True
        except Exception as e:
            logger.error("set_user_blocked error: %s", e)
            return False

    async def set_user_warn_sent(self, user_id: str, sent: bool = True) -> bool:
        try:
            await self.users.update_one(
                {"user_id": str(user_id)},
                {"$set": {"warn_sent": sent}},
            )
            return True
        except Exception as e:
            logger.error("set_user_warn_sent error: %s", e)
            return False

    async def set_user_limit(self, user_id: str, max_bandwidth: int = 0, max_files: int = 0) -> bool:
        """Set per-user bandwidth/file limit. 0 = use global/unlimited."""
        try:
            await self.users.update_one(
                {"user_id": str(user_id)},
                {"$set": {
                    "max_bandwidth": max_bandwidth,
                    "max_files":     max_files,
                }},
                upsert=True,
            )
            return True
        except Exception as e:
            logger.error("set_user_limit error: %s", e)
            return False

    async def check_user_limit(self, user_id: str) -> Dict:
        """
        Returns:
            allowed   : bool   — can user stream/download?
            blocked   : bool   — explicitly blocked by admin
            bw_ok     : bool   — bandwidth not exceeded
            files_ok  : bool   — file count not exceeded
            bw_used   : int
            bw_limit  : int    (0 = global)
            files_used: int
            files_limit: int   (0 = unlimited)
        """
        try:
            user = await self.users.find_one({"user_id": str(user_id)})
            if not user:
                return {"allowed": True, "blocked": False, "bw_ok": True, "files_ok": True,
                        "bw_used": 0, "bw_limit": 0, "files_used": 0, "files_limit": 0}

            blocked = user.get("is_blocked", False)

            # Per-user bandwidth
            ubw      = await self.get_user_bandwidth(str(user_id))
            bw_used  = ubw["total_bytes"]
            bw_limit = user.get("max_bandwidth", 0)
            bw_ok    = True
            if bw_limit > 0 and bw_used >= bw_limit:
                bw_ok = False

            # Per-user file count
            files_limit = user.get("max_files", 0)
            files_used  = await self.files.count_documents({"user_id": str(user_id)})
            files_ok    = True
            if files_limit > 0 and files_used >= files_limit:
                files_ok = False

            allowed = not blocked and bw_ok and files_ok
            return {
                "allowed":     allowed,
                "blocked":     blocked,
                "bw_ok":       bw_ok,
                "files_ok":    files_ok,
                "bw_used":     bw_used,
                "bw_limit":    bw_limit,
                "files_used":  files_used,
                "files_limit": files_limit,
                "warn_sent":   user.get("warn_sent", False),
            }
        except Exception as e:
            logger.error("check_user_limit error: %s", e)
            return {"allowed": True, "blocked": False, "bw_ok": True, "files_ok": True,
                    "bw_used": 0, "bw_limit": 0, "files_used": 0, "files_limit": 0}

    async def get_stats(self) -> Dict:
        try:
            total_files = await self.files.count_documents({})
            total_users = await self.users.count_documents({})
            bw          = await self.get_bandwidth_stats()
            return {
                "total_files":     total_files,
                "total_users":     total_users,
                "total_bandwidth": bw["total_bandwidth"],
                "today_bandwidth": bw["today_bandwidth"],
            }
        except Exception as e:
            logger.error("get stats error: %s", e)
            return {
                "total_files": 0, "total_users": 0,
                "total_bandwidth": 0, "today_bandwidth": 0,
            }

    # ── Sudo Users ───────────────────────────────────────────────────────────

    async def add_sudo_user(self, user_id: str, added_by: str) -> bool:
        try:
            await self.sudo_users.update_one(
                {"user_id": user_id},
                {"$set": {"user_id": user_id, "added_by": added_by, "added_at": datetime.utcnow()}},
                upsert=True,
            )
            return True
        except Exception as e:
            logger.error("add sudo user error: %s", e)
            return False

    async def remove_sudo_user(self, user_id: str) -> bool:
        try:
            result = await self.sudo_users.delete_one({"user_id": user_id})
            return result.deleted_count > 0
        except Exception as e:
            logger.error("remove sudo user error: %s", e)
            return False

    async def is_sudo_user(self, user_id: str) -> bool:
        try:
            result = await self.sudo_users.find_one({"user_id": user_id})
            return result is not None
        except Exception as e:
            logger.error("is sudo user error: %s", e)
            return False

    async def get_sudo_users(self) -> List[Dict]:
        try:
            cursor = self.sudo_users.find({})
            return await cursor.to_list(length=None)
        except Exception as e:
            logger.error("get sudo users error: %s", e)
            return []

    async def get_user_count(self) -> int:
        try:
            return await self.users.count_documents({})
        except Exception as e:
            logger.error("get user count error: %s", e)
            return 0

    async def close(self):
        self.client.close()

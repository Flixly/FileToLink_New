<div align="center">

<img src="https://i.ibb.co/2YsHg8tf/IMG-3062.jpg" width="110" height="110" style="border-radius:24px;" alt="FLiX FileStream Logo"/>

# 🎬 FLiX FileStream Bot

**A blazing-fast, production-ready Telegram file streaming & downloading service.**  
Built with Python · Pyrogram · aiohttp · MongoDB

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Pyrogram](https://img.shields.io/badge/Pyrogram-MTProto-2CA5E0?style=for-the-badge&logo=telegram&logoColor=white)](https://pyrogram.org)
[![MongoDB](https://img.shields.io/badge/MongoDB-Motor-47A248?style=for-the-badge&logo=mongodb&logoColor=white)](https://motor.readthedocs.io)
[![aiohttp](https://img.shields.io/badge/aiohttp-Web%20Server-2C5BB4?style=for-the-badge)](https://docs.aiohttp.org)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://docker.com)
[![License](https://img.shields.io/badge/License-MIT-gold?style=for-the-badge)](LICENSE)

---

*Generate shareable stream/download links for any Telegram file — instantly.*

</div>

---

## ✨ Key Features

| Feature | Description |
|---|---|
| ⚡ **Instant Streaming** | Range-request support for video seeking, audio scrubbing & resumable downloads |
| 🔐 **Secure Links** | HMAC-SHA256 signed file hashes — unforgeable and verifiable |
| 📊 **Live Dashboard** | Real-time stats, bandwidth meter, and system health monitor at `/bot_settings` |
| 🎬 **Multi-Format Player** | Built-in Plyr video/audio player with PiP, speed controls & external player launchers |
| 🌐 **Inline Sharing** | Share any file directly in any Telegram chat via inline mode with rich previews |
| 💾 **Bandwidth Control** | Configurable bandwidth cap with live tracking; auto-blocks new streams on limit |
| 🧑‍💼 **Admin Panel** | Full settings via `/bot_settings` — no config file edits needed after deploy |
| 🔄 **Force Subscription** | Optional channel gate; users must join before accessing files |
| 🐳 **Docker Support** | Single-command deployment via `docker-compose` or `docker run` |
| 📦 **Chunked MTProto** | 1 MB-aligned Telegram `upload.GetFile` chunks with automatic retry & FloodWait handling |

---

## 🖥️ UI Showcase

### Home Page `/`
The public-facing landing page showcasing bot features with a premium animated dark theme, floating particle background, and a direct Telegram CTA button.

### Stream Player `/stream/<hash>`
A cinema-grade media player page powered by **Plyr** with:
- HD video playback with seek, speed (0.5×–2×), mute, fullscreen
- Picture-in-Picture mode
- External player launchers: **VLC**, **MX Player**, **PlayIt**, **KM Player**
- Real-time playback status (Playing · Paused · Buffering)
- One-click stream URL copy

### Bot Control Panel `/bot_settings`
A tabbed admin dashboard with three panels:
- **Stats** — Users, Chats, Files, RAM, CPU, Uptime
- **Bandwidth** — Live usage bar, daily/total/remaining transfer data
- **Health** — Live streaming sessions counter, bot identity, server/API status, response latency meter

### 404 Not Found `/not_found`
Animated ghost error page with contextual "why did this happen?" chips and recovery buttons.

### Bandwidth Exceeded `/bandwidth_limit`
Clean warning card shown when the monthly bandwidth cap is reached, with an owner contact button.

---

## 🏗️ Project Structure

```
filestream-bot/
├── main.py                   # Entry point — boots bot + web server concurrently
├── app.py                    # aiohttp web app: routes, middleware, HTML & JSON responses
├── bot.py                    # Pyrogram client initialisation
├── config.py                 # Configuration loader + logging setup
│
├── FLiX/
│   ├── __init__.py
│   ├── admin.py              # /bot_settings, /adminstats, /revoke, /revokeall, /logs
│   ├── gen.py                # File upload handler, /files, inline query, all callbacks
│   └── start.py              # /start, /help, /about
│
├── database/
│   └── mongodb.py            # Motor async MongoDB client (files, users, bandwidth, settings)
│
├── helper/
│   ├── __init__.py
│   ├── bandwidth.py          # Bandwidth check helper
│   ├── crypto.py             # HMAC-SHA256 file hash utility
│   ├── stream.py             # ByteStreamer (MTProto chunked streaming) + StreamingService
│   └── utils.py              # format_size, small_caps, check_owner, check_fsub, escape_markdown
│
├── templates/                # Jinja2 HTML templates (dark themed, mobile-first)
│   ├── home.html             # Public landing page
│   ├── stream.html           # Plyr media player page
│   ├── bot_settings.html     # Admin control panel (Stats / Bandwidth / Health)
│   ├── not_found.html        # 404 error page
│   └── bandwidth_exceeded.html  # 503 bandwidth limit page
│
├── Dockerfile                # Production Docker image
├── requirements.txt          # Python dependencies
├── .env.example              # Template for environment variables
└── README.md
```

---

## 🚀 Installation

### Prerequisites

- **Python 3.11+**
- **MongoDB 6.0+** (local or Atlas)
- **Telegram Bot Token** → [@BotFather](https://t.me/BotFather)
- **Telegram API credentials** → [my.telegram.org](https://my.telegram.org)
- A **private Telegram channel** as the file dump storage

---

### Method 1 — Docker (Recommended)

```bash
# 1. Clone the repository
git clone https://github.com/yourname/filestream-bot.git
cd filestream-bot

# 2. Create your environment file
cp .env.example .env
nano .env          # Fill in all required values

# 3. Build and run
docker build -t filestream-bot .
docker run -d --env-file .env --name filestream filestream-bot

# 4. View logs
docker logs -f filestream
```

---

### Method 2 — Manual (Virtual Environment)

```bash
# 1. Clone and enter directory
git clone https://github.com/yourname/filestream-bot.git
cd filestream-bot

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
nano .env          # Fill in all required values

# 5. Run the bot
python main.py
```

---

## ⚙️ Configuration

Copy `.env.example` to `.env` and fill in your values.  
All settings can be modified **live** via `/bot_settings` without restarting the bot.

### Required Variables

| Variable | Description |
|---|---|
| `BOT_TOKEN` | Telegram bot token from [@BotFather](https://t.me/BotFather) |
| `API_ID` | Telegram API ID from [my.telegram.org](https://my.telegram.org) |
| `API_HASH` | Telegram API Hash from [my.telegram.org](https://my.telegram.org) |
| `FLOG_CHAT_ID` | Numeric ID of your private file dump channel (e.g. `-100123456789`) |
| `OWNER_ID` | Your Telegram user ID (comma-separated for multiple admins) |
| `DB_URI` | MongoDB connection URI (e.g. `mongodb://localhost:27017` or Atlas URI) |

### Optional Variables

| Variable | Default | Description |
|---|---|---|
| `DATABASE_NAME` | `filestream_bot` | MongoDB database name |
| `URL` | auto-detected | Public base URL for generated links (e.g. `https://stream.yourdomain.com`) |
| `PORT` | `8080` | Web server port |
| `LOGS_CHAT_ID` | `0` | Channel for new-user log events (0 = disabled) |
| `SECRET_KEY` | auto-generated | HMAC secret for link signing |
| `Start_IMG` | — | Image URL displayed with `/start` |
| `Files_IMG` | — | Image URL displayed with `/files` |
| `FSUB_ID` | — | Force-subscription channel ID |
| `FSUB_INV_LINK` | — | Invite link for the force-sub channel |
| `PUBLIC_BOT` | `False` | Allow everyone to upload files |
| `MAX_BANDWIDTH` | `107374182400` | Monthly bandwidth cap in bytes (default: 100 GB) |
| `MAX_FILE_SIZE` | `4294967296` | Maximum accepted file size in bytes (default: 4 GB) |

> **Tip:** `PUBLIC_BOT`, `MAX_BANDWIDTH`, bandwidth mode, force-sub settings, and sudo users are all managed **live** via `/bot_settings` and persisted in MongoDB. The `.env` values serve as **initial defaults only**.

---

## 🤖 Bot Commands

### User Commands

| Command | Description |
|---|---|
| `/start` | Welcome message with feature overview |
| `/help` | Detailed usage guide |
| `/about` | Bot information and credits |
| `/files` | Browse, stream, download, or revoke your uploaded files |

### Owner / Admin Commands

| Command | Description |
|---|---|
| `/bot_settings` | Full interactive settings panel |
| `/adminstats` | Detailed stats: uptime, users, files, bandwidth breakdown |
| `/revoke <hash>` | Revoke a specific file and invalidate its links |
| `/revokeall` | Delete all files (with confirm/cancel prompt) |
| `/revokeall <user_id>` | Delete all files belonging to a specific user |
| `/logs` | Receive the current `bot.log` file as a Telegram document |
| `/files <user_id>` | View another user's files with owner-level revoke access |

---

## 🌐 Web Routes & API

### Browser Routes

| Route | Description |
|---|---|
| `GET /` | Home page — public landing page |
| `GET /stream/<hash>` | Media player page (HTML) or raw stream (Range request / non-browser) |
| `GET /dl/<hash>` | Force-download with `Content-Disposition: attachment` |
| `GET /bot_settings` | Admin control panel |

### JSON API Endpoints

Append `Accept: application/json` header to get raw JSON instead of HTML:

| Endpoint | Response |
|---|---|
| `GET /api/stats` | Users, files, RAM, CPU, uptime, bandwidth summary |
| `GET /api/bandwidth` | Detailed bandwidth stats: used, today, remaining, limit, percentage |
| `GET /api/health` | Bot status, live streaming sessions, bot identity, DC info |
| `GET /stats` | Redirects to `/bot_settings` (JSON if `Accept: application/json`) |
| `GET /bandwidth` | Redirects to `/bot_settings` (JSON if `Accept: application/json`) |
| `GET /health` | Redirects to `/bot_settings` (JSON if `Accept: application/json`) |

#### Example `/api/health` response

```json
{
  "status": "ok",
  "bot_status": "running",
  "bot_name": "FLiX FileStream",
  "bot_username": "FLiXStreamBot",
  "bot_id": "123456789",
  "bot_dc": "5",
  "active_conns": 3,
  "active_conns_description": "Live streaming/download sessions currently transferring bytes"
}
```

> **`active_conns`** is a real-time counter — it increments when a streaming/download session begins sending bytes and decrements the moment the transfer completes or errors. It accurately reflects concurrent live transfers.

---

## 🩺 Health & Monitoring

The `/api/health` and `/bot_settings` → **Health** tab expose:

| Metric | Description |
|---|---|
| **Server Status** | Whether the aiohttp web server is reachable |
| **Bot API Status** | Whether the Pyrogram client is connected and authorized |
| **Flood Status** | Real-time Telegram flood-wait detection |
| **Response Latency** | Round-trip time to `/api/health` in milliseconds |
| **Live Streaming Sessions** | Real-time count of active file transfer connections |
| **Bot Identity** | Name, @username, numeric ID, data-center |

The Health panel auto-refreshes every **5 seconds** when active.  
Stats and Bandwidth panels auto-refresh every **30 seconds**.

---

## 🐳 Docker Deployment

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
CMD ["python", "main.py"]
```

```bash
# Build
docker build -t filestream-bot .

# Run with environment file
docker run -d \
  --name filestream \
  --env-file .env \
  -p 8080:8080 \
  --restart unless-stopped \
  filestream-bot
```

### With Docker Compose

```yaml
version: "3.9"
services:
  bot:
    build: .
    env_file: .env
    ports:
      - "8080:8080"
    restart: unless-stopped
    depends_on:
      - mongo

  mongo:
    image: mongo:6
    volumes:
      - mongo_data:/data/db
    restart: unless-stopped

volumes:
  mongo_data:
```

```bash
docker-compose up -d
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| **Bot Framework** | [Pyrogram](https://pyrogram.org) — async MTProto client |
| **Web Server** | [aiohttp](https://docs.aiohttp.org) — async HTTP server |
| **Templating** | [Jinja2](https://jinja.palletsprojects.com) via `aiohttp-jinja2` |
| **Database** | [MongoDB](https://mongodb.com) via [Motor](https://motor.readthedocs.io) (async driver) |
| **Frontend** | Vanilla HTML/CSS/JS · [Plyr](https://plyr.io) · [Font Awesome](https://fontawesome.com) · Poppins/Sora fonts |
| **Security** | HMAC-SHA256 link signing via `hashlib` |
| **System Metrics** | [psutil](https://psutil.readthedocs.io) for CPU/RAM monitoring |
| **Containerisation** | Docker |

---

## 📦 Dependencies

```
pyrogram
tgcrypto
motor
aiohttp
aiohttp-jinja2
jinja2
python-dotenv
psutil
```

Install with:
```bash
pip install -r requirements.txt
```

---

## 🔒 Security Notes

- File links are signed with **HMAC-SHA256** using a configurable `SECRET_KEY` — links cannot be guessed or forged.
- The bot does **not store file bytes** — all data remains on Telegram servers; only metadata (file ID, size, name, hash) is stored in MongoDB.
- The dump channel (`FLOG_CHAT_ID`) should be a **private channel** inaccessible to regular users.
- Set `PUBLIC_BOT=False` (default) to restrict file uploads to owner + sudo users only.

---

## 👨‍💻 Developer

<div align="center">

Built with ❤️ by **[@FLiX_LY](https://t.me/FLiX_LY)**

</div>

---

## 📄 License

This project is licensed under the **MIT License**.  
See the [LICENSE](LICENSE) file for full details.

---

<div align="center">

⭐ **Star this repo** if FLiX FileStream saves you time!

</div>

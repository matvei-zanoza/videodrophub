import asyncio
import base64
import contextlib
import hashlib
import json
import logging
import os
import re
import shutil
import sqlite3
import tempfile
import time
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TimedOut
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from yt_dlp import YoutubeDL


TIKTOK_URL_RE = re.compile(
    r"https?://(?:www\.)?(?:vm\.|vt\.)?tiktok\.com/[^\s]+|https?://(?:www\.)?tiktok\.com/@[^\s]+/video/\d+",
    re.IGNORECASE,
)

VK_CLIP_URL_RE = re.compile(
    r"https?://(?:m\.)?vk\.com/(?:clip-?\d+_\d+|clips/[^\s]+|clip/[^\s]+)",
    re.IGNORECASE,
)

INSTAGRAM_REEL_URL_RE = re.compile(
    r"https?://(?:www\.)?(?:instagram\.com|instagr\.am)/(?:reel|reels)/[A-Za-z0-9_-]+(?:\?[^\s]+)?",
    re.IGNORECASE,
)

YOUTUBE_SHORTS_URL_RE = re.compile(
    r"https?://(?:www\.)?youtube\.com/shorts/[A-Za-z0-9_-]+(?:\?[^\s]+)?",
    re.IGNORECASE,
)

SORA_SHARE_URL_RE = re.compile(
    r"https?://sora\.chatgpt\.com/p/[A-Za-z0-9_]+(?:\?[^\s]+)?",
    re.IGNORECASE,
)

DIRECT_VIDEO_URL_RE = re.compile(
    r"https?://[^\s]+\.(?:mp4|webm|m3u8|mpd)(?:\?[^\s]+)?",
    re.IGNORECASE,
)


BOT_USERNAME = "videodrophub_bot"
BOT_URL = f"https://t.me/{BOT_USERNAME}"

GLOBAL_DOWNLOAD_SEMAPHORE: asyncio.Semaphore | None = None
MAX_CONCURRENT_DOWNLOADS = 2
USER_LOCKS: dict[int, asyncio.Lock] = {}
ACTIVE_DOWNLOADS = 0
ACTIVE_DOWNLOADS_LOCK: asyncio.Lock | None = None

DOWNLOAD_TIMEOUT = 180
MAX_FILESIZE_BYTES: int | None = None
MAX_VIDEO_DURATION_SECONDS: int | None = None

CACHE_DIR: Path | None = None
CACHE_TTL_SECONDS: int | None = None
MAX_CACHE_FILES: int | None = None

YTDLP_COOKIEFILE: Path | None = None

DATA_DIR: Path | None = None
DB_PATH: Path | None = None
ADMIN_IDS: set[int] = set()
MAINTENANCE_MODE = False

ENABLE_YOUTUBE = False

STARTED_AT = time.time()


def get_share_url() -> str:
    text = "Скачай видео из TikTok через бота"
    url_q = urllib.parse.quote_plus(BOT_URL)
    text_q = urllib.parse.quote_plus(text)
    return f"https://t.me/share/url?url={url_q}&text={text_q}"


def build_result_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Скачать ещё", callback_data="download_more")],
            [
                InlineKeyboardButton("Поделиться ботом", url=get_share_url()),
                InlineKeyboardButton("Открыть бот", url=BOT_URL),
            ],
        ]
    )


def _looks_like_vk_auth_error(message: str) -> bool:
    m = message.lower()
    return (
        "you have to log in" in m
        or "login" in m
        or "authorization" in m
        or "access denied" in m
        or "private" in m
        or "forbidden" in m
        or "http error 403" in m
        or "http error 401" in m
    )


def _looks_like_instagram_rate_limit_error(message: str) -> bool:
    m = message.lower()
    return (
        "please wait a few minutes" in m
        or "rate limit" in m
        or "too many requests" in m
        or "http error 429" in m
    )


def _looks_like_instagram_auth_error(message: str) -> bool:
    m = message.lower()
    return (
        "login required" in m
        or "checkpoint_required" in m
        or "challenge_required" in m
        or "consent_required" in m
        or "http error 403" in m
        or "http error 401" in m
        or "forbidden" in m
    )


def build_admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Статистика", callback_data="admin_stats"),
                InlineKeyboardButton("Кэш", callback_data="admin_cache"),
            ],
            [
                InlineKeyboardButton("Рассылка", callback_data="admin_broadcast"),
                InlineKeyboardButton("Техработы", callback_data="admin_maintenance"),
            ],
            [InlineKeyboardButton("Логи", callback_data="admin_logs")],
            [InlineKeyboardButton("Закрыть", callback_data="admin_close")],
        ]
    )


def _parse_admin_ids(raw: str | None) -> set[int]:
    if not raw:
        return set()
    parts = [p.strip() for p in raw.split(",")]
    out: set[int] = set()
    for p in parts:
        if not p:
            continue
        try:
            out.add(int(p))
        except ValueError:
            continue
    return out


def is_admin(user_id: int | None) -> bool:
    if user_id is None:
        return False
    return user_id in ADMIN_IDS


def _get_active_downloads_lock() -> asyncio.Lock:
    global ACTIVE_DOWNLOADS_LOCK
    if ACTIVE_DOWNLOADS_LOCK is None:
        ACTIVE_DOWNLOADS_LOCK = asyncio.Lock()
    return ACTIVE_DOWNLOADS_LOCK


def _db_connect() -> sqlite3.Connection:
    if DB_PATH is None:
        raise RuntimeError("DB not configured")
    try:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=3000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def db_init() -> None:
    if DB_PATH is None:
        return
    with contextlib.closing(_db_connect()) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS users ("
            " user_id INTEGER PRIMARY KEY,"
            " username TEXT,"
            " first_name TEXT,"
            " last_name TEXT,"
            " first_seen_at REAL NOT NULL,"
            " last_seen_at REAL NOT NULL"
            ")"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS counters ("
            " key TEXT PRIMARY KEY,"
            " value INTEGER NOT NULL"
            ")"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS download_events ("
            " id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " ts REAL NOT NULL,"
            " user_id INTEGER,"
            " platform TEXT NOT NULL,"
            " status TEXT NOT NULL,"
            " cached INTEGER NOT NULL DEFAULT 0,"
            " error_kind TEXT"
            ")"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_download_events_ts ON download_events(ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_download_events_platform_ts ON download_events(platform, ts)")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_events ("
            " id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " ts REAL NOT NULL,"
            " admin_id INTEGER,"
            " action TEXT NOT NULL,"
            " details TEXT"
            ")"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_admin_events_ts ON admin_events(ts)")
        conn.commit()


def _detect_platform(url: str) -> str:
    if TIKTOK_URL_RE.search(url):
        return "tiktok"
    if VK_CLIP_URL_RE.search(url):
        return "vk"
    if INSTAGRAM_REEL_URL_RE.search(url):
        return "instagram"
    if YOUTUBE_SHORTS_URL_RE.search(url):
        return "youtube"
    if DIRECT_VIDEO_URL_RE.search(url):
        return "direct"
    if SORA_SHARE_URL_RE.search(url):
        return "sora"
    return "other"


def db_log_download_event(
    *,
    user_id: int | None,
    url: str,
    status: str,
    cached: bool = False,
    error_kind: str | None = None,
) -> None:
    now = time.time()
    platform = _detect_platform(url)
    with contextlib.closing(_db_connect()) as conn:
        conn.execute(
            "INSERT INTO download_events(ts, user_id, platform, status, cached, error_kind) VALUES(?, ?, ?, ?, ?, ?)",
            (now, user_id, platform, status, 1 if cached else 0, error_kind),
        )
        conn.commit()


def db_log_admin_event(*, admin_id: int | None, action: str, details: str | None = None) -> None:
    now = time.time()
    with contextlib.closing(_db_connect()) as conn:
        conn.execute(
            "INSERT INTO admin_events(ts, admin_id, action, details) VALUES(?, ?, ?, ?)",
            (now, admin_id, action, details),
        )
        conn.commit()


def _time_human(ts: float) -> str:
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
    except Exception:
        return str(int(ts))


def db_get_download_stats_since(since_ts: float) -> dict[str, int]:
    with contextlib.closing(_db_connect()) as conn:
        cur = conn.execute(
            "SELECT platform, status, COUNT(*) AS c FROM download_events WHERE ts>=? GROUP BY platform, status",
            (since_ts,),
        )
        out: dict[str, int] = {}
        for r in cur.fetchall():
            key = f"{str(r['platform'])}:{str(r['status'])}"
            out[key] = int(r["c"])
        return out


def db_get_user_status_counts(user_id: int, since_ts: float | None = None) -> dict[str, int]:
    with contextlib.closing(_db_connect()) as conn:
        if since_ts is None:
            cur = conn.execute(
                "SELECT status, COUNT(*) AS c FROM download_events WHERE user_id=? GROUP BY status",
                (user_id,),
            )
        else:
            cur = conn.execute(
                "SELECT status, COUNT(*) AS c FROM download_events WHERE user_id=? AND ts>=? GROUP BY status",
                (user_id, since_ts),
            )
        out: dict[str, int] = {}
        for r in cur.fetchall():
            out[str(r["status"])] = int(r["c"])
        return out


def db_get_user_total_requests(user_id: int, since_ts: float | None = None) -> int:
    with contextlib.closing(_db_connect()) as conn:
        if since_ts is None:
            cur = conn.execute("SELECT COUNT(*) AS c FROM download_events WHERE user_id=?", (user_id,))
        else:
            cur = conn.execute("SELECT COUNT(*) AS c FROM download_events WHERE user_id=? AND ts>=?", (user_id, since_ts))
        row = cur.fetchone()
        if not row:
            return 0
        return int(row["c"])


def db_get_recent_admin_events(limit: int = 20) -> list[sqlite3.Row]:
    with contextlib.closing(_db_connect()) as conn:
        cur = conn.execute(
            "SELECT ts, admin_id, action, details FROM admin_events ORDER BY ts DESC LIMIT ?",
            (limit,),
        )
        return list(cur.fetchall())


def db_set_counter(key: str, value: int) -> None:
    with contextlib.closing(_db_connect()) as conn:
        conn.execute(
            "INSERT INTO counters(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        conn.commit()


def db_get_counter(key: str, default: int = 0) -> int:
    with contextlib.closing(_db_connect()) as conn:
        cur = conn.execute("SELECT value FROM counters WHERE key=?", (key,))
        row = cur.fetchone()
        if not row:
            return default
        return int(row["value"])


def db_inc_counter(key: str, delta: int = 1) -> None:
    with contextlib.closing(_db_connect()) as conn:
        conn.execute(
            "INSERT INTO counters(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=value+excluded.value",
            (key, delta),
        )
        conn.commit()


def db_upsert_user(user_id: int, username: str | None, first_name: str | None, last_name: str | None) -> None:
    now = time.time()
    with contextlib.closing(_db_connect()) as conn:
        conn.execute(
            "INSERT INTO users(user_id, username, first_name, last_name, first_seen_at, last_seen_at) "
            "VALUES(?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET "
            " username=excluded.username,"
            " first_name=excluded.first_name,"
            " last_name=excluded.last_name,"
            " last_seen_at=excluded.last_seen_at",
            (user_id, username, first_name, last_name, now, now),
        )
        conn.commit()


def db_list_user_ids() -> list[int]:
    with contextlib.closing(_db_connect()) as conn:
        cur = conn.execute("SELECT user_id FROM users")
        return [int(r["user_id"]) for r in cur.fetchall()]


def db_get_summary() -> dict[str, int]:
    with contextlib.closing(_db_connect()) as conn:
        cur = conn.execute("SELECT key, value FROM counters")
        counters = {str(r["key"]): int(r["value"]) for r in cur.fetchall()}
        cur2 = conn.execute("SELECT COUNT(*) AS c FROM users")
        users_count = int(cur2.fetchone()["c"])
    counters["users"] = users_count
    return counters


def get_cache_stats() -> tuple[int, int]:
    if CACHE_DIR is None:
        return 0, 0
    try:
        count = 0
        total = 0
        for p in CACHE_DIR.glob("*.mp4"):
            try:
                count += 1
                total += p.stat().st_size
            except Exception:
                pass
        return count, total
    except Exception:
        return 0, 0


def _format_bytes(num: int) -> str:
    try:
        mb = num / (1024 * 1024)
        if mb < 1024:
            return f"{mb:.2f} MB"
        gb = mb / 1024
        return f"{gb:.2f} GB"
    except Exception:
        return str(num)


def find_first_supported_url(text: str) -> str | None:
    matches: list[re.Match[str]] = []
    m1 = TIKTOK_URL_RE.search(text)
    if m1:
        matches.append(m1)
    m_vk = VK_CLIP_URL_RE.search(text)
    if m_vk:
        matches.append(m_vk)
    m_ig = INSTAGRAM_REEL_URL_RE.search(text)
    if m_ig:
        matches.append(m_ig)
    m0 = DIRECT_VIDEO_URL_RE.search(text)
    if m0:
        matches.append(m0)
    if ENABLE_YOUTUBE:
        m2 = YOUTUBE_SHORTS_URL_RE.search(text)
        if m2:
            matches.append(m2)
    if not matches:
        return None
    first = min(matches, key=lambda m: m.start())
    return first.group(0)


def _parse_bool(raw: str | None, default: bool = False) -> bool:
    if raw is None:
        return default
    v = raw.strip().lower()
    if v in {"1", "true", "yes", "y", "on"}:
        return True
    if v in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _looks_like_youtube_cookie_error(message: str) -> bool:
    m = message.lower()
    return (
        "confirm you’re not a bot" in m
        or "confirm you're not a bot" in m
        or "use --cookies" in m
        or "cookies-from-browser" in m
        or "sign in to confirm" in m
    )


def _has_ytdlp_cookies() -> bool:
    return YTDLP_COOKIEFILE is not None and YTDLP_COOKIEFILE.exists()


def _configure_ytdlp_cookies(data_dir: Path | None) -> Path | None:
    cookie_file_env = os.getenv("YTDLP_COOKIE_FILE")
    if cookie_file_env:
        path = Path(cookie_file_env)
        if path.exists():
            return path

    cookies_b64 = os.getenv("YTDLP_COOKIES_B64")
    if not cookies_b64:
        return None

    try:
        raw = base64.b64decode(cookies_b64.encode("utf-8"), validate=True)
    except Exception:
        return None

    if data_dir is None:
        return None

    try:
        data_dir.mkdir(parents=True, exist_ok=True)
        out_path = data_dir / "youtube_cookies.txt"
        out_path.write_bytes(raw)
        try:
            os.chmod(out_path, 0o600)
        except Exception:
            pass
        return out_path
    except Exception:
        return None


def _cache_key(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:32]


def _cache_paths(url: str) -> tuple[Path, Path] | None:
    if CACHE_DIR is None:
        return None
    key = _cache_key(url)
    return (CACHE_DIR / f"{key}.mp4", CACHE_DIR / f"{key}.json")


def _read_cache_meta(meta_path: Path) -> dict | None:
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _is_cache_valid(video_path: Path, meta_path: Path) -> bool:
    if CACHE_TTL_SECONDS is None:
        return False
    if not video_path.exists() or not meta_path.exists():
        return False
    meta = _read_cache_meta(meta_path)
    if not meta or "created_at" not in meta:
        return False
    try:
        created_at = float(meta["created_at"])
    except Exception:
        return False
    return (time.time() - created_at) <= CACHE_TTL_SECONDS


def _touch(path: Path) -> None:
    try:
        now = time.time()
        os.utime(path, (now, now))
    except Exception:
        pass


def _prune_cache() -> None:
    if CACHE_DIR is None or CACHE_TTL_SECONDS is None or MAX_CACHE_FILES is None:
        return
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

        now = time.time()
        metas = list(CACHE_DIR.glob("*.json"))

        for meta_path in metas:
            video_path = meta_path.with_suffix(".mp4")
            meta = _read_cache_meta(meta_path)
            created_at = None
            if meta and "created_at" in meta:
                try:
                    created_at = float(meta["created_at"])
                except Exception:
                    created_at = None

            expired = created_at is None or (now - created_at) > CACHE_TTL_SECONDS
            if expired:
                try:
                    meta_path.unlink(missing_ok=True)
                except Exception:
                    pass
                try:
                    video_path.unlink(missing_ok=True)
                except Exception:
                    pass

        metas = list(CACHE_DIR.glob("*.json"))
        if len(metas) <= MAX_CACHE_FILES:
            return

        metas_sorted = sorted(metas, key=lambda p: p.stat().st_mtime)
        to_remove = metas_sorted[: max(0, len(metas_sorted) - MAX_CACHE_FILES)]
        for meta_path in to_remove:
            video_path = meta_path.with_suffix(".mp4")
            try:
                meta_path.unlink(missing_ok=True)
            except Exception:
                pass
            try:
                video_path.unlink(missing_ok=True)
            except Exception:
                pass
    except Exception:
        return


def _download_tiktok_video_sync(url: str, out_dir: Path, filename_base: str | None = None) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    if filename_base:
        outtmpl = str(out_dir / f"{filename_base}.%(ext)s")
    else:
        outtmpl = str(out_dir / "%(id)s.%(ext)s")

    ydl_opts = {
        "outtmpl": outtmpl,
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "merge_output_format": "mp4",
        "format": "mp4/best",
    }

    if MAX_FILESIZE_BYTES is not None:
        ydl_opts["max_filesize"] = int(MAX_FILESIZE_BYTES)

    if MAX_VIDEO_DURATION_SECONDS is not None:
        max_dur = int(MAX_VIDEO_DURATION_SECONDS)

        def _match_filter(info_dict: dict, *args) -> str | None:
            try:
                dur = info_dict.get("duration")
            except Exception:
                dur = None
            if dur is None:
                return None
            try:
                dur_i = int(dur)
            except Exception:
                return None
            if dur_i > max_dur:
                return "duration_limit_exceeded"
            return None

        ydl_opts["match_filter"] = _match_filter

    if YTDLP_COOKIEFILE is not None and YTDLP_COOKIEFILE.exists():
        ydl_opts["cookiefile"] = str(YTDLP_COOKIEFILE)

    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        file_path = ydl.prepare_filename(info)

    path = Path(file_path)
    if path.suffix.lower() != ".mp4":
        mp4_candidate = path.with_suffix(".mp4")
        if mp4_candidate.exists():
            return mp4_candidate
    return path


async def download_tiktok_video(url: str) -> tuple[Path, bool]:
    cache_pair = _cache_paths(url)
    if cache_pair is not None:
        video_path, meta_path = cache_pair
        if _is_cache_valid(video_path, meta_path):
            _touch(video_path)
            _touch(meta_path)
            return video_path, True

    if cache_pair is not None:
        video_path, meta_path = cache_pair
        key = video_path.stem
        path = await asyncio.to_thread(_download_tiktok_video_sync, url, CACHE_DIR, key)
        final_path = path
        if final_path.suffix.lower() != ".mp4":
            mp4_candidate = final_path.with_suffix(".mp4")
            if mp4_candidate.exists():
                final_path = mp4_candidate

        if final_path != video_path and final_path.exists():
            try:
                final_path.replace(video_path)
            except Exception:
                pass

        try:
            meta_path.write_text(
                json.dumps({"created_at": time.time()}, ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception:
            pass

        await asyncio.to_thread(_prune_cache)
        return video_path, True

    tmp_dir = Path(tempfile.mkdtemp(prefix="tiktok_"))
    path = await asyncio.to_thread(_download_tiktok_video_sync, url, tmp_dir, None)
    return path, False


def _looks_like_filesize_error(message: str) -> bool:
    m = message.lower()
    return "max-filesize" in m or "max filesize" in m or "larger than max" in m


def _looks_like_duration_error(message: str) -> bool:
    m = message.lower()
    return "duration_limit_exceeded" in m or "duration" in m and "limit" in m


def _looks_like_network_error(message: str) -> bool:
    m = message.lower()
    return (
        "timed out" in m
        or "timeout" in m
        or "temporarily unavailable" in m
        or "temporary failure" in m
        or "connection reset" in m
        or "connection aborted" in m
        or "connection refused" in m
        or "network is unreachable" in m
        or "name or service not known" in m
        or "tls" in m and "handshake" in m
    )


def _purge_cache_for_url(url: str) -> None:
    pair = _cache_paths(url)
    if pair is None:
        return
    video_path, meta_path = pair
    try:
        video_path.unlink(missing_ok=True)
    except Exception:
        pass
    try:
        meta_path.unlink(missing_ok=True)
    except Exception:
        pass


def parse_message(text: str) -> tuple[str | None, str | None]:
    if SORA_SHARE_URL_RE.search(text) and not DIRECT_VIDEO_URL_RE.search(text):
        return (
            None,
            "Ссылки Sora (sora.chatgpt.com/p/...) сервер часто блокирует для ботов. "
            "Пришли, пожалуйста, прямую ссылку на видео (.mp4/.webm) или поток (.m3u8/.mpd) — я смогу скачать и отправить.",
        )
    url = find_first_supported_url(text)
    if url is None:
        return None, None
    return url, None


def validate_request(user_id: int | None, url: str) -> str | None:
    if MAINTENANCE_MODE and not is_admin(user_id):
        return "Сейчас идут техработы. Попробуй позже."
    if not ENABLE_YOUTUBE and YOUTUBE_SHORTS_URL_RE.search(url):
        return "YouTube Shorts пока временно не поддерживаются. Пришли ссылку TikTok."
    return None


async def process_download(
    *,
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    url: str,
) -> None:
    global ACTIVE_DOWNLOADS

    user_lock = USER_LOCKS.get(user_id)
    if user_lock is None:
        user_lock = asyncio.Lock()
        USER_LOCKS[user_id] = user_lock

    if DB_PATH is not None:
        await asyncio.to_thread(db_inc_counter, "requests", 1)

    async with user_lock:
        async with GLOBAL_DOWNLOAD_SEMAPHORE:
            async with _get_active_downloads_lock():
                ACTIVE_DOWNLOADS += 1
            video_path = None
            cached = False
            status = await update.message.reply_text("Скачиваю... ⏳")
            try:
                last_exc: Exception | None = None
                for attempt in range(2):
                    try:
                        video_path, cached = await asyncio.wait_for(download_tiktok_video(url), timeout=DOWNLOAD_TIMEOUT)
                        break
                    except asyncio.TimeoutError as e:
                        last_exc = e
                        if attempt == 0:
                            continue
                        raise
                    except Exception as e:
                        last_exc = e
                        err_text = str(e)
                        is_auth = (
                            (YOUTUBE_SHORTS_URL_RE.search(url) and _looks_like_youtube_cookie_error(err_text))
                            or (VK_CLIP_URL_RE.search(url) and _looks_like_vk_auth_error(err_text))
                            or (INSTAGRAM_REEL_URL_RE.search(url) and _looks_like_instagram_auth_error(err_text))
                        )
                        is_rate_limit = INSTAGRAM_REEL_URL_RE.search(url) and _looks_like_instagram_rate_limit_error(err_text)
                        if is_auth:
                            raise
                        if is_rate_limit and attempt == 0:
                            continue
                        if _looks_like_network_error(err_text) and attempt == 0:
                            continue
                        raise

                if video_path is None:
                    raise last_exc or RuntimeError("Не удалось скачать видео")

                if DB_PATH is not None:
                    await asyncio.to_thread(db_inc_counter, "cache_hit" if cached else "cache_miss", 1)

                if not video_path.exists():
                    raise RuntimeError("Видео не удалось скачать: файл не найден")

                caption = "Готово. Вот ваше видео!\nПоделись ботом: @videodrophub_bot"
                keyboard = build_result_keyboard()

                try:
                    with video_path.open("rb") as f:
                        await context.bot.send_video(
                            chat_id=update.effective_chat.id,
                            video=f,
                            caption=caption,
                            reply_markup=keyboard,
                        )
                except Exception:
                    _purge_cache_for_url(url)
                    raise

                try:
                    await status.delete()
                except Exception:
                    pass

                if DB_PATH is not None:
                    await asyncio.to_thread(db_inc_counter, "success", 1)
                    await asyncio.to_thread(
                        db_log_download_event,
                        user_id=update.effective_user.id if update.effective_user else None,
                        url=url,
                        status="success",
                        cached=cached,
                    )
            except asyncio.TimeoutError:
                user_error = "Скачивание заняло слишком много времени. Попробуй ещё раз."
                try:
                    await status.edit_text(user_error)
                except Exception:
                    await update.message.reply_text(user_error)
                if DB_PATH is not None:
                    await asyncio.to_thread(db_inc_counter, "errors", 1)
                    await asyncio.to_thread(
                        db_log_download_event,
                        user_id=update.effective_user.id if update.effective_user else None,
                        url=url,
                        status="error",
                        cached=cached,
                        error_kind="timeout",
                    )
            except Exception as e:
                err_text = str(e)
                error_kind = "other"
                if _looks_like_filesize_error(err_text):
                    mb = int((MAX_FILESIZE_BYTES or 0) / (1024 * 1024))
                    user_error = f"Видео слишком большое. Лимит: {mb} MB."
                    error_kind = "max_filesize"
                elif _looks_like_duration_error(err_text):
                    user_error = f"Видео слишком длинное. Лимит: {int(MAX_VIDEO_DURATION_SECONDS or 0)} сек."
                    error_kind = "max_duration"
                elif YOUTUBE_SHORTS_URL_RE.search(url) and _looks_like_youtube_cookie_error(err_text):
                    if _has_ytdlp_cookies():
                        user_error = (
                            "YouTube попросил подтверждение (анти-бот). "
                            "Cookies уже подключены, но не помогли — нужно обновить cookies."
                        )
                    else:
                        user_error = (
                            "YouTube попросил подтверждение (анти-бот). "
                            "Чтобы скачивание работало, админ должен подключить cookies для yt-dlp."
                        )
                    error_kind = "youtube_auth"
                elif VK_CLIP_URL_RE.search(url) and _looks_like_vk_auth_error(err_text):
                    user_error = (
                        "VK ограничил доступ к этому клипу (возможна приватность/нужна авторизация). "
                        "Попробуй другой клип или отправь прямую ссылку на .mp4/.m3u8."
                    )
                    error_kind = "vk_auth"
                elif INSTAGRAM_REEL_URL_RE.search(url) and _looks_like_instagram_auth_error(err_text):
                    user_error = (
                        "Instagram ограничил доступ (часто нужна авторизация/подтверждение). "
                        "Попробуй другой рилс или пришли прямую ссылку на .mp4/.m3u8."
                    )
                    error_kind = "instagram_auth"
                elif INSTAGRAM_REEL_URL_RE.search(url) and _looks_like_instagram_rate_limit_error(err_text):
                    user_error = "Instagram временно ограничил запросы (слишком часто). Подожди 5–10 минут и попробуй ещё раз."
                    error_kind = "instagram_rate_limit"
                else:
                    user_error = f"Ошибка при скачивании: {e}"

                try:
                    await status.edit_text(user_error)
                except Exception:
                    await update.message.reply_text(user_error)

                if DB_PATH is not None:
                    await asyncio.to_thread(db_inc_counter, "errors", 1)
                    await asyncio.to_thread(
                        db_log_download_event,
                        user_id=update.effective_user.id if update.effective_user else None,
                        url=url,
                        status="error",
                        cached=cached,
                        error_kind=error_kind,
                    )
            finally:
                async with _get_active_downloads_lock():
                    ACTIVE_DOWNLOADS -= 1
                if video_path is not None and not cached:
                    await safe_cleanup(video_path)


async def safe_cleanup(path: Path) -> None:
    try:
        if path.exists():
            path.unlink(missing_ok=True)
        if path.parent.exists():
            for child in path.parent.iterdir():
                try:
                    if child.is_file():
                        child.unlink(missing_ok=True)
                except Exception:
                    pass
            try:
                path.parent.rmdir()
            except Exception:
                pass
    except Exception:
        pass


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if update.effective_user and DB_PATH is not None:
        await asyncio.to_thread(
            db_upsert_user,
            update.effective_user.id,
            update.effective_user.username,
            update.effective_user.first_name,
            update.effective_user.last_name,
        )
    start_text = (
        "👋 Привет!\n\n"
        "Я скачиваю видео по ссылке и отправляю файлом 📥\n"
        "Без водяных знаков и лишних телодвижений.\n\n"
        "Поддерживаю:\n"
        "•\tTikTok 🎬\n"
        "•\tVK Клипы 🟦\n"
        "•\tInstagram Reels 📸\n\n"
        "Можно присылать прямые ссылки:\n"
        ".mp4 · .webm · .m3u8 · .mpd\n\n"
        "Просто вставь ссылку и получи видео. Всё."
    )
    await update.message.reply_text(start_text, parse_mode=ParseMode.HTML)


async def cmd_myid(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    await update.message.reply_text(str(update.effective_user.id))


async def cmd_health(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Нет доступа")
        return

    uptime_s = int(time.time() - STARTED_AT)
    uptime_h = uptime_s // 3600
    uptime_m = (uptime_s % 3600) // 60

    async with _get_active_downloads_lock():
        active = ACTIVE_DOWNLOADS

    cache_count, cache_size = await asyncio.to_thread(get_cache_stats)

    free_disk = None
    try:
        if DATA_DIR is not None:
            free_disk = int(shutil.disk_usage(str(DATA_DIR)).free)
    except Exception:
        free_disk = None

    text = (
        f"⏱ Uptime: {uptime_h}h {uptime_m}m\n"
        f"📥 Active downloads: {active}\n"
        f"🎛 MAX_CONCURRENT_DOWNLOADS: {int(MAX_CONCURRENT_DOWNLOADS)}\n"
        f"💾 Free disk (DATA_DIR): {_format_bytes(free_disk) if free_disk is not None else 'N/A'}\n"
        f"🗂 Cache: {cache_count} files / {_format_bytes(cache_size)}\n"
        f"🚧 Maintenance: {'ON' if MAINTENANCE_MODE else 'OFF'}"
    )
    await update.message.reply_text(text)


async def cmd_limits(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return

    max_mb = None
    if MAX_FILESIZE_BYTES is not None:
        try:
            max_mb = int(MAX_FILESIZE_BYTES / (1024 * 1024))
        except Exception:
            max_mb = None

    max_min = None
    if MAX_VIDEO_DURATION_SECONDS is not None:
        try:
            max_min = round(int(MAX_VIDEO_DURATION_SECONDS) / 60, 1)
        except Exception:
            max_min = None

    text = (
        "Ограничения:\n"
        f"• Максимальный размер: {f'{max_mb} MB' if max_mb is not None else 'N/A'}\n"
        f"• Максимальная длительность: {f'{max_min} мин' if max_min is not None else 'N/A'}\n"
        "• Лимит: 1 загрузка на пользователя\n"
        f"• Глобальный лимит: {int(MAX_CONCURRENT_DOWNLOADS)} одновременных загрузок\n"
        "• Поддерживаемые прямые форматы: .mp4 / .webm / .m3u8 / .mpd"
    )
    await update.message.reply_text(text)


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return

    args = [a.strip().lower() for a in (context.args or [])]
    if args != ["me"]:
        await update.message.reply_text("Использование: /stats me")
        return

    if DB_PATH is None:
        await update.message.reply_text("Статистика недоступна")
        return

    user_id = update.effective_user.id
    now = time.time()
    since_24h = now - 24 * 3600
    since_7d = now - 7 * 24 * 3600

    try:
        total = await asyncio.to_thread(db_get_user_total_requests, user_id)
        status_all = await asyncio.to_thread(db_get_user_status_counts, user_id)
        total_24h = await asyncio.to_thread(db_get_user_total_requests, user_id, since_24h)
        status_24h = await asyncio.to_thread(db_get_user_status_counts, user_id, since_24h)
        total_7d = await asyncio.to_thread(db_get_user_total_requests, user_id, since_7d)
        status_7d = await asyncio.to_thread(db_get_user_status_counts, user_id, since_7d)
    except Exception:
        await update.message.reply_text("Статистика недоступна")
        return

    if total <= 0:
        await update.message.reply_text("Пока нет статистики по твоим запросам.")
        return

    ok = status_all.get("success", 0)
    err = status_all.get("error", 0)
    ok_24h = status_24h.get("success", 0)
    err_24h = status_24h.get("error", 0)
    ok_7d = status_7d.get("success", 0)
    err_7d = status_7d.get("error", 0)

    text = (
        f"📥 Всего запросов: {total}\n"
        f"✅ Успешных: {ok}\n"
        f"❌ Ошибок: {err}\n"
        "\n"
        f"🕒 За 24 часа: {total_24h} (✅{ok_24h} ❌{err_24h})\n"
        f"📅 За 7 дней: {total_7d} (✅{ok_7d} ❌{err_7d})"
    )
    await update.message.reply_text(text)


async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Нет доступа")
        return
    await update.message.reply_text("Админ-панель", reply_markup=build_admin_keyboard())


async def on_admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.callback_query:
        return

    user_id = update.effective_user.id if update.effective_user else None
    if not is_admin(user_id):
        await update.callback_query.answer("Нет доступа", show_alert=True)
        return

    data = update.callback_query.data or ""
    await update.callback_query.answer()

    global MAINTENANCE_MODE

    if data == "admin_close":
        try:
            await update.callback_query.edit_message_text("Админ-панель закрыта")
        except Exception:
            pass
        return

    if data == "admin_stats":
        if DB_PATH is None:
            text = "DB не настроена"
        else:
            await asyncio.to_thread(db_log_admin_event, admin_id=user_id, action="admin_stats")
            summary = await asyncio.to_thread(db_get_summary)
            now = time.time()
            stats_24h = await asyncio.to_thread(db_get_download_stats_since, now - 24 * 3600)
            stats_7d = await asyncio.to_thread(db_get_download_stats_since, now - 7 * 24 * 3600)
            uptime_s = int(time.time() - STARTED_AT)
            uptime_h = uptime_s // 3600
            uptime_m = (uptime_s % 3600) // 60
            async with _get_active_downloads_lock():
                active = ACTIVE_DOWNLOADS
            cache_count, cache_size = await asyncio.to_thread(get_cache_stats)
            cache_mb = round(cache_size / (1024 * 1024), 2)

            def _fmt_platform(stats: dict[str, int], platform: str) -> str:
                ok = stats.get(f"{platform}:success", 0)
                err = stats.get(f"{platform}:error", 0)
                return f"{platform} ✅{ok} ❌{err}"

            text = (
                f"Пользователи: {summary.get('users', 0)}\n"
                f"Запросы: {summary.get('requests', 0)}\n"
                f"Успех: {summary.get('success', 0)}\n"
                f"Ошибки: {summary.get('errors', 0)}\n"
                f"Cache hit: {summary.get('cache_hit', 0)}\n"
                f"Cache miss: {summary.get('cache_miss', 0)}\n"
                "\n"
                f"За 24ч: {_fmt_platform(stats_24h, 'tiktok')} | {_fmt_platform(stats_24h, 'vk')} | {_fmt_platform(stats_24h, 'instagram')}\n"
                f"За 7д: {_fmt_platform(stats_7d, 'tiktok')} | {_fmt_platform(stats_7d, 'vk')} | {_fmt_platform(stats_7d, 'instagram')}\n"
                "\n"
                f"Uptime: {uptime_h}h {uptime_m}m\n"
                f"Active downloads: {active}\n"
                f"Cache: {cache_count} files / {cache_mb} MB\n"
                f"ENABLE_YOUTUBE: {'ON' if ENABLE_YOUTUBE else 'OFF'}\n"
                f"YouTube cookies: {'ON' if _has_ytdlp_cookies() else 'OFF'}\n"
                f"Техработы: {'ON' if MAINTENANCE_MODE else 'OFF'}"
            )
        await update.callback_query.edit_message_text(text, reply_markup=build_admin_keyboard())
        return

    if data == "admin_cache":
        if DB_PATH is not None:
            await asyncio.to_thread(db_log_admin_event, admin_id=user_id, action="admin_cache")
        count, size = await asyncio.to_thread(get_cache_stats)
        size_mb = round(size / (1024 * 1024), 2)
        ttl = CACHE_TTL_SECONDS or 0
        max_files = MAX_CACHE_FILES or 0
        text = (
            f"Файлов в кэше: {count}\n"
            f"Размер кэша: {size_mb} MB\n"
            f"TTL: {ttl} сек\n"
            f"MAX_CACHE_FILES: {max_files}"
        )
        await update.callback_query.edit_message_text(text, reply_markup=build_admin_keyboard())
        return

    if data == "admin_maintenance":
        MAINTENANCE_MODE = not MAINTENANCE_MODE
        if DB_PATH is not None:
            await asyncio.to_thread(db_set_counter, "maintenance", 1 if MAINTENANCE_MODE else 0)
            await asyncio.to_thread(
                db_log_admin_event,
                admin_id=user_id,
                action="admin_maintenance_toggle",
                details="ON" if MAINTENANCE_MODE else "OFF",
            )
        text = f"Техработы: {'ON' if MAINTENANCE_MODE else 'OFF'}"
        await update.callback_query.edit_message_text(text, reply_markup=build_admin_keyboard())
        return

    if data == "admin_broadcast":
        context.user_data["awaiting_broadcast"] = True
        if DB_PATH is not None:
            await asyncio.to_thread(db_log_admin_event, admin_id=user_id, action="admin_broadcast_prompt")
        await update.callback_query.edit_message_text(
            "Пришли текст рассылки следующим сообщением (или используй /broadcast <текст>).",
            reply_markup=build_admin_keyboard(),
        )
        return

    if data == "admin_logs":
        if DB_PATH is None:
            text = "DB не настроена"
        else:
            await asyncio.to_thread(db_log_admin_event, admin_id=user_id, action="admin_logs")
            rows = await asyncio.to_thread(db_get_recent_admin_events, 20)
            if not rows:
                text = "Логи пустые"
            else:
                lines: list[str] = []
                for r in rows:
                    ts = _time_human(float(r["ts"]))
                    aid = r["admin_id"]
                    action = r["action"]
                    details = r["details"]
                    if details:
                        lines.append(f"{ts} | {aid} | {action} | {details}")
                    else:
                        lines.append(f"{ts} | {aid} | {action}")
                text = "\n".join(lines)
        await update.callback_query.edit_message_text(text, reply_markup=build_admin_keyboard())
        return


async def do_broadcast(context: ContextTypes.DEFAULT_TYPE, text: str) -> tuple[int, int]:
    if DB_PATH is None:
        return 0, 0
    user_ids = await asyncio.to_thread(db_list_user_ids)
    ok = 0
    failed = 0
    for uid in user_ids:
        try:
            await context.bot.send_message(chat_id=uid, text=text)
            ok += 1
        except RetryAfter as e:
            await asyncio.sleep(float(getattr(e, "retry_after", 1.0)))
            try:
                await context.bot.send_message(chat_id=uid, text=text)
                ok += 1
            except Exception:
                failed += 1
        except (Forbidden, BadRequest):
            failed += 1
        except (TimedOut, NetworkError):
            failed += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.03)
    return ok, failed


async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Нет доступа")
        return

    text = " ".join(getattr(context, "args", []) or []).strip()
    if not text:
        context.user_data["awaiting_broadcast"] = True
        await update.message.reply_text("Пришли текст рассылки следующим сообщением.")
        return

    ok, failed = await do_broadcast(context, text)
    if DB_PATH is not None:
        await asyncio.to_thread(
            db_log_admin_event,
            admin_id=update.effective_user.id,
            action="broadcast_done",
            details=f"ok={ok} fail={failed} len={len(text)}",
        )
    await update.message.reply_text(f"Рассылка завершена. OK: {ok}, FAIL: {failed}")


async def on_download_more(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.callback_query:
        return
    await update.callback_query.answer()
    if update.effective_chat:
        prompt = "Пришли новую ссылку на TikTok / VK Клипы / Instagram Reels — я скачаю видео."
        if ENABLE_YOUTUBE:
            prompt = "Пришли новую ссылку на TikTok / VK Клипы / Instagram Reels / YouTube Shorts — я скачаю видео."
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=prompt,
        )


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.text:
        return

    if update.effective_user and DB_PATH is not None:
        await asyncio.to_thread(
            db_upsert_user,
            update.effective_user.id,
            update.effective_user.username,
            update.effective_user.first_name,
            update.effective_user.last_name,
        )

    if update.effective_user and is_admin(update.effective_user.id) and context.user_data.get("awaiting_broadcast"):
        context.user_data["awaiting_broadcast"] = False
        ok, failed = await do_broadcast(context, update.message.text)
        await update.message.reply_text(f"Рассылка завершена. OK: {ok}, FAIL: {failed}")
        return

    if update.message.text.strip().casefold() == "матвей заноза":
        await update.message.reply_text("Галоши постирал!")
        return

    if update.message.text.strip().casefold() in {"что ты умеешь?", "что ты умеешь"}:
        await update.message.reply_text("Я нихуя не умею как и кое кто(")
        return

    url, parse_error = parse_message(update.message.text)
    if parse_error:
        await update.message.reply_text(parse_error)
        return

    if not url:
        text = "Не вижу ссылку TikTok / VK Клипы / Instagram Reels. Пришли, пожалуйста, ссылку на видео."
        if ENABLE_YOUTUBE:
            text = "Не вижу ссылку TikTok / VK Клипы / Instagram Reels / YouTube Shorts. Пришли, пожалуйста, ссылку на видео."
        await update.message.reply_text(text)
        return

    if not update.effective_user:
        await update.message.reply_text("Не удалось определить пользователя.")
        return

    user_id = update.effective_user.id
    user_lock = USER_LOCKS.get(user_id)
    if user_lock is None:
        user_lock = asyncio.Lock()
        USER_LOCKS[user_id] = user_lock

    if user_lock.locked():
        await update.message.reply_text("У тебя уже идёт загрузка. Подожди, пожалуйста.")
        return

    if GLOBAL_DOWNLOAD_SEMAPHORE is None:
        await update.message.reply_text("Бот ещё запускается, попробуй через пару секунд.")
        return

    validation_error = validate_request(user_id, url)
    if validation_error:
        await update.message.reply_text(validation_error)
        return

    await process_download(update=update, context=context, user_id=user_id, url=url)


def main() -> None:
    load_dotenv()
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN не задан. Создай .env на основе .env.example")

    global ADMIN_IDS
    ADMIN_IDS = _parse_admin_ids(os.getenv("ADMIN_IDS"))

    global ENABLE_YOUTUBE
    ENABLE_YOUTUBE = _parse_bool(os.getenv("ENABLE_YOUTUBE"), default=False)

    max_concurrent = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "2"))
    global MAX_CONCURRENT_DOWNLOADS
    MAX_CONCURRENT_DOWNLOADS = max_concurrent
    global GLOBAL_DOWNLOAD_SEMAPHORE
    GLOBAL_DOWNLOAD_SEMAPHORE = asyncio.Semaphore(max_concurrent)

    global DOWNLOAD_TIMEOUT
    DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", "180"))

    global MAX_FILESIZE_BYTES
    MAX_FILESIZE_BYTES = int(os.getenv("MAX_FILESIZE_BYTES", str(200 * 1024 * 1024)))

    global MAX_VIDEO_DURATION_SECONDS
    MAX_VIDEO_DURATION_SECONDS = int(os.getenv("MAX_VIDEO_DURATION_SECONDS", "900"))

    global CACHE_DIR, CACHE_TTL_SECONDS, MAX_CACHE_FILES
    CACHE_DIR = Path(os.getenv("CACHE_DIR", ".cache"))
    CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "21600"))
    MAX_CACHE_FILES = int(os.getenv("MAX_CACHE_FILES", "50"))
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        CACHE_DIR = None

    try:
        if CACHE_DIR is not None and MAX_CACHE_FILES is not None:
            count, _size = get_cache_stats()
            if count > int(MAX_CACHE_FILES * 1.2):
                _prune_cache()
    except Exception:
        pass

    global DATA_DIR, DB_PATH, MAINTENANCE_MODE
    DATA_DIR = Path(os.getenv("DATA_DIR", ".data"))
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        DB_PATH = DATA_DIR / "bot.db"
        db_init()
        MAINTENANCE_MODE = bool(db_get_counter("maintenance", 0))
    except Exception:
        DB_PATH = None
        MAINTENANCE_MODE = False

    global YTDLP_COOKIEFILE
    YTDLP_COOKIEFILE = _configure_ytdlp_cookies(DATA_DIR)

    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)

    application = Application.builder().token(token).build()
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("myid", cmd_myid))
    application.add_handler(CommandHandler("health", cmd_health))
    application.add_handler(CommandHandler("limits", cmd_limits))
    application.add_handler(CommandHandler("stats", cmd_stats))
    application.add_handler(CommandHandler("admin", cmd_admin))
    application.add_handler(CommandHandler("broadcast", cmd_broadcast))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    application.add_handler(CallbackQueryHandler(on_download_more, pattern=r"^download_more$"))
    application.add_handler(CallbackQueryHandler(on_admin_callback, pattern=r"^admin_"))

    application.run_polling()


if __name__ == "__main__":
    main()

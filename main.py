import asyncio
import base64
import contextlib
import hashlib
import json
import logging
import os
import re
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

YOUTUBE_SHORTS_URL_RE = re.compile(
    r"https?://(?:www\.)?youtube\.com/shorts/[A-Za-z0-9_-]+(?:\?[^\s]+)?",
    re.IGNORECASE,
)


BOT_USERNAME = "videodrophub_bot"
BOT_URL = f"https://t.me/{BOT_USERNAME}"

GLOBAL_DOWNLOAD_SEMAPHORE: asyncio.Semaphore | None = None
USER_LOCKS: dict[int, asyncio.Lock] = {}

CACHE_DIR: Path | None = None
CACHE_TTL_SECONDS: int | None = None
MAX_CACHE_FILES: int | None = None

YTDLP_COOKIEFILE: Path | None = None

DATA_DIR: Path | None = None
DB_PATH: Path | None = None
ADMIN_IDS: set[int] = set()
MAINTENANCE_MODE = False


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


def _db_connect() -> sqlite3.Connection:
    if DB_PATH is None:
        raise RuntimeError("DB not configured")
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
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
        conn.commit()


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


def find_first_supported_url(text: str) -> str | None:
    matches: list[re.Match[str]] = []
    m1 = TIKTOK_URL_RE.search(text)
    if m1:
        matches.append(m1)
    m2 = YOUTUBE_SHORTS_URL_RE.search(text)
    if m2:
        matches.append(m2)
    if not matches:
        return None
    first = min(matches, key=lambda m: m.start())
    return first.group(0)


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
    await update.message.reply_text(
        "Пришли ссылку на TikTok или YouTube Shorts — я попробую скачать и отправить файл обратно.",
        parse_mode=ParseMode.HTML,
    )


async def cmd_myid(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    await update.message.reply_text(str(update.effective_user.id))


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
            summary = await asyncio.to_thread(db_get_summary)
            text = (
                f"Пользователи: {summary.get('users', 0)}\n"
                f"Запросы: {summary.get('requests', 0)}\n"
                f"Успех: {summary.get('success', 0)}\n"
                f"Ошибки: {summary.get('errors', 0)}\n"
                f"Cache hit: {summary.get('cache_hit', 0)}\n"
                f"Cache miss: {summary.get('cache_miss', 0)}\n"
                f"YouTube cookies: {'ON' if _has_ytdlp_cookies() else 'OFF'}\n"
                f"Техработы: {'ON' if MAINTENANCE_MODE else 'OFF'}"
            )
        await update.callback_query.edit_message_text(text, reply_markup=build_admin_keyboard())
        return

    if data == "admin_cache":
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
        text = f"Техработы: {'ON' if MAINTENANCE_MODE else 'OFF'}"
        await update.callback_query.edit_message_text(text, reply_markup=build_admin_keyboard())
        return

    if data == "admin_broadcast":
        context.user_data["awaiting_broadcast"] = True
        await update.callback_query.edit_message_text(
            "Пришли текст рассылки следующим сообщением (или используй /broadcast <текст>).",
            reply_markup=build_admin_keyboard(),
        )
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
    await update.message.reply_text(f"Рассылка завершена. OK: {ok}, FAIL: {failed}")


async def on_download_more(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.callback_query:
        return
    await update.callback_query.answer()
    if update.effective_chat:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Пришли новую ссылку на TikTok или YouTube Shorts — я скачаю видео.",
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

    url = find_first_supported_url(update.message.text)
    if not url:
        await update.message.reply_text(
            "Не вижу ссылку TikTok или YouTube Shorts. Пришли, пожалуйста, ссылку на видео."
        )
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

    if MAINTENANCE_MODE and not is_admin(user_id):
        await update.message.reply_text("Сейчас идут техработы. Попробуй позже.")
        return

    if DB_PATH is not None:
        await asyncio.to_thread(db_inc_counter, "requests", 1)

    async with user_lock:
        async with GLOBAL_DOWNLOAD_SEMAPHORE:
            status = await update.message.reply_text("Скачиваю…")

            video_path: Path | None = None
            cached = False
            try:
                video_path, cached = await download_tiktok_video(url)
                if DB_PATH is not None:
                    await asyncio.to_thread(db_inc_counter, "cache_hit" if cached else "cache_miss", 1)
                if not video_path.exists():
                    raise RuntimeError("Видео не удалось скачать: файл не найден")

                caption = "Готово. Вот ваше видео!\nПоделись ботом: @videodrophub_bot"
                keyboard = build_result_keyboard()

                try:
                    await update.message.reply_video(
                        video=str(video_path),
                        caption=caption,
                        reply_markup=keyboard,
                    )
                except Exception:
                    await update.message.reply_document(
                        document=str(video_path),
                        caption=caption,
                        reply_markup=keyboard,
                    )

                try:
                    await status.delete()
                except Exception:
                    pass

                if DB_PATH is not None:
                    await asyncio.to_thread(db_inc_counter, "success", 1)
            except Exception as e:
                err_text = str(e)
                if YOUTUBE_SHORTS_URL_RE.search(url) and _looks_like_youtube_cookie_error(err_text):
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
                else:
                    user_error = f"Ошибка при скачивании: {e}"

                try:
                    await status.edit_text(user_error)
                except Exception:
                    await update.message.reply_text(user_error)

                if DB_PATH is not None:
                    await asyncio.to_thread(db_inc_counter, "errors", 1)
            finally:
                if video_path is not None and not cached:
                    await safe_cleanup(video_path)


def main() -> None:
    load_dotenv()
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN не задан. Создай .env на основе .env.example")

    global ADMIN_IDS
    ADMIN_IDS = _parse_admin_ids(os.getenv("ADMIN_IDS"))

    max_concurrent = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "2"))
    global GLOBAL_DOWNLOAD_SEMAPHORE
    GLOBAL_DOWNLOAD_SEMAPHORE = asyncio.Semaphore(max_concurrent)

    global CACHE_DIR, CACHE_TTL_SECONDS, MAX_CACHE_FILES
    CACHE_DIR = Path(os.getenv("CACHE_DIR", ".cache"))
    CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "21600"))
    MAX_CACHE_FILES = int(os.getenv("MAX_CACHE_FILES", "50"))
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        CACHE_DIR = None

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
    application.add_handler(CommandHandler("admin", cmd_admin))
    application.add_handler(CommandHandler("broadcast", cmd_broadcast))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    application.add_handler(CallbackQueryHandler(on_download_more, pattern=r"^download_more$"))
    application.add_handler(CallbackQueryHandler(on_admin_callback, pattern=r"^admin_"))

    application.run_polling()


if __name__ == "__main__":
    main()

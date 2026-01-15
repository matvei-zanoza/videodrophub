import asyncio
import logging
import os
import re
import tempfile
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
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


BOT_USERNAME = "videodrophub_bot"
BOT_URL = f"https://t.me/{BOT_USERNAME}"

GLOBAL_DOWNLOAD_SEMAPHORE: asyncio.Semaphore | None = None
USER_LOCKS: dict[int, asyncio.Lock] = {}


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


def _download_tiktok_video_sync(url: str, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    outtmpl = str(out_dir / "%(id)s.%(ext)s")

    ydl_opts = {
        "outtmpl": outtmpl,
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "merge_output_format": "mp4",
        "format": "mp4/best",
    }

    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        file_path = ydl.prepare_filename(info)

    path = Path(file_path)
    if path.suffix.lower() != ".mp4":
        mp4_candidate = path.with_suffix(".mp4")
        if mp4_candidate.exists():
            return mp4_candidate
    return path


async def download_tiktok_video(url: str) -> Path:
    tmp_dir = Path(tempfile.mkdtemp(prefix="tiktok_"))
    return await asyncio.to_thread(_download_tiktok_video_sync, url, tmp_dir)


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
    await update.message.reply_text(
        "Пришли ссылку на видео TikTok — я попробую скачать и отправить файл обратно.",
        parse_mode=ParseMode.HTML,
    )


async def on_download_more(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.callback_query:
        return
    await update.callback_query.answer()
    if update.effective_chat:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Пришли новую ссылку на TikTok — я скачаю видео.",
        )


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.text:
        return

    match = TIKTOK_URL_RE.search(update.message.text)
    if not match:
        await update.message.reply_text("Не вижу ссылку TikTok. Пришли, пожалуйста, ссылку на видео.")
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

    url = match.group(0)

    async with user_lock:
        async with GLOBAL_DOWNLOAD_SEMAPHORE:
            status = await update.message.reply_text("Скачиваю…")

            video_path: Path | None = None
            try:
                video_path = await download_tiktok_video(url)
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
            except Exception as e:
                try:
                    await status.edit_text(f"Ошибка при скачивании: {e}")
                except Exception:
                    await update.message.reply_text(f"Ошибка при скачивании: {e}")
            finally:
                if video_path is not None:
                    await safe_cleanup(video_path)


def main() -> None:
    load_dotenv()
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN не задан. Создай .env на основе .env.example")

    max_concurrent = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "2"))
    global GLOBAL_DOWNLOAD_SEMAPHORE
    GLOBAL_DOWNLOAD_SEMAPHORE = asyncio.Semaphore(max_concurrent)

    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)

    application = Application.builder().token(token).build()
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    application.add_handler(CallbackQueryHandler(on_download_more, pattern=r"^download_more$"))

    application.run_polling()


if __name__ == "__main__":
    main()

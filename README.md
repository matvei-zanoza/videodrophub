# vedeobot

## Описание
Асинхронный Telegram-бот (python-telegram-bot, polling), который принимает ссылки на видео и отправляет скачанный файл пользователю.
Скачивание выполняется через `yt-dlp` в отдельном потоке.

## Архитектура (высокоуровнево)
- Вся логика находится в `main.py`
- Telegram: `python-telegram-bot` (polling)
- Скачивание: `yt-dlp` (вызов в `asyncio.to_thread`)
- Ограничение нагрузки:
  - `asyncio.Lock` на пользователя (1 активная загрузка на пользователя)
  - глобальный `asyncio.Semaphore` (`MAX_CONCURRENT_DOWNLOADS`)
- SQLite: WAL + `busy_timeout`
- Файловый кэш: TTL + лимит файлов

## Основной пользовательский флоу
1. Пользователь отправляет сообщение со ссылкой.
2. Бот извлекает первую поддерживаемую ссылку.
3. Проверяет режим техработ и флаги конфигурации.
4. Ограничивает конкуррентность (per-user lock + global semaphore).
5. Проверяет кэш, при необходимости скачивает через `yt-dlp`.
6. Отправляет видео пользователю.

## ENV-переменные
- `BOT_TOKEN` (обязательно)
- `ADMIN_IDS` (например: `123,456`)
- `ENABLE_YOUTUBE` (`true/false`)
- `MAX_CONCURRENT_DOWNLOADS` (например: `2`)
- `DOWNLOAD_TIMEOUT` (секунды)
- `MAX_FILESIZE_BYTES`
- `MAX_VIDEO_DURATION_SECONDS`
- `CACHE_DIR`
- `CACHE_TTL_SECONDS`
- `MAX_CACHE_FILES`
- `DATA_DIR`
- `YTDLP_COOKIE_FILE` (путь к cookies-файлу)
- `YTDLP_COOKIES_B64` (cookies в base64, альтернативный способ)

## Команды
- `/start` — приветствие
- `/myid` — показать ваш Telegram user id
- `/limits` — показать текущие ограничения
- `/stats me` — личная статистика по запросам
- `/admin` — админ-панель
- `/broadcast` — рассылка (админ)
- `/health` — диагностика/метрики (только админ)

## Разработка
- Python 3.11+
- Установить зависимости: `pip install -r requirements.txt`
- Создать `.env` на основе переменных выше
- Запуск: `python main.py`

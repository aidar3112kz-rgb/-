import os
import re
import logging
from typing import Dict, Tuple
from sheets import SheetsClient
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(_name_)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CODE_HEADER = os.getenv("CODE_HEADER", "Код товара").strip()

CODE_PATTERNS = [
    r"(?i)\bкод(?:\s*товара)?\s*[:=\-]\s([A-Za-z0-9_\-\.]+)",
    r"(?i)\bcode\s*[:=\-]\s([A-Za-z0-9_\-\.]+)",
]

# пары вида: "Ключ=Значение" или "Ключ: Значение" или "Ключ - Значение"
FIELD_KV_PATTERN = re.compile(r"(?P<k>[А-Яа-яA-Za-z0-9_\.\s/-]{2,30})\s*[:=\-]\s*(?P<v>[^\n;|,]{1,120})")

def parse_message(text: str) -> Tuple[str, Dict[str, str]]:
    """
    Из текста вытаскиваем:
    - code (по триггерам "Код:", "code:")
    - пары ключ: значение / ключ=значение / ключ - значение
    """
    code = ""
    for pat in CODE_PATTERNS:
        m = re.search(pat, text)
        if m:
            code = m.group(1).strip()
            break

    # извлекаем пары ключ-значение
    fields: Dict[str, str] = {}
    for m in FIELD_KV_PATTERN.finditer(text):
        k = m.group("k").strip().strip(".").strip()
        v = m.group("v").strip()
        # пропускаем само слово "код", чтобы не дублировать
        if k.lower().startswith("код"):
            continue
        fields[k] = v

    return code, fields

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Отправь сообщение вида:\n"
        "Код: A123; Цена=36500; Город: Алматы\n\n"
        f"Столбец кода сейчас: '{CODE_HEADER}'. Измени через переменную окружения CODE_HEADER."
    )

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    if not text:
        return

    code, fields = parse_message(text)

    if not code:
        await update.message.reply_text(
            "Не нашёл код товара. Напиши, например: 'Код: A123; Цена=36500; Город: Алматы'."
        )
        return

    # Если пользователь прислал только код — всё равно запишем/создадим строку
    try:
        sc = SheetsClient()
        row_idx = sc.upsert_by_code(code, fields)
        if fields:
            await update.message.reply_text(
                f"Ок! Обновил строку #{row_idx} для кода '{code}'. Поля: {', '.join(fields.keys())}."
            )
        else:
            await update.message.reply_text(
                f"Ок! Строка для кода '{code}' создана/актуализирована (данных кроме кода не было)."
            )
    except Exception as e:
        logger.exception("Sheets update error")
        await update.message.reply_text(f"Ошибка записи в Google Sheets: {e}")

def build_app() -> Application:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("Missing env TELEGRAM_BOT_TOKEN")
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_text))
    return app

if _name_ == "_main_":
    app = build_app()

    # Локальный запуск: polling
    if os.getenv("USE_POLLING", "1") == "1":
        app.run_polling()
    else:
        # Для Render/Webhook:
        from telegram.ext import ContextTypes
        import asyncio
        WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # https://<your-service>/webhook
        PORT = int(os.getenv("PORT", "8000"))

        async def main():
            await app.bot.set_webhook(WEBHOOK_URL)
            await app.start()
            await app.updater.start_webhook(listen="0.0.0.0", port=PORT, url_path="")
            await asyncio.Event().wait()

        asyncio.run(main())

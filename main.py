# main.py
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.exceptions import TelegramForbiddenError
import asyncio
import logging
from logging.handlers import TimedRotatingFileHandler
import os
from dotenv import load_dotenv

from text_handlers import handle_text_message
from start_handlers import on_start

load_dotenv()
API_TOKEN = os.getenv('API_TOKEN')

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

@dp.message(CommandStart())
async def start_handler(message):
    try:
        await on_start(message)
    except TelegramForbiddenError:
        logging.warning(f"Пользователь {message.from_user.id} заблокировал бота")
    except Exception as e:
        logging.error(f"Ошибка в обработчике старта: {e}")

# Безопасный обработчик текста
async def safe_text_handler(message):
    try:
        await handle_text_message(message)
    except TelegramForbiddenError:
        logging.warning(f"Пользователь {message.from_user.id} заблокировал бота")
    except Exception as e:
        logging.error(f"Ошибка обработки текста: {e}")

# Регистрируем хэндлер для текстовых сообщений (без голоса и фото)
dp.message.register(safe_text_handler, F.text & ~F.voice & ~F.photo)

# Настройка логирования
os.makedirs("logs", exist_ok=True)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

# Файловый хэндлер с ротацией по дате
file_handler = TimedRotatingFileHandler(
    filename="logs/bot.log",
    when="midnight",
    interval=1,
    backupCount=7,
    encoding="utf-8"
)
file_handler.setFormatter(formatter)

# Консольный хэндлер
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Добавляем хэндлеры
logger.addHandler(file_handler)
logger.addHandler(console_handler)


async def main():
    logging.info("Запуск polling…")
    try:
        await dp.start_polling(bot)
    except TelegramForbiddenError as e:
        logging.error(f"Бот заблокирован пользователем: {e}")
    except Exception:
        logging.exception("Критическая ошибка в polling")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Бот остановлен пользователем")
    except Exception:
        logging.exception("Непредвиденная ошибка")

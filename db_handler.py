# db_handler.py
import pytz
import os
import asyncpg
from asyncpg.exceptions import UniqueViolationError
from datetime import datetime
from dotenv import load_dotenv
from typing import Optional, Dict, Any

# Загружаем DATABASE_URL из .env
load_dotenv()

# Приватный пул подключений
_db_pool: asyncpg.Pool | None = None

async def _get_pool() -> asyncpg.Pool:
    """
    Возвращает единый пул подключений, инициализируется при первом вызове.
    """
    global _db_pool
    if _db_pool is None:
        dsn = os.getenv('DATABASE_URL')
        if not dsn:
            raise RuntimeError("DATABASE_URL is not set")
        _db_pool = await asyncpg.create_pool(dsn)
    return _db_pool

async def save_expense(
    user_id: int,
    chat_id: int,
    username: str,
    category: str,
    subcategory: str,
    price: float
) -> None:
    """
    Добавляет пользователя, категорию, подкатегорию и новую запись в purchases.
    Записывает поле ts как Moscow datetime.
    """
    pool = await _get_pool()
    ts  = datetime.now(pytz.timezone("Europe/Moscow")).replace(tzinfo=None, microsecond=0)


    async with pool.acquire() as conn:
        # 1) users
        await conn.execute("""
            INSERT INTO users (user_id, chat_id, username)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id) DO UPDATE
              SET chat_id = EXCLUDED.chat_id,
                  username = EXCLUDED.username;
        """, user_id, chat_id, username)

        # 2) categories
        await conn.execute("""
            INSERT INTO categories (user_id, name)
            VALUES ($1, $2)
            ON CONFLICT (user_id, name) DO NOTHING;
        """, user_id, category)

        # 3) получить category_id
        row = await conn.fetchrow("""
            SELECT id FROM categories
            WHERE user_id = $1 AND name = $2;
        """, user_id, category)
        category_id = row['id']

        # 4) subcategories
        await conn.execute("""
            INSERT INTO subcategories (user_id, category_id, name)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id, category_id, name) DO NOTHING;
        """, user_id, category_id, subcategory)

        # 5) purchases — записываем ts с датой и временем
        await conn.execute("""
            INSERT INTO purchases (user_id, category, subcategory, price, ts)
            VALUES ($1, $2, $3, $4, $5);
        """, user_id, category, subcategory, price, ts)

async def save_expenses_ph(
    user_id: int,
    chat_id: int,
    username: str,
    items: list[tuple[str, str, float]]
) -> None:
    """
    Сохраняет в БД список позиций чека с их категориями.

    Параметры:
        user_id (int): Telegram user_id
        chat_id (int): Telegram chat_id
        username (str): Telegram username
        items (List[Tuple[category, name, price]]):
            Каждая запись — кортеж (категория, название_товара, цена).
    """
    pool = await _get_pool()
    ts = datetime.now(pytz.timezone("Europe/Moscow")).replace(tzinfo=None, microsecond=0)

    async with pool.acquire() as conn:
        # 1) Пользователь
        await conn.execute(
            """
            INSERT INTO users (user_id, chat_id, username)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id) DO UPDATE
              SET chat_id = EXCLUDED.chat_id,
                  username = EXCLUDED.username;
            """,
            user_id, chat_id, username
        )

        for category, name, price in items:
            # 2) Категория
            await conn.execute(
                """
                INSERT INTO categories (user_id, name)
                VALUES ($1, $2)
                ON CONFLICT (user_id, name) DO NOTHING;
                """,
                user_id, category
            )
            # 3) Получаем id категории
            row = await conn.fetchrow(
                """
                SELECT id FROM categories
                WHERE user_id = $1 AND name = $2;
                """,
                user_id, category
            )
            category_id = row['id']

            # 4) Подкатегория (название товара)
            await conn.execute(
                """
                INSERT INTO subcategories (user_id, category_id, name)
                VALUES ($1, $2, $3)
                ON CONFLICT (user_id, category_id, name) DO NOTHING;
                """,
                user_id, category_id, name
            )

            # 5) Покупка
            await conn.execute(
                """
                INSERT INTO purchases (user_id, category, subcategory, price, ts)
                VALUES ($1, $2, $3, $4, $5);
                """,
                user_id, category, name, price, ts
            )

# старая процедура ниже (убрал с использования)
async def update_last_field(
    user_id: int,
    field: str,
    value: str
) -> bool:
    """
    Обновляет указанное поле (category, subcategory или price) в последней
    записи purchases для данного user_id. При category/subcategory — синхронизирует
    справочники и удаляет дубли. Записывает ts как текущее локальное datetime.
    """
    pool = await _get_pool()
    ts = datetime.now()  # локальное дата-время

    async with pool.acquire() as conn:
        # 1) ищем последнюю запись
        row = await conn.fetchrow("""
            SELECT id, category, subcategory
            FROM purchases
            WHERE user_id = $1
            ORDER BY ts DESC
            LIMIT 1;
        """, user_id)
        if not row:
            return False

        purchase_id = row['id']
        old_cat     = row['category']
        old_subcat  = row['subcategory']

        if field == 'category':
            # обновляем справочник categories
            try:
                await conn.execute("""
                    UPDATE categories
                    SET name = $1
                    WHERE user_id = $2 AND name = $3;
                """, value, user_id, old_cat)
            except asyncpg.exceptions.UniqueViolationError:
                dups = await conn.fetch(
                    "SELECT id FROM categories WHERE user_id=$1::BIGINT AND name=$2 ORDER BY id;",
                    user_id, value
                )
                for dup in dups[1:]:
                    await conn.execute("DELETE FROM categories WHERE id = $1;", dup['id'])

            # обновляем purchases
            await conn.execute("""
                UPDATE purchases
                SET category = $1, ts = $2
                WHERE id = $3;
            """, value, ts, purchase_id)

        elif field == 'subcategory':
            # находим category_id
            cat_row = await conn.fetchrow(
                "SELECT id FROM categories WHERE user_id=$1::BIGINT AND name=$2;",
                user_id, old_cat
            )
            category_id = cat_row['id']

            # обновляем справочник subcategories
            try:
                await conn.execute("""
                    UPDATE subcategories
                    SET name = $1
                    WHERE user_id = $2 AND category_id = $3 AND name = $4;
                """, value, user_id, category_id, old_subcat)
            except asyncpg.exceptions.UniqueViolationError:
                dups = await conn.fetch(
                    "SELECT id FROM subcategories WHERE user_id=$1::BIGINT AND category_id=$2 AND name=$3 ORDER BY id;",
                    user_id, category_id, value
                )
                for dup in dups[1:]:
                    await conn.execute("DELETE FROM subcategories WHERE id = $1;", dup['id'])

            # обновляем purchases
            await conn.execute("""
                UPDATE purchases
                SET subcategory = $1, ts = $2
                WHERE id = $3;
            """, value, ts, purchase_id)

        elif field == 'price':
            price_val = float(value)  # ValueError, если не число
            await conn.execute("""
                UPDATE purchases
                SET price = $1, ts = $2
                WHERE id = $3;
            """, price_val, ts, purchase_id)

        else:
            raise ValueError(f"Unsupported field: {field}")

        return True

async def get_today_purchases(user_id: int):
    pool = await _get_pool()
    sql = """
        SELECT category, subcategory, price, ts
        FROM purchases
        WHERE user_id = $1
          AND (ts AT TIME ZONE 'Europe/Moscow')::date = CURRENT_DATE
        ORDER BY ts;
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, user_id)
    return rows


async def get_user_purchases(user_id: int) -> list[asyncpg.Record]:
    """
    Возвращает список записей из purchases для данного user_id, вызывая хранимую процедуру PostgreSQL.
    Каждая запись содержит поля category, subcategory, price, ts.
    """
    pool = await _get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM get_user_purchases($1);", user_id)
    return rows

async def get_user_categories(user_id: int) -> list[asyncpg.Record]:
    """
    Возвращает список категорий для заданного user_id.
    Каждая запись содержит поля id и name.
    """
    pool = await _get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, name
            FROM categories
            WHERE user_id = $1
            ORDER BY id;
        """, user_id)
    return rows


# Получить доходы пользователя за последние N дней
from datetime import timedelta

async def get_user_incomes_days(user_id: int, days: int) -> list[asyncpg.Record]:
    """
    Возвращает записи из income за последние `days` дней для данного user_id.
    """
    pool = await _get_pool()
    async with pool.acquire() as conn:
        since = datetime.now() - timedelta(days=days)
        rows = await conn.fetch("""
            SELECT source, amount, ts
            FROM income
            WHERE user_id = $1 AND ts >= $2
            ORDER BY ts;
        """, user_id, since)
    return rows

async def save_income(user_id: int, source: str, amount: float) -> None:
    """
    Сохраняет источник дохода и сумму для данного user_id.
    """
    pool = await _get_pool()
    ts = datetime.now(pytz.timezone("Europe/Moscow")).replace(tzinfo=None, microsecond=0)
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO income (user_id, source, amount, ts)
            VALUES ($1, $2, $3, $4);
        """, user_id, source, amount, ts)


async def delete_last_purchase(user_id: int) -> bool:


    pool = await _get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT delete_last_purchase($1);",
            user_id
        )

async def get_last_purchase(user_id: str) -> Optional[Dict[str, Any]]:
    """Возвращает последнюю запись о покупке пользователя"""
    pool = await _get_pool()
    async with pool.acquire() as conn:
        # Конвертируем user_id в int только для передачи в запрос
        try:
            user_id_int = int(user_id)
        except ValueError:
            logger.error(f"Invalid user_id: {user_id}")
            return None
            
        row = await conn.fetchrow("""
            SELECT category, subcategory, price
            FROM purchases
            WHERE user_id = $1
            ORDER BY ts DESC
            LIMIT 1;
        """, user_id_int)
        return dict(row) if row else None

async def update_last_purchase_field(
    user_id: str,
    field: str,
    value: str
) -> bool:
    """Обновляет поле в последней записи покупки (для цены)"""
    pool = await _get_pool()
    ts = datetime.now()

    async with pool.acquire() as conn:
        try:
            user_id_int = int(user_id)
        except ValueError:
            logger.error(f"Invalid user_id: {user_id}")
            return False

        # Получаем ID последней покупки
        purchase_id = await conn.fetchval("""
            SELECT id
            FROM purchases
            WHERE user_id = $1
            ORDER BY ts DESC
            LIMIT 1;
        """, user_id_int)
        
        if not purchase_id:
            return False

        if field == 'price':
            try:
                price_val = float(value)
            except ValueError:
                raise ValueError("Цена должна быть числом")
                
            await conn.execute("""
                UPDATE purchases
                SET price = $1, ts = $2
                WHERE id = $3;
            """, price_val, ts, purchase_id)
            return True
        
        raise ValueError(f"Неподдерживаемое поле: {field}")

async def update_dictionary(
    user_id: str,
    dict_type: str,
    old_name: str,
    new_name: str,
    category_name: Optional[str] = None
) -> bool:
    """Обновляет справочник и связанные покупки"""
    pool = await _get_pool()
    async with pool.acquire() as conn:
        try:
            user_id_int = int(user_id)
        except ValueError:
            logger.error(f"Invalid user_id: {user_id}")
            return False
            
        if dict_type == 'category':
            return await conn.fetchval(
                "SELECT update_category($1::bigint, $2::text, $3::text);",
                user_id_int, old_name, new_name
            )
        elif dict_type == 'subcategory':
            if not category_name:
                raise ValueError("Для подкатегории требуется указать категорию")
            return await conn.fetchval(
                "SELECT update_subcategory($1::bigint, $2::text, $3::text, $4::text);",
                user_id_int, category_name, old_name, new_name
            )
        else:
            raise ValueError(f"Неподдерживаемый тип справочника: {dict_type}")
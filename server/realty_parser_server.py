"""
HTTP API сервер для парсинга недвижимости
Предназначен для использования с Fastify (Node.js)
"""

import asyncio
import logging
from datetime import datetime
import importlib.util
import os
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict

import aiohttp
import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import uvicorn
from dotenv import load_dotenv

from models import (
    FlatReportRequest,
    ParseResponse,
    ParseTextRequest,
    ParseUrlsRequest,
    ReportPreparationRequest,
    SendExcelDocumentRequest,
)
from parser_service import (
    AVITO_AVAILABLE,
    BAZA_WINNER_AVAILABLE,
    EXTENDED_COLLECTOR_AVAILABLE,
    YANDEX_AVAILABLE,
    extract_urls,
    get_property_by_guid,
    parse_properties_batch as parser_parse_properties_batch,
    parse_property as parser_parse_property,
    parse_property_extended as parser_parse_property_extended,
    parser,
)
from report_pipeline import ReportPipeline


class HistoryTableMeta:
    FIELD_ALIASES: dict[str, tuple[str, ...]] = {
        "id": ("id",),
        "ad_id": ("ad_id", "adId"),
        "price": ("price", "price_raw"),
        "status": ("status", "is_active"),
        "views_today": ("views_today", "today_views"),
        "created_at": ("created_at", "recorded_at", "changed_at", "ts"),
    }

    def __init__(self, cursor: RealDictCursor) -> None:
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'users' AND table_name = 'ad_history'
            """
        )
        rows = cursor.fetchall()
        if not rows:
            raise RuntimeError("users.ad_history table not found")
        self._columns = {row["column_name"].lower(): row["column_name"] for row in rows}

    def find_column(self, canonical: str) -> str | None:
        candidates = self.FIELD_ALIASES.get(canonical, (canonical,))
        for candidate in candidates:
            normalized = candidate.lower()
            if normalized in self._columns:
                return self._columns[normalized]
        return None

    def build_columns(self, payload: dict[str, object | None]) -> tuple[list[str], list[object]]:
        columns: list[str] = []
        values: list[object] = []
        for canonical, value in payload.items():
            if value is None:
                continue
            column = self.find_column(canonical)
            if not column:
                continue
            columns.append(column)
            values.append(value)
        return columns, values

logger = logging.getLogger(__name__)
from persistent_browser import get_persistent_browser, start_persistent_browser_thread

# Загружаем переменные из .env файла
load_dotenv()
FLAT_REPORTS_DSN = os.getenv("FLAT_REPORTS_DSN") or os.getenv("DATABASE_URL")
DEFAULT_RADIUS_M = 1500
DEFAULT_ANALOGS_AREA_RATIO = 0.15
DEFAULT_ANALOGS_FLOOR_DELTA = 2
DEFAULT_ANALOGS_DAYS_LIMIT = 30

# Настройки автозапуска persistent браузера
_persistent_browser_flag = os.getenv("PERSISTENT_BROWSER_AUTO_START", "false").strip().lower()
PERSISTENT_BROWSER_AUTO_START = _persistent_browser_flag in {"1", "true", "yes", "on"}

# Загружаем модуль генерации отчётов по необходимости
REPORT_MODULE_AVAILABLE = False
REPORT_MODULE_ERROR = None
fetch_latest_report_json = None
build_flat_report_pdf = None

_report_module_path = Path(__file__).with_name("reportlab.py")
if _report_module_path.exists():
    _spec = importlib.util.spec_from_file_location("flat_report_builder", _report_module_path)
    if _spec and _spec.loader:
        _report_module = importlib.util.module_from_spec(_spec)
        try:
            _spec.loader.exec_module(_report_module)
            fetch_latest_report_json = getattr(_report_module, "fetch_latest_report_json", None)
            build_flat_report_pdf = getattr(_report_module, "build_flat_report_pdf", None)
            REPORT_MODULE_AVAILABLE = callable(fetch_latest_report_json) and callable(build_flat_report_pdf)
        except Exception as exc:  # noqa: BLE001
            REPORT_MODULE_ERROR = str(exc)
            print(f"⚠️ Модуль генерации PDF отчётов недоступен: {exc}")
else:
    REPORT_MODULE_ERROR = "reportlab.py not found"

def _run_flat_report_generation(
    tg_user_id: int,
    house_id: int,
    floor: int,
    rooms: int,
    radius_m: int,
    analogs_area_ratio: float,
    analogs_floor_delta: int,
    analogs_days_limit: int,
) -> None:
    """Вызывает SQL-функции users.build_flat_report и users.build_flat_report_analogs."""
    if not FLAT_REPORTS_DSN:
        raise RuntimeError("FLAT_REPORTS_DSN / DATABASE_URL не настроены")

    main_sql = "SELECT users.build_flat_report(%s, %s, %s, %s, %s);"
    analogs_sql = (
        "SELECT users.build_flat_report_analogs(%s, %s, %s, %s, %s, %s, %s, %s);"
    )

    with psycopg2.connect(FLAT_REPORTS_DSN) as conn, conn.cursor() as cur:
        cur.execute(
            main_sql,
            (tg_user_id, house_id, floor, rooms, radius_m),
        )
        cur.fetchone()
        cur.execute(
            analogs_sql,
            (
                tg_user_id,
                house_id,
                floor,
                rooms,
                radius_m,
                analogs_area_ratio,
                analogs_floor_delta,
                analogs_days_limit,
            ),
        )
        cur.fetchone()


def _positive_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return None
    return normalized if normalized > 0 else None


def _int_or_default(value: Any, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _float_or_default(value: Any, default: float) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _fetch_user_flat(dsn: str, flat_id: int) -> Dict[str, Any]:
    with psycopg2.connect(dsn) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, tg_user_id, house_id, floor, rooms, radius_m,
                   area_ratio, floor_delta, days_limit
            FROM users.user_flats
            WHERE id = %s
            """,
            (flat_id,),
        )
        row = cur.fetchone()
    if not row:
        raise ValueError(f"user_flat id={flat_id} not found")
    return row


def _build_report_parameters(user_flat: Dict[str, Any]) -> Dict[str, Any]:
    tg_user_id = user_flat.get("tg_user_id")
    if tg_user_id is None:
        raise ValueError("tg_user_id отсутствует в user_flat")
    house_id = user_flat.get("house_id")
    if house_id is None:
        raise ValueError("house_id отсутствует в user_flat")
    floor = user_flat.get("floor")
    if floor is None:
        raise ValueError("floor отсутствует в user_flat")
    rooms = user_flat.get("rooms")
    if rooms is None:
        raise ValueError("rooms отсутствует в user_flat")

    radius_m = _positive_int(user_flat.get("radius_m")) or DEFAULT_RADIUS_M
    return {
        "tg_user_id": int(tg_user_id),
        "house_id": int(house_id),
        "floor": int(floor),
        "rooms": int(rooms),
        "radius_m": radius_m,
        "analogs_area_ratio": _float_or_default(
            user_flat.get("area_ratio"), DEFAULT_ANALOGS_AREA_RATIO
        ),
        "analogs_floor_delta": _int_or_default(
            user_flat.get("floor_delta"), DEFAULT_ANALOGS_FLOOR_DELTA
        ),
        "analogs_days_limit": _int_or_default(
            user_flat.get("days_limit"), DEFAULT_ANALOGS_DAYS_LIMIT
        ),
    }

# Запускаем инициализацию persistent браузера в отдельном потоке по настройке
browser_thread = None
if PERSISTENT_BROWSER_AUTO_START:
    browser_thread = start_persistent_browser_thread()

# Создаем FastAPI приложение
app = FastAPI(
    title="Realty Parser HTTP API",
    description="API для парсинга объявлений недвижимости с Avito и Cian",
    version="1.0.0"
)

# Настройка CORS для Fastify
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # В продакшене укажите конкретные домены
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API endpoints
@app.post("/api/parse/urls", response_model=ParseResponse)
async def parse_by_urls(request: ParseUrlsRequest):
    """Парсинг по списку URL"""
    try:
        properties = await parser.parse_properties_batch(request.urls)
        
        return ParseResponse(
            success=True,
            data=properties,
            total=len(properties),
            message=f"Успешно спарсено {len(properties)} из {len(request.urls)} объявлений",
            timestamp=str(datetime.now())
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка парсинга: {str(e)}")

@app.post("/api/parse/text", response_model=ParseResponse)
async def parse_from_text(request: ParseTextRequest):
    """Парсинг объявлений из текста"""
    try:
        # Извлекаем URL из текста
        urls = extract_urls(request.text)
        if not urls:
            raise HTTPException(status_code=400, detail="URL не найдены в тексте")
        
        # Парсим объявления
        properties = await parser.parse_properties_batch(urls)
        
        return ParseResponse(
            success=True,
            data=properties,
            total=len(properties),
            message=f"Извлечено {len(urls)} URL, успешно спарсено {len(properties)} объявлений",
            timestamp=str(datetime.now())
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка парсинга из текста: {str(e)}")

@app.get("/api/parse/single")
async def parse_single_property(url: str):
    """Парсинг одного объявления по URL (быстрый режим)"""
    try:
        property_data = await parser.parse_property(url)
        if property_data:
            return {
                "success": True,
                "data": property_data.to_dict(),
                "message": "Объявление успешно спарсено (быстрый режим)"
            }
        else:
            raise HTTPException(status_code=400, detail="Не удалось спарсить объявление")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка парсинга: {str(e)}")

@app.get("/api/parse/extended")
async def parse_extended_property(url: str):
    """Расширенный парсинг одного объявления по URL (полные данные)"""
    try:
        property_data = await parser.parse_property_extended(url)
        if property_data:
            return {
                "success": True,
                "data": property_data.to_dict(),
                "message": "Объявление успешно спарсено (расширенный режим)"
            }
        else:
            raise HTTPException(status_code=400, detail="Не удалось спарсить объявление")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка расширенного парсинга: {str(e)}")


@app.get("/api/parse/ext")
async def parse_extended_minimal(url: str):
    """Расширенный парсинг без адреса и фотографий."""
    try:
        property_data = await parser.parse_property_extended(url)
        if property_data:
            data = property_data.to_dict()
            data.pop("address", None)
            data.pop("photo_urls", None)
            return {
                "success": True,
                "data": data,
                "message": "Объявление успешно спарсено (минимальный расширенный режим)"
            }
        else:
            raise HTTPException(status_code=400, detail="Не удалось спарсить объявление")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка минимального расширенного парсинга: {str(e)}")


@app.get("/api/parse/flat_state")
async def parse_flat_state(url: str):
    """Парсинг статуса и цены объявления."""
    try:
        property_data = await parser.parse_property_flat_state(url)
        if property_data:
            data = property_data.to_dict()
            return {
                "success": True,
                "data": {
                    "price": data.get("price"),
                    "status": data.get("status"),
                    "views_today": data.get("views_today"),
                },
                "message": "Статус и цена успешно спарсены"
            }
        else:
            raise HTTPException(status_code=400, detail="Не удалось спарсить объявление")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка парсинга статуса квартиры: {str(e)}")

@app.get("/api/health")
async def health_check():
    """Проверка состояния API"""
    # Проверяем статус persistent браузера
    browser_status = "unknown"
    try:
        browser = get_persistent_browser()
        session_info = browser.get_session_info()
        browser_status = session_info.get('status', 'unknown')
    except:
        browser_status = "error"

    return {
        "status": "healthy",
        "service": "realty-parser-api",
        "avito_available": AVITO_AVAILABLE,
        "cian_available": True,
        "yandex_available": YANDEX_AVAILABLE,
        "baza_winner_available": BAZA_WINNER_AVAILABLE,
        "extended_collector_available": EXTENDED_COLLECTOR_AVAILABLE,
        "persistent_browser": browser_status,
        "reports_available": REPORT_MODULE_AVAILABLE,
    }

@app.post("/api/send-excel-document")
async def send_excel_document(request: SendExcelDocumentRequest):
    """Создание и отправка Excel документа в Telegram"""
    try:
        import pandas as pd
        import io

        # Проверяем наличие Bot Token в переменных окружения
        bot_token = os.getenv('API_TOKEN')

        if not bot_token:
            return {
                "success": False,
                "message": "Токен бота не настроен",
                "user_id": request.user_id,
                "filename": request.filename,
                "note": "Добавьте API_TOKEN в .env файл для отправки"
            }

        # Проверяем данные
        if not request.excel_data:
            raise HTTPException(status_code=400, detail="Нет данных для создания Excel файла")

        # Создаем DataFrame из данных
        df = pd.DataFrame(request.excel_data)

        # Создаем Excel файл в памяти
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Квартиры')

            # Автоматически подгоняем ширину колонок
            worksheet = writer.sheets['Квартиры']
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter

                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass

                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width

        excel_buffer.seek(0)
        file_content = excel_buffer.getvalue()

        # Отправляем через Telegram Bot API
        url = f"https://api.telegram.org/bot{bot_token}/sendDocument"

        # Создаем form data для отправки
        data = aiohttp.FormData()
        data.add_field('chat_id', request.user_id)
        data.add_field('caption', request.caption)
        data.add_field('document', file_content, filename=request.filename)

        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=data) as response:
                result = await response.json()

                if response.status == 200 and result.get('ok'):
                    return {
                        "success": True,
                        "message": f"Excel файл {request.filename} успешно отправлен пользователю {request.user_id}",
                        "user_id": request.user_id,
                        "filename": request.filename,
                        "file_size": len(file_content),
                        "rows_count": len(request.excel_data),
                        "caption": request.caption,
                        "telegram_response": result
                    }
                else:
                    raise Exception(f"Telegram API error: {result}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка создания и отправки Excel: {str(e)}")


@app.post("/api/reports/flat")
async def create_flat_report(request: FlatReportRequest):
    """Генерация PDF-отчёта по квартире."""
    if not REPORT_MODULE_AVAILABLE or not fetch_latest_report_json or not build_flat_report_pdf:
        raise HTTPException(
            status_code=503,
            detail=f"Генератор отчётов недоступен: {REPORT_MODULE_ERROR or 'reportlab не установлен'}",
        )
    if not FLAT_REPORTS_DSN:
        raise HTTPException(status_code=500, detail="Переменная FLAT_REPORTS_DSN или DATABASE_URL не настроена")

    loop = asyncio.get_event_loop()

    def _load_user_flat():
        return _fetch_user_flat(FLAT_REPORTS_DSN, request.flat_id)

    try:
        user_flat = await loop.run_in_executor(None, _load_user_flat)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Ошибка получения данных user_flat: {exc}")

    if user_flat.get("house_id") is None:
        pipeline = ReportPipeline(FLAT_REPORTS_DSN)

        def _prepare_flat():
            return pipeline.prepare(request.flat_id, False)

        try:
            await loop.run_in_executor(None, _prepare_flat)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except psycopg2.Error as exc:
            raise HTTPException(status_code=500, detail=f"Ошибка базы данных: {exc}")
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=f"Не удалось подготовить данные для user_flat: {exc}")

        try:
            user_flat = await loop.run_in_executor(None, _load_user_flat)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=f"Ошибка получения данных user_flat: {exc}")

    if user_flat.get("house_id") is None:
        raise HTTPException(status_code=500, detail="Не удалось определить house_id для user_flat")

    try:
        report_values = _build_report_parameters(user_flat)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    if request.regenerate:
        def _build_db_snapshot():
            return _run_flat_report_generation(
                report_values["tg_user_id"],
                report_values["house_id"],
                report_values["floor"],
                report_values["rooms"],
                report_values["radius_m"],
                report_values["analogs_area_ratio"],
                report_values["analogs_floor_delta"],
                report_values["analogs_days_limit"],
            )

        try:
            await loop.run_in_executor(None, _build_db_snapshot)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=f"Ошибка подготовки отчёта в БД: {exc}")

    def _fetch():
        return fetch_latest_report_json(
            dsn=FLAT_REPORTS_DSN,
            tg_user_id=report_values["tg_user_id"],
            house_id=report_values["house_id"],
            floor=report_values["floor"],
            rooms=report_values["rooms"],
            radius_m=report_values["radius_m"],
            report_date=request.report_date,
        )

    try:
        report_json = await loop.run_in_executor(None, _fetch)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Ошибка получения исходных данных: {exc}")

    timestamp_suffix = datetime.now().strftime("%Y%m%d%H%M%S")
    output_path = request.output_path or os.path.join(
        "/tmp",
        f"flat_report_{report_values['house_id']}_{report_values['floor']}_{report_values['rooms']}_{timestamp_suffix}.pdf",
    )
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    def _build():
        return build_flat_report_pdf(report_json, output_path)

    try:
        pdf_path = await loop.run_in_executor(None, _build)
        file_size = os.path.getsize(pdf_path)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Ошибка генерации PDF: {exc}")

    response_params = {
        "flat_id": request.flat_id,
        "report_date": request.report_date,
        "regenerate": request.regenerate,
        "tg_user_id": report_values["tg_user_id"],
        "house_id": report_values["house_id"],
        "floor": report_values["floor"],
        "rooms": report_values["rooms"],
        "radius_m": report_values["radius_m"],
        "analogs_area_ratio": report_values["analogs_area_ratio"],
        "analogs_floor_delta": report_values["analogs_floor_delta"],
        "analogs_days_limit": report_values["analogs_days_limit"],
        "output_path": output_path,
    }

    return {
        "success": True,
        "message": "PDF-отчёт сформирован",
        "pdf_path": pdf_path,
        "file_size": file_size,
        "params": response_params,
    }


@app.post("/api/reports/prepare")
async def prepare_report_data(request: ReportPreparationRequest):
    """Собирает объявления вокруг user_flat и сохраняет их в users.ads."""
    if not FLAT_REPORTS_DSN:
        raise HTTPException(status_code=500, detail="FLAT_REPORTS_DSN или DATABASE_URL не настроены")

    pipeline = ReportPipeline(FLAT_REPORTS_DSN)

    if request.flat_id is not None:
        try:
            result = await asyncio.to_thread(
                pipeline.prepare,
                request.flat_id,
                request.run_parser,
                request.max_history,
                request.max_nearby,
                request.radius_m,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except psycopg2.Error as exc:
            raise HTTPException(status_code=500, detail=f"Ошибка базы данных: {exc}")
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=f"Не удалось подготовить данные: {exc}")

        return {
            "success": True,
            "message": "Данные для отчёта подготовлены",
            "result": result,
        }

    flat_ids = await asyncio.to_thread(_fetch_flats_missing_house_id, FLAT_REPORTS_DSN)
    if not flat_ids:
        return {
            "success": True,
            "message": "Нет user_flats с house_id NULL для обработки",
            "processed_flats": [],
            "errors": [],
        }

    processed: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for flat_id in flat_ids:
        try:
            result = await asyncio.to_thread(
                pipeline.prepare,
                flat_id,
                request.run_parser,
                request.max_history,
                request.max_nearby,
                request.radius_m,
            )
            processed.append({"flat_id": flat_id, "result": result})
        except ValueError as exc:
            errors.append({"flat_id": flat_id, "error": str(exc)})
        except psycopg2.Error as exc:
            errors.append({"flat_id": flat_id, "error": f"Ошибка базы данных: {exc}"})
        except Exception as exc:  # noqa: BLE001
            errors.append({"flat_id": flat_id, "error": f"Не удалось подготовить данные: {exc}"})

    return {
        "success": True,
        "message": f"Обработано {len(processed)} из {len(flat_ids)} user_flats с house_id NULL",
        "processed_flats": processed,
        "errors": errors,
    }


def _fetch_active_ads(dsn: str) -> list[dict[str, Any]]:
    with psycopg2.connect(dsn) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT id, url, price, status, views_today FROM users.ads WHERE status = TRUE AND url IS NOT NULL"
        )
        return cur.fetchall()


def _fetch_flats_missing_house_id(dsn: str) -> list[int]:
    with psycopg2.connect(dsn) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id FROM users.user_flats WHERE house_id IS NULL")
        rows = cur.fetchall()
        return [row["id"] for row in rows]


async def _parse_flat_state_via_api(session: aiohttp.ClientSession, url: str) -> tuple[dict[str, Any] | None, str | None]:
    try:
        async with session.get(
            f"{REPORT_PARSER_BASE_URL}/api/parse/flat_state",
            params={"url": url},
            timeout=aiohttp.ClientTimeout(total=25),
        ) as response:
            if response.status != 200:
                text = await response.text()
                return None, f"status={response.status}; body={text[:200]}"
            payload = await response.json()
            data = payload.get("data")
            if not isinstance(data, dict):
                return None, "invalid payload"
            return data, None
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to fetch /api/parse/flat_state for %s: %s", url, exc)
        return None, str(exc)


def _detect_ad_change(ad: dict[str, Any], parsed: dict[str, Any]) -> dict[str, Any] | None:
    new_price = _to_decimal(parsed.get("price"))
    old_price = _to_decimal(ad.get("price"))
    price_changed = new_price is not None and old_price != new_price
    status_value = parsed.get("status")
    status_changed = status_value is not None and bool(status_value) != bool(ad.get("status"))

    if not price_changed and not status_changed:
        return None

    change: dict[str, Any] = {
        "ad_id": ad["id"],
        "url": ad["url"],
        "checked_at": datetime.utcnow(),
        "views_today": _to_int(parsed.get("views_today")),
        "price_changed": price_changed,
        "status_changed": status_changed,
    }

    change["price"] = new_price if price_changed else None
    change["status"] = bool(status_value) if status_changed else None
    return change


def _persist_flats_state_changes(dsn: str, changes: list[dict[str, Any]]) -> None:
    with psycopg2.connect(dsn) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        history_meta = HistoryTableMeta(cur)
        for change in changes:
            set_fields: list[str] = []
            set_values: list[Any] = []
            if change.get("price") is not None:
                set_fields.append("price")
                set_values.append(change["price"])
            if change.get("status") is not None:
                set_fields.append("status")
                set_values.append(change["status"])
            if change.get("views_today") is not None:
                set_fields.append("views_today")
                set_values.append(change["views_today"])

            if set_fields:
                set_clause = sql.SQL(", ").join(
                    sql.SQL("{} = %s").format(sql.Identifier(field)) for field in set_fields
                )
                cur.execute(
                    sql.SQL("UPDATE users.ads SET {set_clause} WHERE id = %s").format(
                        set_clause=set_clause
                    ),
                    (*set_values, change["ad_id"]),
                )

            history_payload = {
                "ad_id": change["ad_id"],
                "price": change.get("price"),
                "status": change.get("status"),
                "views_today": change.get("views_today"),
                "created_at": change["checked_at"],
            }
            columns, values = history_meta.build_columns(history_payload)
            if not columns:
                continue
            insert_stmt = sql.SQL(
                "INSERT INTO users.ad_history ({cols}) VALUES ({vals})"
            ).format(
                cols=sql.SQL(", ").join(sql.Identifier(col) for col in columns),
                vals=sql.SQL(", ").join(sql.Placeholder() for _ in columns),
            )
            cur.execute(insert_stmt, values)
        conn.commit()


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


@app.post("/api/reports/flats_state")
async def refresh_flats_state():
    """Проверяет актуальность всех активных объявлений и записывает историю изменений."""
    if not FLAT_REPORTS_DSN:
        raise HTTPException(status_code=500, detail="FLAT_REPORTS_DSN или DATABASE_URL не настроены")

    loop = asyncio.get_running_loop()
    try:
        ads = await loop.run_in_executor(None, _fetch_active_ads, FLAT_REPORTS_DSN)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Ошибка чтения объявлений: {exc}")

    if not ads:
        return {
            "success": True,
            "checked": 0,
            "updated": 0,
            "changes": [],
            "errors": [],
        }

    changes: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    timeout = aiohttp.ClientTimeout(total=25)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for ad in ads:
            data, error = await _parse_flat_state_via_api(session, ad["url"])
            if data is None:
                errors.append({"url": ad["url"], "reason": error or "parse_failed"})
                continue
            change = _detect_ad_change(ad, data)
            if change:
                changes.append(change)

    if changes:
        await loop.run_in_executor(None, _persist_flats_state_changes, FLAT_REPORTS_DSN, changes)

    return {
        "success": True,
        "checked": len(ads),
        "updated": len(changes),
        "changes": [
            {
                "ad_id": change["ad_id"],
                "url": change["url"],
                "price_changed": change["price_changed"],
                "status_changed": change["status_changed"],
            }
            for change in changes
        ],
        "errors": errors,
    }


@app.get("/api/browser/status")
async def browser_status():
    """Проверка статуса persistent браузера"""
    try:
        browser = get_persistent_browser()
        session_info = browser.get_session_info()
        return {
            "success": True,
            "browser_session": session_info,
            "message": "Статус браузера получен"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "message": "Ошибка получения статуса браузера"
        }

@app.post("/api/browser/init")
async def init_browser():
    """Принудительная инициализация persistent браузера"""
    try:
        browser = get_persistent_browser()
        if browser.setup_browser():
            session_info = browser.get_session_info()
            return {
                "success": True,
                "browser_session": session_info,
                "message": "Браузер успешно инициализирован"
            }
        else:
            return {
                "success": False,
                "message": "Не удалось инициализировать браузер"
            }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "message": "Ошибка инициализации браузера"
        }

@app.post("/api/browser/refresh")
async def refresh_browser():
    """Обновление сессии persistent браузера"""
    try:
        browser = get_persistent_browser()
        if browser.refresh_session():
            session_info = browser.get_session_info()
            return {
                "success": True,
                "browser_session": session_info,
                "message": "Сессия браузера обновлена"
            }
        else:
            return {
                "success": False,
                "message": "Не удалось обновить сессию браузера"
            }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "message": "Ошибка обновления сессии браузера"
        }

@app.get("/api/property/guid/{guid}")
async def get_property_by_guid_endpoint(guid: str):
    """Получение данных квартиры по GUID в формате ЦИАН"""
    if not EXTENDED_COLLECTOR_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Сервис получения данных по GUID недоступен"
        )

    try:
        property_data = await get_property_by_guid(guid)

        if property_data:
            return {
                "success": True,
                "data": property_data,
                "message": "Объявление успешно спарсено по GUID"
            }
        else:
            raise HTTPException(
                status_code=404,
                detail=f"Объявление с GUID {guid} не найдено"
            )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Ошибка получения данных по GUID: {str(e)}"
        )

@app.get("/api/sources")
async def get_supported_sources():
    """Получение списка поддерживаемых источников"""
    return {
        "sources": [
            {
                "name": "Avito",
                "domain": "avito.ru",
                "available": AVITO_AVAILABLE,
                "source_id": "avito"
            },
            {
                "name": "Cian",
                "domain": "cian.ru",
                "available": True,
                "source_id": "cian"
            },
            {
                "name": "Yandex Realty",
                "domain": "realty.yandex.ru",
                "available": YANDEX_AVAILABLE,
                "source_id": "yandex"
            },
            {
                "name": "Baza Winner (GUID)",
                "domain": "baza-winner.ru",
                "available": EXTENDED_COLLECTOR_AVAILABLE,
                "source_id": "baza_winner_guid"
            }
        ]
    }

@app.get("/api/docs")
async def api_documentation():
    """Документация API"""
    return {
        "title": "Realty Parser HTTP API",
        "version": "1.0.0",
        "description": "API для парсинга объявлений недвижимости с Avito, Cian и Yandex Realty",
        "base_url": "http://localhost:8008",
        "endpoints": {
            "monitoring": {
                "GET /api/health": {
                    "description": "Проверка состояния API и persistent браузера",
                    "response": {
                        "status": "healthy",
                        "service": "realty-parser-api",
                        "avito_available": True,
                        "cian_available": True,
                        "yandex_available": True,
                        "persistent_browser": "active"
                    }
                },
                "GET /api/sources": {
                    "description": "Список поддерживаемых источников",
                    "response": {
                        "sources": [
                            {"name": "Avito", "domain": "avito.ru", "available": True}
                        ]
                    }
                }
            },
            "parsing": {
                "GET /api/parse/single": {
                    "description": "Быстрый парсинг одного объявления (из заголовка)",
                    "parameters": {
                        "url": "URL объявления (обязательный)"
                    },
                    "example": "/api/parse/single?url=https://www.avito.ru/moskva/kvartiry/1-k._kvartira_295_m_25_et._7627589190",
                    "response": {
                        "success": True,
                        "data": {
                            "rooms": 1,
                            "total_area": 29.5,
                            "floor": "3",
                            "total_floors": 5,
                            "price": 9000000,
                            "source": "avito",
                            "status": True
                        },
                        "message": "Объявление успешно спарсено (быстрый режим)"
                    }
                },
                "GET /api/parse/extended": {
                    "description": "Расширенный парсинг с полными данными",
                    "parameters": {
                        "url": "URL объявления (обязательный)"
                    },
                    "example": "/api/parse/extended?url=https://www.avito.ru/moskva/kvartiry/1-k._kvartira_295_m_25_et._7627589190",
                    "response": {
                        "success": True,
                        "data": {
                            "rooms": 1,
                            "price": 9000000,
                            "total_area": 29.5,
                            "address": "Москва, 3-я Рыбинская ул., 30",
                            "metro_station": "Митьково",
                            "description": "...",
                            "source": "avito"
                        },
                        "message": "Объявление успешно спарсено (расширенный режим)"
                    }
                },
                "GET /api/parse/ext": {
                    "description": "Упрощённый расширенный парсинг – без адреса и фото (легковесный JSON)",
                    "parameters": {
                        "url": "URL объявления (обязательный)"
                    },
                    "example": "/api/parse/ext?url=https://www.cian.ru/sale/flat/322328152/",
                    "response": {
                        "success": True,
                        "data": {
                            "rooms": 1,
                            "price": 9000000,
                            "total_area": 29.5,
                            "metro_station": "Митьково",
                            "description": "...",
                            "source": "avito"
                        },
                        "message": "Объявление успешно спарсено (упрощённый расширенный режим)"
                    }
                },
                "GET /api/parse/flat_state": {
                    "description": "Быстрый парсинг статуса и цены страницы (для мониторинга объявлений)",
                    "parameters": {
                        "url": "URL объявления (обязательный)"
                    },
                    "example": "/api/parse/flat_state?url=https://www.cian.ru/sale/flat/320454077/",
                    "response": {
                        "success": True,
                        "data": {
                            "price": 12000000,
                            "status": false,
                            "views_today": 2
                        },
                        "message": "Статус и цена успешно спарсены"
                    }
                },
                "POST /api/parse/urls": {
                    "description": "Пакетный парсинг списка URL",
                    "body": {
                        "urls": ["url1", "url2", "..."]
                    },
                    "response": {
                        "success": True,
                        "data": ["array of PropertyData objects"],
                        "total": 2,
                        "message": "Успешно спарсено 2 из 2 объявлений"
                    }
                },
                "POST /api/parse/text": {
                    "description": "Парсинг URL из текста",
                    "body": {
                        "text": "Текст с URL объявлений"
                    },
                    "response": {
                        "success": True,
                        "data": ["array of PropertyData objects"],
                        "total": 1,
                        "message": "Извлечено 1 URL, успешно спарсено 1 объявлений"
                    }
                },
                "GET /api/property/guid/{guid}": {
                    "description": "Получение данных квартиры по GUID в формате ЦИАН",
                    "parameters": {
                        "guid": "GUID объявления из Baza Winner (обязательный)"
                    },
                    "example": "/api/property/guid/0D5F135E-7791-0000-0FA2-005B7F230000",
                    "response": {
                        "success": True,
                        "data": {
                            "URL": "https://www.cian.ru/sale/flat/322177282/",
                            "Комнат": 1,
                            "Цена_raw": 6750000,
                            "Общая площадь": 19.1,
                            "Этаж": 1,
                            "Всего этажей": 5,
                            "Материал стен": "монолитный",
                            "Адрес": "Москва г., Ирининский 2-й пер., 4",
                            "Минут метро": "11 Бауманская м.",
                            "Описание": "Комната, назначение: жилое...",
                            "GUID": "0D5F135E-7791-0000-0FA2-005B7F230000"
                        },
                        "guid": "0D5F135E-7791-0000-0FA2-005B7F230000",
                        "format": "cian_compatible",
                        "message": "Данные объявления успешно получены по GUID"
                    }
                }
            },
            "browser_management": {
                "GET /api/browser/status": {
                    "description": "Подробный статус persistent браузера",
                    "response": {
                        "success": True,
                        "browser_session": {
                            "status": "active",
                            "url": "https://www.avito.ru/",
                            "title": "Авито",
                            "session_age_minutes": 15.5,
                            "is_on_avito": True
                        },
                        "message": "Статус браузера получен"
                    }
                },
                "POST /api/browser/init": {
                    "description": "Принудительная инициализация браузера",
                    "response": {
                        "success": True,
                        "browser_session": {"status": "active"},
                        "message": "Браузер успешно инициализирован"
                    }
                },
                "POST /api/browser/refresh": {
                    "description": "Обновление сессии браузера",
                    "response": {
                        "success": True,
                        "browser_session": {"status": "active"},
                        "message": "Сессия браузера обновлена"
                    }
                }
            },
            "reports": {
                "POST /api/reports/flat": {
                    "description": "Генерация PDF-отчёта по user_flat; параметры берутся из users.user_flats, а при отсутствии house_id автоматически запускается подготовка",
                    "body": {
                        "flat_id": 77,
                        "report_date": "2024-01-01 (опционально)",
                        "output_path": "/tmp/custom.pdf (опционально)",
                        "regenerate": true
                    },
                    "response": {
                        "success": True,
                        "pdf_path": "/tmp/flat_report_92207_7_2_20240101010101.pdf",
                        "message": "PDF-отчёт сформирован",
                        "file_size": 123456,
                        "params": {
                            "flat_id": 77,
                            "tg_user_id": 123456789,
                            "house_id": 92207,
                            "floor": 7,
                            "rooms": 2,
                            "radius_m": 1500,
                            "analogs_area_ratio": 0.15,
                            "analogs_floor_delta": 2,
                            "analogs_days_limit": 30,
                            "report_date": "2024-01-01",
                            "output_path": "/tmp/flat_report_92207_7_2_20240101010101.pdf",
                            "regenerate": true
                        }
                    }
                },
                "POST /api/reports/prepare": {
                    "description": "Формирует users.ads по user_flat (дом + близлежащие объявления)",
                    "body": {
                        "flat_id": 77,
                        "radius_m": 1000,
                        "max_history": 3,
                        "max_nearby": 20,
                        "run_parser": true
                    },
                    "response": {
                        "success": true,
                        "message": "Данные для отчета подготовлены",
                        "result": {
                            "flat_id": 77,
                            "tg_user_id": 123456789,
                            "house_id": 92207,
                            "radius_m": 1000,
                            "history_ads": 1,
                            "nearby_ads": 20,
                            "prepared_ads": 21,
                            "persisted_ads": 21,
                            "parsed_ads": ["https://www.cian.ru/sale/flat/123/"],
                            "parser_errors": []
                        }
                    }
                },
                "POST /api/reports/flats_state": {
                    "description": "Проверяет статус/цену активных объявлений и сохраняет дельту в users.ad_history",
                    "response": {
                        "success": true,
                        "checked": 42,
                        "updated": 3,
                        "changes": [
                            {
                                "ad_id": 123,
                                "url": "https://www.cian.ru/sale/flat/320454077/",
                                "price_changed": true,
                                "status_changed": false
                            }
                        ],
                        "errors": []
                    }
                }
            }
        },
        "data_structure": {
            "PropertyData": {
                "description": "Структура данных объявления",
                "fields": {
                    "rooms": "Количество комнат (int, 0 для студий)",
                    "price": "Цена в рублях (float)",
                    "total_area": "Общая площадь в м² (float)",
                    "living_area": "Жилая площадь в м² (float)",
                    "kitchen_area": "Площадь кухни в м² (float)",
                    "floor": "Этаж (string)",
                    "total_floors": "Этажей в доме (int)",
                    "bathroom": "Тип санузла (string)",
                    "balcony": "Балкон/лоджия (string)",
                    "renovation": "Тип ремонта (string)",
                    "construction_year": "Год постройки (int)",
                    "house_type": "Тип дома (string)",
                    "ceiling_height": "Высота потолков в м (float)",
                    "furniture": "Мебель (string)",
                    "address": "Адрес (string)",
                    "metro_station": "Ближайшая станция метро (string)",
                    "metro_time": "Время до метро в минутах (int)",
                    "metro_way": "Способ добраться до метро (string)",
                    "tags": "Метки объявления (array of strings)",
                    "description": "Описание (string)",
                    "photo_urls": "Ссылки на фото (array of strings)",
                    "source": "Источник: avito/cian/yandex (string)",
                    "url": "URL объявления (string)",
                    "status": "Активность объявления (boolean)",
                    "views_today": "Просмотров сегодня (int)"
                }
            }
        },
        "features": {
            "fast_parsing": "Быстрый парсинг из заголовка (3-5 сек)",
            "persistent_browser": "Постоянный браузер с cookies (вручную через /api/browser/init или PERSISTENT_BROWSER_AUTO_START)",
            "multiple_sources": "Поддержка Avito, Cian, Yandex Realty",
            "batch_processing": "Пакетная обработка множества URL",
            "auto_extraction": "Автоматическое извлечение URL из текста"
        },
        "performance": {
            "fast_mode": "3-5 секунд (только заголовок)",
            "extended_mode": "10-30 секунд (полные данные)",
            "memory_usage": "~435 MB (persistent браузер)",
            "concurrent_requests": "Поддерживается"
        },
        "configuration": {
            "PERSISTENT_BROWSER_AUTO_START": {
                "description": "Автоматически запускать persistent браузер при старте сервера",
                "default": False,
                "values": ["true", "1", "yes", "on"]
            }
        }
    }

@app.get("/", response_class=HTMLResponse)
async def api_docs_html():
    """HTML документация API"""
    return """
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Realty Parser API - Документация</title>
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #333; max-width: 1200px; margin: 0 auto; padding: 20px; }
            h1, h2, h3 { color: #2c3e50; }
            .endpoint { background: #f8f9fa; border-left: 4px solid #007bff; padding: 15px; margin: 10px 0; border-radius: 5px; }
            .method { display: inline-block; padding: 4px 8px; border-radius: 3px; font-weight: bold; color: white; font-size: 12px; }
            .get { background: #28a745; }
            .post { background: #007bff; }
            code { background: #f1f1f1; padding: 2px 4px; border-radius: 3px; font-family: 'Monaco', 'Consolas', monospace; }
            .example { background: #e9ecef; padding: 10px; border-radius: 5px; overflow-x: auto; }
            .status { display: inline-block; padding: 2px 6px; border-radius: 3px; font-size: 11px; }
            .active { background: #d4edda; color: #155724; }
            .fast { background: #fff3cd; color: #856404; }
            .extended { background: #f8d7da; color: #721c24; }
            .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px; margin-bottom: 30px; text-align: center; }
            .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin: 20px 0; }
            .card { background: white; border: 1px solid #e1e5e9; border-radius: 8px; padding: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .badge { display: inline-block; padding: 3px 8px; background: #e9ecef; border-radius: 12px; font-size: 12px; margin: 2px; }
            .success { color: #28a745; }
            .warning { color: #ffc107; }
            .error { color: #dc3545; }
        </style>
    </head>
    <body>
        <div class="header">
            <h1>🏠 Realty Parser API</h1>
            <p>HTTP API для парсинга объявлений недвижимости</p>
            <p><strong>Поддерживает:</strong> Avito, Cian, Yandex Realty</p>
        </div>

        <div class="grid">
            <div class="card">
                <h3>🚀 Быстрый старт</h3>
                <div class="example">
                    <strong>Проверка статуса:</strong><br>
                    <code>GET /api/health</code><br><br>
                    <strong>Быстрый парсинг:</strong><br>
                    <code>GET /api/parse/single?url=https://www.avito.ru/...</code><br><br>
                    <strong>Статус браузера:</strong><br>
                    <code>GET /api/browser/status</code>
                </div>
            </div>

            <div class="card">
                <h3>📊 Производительность</h3>
                <span class="badge fast">Быстрый режим: 3-5 сек</span><br>
                <span class="badge extended">Расширенный: 10-30 сек</span><br>
                <span class="badge">Память: ~435 MB</span><br>
                <span class="badge active">Persistent браузер</span>
            </div>
        </div>

        <h2>📡 Endpoints</h2>

        <h3>🔍 Мониторинг</h3>
        <div class="endpoint">
            <span class="method get">GET</span> <code>/api/health</code>
            <p>Проверка состояния API и persistent браузера</p>
        </div>

        <div class="endpoint">
            <span class="method get">GET</span> <code>/api/sources</code>
            <p>Список поддерживаемых источников</p>
        </div>

        <h3>🏠 Парсинг объявлений</h3>
        <div class="endpoint">
            <span class="method get">GET</span> <code>/api/parse/single?url={URL}</code>
            <p><strong>Быстрый парсинг</strong> - извлекает основные данные из заголовка (3-5 сек)</p>
            <div class="example">
                <strong>Возвращает:</strong> комнаты, площадь, этаж, цену (для Avito)
            </div>
        </div>

        <div class="endpoint">
            <span class="method get">GET</span> <code>/api/parse/extended?url={URL}</code>
            <p><strong>Расширенный парсинг</strong> - полные данные со страницы (10-30 сек)</p>
            <div class="example">
                <strong>Возвращает:</strong> все поля + адрес, метро, описание, фото
            </div>
        </div>

        <div class="endpoint">
            <span class="method get">GET</span> <code>/api/parse/ext?url={URL}</code>
            <p><strong>Лёгкий расширенный парсинг</strong> - то же, но без адреса и фото</p>
            <div class="example">
                <strong>Возвращает:</strong> подробные поля, но без адреса и ссылок на фото
            </div>
        </div>

        <div class="endpoint">
            <span class="method get">GET</span> <code>/api/parse/flat_state?url={URL}</code>
            <p><strong>Статус и цена</strong> - минимальный запрос, возвращающий только `price`, `status` и `views_today`.</p>
            <div class="example">
                <strong>Используем для:</strong> `/api/reports/flats_state`, регулярных проверок жизнеспособности объявления.
            </div>
        </div>

        <div class="endpoint">
            <span class="method post">POST</span> <code>/api/parse/urls</code>
            <p><strong>Пакетный парсинг</strong> списка URL</p>
            <div class="example">
                <strong>Body:</strong> <code>{"urls": ["url1", "url2", ...]}</code>
            </div>
        </div>

        <div class="endpoint">
            <span class="method post">POST</span> <code>/api/parse/text</code>
            <p><strong>Парсинг из текста</strong> - автоматически извлекает URL</p>
            <div class="example">
                <strong>Body:</strong> <code>{"text": "Текст с URL объявлений"}</code>
            </div>
        </div>

        <div class="endpoint">
            <span class="method get">GET</span> <code>/api/property/guid/{guid}</code>
            <p><strong>Данные по GUID</strong> - получает детальную информацию из Baza Winner по GUID в формате ЦИАН</p>
            <div class="example">
                <strong>Возвращает:</strong> полные данные квартиры в совместимом с ЦИАН формате
            </div>
        </div>

        <h3>🔧 Управление браузером</h3>
        <div class="endpoint">
            <span class="method get">GET</span> <code>/api/browser/status</code>
            <p>Подробный статус persistent браузера</p>
        </div>

        <div class="endpoint">
            <span class="method post">POST</span> <code>/api/browser/init</code>
            <p>Принудительная инициализация браузера</p>
        </div>

        <div class="endpoint">
            <span class="method post">POST</span> <code>/api/browser/refresh</code>
            <p>Обновление сессии браузера</p>
        </div>

        <h3>📝 Отчёты</h3>
        <div class="endpoint">
            <span class="method post">POST</span> <code>/api/reports/flat</code>
            <p>Формирует PDF-отчёт по квартире из таблицы <code>users.flat_reports</code></p>
            <div class="example">
                <strong>Body:</strong><br>
                <code>{
  "tg_user_id": 123456789,
  "house_id": 92207,
  "floor": 7,
  "rooms": 2,
  "radius_m": 1500,
  "regenerate": true,
  "analogs_area_ratio": 0.15,
  "analogs_floor_delta": 2,
  "analogs_days_limit": 30
}</code><br><br>
                <strong>Response:</strong><br>
                <code>{"success": true, "pdf_path": "/tmp/flat_report_92207_7_2_20240101010101.pdf", "file_size": 123456}</code>
            </div>
            <p>При <code>regenerate=true</code> перед генерацией вызываются SQL-функции <code>users.build_flat_report</code> и <code>users.build_flat_report_analogs</code> (используется <code>FLAT_REPORTS_DSN</code>/<code>DATABASE_URL</code>).</p>
        </div>

        <div class="endpoint">
            <span class="method post">POST</span> <code>/api/reports/flats_state</code>
            <p>Перепроверяет все <code>users.ads</code> со <code>status=true</code>, парсит каждую ссылку и сохраняет дельту (цена/статус) в <code>users.ad_history</code>.</p>
            <div class="example">
                <strong>Response:</strong><br>
                <code>{"success":true,"checked":42,"updated":3,"changes":[{"ad_id":123,"price_changed":true,"status_changed":false}],"errors":[]}</code>
            </div>
        </div>

        <h2>📋 Структура данных</h2>
        <div class="card">
            <h3>PropertyData</h3>
            <div class="grid">
                <div>
                    <strong>Основные поля:</strong><br>
                    <span class="badge">rooms</span> - количество комнат<br>
                    <span class="badge">price</span> - цена в рублях<br>
                    <span class="badge">total_area</span> - общая площадь<br>
                    <span class="badge">floor</span> - этаж<br>
                    <span class="badge">source</span> - avito/cian/yandex<br>
                    <span class="badge">status</span> - активность объявления
                </div>
                <div>
                    <strong>Дополнительные:</strong><br>
                    <span class="badge">address</span> - адрес<br>
                    <span class="badge">metro_station</span> - метро<br>
                    <span class="badge">description</span> - описание<br>
                    <span class="badge">photo_urls</span> - фотографии<br>
                    <span class="badge">renovation</span> - ремонт<br>
                    <span class="badge">house_type</span> - тип дома
                </div>
            </div>
        </div>

        <h2>🌟 Особенности</h2>
        <div class="grid">
            <div class="card">
                <h3>⚡ Быстрый парсинг</h3>
                <p>Извлечение данных из заголовка страницы за 3-5 секунд</p>
            </div>
            <div class="card">
                <h3>🍪 Persistent браузер</h3>
                <p>Постоянный браузер с cookies для обхода блокировок</p>
            </div>
            <div class="card">
                <h3>🔄 Автообновление</h3>
                <p>Автоматическое обновление сессии и возврат на Avito</p>
            </div>
            <div class="card">
                <h3>📦 Пакетная обработка</h3>
                <p>Обработка множества URL одним запросом</p>
            </div>
        </div>

        <h2>🔗 Полезные ссылки</h2>
        <div class="example">
            📊 <a href="/api/health">Статус API</a><br>
            🔍 <a href="/api/browser/status">Статус браузера</a><br>
            📚 <a href="/api/docs">JSON документация</a><br>
            🎯 <a href="/api/sources">Поддерживаемые источники</a>
        </div>

        <footer style="margin-top: 50px; text-align: center; color: #6c757d; border-top: 1px solid #e1e5e9; padding-top: 20px;">
            <p>Realty Parser API v1.0.0 | Поддержка: Avito, Cian, Yandex Realty</p>
        </footer>
    </body>
    </html>
    """

# Функции для быстрого доступа (для использования в других Python модулях)
parse_property = parser_parse_property
parse_property_extended = parser_parse_property_extended
parse_properties_batch = parser_parse_properties_batch

# Создаем экземпляр класса для использования в других модулях
realty_parser = parser

if __name__ == "__main__":
    print("🚀 Запуск Realty Parser Server")
    print("=" * 60)
    print("🌐 HTTP API: http://localhost:8008")
    print("📚 Документация: http://localhost:8008/")
    print("📊 Статус API: http://localhost:8008/api/health")
    print("🔍 Статус браузера: http://localhost:8008/api/browser/status")
    if PERSISTENT_BROWSER_AUTO_START:
        print("🔄 Persistent браузер инициализируется в фоне...")
    else:
        print("ℹ️ Persistent браузер не запускается автоматически (включите PERSISTENT_BROWSER_AUTO_START или вызовите /api/browser/init).")
    print("=" * 60)

    # Запуск HTTP сервера
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8008,
        log_level="info"
    )

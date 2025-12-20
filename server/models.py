"""Data structures and request/response schemas for the realty parser API."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel


@dataclass
class PropertyData:
    """Структурированные данные о недвижимости."""

    rooms: Optional[int] = None
    price: Optional[float] = None
    total_area: Optional[float] = None
    living_area: Optional[float] = None
    kitchen_area: Optional[float] = None
    floor: Optional[str] = None
    total_floors: Optional[int] = None
    bathroom: Optional[str] = None
    balcony: Optional[str] = None
    renovation: Optional[str] = None
    construction_year: Optional[int] = None
    house_type: Optional[str] = None
    ceiling_height: Optional[float] = None
    furniture: Optional[str] = None
    address: Optional[str] = None
    metro_station: Optional[str] = None
    metro_time: Optional[Union[int, str]] = None
    metro_way: Optional[str] = None
    tags: Optional[List[str]] = None
    description: Optional[str] = None
    photo_urls: Optional[List[str]] = None
    source: Optional[str] = None
    url: Optional[str] = None
    status: Optional[bool] = None
    views_today: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Преобразует данные в словарь."""

        return asdict(self)

    def to_json(self) -> str:
        """Возвращает JSON-представление."""

        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)


class ParseUrlsRequest(BaseModel):
    urls: List[str]


class ParseTextRequest(BaseModel):
    text: str


class BazaWinnerAuthRequest(BaseModel):
    username: str
    password: str


class BazaWinnerSearchRequest(BaseModel):
    username: str
    password: str
    search_params: Dict[str, Any] = {}


class SendExcelDocumentRequest(BaseModel):
    user_id: str
    caption: str = "📊 Сравнение квартир"
    filename: str = "сравнение-квартир.xlsx"
    excel_data: List[Dict[str, Any]]


class FlatReportRequest(BaseModel):
    flat_id: int
    report_date: Optional[str] = None
    output_path: Optional[str] = None
    regenerate: bool = True


class ReportPreparationRequest(BaseModel):
    flat_id: Optional[int] = None
    radius_m: Optional[int] = None
    max_history: int = 3
    max_nearby: int = 20
    run_parser: bool = True


class ParseResponse(BaseModel):
    success: bool
    data: List[PropertyData]
    total: int
    message: str
    timestamp: str


__all__ = [
    "PropertyData",
    "ParseUrlsRequest",
    "ParseTextRequest",
    "BazaWinnerAuthRequest",
    "BazaWinnerSearchRequest",
    "SendExcelDocumentRequest",
    "FlatReportRequest",
    "ReportPreparationRequest",
    "ParseResponse",
]

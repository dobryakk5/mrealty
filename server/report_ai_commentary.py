"""AI-based commentary generator for publication tables."""

from __future__ import annotations

import os
from typing import Any, Mapping, Sequence

from .ai_generator_draft import BaseAIContentGenerator
from .formatting import fmt_money, fmt_num


class ReportAICommentary:
    """Produces textual explanations for report tables via OpenRouter."""

    def __init__(self, enabled: bool | None = None) -> None:
        toggle = enabled
        if toggle is None:
            toggle = os.getenv("REPORT_AI_ENABLED", "0").lower() in {"1", "true", "yes", "on"}
        if toggle:
            try:
                self._generator = BaseAIContentGenerator()
            except ValueError:
                self._generator = None
        else:
            self._generator = None

    def subject_commentary(
        self,
        subject: Mapping[str, Any] | None,
        meta: Mapping[str, Any] | None,
    ) -> str:
        prompt = self._build_subject_prompt(subject or {}, meta or {})
        fallback = self._subject_fallback(subject or {}, meta or {})
        return self._query_ai(prompt, fallback)

    def market_commentary(self, market: Mapping[str, Any] | None) -> str:
        prompt = self._build_market_prompt(market or {})
        fallback = self._market_fallback(market or {})
        return self._query_ai(prompt, fallback)

    def competitors_commentary(
        self,
        competitors: Sequence[Mapping[str, Any]] | None,
        subject: Mapping[str, Any] | None,
    ) -> str:
        if not competitors:
            return "Нет данных по конкурентам."
        prompt = self._build_competitors_prompt(competitors[:3], subject or {})
        fallback = self._competitors_fallback(competitors[:3])
        return self._query_ai(prompt, fallback)

    def _query_ai(self, prompt: str, fallback: str) -> str:
        if not self._generator:
            return fallback
        result = self._generator.get_ai_response(prompt, max_tokens=360, temperature=0.4)
        return (result or fallback).strip()

    def _build_subject_prompt(
        self, subject: Mapping[str, Any], meta: Mapping[str, Any]
    ) -> str:
        lines = [
            f"- Площадь: {fmt_num(subject.get('area_m2'), 2)} м²",
            f"- Цена: {fmt_money(subject.get('price'))} ₽",
            f"- Цена за м²: {fmt_num(subject.get('ppm'), 0)} ₽/м²",
        ]
        rooms = subject.get("rooms")
        if rooms is not None:
            lines.append(f"- Комнат: {int(rooms)}")
        floor = subject.get("floor")
        if floor is not None:
            lines.append(f"- Этаж: {int(floor)}")
        radius = meta.get("radius_m")
        if radius is not None:
            lines.append(f"- Радиус анализа: {int(radius)} м")

        return (
            "Ты аналитик. Напиши 2-3 коротких предложения с выводами по объекту и рекомендацией "
            "по цене или позиции на рынке. Вот исходные значения:\n"
            + "\n".join(lines)
        )

    def _build_market_prompt(self, market: Mapping[str, Any]) -> str:
        suggested = market.get("suggested_price") or {}
        lines = [
            f"- Учитывается конкурентов: {int(market.get('competitors_count', 0))}",
            f"- P25: {fmt_num(market.get('ppm_p25'), 0)} ₽/м²",
            f"- P50: {fmt_num(market.get('ppm_p50'), 0)} ₽/м²",
            f"- P75: {fmt_num(market.get('ppm_p75'), 0)} ₽/м²",
            f"- Позиция: {fmt_num(market.get('position01'), 3)}",
            f"- Вердикт: {market.get('verdict', '—')}",
            f"- Рекомендуемая цена P25: {fmt_money(suggested.get('p25'))} ₽",
            f"- Рекомендуемая цена P50: {fmt_money(suggested.get('p50'))} ₽",
            f"- Рекомендуемая цена P75: {fmt_money(suggested.get('p75'))} ₽",
        ]
        return (
            "На основе рыночных данных сделай 2-3 предложения о позиции объекта. Укажи, "
            "как соотносятся текущая цена и средние P25/P75 и что стоит учесть при корректировке. "
            "Данные:\n"
            + "\n".join(lines)
        )

    def _build_competitors_prompt(
        self, competitors: Sequence[Mapping[str, Any]], subject: Mapping[str, Any]
    ) -> str:
        rows: list[str] = []
        for index, item in enumerate(competitors, start=1):
            rows.append(
                f"{index}. Цена {fmt_money(item.get('price'))} ₽, площадь {fmt_num(item.get('area_m2'), 2)} м², "
                f"{fmt_num(item.get('ppm'), 0)} ₽/м², расстояние {fmt_num(item.get('dist_m'), 0)} м"
            )
        subject_info = []
        if subject.get("price") is not None:
            subject_info.append(f"цена объекта {fmt_money(subject.get('price'))} ₽")
        if subject.get("area_m2") is not None:
            subject_info.append(f"площадь {fmt_num(subject.get('area_m2'), 2)} м²")
        subject_context = "; ".join(subject_info) or "без дополнительных данных по объекту"
        return (
            "Задача: описать выборку конкурентов (до трёх) и её влияние на позиционирование. "
            f"Объект: {subject_context}.\n"
            "Сравни кратко, кто из конкурентов ближе по цене/площади и что стоит учитывать при продаже/аренде.\n"
            + "\n".join(rows)
        )

    def _subject_fallback(self, subject: Mapping[str, Any], meta: Mapping[str, Any]) -> str:
        parts = [
            f"Площадь: {fmt_num(subject.get('area_m2'), 2)} м²",
            f"Цена: {fmt_money(subject.get('price'))} ₽",
            f"Цена за м²: {fmt_num(subject.get('ppm'), 0)} ₽/м²",
        ]
        rooms = subject.get("rooms")
        if rooms is not None:
            parts.append(f"комнат: {int(rooms)}")
        floor = subject.get("floor")
        if floor is not None:
            parts.append(f"этаж: {int(floor)}")
        radius = meta.get("radius_m")
        if radius is not None:
            parts.append(f"радиус анализа: {int(radius)} м")
        return "Объект: " + ", ".join(parts) + "."

    def _market_fallback(self, market: Mapping[str, Any]) -> str:
        suggested = market.get("suggested_price") or {}
        parts = [
            f"конкурентов: {int(market.get('competitors_count', 0))}",
            f"P25: {fmt_num(market.get('ppm_p25'), 0)} ₽/м²",
            f"P50: {fmt_num(market.get('ppm_p50'), 0)} ₽/м²",
            f"P75: {fmt_num(market.get('ppm_p75'), 0)} ₽/м²",
            f"позиция: {fmt_num(market.get('position01'), 3)}",
            f"вердикт: {market.get('verdict', '—')}",
            f"рекомендуемая цена P50: {fmt_money(suggested.get('p50'))} ₽",
        ]
        return "Рынок: " + ", ".join(parts) + "."

    def _competitors_fallback(self, competitors: Sequence[Mapping[str, Any]]) -> str:
        descriptions: list[str] = []
        for item in competitors[:3]:
            descriptions.append(
                f"{fmt_money(item.get('price'))} ₽ за {fmt_num(item.get('area_m2'), 2)} м², "
                f"{fmt_num(item.get('ppm'), 0)} ₽/м²"
            )
        summary = "; ".join(descriptions)
        return f"Топ-конкуренты: {summary}."

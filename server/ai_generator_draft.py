"""Shared base logic for AI content generator."""

from __future__ import annotations

import logging
import os
from typing import List, Optional

import requests

from .system_settings import (
    get_default_ai_model,
    get_post_ai_model,
    get_fallback_ai_model,
)

try:
    from huggingface_hub import InferenceClient

    HF_HUB_AVAILABLE = True
except ImportError:  # pragma: no cover - optional dependency
    HF_HUB_AVAILABLE = False
    InferenceClient = None  # type: ignore

logger = logging.getLogger("backend.core.ai_generator")


class BaseAIContentGenerator:
    """Transport helpers and API configuration shared by specialized mixins."""

    api_url = "https://openrouter.ai/api/v1/chat/completions"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("OPENROUTER_API_KEY")
        if not self.api_key:
            raise ValueError("OPENROUTER_API_KEY not found in environment")

        self.model = get_default_ai_model()
        self.post_model = get_post_ai_model()
        self.fallback_model = get_fallback_ai_model()

        self.hf_client = None
        if HF_HUB_AVAILABLE:
            hf_token = os.getenv("HUGGINGFACE_TOKEN") or os.getenv("HF_TOKEN")
            if hf_token:
                try:
                    self.hf_client = InferenceClient(
                        provider="nebius",
                        api_key=hf_token,  # type: ignore[arg-type]
                    )
                    logger.info("HuggingFace Nebius client initialized successfully")
                except Exception as exc:  # pragma: no cover - log only
                    logger.warning("Failed to initialize HuggingFace client: %s", exc)
            else:
                logger.debug(
                    "HuggingFace token not found, HF image generation will be unavailable"
                )

    def _call_openrouter(
        self,
        model: str,
        prompt: str,
        max_tokens: int,
        temperature: float,
    ) -> Optional[str]:
        """Call OpenRouter chat completions API and return text."""
        try:
            response = requests.post(
                self.api_url,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://zavod-content-factory.com",
                    "X-Title": "Content Factory AI Generator",
                },
                json={
                    "model": model,
                    "messages": [
                        {"role": "user", "content": prompt},
                    ],
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                },
                timeout=60,
            )

            if response.status_code == 200:
                data = response.json()
                choices = data.get("choices")
                if isinstance(choices, list) and choices:
                    message = choices[0].get("message") or {}
                    content = message.get("content")
                    if isinstance(content, str) and content.strip():
                        return content.strip()
                    logger.error(
                        "OpenRouter API response for model %s is missing message content: %s",
                        model,
                        data,
                    )
                    return None

                logger.error(
                    "OpenRouter API response for model %s missing choices: %s",
                    model,
                    data,
                )
                return None

            logger.error(
                "OpenRouter API Error (%s) for model %s - %s",
                response.status_code,
                model,
                response.text,
            )
            return None

        except requests.exceptions.Timeout:
            logger.error("OpenRouter API request timed out for model %s", model)
            return None
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error(
                "Error calling OpenRouter API for model %s: %s",
                model,
                exc,
                exc_info=True,
            )
            return None

    def _generate_text_with_fallback(
        self,
        prompt: str,
        max_tokens: int,
        temperature: float,
        primary_model: Optional[str] = None,
        fallback_models: Optional[List[str]] = None,
    ) -> Optional[str]:
        """Try a primary model and optional fallback models sequentially."""

        fallback_models = fallback_models or []
        models_to_try: List[str] = []

        normalized_primary = (primary_model or self.model or "").strip()
        if not normalized_primary:
            normalized_primary = get_default_ai_model()
        if normalized_primary:
            models_to_try.append(normalized_primary)

        for candidate in fallback_models:
            normalized = (candidate or "").strip()
            if normalized and normalized not in models_to_try:
                models_to_try.append(normalized)

        if not models_to_try:
            logger.error("No models configured for AI request")
            return None

        for index, model_name in enumerate(models_to_try):
            response = self._call_openrouter(model_name, prompt, max_tokens, temperature)
            if response:
                return response
            if index + 1 < len(models_to_try):
                logger.info(
                    "Model %s failed, trying fallback %s",
                    model_name,
                    models_to_try[index + 1],
                )

        return None

    def get_ai_response(
        self,
        prompt: str,
        max_tokens: int = 2000,
        temperature: float = 0.7,
        model: Optional[str] = None,
        allow_fallback: bool = True,
    ) -> Optional[str]:
        """Send request to OpenRouter API with automatic fallback model."""

        selected_model = (model or self.model or "").strip()
        fallback_model = self.fallback_model.strip() if self.fallback_model else ""
        fallback_models: List[str] = []
        if allow_fallback and fallback_model and fallback_model != selected_model:
            fallback_models.append(fallback_model)

        return self._generate_text_with_fallback(
            prompt=prompt,
            max_tokens=max_tokens,
            temperature=temperature,
            primary_model=selected_model,
            fallback_models=fallback_models,
        )

    def test_connection(self) -> bool:
        """Quick health check for OpenRouter connectivity."""
        try:
            response = self.get_ai_response("Ответь одним словом: 'готов'", max_tokens=10)
            return response is not None
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("Connection test failed: %s", exc)
            return False


__all__ = ["BaseAIContentGenerator", "logger"]

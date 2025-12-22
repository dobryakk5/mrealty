"""Settings helpers for AI utilities."""

from __future__ import annotations

import os


def _env_or_default(env_name: str, default: str) -> str:
    return os.getenv(env_name) or default


def get_default_ai_model() -> str:
    """Model used by default for OpenRouter requests."""
    return _env_or_default("OPENROUTER_MODEL", "nex-agi/deepseek-v3.1-nex-n1:free")


def get_post_ai_model() -> str:
    """Model used for follow-up refinement requests."""
    return os.getenv("OPENROUTER_POST_MODEL") or ""


def get_fallback_ai_model() -> str:
    """Fallback model when the primary one fails."""
    return os.getenv("OPENROUTER_FALLBACK_MODEL") or get_default_ai_model()

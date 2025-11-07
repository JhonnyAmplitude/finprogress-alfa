from __future__ import annotations
import re
import logging
import os
from datetime import datetime
from typing import Any, Optional, Dict


def get_logger(name: str = "parser_vtb") -> logging.Logger:
    level_name = os.getenv("PARSER_LOGLEVEL", "DEBUG").upper()
    level = getattr(logging, level_name, logging.INFO)
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        handler.setFormatter(logging.Formatter(fmt))
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger

logger = get_logger()

DATE_RE = re.compile(r"\d{2}[,.]\d{2}[,.]\d{4}")
ISIN_RE = re.compile(r"[A-Z]{2}[A-Z0-9]{9}\d", re.IGNORECASE)


def format_date_from_match(value: str) -> str:
    return value.replace(",", ".")


def extract_date(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        return value.strftime("%d.%m.%Y")

    s = str(value).strip() if value else ""
    s = re.sub(r"[\s\u00A0]", "", s)

    if re.match(r"\d{2}[,.]\d{2}[,.]\d{4}", s):
        return s.replace(",", ".")

    return None


def to_float_safe(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        s = str(v).strip()
        if s in ("", "-", "--"):
            return 0.0
        s = s.replace("\u00A0", " ").replace(" ", "").replace(",", ".")
        return float(s)
    except Exception:
        try:
            return float(str(v).replace(",", "."))
        except Exception:
            return 0.0


def to_int_safe(v: Any) -> int:
    """
    Аналогично, безопасно в int.
    """
    try:
        return int(round(float(str(v).replace("\u00A0", " ").replace(" ", "").replace(",", ".") or 0.0)))
    except Exception:
        return 0

def _local_name(tag: str) -> str:
    """Возвращает локальное имя тега без namespace."""
    if tag is None:
        return ""
    return tag.split("}")[-1] if "}" in tag else tag

def _normalize_attrib(attrib: Dict[str, str]) -> Dict[str, str]:
    """Нормализация атрибутов: приводим ключи к lowercase."""
    return {k.lower(): v for k, v in attrib.items()}

def extract_isin_from_attr(s: Optional[str]) -> str:
    if not s:
        return ""
    m = ISIN_RE.search(str(s))
    return m.group(0).upper() if m else str(s).strip()
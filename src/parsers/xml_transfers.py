# src/parsers/xml_transfers.py
from __future__ import annotations
from typing import List, Tuple, Dict, Any, Optional
import xml.etree.ElementTree as ET
import re
from datetime import datetime
from pathlib import Path
from src.OperationDTO import OperationDTO
from src.utils import logger, to_float_safe, _local_name, _normalize_attrib, extract_isin_from_attr

ISIN_RE = re.compile(r"[A-Z]{2}[A-Z0-9]{9}\d", re.IGNORECASE)
DATE_TIME_RE = re.compile(r"\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2}:\d{2}")


def parse_datetime_from_settlement(settlement_date: Optional[str], settlement_time: Optional[str]) -> Optional[
    datetime]:
    """
    Парсинг даты и времени из settlement_date и settlement_time.
    settlement_date="2025-10-13T00:00:00"
    settlement_time="04:08:24"
    Возвращает datetime объект.
    """
    if not settlement_date:
        return None

    try:
        # Если в settlement_date уже есть время
        if "T" in settlement_date:
            date_part = settlement_date.split("T")[0]

            # Если есть settlement_time, используем его
            if settlement_time:
                time_part = settlement_time.strip()
                # Убираем миллисекунды если есть
                time_part = time_part.split(".")[0]
                datetime_str = f"{date_part} {time_part}"
                return datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
            else:
                # Используем дату из settlement_date
                return datetime.fromisoformat(settlement_date)
        else:
            # Если формат без T
            if settlement_time:
                datetime_str = f"{settlement_date} {settlement_time}"
                return datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
            else:
                return datetime.strptime(settlement_date, "%Y-%m-%d")
    except Exception as e:
        logger.debug("Failed to parse datetime from '%s' and '%s': %s", settlement_date, settlement_time, e)

        # Fallback: пробуем извлечь только дату
        try:
            if "T" in settlement_date:
                date_part = settlement_date.split("T")[0]
                return datetime.strptime(date_part, "%Y-%m-%d")
            else:
                return datetime.strptime(settlement_date, "%Y-%m-%d")
        except Exception:
            return None


def parse_transfers_from_xml(path_or_bytes: str | bytes) -> Tuple[List[OperationDTO], Dict[str, Any]]:
    """
    Парсинг конвертаций из раздела Trades3 > Report Name="4_Transfers".

    Фильтрует операции:
    - oper_type="Перевод"
    - В comment_new содержится "Конвертация"

    Возвращает (List[OperationDTO], stats_dict).
    """
    logger.info("Start parsing XML transfers (conversions): %s", str(path_or_bytes)[:200])

    stats = {
        "total_rows": 0,
        "parsed": 0,
        "skipped_not_conversion": 0,
        "skipped_no_date": 0,
        "skipped_no_qty": 0,
        "skipped_invalid": 0,
    }
    ops: List[OperationDTO] = []

    # Подготавливаем источник для iterparse
    try:
        if isinstance(path_or_bytes, (bytes, bytearray)):
            tmp = Path("__xml_transfers_tmp__.xml")
            tmp.write_bytes(path_or_bytes)
            source = str(tmp)
            cleanup_tmp = True
        else:
            source = str(path_or_bytes)
            cleanup_tmp = False
    except Exception as e:
        logger.exception("Failed to prepare XML source: %s", e)
        return [], {"error": str(e)}

    try:
        # iterparse по событию 'end'
        for event, elem in ET.iterparse(source, events=("end",)):
            if _local_name(elem.tag).lower() != "details":
                elem.clear()
                continue

            stats["total_rows"] += 1
            attrib_raw = dict(elem.attrib)
            attrib = _normalize_attrib(attrib_raw)

            # Проверяем oper_type
            oper_type = (attrib.get("oper_type") or "").strip()
            if oper_type.lower() != "перевод":
                stats["skipped_not_conversion"] += 1
                elem.clear()
                continue

            # Проверяем comment_new на наличие "Конвертация"
            comment_new = (attrib.get("comment_new") or "").strip()
            if "конвертация" not in comment_new.lower():
                stats["skipped_not_conversion"] += 1
                elem.clear()
                continue

            # Извлекаем qty
            qty = to_float_safe(attrib.get("qty") or "0")
            if qty == 0:
                stats["skipped_no_qty"] += 1
                elem.clear()
                continue

            # Парсим дату и время
            settlement_date = attrib.get("settlement_date")
            settlement_time = attrib.get("settlement_time")
            date_val = parse_datetime_from_settlement(settlement_date, settlement_time)

            if date_val is None:
                stats["skipped_no_date"] += 1
                elem.clear()
                continue

            # Определяем тип операции по знаку qty
            if qty > 0:
                operation_type = "asset_receive"
            else:
                operation_type = "asset_withdrawal"

            # Извлекаем ISIN из p_name
            p_name = attrib.get("p_name") or ""
            isin = extract_isin_from_attr(p_name)

            # Место проведения операции
            place_name = attrib.get("place_name") or ""

            try:
                dto = OperationDTO(
                    date=date_val,
                    operation_type=operation_type,
                    payment_sum=0.0,
                    currency="",
                    ticker="",
                    isin=isin,
                    reg_number="",
                    price=0.0,
                    quantity=abs(qty),
                    aci=0.0,
                    comment=comment_new,
                    operation_id="",
                    commission=0.0,
                )
                ops.append(dto)
                stats["parsed"] += 1
            except Exception as e:
                logger.exception("Failed to build OperationDTO for conversion: %s", e)
                stats["skipped_invalid"] += 1

            elem.clear()

    except Exception as e:
        logger.exception("XML transfers parsing failed: %s", e)
        return ops, {"error": str(e)}
    finally:
        if cleanup_tmp:
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass

    logger.info(
        "Parsed %s conversions (checked %s rows, skipped %s non-conversions)",
        stats["parsed"],
        stats["total_rows"],
        stats["skipped_not_conversion"]
    )

    return ops, stats
# src/parsers/xml_trades.py
from __future__ import annotations
from typing import List, Tuple, Dict, Any, Optional
import xml.etree.ElementTree as ET
import re
from datetime import datetime
from pathlib import Path
from src.OperationDTO import OperationDTO
from src.utils import logger, to_float_safe, _local_name, _normalize_attrib, extract_isin_from_attr, to_int_safe

DATE_TIME_RE = re.compile(r"\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2}:\d{2}")
DATE_TIME_SHORT_RE = re.compile(r"\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2}")


def parse_datetime_from_text(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    s = str(s)
    m = DATE_TIME_RE.search(s)
    if m:
        try:
            return datetime.strptime(m.group(0), "%d.%m.%Y %H:%M:%S")
        except Exception:
            pass
    m2 = DATE_TIME_SHORT_RE.search(s)
    if m2:
        try:
            return datetime.strptime(m2.group(0), "%d.%m.%Y %H:%M")
        except Exception:
            pass
    # try common formats
    for fmt in ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s.strip(), fmt)
        except Exception:
            continue
    return None


def extract_ticker_from_name(name: Optional[str]) -> str:
    if not name:
        return ""
    tok = str(name).strip().split()[0]
    if re.fullmatch(r"[A-Za-z0-9\-\.]{1,8}", tok):
        return tok
    return ""


def extract_first_trade_no(trade_no_raw: Optional[str]) -> str:
    """
    Извлекает первый номер сделки из строки.
    Примеры:
      "14533071091\r\n1280737003" -> "14533071091"
      "12087770319 907346105" -> "12087770319"
      "123456" -> "123456"
    """
    if not trade_no_raw:
        return ""

    # Удаляем лишние пробелы и переносы строк
    cleaned = str(trade_no_raw).strip()

    # Разбиваем по пробелам, табам, переносам строк
    parts = re.split(r'[\s\r\n\t]+', cleaned)

    # Берём первую часть (первый номер)
    if parts and parts[0]:
        return parts[0].strip()

    return ""


def parse_trades_from_xml(path_or_bytes: str | bytes) -> Tuple[List[OperationDTO], Dict[str, Any]]:
    """
    Парсинг всех <Details ...> записей из раздела Trades (streaming).
    Возвращает (List[OperationDTO], stats_dict).
    """
    logger.info("Start parsing XML trades: %s", str(path_or_bytes)[:200])
    stats = {
        "total_rows": 0,
        "parsed": 0,
        "skipped_no_date": 0,
        "skipped_no_qty": 0,
        "skipped_invalid": 0,
        "total_commission": 0.0,
    }
    ops: List[OperationDTO] = []

    # подготавливаем источник для iterparse
    try:
        if isinstance(path_or_bytes, (bytes, bytearray)):
            # write to temp file to use iterparse reliably
            tmp = Path("__xml_trades_tmp__.xml")
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
        # iterparse по событию 'end' для элементов чтобы сразу обрабатывать и чистить
        for event, elem in ET.iterparse(source, events=("end",)):
            if _local_name(elem.tag).lower() != "details":
                # не нужный элемент — очистим и продолжим
                elem.clear()
                continue

            stats["total_rows"] += 1
            attrib_raw = dict(elem.attrib)  # raw attributes
            attrib = _normalize_attrib(attrib_raw)  # lowercased keys

            # quantity
            qty = to_int_safe(
                attrib.get("qty") or attrib.get("quantity") or attrib.get("textbox14") or attrib.get("qty "))
            if qty == 0:
                stats["skipped_no_qty"] += 1
                elem.clear()
                continue

            # date: prefer db_time, затем settlement_time, save_settlement_date
            date_val = None
            for key in ("db_time", "dbtime", "settlement_time", "save_settlement_date", "save_depo_settlement_date"):
                if attrib.get(key):
                    date_val = parse_datetime_from_text(attrib.get(key))
                    if date_val:
                        break
            if date_val is None:
                stats["skipped_no_date"] += 1
                elem.clear()
                continue

            # price / totals / nkd / currency
            price = to_float_safe(attrib.get("price") or attrib.get("textbox25") or attrib.get("price "))
            total = to_float_safe(
                attrib.get("summ_trade") or attrib.get("summtrade") or attrib.get("summ_trade".lower()) or attrib.get(
                    "summ_trade"))
            nkd = to_float_safe(attrib.get("summ_nkd") or attrib.get("summnkd") or attrib.get("summ_nkd".lower()))
            currency = (attrib.get("curr_calc") or attrib.get("curr") or attrib.get("textbox14") or "").strip()
            if currency.upper() in ("RUR", "РУБ", "РУБЛЬ"):
                currency = "RUB"

            # isin / p_name / ticker / trade_no / place
            isin = extract_isin_from_attr(attrib.get("isin_reg") or attrib.get("isin1") or attrib.get("isin"))
            p_name = attrib.get("p_name") or attrib.get("pname") or attrib.get("active_name") or ""
            ticker = extract_ticker_from_name(p_name)

            # Извлекаем первый номер сделки
            trade_no_raw = (attrib.get("trade_no") or attrib.get("tradeno") or attrib.get("trade")) or ""
            trade_no = extract_first_trade_no(trade_no_raw)

            # commission
            commission = to_float_safe(attrib.get("bank_tax") or attrib.get("banktax") or attrib.get("bank_tax"))
            stats["total_commission"] += float(commission or 0.0)

            # decide operation type (qty sign)
            op_type = "buy" if float(qty) > 0 else "sale"

            try:
                dto = OperationDTO(
                    date=date_val,
                    operation_type=op_type,
                    payment_sum=total,
                    currency=currency,
                    ticker=(ticker or None) or "",
                    isin=(isin or None) or "",
                    reg_number="",
                    price=price,
                    quantity=int(round(abs(float(qty)))),
                    aci=nkd,
                    comment=str(attrib.get("place_name") or attrib.get("place") or ""),
                    operation_id=trade_no,
                    commission=float(commission),
                )
                ops.append(dto)
                stats["parsed"] += 1
            except Exception as e:
                logger.exception("Failed to build OperationDTO for attrs %s: %s", attrib, e)
                stats["skipped_invalid"] += 1

            elem.clear()

    except Exception as e:
        logger.exception("XML parsing failed: %s", e)
        return ops, {"error": str(e)}
    finally:
        if cleanup_tmp:
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass

    logger.info("Parsed %s trades (checked %s rows). total_commission=%s", stats["parsed"], stats["total_rows"],
                stats["total_commission"])
    return ops, stats
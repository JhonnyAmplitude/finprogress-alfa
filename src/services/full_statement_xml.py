# src/services/full_statement_xml.py
from __future__ import annotations
from typing import Union, List, Dict, Any, Tuple
from datetime import datetime

from src.parsers.xml_trades import parse_trades_from_xml
from src.parsers.xml_fin_ops import parse_fin_operations_from_xml
from src.parsers.xml_transfers import parse_transfers_from_xml
from src.utils import logger
from src.OperationDTO import OperationDTO


def _op_key(op: OperationDTO) -> str:
    """
    Составляем ключ для дедупации:
    - если есть operation_id, используем "id:<operation_id>"
    - иначе используем "auto:<date>|<type>|<sum>|<ticker>|<isin>"
    """
    oid = (op.operation_id or "").strip()
    if oid:
        return f"id:{oid}"
    # date normalization
    date_part = ""
    if isinstance(op.date, datetime):
        date_part = op.date.isoformat()
    else:
        date_part = str(op.date or "")
    try:
        sum_part = float(op.payment_sum) if op.payment_sum not in (None, "") else 0.0
    except Exception:
        sum_part = str(op.payment_sum or "")
    return f"auto:{date_part}|{op.operation_type}|{sum_part}|{op.ticker or ''}|{op.isin or ''}"


def _dedupe_ops(ops: List[OperationDTO]) -> Tuple[List[OperationDTO], int]:
    seen = set()
    deduped: List[OperationDTO] = []
    for o in ops:
        k = _op_key(o)
        if k in seen:
            continue
        seen.add(k)
        deduped.append(o)
    return deduped, len(deduped)


def _sort_key_for_operation(op_dict: Dict[str, Any]) -> tuple:
    """
    Ключ сортировки для операции.
    Сортируем по:
    1. Дате (datetime или строка)
    2. Типу операции (строка) - для стабильности сортировки при одинаковых датах

    Возвращает кортеж (datetime_obj, operation_type)
    """
    date_val = op_dict.get("date")
    op_type = op_dict.get("operation_type", "")

    # Преобразуем дату в datetime для корректной сортировки
    if isinstance(date_val, datetime):
        dt = date_val
    elif isinstance(date_val, str):
        try:
            # Пробуем распарсить ISO формат
            dt = datetime.fromisoformat(date_val)
        except Exception:
            try:
                # Пробуем другие форматы
                dt = datetime.strptime(date_val.split()[0], "%d.%m.%Y")
            except Exception:
                # Если не удалось - ставим минимальную дату
                dt = datetime.min
    else:
        dt = datetime.min

    return (dt, op_type)


def parse_full_statement_xml(path_or_bytes: Union[str, bytes]) -> Dict[str, Any]:
    """
    Parse trades + financial operations + transfers (conversions) from XML source (path or bytes).
    Возвращает операции отсортированные по дате.
    """
    logger.info("Starting full XML parse for %s", str(path_or_bytes)[:200])

    # --- parse trades ---
    trades, trade_stats = parse_trades_from_xml(path_or_bytes)
    if isinstance(trade_stats, dict) and trade_stats.get("error"):
        logger.error("XML trades parser error: %s", trade_stats.get("error"))
        return {"operations": [], "meta": {"error": trade_stats.get("error")}}

    # --- parse financial operations ---
    fin_ops, fin_stats = parse_fin_operations_from_xml(path_or_bytes)
    if isinstance(fin_stats, dict) and fin_stats.get("error"):
        logger.error("XML financial ops parser error: %s", fin_stats.get("error"))
        return {"operations": [], "meta": {"error": fin_stats.get("error")}}

    # --- parse transfers (conversions) ---
    transfers, transfer_stats = parse_transfers_from_xml(path_or_bytes)
    if isinstance(transfer_stats, dict) and transfer_stats.get("error"):
        logger.error("XML transfers parser error: %s", transfer_stats.get("error"))
        # Не фатальная ошибка — продолжаем без transfer'ов
        transfers = []
        transfer_stats = {"parsed": 0, "total_rows": 0}

    # raw counts
    trade_raw_count = int(trade_stats.get("total_rows", len(trades)))
    fin_raw_count = int(fin_stats.get("total_rows", len(fin_ops)))
    transfer_raw_count = int(transfer_stats.get("total_rows", len(transfers)))

    # dedupe within each group
    deduped_fin, after_dedupe_fin = _dedupe_ops(fin_ops)
    deduped_trades, after_dedupe_trades = _dedupe_ops(trades)
    deduped_transfers, after_dedupe_transfers = _dedupe_ops(transfers)

    # combine all: финоперации + сделки + переводы/конвертации
    combined_ops_dto = deduped_fin + deduped_trades + deduped_transfers

    # convert to dicts
    combined_ops = [o.to_dict() for o in combined_ops_dto]

    # sort by date
    try:
        combined_ops.sort(key=_sort_key_for_operation)
        logger.info("Operations sorted by date successfully")
    except Exception as e:
        logger.warning("Failed to sort operations by date: %s", e)

    # meta
    meta: Dict[str, Any] = {
        "fin_ops_raw_count": fin_raw_count,
        "trade_ops_raw_count": trade_raw_count,
        "transfer_ops_raw_count": transfer_raw_count,
        "total_ops_count": len(combined_ops),
        "fin_ops_stats": fin_stats,
        "trade_ops_stats": trade_stats,
        "transfer_ops_stats": transfer_stats,
        "after_dedupe_from_fin": after_dedupe_fin,
        "after_dedupe_from_trade": after_dedupe_trades,
        "after_dedupe_from_transfer": after_dedupe_transfers,
    }

    logger.info(
        "XML full parse summary: fin=%s, trades=%s, transfers=%s → total_ops=%s (sorted)",
        meta["fin_ops_stats"].get("parsed", 0),
        meta["trade_ops_stats"].get("parsed", 0),
        meta["transfer_ops_stats"].get("parsed", 0),
        meta["total_ops_count"],
    )

    logger.info("Finished full XML parse: parsed %s operations", meta["total_ops_count"])
    return {"operations": combined_ops, "meta": meta}
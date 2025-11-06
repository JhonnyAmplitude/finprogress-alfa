# src/services/full_statement_xml.py
from __future__ import annotations
from typing import Union, List, Dict, Any, Tuple
from datetime import datetime

from src.parsers.xml_trades import parse_trades_from_xml
from src.parsers.xml_fin_ops import parse_fin_operations_from_xml
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
    # payment_sum might be str or float
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


def parse_full_statement_xml(path_or_bytes: Union[str, bytes]) -> Dict[str, Any]:
    """
    Parse trades + financial operations from XML source (path or bytes).
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

    # raw counts (as reported by parsers)
    trade_raw_count = int(trade_stats.get("total_rows", len(trades)))
    fin_raw_count = int(fin_stats.get("total_rows", len(fin_ops)))

    # dedupe within each group
    deduped_fin, after_dedupe_fin = _dedupe_ops(fin_ops)
    deduped_trades, after_dedupe_trades = _dedupe_ops(trades)

    # combine operations: keep financial ops first (so ordering in earlier UX preserved)
    combined_ops = [o.to_dict() for o in deduped_fin] + [o.to_dict() for o in deduped_trades]

    # meta assembly
    meta: Dict[str, Any] = {
        "fin_ops_raw_count": fin_raw_count,
        "trade_ops_raw_count": trade_raw_count,
        "total_ops_count": len(combined_ops),
        "fin_ops_stats": fin_stats,
        "trade_ops_stats": trade_stats,
        "after_dedupe_from_fin": after_dedupe_fin,
        "after_dedupe_from_trade": after_dedupe_trades,
    }

    # helper short summary log similar to CLI output
    logger.info(
        "%s parsed: fin=%s (raw=%s -> deduped=%s), trades=%s (raw=%s -> deduped=%s), total_ops=%s",
        "XML full parse summary",
        meta["fin_ops_stats"].get("parsed", len(deduped_fin)),
        fin_raw_count,
        after_dedupe_fin,
        meta["trade_ops_stats"].get("parsed", len(deduped_trades)),
        trade_raw_count,
        after_dedupe_trades,
        meta["total_ops_count"],
    )

    logger.info("Finished full XML parse: parsed %s operations", meta["total_ops_count"])
    return {"operations": combined_ops, "meta": meta}

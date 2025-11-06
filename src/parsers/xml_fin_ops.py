# src/parsers/xml_fin_ops.py
from __future__ import annotations
from typing import Optional, List, Dict, Any, Tuple, Union
from datetime import datetime
import xml.etree.ElementTree as ET
from decimal import Decimal, InvalidOperation
import re
import logging

from src.OperationDTO import OperationDTO
from src.constants import SKIP_OPERATIONS

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# --- regexes / heuristics ---
RE_ISIN = re.compile(r"\b[A-Z]{2}[A-Z0-9]{10}\b")
RE_LONG_DIGITS = re.compile(r"\b(\d{5,})\b")
RE_OPERATION_ID = RE_LONG_DIGITS
RE_REG_NUMBER = re.compile(
    r"\b[0-9][0-9A-ZА-Я]{0,7}[-/][0-9A-ZА-Я\-\/]*\d[0-9A-ZА-Я\-\/]*\b",
    re.IGNORECASE
)


# --- helpers ---
def _local_name(tag: str) -> str:
    """Return local name of element tag, without namespace."""
    if tag is None:
        return ""
    return tag.split("}")[-1] if "}" in tag else tag


def _safe_attr(elem: Optional[ET.Element], name: str) -> Optional[str]:
    if elem is None:
        return None
    v = elem.attrib.get(name)
    if v is None:
        v = elem.attrib.get(name.lower())
    if v is None:
        return None
    v = v.strip()
    return v if v != "" else None


def _parse_decimal(v: Optional[str]) -> Optional[Decimal]:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return v
    try:
        return Decimal(v)
    except (InvalidOperation, ValueError):
        try:
            return Decimal(v.replace(",", "."))
        except Exception:
            return None


def _decimal_to_float(d: Optional[Decimal]) -> float:
    if d is None:
        return 0.0
    try:
        return float(d)
    except Exception:
        try:
            return float(str(d))
        except Exception:
            return 0.0


def _parse_iso_datetime(v: Optional[str]) -> Optional[datetime]:
    if not v:
        return None
    try:
        return datetime.fromisoformat(v)
    except Exception:
        try:
            return datetime.strptime(v.split("T")[0], "%Y-%m-%d")
        except Exception:
            return None


def _collect_elements_by_local_name(root: ET.Element, local: str) -> List[ET.Element]:
    """Return all descendant elements whose local name equals `local` (namespace-agnostic)."""
    out = []
    for el in root.iter():
        if _local_name(el.tag) == local:
            out.append(el)
    return out


def _find_first_descendant_by_local_name(root: ET.Element, local: str) -> Optional[ET.Element]:
    for el in root.iter():
        if _local_name(el.tag) == local:
            return el
    return None


def _collect_p_code_candidates(elem: Optional[ET.Element]) -> List[Dict[str, str]]:
    res = []
    if elem is None:
        return res
    for p in elem.iter():
        if _local_name(p.tag) == "p_code":
            res.append(dict(p.attrib))
    return res


def _extract_currency_and_amount(comment_elem: Optional[ET.Element]) -> (str, Optional[Decimal], Dict[str, Any]):
    candidates = _collect_p_code_candidates(comment_elem)
    currency = ""
    amount = None
    debug = {"p_code_candidates": candidates}
    for c in candidates:
        cur = c.get("p_code") or c.get("currency")
        vol = c.get("volume") or c.get("volume1") or c.get("amount")
        if cur and not currency:
            currency = cur.strip()
        if vol and amount is None:
            amount = _parse_decimal(vol.strip())
    return currency or "", amount, debug


def _extract_textbox_values(comment_elem: Optional[ET.Element]) -> Dict[str, Optional[str]]:
    res = {"money_volume": None, "all_volume": None, "debet_volume": None, "acc_code": None}
    if comment_elem is None:
        return res
    for node in comment_elem.iter():
        ln = _local_name(node.tag)
        if ln == "Textbox83" and res["money_volume"] is None:
            mv = node.attrib.get("money_volume")
            if mv and mv.strip() != "":
                res["money_volume"] = mv.strip()
        elif ln == "Textbox84" and res["all_volume"] is None:
            av = node.attrib.get("all_volume")
            if av and av.strip() != "":
                res["all_volume"] = av.strip()
        elif ln == "Textbox93" and res["debet_volume"] is None:
            dv = node.attrib.get("debet_volume")
            if dv and dv.strip() != "":
                res["debet_volume"] = dv.strip()
        elif ln == "Textbox11" and res["acc_code"] is None:
            ac = node.attrib.get("acc_code")
            if ac and ac.strip() != "":
                res["acc_code"] = ac.strip()
    return res


def _extract_reg_and_opid_improved(comment_text: Optional[str], oper_type_text: Optional[str]) -> (str, str):
    """
    Extract reg_number from comment text.
    - reg_number: извлекаем полный номер вида 4B02-01-00965-B-001P или 1-04-33498-E
    - operation_id: не используется для финансовых операций (всегда пустой)
    Returns (reg_number, operation_id) as strings (possibly empty).
    """
    reg = ""
    opid = ""  # не используется для финансовых операций

    if comment_text:
        # Извлекаем полный регистрационный номер
        m = RE_REG_NUMBER.search(comment_text)
        if m:
            reg = m.group(0)  # весь найденный паттерн

    return reg or "", opid


def _determine_operation_type(oper_type_val: str, comment_text: str, payment_sum: float) -> str:
    """
    Определяет тип операции на основе oper_type и комментария.
    Возвращает mapped operation_type строку.
    """
    oper_lower = oper_type_val.lower()
    comment_lower = comment_text.lower()

    # 1. НДФЛ - зависит от знака суммы
    if "ндфл" in oper_lower:
        return "refund" if payment_sum > 0 else "withholding"

    # 2. Перевод - смотрим в комментарий
    if "перевод" in oper_lower:
        if "погашение купона" in comment_lower or "погашением купона" in comment_lower:
            return "coupon"
        elif "частичное погашение номинала" in comment_lower or "частичном погашении номинала" in comment_lower:
            return "amortization"
        elif "полное погашение номинала" in comment_lower or "полном погашении номинала" in comment_lower:
            return "repayment"
        elif "из ао \"альфа-банк" in comment_lower or "из ао альфа-банк" in comment_lower:
            return "deposit"
        elif "дивиденд" in comment_lower:
            return "dividend"
        elif "списание по поручению клиента" in comment_lower:
            return "withdrawal"
        elif "возврат средств по дог" in comment_lower:
            return "withdrawal"
        elif "выплата по поручению клиента в рамках акция" in comment_lower:
            return "other_income"
        # Пропускаем промежуточные дивиденды
        elif "выплата промежуточных дивидендов" in comment_lower:
            return "_skip_"
        # Fallback для переводов
        return "transfer"

    # 3. Дивиденды
    if "дивиденд" in oper_lower or "дивиденд" in comment_lower:
        return "dividend"

    # 4. Купоны
    if "купон" in oper_lower or "купон" in comment_lower:
        return "coupon"

    # 5. Погашение облигации
    if "погашение облигации" in oper_lower or "погашение облигации" in comment_lower:
        return "repayment"

    # 6. Частичное погашение
    if "частичное погашение" in oper_lower or "частичное погашение" in comment_lower:
        return "amortization"

    # 7. Приход/Вывод ДС
    if "приход дс" in oper_lower:
        return "deposit"
    if "вывод дс" in oper_lower:
        return "withdrawal"

    # 8. Комиссии
    if "комисси" in oper_lower:
        return "commission_refund" if payment_sum > 0 else "commission"

    # 9. Проценты по займам
    if "проценты по займам" in oper_lower:
        return "other_income" if payment_sum != 0 else "other_expense"

    # 10. Вознаграждение компании
    if "вознаграждение компании" in oper_lower:
        return "other_income"

    # Fallback: возвращаем оригинальный oper_type
    return oper_type_val or "unknown"


# --- main parsing logic (namespace-agnostic) ---
def _parse_root(root: ET.Element) -> Tuple[List[OperationDTO], Dict[str, Any]]:
    ops: List[OperationDTO] = []
    total_rn = 0
    skipped_count = 0
    example_comments: List[str] = []

    try:
        # find Report element with name or fallback to root
        report_elem = None
        for rep in root.iter():
            if _local_name(rep.tag) == "Report":
                name = rep.attrib.get("Name") or rep.attrib.get("name")
                if name and "BrokerMoneyMove" in name:
                    report_elem = rep
                    break
                if report_elem is None:
                    report_elem = rep
        if report_elem is None:
            report_elem = root

        # collect settlement_date nodes by local name (namespace-agnostic)
        settlement_nodes = _collect_elements_by_local_name(report_elem, "settlement_date")
        logger.info("Found %d settlement_date nodes (namespace-agnostic search)", len(settlement_nodes))

        for s in settlement_nodes:
            settlement_date_attr = _safe_attr(s, "settlement_date")
            settlement_date_dt = _parse_iso_datetime(settlement_date_attr)

            # collect rn nodes under this settlement_date (local name 'rn')
            rn_nodes = [el for el in s.iter() if _local_name(el.tag) == "rn"]
            if not rn_nodes:
                rn_nodes = [c for c in list(s) if _local_name(c.tag) == "rn"]

            for rn in rn_nodes:
                total_rn += 1
                last_update_attr = _safe_attr(rn, "last_update")
                rn_last_update_dt = _parse_iso_datetime(last_update_attr)

                # find oper_type under rn
                oper_type_elem = None
                for el in rn.iter():
                    if _local_name(el.tag) == "oper_type":
                        oper_type_elem = el
                        break
                oper_type_val = _safe_attr(oper_type_elem, "oper_type") if oper_type_elem is not None else ""

                # comment may be inside oper_type or directly under rn
                comment_elem = None
                if oper_type_elem is not None:
                    comment_elem = _find_first_descendant_by_local_name(oper_type_elem, "comment")
                if comment_elem is None:
                    comment_elem = _find_first_descendant_by_local_name(rn, "comment")
                comment_text = _safe_attr(comment_elem, "comment") or ""

                if comment_text and len(example_comments) < 5:
                    example_comments.append(comment_text)

                # currency & amount extraction
                currency, amount_decimal, pcode_debug = _extract_currency_and_amount(comment_elem)
                textbox = _extract_textbox_values(comment_elem)

                # fallback to textbox values if p_code absent
                if amount_decimal is None:
                    if textbox.get("money_volume"):
                        amount_decimal = _parse_decimal(textbox.get("money_volume"))
                    elif textbox.get("all_volume"):
                        amount_decimal = _parse_decimal(textbox.get("all_volume"))
                    elif textbox.get("debet_volume"):
                        amount_decimal = _parse_decimal(textbox.get("debet_volume"))

                payment_sum = _decimal_to_float(amount_decimal) if amount_decimal is not None else 0.0
                commission_val = 0.0

                # improved reg_number & operation_id extraction
                reg_number, operation_id = _extract_reg_and_opid_improved(comment_text, oper_type_val)

                # ticker: blank when not reliable
                ticker = ""

                # isin heuristic (keep empty if not found)
                isin = ""
                if comment_text:
                    m_isin = RE_ISIN.search(comment_text)
                    if m_isin:
                        isin = m_isin.group(0)

                # decide label source for skip/mapping
                label_source = (oper_type_val or "").strip() or (comment_text or "").strip()

                # 1) Проверка на SKIP_OPERATIONS (case-insensitive substring match)
                should_skip = False
                if label_source:
                    low = label_source.lower()
                    for skip_pattern in SKIP_OPERATIONS:
                        if skip_pattern.lower() in low:
                            should_skip = True
                            logger.debug("Skipping operation by SKIP_OPERATIONS match: %s (pattern=%s)", label_source,
                                         skip_pattern)
                            break

                # также пропускаем пустые операции с нулевыми суммами
                if not label_source and payment_sum == 0.0:
                    should_skip = True
                    logger.debug("Skipping empty/zero operation (no label, zero amount)")

                if should_skip:
                    skipped_count += 1
                    continue

                # 2) Определение operation_type с новой логикой
                mapped_type = _determine_operation_type(oper_type_val, comment_text, payment_sum)

                # Специальная метка для пропуска
                if mapped_type == "_skip_":
                    skipped_count += 1
                    logger.debug("Skipping operation: %s (промежуточные дивиденды)", comment_text[:50])
                    continue

                # date field prefer settlement_date else rn_last_update
                date_field = settlement_date_dt or rn_last_update_dt

                dto = OperationDTO(
                    date=date_field,
                    operation_type=mapped_type,
                    payment_sum=payment_sum,
                    currency=currency or "",
                    ticker=ticker or "",
                    isin=isin or "",
                    reg_number=reg_number or "",
                    price=0.0,
                    quantity=0,
                    aci=0.0,
                    comment=comment_text or "",
                    operation_id=operation_id or "",
                    commission=commission_val,
                )

                ops.append(dto)

    except Exception as e:
        logger.exception("Error while parsing financial operations: %s", e)
        return [], {"total_rows": total_rn, "parsed": len(ops), "skipped": skipped_count, "error": str(e)}

    stats = {"total_rows": total_rn, "parsed": len(ops), "skipped": skipped_count, "example_comments": example_comments}
    return ops, stats


# --- public API ---
def parse_fin_operations_from_xml(path_or_bytes: Union[str, bytes]) -> Tuple[List[OperationDTO], Dict[str, Any]]:
    """
    Parse financial operations from XML.
    Accepts:
      - path_or_bytes: path to file (str), bytes containing XML, or string with XML.
    Returns:
      - (list_of_OperationDTO, stats_dict)
    """
    tree_root = None
    try:
        if isinstance(path_or_bytes, (bytes, bytearray)):
            data = bytes(path_or_bytes)
            # strip UTF-8 BOM if present
            if data.startswith(b'\xef\xbb\xbf'):
                logger.debug("Detected BOM in input bytes; stripping BOM before parsing")
                data = data.lstrip(b'\xef\xbb\xbf')
            tree_root = ET.fromstring(data)
        elif isinstance(path_or_bytes, str):
            s = path_or_bytes.strip()
            if s.startswith("<"):
                tree_root = ET.fromstring(s)
            else:
                tree = ET.parse(path_or_bytes)
                tree_root = tree.getroot()
        else:
            raise ValueError("Unsupported type for path_or_bytes: %s" % type(path_or_bytes))
    except Exception as e:
        logger.exception("Failed to read XML input: %s", e)
        return [], {"total_rows": 0, "parsed": 0, "skipped": 0, "error": f"XML read error: {e}"}

    return _parse_root(tree_root)
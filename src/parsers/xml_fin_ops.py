# src/parsers/xml_fin_ops.py
from __future__ import annotations
from typing import Optional, List, Dict, Any, Tuple, Union
from datetime import datetime
import xml.etree.ElementTree as ET
from decimal import Decimal, InvalidOperation
import re
import logging

from src.OperationDTO import OperationDTO
from src.constants import (
    SKIP_OPERATIONS,
    VALID_OPERATIONS,
    OPERATION_TYPE_MAP,
    SPECIAL_OPERATION_HANDLERS,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# --- regexes / heuristics ---
RE_ISIN = re.compile(r"\b[A-Z]{2}[A-Z0-9]{10}\b")
RE_LONG_DIGITS = re.compile(r"\b(\d{5,})\b")
RE_OPERATION_ID = RE_LONG_DIGITS  # reuse for operation id
# match digits between hyphens like 4B02-02-00116-L  -> capture 00116 (leading zeros preserved)
RE_REG_HYPHEN = re.compile(r"-0*([0-9]{2,})-")
# fallback smaller group (2..4) if needed
RE_REG_HYPHEN_MIN2 = re.compile(r"-([0-9]{2,})-")

# --- helpers ---
def _local_name(tag: str) -> str:
    """Return local name of element tag, without namespace."""
    if tag is None:
        return ""
    return tag.split("}")[-1] if "}" in tag else tag

def _safe_attr(elem: Optional[ET.Element], name: str) -> Optional[str]:
    if elem is None:
        return None
    # attribute names are usually not namespaced in these reports
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
    Improved logic to extract reg_number and operation_id:
    - reg_number: first try RE_REG_HYPHEN (digits between hyphens, e.g. 4B02-02-00116-L -> 00116),
                  then fallback to long digit sequence (>=5 digits) from comment.
    - operation_id: prefer long digit sequence (>=5) from oper_type, else from comment.
    Returns (reg_number, operation_id) as strings (possibly empty).
    """
    reg = ""
    opid = ""

    if comment_text:
        # try hyphen-based reg extraction
        m = RE_REG_HYPHEN.search(comment_text)
        if m:
            reg = m.group(1)
        else:
            m2 = RE_REG_HYPHEN_MIN2.search(comment_text)
            if m2:
                reg = m2.group(1)

    # operation id preference: from oper_type first
    if oper_type_text:
        mo = RE_OPERATION_ID.search(oper_type_text)
        if mo:
            opid = mo.group(1)

    # if no opid found in oper_type, try comment
    if not opid and comment_text:
        mo2 = RE_OPERATION_ID.search(comment_text)
        if mo2:
            opid = mo2.group(1)

    # fallback reg: if not set yet, and there's a long digit in comment, use it
    if not reg and comment_text:
        mlong = RE_LONG_DIGITS.search(comment_text)
        if mlong:
            # ensure not to take opid if opid already present and same substring? still use first for reg
            reg = mlong.group(1)

    # final normalization: keep as-is (preserve leading zeros from hyphen pattern).
    return reg or "", opid or ""

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

                payment_sum = 0.0
                commission_val = 0.0
                if amount_decimal is not None:
                    # detect commission-like operations by oper_type text
                    is_commission = False
                    if oper_type_val and "комисси" in oper_type_val.lower():
                        is_commission = True
                    if is_commission:
                        commission_val = abs(_decimal_to_float(amount_decimal))
                        payment_sum = 0.0
                    else:
                        payment_sum = _decimal_to_float(amount_decimal)

                # improved reg_number & operation_id extraction
                reg_number, operation_id = _extract_reg_and_opid_improved(comment_text, oper_type_val)

                # ticker: user requested blank ticker when not reliable
                ticker = ""

                # isin heuristic (keep empty if not found)
                isin = ""
                if comment_text:
                    m_isin = RE_ISIN.search(comment_text)
                    if m_isin:
                        isin = m_isin.group(0)

                # decide label source for skip/mapping (use oper_type primarily, fallback comment)
                label_source = (oper_type_val or "").strip() or (comment_text or "").strip()

                # 1) skip by SKIP_OPERATIONS (case-insensitive substring match)
                should_skip = False
                if label_source:
                    low = label_source.lower()
                    for skip_pattern in SKIP_OPERATIONS:
                        if skip_pattern.lower() in low:
                            should_skip = True
                            logger.debug("Skipping operation by SKIP_OPERATIONS match: %s (pattern=%s)", label_source, skip_pattern)
                            break

                # also optionally skip when label empty and zero amounts
                if not label_source and (payment_sum == 0.0 and commission_val == 0.0):
                    should_skip = True
                    logger.debug("Skipping empty/zero operation (no label, zero amount)")

                if should_skip:
                    skipped_count += 1
                    continue

                # 2) map operation_type using OPERATION_TYPE_MAP and SPECIAL_OPERATION_HANDLERS
                mapped_type = None
                # try direct mapping
                if label_source in OPERATION_TYPE_MAP:
                    mapped_type = OPERATION_TYPE_MAP[label_source]
                # try substring matching
                if mapped_type is None:
                    for k, v in OPERATION_TYPE_MAP.items():
                        if k.lower() in label_source.lower():
                            mapped_type = v
                            break

                # special handlers
                if mapped_type is None:
                    for k, handler in SPECIAL_OPERATION_HANDLERS.items():
                        if k.lower() in label_source.lower():
                            try:
                                mapped_type = handler(payment_sum, None)
                            except Exception as ex:
                                logger.exception("SPECIAL_OPERATION_HANDLERS failed for %s: %s", k, ex)
                            break

                # fallback: if label matches VALID_OPERATIONS use it (or substring)
                if mapped_type is None:
                    for k in VALID_OPERATIONS:
                        if k.lower() in label_source.lower():
                            mapped_type = k
                            break

                if mapped_type is None:
                    mapped_type = label_source or ""

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

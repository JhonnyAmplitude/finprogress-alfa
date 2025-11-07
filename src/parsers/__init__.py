# src/parsers/__init__.py
from .xml_trades import parse_trades_from_xml
from .xml_fin_ops import parse_fin_operations_from_xml
from .xml_transfers import parse_transfers_from_xml

__all__ = [
    "parse_trades_from_xml",
    "parse_fin_operations_from_xml",
    "parse_transfers_from_xml",
]
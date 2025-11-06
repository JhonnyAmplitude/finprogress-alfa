from datetime import datetime
from dataclasses import dataclass, asdict, field
from typing import Optional, Union

@dataclass
class OperationDTO:
    date: Optional[Union[str, datetime]]
    operation_type: str
    payment_sum: Union[str, float]
    currency: str
    ticker: Optional[str] = ""
    isin: Optional[str] = ""
    reg_number: Optional[str] = ""
    price: Optional[float] = 0.0
    quantity: Optional[int] = 0
    aci: Optional[Union[str, float]] = 0.0
    comment: Optional[str] = ""
    operation_id: Optional[str] = ""
    commission: Optional[float] = 0.0
    _sort_key: Optional[str] = field(init=False, default=None)

    def __post_init__(self):
        if self.date:
            if isinstance(self.date, str) and len(self.date) == 10:
                self.date += " 00:00:00"
            self._sort_key = str(self.date)
        else:
            self._sort_key = ""

        if isinstance(self.aci, str):
            try:
                self.aci = float(self.aci.replace(',', '.'))
            except ValueError:
                self.aci = 0.0

        if isinstance(self.commission, str):
            try:
                self.commission = float(self.commission.replace(',', '.'))
            except Exception:
                try:
                    self.commission = float(self.commission)
                except Exception:
                    self.commission = 0.0
        elif self.commission is None:
            self.commission = 0.0

    def to_dict(self):
        result = asdict(self)
        if isinstance(self.date, datetime):
            result['date'] = self.date.isoformat()
        result.pop("_sort_key", None)
        return result

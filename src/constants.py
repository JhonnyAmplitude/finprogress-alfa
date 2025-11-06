from typing import Any

#  Валидные операции, которые обрабатываются
VALID_OPERATIONS = {
    "Вознаграждение компании",
    "Дивиденды",
    "НДФЛ",
    "Погашение купона",
    "Погашение облигации",
    "Приход ДС",
    "Частичное погашение облигации",
    "Вывод ДС",
}

#  Операции, которые нужно игнорировать
SKIP_OPERATIONS = {
    "Расчеты по сделке",
    "Займы \"овернайт\"",
    "НКД по сделке",
    "Покупка/Продажа",
    "Покупка/Продажа (репо)",
    "Переводы между площадками",
}

#  Маппинг строковых названий операций на типы
OPERATION_TYPE_MAP = {
    "Дивиденды": "dividend",
    "Купоны": "coupon",
    "Погашение облигации": "repayment",
    "Приход ДС": "deposit",
    "Частичное погашение облигации": "amortization",
    "Вывод ДС": "withdrawal",
}

#  Обработка операций, тип которых зависит от контекста (доход/расход)
SPECIAL_OPERATION_HANDLERS = {
    'Проценты по займам "овернайт"': lambda i, e: "other_income" if is_nonzero(i) else "other_expense",
    'Проценты по займам "овернайт ЦБ"': lambda i, e: "other_income" if is_nonzero(i) else "other_expense",
    "Комиссия по сделке": lambda i, e: "commission_refund" if is_nonzero(i) else "commission",
    "НДФЛ": lambda i, e: "refund" if is_nonzero(i) else "withholding",
}


CURRENCY_DICT = {
    "AED": "AED", "AMD": "AMD", "BYN": "BYN", "CHF": "CHF", "CNY": "CNY",
    "EUR": "EUR", "GBP": "GBP", "HKD": "HKD", "JPY": "JPY", "KGS": "KGS",
    "KZT": "KZT", "NOK": "NOK", "RUB": "RUB", "РУБЛЬ": "RUB", "Рубль": "RUB",
    "SEK": "SEK", "TJS": "TJS", "TRY": "TRY", "USD": "USD", "UZS": "UZS",
    "XAG": "XAG", "XAU": "XAU", "ZAR": "ZAR"
}


def is_nonzero(value: Any) -> bool:
    """
    Проверка на значение, отличное от нуля.
    """
    try:
        return float(str(value).replace(",", ".").replace(" ", "")) != 0
    except (ValueError, TypeError):
        return False
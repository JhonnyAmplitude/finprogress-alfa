from typing import Any

# Операции, которые нужно игнорировать (уже учтены в сделках)
SKIP_OPERATIONS = {
    "Расчеты по сделке",
    "Комиссия по сделке",
    "НКД по сделке",
    "Покупка/Продажа",
    "Покупка/Продажа (репо)",
    "Переводы между площадками",
}

# Прямой маппинг названий операций на типы
OPERATION_TYPE_MAP = {
    "Дивиденды": "dividend",
    "Купонный доход": "coupon",
    "Погашение купона": "coupon",
    "Погашение облигации": "repayment",
    "Полное погашение номинала": "repayment",
    "Частичное погашение облигации": "amortization",
    "Частичное погашение номинала": "amortization",
    "Приход ДС": "deposit",
    "Вывод ДС": "withdrawal",
    "Вознаграждение компании": "other_income",
}

# Валюты
CURRENCY_DICT = {
    "AED": "AED", "AMD": "AMD", "BYN": "BYN", "CHF": "CHF", "CNY": "CNY",
    "EUR": "EUR", "GBP": "GBP", "HKD": "HKD", "JPY": "JPY", "KGS": "KGS",
    "KZT": "KZT", "NOK": "NOK", "RUB": "RUB", "РУБЛЬ": "RUB", "Рубль": "RUB",
    "SEK": "SEK", "TJS": "TJS", "TRY": "TRY", "USD": "USD", "UZS": "UZS",
    "XAG": "XAG", "XAU": "XAU", "ZAR": "ZAR"
}

def is_negative(value: Any) -> bool:
    """
    Проверяет, что значение — отрицательное число.
    """
    try:
        num = float(str(value).replace(",", ".").replace(" ", ""))
        return num < 0
    except (ValueError, TypeError):
        return False
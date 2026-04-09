import logging
from time import sleep

from pybinbot import ExchangeId, MarketType
from requests import Response

from shared.exceptions import BinbotError, InvalidSymbol


def clamp(value: float, low: float = -1.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value)))


def non_negative(value: float) -> float:
    return max(0.0, float(value))


def safe_pct(current: float, previous: float) -> float:
    if previous == 0:
        return 0.0
    return (float(current) - float(previous)) / abs(float(previous))


def safe_format(value, spec: str = ".2f") -> str:
    """Safely format a value with a numeric format specification.

    Attempts to coerce the input to float and apply the provided format spec.
    If coercion fails (TypeError/ValueError), it returns the plain string
    representation of the value to avoid raising:
        ValueError: Unknown format code 'f' for object of type 'str'

    Parameters
    ----------
    value : Any
        The value to format.
    spec : str, default '.2f'
        The numeric format specifier (e.g. '.2f', '.4f').

    Returns
    -------
    str
        Formatted string or fallback string(value) when formatting fails.
    """
    try:
        return format(float(value), spec)
    except (TypeError, ValueError):
        return str(value)


def handle_binance_errors(response: Response):
    """
    Handles:
    - HTTP codes, not authorized, rate limits...
    - Bad request errors, binance internal e.g. {"code": -1013, "msg": "Invalid quantity"}
    - Binbot internal errors - bot errors, returns "errored"

    """
    response.raise_for_status()
    if "x-mbx-used-weight-1m" in response.headers:
        logging.info(
            f"Request to {response.url} weight: {response.headers.get('x-mbx-used-weight-1m')}"
        )
    # Calculate request weights and pause half of the way (1200/2=600)
    if (
        "x-mbx-used-weight-1m" in response.headers
        and int(response.headers["x-mbx-used-weight-1m"]) > 1000
    ) or response.status_code == 418:
        logging.warning("Request weight limit prevention pause, waiting 1 min")
        sleep(120)

    content = response.json()

    if "code" in content:
        if content["code"] == 200 or content["code"] == "000000":
            return content

        if content["code"] == -1121:
            raise InvalidSymbol("Binance error, invalid symbol")

    elif "error" in content and content["error"] == 1:
        raise BinbotError(f"Binbot internal error: {content['message']}")

    else:
        return content


def build_links_msg(
    env: str, exchange: ExchangeId, market_type: MarketType, symbol: str
) -> tuple[str, str]:
    """
    Builds links for the teleagram message
    - env: getenv("ENV"), check the .env file in different environments
    - exchange: ExchangeId, determined in the autotrade_settings
    - market_type: MarketType, FUTURES, SPOT, determined in the bot
    - symbol: can be Binance style or Kucoin style, up to the user of this function to pass the right symbol format. This function will try to handle both.
    """

    if exchange == ExchangeId.BINANCE:
        exchange_link = f"https://www.binance.com/en/trade/{symbol}"

    if exchange == ExchangeId.KUCOIN and market_type == MarketType.FUTURES:
        exchange_link = f"https://www.kucoin.com/trade/futures/{symbol}"
    elif exchange == ExchangeId.KUCOIN and market_type == MarketType.SPOT:
        exchange_link = f"https://www.kucoin.com/trade/{symbol}"

    terminal_host = (
        "https://terminal.binbot.in" if env == "production" else "http://localhost:3000"
    )
    terminal_link = (
        market_type == MarketType.FUTURES
        and f"{terminal_host}/bots/futures/new/{symbol}"
        or f"{terminal_host}/bots/new/{symbol}"
    )

    return exchange_link, terminal_link

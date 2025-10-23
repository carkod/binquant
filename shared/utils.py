import logging
import math
import os
import re
from datetime import datetime
from decimal import Decimal
from time import sleep
from zoneinfo import ZoneInfo

from aiohttp import ClientResponse
from requests import Response

from shared.exceptions import BinbotError, InvalidSymbol


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


def round_numbers(value, decimals: int = 6) -> float:
    decimal_points = 10 ** int(decimals)
    number = float(value)
    result = math.floor(number * decimal_points) / decimal_points
    if decimals == 0:
        result = float(result)
    return result


def suppress_trailing(value: str | float | int) -> float:
    """
    Supress trilling 0s
    this function will not round the number
    e.g. 3.140, 3.140000004

    also supress scientific notation
    e.g. 2.05-5
    """
    value = float(value)
    # supress scientific notation
    number = float(f"{value:f}")
    number = float(f"{number:g}")
    return number


def round_numbers_ceiling(value, decimals: int = 6) -> float:
    decimal_points = 10 ** int(decimals)
    number = float(value)
    result = math.ceil(number * decimal_points) / decimal_points
    if decimals == 0:
        result = float(result)
    return result


def interval_to_millisecs(interval: str) -> int:
    time, notation = re.findall(r"[A-Za-z]+|\d+", interval)
    if notation == "m":
        # minutes
        return int(time) * 60 * 1000

    if notation == "h":
        # hours
        return int(time) * 60 * 60 * 1000

    if notation == "d":
        # day
        return int(time) * 24 * 60 * 60 * 1000

    if notation == "w":
        # weeks
        return int(time) * 5 * 24 * 60 * 60 * 1000

    if notation == "M":
        # month
        return int(time) * 30 * 24 * 60 * 60 * 1000

    return 0


def suppress_notation(num: float, precision: int = 0) -> str:
    """
    Supress scientific notation
    e.g. 8e-5 = "0.00008"
    """
    num = float(num)
    if precision >= 0:
        decimal_points = precision
    else:
        decimal_points = int(Decimal(num).as_tuple().exponent * -1)
    return f"{num:.{str(decimal_points)}f}"


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


def timestamp_to_datetime(timestamp: str | int) -> str:
    """
    Convert a timestamp in milliseconds to seconds
    to match expectation of datetime
    Then convert to a human readable format.

    Parameters
    ----------
    timestamp : str | int
        The timestamp in milliseconds. Always in London timezone
        to avoid inconsistencies across environments (Github, prod, local)
    """
    format = "%Y-%m-%d %H:%M:%S"
    timestamp = int(round_numbers_ceiling(int(timestamp) / 1000, 0))
    dt = datetime.fromtimestamp(
        timestamp, tz=ZoneInfo(os.getenv("TZ", "Europe/London"))
    )
    return dt.strftime(format)


async def aio_response_handler(response: ClientResponse):
    content = await response.json()
    return content

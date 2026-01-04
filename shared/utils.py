import logging
import re
from decimal import Decimal
from time import sleep

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


async def aio_response_handler(response: ClientResponse):
    content = await response.json()
    return content

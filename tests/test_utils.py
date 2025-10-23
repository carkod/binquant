import asyncio
import math
from unittest.mock import MagicMock

import aiohttp
import pytest
import requests

from shared.utils import (
    aio_response_handler,
    handle_binance_errors,
    interval_to_millisecs,
    round_numbers,
    round_numbers_ceiling,
    safe_format,
    suppress_notation,
    suppress_trailing,
    timestamp_to_datetime,
)


# Helper to create a mock requests.Response
def make_response(status_code=200, headers=None, json_data=None, url="http://test"):
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.headers = headers or {}
    resp.url = url
    resp.json.return_value = json_data or {}
    resp.raise_for_status.side_effect = (
        None if status_code < 400 else Exception("HTTP error")
    )
    return resp


# safe_format
def test_safe_format_float():
    assert safe_format(3.14159) == "3.14"


def test_safe_format_str():
    assert safe_format("abc") == "abc"


def test_safe_format_int():
    assert safe_format(42) == "42.00"


def test_round_numbers():
    assert round_numbers(3.1415926535, 2) == 3.14
    assert round_numbers(3.1415926535, 0) == 3.0
    assert round_numbers(2.9999, 0) == 2.0


def test_suppress_trailing():
    assert math.isclose(suppress_trailing("3.1400000"), 3.14, rel_tol=1e-9)
    assert math.isclose(suppress_trailing(2.05e-5), 2.1e-5, rel_tol=1e-9)
    assert math.isclose(suppress_trailing(3.140), 3.14, rel_tol=1e-9)


def test_round_numbers_ceiling():
    assert round_numbers_ceiling(3.1415926535, 2) == 3.15
    assert round_numbers_ceiling(2.9999, 0) == 3.0


def test_interval_to_millisecs():
    assert interval_to_millisecs("1m") == 60000
    assert interval_to_millisecs("2h") == 7200000
    assert interval_to_millisecs("1d") == 86400000
    assert interval_to_millisecs("1w") == 432000000
    assert interval_to_millisecs("1M") == 2592000000
    assert interval_to_millisecs("5x") == 0


def test_suppress_notation():
    assert suppress_notation(8e-5, 5) == "0.00008"
    assert suppress_notation(123.456, 2) == "123.46"
    # Compare only the first 8 characters for precision-sensitive case
    assert suppress_notation(0.0001234, -1)[:8] == "0.000123"


def test_handle_binance_errors_success():
    resp = make_response(
        headers={"x-mbx-used-weight-1m": "10"}, json_data={"code": 200, "msg": "ok"}
    )
    assert handle_binance_errors(resp) == {"code": 200, "msg": "ok"}


def test_handle_binance_errors_invalid_symbol():
    resp = make_response(json_data={"code": -1121, "msg": "Invalid symbol"})
    from shared.exceptions import InvalidSymbol

    with pytest.raises(InvalidSymbol):
        handle_binance_errors(resp)


def test_handle_binance_errors_binbot_error():
    resp = make_response(json_data={"error": 1, "message": "fail"})
    from shared.exceptions import BinbotError

    with pytest.raises(BinbotError):
        handle_binance_errors(resp)


def test_handle_binance_errors_other():
    resp = make_response(json_data={"foo": "bar"})
    assert handle_binance_errors(resp) == {"foo": "bar"}


def test_timestamp_to_datetime_utc():
    # 1633046400000 ms = 2021-10-01 01:00:00 in Europe/London (BST)
    assert timestamp_to_datetime(1633046400000) == "2021-10-01 01:00:00"


def make_aio_response(json_data):
    resp = MagicMock(spec=aiohttp.ClientResponse)

    async def json():
        return json_data

    resp.json = json
    return resp


@pytest.mark.asyncio
def test_aio_response_handler():
    resp = make_aio_response({"foo": "bar"})
    result = asyncio.run(aio_response_handler(resp))
    assert result == {"foo": "bar"}

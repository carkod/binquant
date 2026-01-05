import asyncio
from unittest.mock import MagicMock

import aiohttp
import pytest
import requests

from shared.utils import (
    aio_response_handler,
    handle_binance_errors,
    safe_format,
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

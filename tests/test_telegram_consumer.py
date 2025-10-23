from unittest.mock import AsyncMock, patch

import pytest

from consumers.telegram_consumer import TelegramConsumer


@patch("consumers.telegram_consumer.Bot", autospec=True)
class TestTelegramConsumer:
    def test_parse_signal_valid(self, MockBot):
        consumer = TelegramConsumer()
        result = '{"msg": "Hello"}'
        assert consumer.parse_signal(result) == "Hello"

    def test_parse_signal_invalid(self, MockBot):
        consumer = TelegramConsumer()
        result = '{"foo": "bar"}'
        assert consumer.parse_signal(result) is None

    @pytest.mark.asyncio
    async def test_send_msg(self, MockBot):
        mock_bot_instance = MockBot.return_value
        mock_bot_instance.__aenter__.return_value = mock_bot_instance
        mock_bot_instance.send_message = AsyncMock()
        consumer = TelegramConsumer()
        consumer.bot = mock_bot_instance
        with patch.object(consumer, "parse_signal", return_value="Hello"):
            await consumer.send_msg('{"msg": "Hello"}')
        mock_bot_instance.send_message.assert_awaited()

    @pytest.mark.asyncio
    async def test_send_signal(self, MockBot):
        mock_bot_instance = MockBot.return_value
        mock_bot_instance.send_message = AsyncMock()
        consumer = TelegramConsumer()
        consumer.bot = mock_bot_instance
        payload = (
            "{"
            '"msg": "<b>Signal</b>\\n",'
            '"symbol": "BTCUSDT",'
            '"algo": "test_algo",'
            '"bb_spreads": {"bb_high": 1.0, "bb_mid": 0.5, "bb_low": 0.1}'
            "}"
        )
        await consumer.send_signal(payload)
        mock_bot_instance.send_message.assert_awaited()

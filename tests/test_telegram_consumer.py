from unittest.mock import AsyncMock, patch

import pytest
from telegram.error import RetryAfter

from consumers.telegram_consumer import TelegramConsumer

consumer = TelegramConsumer(token="fake_token", chat_id="fake_chat_id")


@patch("consumers.telegram_consumer.Bot")
class TestTelegramConsumer:
    def test_parse_signal_valid(self, MockBot):
        result = '{"msg": "Hello"}'
        assert consumer.parse_signal(result) == "Hello"

    def test_parse_signal_invalid(self, MockBot):
        result = '{"foo": "bar"}'
        assert consumer.parse_signal(result) is None

    @pytest.mark.asyncio
    async def test_send_msg(self, MockBot):
        mock_bot_instance = MockBot.return_value
        mock_bot_instance.send_message = AsyncMock()
        consumer.bot = mock_bot_instance
        await consumer.send_msg("Hello")
        mock_bot_instance.send_message.assert_awaited()

    @pytest.mark.asyncio
    async def test_send_signal(self, MockBot):
        mock_bot_instance = MockBot.return_value
        mock_bot_instance.send_message = AsyncMock()
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

    @pytest.mark.asyncio
    async def test_send_msg_retries_after_flood_control(self, MockBot):
        mock_bot_instance = MockBot.return_value
        mock_bot_instance.send_message = AsyncMock(side_effect=[RetryAfter(0), None])
        telegram_consumer = TelegramConsumer(token="fake_token", chat_id="fake_chat_id")
        telegram_consumer._min_send_interval_seconds = 0
        telegram_consumer._retry_after_pad_seconds = 0
        telegram_consumer.bot = mock_bot_instance

        await telegram_consumer.send_msg("Hello")

        assert mock_bot_instance.send_message.await_count == 2

    @pytest.mark.asyncio
    async def test_dispatch_signal_deduplicates_matching_signal_key(self, MockBot):
        mock_bot_instance = MockBot.return_value
        mock_bot_instance.send_message = AsyncMock()
        telegram_consumer = TelegramConsumer(token="fake_token", chat_id="fake_chat_id")
        telegram_consumer._min_send_interval_seconds = 0
        telegram_consumer.bot = mock_bot_instance

        first_message = """
            - [production] <strong>#top_loser_breadth algorithm</strong> #TAUSDTM
            - Action: LONG ENTRY
            - Current price: 0.071
            - Strategy: top_loser_breadth
            - Autotrade route: market_trend_up_symbol_trend_up
            - Autotrade is enabled
        """
        duplicate_message = """
            - [production] <strong>#top_loser_breadth algorithm</strong> #TAUSDTM
            - Action: LONG ENTRY
            - Current price: 0.074
            - Strategy: top_loser_breadth
            - Autotrade route: market_trend_up_symbol_trend_up
            - Autotrade is enabled
        """

        sent = await telegram_consumer.dispatch_signal(first_message)
        recent_duplicate = await telegram_consumer.dispatch_signal(duplicate_message)

        assert sent is True
        assert recent_duplicate is False
        assert mock_bot_instance.send_message.await_count == 1

    @pytest.mark.asyncio
    async def test_dispatch_signal_rejects_disabled_strategy(self, MockBot):
        mock_bot_instance = MockBot.return_value
        mock_bot_instance.send_message = AsyncMock()
        telegram_consumer = TelegramConsumer(token="fake_token", chat_id="fake_chat_id")
        telegram_consumer.bot = mock_bot_instance

        sent = await telegram_consumer.dispatch_signal(
            """
            - [production] <strong>#higher_low_pattern algorithm</strong> #TAUSDTM
            - Action: LONG ENTRY
            """
        )

        assert sent is False
        mock_bot_instance.send_message.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "algorithm",
        [
            "top_gainer_breadth",
            "top_loser_breadth",
            "top_gainer_early_momentum",
            "top_loser_early_momentum",
        ],
    )
    async def test_dispatch_signal_allows_selected_strategies(self, MockBot, algorithm):
        mock_bot_instance = MockBot.return_value
        mock_bot_instance.send_message = AsyncMock()
        telegram_consumer = TelegramConsumer(token="fake_token", chat_id="fake_chat_id")
        telegram_consumer._min_send_interval_seconds = 0
        telegram_consumer.bot = mock_bot_instance

        sent = await telegram_consumer.dispatch_signal(
            f"""
            - [production] <strong>#{algorithm} algorithm</strong> #TAUSDTM
            - Action: LONG ENTRY
            """
        )

        assert sent is True
        mock_bot_instance.send_message.assert_awaited_once()

    def test_sanitize_html_preserves_supported_tags(self, MockBot):

        sanitized = consumer._sanitize_html(
            "<strong>Signal</strong> ratio <= 0.45 and >= 0.55"
        )

        assert sanitized == ("<strong>Signal</strong> ratio &lt;= 0.45 and &gt;= 0.55")

    def test_sanitize_html_preserves_existing_entities(self, MockBot):

        sanitized = consumer._sanitize_html("RSI (14) &lt; 30 and MACD &gt; -1")

        assert sanitized == "RSI (14) &lt; 30 and MACD &gt; -1"

    def test_sanitize_html_preserves_anchor_links(self, MockBot):

        sanitized = consumer._sanitize_html(
            "<a href='https://www.kucoin.com/trade/futures/TRUTHUSDTM'>KuCoin</a>"
        )

        assert (
            sanitized
            == '<a href="https://www.kucoin.com/trade/futures/TRUTHUSDTM">KuCoin</a>'
        )

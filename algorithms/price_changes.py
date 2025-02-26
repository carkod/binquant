import os
from typing import TYPE_CHECKING
from models.signals import SignalsConsumer, BollinguerSpread
from shared.enums import KafkaTopics, MarketDominance, Strategy

if TYPE_CHECKING:
    from producers.technical_indicators import TechnicalIndicators


def price_rise_15(
    cls: "TechnicalIndicators",
    close_price,
    symbol,
    prev_price,
    p_value,
):
    """
    Price increase/decrease algorithm

    https://www.binance.com/en/support/faq/understanding-top-movers-statuses-on-binance-spot-trading-18c97e8ab67a4e1b824edd590cae9f16
    """

    algo = "price_rise_15_rally_pullback"
    price_diff = (float(close_price) - float(prev_price)) / close_price

    if (
        cls.market_domination_reversal
        and cls.current_market_dominance == MarketDominance.LOSERS
        or cls.current_market_dominance == MarketDominance.NEUTRAL
    ):
        # market is bullish, most prices increasing,
        # but looks like it's dropping and going bearish (reversal)
        # candlesticks of this specific crypto are seeing 15% rise
        bot_strategy = Strategy.long

    else:
        btc_correlation = cls.get_btc_correlation(symbol=cls.symbol)
        if (
            cls.current_market_dominance == MarketDominance.LOSERS
            and btc_correlation > 0
        ):
            bot_strategy = Strategy.long
        else:
            return

    bb_high, bb_mid, bb_low = cls.bb_spreads()

    if 0.07 <= price_diff < 0.11:
        first_line = "<strong>Price increase</strong> over 7%"

    elif -0.07 <= price_diff < -0.11:
        first_line = f"<strong>{algo} #algorithm</strong> over 7%"

    else:
        return

    msg = f"""
    - [{os.getenv('ENV')}] {first_line} #{symbol}
    - Current price: {close_price}
    - P-value: {p_value}
    - Bollinguer bands spread: {(bb_high - bb_low) / bb_high}
    - Bot strategy {bot_strategy.value}
    - https://www.binance.com/en/trade/{symbol}
    - <a href='http://terminal.binbot.in/bots/new/{symbol}'>Dashboard trade</a>
    """

    value = SignalsConsumer(
        current_price=close_price,
        msg=msg,
        symbol=symbol,
        algo=algo,
        bot_strategy=bot_strategy,
        bb_spreads=BollinguerSpread(high=bb_high, mid=bb_mid, low=bb_low),
    )

    cls.producer.send(
        KafkaTopics.signals.value, value=value.model_dump_json()
    ).add_callback(cls.base_producer.on_send_success).add_errback(
        cls.base_producer.on_send_error
    )

    return

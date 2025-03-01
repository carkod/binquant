import os
from typing import TYPE_CHECKING

from models.signals import BollinguerSpread, SignalsConsumer
from shared.enums import KafkaTopics, MarketDominance, Strategy
from shared.utils import round_numbers

if TYPE_CHECKING:
    from producers.technical_indicators import TechnicalIndicators


def fast_and_slow_macd(
    cls: "TechnicalIndicators",
    close_price,
    macd,
    macd_signal,
    ma_7,
    ma_25,
    volatility,
):
    """
    Coinrule top performance rule
    https://web.coinrule.com/share-rule/Fast-EMA-above-Slow-EMA-with-MACD-6f8653

    """
    algo = "coinrule_fast_and_slow_macd"
    volatility = round_numbers(volatility, 6)
    spread = volatility
    bb_high, bb_mid, bb_low = cls.bb_spreads()

    # If volatility is too low, dynamic trailling will close too early with bb_spreads
    if macd > macd_signal and ma_7 > ma_25 and bb_high < 1 and bb_high > 0.001:
        bot_strategy = cls.bot_strategy
        btc_correlation = cls.get_btc_correlation(symbol=cls.symbol)
        if cls.current_market_dominance == MarketDominance.NEUTRAL:
            return

        if cls.market_domination_reversal:
            if (
                # market is bullish, most prices increasing,
                # but looks like it's dropping and going bearish (reversal)
                # candlesticks of this specific crypto are seeing a huge jump (candlstick jump algo)
                # and correlation with BTC is positive
                (
                    cls.current_market_dominance == MarketDominance.GAINERS
                    and btc_correlation > 0
                )
                # market is bearish (most prices decreasing),
                # but looks like it's picking up (reversal)
                # candlesticks of this specific crypto are seeing a huge jump (candlstick jump algo)
                # but correlation with BTC is negative
                or (
                    cls.current_market_dominance == MarketDominance.LOSERS
                    and btc_correlation < 0
                )
            ):
                bot_strategy = Strategy.margin_short
            else:
                bot_strategy = Strategy.long
        else:
            if (
                cls.current_market_dominance == MarketDominance.GAINERS
                and btc_correlation > 0
            ) or (
                cls.current_market_dominance == MarketDominance.LOSERS
                and btc_correlation < 0
            ):
                # market is bullish, most prices increasing,
                # but looks like it's dropping and going bearish (reversal)
                # candlesticks of this specific crypto are seeing a huge jump (candlstick jump algo)
                bot_strategy = Strategy.long
            else:
                bot_strategy = Strategy.margin_short

        msg = f"""
        - [{os.getenv('ENV')}] <strong>#{algo} algorithm</strong> #{cls.symbol}
        - Current price: {close_price}
        - Log volatility (log SD): {volatility}
        - Reversal? {"Yes" if cls.market_domination_reversal else "No"}
        - Strategy: {bot_strategy.value}
        - Bollinguer bands spread: {(bb_high - bb_low) / bb_high }
        - BTC correlation: {btc_correlation}
        - TimesGPT forecast: {cls.forecast}
        - <a href='https://www.binance.com/en/trade/{cls.symbol}'>Binance</a>
        - <a href='http://terminal.binbot.in/bots/new/{cls.symbol}'>Dashboard trade</a>
        """

        value = SignalsConsumer(
            spread=spread,
            current_price=close_price,
            msg=msg,
            symbol=cls.symbol,
            algo=algo,
            bot_strategy=bot_strategy,
            bb_spreads=BollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )

        cls.producer.send(
            KafkaTopics.signals.value, value=value.model_dump_json()
        ).add_callback(cls.base_producer.on_send_success).add_errback(
            cls.base_producer.on_send_error
        )

    pass


def buy_low_sell_high(
    cls: "TechnicalIndicators",
    close_price,
    rsi,
    ma_25,
    volatility,
):
    """
    Coinrule top performance rule
    https://web.coinrule.com/share-rule/Multi-Time-Frame-Buy-Low-Sell-High-Short-term-8f02df
    """
    volatility = round_numbers(volatility, 6)
    bb_high, bb_mid, bb_low = cls.bb_spreads()
    bot_strategy = cls.bot_strategy

    if rsi < 35 and close_price > ma_25 and volatility > 0.01:
        algo = "coinrule_buy_low_sell_high"
        volatility = round_numbers(volatility, 6)

        # market is bearish, most prices decreasing, (LOSERS)
        # but looks like it's picking up and going bullish (reversal)
        # candlesticks of this specific crypto are seeing a huge drop (candlstick drop algo)
        if (
            cls.market_domination_reversal
            and cls.current_market_dominance == MarketDominance.LOSERS
        ):
            bot_strategy = Strategy.long
        else:
            return

        # Second stage filtering when volatility is high
        # when volatility is high we assume that
        # difference between MA_7 and MA_25 is wide
        # if this is not the case it may fail to signal correctly
        bb_high, bb_mid, bb_low = cls.bb_spreads()

        msg = f"""
        - [{os.getenv('ENV')}] <strong>{algo} #algorithm</strong> #{cls.symbol}
        - Current price: {close_price}
        - Log volatility (log SD): {volatility}
        - Bollinguer bands spread: {(bb_high - bb_low) / bb_high }
        - Strategy: {bot_strategy.value}
        - Reversal? {"No reversal" if not cls.market_domination_reversal else "Positive" if cls.market_domination_reversal else "Negative"}
        - TimesGPT forecast: {cls.forecast}
        - https://www.binance.com/en/trade/{cls.symbol}
        - <a href='http://terminal.binbot.in/bots/new/{cls.symbol}'>Dashboard trade</a>
        """

        value = SignalsConsumer(
            current_price=close_price,
            msg=msg,
            symbol=cls.symbol,
            algo=algo,
            bot_strategy=bot_strategy,
            bb_spreads=BollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )

        cls.producer.send(
            KafkaTopics.signals.value, value=value.model_dump_json()
        ).add_callback(cls.base_producer.on_send_success).add_errback(
            cls.base_producer.on_send_error
        )

    pass

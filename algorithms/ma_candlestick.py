import os
from typing import TYPE_CHECKING

from models.signals import BollinguerSpread, SignalsConsumer
from shared.enums import KafkaTopics, MarketDominance, Strategy
from shared.utils import round_numbers

if TYPE_CHECKING:
    from producers.technical_indicators import TechnicalIndicators

# Algorithms based on Bollinguer bands


def ma_candlestick_jump(
    cls: "TechnicalIndicators",
    close_price,
    open_price,
    ma_7,
    ma_25,
    ma_100,
    ma_7_prev,
    volatility,
):
    """
    Candlesticks are in an upward trending motion for several periods
    This algorithm checks last close prices > MAs to decide whether to trade

    Intercept: the larger the value, the higher the potential for growth
        e.g. Given predictor y = 0.123x + 2.5, for x = 1, y = 0.123 + 2.5 = 2.623
             Given predictor y = 0.123x + 10, for x = 1, y = 0.123 + 10 = 10.123

    Chaikin_diff: positive values indicate overbought, negative values indicate oversold
    - Buy when oversold, sell when overbought

    SD: standard deviation of 0.006 seems to be a good threshold after monitoring signals,
    whereas it is possible to get around 3% increase to actually make a profit
    """
    bb_high, bb_mid, bb_low = cls.bb_spreads()

    if (
        float(close_price) > float(open_price)
        and bb_high < 1
        and bb_high > 0.01
        and close_price > ma_7
        and open_price > ma_7
        and close_price > ma_25
        and open_price > ma_25
        and ma_7 > ma_7_prev
        and close_price > ma_7_prev
        and open_price > ma_7_prev
        and close_price > ma_100
        and open_price > ma_100
    ):
        volatility = round_numbers(volatility, 6)
        algo = "ma_candlestick_jump"
        spread = volatility
        bot_strategy = cls.bot_strategy

        if cls.current_market_dominance == MarketDominance.GAINERS:
            # market is bullish, most prices increasing,
            # but looks like it's dropping and going bearish (reversal)
            # candlesticks of this specific crypto are seeing a huge jump (candlstick jump algo)
            bot_strategy = Strategy.long
        else:
            btc_correlation = cls.get_btc_correlation(symbol=cls.symbol)
            # Negative correlation with BTC and when market is downtrend
            # means this crypto is good for hedging against BTC going down
            if (
                btc_correlation < 0
                and cls.current_market_dominance == MarketDominance.LOSERS
                and not cls.market_domination_reversal
            ):
                bot_strategy = Strategy.long

            elif (
                btc_correlation > 0
                and cls.current_market_dominance == MarketDominance.LOSERS
            ):
                bot_strategy = Strategy.margin_short

            else:
                return

        msg = f"""
        - [{os.getenv('ENV')}] Candlestick <strong>#{algo}</strong> #{cls.symbol}
        - Current price: {close_price}
        - %threshold based on volatility: {volatility}
        - Reversal? {"Yes" if cls.market_domination_reversal else "No"}
        - Strategy: {bot_strategy.value}
        - Bollinguer bands spread: {(bb_high - bb_low) / bb_high}
        - TimesGPT forecast: {cls.forecast}
        - BTC correlation: {btc_correlation}
        - https://www.binance.com/en/trade/{cls.symbol}
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

    return


def ma_candlestick_drop(
    cls: "TechnicalIndicators",
    close_price,
    open_price,
    ma_7,
    ma_100,
    ma_25,
    ma_25_prev,
    volatility,
):
    """
    Opposite algorithm of ma_candletick_jump
    This algorithm detects Candlesticks that are in a downard trending motion for several periods

    Suitable for margin short trading (borrow - margin sell - buy back - repay)
    """
    volatility = round_numbers(volatility, 6)
    if (
        float(close_price) < float(open_price)
        and volatility > 0.009
        and close_price < ma_7
        and open_price < ma_7
        and close_price < ma_25
        and open_price < ma_25
        and ma_7 < ma_25_prev
        and close_price < ma_25_prev
        and open_price < ma_25_prev
        and close_price < ma_100
        and open_price < ma_100
        # remove high standard deviation
        # big candles. too many signals with little profitability
        and (abs(float(close_price) - float(open_price)) / float(close_price)) > 0.02
    ):
        algo = "ma_candlestick_drop"
        bb_high, bb_mid, bb_low = cls.bb_spreads()
        bot_strategy = cls.bot_strategy

        if cls.market_domination_reversal:
            if cls.current_market_dominance == MarketDominance.GAINERS:
                # market is bullish, most prices increasing,
                # but looks like it's dropping and going bearish (reversal)
                # candlesticks of this specific crypto are seeing a huge drop (candlstick drop algo)
                bot_strategy = Strategy.margin_short
            else:
                # market is bearish, most prices decreasing, (LOSERS)
                # but looks like it's picking up and going bullish (reversal)
                # candlesticks of this specific crypto are seeing a huge drop (candlstick drop algo)
                bot_strategy = Strategy.long
        else:
            return

        msg = f"""
        - [{os.getenv('ENV')}] Candlestick <strong>#{algo}</strong> #{cls.symbol}
        - Current price: {close_price}
        - Log volatility (log SD): {volatility}
        - Reversal? {cls.market_domination_reversal}
        - Strategy: {bot_strategy.value}
        - Bollinguer bands spread: {(bb_high - bb_low) / bb_high}
        - TimesGPT forecast: {cls.forecast}
        - https://www.binance.com/en/trade/{cls.symbol}
        - <a href='http://terminal.binbot.in/bots/new/{cls.symbol}'>Dashboard trade</a>
        """

        value = SignalsConsumer(
            spread=None,
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

    return

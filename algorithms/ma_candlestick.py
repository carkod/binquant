import logging
import os
from typing import TYPE_CHECKING

from models.signals import BollinguerSpread, SignalsConsumer
from shared.enums import KafkaTopics, MarketDominance, Strategy
from shared.utils import round_numbers

if TYPE_CHECKING:
    from producers.technical_indicators import TechnicalIndicators


async def reverse_atr_breakout(cls: "TechnicalIndicators", bb_high, bb_low, bb_mid):
    """
    Reverse atr_breakout
    When market is bearish, most prices decreasing,
    assets that have a negative correlation to BTC can potentially go up

    """

    if "ATR_breakout" not in cls.df:
        logging.error(f"ATP breakout not enough data for symbol: {cls.symbol}")
        return

    green_candle = cls.df["close"] > cls.df["open"]

    adp_diff = cls.market_breadth_data["adp"][-1] - cls.market_breadth_data["adp"][-2]
    adp_diff_prev = (
        cls.market_breadth_data["adp"][-2] - cls.market_breadth_data["adp"][-3]
    )

    if (
        cls.df["ATR_breakout"].iloc[-1]
        and green_candle.iloc[-1]
        # and volume_confirmation.iloc[-1]
        and cls.btc_correlation < 0
        # because the potential of growth is low, market is already mature
        # still want to get in when there is a trend (positive ADP)
        and cls.market_breadth_data["adp"][-1] > 0
        and adp_diff > 0
        and adp_diff_prev > 0
    ):
        algo = "reverse_atr_breakout"
        close_price = cls.df["close"].iloc[-1]

        msg = f"""
        - [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{cls.symbol}
        - Current price: {close_price}
        - Strategy: {cls.bot_strategy.value}
        - BTC correlation: {round_numbers(cls.btc_correlation)}
        - <a href='https://www.binance.com/en/trade/{cls.symbol}'>Binance</a>
        - <a href='http://terminal.binbot.in/bots/new/{cls.symbol}'>Dashboard trade</a>
        """

        value = SignalsConsumer(
            autotrade=False,
            current_price=close_price,
            msg=msg,
            symbol=cls.symbol,
            algo=algo,
            bot_strategy=cls.bot_strategy,
            bb_spreads=BollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )

        await cls.producer.send(
            KafkaTopics.signals.value, value=value.model_dump_json()
        )


async def atr_breakout(cls: "TechnicalIndicators", bb_high, bb_low, bb_mid):
    """
    ATR breakout detection algorithm based on chatGPT

    Detect breakout: price close above previous high AND ATR spike
    """

    if "ATR_breakout" not in cls.df:
        logging.error(f"ATP breakout not enough data for symbol: {cls.symbol}")
        return

    green_candle = cls.df["close"] > cls.df["open"]
    volume_confirmation = cls.df["volume"] > cls.df["volume"].rolling(20).mean()

    adp_diff = cls.market_breadth_data["adp"][-1] - cls.market_breadth_data["adp"][-2]
    adp_diff_prev = (
        cls.market_breadth_data["adp"][-2] - cls.market_breadth_data["adp"][-3]
    )

    if (
        (
            cls.df["ATR_breakout"].iloc[-1]
            or cls.df["ATR_breakout"].iloc[-2]
            or cls.df["ATR_breakout"].iloc[-3]
        )
        and green_candle.iloc[-1]
        and volume_confirmation.iloc[-1]
        and cls.current_market_dominance == MarketDominance.LOSERS
        # check market is bullish. we don't want to trade when all assets are uptrend
        # because the potential of growth is low, market is already mature
        # still want to get in when there is a trend (positive ADP)
        and adp_diff > 0
        and adp_diff_prev > 0
    ):
        algo = "atr_breakout"
        close_price = cls.df["close"].iloc[-1]

        msg = f"""
        - [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{cls.symbol}
        - Current price: {close_price}
        - Strategy: {cls.bot_strategy.value}
        - BTC correlation: {round_numbers(cls.btc_correlation)}
        - <a href='https://www.binance.com/en/trade/{cls.symbol}'>Binance</a>
        - <a href='http://terminal.binbot.in/bots/new/{cls.symbol}'>Dashboard trade</a>
        """

        value = SignalsConsumer(
            current_price=close_price,
            msg=msg,
            symbol=cls.symbol,
            algo=algo,
            bot_strategy=cls.bot_strategy,
            bb_spreads=BollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )

        await cls.producer.send(
            KafkaTopics.signals.value, value=value.model_dump_json()
        )


async def ma_candlestick_jump(
    cls: "TechnicalIndicators",
    close_price,
    open_price,
    ma_7,
    ma_25,
    ma_100,
    ma_7_prev,
    volatility,
    bb_high,
    bb_mid,
    bb_low,
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

    if (
        float(close_price) > float(open_price)
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
        btc_correlation = cls.binbot_api.get_btc_correlation(symbol=cls.symbol)

        if cls.current_market_dominance == MarketDominance.GAINERS:
            # market is bullish, most prices increasing,
            # but looks like it's dropping and going bearish (reversal)
            # candlesticks of this specific crypto are seeing a huge jump (candlstick jump algo)
            bot_strategy = Strategy.long

            msg = f"""
            - [{os.getenv("ENV")}] Candlestick <strong>#{algo}</strong> #{cls.symbol}
            - Current price: {close_price}
            - %threshold based on volatility: {volatility}
            - Reversal? {"Yes" if cls.market_domination_reversal else "No"}
            - Strategy: {bot_strategy.value}
            - Bollinguer bands spread: {(bb_high - bb_low) / bb_high}
            - BTC correlation: {btc_correlation}
            - https://www.binance.com/en/trade/{cls.symbol}
            - <a href='http://terminal.binbot.in/bots/new/{cls.symbol}'>Dashboard trade</a>
            """

            value = SignalsConsumer(
                autotrade=False,
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

            await cls.producer.send(
                KafkaTopics.signals.value, value=value.model_dump_json()
            )
        else:
            # Negative correlation with BTC and when market is downtrend
            # means this crypto is good for hedging against BTC going down
            if btc_correlation < 0:
                bot_strategy = Strategy.long

            elif btc_correlation > 0:
                bot_strategy = Strategy.margin_short
                # temporarily disable margin bots
                return

            else:
                return

    return

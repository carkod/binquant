import logging
import os
from typing import TYPE_CHECKING

from models.signals import BollinguerSpread, SignalsConsumer
from shared.enums import KafkaTopics, MarketDominance, Strategy
from shared.utils import round_numbers

if TYPE_CHECKING:
    from producers.technical_indicators import TechnicalIndicators


async def atr_breakout(cls: "TechnicalIndicators"):
    """
    ATR breakout detection algorithm based chatGPT

    Detect breakout: price close above previous high AND ATR spike
    """

    if "ATR_14" not in cls.df_1h:
        logging.error(
            f"1h candles atp breakout not enough data for symbol: {cls.symbol}"
        )
        return

    prev_high = cls.df_1h["high"].shift(1)
    atr_spike = cls.df_1h["ATR_14"] > 1.5 * cls.df_1h["ATR_baseline"]
    price_breakout = cls.df_1h["close"] > prev_high
    breakout_signal = atr_spike & price_breakout

    if breakout_signal.iloc[-1]:
        algo = "atr_breakout"
        close_price = cls.df_1h["close"].iloc[-1]
        bb_high = cls.df_1h["bb_high"].iloc[-1]
        bb_mid = cls.df_1h["bb_mid"].iloc[-1]
        bb_low = cls.df_1h["bb_low"].iloc[-1]

        msg = f"""
        - [{os.getenv('ENV')}] <strong>#{algo} algorithm</strong> #{cls.symbol}
        - Current price: {close_price}
        - Strategy: {cls.bot_strategy.value}
        - ATR spike: {cls.df_1h['ATR_14'].iloc[-1]}
        - Previous high: {prev_high.iloc[-1]}
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

    pass


# Algorithms based on Bollinguer bands
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
            - [{os.getenv('ENV')}] Candlestick <strong>#{algo}</strong> #{cls.symbol}
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


async def ma_candlestick_drop(
    cls: "TechnicalIndicators",
    close_price,
    open_price,
    ma_7,
    ma_100,
    ma_25,
    ma_25_prev,
    volatility,
    bb_high,
    bb_mid,
    bb_low,
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
        bot_strategy = cls.bot_strategy

        if cls.market_domination_reversal:
            if cls.current_market_dominance == MarketDominance.GAINERS:
                # market is bullish, most prices increasing,
                # but looks like it's dropping and going bearish (reversal)
                # candlesticks of this specific crypto are seeing a huge drop (candlstick drop algo)
                bot_strategy = Strategy.margin_short
                # temporarily disable margin bots
                return
            else:
                # market is bearish, most prices decreasing, (LOSERS)
                # but looks like it's picking up and going bullish (reversal)
                # candlesticks of this specific crypto are seeing a huge drop (candlstick drop algo)
                bot_strategy = Strategy.long

                msg = f"""
                - [{os.getenv('ENV')}] Candlestick <strong>#{algo}</strong> #{cls.symbol}
                - Current price: {close_price}
                - Log volatility (log SD): {volatility}
                - Reversal? {cls.market_domination_reversal}
                - Strategy: {bot_strategy.value}
                - Bollinguer bands spread: {(bb_high - bb_low) / bb_high}
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

                await cls.producer.send(
                    KafkaTopics.signals.value, value=value.model_dump_json()
                )
        else:
            return

    return

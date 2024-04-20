import json
import math
import logging

from datetime import datetime
from api.tools.enum_definitions import KafkaTopics
from producers.base import BaseProducer
from shared.enums import CloseConditions
from models.signals import BotPayload, SignalsConsumer, TrendEnum
from shared.exceptions import AutotradeError
from shared.enums import Strategy
from shared.apis import BinbotApi
from shared.utils import round_numbers, supress_notation


class Autotrade(BaseProducer, BinbotApi):
    def __init__(
        self, pair, settings, algorithm_name, db_collection_name="paper_trading"
    ) -> None:
        """
        Initialize automatic bot trading.
        This hits the same endpoints as the UI terminal.binbot dashboard,
        but it's triggered by signals

        There are two types of autotrade: autotrade and test_autotrade. The test_autotrade uses
        the paper_trading db collection and it doesn't use real quantities.

        Args:
        settings: autotrade/test_autotrade settings
        algorithm_name: usually the filename
        db_collection_name: Mongodb collection name ["paper_trading", "bots"]
        """
        self.pair = pair
        self.decimals = self.price_precision(pair)
        current_date = datetime.now().strftime("%Y-%m-%dT%H:%M")
        self.algorithm_name = algorithm_name
        self.default_bot = BotPayload(
            pair=pair,
            name=f"{algorithm_name}_{current_date}",
            balance_size_to_use=str(settings["balance_size_to_use"]),
            balance_to_use=settings["balance_to_use"],
            base_order_size=settings["base_order_size"],
            stop_loss=settings["stop_loss"],
            take_profit=settings["take_profit"],
            trailling=settings["trailling"],
            trailling_deviation=settings["trailling_deviation"],
            strategy=settings["strategy"],
            close_condition=CloseConditions.dynamic_trailling,
        )
        self.db_collection_name = db_collection_name
        self.blacklist: list = self.get_blacklist()
        # restart streams after bot activation
        super().__init__()
        self.producer = self.start_producer()

    def _set_bollinguer_spreads(self, data: SignalsConsumer, **kwargs):
        if data.bollinguer_spread:
            band_1 = kwargs["spread"]["band_1"]
            band_2 = kwargs["spread"]["band_2"]

            self.default_bot.take_profit = band_1 * 100
            self.default_bot.stop_loss = band_1 + band_2
            self.default_bot.trailling = True
            self.default_bot.trailling_deviation = band_1 * 100

    def handle_error(self, msg):
        """
        Submit errors to event logs of the bot
        """
        try:
            self.default_bot.errors.append(msg)
        except AttributeError:
            self.default_bot.errors = []
            self.default_bot.errors.append(msg)

    def set_margin_short_values(self, data: SignalsConsumer):
        """
        Set up values for margin_short
        this overrides the settings in research_controller autotrade settings
        """

        # Binances forces isolated pair to go through 24hr deactivation after traded
        self.default_bot.cooldown = 1440
        self.default_bot.margin_short_reversal = True

        self._set_bollinguer_spreads(data)

        # Override for top_gainers_drop
        if self.algorithm_name == "top_gainers_drop":
            self.default_bot.stop_loss = 5
            self.default_bot.trailling_deviation = 3.2

    def set_bot_values(self, data):
        """
        Set values for default_bot
        """
        self.default_bot.cooldown = 360  # Avoid cannibalization of profits
        self.default_bot.margin_short_reversal = True

        self._set_bollinguer_spreads(data)

    def handle_price_drops(
        self,
        balances,
        price,
        per_deviation=1.2,
        exp_increase=1.2,
        total_num_so=3,
        trend=TrendEnum.up_trend,  # ["upward", "downward"] Upward trend is for candlestick_jumps and similar algorithms. Downward trend is for panic sells in the market
        lowest_price=0,
        sd=0,
    ):
        """
        Sets the values for safety orders, short sell prices to hedge from drops in price.

        Safety orders here are designed to use qfl for price bounces: prices drop a bit but then overall the trend is bullish
        However short sell uses the short strategy: it sells the asset completely, to buy again after a dip.
        """
        available_balance = next(
            (
                b["free"]
                for b in balances["data"]
                if b["asset"] == self.default_bot.balance_to_use
            ),
            None,
        )

        if not available_balance:
            print(f"Not enough {self.default_bot.balance_to_use} for safety orders")
            return

        if trend == "downtrend":
            down_short_buy_spread = total_num_so * (per_deviation / 100)
            down_short_sell_price = round_numbers(price - (price * 0.05))
            down_short_buy_price = round_numbers(
                down_short_sell_price - (down_short_sell_price * down_short_buy_spread)
            )
            self.default_bot.short_sell_price = down_short_sell_price

            if lowest_price > 0 and lowest_price <= down_short_buy_price:
                self.default_bot.short_buy_price = lowest_price
            else:
                self.default_bot.short_buy_price = down_short_buy_price

        return

    def set_paper_trading_values(self, balances, qty):

        # Get balance that match the pair
        # Check that we have minimum binance required qty to trade
        for b in balances["data"]:
            if self.pair.endswith(b["asset"]):
                qty = supress_notation(b["free"], self.decimals)
                if self.min_amount_check(self.pair, qty):
                    # balance_size_to_use = 0.0 means "Use all balance". float(0) = 0.0
                    if float(self.default_bot.balance_size_to_use) != 0.0:
                        if b["free"] < float(self.default_bot.balance_size_to_use):
                            # Display warning and continue with full balance
                            print(
                                f"Error: balance ({qty}) is less than balance_size_to_use ({float(self.default_bot['balance_size_to_use'])}). Autotrade will use all balance"
                            )
                        else:
                            qty = float(self.default_bot.balance_size_to_use)

                    self.default_bot.base_order_size = qty
                    break

                rate = rate["price"]
                qty = supress_notation(b["free"], self.decimals)
                # Round down to 6 numbers to avoid not enough funds
                base_order_size = (
                    math.floor((float(qty) / float(rate)) * 10000000) / 10000000
                )
                self.default_bot.base_order_size = supress_notation(
                    base_order_size, self.decimals
                )
                pass

    def activate_autotrade(self, data: SignalsConsumer, **kwargs):
        """
        Run autotrade
        2. Create bot with given parameters from research_controller
        3. Activate bot
        """
        logging.info(f"{self.db_collection_name} Autotrade running with {self.pair}...")
        if self.blacklist:
            for item in self.blacklist:
                if item["pair"] == self.pair:
                    logging.info(f"Pair {self.pair} is blacklisted")
                    return

        if data.trend == TrendEnum.down_trend:
            self.default_bot.strategy = Strategy.margin_short
            # self.default_bot["close_condition"] = CloseConditions.market_reversal

        if data.trend == TrendEnum.up_trend:
            self.default_bot.strategy = Strategy.long

        if self.db_collection_name == "paper_trading":
            # Dynamic switch to real bot URLs
            create_func = self.create_paper_bot
            activate_func = self.activate_paper_bot

            if self.default_bot.strategy == Strategy.margin_short:
                self.set_margin_short_values(data)
                pass
            else:
                balances = self.get_balances()
                qty = 0
                self.set_paper_trading_values(balances, qty)
                pass

        # Can't get balance qty, because balance = 0 if real bot is trading
        # Base order set to default 1 to avoid errors
        # and because there is no matching engine endpoint to get market qty
        # So deal base_order should update this to the correct amount
        if self.db_collection_name == "bots":
            create_func = self.create_bot
            activate_func = self.activate_bot

            if self.default_bot.strategy == Strategy.margin_short:
                ticker = self.ticker_price(self.default_bot.pair)
                initial_price = ticker["price"]
                estimate_qty = float(self.default_bot.base_order_size) / float(
                    initial_price
                )
                stop_loss_price_inc = float(initial_price) * (
                    1 + (self.default_bot.stop_loss / 100)
                )
                # transfer quantity required to cover losses
                transfer_qty = stop_loss_price_inc * estimate_qty
                balances = self.balance_estimate()
                if balances < transfer_qty:
                    logging.error(
                        f"Not enough funds to autotrade margin_short bot. Unable to cover potential losses. balances: {balances}. transfer qty: {transfer_qty}"
                    )
                    return
                self.set_margin_short_values(data)
                pass
            else:
                self.set_bot_values(data)
                pass

        # Create bot
        payload = self.default_bot.model_dump()
        # create paper or real bot
        create_bot = create_func(payload)

        if "error" in create_bot and create_bot["error"] == 1:
            print(
                f"Autotrade: {create_bot['message']}",
                f"Pair: {self.pair}.",
            )
            self.submit_bot_event_logs(create_bot["botId"], message)
            return

        # Activate bot
        botId = create_bot["botId"]
        # paper or real bot activation
        bot = activate_func(botId)

        if "error" in bot and bot["error"] > 0:
            # Failed to activate bot so:
            # (1) Add to blacklist/exclude from future autotrades
            # (2) Submit error to event logs
            # (3) Delete inactive bot
            # this prevents cluttering UI with loads of useless bots
            message = bot["message"]
            self.submit_bot_event_logs(botId, message)
            self.blacklist.append(self.default_bot.pair)
            if self.default_bot.strategy == Strategy.margin_short:
                self.clean_margin_short(self.default_bot.pair)
            self.delete_bot(botId)
            raise AutotradeError(message)

        else:
            message = f"Succesful {self.db_collection_name} autotrade, opened with {self.pair}!"
            value = {
                "botId": botId,
                "message": message
            }
            self.submit_bot_event_logs(botId, message)
            # Send message to restart streaming at the end to avoid blocking
            # Message is sent only after activation is successful,
            # if bot activation failed, we want to try again with a new bot
            self.producer.send(KafkaTopics.restart_streaming.value, value=json.dumps(value)).add_callback(self.base_producer.on_send_success).add_errback(self.base_producer.on_send_error)

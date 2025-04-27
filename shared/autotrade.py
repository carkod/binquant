import logging
import math
from datetime import datetime

from aiokafka import AIOKafkaProducer

from models.bot import BotModel
from models.signals import SignalsConsumer
from producers.base import AsyncProducer
from shared.apis.binbot_api import BinbotApi
from shared.enums import CloseConditions, Strategy
from shared.exceptions import AutotradeError
from shared.utils import round_numbers, supress_notation


class Autotrade(AsyncProducer, BinbotApi):
    def __init__(
        self,
        pair,
        settings,
        algorithm_name,
        db_collection_name="paper_trading",
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
        self.pair: str = pair
        self.decimals = self.price_precision(pair)
        current_date = datetime.now().strftime("%Y-%m-%dT%H:%M")
        self.algorithm_name = algorithm_name
        self.default_bot = BotModel(
            pair=pair,
            name=f"{algorithm_name}_{current_date}",
            fiat=settings["fiat"],
            base_order_size=settings["base_order_size"],
            strategy=Strategy.long,
            stop_loss=settings["stop_loss"],
            take_profit=settings["take_profit"],
            trailling=settings["trailling"],
            trailling_deviation=settings["trailling_deviation"],
            trailling_profit=settings["trailling_profit"],
            close_condition=CloseConditions.dynamic_trailling,
            dynamic_trailling=True,  # not added to settings yet
        )
        self.db_collection_name = db_collection_name
        # restart streams after bot activation
        super().__init__()
        self.producer: AIOKafkaProducer

    def _set_bollinguer_spreads(self, data: SignalsConsumer):
        bb_spreads = data.bb_spreads

        if (
            bb_spreads
            and bb_spreads.bb_high
            and bb_spreads.bb_low
            and bb_spreads.bb_mid
        ):
            top_spread = (
                abs((bb_spreads.bb_high - bb_spreads.bb_mid) / bb_spreads.bb_high) * 100
            )
            whole_spread = (
                abs((bb_spreads.bb_high - bb_spreads.bb_low) / bb_spreads.bb_high) * 100
            )
            bottom_spread = (
                abs((bb_spreads.bb_mid - bb_spreads.bb_low) / bb_spreads.bb_mid) * 100
            )

            # Otherwise it'll close too soon
            if whole_spread > 2 and whole_spread < 20:
                if self.default_bot.strategy == Strategy.long:
                    self.default_bot.stop_loss = round_numbers(whole_spread)
                    self.default_bot.take_profit = round_numbers(top_spread)
                    # too much risk, reduce stop loss
                    self.default_bot.trailling_deviation = round_numbers(bottom_spread)

                if self.default_bot.strategy == Strategy.margin_short:
                    self.default_bot.stop_loss = round_numbers(whole_spread)
                    self.default_bot.take_profit = round_numbers(bottom_spread)
                    self.default_bot.trailling_deviation = round_numbers(top_spread)

    def handle_error(self, msg):
        """
        Submit errors to event logs of the bot
        """
        try:
            self.default_bot.logs.append(msg)
        except AttributeError:
            self.default_bot.logs = []
            self.default_bot.logs.append(msg)

    def set_margin_short_values(self, data: SignalsConsumer):
        """
        Set up values for margin_short
        this overrides the settings in research_controller autotrade settings
        """
        # Binances forces isolated pair to go through 24hr deactivation after traded
        self.default_bot.cooldown = 1440
        self.default_bot.margin_short_reversal = True

        if data.bb_spreads:
            self._set_bollinguer_spreads(data)

    def set_bot_values(self, data: SignalsConsumer):
        """
        Set values for default_bot
        """
        self.default_bot.cooldown = 360  # Avoid cannibalization of profits
        self.default_bot.margin_short_reversal = True

        if data.bb_spreads:
            self._set_bollinguer_spreads(data)

    def set_paper_trading_values(self, balances, qty):
        # Get balance that match the pair
        # Check that we have minimum binance required qty to trade
        for b in balances["data"]:
            if self.pair.endswith(b["asset"]):
                qty = round_numbers(b["free"], self.decimals)
                if self.min_amount_check(self.pair, qty):
                    self.default_bot.base_order_size = qty
                    break

                ticker = self.ticker_24_price(symbol=self.pair)
                rate = ticker["price"]
                qty = supress_notation(b["free"], self.decimals)
                # Round down to 6 numbers to avoid not enough funds
                base_order_size = (
                    math.floor((float(qty) / float(rate)) * 10000000) / 10000000
                )
                self.default_bot.base_order_size = round_numbers(
                    base_order_size, self.decimals
                )
                pass

    async def activate_autotrade(self, data: SignalsConsumer):
        """
        Run autotrade
        2. Create bot with given parameters from research_controller
        3. Activate bot
        """
        logging.info(f"{self.db_collection_name} Autotrade running with {self.pair}...")

        self.default_bot.strategy = data.bot_strategy
        if self.db_collection_name == "paper_trading":
            # Dynamic switch to real bot URLs
            create_func = self.create_paper_bot
            activate_func = self.activate_paper_bot
            errors_func = self.submit_paper_trading_event_logs

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
            errors_func = self.submit_bot_event_logs

            if self.default_bot.strategy == Strategy.margin_short:
                try:
                    ticker = self.ticker_24_price(self.default_bot.pair)
                except Exception as e:
                    logging.error(f"Error getting ticker price: {e}")
                    return
                initial_price = ticker["price"]
                estimate_qty = float(self.default_bot.base_order_size) / float(
                    initial_price
                )
                stop_loss_price_inc = float(initial_price) * (
                    1 + (self.default_bot.stop_loss / 100)
                )
                # transfer quantity required to cover losses
                transfer_qty = stop_loss_price_inc * estimate_qty
                balance_check = self.get_available_fiat()
                if balance_check < transfer_qty:
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
        payload = self.default_bot.model_dump_json()
        # create paper or real bot
        create_bot = create_func(payload)

        if "error" in create_bot and create_bot["error"] == 1:
            errors_func(create_bot["botId"], create_bot["message"])
            return

        # Activate bot
        bot_id = create_bot["data"]["id"]
        # paper or real bot activation
        bot = activate_func(bot_id)

        if "error" in bot and bot["error"] > 0:
            message = bot["message"]
            errors_func(bot_id, message)
            if self.default_bot.strategy == Strategy.margin_short:
                self.clean_margin_short(self.default_bot.pair)
            self.delete_bot(bot_id)
            raise AutotradeError(message)

        else:
            message = f"Succesful {self.db_collection_name} autotrade, opened with {self.pair}!"
            errors_func(bot_id, message)
            # restart streaming is not necessary, because activation already did it

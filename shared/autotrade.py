import logging

from pybinbot import (
    CloseConditions,
    SignalsConsumer,
    Strategy,
    round_numbers,
)
from models.bot import BotModel
from shared.exceptions import AutotradeError
from pybinbot import ExchangeId, BinanceApi, KucoinApi, BinbotApi


class Autotrade:
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
        self.binbot_api = BinbotApi()
        self.exchange = ExchangeId(settings["exchange_id"])
        self.api: BinanceApi | KucoinApi

        if self.exchange == ExchangeId.KUCOIN:
            self.api = KucoinApi()
        else:
            self.api = BinanceApi()

        self.symbol_data = self.binbot_api.get_single_symbol(self.pair)
        self.decimals = self.symbol_data["price_precision"]
        self.algorithm_name = algorithm_name
        self.default_bot = BotModel(
            pair=pair,
            mode="autotrade",
            name=algorithm_name,
            fiat=settings["fiat"],
            fiat_order_size=settings["base_order_size"],
            quote_asset=self.symbol_data["quote_asset"],
            strategy=Strategy.long,
            stop_loss=settings["stop_loss"],
            take_profit=settings["take_profit"],
            trailling=settings["trailling"],
            trailling_deviation=settings["trailling_deviation"],
            trailling_profit=settings["trailling_profit"],
            margin_short_reversal=settings["autoswitch"],
            close_condition=CloseConditions.dynamic_trailling,
            dynamic_trailling=True,  # not added to settings yet
        )
        self.db_collection_name = db_collection_name
        # restart streams after bot activation
        super().__init__()

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

        if data.bb_spreads:
            self._set_bollinguer_spreads(data)

    def set_bot_values(self, data: SignalsConsumer):
        """
        Set values for default_bot
        """
        self.default_bot.cooldown = 360  # Avoid cannibalization of profits

        # disable margin short if not available to prevent bot erroring
        if not self.symbol_data["is_margin_trading_allowed"]:
            self.default_bot.margin_short_reversal = False

        if data.bb_spreads:
            self._set_bollinguer_spreads(data)

    def set_paper_trading_values(self, data: SignalsConsumer):
        if data.bb_spreads:
            self._set_bollinguer_spreads(data)
        pass

    async def activate_autotrade(self, data: SignalsConsumer):
        """
        Run autotrade
        1. Make sure we are not duplicating bots filter_excluded_symbols
        2. Create bot with given parameters from research_controller
        3. Activate bot
        """
        excluded_symbols = self.binbot_api.filter_excluded_symbols()
        if self.pair in excluded_symbols:
            logging.info(
                f"Autotrade already active or in exclusion list for {self.pair}, skipping..."
            )
            return

        self.default_bot.strategy = Strategy(data.bot_strategy)
        if self.db_collection_name == "paper_trading":
            # Dynamic switch to real bot URLs
            create_func = self.binbot_api.create_paper_bot
            activate_func = self.binbot_api.activate_paper_bot
            errors_func = self.binbot_api.submit_paper_trading_event_logs
            delete_func = self.binbot_api.delete_paper_bot

            if self.default_bot.strategy == Strategy.margin_short:
                self.set_margin_short_values(data)
                pass
            else:
                self.set_paper_trading_values(data)
                pass

        # Can't get balance qty, because balance = 0 if real bot is trading
        # Base order set to default 1 to avoid errors
        # and because there is no matching engine endpoint to get market qty
        # So deal base_order should update this to the correct amount
        if self.db_collection_name == "bots":
            create_func = self.binbot_api.create_bot
            activate_func = self.binbot_api.activate_bot
            errors_func = self.binbot_api.submit_bot_event_logs
            delete_func = self.binbot_api.delete_bot

            if self.default_bot.strategy == Strategy.margin_short:
                initial_price = self.api.get_ticker_price(self.default_bot.pair)

                estimate_qty = float(self.default_bot.fiat_order_size) / float(
                    initial_price
                )
                stop_loss_price_inc = float(initial_price) * (
                    1 + (self.default_bot.stop_loss / 100)
                )
                # transfer quantity required to cover losses
                transfer_qty = stop_loss_price_inc * estimate_qty
                balance_check = self.binbot_api.get_available_fiat(
                    exchange=self.exchange, fiat=self.default_bot.fiat
                )
                if balance_check < transfer_qty:
                    logging.error(
                        f"Not enough funds to autotrade margin_short bot. Unable to cover potential losses. balances: {balance_check}. transfer qty: {transfer_qty}"
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
                self.binbot_api.clean_margin_short(self.default_bot.pair)
            delete_func(bot_id)
            raise AutotradeError(message)

        else:
            message = f"Succesful {self.db_collection_name} autotrade, opened with {self.pair}!"
            errors_func(bot_id, message)
            # restart streaming is not necessary, because activation already did it

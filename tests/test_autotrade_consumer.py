from consumers.autotrade_consumer import AutotradeConsumer


class TestAutotradeConsumer:
    def setup_method(self):
        self.settings = {"max_active_autotrade_bots": 2, "exchange_id": "binance"}
        self.test_settings = {"max_active_autotrade_bots": 1}
        self.consumer = AutotradeConsumer(
            autotrade_settings=self.settings,
            active_test_bots=[1],
            all_symbols=[],
            test_autotrade_settings=self.test_settings,
        )
        self.consumer.active_bots = [1, 2]
        self.consumer.active_test_bots = [1]

    def test_reached_max_active_autobots_paper_trading(self):
        assert not self.consumer.reached_max_active_autobots("paper_trading")
        self.consumer.active_test_bots.append(2)
        assert self.consumer.reached_max_active_autobots("paper_trading")

    def test_reached_max_active_autobots_bots(self):
        assert not self.consumer.reached_max_active_autobots("bots")
        self.consumer.active_bots.append(3)
        assert self.consumer.reached_max_active_autobots("bots")

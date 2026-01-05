from consumers.autotrade_consumer import AutotradeConsumer


class TestAutotradeConsumer:
    def setup_method(self):
        self.settings = {"max_active_autotrade_bots": 2, "exchange_id": "binance"}
        self.test_settings = {"max_active_autotrade_bots": 1}
        self.consumer = AutotradeConsumer(
            autotrade_settings=self.settings,
            active_test_bots=[],
            all_symbols=[],
            test_autotrade_settings=self.test_settings,
        )

    def test_reached_max_active_autobots_paper_trading(self, monkeypatch):
        # First, mock BinbotApi to return 1 active test bot (below limit)
        monkeypatch.setattr(
            self.consumer.binbot_api,
            "get_active_pairs",
            lambda collection_name: [1]
            if collection_name == "paper_trading"
            else [],
        )
        assert not self.consumer.reached_max_active_autobots("paper_trading")

        # Now mock to return 2 active test bots (above limit of 1)
        monkeypatch.setattr(
            self.consumer.binbot_api,
            "get_active_pairs",
            lambda collection_name: [1, 2]
            if collection_name == "paper_trading"
            else [],
        )
        assert self.consumer.reached_max_active_autobots("paper_trading")

    def test_reached_max_active_autobots_bots(self, monkeypatch):
        # First, mock BinbotApi to return 2 active bots (at limit, not over)
        monkeypatch.setattr(
            self.consumer.binbot_api,
            "get_active_pairs",
            lambda collection_name: [1, 2] if collection_name == "bots" else [],
        )
        assert not self.consumer.reached_max_active_autobots("bots")

        # Now mock to return 3 active bots (over limit of 2)
        monkeypatch.setattr(
            self.consumer.binbot_api,
            "get_active_pairs",
            lambda collection_name: [1, 2, 3]
            if collection_name == "bots"
            else [],
        )
        assert self.consumer.reached_max_active_autobots("bots")

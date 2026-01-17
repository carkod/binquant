import time
import requests
from math import tanh
from typing import TYPE_CHECKING
from pybinbot import BinbotApi

if TYPE_CHECKING:
    from producers.analytics import CryptoAnalytics


class BinanceAIReport:
    # Helper to derive base token from a trading symbol (simple heuristic)
    QUOTE_ASSETS = ["USDT", "USDC", "BUSD", "TRY", "EUR", "BTC", "ETH"]
    BINANCE_AI_ENDPOINT = "https://www.binance.com/bapi/bigdata/v3/friendly/bigdata/search/ai-report/report"

    def __init__(
        self,
        cls: "CryptoAnalytics",
    ) -> None:
        self.symbol = cls.symbol.replace("-", "")
        self.binbot_api = BinbotApi()
        self.price_precision = cls.price_precision
        self.current_symbol_data = cls.current_symbol_data
        self.btc_correlation = cls.btc_correlation
        self.telegram_consumer = cls.telegram_consumer
        self.at_consumer = cls.at_consumer

    @staticmethod
    def count_points(mod_list):
        return sum(len(m.get("points", []) or []) for m in mod_list)

    # --- Binance AI Report Inline Logic ---
    def fetch_report(self) -> dict | None:
        """Fetch raw Binance AI report JSON for a given base token using POST request.
        Returns raw dictionary or None if network/scheme failure.
        """
        if not self.current_symbol_data:
            return None

        token = self.current_symbol_data["base_asset"]
        try:
            timestamp = int(time.time() * 1000)
            payload = {
                "lang": "en",
                "token": token,
                "symbol": self.symbol.upper(),
                "product": "web-spot",
                "timestamp": str(timestamp),
                "translateToken": None,
            }
            resp = requests.post(self.BINANCE_AI_ENDPOINT, json=payload, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:  # pragma: no cover
            print("[AIReport] Fetch failed:", e)
            return None

    def extract_features(
        self, max_fresh_minutes: int = 8 * 60, normalize: bool = True
    ) -> dict | None:
        """
        Extract heuristic external feature vector from raw report JSON.
        """
        report_json = self.fetch_report()
        if not report_json:
            return None

        data = report_json.get("data", {})
        original = (
            data.get("report", {}).get("original", {})
            if "report" in data
            else report_json.get("data", {}).get("original", {})
        )
        if not original:
            print("Original not found. Symbol: ", self.symbol)
            return None
        report_meta = original.get("reportMeta", {})
        modules = original.get("modules", []) or []
        update_ms = int(report_meta.get("updateAt", 0))
        age_minutes = (time.time() * 1000 - update_ms) / 60000.0 if update_ms else 1e9
        fresh = age_minutes <= max_fresh_minutes
        base = {
            "external_available": 1,
            "external_stale_flag": int(not fresh),
            "external_age_minutes": round(age_minutes, 2),
        }
        if not fresh:
            return base
        by_type: dict = {}
        for m in modules:
            mtype = m.get("type", "")
            by_type.setdefault(mtype, []).append(m)
        opp_mods = by_type.get("opportunities", [])
        risk_mods = by_type.get("risks", [])
        comm_mods = by_type.get("community_sentiment", [])
        opp_count = self.count_points(opp_mods)
        risk_count = self.count_points(risk_mods)
        community_posts = 0
        if comm_mods:
            for m in comm_mods:
                for p in m.get("points", []) or []:
                    for ref in p.get("citationRefs", []) or []:
                        if ref.get("type") == "post":
                            community_posts += int(ref.get("count", 0))
        texts = []
        for m in modules:
            for p in m.get("points", []) or []:
                txt = p.get("content", "")
                if txt:
                    texts.append(txt)
            if m.get("overview"):
                texts.append(m["overview"])
        joined = " \n ".join(texts).lower()

        def kw_flag(*phrases):
            return int(any(ph.lower() in joined for ph in phrases))

        macd_bullish_flag = kw_flag("macd", "bullish crossover")
        ema_bearish_flag = kw_flag("ema7", "ema25", "ema99", "bearish")
        volatility_decreasing_flag = kw_flag("decreasing volatility")
        price_resilience_flag = kw_flag("resilience", "altcoins", "80-99%")
        outflow_flag = kw_flag("net outflow", "outflow")
        coinbase_premium_weak_flag = kw_flag("premium gaps", "weak demand", "coinbase")
        institutional_adoption_flag = kw_flag("institutional", "adoption", "survey")
        macro_headwind_flag = kw_flag("geopolitical", "trade tensions", "tariff")
        sentiment_mixed_flag = kw_flag("mixed sentiment", "mixed outlook")
        bull_support = (
            macd_bullish_flag + institutional_adoption_flag + price_resilience_flag
        )
        bear_pressure = ema_bearish_flag + outflow_flag + macro_headwind_flag
        net_bias = bull_support - bear_pressure
        bias_norm = tanh(net_bias) if normalize else net_bias
        ratio = (opp_count + 1) / (risk_count + 1)
        net_signal_score = opp_count - risk_count
        base.update(
            {
                "opp_count": opp_count,
                "risk_count": risk_count,
                "opp_risk_ratio": round(ratio, 4),
                "net_signal_score": net_signal_score,
                "community_post_count": community_posts,
                "large_discussion_flag": int(community_posts >= 10),
                "external_net_bias": net_bias,
                "external_bias_normalized": round(bias_norm, 4),
                "macd_bullish_flag": macd_bullish_flag,
                "ema_bearish_flag": ema_bearish_flag,
                "sentiment_mixed_flag": sentiment_mixed_flag,
                "volatility_decreasing_flag": volatility_decreasing_flag,
                "coinbase_premium_weak_flag": coinbase_premium_weak_flag,
            }
        )
        return base

    def ai_report_signal(
        self, bias_thr: float = 0.5, opp_risk_thr: float = 1.2, net_score_thr: int = 1
    ) -> dict | None:
        """Return a directional AI report signal.

        1  bullish, -1 bearish, 0 neutral.
        Uses external bias, opportunity/risk ratio and net signal score plus MACD / EMA flags.
        (Social features are exposed via separate social_features_flag()).
        """
        features = self.extract_features()
        if not features:
            return None

        signal_type = {}

        # Directional criteria (heuristic – tune thresholds in runtime if needed)

        if features.get("external_bias_normalized", 0) > bias_thr:
            signal_type["external_bias_normalized"] = features.get(
                "external_bias_normalized"
            )

        if features.get("opp_risk_ratio", 1):
            signal_type["opp_risk_ratio"] = features.get("opp_risk_ratio")

        if features.get("net_signal_score", 0) > net_score_thr:
            signal_type["net_signal_score"] = features.get("net_signal_score")

        if features.get("macd_bullish_flag", 0) == 1:
            signal_type["macd_bullish_flag"] = features.get("macd_bullish_flag")

        if features.get("external_bias_normalized", 0) < -bias_thr:
            signal_type["external_bias_normalized"] = features.get(
                "external_bias_normalized"
            )

        if features.get("opp_risk_ratio", 1) < 1:
            signal_type["opp_risk_ratio"] = features.get("opp_risk_ratio")

        if features.get("net_signal_score", 0) < -net_score_thr:
            signal_type["net_signal_score"] = features.get("net_signal_score")

        if features.get("ema_bearish_flag", 0) == 1:
            signal_type["ema_bearish_flag"] = features.get("ema_bearish_flag")

        signal = (
            features.get("external_bias_normalized", 0) > bias_thr
            or features.get("opp_risk_ratio", 1) > opp_risk_thr
            or features.get("net_signal_score", 0) > net_score_thr
            or features.get("macd_bullish_flag", 0) == 1
            or features.get("external_bias_normalized", 0) < -bias_thr
            or features.get("opp_risk_ratio", 1) < 1
            or features.get("net_signal_score", 0) < -net_score_thr
            or features.get("ema_bearish_flag", 0) == 1
        )
        if signal:
            return signal_type

        return None

    def social_features_flag(self) -> dict | None:
        """
        Aggregate social-/community-related external flags into a single boolean.

        Returns True if ANY of these are present:
        - large_discussion_flag (community_post_count >= 10 in extract_features)
        - community_post_count >= min_posts (override threshold if desired)
        - sentiment_mixed_flag (mixed sentiment detected – volatility of opinion)
        - coinbase_premium_weak_flag (weak premium / demand phrase detected)

        NOTE: mixed sentiment and weak premium may indicate caution rather than
        bullishness; the intent here is to surface ANY notable social/contextual
        condition, not strictly positive ones. Caller can interpret polarity.
        """
        # min_posts: int = 10
        features = self.extract_features()
        signal_type = {}

        if not features:
            return None

        if features.get("large_discussion_flag", 0) > 0:
            signal_type["large_discussion_flag"] = features.get("large_discussion_flag")
        if features.get("community_post_count", 0) >= 2:
            signal_type["community_post_count"] = features.get("community_post_count")
        if features.get("sentiment_mixed_flag", 0) > 0:
            signal_type["sentiment_mixed_flag"] = features.get("sentiment_mixed_flag")
        if features.get("coinbase_premium_weak_flag", 0) > 1:
            signal_type["coinbase_premium_weak_flag"] = features.get(
                "coinbase_premium_weak_flag"
            )

        signal = (
            features.get("large_discussion_flag", 0) > 1
            or features.get("community_post_count", 0) > 1
            or features.get("sentiment_mixed_flag", 0) > 1
            or features.get("coinbase_premium_weak_flag", 0) > 1
        )
        if signal:
            return signal_type

        return None

    def final_report(
        self, bias_thr: float = 0.5, opp_risk_thr: float = 1.2, net_score_thr: int = 1
    ):
        features = self.extract_features()
        if not features or not features.get("external_available", 0):
            return 0
        bullish = (
            features.get("external_bias_normalized", 0) > bias_thr
            and features.get("opp_risk_ratio", 1) > opp_risk_thr
            and features.get("net_signal_score", 0) > net_score_thr
            and features.get("macd_bullish_flag", 0) == 1
        )
        bearish = (
            features.get("external_bias_normalized", 0) < -bias_thr
            and features.get("opp_risk_ratio", 1) < 1
            and features.get("net_signal_score", 0) < -net_score_thr
            and features.get("ema_bearish_flag", 0) == 1
        )
        if bullish:
            return 1
        if bearish:
            return -1
        return 0

from dataclasses import asdict, dataclass
from time import perf_counter

import numpy as np
from pandas import DataFrame
from sklearn.decomposition import PCA
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler


@dataclass(frozen=True)
class ActivityBurstAnomalyEvaluation:
    training_rows: int
    pca_score: float
    pca_percentile: float
    pca_confirmed: bool
    isolation_forest_score: float
    isolation_forest_percentile: float
    isolation_forest_confirmed: bool
    gate_passed: bool
    fit_latency_ms: float
    inference_latency_ms: float

    def as_indicators(self) -> dict[str, float | int | bool]:
        return {
            f"activity_burst_anomaly_{key}": value
            for key, value in asdict(self).items()
        }


class ActivityBurstAnomalyGate:
    """Evaluate activity bursts with fixed models trained on earlier bars only."""

    FEATURE_COLUMNS = (
        "price_jump",
        "volume_ratio",
        "quote_volume_ratio",
        "range_frac",
        "body_frac",
        "close_to_high",
        "recent_up_closes",
    )
    MIN_TRAINING_ROWS = 100
    CONFIRMATION_PERCENTILE = 0.95
    PCA_COMPONENTS = 3

    def __init__(self) -> None:
        self.scaler: StandardScaler | None = None
        self.pca: PCA | None = None
        self.isolation_forest: IsolationForest | None = None
        self.pca_training_scores = np.array([], dtype=float)
        self.isolation_forest_training_scores = np.array([], dtype=float)
        self.training_rows = 0
        self.fit_latency_ms = 0.0

    @property
    def is_fitted(self) -> bool:
        return (
            self.scaler is not None
            and self.pca is not None
            and self.isolation_forest is not None
        )

    @staticmethod
    def _score_percentile(training_scores: np.ndarray, score: float) -> float:
        return float(
            np.searchsorted(np.sort(training_scores), score, side="right")
            / training_scores.size
        )

    @classmethod
    def _feature_matrix(cls, frame: DataFrame) -> DataFrame:
        features = DataFrame(
            frame.loc[:, list(cls.FEATURE_COLUMNS)].to_numpy(dtype=float),
            columns=cls.FEATURE_COLUMNS,
            index=frame.index,
        )
        features["volume_ratio"] = np.log1p(
            np.clip(features["volume_ratio"].to_numpy(), 0, None)
        )
        features["quote_volume_ratio"] = np.log1p(
            np.clip(features["quote_volume_ratio"].to_numpy(), 0, None)
        )
        return DataFrame(features.replace([np.inf, -np.inf], np.nan))

    def _fit(self, training: np.ndarray) -> None:
        started_at = perf_counter()
        self.scaler = StandardScaler().fit(training)
        scaled_training = self.scaler.transform(training)

        self.pca = PCA(
            n_components=min(self.PCA_COMPONENTS, training.shape[1] - 1),
            svd_solver="full",
        ).fit(scaled_training)
        reconstructed = self.pca.inverse_transform(self.pca.transform(scaled_training))
        self.pca_training_scores = np.mean(
            np.square(scaled_training - reconstructed), axis=1
        )

        self.isolation_forest = IsolationForest(
            n_estimators=100,
            max_samples="auto",
            contamination="auto",
            random_state=2024,
            n_jobs=1,
        ).fit(scaled_training)
        self.isolation_forest_training_scores = -self.isolation_forest.score_samples(
            scaled_training
        )
        self.training_rows = len(training)
        self.fit_latency_ms = (perf_counter() - started_at) * 1000

    def evaluate(self, frame: DataFrame) -> ActivityBurstAnomalyEvaluation | None:
        """
        Fit once on the initial historical batch and score the current bar.

        The current bar is excluded before fitting. Normalization and model
        parameters remain fixed for all later calls, matching StrAD's online
        evaluation protocol.
        """
        if frame.empty or any(column not in frame for column in self.FEATURE_COLUMNS):
            return None

        features = self._feature_matrix(frame)
        current = features.iloc[-1]
        if current.isna().any():
            return None

        if not self.is_fitted:
            training = features.iloc[:-1].dropna()
            if len(training) < self.MIN_TRAINING_ROWS:
                return None
            self._fit(training.to_numpy())

        assert self.scaler is not None
        assert self.pca is not None
        assert self.isolation_forest is not None

        started_at = perf_counter()
        scaled_current = self.scaler.transform(current.to_numpy().reshape(1, -1))
        reconstructed = self.pca.inverse_transform(self.pca.transform(scaled_current))
        pca_score = float(np.mean(np.square(scaled_current - reconstructed)))
        isolation_forest_score = float(
            -self.isolation_forest.score_samples(scaled_current)[0]
        )
        inference_latency_ms = (perf_counter() - started_at) * 1000

        pca_percentile = self._score_percentile(self.pca_training_scores, pca_score)
        isolation_forest_percentile = self._score_percentile(
            self.isolation_forest_training_scores,
            isolation_forest_score,
        )
        pca_confirmed = pca_percentile >= self.CONFIRMATION_PERCENTILE
        isolation_forest_confirmed = (
            isolation_forest_percentile >= self.CONFIRMATION_PERCENTILE
        )

        return ActivityBurstAnomalyEvaluation(
            training_rows=self.training_rows,
            pca_score=pca_score,
            pca_percentile=pca_percentile,
            pca_confirmed=pca_confirmed,
            isolation_forest_score=isolation_forest_score,
            isolation_forest_percentile=isolation_forest_percentile,
            isolation_forest_confirmed=isolation_forest_confirmed,
            gate_passed=pca_confirmed or isolation_forest_confirmed,
            fit_latency_ms=self.fit_latency_ms,
            inference_latency_ms=inference_latency_ms,
        )

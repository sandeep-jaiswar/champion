"""Tests for ML models."""

import numpy as np
import pandas as pd
import pytest

from champion.ml.alerts import AlertGenerator, AlertSeverity, AlertType
from champion.ml.backtesting import Backtester
from champion.ml.models.anomaly_detector import AutoencoderDetector, IsolationForestDetector
from champion.ml.models.lstm_predictor import LSTMPricePredictor
from champion.ml.optimization import PortfolioOptimizer


@pytest.fixture
def sample_ohlc_data():
    """Create sample OHLC data for testing."""
    np.random.seed(42)
    dates = pd.date_range(start="2024-01-01", periods=300, freq="D")

    data = []
    base_price = 1000.0

    for trade_date in dates:
        # Random walk
        change = np.random.normal(0.001, 0.02)
        close = base_price * (1 + change)
        open_price = close * (1 + np.random.normal(0, 0.01))
        high = max(open_price, close) * (1 + abs(np.random.normal(0, 0.01)))
        low = min(open_price, close) * (1 - abs(np.random.normal(0, 0.01)))
        volume = int(np.random.uniform(1e6, 5e6))

        data.append(
            {
                "symbol": "TEST",
                "trade_date": trade_date,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "total_traded_quantity": int(volume * 0.8),
                "number_of_trades": int(volume / 100),
            }
        )

        base_price = close

    return pd.DataFrame(data)


@pytest.fixture
def temp_model_dir(tmp_path):
    """Create temporary model directory."""
    model_dir = tmp_path / "models"
    model_dir.mkdir()
    return model_dir


class TestLSTMPricePredictor:
    """Tests for LSTM price prediction model."""

    def test_initialization(self):
        """Test model initialization."""
        model = LSTMPricePredictor(
            sequence_length=30,
            lstm_units=[32, 16],
            dropout_rate=0.3,
        )

        assert model.sequence_length == 30
        assert model.lstm_units == [32, 16]
        assert model.dropout_rate == 0.3
        assert model.model is None

    def test_training(self, sample_ohlc_data):
        """Test model training."""
        model = LSTMPricePredictor(sequence_length=30, lstm_units=[16])

        history = model.train(
            sample_ohlc_data,
            epochs=2,
            batch_size=16,
            experiment_name="test-lstm",
        )

        assert "loss" in history
        assert len(history["loss"]) == 2
        assert model.model is not None

    def test_prediction(self, sample_ohlc_data):
        """Test price prediction."""
        model = LSTMPricePredictor(sequence_length=30, lstm_units=[16])

        # Train model
        model.train(sample_ohlc_data[:200], epochs=2, experiment_name="test-lstm-pred")

        # Predict
        predictions = model.predict(sample_ohlc_data[170:200], steps=3)

        assert len(predictions) == 3
        # Predictions should be reasonable values (not NaN or extreme)
        assert all(not np.isnan(pred) for pred in predictions)
        assert all(pred < 1e6 for pred in predictions)  # Sanity check for extreme values

    def test_save_and_load(self, sample_ohlc_data, temp_model_dir):
        """Test model save and load."""
        model = LSTMPricePredictor(sequence_length=30, lstm_units=[16])
        model.train(sample_ohlc_data[:200], epochs=2, experiment_name="test-lstm-save")

        # Save
        model_path = temp_model_dir / "lstm_test"
        model.save(model_path)

        # Load
        loaded_model = LSTMPricePredictor(sequence_length=30)
        loaded_model.load(model_path)

        # Test loaded model
        predictions = loaded_model.predict(sample_ohlc_data[170:200], steps=1)
        assert len(predictions) == 1


class TestIsolationForestDetector:
    """Tests for Isolation Forest anomaly detector."""

    def test_initialization(self):
        """Test detector initialization."""
        detector = IsolationForestDetector(
            contamination=0.1,
            n_estimators=50,
        )

        assert detector.contamination == 0.1
        assert detector.n_estimators == 50

    def test_fit_and_predict(self, sample_ohlc_data):
        """Test fitting and prediction."""
        detector = IsolationForestDetector(contamination=0.1)

        # Fit
        metrics = detector.fit(
            sample_ohlc_data,
            feature_columns=["volume", "total_traded_quantity"],
            experiment_name="test-iforest",
        )

        assert "n_samples" in metrics
        assert "n_anomalies" in metrics
        assert metrics["anomaly_rate"] > 0

        # Predict
        predictions, scores = detector.predict(sample_ohlc_data[:50])

        assert len(predictions) == 50
        assert len(scores) == 50
        assert all(p in [-1, 1] for p in predictions)

    def test_save_and_load(self, sample_ohlc_data, temp_model_dir):
        """Test model save and load."""
        detector = IsolationForestDetector()
        detector.fit(
            sample_ohlc_data,
            feature_columns=["volume"],
            experiment_name="test-iforest-save",
        )

        # Save
        model_path = temp_model_dir / "iforest_test"
        detector.save(model_path)

        # Load
        loaded_detector = IsolationForestDetector()
        loaded_detector.load(model_path)

        # Test loaded model
        predictions, _ = loaded_detector.predict(sample_ohlc_data[:10])
        assert len(predictions) == 10


class TestAutoencoderDetector:
    """Tests for Autoencoder anomaly detector."""

    def test_initialization(self):
        """Test detector initialization."""
        detector = AutoencoderDetector(
            sequence_length=20,
            encoding_dim=8,
        )

        assert detector.sequence_length == 20
        assert detector.encoding_dim == 8

    def test_fit_and_predict(self, sample_ohlc_data):
        """Test fitting and prediction."""
        detector = AutoencoderDetector(sequence_length=15, encoding_dim=5)

        # Fit
        history = detector.fit(
            sample_ohlc_data[:200],
            feature_columns=["open", "high", "low", "close"],
            epochs=2,
            experiment_name="test-autoencoder",
        )

        assert "loss" in history
        assert detector.threshold is not None

        # Predict
        predictions, errors = detector.predict(sample_ohlc_data[150:200])

        assert len(predictions) > 0
        assert len(errors) > 0
        assert all(p in [0, 1] for p in predictions)


class TestPortfolioOptimizer:
    """Tests for portfolio optimizer."""

    @pytest.fixture
    def multi_asset_data(self):
        """Create multi-asset data for testing."""
        np.random.seed(42)
        symbols = ["STOCK1", "STOCK2", "STOCK3"]
        dates = pd.date_range(start="2023-01-01", periods=300, freq="D")

        data = []
        for symbol in symbols:
            base_price = np.random.uniform(500, 2000)
            for trade_date in dates:
                change = np.random.normal(0.001, 0.02)
                close = base_price * (1 + change)

                data.append(
                    {
                        "symbol": symbol,
                        "trade_date": trade_date,
                        "close": close,
                    }
                )

                base_price = close

        return pd.DataFrame(data)

    def test_initialization(self):
        """Test optimizer initialization."""
        optimizer = PortfolioOptimizer(
            risk_free_rate=0.05,
            max_position_size=0.4,
        )

        assert optimizer.risk_free_rate == 0.05
        assert optimizer.max_position_size == 0.4

    def test_optimization(self, multi_asset_data):
        """Test portfolio optimization."""
        optimizer = PortfolioOptimizer()

        results = optimizer.optimize(
            multi_asset_data,
            price_column="close",
            lookback_period=200,
            experiment_name="test-portfolio",
        )

        assert "weights" in results
        assert "expected_return" in results
        assert "sharpe_ratio" in results
        assert len(results["weights"]) == 3
        assert abs(sum(results["weights"].values()) - 1.0) < 0.01  # Weights sum to 1

    def test_sector_constraints(self, multi_asset_data):
        """Test portfolio optimization with sector constraints."""
        optimizer = PortfolioOptimizer()

        sector_mapping = {
            "STOCK1": "Tech",
            "STOCK2": "Finance",
            "STOCK3": "Tech",
        }

        sector_limits = {
            "Tech": 0.6,
            "Finance": 0.5,
        }

        results = optimizer.optimize_with_sector_constraints(
            multi_asset_data,
            sector_mapping,
            sector_limits,
            experiment_name="test-portfolio-sectors",
        )

        assert "sector_exposures" in results
        assert results["sector_exposures"]["Tech"] <= 0.61  # Small tolerance
        assert results["sector_exposures"]["Finance"] <= 0.51


class TestBacktester:
    """Tests for backtesting framework."""

    def test_initialization(self):
        """Test backtester initialization."""
        backtester = Backtester(
            initial_capital=100000,
            transaction_cost=0.001,
        )

        assert backtester.initial_capital == 100000
        assert backtester.transaction_cost == 0.001

    def test_run_backtest(self, sample_ohlc_data):
        """Test backtest execution."""
        model = LSTMPricePredictor(sequence_length=30, lstm_units=[16])
        backtester = Backtester(initial_capital=100000)

        results = backtester.run_backtest(
            sample_ohlc_data,
            model,
            train_window=150,
            test_window=10,
            retrain_frequency=20,
            experiment_name="test-backtest",
        )

        assert "final_value" in results
        assert "total_return" in results
        assert "sharpe_ratio" in results
        assert results["initial_capital"] == 100000
        assert len(results["trades"]) >= 0


class TestAlertGenerator:
    """Tests for alert generation."""

    def test_initialization(self):
        """Test alert generator initialization."""
        alert_gen = AlertGenerator(
            volume_threshold=2.0,
            price_movement_threshold=0.05,
        )

        assert alert_gen.volume_threshold == 2.0
        assert alert_gen.price_movement_threshold == 0.05

    def test_volume_alerts(self, sample_ohlc_data):
        """Test volume anomaly alert generation."""
        alert_gen = AlertGenerator()

        # Create fake anomaly predictions
        predictions = [-1 if i % 10 == 0 else 1 for i in range(len(sample_ohlc_data[:50]))]
        scores = np.random.uniform(-0.5, 0, len(sample_ohlc_data[:50]))

        alerts = alert_gen.generate_volume_alerts(
            sample_ohlc_data[:50],
            predictions,
            scores.tolist(),
        )

        assert len(alerts) > 0
        assert all(isinstance(alert.severity, AlertSeverity) for alert in alerts)
        assert all(alert.alert_type == AlertType.VOLUME_ANOMALY for alert in alerts)

    def test_price_movement_alerts(self, sample_ohlc_data):
        """Test price movement alert generation."""
        alert_gen = AlertGenerator(price_movement_threshold=0.01)

        # Create data with large movements
        test_data = sample_ohlc_data.copy()
        test_data.loc[10, "close"] = test_data.loc[10, "open"] * 1.08  # 8% gain

        alerts = alert_gen.generate_price_movement_alerts(test_data[:50])

        assert len(alerts) > 0
        assert all(alert.alert_type == AlertType.LARGE_PRICE_MOVEMENT for alert in alerts)

    def test_export_alerts(self, sample_ohlc_data):
        """Test alert export to DataFrame."""
        alert_gen = AlertGenerator()

        # Generate some alerts
        alert_gen.generate_price_movement_alerts(sample_ohlc_data[:50])

        # Export
        df = alert_gen.export_alerts()

        assert len(df) > 0
        assert "alert_type" in df.columns
        assert "severity" in df.columns

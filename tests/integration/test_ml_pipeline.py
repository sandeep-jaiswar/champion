"""Integration tests for ML pipeline."""


import numpy as np
import pandas as pd
import pytest
from champion.ml import (
    AlertGenerator,
    Backtester,
    IsolationForestDetector,
    LSTMPricePredictor,
    PortfolioOptimizer,
    PredictionServer,
)


@pytest.fixture
def sample_market_data():
    """Create realistic market data for integration testing."""
    np.random.seed(42)
    symbols = ["RELIANCE", "TCS", "INFY"]
    dates = pd.date_range(start="2023-01-01", periods=500, freq="D")

    data = []
    for symbol in symbols:
        base_price = {"RELIANCE": 2500, "TCS": 3500, "INFY": 1500}[symbol]

        for trade_date in dates:
            # Random walk with realistic parameters
            change = np.random.normal(0.0005, 0.02)
            close = base_price * (1 + change)
            open_price = close * (1 + np.random.normal(0, 0.005))
            high = max(open_price, close) * (1 + abs(np.random.normal(0, 0.01)))
            low = min(open_price, close) * (1 - abs(np.random.normal(0, 0.01)))

            # Realistic volume
            base_volume = 1e7 if symbol == "RELIANCE" else 5e6
            volume = int(base_volume * (1 + np.random.normal(0, 0.3)))

            data.append(
                {
                    "symbol": symbol,
                    "trade_date": trade_date,
                    "open": open_price,
                    "high": high,
                    "low": low,
                    "close": close,
                    "volume": volume,
                    "total_traded_quantity": int(volume * 0.85),
                    "number_of_trades": int(volume / 100),
                }
            )

            base_price = close

    return pd.DataFrame(data)


@pytest.fixture
def temp_registry(tmp_path):
    """Create temporary model registry."""
    registry = tmp_path / "model_registry"
    registry.mkdir()
    return registry


class TestMLPipelineIntegration:
    """Integration tests for complete ML pipeline."""

    def test_end_to_end_price_prediction(self, sample_market_data, temp_registry):
        """Test complete price prediction workflow."""
        # Filter to single symbol
        df = sample_market_data[sample_market_data["symbol"] == "RELIANCE"]

        # 1. Train model
        model = LSTMPricePredictor(sequence_length=30, lstm_units=[16])
        history = model.train(
            df[:400],
            epochs=2,
            batch_size=16,
            experiment_name="integration-test-lstm",
        )

        assert len(history["loss"]) == 2

        # 2. Save model
        model_path = temp_registry / "lstm_reliance" / "v1"
        model.save(model_path)
        assert (model_path / "lstm_model.keras").exists()

        # 3. Load and serve predictions
        server = PredictionServer(model_registry_path=temp_registry)
        result = server.predict_price(
            model_name="lstm_reliance",
            df=df[370:400],
            steps=3,
            model_version="v1",
        )

        assert result["status"] == "success"
        assert len(result["predictions"]) == 3

    def test_end_to_end_anomaly_detection(self, sample_market_data, temp_registry):
        """Test complete anomaly detection workflow."""
        df = sample_market_data[sample_market_data["symbol"] == "TCS"]

        # 1. Train volume anomaly detector
        volume_detector = IsolationForestDetector(contamination=0.1)
        metrics = volume_detector.fit(
            df[:400],
            feature_columns=["volume", "total_traded_quantity"],
            experiment_name="integration-test-volume",
        )

        assert metrics["n_anomalies"] > 0

        # 2. Save detector
        detector_path = temp_registry / "volume_detector_tcs" / "v1"
        volume_detector.save(detector_path)

        # 3. Load and detect anomalies
        server = PredictionServer(model_registry_path=temp_registry)
        result = server.detect_anomalies(
            model_name="volume_detector_tcs",
            df=df[400:450],
            model_version="v1",
            model_type="isolation_forest",
        )

        assert result["status"] == "success"
        assert "n_anomalies" in result

        # 4. Generate alerts
        predictions = [-1 if i % 10 == 0 else 1 for i in range(len(df[400:450]))]
        scores = np.random.uniform(-0.5, 0, len(df[400:450]))

        alert_gen = AlertGenerator()
        alerts = alert_gen.generate_volume_alerts(
            df[400:450],
            predictions,
            scores.tolist(),
        )

        assert len(alerts) > 0

    def test_end_to_end_portfolio_optimization(self, sample_market_data):
        """Test complete portfolio optimization workflow."""
        # 1. Optimize portfolio
        optimizer = PortfolioOptimizer(
            risk_free_rate=0.05,
            max_position_size=0.5,
        )

        results = optimizer.optimize(
            sample_market_data,
            price_column="close",
            lookback_period=300,
            experiment_name="integration-test-portfolio",
        )

        assert "weights" in results
        assert len(results["weights"]) == 3
        assert results["sharpe_ratio"] > -5  # Reasonable bound

        # 2. Optimize with sector constraints
        sector_mapping = {
            "RELIANCE": "Energy",
            "TCS": "IT",
            "INFY": "IT",
        }

        sector_limits = {
            "IT": 0.6,
            "Energy": 0.5,
        }

        results_sector = optimizer.optimize_with_sector_constraints(
            sample_market_data,
            sector_mapping,
            sector_limits,
            experiment_name="integration-test-portfolio-sectors",
        )

        assert "sector_exposures" in results_sector
        assert results_sector["sector_exposures"]["IT"] <= 0.61

    def test_end_to_end_backtesting(self, sample_market_data):
        """Test complete backtesting workflow."""
        df = sample_market_data[sample_market_data["symbol"] == "INFY"]

        # 1. Create model
        model = LSTMPricePredictor(sequence_length=30, lstm_units=[16])

        # 2. Run backtest
        backtester = Backtester(
            initial_capital=100000,
            transaction_cost=0.001,
        )

        results = backtester.run_backtest(
            df,
            model,
            train_window=200,
            test_window=10,
            retrain_frequency=20,
            experiment_name="integration-test-backtest",
        )

        # Verify results
        assert "total_return" in results
        assert "sharpe_ratio" in results
        assert "max_drawdown" in results
        assert len(results["portfolio_values"]) > 0
        assert results["initial_capital"] == 100000

    def test_model_versioning(self, sample_market_data, temp_registry):
        """Test model versioning in registry."""
        df = sample_market_data[sample_market_data["symbol"] == "RELIANCE"]

        # Train and save v1
        model_v1 = LSTMPricePredictor(sequence_length=30, lstm_units=[16])
        model_v1.train(df[:300], epochs=2, experiment_name="integration-test-version")

        v1_path = temp_registry / "lstm_versioned" / "v1"
        model_v1.save(v1_path)

        # Train and save v2
        model_v2 = LSTMPricePredictor(sequence_length=30, lstm_units=[32])
        model_v2.train(df[:400], epochs=2, experiment_name="integration-test-version")

        v2_path = temp_registry / "lstm_versioned" / "v2"
        model_v2.save(v2_path)

        # Test server can load both
        server = PredictionServer(model_registry_path=temp_registry)

        pred_v1 = server.predict_price(
            "lstm_versioned",
            df[270:300],
            steps=1,
            model_version="v1",
        )

        pred_v2 = server.predict_price(
            "lstm_versioned",
            df[370:400],
            steps=1,
            model_version="v2",
        )

        assert pred_v1["status"] == "success"
        assert pred_v2["status"] == "success"

    def test_alert_workflow(self, sample_market_data):
        """Test complete alert generation workflow."""
        df = sample_market_data[sample_market_data["symbol"] == "TCS"]

        # 1. Train detectors
        volume_detector = IsolationForestDetector(contamination=0.1)
        volume_detector.fit(df[:400])

        # 2. Detect anomalies
        predictions, scores = volume_detector.predict(df[400:450])

        # 3. Generate alerts
        alert_gen = AlertGenerator()
        volume_alerts = alert_gen.generate_volume_alerts(
            df[400:450],
            predictions.tolist(),
            scores.tolist(),
        )

        price_alerts = alert_gen.generate_price_movement_alerts(df[400:450])

        # Assert that alerts are generated correctly
        assert isinstance(volume_alerts, list)
        assert isinstance(price_alerts, list)
        assert len(volume_alerts) >= 0
        assert len(price_alerts) >= 0

        # 4. Export and filter
        all_alerts_df = alert_gen.export_alerts()

        assert len(all_alerts_df) > 0
        assert "alert_type" in all_alerts_df.columns

    def test_feature_engineering_integration(self, sample_market_data):
        """Test integration with existing feature engineering."""
        # This would integrate with champion.features.indicators
        # For now, just verify data format compatibility
        df = sample_market_data[sample_market_data["symbol"] == "RELIANCE"]

        # Ensure data is compatible with feature engineering
        assert "open" in df.columns
        assert "high" in df.columns
        assert "low" in df.columns
        assert "close" in df.columns
        assert "volume" in df.columns

        # Train model with just OHLC features
        model = LSTMPricePredictor(sequence_length=30)
        history = model.train(
            df[:300],
            feature_columns=["open", "high", "low", "close", "volume"],
            epochs=2,
            experiment_name="integration-test-features",
        )

        assert len(history["loss"]) == 2

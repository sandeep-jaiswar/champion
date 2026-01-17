"""Demo script for ML pipeline features.

This script demonstrates:
1. Training LSTM price prediction model
2. Running backtesting
3. Detecting anomalies (volume and price patterns)
4. Portfolio optimization
5. Real-time prediction serving
6. Alert generation
"""

from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from champion.ml import (
    AlertGenerator,
    AutoencoderDetector,
    Backtester,
    IsolationForestDetector,
    LSTMPricePredictor,
    PortfolioOptimizer,
    PredictionServer,
)


def generate_sample_data(
    symbols: list[str] = None,
    start_date: date = None,
    end_date: date = None,
) -> pd.DataFrame:
    """Generate sample OHLC data for demonstration.

    Args:
        symbols: List of symbol names
        start_date: Start date for data
        end_date: End date for data

    Returns:
        DataFrame with OHLC data
    """
    if symbols is None:
        symbols = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "WIPRO"]

    if start_date is None:
        start_date = date.today() - timedelta(days=500)

    if end_date is None:
        end_date = date.today()

    # Generate date range
    date_range = pd.date_range(start_date, end_date, freq="B")  # Business days

    records = []
    np.random.seed(42)

    for symbol in symbols:
        base_price = np.random.uniform(500, 3000)

        # Random walk with drift
        prices = [base_price]
        for _ in range(len(date_range) - 1):
            change = np.random.normal(0.001, 0.02)  # 0.1% drift, 2% volatility
            prices.append(prices[-1] * (1 + change))

        prices = np.array(prices)

        for i, trade_date in enumerate(date_range):
            close_price = prices[i]
            open_price = close_price * (1 + np.random.normal(0, 0.01))
            high_price = max(open_price, close_price) * (1 + abs(np.random.normal(0, 0.01)))
            low_price = min(open_price, close_price) * (1 - abs(np.random.normal(0, 0.01)))

            # Add occasional volume spikes for anomaly detection
            base_volume = np.random.uniform(1e6, 5e6)
            if np.random.random() < 0.05:  # 5% chance of volume spike
                volume = base_volume * np.random.uniform(3, 10)
            else:
                volume = base_volume * (1 + np.random.normal(0, 0.3))

            records.append(
                {
                    "symbol": symbol,
                    "trade_date": trade_date,
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "volume": int(volume),
                    "total_traded_quantity": int(volume * 0.8),
                    "number_of_trades": int(volume / 100),
                }
            )

    return pd.DataFrame(records)


def demo_lstm_prediction():
    """Demonstrate LSTM price prediction."""
    print("\n" + "=" * 60)
    print("DEMO 1: LSTM Price Prediction")
    print("=" * 60)

    # Generate sample data
    df = generate_sample_data(symbols=["RELIANCE"])
    print(f"Generated {len(df)} samples for RELIANCE")

    # Initialize and train model
    model = LSTMPricePredictor(
        sequence_length=60,
        lstm_units=[50, 50],
        dropout_rate=0.2,
    )

    print("\nTraining LSTM model...")
    history = model.train(
        df,
        target_column="close",
        epochs=10,
        batch_size=32,
        experiment_name="demo-lstm-prediction",
    )

    print(f"Training completed. Final loss: {history['loss'][-1]:.4f}")

    # Make predictions
    print("\nMaking predictions...")
    recent_data = df.tail(100)
    predictions = model.predict(recent_data, steps=5)

    print("Predicted prices for next 5 days:")
    for i, pred in enumerate(predictions, 1):
        print(f"  Day {i}: ₹{pred:.2f}")

    # Save model
    model_path = Path("data/models/lstm_reliance/v1")
    model.save(model_path)
    print(f"\nModel saved to {model_path}")


def demo_backtesting():
    """Demonstrate backtesting framework."""
    print("\n" + "=" * 60)
    print("DEMO 2: Backtesting Framework")
    print("=" * 60)

    # Generate sample data
    df = generate_sample_data(symbols=["TCS"])
    print(f"Generated {len(df)} samples for TCS")

    # Initialize model
    model = LSTMPricePredictor(sequence_length=60, lstm_units=[30, 30])

    # Initialize backtester
    backtester = Backtester(
        initial_capital=100000,
        transaction_cost=0.001,
    )

    print("\nRunning backtest...")
    results = backtester.run_backtest(
        df,
        model,
        train_window=200,
        test_window=20,
        retrain_frequency=20,
        experiment_name="demo-backtesting",
    )

    print("\nBacktest Results:")
    print(f"  Initial Capital: ₹{results['initial_capital']:,.2f}")
    print(f"  Final Value: ₹{results['final_value']:,.2f}")
    print(f"  Total Return: {results['total_return']*100:.2f}%")
    print(f"  Buy & Hold Return: {results['buy_and_hold_return']*100:.2f}%")
    print(f"  Alpha: {results['alpha']*100:.2f}%")
    print(f"  Sharpe Ratio: {results['sharpe_ratio']:.2f}")
    print(f"  Max Drawdown: {results['max_drawdown']*100:.2f}%")
    print(f"  Number of Trades: {results['n_trades']}")


def demo_anomaly_detection():
    """Demonstrate anomaly detection models."""
    print("\n" + "=" * 60)
    print("DEMO 3: Anomaly Detection")
    print("=" * 60)

    # Generate sample data
    df = generate_sample_data(symbols=["INFY"])
    print(f"Generated {len(df)} samples for INFY")

    # Volume anomaly detection with Isolation Forest
    print("\n3a. Volume Anomaly Detection (Isolation Forest)")
    volume_detector = IsolationForestDetector(contamination=0.1)

    print("Training Isolation Forest...")
    volume_metrics = volume_detector.fit(
        df,
        feature_columns=["volume", "total_traded_quantity", "number_of_trades"],
        experiment_name="demo-volume-anomaly",
    )

    print(f"  Samples: {volume_metrics['n_samples']}")
    print(f"  Anomalies detected: {volume_metrics['n_anomalies']}")
    print(f"  Anomaly rate: {volume_metrics['anomaly_rate']*100:.2f}%")

    # Test on recent data
    recent_data = df.tail(50)
    predictions, scores = volume_detector.predict(recent_data)
    n_anomalies = (predictions == -1).sum()
    print(f"  Recent anomalies: {n_anomalies} out of 50")

    # Price pattern anomaly detection with Autoencoder
    print("\n3b. Price Pattern Anomaly Detection (Autoencoder)")
    pattern_detector = AutoencoderDetector(sequence_length=20, encoding_dim=10)

    print("Training Autoencoder...")
    pattern_history = pattern_detector.fit(
        df,
        feature_columns=["open", "high", "low", "close"],
        epochs=10,
        experiment_name="demo-price-pattern-anomaly",
    )

    print(f"  Training loss: {pattern_history['loss'][-1]:.4f}")
    print(f"  Anomaly threshold: {pattern_detector.threshold:.4f}")

    # Test on recent data
    predictions, errors = pattern_detector.predict(recent_data)
    n_anomalies = predictions.sum()
    print(f"  Recent anomalies: {n_anomalies} out of {len(recent_data) - 20}")


def demo_portfolio_optimization():
    """Demonstrate portfolio optimization."""
    print("\n" + "=" * 60)
    print("DEMO 4: Portfolio Optimization (Modern Portfolio Theory)")
    print("=" * 60)

    # Generate sample data for multiple assets
    symbols = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "WIPRO"]
    df = generate_sample_data(symbols=symbols)
    print(f"Generated data for {len(symbols)} symbols")

    # Initialize optimizer
    optimizer = PortfolioOptimizer(
        risk_free_rate=0.05,
        max_position_size=0.40,
        min_position_size=0.05,
    )

    print("\nOptimizing portfolio...")
    results = optimizer.optimize(
        df,
        price_column="close",
        lookback_period=252,
        experiment_name="demo-portfolio-optimization",
    )

    print("\nOptimal Portfolio:")
    print("  Weights:")
    for symbol, weight in results["weights"].items():
        print(f"    {symbol}: {weight*100:.2f}%")

    print(f"\n  Expected Annual Return: {results['expected_return']*100:.2f}%")
    print(f"  Annual Volatility: {results['volatility']*100:.2f}%")
    print(f"  Sharpe Ratio: {results['sharpe_ratio']:.2f}")

    # Portfolio with sector constraints
    print("\n4b. Portfolio Optimization with Sector Constraints")

    sector_mapping = {
        "RELIANCE": "Energy",
        "TCS": "IT",
        "INFY": "IT",
        "HDFCBANK": "Finance",
        "WIPRO": "IT",
    }

    sector_limits = {
        "IT": 0.50,  # Max 50% in IT
        "Energy": 0.30,  # Max 30% in Energy
        "Finance": 0.30,  # Max 30% in Finance
    }

    results_sector = optimizer.optimize_with_sector_constraints(
        df,
        sector_mapping,
        sector_limits,
        experiment_name="demo-portfolio-optimization-sectors",
    )

    print("\nOptimal Portfolio (with sector constraints):")
    print("  Sector Exposures:")
    for sector, weight in results_sector["sector_exposures"].items():
        print(f"    {sector}: {weight*100:.2f}%")


def demo_alert_generation():
    """Demonstrate alert generation."""
    print("\n" + "=" * 60)
    print("DEMO 5: Alert Generation")
    print("=" * 60)

    # Generate sample data
    df = generate_sample_data(symbols=["HDFCBANK"])

    # Initialize alert generator
    alert_gen = AlertGenerator(
        volume_threshold=2.0,
        price_movement_threshold=0.05,
    )

    # Detect volume anomalies
    volume_detector = IsolationForestDetector(contamination=0.1)
    volume_detector.fit(df)
    predictions, scores = volume_detector.predict(df.tail(50))

    # Generate volume alerts
    print("\nGenerating volume anomaly alerts...")
    volume_alerts = alert_gen.generate_volume_alerts(
        df.tail(50),
        predictions.tolist(),
        scores.tolist(),
    )

    print(f"Generated {len(volume_alerts)} volume alerts")
    for alert in volume_alerts[:3]:  # Show first 3
        print(f"\n  [{alert.severity.upper()}] {alert.alert_type}")
        print(f"  {alert.message}")
        print(f"  Symbol: {alert.symbol}")

    # Generate price movement alerts
    print("\nGenerating price movement alerts...")
    price_alerts = alert_gen.generate_price_movement_alerts(df.tail(50))

    print(f"Generated {len(price_alerts)} price movement alerts")
    for alert in price_alerts[:3]:  # Show first 3
        print(f"\n  [{alert.severity.upper()}] {alert.alert_type}")
        print(f"  {alert.message}")

    # Export alerts
    alerts_df = alert_gen.export_alerts()
    print(f"\nTotal alerts: {len(alerts_df)}")


def demo_prediction_serving():
    """Demonstrate real-time prediction serving."""
    print("\n" + "=" * 60)
    print("DEMO 6: Real-time Prediction Serving")
    print("=" * 60)

    # First, train and save a model
    df = generate_sample_data(symbols=["WIPRO"])

    model = LSTMPricePredictor(sequence_length=60)
    print("Training model for serving...")
    model.train(df, epochs=5, verbose=0, experiment_name="demo-prediction-serving")

    model_path = Path("data/models/lstm_wipro/latest")
    model.save(model_path)
    print(f"Model saved to {model_path}")

    # Initialize prediction server
    print("\nInitializing prediction server...")
    server = PredictionServer(model_registry_path=Path("data/models"))

    # List available models
    available_models = server.list_models()
    print(f"Available models: {len(available_models)}")
    for model_info in available_models:
        print(f"  - {model_info['model_name']} (version: {model_info['version']})")

    # Get predictions
    print("\nGetting predictions...")
    pred_result = server.predict_price(
        model_name="lstm_wipro",
        df=df.tail(100),
        steps=3,
        model_version="latest",
    )

    if pred_result["status"] == "success":
        print("Predictions for next 3 days:")
        for i, pred in enumerate(pred_result["predictions"], 1):
            print(f"  Day {i}: ₹{pred:.2f}")
    else:
        print(f"Error: {pred_result['error']}")


def main():
    """Run all demos."""
    print("\n" + "=" * 60)
    print("CHAMPION ML PIPELINE DEMONSTRATION")
    print("=" * 60)

    try:
        demo_lstm_prediction()
        demo_backtesting()
        demo_anomaly_detection()
        demo_portfolio_optimization()
        demo_alert_generation()
        demo_prediction_serving()

        print("\n" + "=" * 60)
        print("ALL DEMOS COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print("\nCheck MLflow UI at http://localhost:5000 for experiment tracking")

    except Exception as e:
        print(f"\nError during demo: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()

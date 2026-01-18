# Machine Learning Pipeline for Predictive Analytics

This module provides production-ready ML models for stock market prediction, anomaly detection, and portfolio optimization.

## Features

### 1. Price Prediction (LSTM)

Time series prediction using LSTM neural networks.

```python
from champion.ml import LSTMPricePredictor

# Initialize model
model = LSTMPricePredictor(
    sequence_length=60,
    lstm_units=[50, 50],
    dropout_rate=0.2,
)

# Train on historical data
history = model.train(
    df,
    target_column="close",
    epochs=50,
    batch_size=32,
    experiment_name="price-prediction",
)

# Make predictions
predictions = model.predict(df, steps=5)  # Predict next 5 days
```

**Features:**

- Multi-layer LSTM architecture
- Dropout regularization
- MinMax scaling for normalization
- MLflow integration for experiment tracking
- Model persistence (save/load)

### 2. Anomaly Detection

Two complementary approaches for detecting market anomalies:

#### Isolation Forest (Volume Anomalies)

```python
from champion.ml import IsolationForestDetector

detector = IsolationForestDetector(contamination=0.1)

# Train on historical data
metrics = detector.fit(
    df,
    feature_columns=["volume", "total_traded_quantity"],
    experiment_name="volume-anomaly-detection",
)

# Detect anomalies
predictions, scores = detector.predict(df)
# predictions: -1 for anomaly, 1 for normal
# scores: Lower scores indicate stronger anomalies
```

#### Autoencoder (Price Pattern Anomalies)

```python
from champion.ml import AutoencoderDetector

detector = AutoencoderDetector(
    sequence_length=20,
    encoding_dim=10,
)

# Train on normal data
history = detector.fit(
    df,
    feature_columns=["open", "high", "low", "close"],
    epochs=50,
    experiment_name="price-pattern-anomaly",
)

# Detect anomalies
predictions, errors = detector.predict(df)
# predictions: 1 for anomaly, 0 for normal
# errors: Reconstruction error for each sequence
```

### 3. Portfolio Optimization

Modern Portfolio Theory (MPT) implementation with sector constraints.

```python
from champion.ml import PortfolioOptimizer

optimizer = PortfolioOptimizer(
    risk_free_rate=0.05,
    max_position_size=0.30,
)

# Optimize portfolio
results = optimizer.optimize(
    df,
    price_column="close",
    lookback_period=252,
    experiment_name="portfolio-optimization",
)

print(f"Optimal weights: {results['weights']}")
print(f"Expected return: {results['expected_return']:.2%}")
print(f"Sharpe ratio: {results['sharpe_ratio']:.2f}")
```

### 4. Backtesting Framework

Walk-forward backtesting for model validation.

```python
from champion.ml import Backtester

backtester = Backtester(
    initial_capital=100000,
    transaction_cost=0.001,  # 0.1%
)

results = backtester.run_backtest(
    df,
    model,
    train_window=252,
    test_window=20,
    retrain_frequency=20,
)
```

### 5. Real-time Prediction Serving

```python
from champion.ml import PredictionServer

server = PredictionServer(model_registry_path="data/models")

# Get predictions
result = server.predict_price(
    model_name="lstm_reliance",
    df=recent_data,
    steps=3,
)
```

### 6. Alert Generation

```python
from champion.ml import AlertGenerator

alert_gen = AlertGenerator()

# Generate alerts
alerts = alert_gen.generate_volume_alerts(df, predictions, scores)
```

## Quick Start

Run the demo script:

```bash
poetry run python src/champion/ml/demo_ml_pipeline.py
```

## Dependencies

- `tensorflow` >= 2.15.0
- `scikit-learn` >= 1.3.0
- `scipy` >= 1.11.0
- `mlflow` >= 3.5.0

## MLflow Integration

All models integrate with MLflow for experiment tracking. Access UI at `http://localhost:5000`

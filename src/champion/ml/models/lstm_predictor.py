"""LSTM-based price prediction model for time series forecasting."""

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import structlog
from sklearn.preprocessing import MinMaxScaler

try:
    import tensorflow as tf
    from tensorflow import keras
    from tensorflow.keras import layers
except ImportError:
    tf = None
    keras = None
    layers = None

from champion.ml.tracking import MLflowTracker

logger = structlog.get_logger()


class LSTMPricePredictor:
    """LSTM model for stock price prediction.

    This model uses historical OHLC data and technical indicators to
    predict future price movements.

    Features:
    - Multi-layer LSTM architecture
    - Dropout for regularization
    - MinMax scaling for normalization
    - MLflow integration for experiment tracking
    """

    def __init__(
        self,
        sequence_length: int = 60,
        lstm_units: list[int] | None = None,
        dropout_rate: float = 0.2,
        learning_rate: float = 0.001,
    ):
        """Initialize LSTM predictor.

        Args:
            sequence_length: Number of time steps to use for prediction
            lstm_units: List of LSTM layer units (default: [50, 50])
            dropout_rate: Dropout rate for regularization
            learning_rate: Learning rate for Adam optimizer
        """
        if tf is None:
            raise ImportError("TensorFlow is required for LSTMPricePredictor")

        self.sequence_length = sequence_length
        self.lstm_units = lstm_units or [50, 50]
        self.dropout_rate = dropout_rate
        self.learning_rate = learning_rate

        self.model = None
        self.scaler = MinMaxScaler(feature_range=(0, 1))
        self.feature_columns = None

        logger.info(
            "lstm_predictor_initialized",
            sequence_length=sequence_length,
            lstm_units=self.lstm_units,
            dropout_rate=dropout_rate,
        )

    def _build_model(self, input_shape: tuple[int, int]) -> keras.Model:
        """Build LSTM model architecture.

        Args:
            input_shape: Shape of input data (sequence_length, n_features)

        Returns:
            Compiled Keras model
        """
        model = keras.Sequential(name="lstm_price_predictor")

        # First LSTM layer
        model.add(
            layers.LSTM(
                self.lstm_units[0],
                return_sequences=len(self.lstm_units) > 1,
                input_shape=input_shape,
            )
        )
        model.add(layers.Dropout(self.dropout_rate))

        # Additional LSTM layers
        for i, units in enumerate(self.lstm_units[1:], 1):
            return_sequences = i < len(self.lstm_units) - 1
            model.add(layers.LSTM(units, return_sequences=return_sequences))
            model.add(layers.Dropout(self.dropout_rate))

        # Output layer
        model.add(layers.Dense(1))

        # Compile model
        model.compile(
            optimizer=keras.optimizers.Adam(learning_rate=self.learning_rate),
            loss="mean_squared_error",
            metrics=["mean_absolute_error"],
        )

        logger.info("lstm_model_built", input_shape=input_shape)
        return model

    def _prepare_sequences(
        self,
        data: np.ndarray,
        target_col_idx: int = 0,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Prepare sequences for training/prediction.

        Args:
            data: Scaled data array
            target_col_idx: Index of target column (default: 0 for close price)

        Returns:
            Tuple of (X, y) arrays
        """
        X, y = [], []

        for i in range(self.sequence_length, len(data)):
            X.append(data[i - self.sequence_length : i])
            y.append(data[i, target_col_idx])

        return np.array(X), np.array(y)

    def train(
        self,
        df: pd.DataFrame,
        target_column: str = "close",
        feature_columns: list[str] | None = None,
        epochs: int = 50,
        batch_size: int = 32,
        validation_split: float = 0.2,
        experiment_name: str = "lstm-price-prediction",
    ) -> dict[str, Any]:
        """Train LSTM model on historical data.

        Args:
            df: DataFrame with OHLC and feature data
            target_column: Column to predict
            feature_columns: List of feature columns to use
            epochs: Number of training epochs
            batch_size: Training batch size
            validation_split: Fraction of data for validation
            experiment_name: MLflow experiment name

        Returns:
            Training history dictionary
        """
        if feature_columns is None:
            feature_columns = [
                "open",
                "high",
                "low",
                "close",
                "volume",
            ]

        self.feature_columns = feature_columns

        # Ensure target is first column for easy indexing
        if target_column not in feature_columns:
            feature_columns = [target_column] + feature_columns

        # Extract features
        data = df[feature_columns].values

        # Scale data
        scaled_data = self.scaler.fit_transform(data)

        # Prepare sequences
        X, y = self._prepare_sequences(scaled_data)

        logger.info(
            "training_data_prepared",
            samples=len(X),
            features=X.shape[2],
            sequence_length=X.shape[1],
        )

        # Build model
        self.model = self._build_model((X.shape[1], X.shape[2]))

        # Track with MLflow
        tracker = MLflowTracker(experiment_name=experiment_name)

        with tracker.start_run(run_name=f"lstm-train-{pd.Timestamp.now().isoformat()}"):
            # Log hyperparameters
            tracker.log_params(
                {
                    "sequence_length": self.sequence_length,
                    "lstm_units": str(self.lstm_units),
                    "dropout_rate": self.dropout_rate,
                    "learning_rate": self.learning_rate,
                    "epochs": epochs,
                    "batch_size": batch_size,
                    "validation_split": validation_split,
                    "n_features": len(feature_columns),
                    "target_column": target_column,
                }
            )

            # Train model
            history = self.model.fit(
                X,
                y,
                epochs=epochs,
                batch_size=batch_size,
                validation_split=validation_split,
                verbose=1,
            )

            # Log metrics
            final_loss = history.history["loss"][-1]
            final_val_loss = history.history["val_loss"][-1]
            final_mae = history.history["mean_absolute_error"][-1]
            final_val_mae = history.history["val_mean_absolute_error"][-1]

            tracker.log_metrics(
                {
                    "final_loss": final_loss,
                    "final_val_loss": final_val_loss,
                    "final_mae": final_mae,
                    "final_val_mae": final_val_mae,
                }
            )

            logger.info(
                "lstm_training_completed",
                final_loss=final_loss,
                final_val_loss=final_val_loss,
            )

        return history.history

    def predict(self, df: pd.DataFrame, steps: int = 1) -> np.ndarray:
        """Predict future prices.

        Args:
            df: DataFrame with recent data (at least sequence_length rows)
            steps: Number of steps ahead to predict

        Returns:
            Array of predicted prices
        """
        if self.model is None:
            raise ValueError("Model must be trained before prediction")

        if len(df) < self.sequence_length:
            raise ValueError(
                f"Need at least {self.sequence_length} rows for prediction, got {len(df)}"
            )

        # Extract and scale features
        data = df[self.feature_columns].values
        scaled_data = self.scaler.transform(data)

        predictions = []

        for _ in range(steps):
            # Use last sequence_length points
            X = scaled_data[-self.sequence_length :].reshape(1, self.sequence_length, -1)

            # Predict next value
            pred_scaled = self.model.predict(X, verbose=0)

            # Inverse transform
            pred_full = np.zeros((1, len(self.feature_columns)))
            pred_full[0, 0] = pred_scaled[0, 0]
            pred = self.scaler.inverse_transform(pred_full)[0, 0]

            predictions.append(pred)

            # Update scaled_data for multi-step prediction
            if steps > 1:
                new_row = scaled_data[-1].copy()
                new_row[0] = pred_scaled[0, 0]
                scaled_data = np.vstack([scaled_data, new_row])

        return np.array(predictions)

    def save(self, path: Path) -> None:
        """Save model to disk.

        Args:
            path: Directory path to save model
        """
        if self.model is None:
            raise ValueError("No model to save")

        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)

        # Save Keras model
        self.model.save(path / "lstm_model.keras")

        # Save scaler
        import joblib

        joblib.dump(self.scaler, path / "scaler.pkl")
        joblib.dump(self.feature_columns, path / "features.pkl")

        logger.info("lstm_model_saved", path=str(path))

    def load(self, path: Path) -> None:
        """Load model from disk.

        Args:
            path: Directory path containing saved model
        """
        path = Path(path)

        # Load Keras model
        self.model = keras.models.load_model(path / "lstm_model.keras")

        # Load scaler
        import joblib

        self.scaler = joblib.load(path / "scaler.pkl")
        self.feature_columns = joblib.load(path / "features.pkl")

        logger.info("lstm_model_loaded", path=str(path))

"""Anomaly detection models for market data analysis."""

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import structlog
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

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


class IsolationForestDetector:
    """Isolation Forest model for detecting volume anomalies.

    This model identifies unusual trading volumes that may indicate
    market events, news, or manipulation.
    """

    def __init__(
        self,
        contamination: float = 0.1,
        n_estimators: int = 100,
        max_samples: int = 256,
        random_state: int = 42,
    ):
        """Initialize Isolation Forest detector.

        Args:
            contamination: Expected proportion of outliers
            n_estimators: Number of trees in the forest
            max_samples: Number of samples to draw for each tree
            random_state: Random seed for reproducibility
        """
        self.contamination = contamination
        self.n_estimators = n_estimators
        self.max_samples = max_samples
        self.random_state = random_state

        self.model = IsolationForest(
            contamination=contamination,
            n_estimators=n_estimators,
            max_samples=max_samples,
            random_state=random_state,
        )
        self.scaler = StandardScaler()
        self.feature_columns = None

        logger.info(
            "isolation_forest_initialized",
            contamination=contamination,
            n_estimators=n_estimators,
        )

    def fit(
        self,
        df: pd.DataFrame,
        feature_columns: list[str] | None = None,
        experiment_name: str = "anomaly-detection-volume",
    ) -> dict[str, Any]:
        """Train Isolation Forest on historical data.

        Args:
            df: DataFrame with volume and related features
            feature_columns: List of feature columns
            experiment_name: MLflow experiment name

        Returns:
            Training metrics dictionary
        """
        if feature_columns is None:
            feature_columns = [
                "volume",
                "total_traded_quantity",
                "number_of_trades",
            ]

        self.feature_columns = feature_columns

        # Extract features
        X = df[feature_columns].values

        # Scale features
        X_scaled = self.scaler.fit_transform(X)

        # Track with MLflow
        tracker = MLflowTracker(experiment_name=experiment_name)

        with tracker.start_run(run_name=f"iforest-train-{pd.Timestamp.now().isoformat()}"):
            # Log hyperparameters
            tracker.log_params(
                {
                    "contamination": self.contamination,
                    "n_estimators": self.n_estimators,
                    "max_samples": self.max_samples,
                    "n_features": len(feature_columns),
                }
            )

            # Fit model
            self.model.fit(X_scaled)

            # Calculate anomaly scores
            scores = self.model.score_samples(X_scaled)
            predictions = self.model.predict(X_scaled)

            # Log metrics
            n_anomalies = (predictions == -1).sum()
            anomaly_rate = n_anomalies / len(predictions)

            tracker.log_metrics(
                {
                    "n_samples": len(X),
                    "n_anomalies": float(n_anomalies),
                    "anomaly_rate": anomaly_rate,
                    "mean_score": scores.mean(),
                    "std_score": scores.std(),
                }
            )

            logger.info(
                "isolation_forest_trained",
                n_samples=len(X),
                n_anomalies=n_anomalies,
                anomaly_rate=anomaly_rate,
            )

        return {
            "n_samples": len(X),
            "n_anomalies": int(n_anomalies),
            "anomaly_rate": float(anomaly_rate),
        }

    def predict(self, df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        """Detect anomalies in new data.

        Args:
            df: DataFrame with features

        Returns:
            Tuple of (predictions, anomaly_scores)
            predictions: -1 for anomalies, 1 for normal
            anomaly_scores: Lower scores indicate anomalies
        """
        X = df[self.feature_columns].values
        X_scaled = self.scaler.transform(X)

        predictions = self.model.predict(X_scaled)
        scores = self.model.score_samples(X_scaled)

        return predictions, scores

    def save(self, path: Path) -> None:
        """Save model to disk."""
        import joblib

        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)

        joblib.dump(self.model, path / "isolation_forest.pkl")
        joblib.dump(self.scaler, path / "scaler.pkl")
        joblib.dump(self.feature_columns, path / "features.pkl")

        logger.info("isolation_forest_saved", path=str(path))

    def load(self, path: Path) -> None:
        """Load model from disk."""
        import joblib

        path = Path(path)

        self.model = joblib.load(path / "isolation_forest.pkl")
        self.scaler = joblib.load(path / "scaler.pkl")
        self.feature_columns = joblib.load(path / "features.pkl")

        logger.info("isolation_forest_loaded", path=str(path))


class AutoencoderDetector:
    """Autoencoder model for detecting price pattern anomalies.

    This model learns to reconstruct normal price patterns and flags
    sequences with high reconstruction error as anomalies.
    """

    def __init__(
        self,
        sequence_length: int = 20,
        encoding_dim: int = 10,
        learning_rate: float = 0.001,
    ):
        """Initialize Autoencoder detector.

        Args:
            sequence_length: Length of price sequences
            encoding_dim: Dimension of encoded representation
            learning_rate: Learning rate for training
        """
        if tf is None:
            raise ImportError("TensorFlow is required for AutoencoderDetector")

        self.sequence_length = sequence_length
        self.encoding_dim = encoding_dim
        self.learning_rate = learning_rate

        self.model = None
        self.scaler = StandardScaler()
        self.feature_columns = None
        self.threshold = None

        logger.info(
            "autoencoder_initialized",
            sequence_length=sequence_length,
            encoding_dim=encoding_dim,
        )

    def _build_model(self, n_features: int) -> keras.Model:
        """Build autoencoder architecture.

        Args:
            n_features: Number of input features

        Returns:
            Compiled Keras model
        """
        # Encoder
        encoder_input = layers.Input(shape=(self.sequence_length, n_features))
        x = layers.LSTM(32, return_sequences=True)(encoder_input)
        x = layers.LSTM(16, return_sequences=False)(x)
        encoded = layers.Dense(self.encoding_dim, activation="relu")(x)

        # Decoder
        x = layers.RepeatVector(self.sequence_length)(encoded)
        x = layers.LSTM(16, return_sequences=True)(x)
        x = layers.LSTM(32, return_sequences=True)(x)
        decoded = layers.TimeDistributed(layers.Dense(n_features))(x)

        # Autoencoder model
        model = keras.Model(encoder_input, decoded, name="autoencoder")

        model.compile(
            optimizer=keras.optimizers.Adam(learning_rate=self.learning_rate),
            loss="mse",
        )

        logger.info("autoencoder_model_built", n_features=n_features)
        return model

    def _prepare_sequences(self, data: np.ndarray) -> np.ndarray:
        """Prepare sequences for training/prediction.

        Args:
            data: Scaled data array

        Returns:
            Array of sequences
        """
        X = []
        for i in range(self.sequence_length, len(data) + 1):
            X.append(data[i - self.sequence_length : i])
        return np.array(X)

    def fit(
        self,
        df: pd.DataFrame,
        feature_columns: list[str] | None = None,
        epochs: int = 50,
        batch_size: int = 32,
        validation_split: float = 0.2,
        experiment_name: str = "anomaly-detection-price-patterns",
    ) -> dict[str, Any]:
        """Train autoencoder on normal data.

        Args:
            df: DataFrame with normal price data
            feature_columns: List of feature columns
            epochs: Number of training epochs
            batch_size: Training batch size
            validation_split: Fraction for validation
            experiment_name: MLflow experiment name

        Returns:
            Training history dictionary
        """
        if feature_columns is None:
            feature_columns = ["open", "high", "low", "close"]

        self.feature_columns = feature_columns

        # Extract features
        data = df[feature_columns].values

        # Scale data
        scaled_data = self.scaler.fit_transform(data)

        # Prepare sequences
        X = self._prepare_sequences(scaled_data)

        logger.info(
            "autoencoder_training_data_prepared",
            samples=len(X),
            sequence_length=X.shape[1],
            features=X.shape[2],
        )

        # Build model
        self.model = self._build_model(X.shape[2])

        # Track with MLflow
        tracker = MLflowTracker(experiment_name=experiment_name)

        with tracker.start_run(run_name=f"autoencoder-train-{pd.Timestamp.now().isoformat()}"):
            # Log hyperparameters
            tracker.log_params(
                {
                    "sequence_length": self.sequence_length,
                    "encoding_dim": self.encoding_dim,
                    "learning_rate": self.learning_rate,
                    "epochs": epochs,
                    "batch_size": batch_size,
                    "n_features": len(feature_columns),
                }
            )

            # Train model (autoencoder reconstructs input)
            history = self.model.fit(
                X,
                X,
                epochs=epochs,
                batch_size=batch_size,
                validation_split=validation_split,
                verbose=1,
            )

            # Calculate reconstruction errors on training data
            reconstructions = self.model.predict(X, verbose=0)
            train_errors = np.mean(np.abs(X - reconstructions), axis=(1, 2))

            # Set threshold at 95th percentile
            self.threshold = np.percentile(train_errors, 95)

            # Log metrics
            final_loss = history.history["loss"][-1]
            final_val_loss = history.history["val_loss"][-1]

            tracker.log_metrics(
                {
                    "final_loss": final_loss,
                    "final_val_loss": final_val_loss,
                    "threshold": self.threshold,
                    "mean_reconstruction_error": train_errors.mean(),
                }
            )

            logger.info(
                "autoencoder_trained",
                final_loss=final_loss,
                threshold=self.threshold,
            )

        return history.history

    def predict(self, df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        """Detect anomalies in price patterns.

        Args:
            df: DataFrame with price data

        Returns:
            Tuple of (predictions, reconstruction_errors)
            predictions: 1 for anomalies, 0 for normal
            reconstruction_errors: Error for each sequence
        """
        if self.model is None:
            raise ValueError("Model must be trained before prediction")

        # Extract and scale features
        data = df[self.feature_columns].values
        scaled_data = self.scaler.transform(data)

        # Prepare sequences
        X = self._prepare_sequences(scaled_data)

        # Reconstruct
        reconstructions = self.model.predict(X, verbose=0)

        # Calculate reconstruction errors
        errors = np.mean(np.abs(X - reconstructions), axis=(1, 2))

        # Flag anomalies based on threshold
        predictions = (errors > self.threshold).astype(int)

        return predictions, errors

    def save(self, path: Path) -> None:
        """Save model to disk."""
        import joblib

        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)

        self.model.save(path / "autoencoder_model.keras")
        joblib.dump(self.scaler, path / "scaler.pkl")
        joblib.dump(self.feature_columns, path / "features.pkl")
        joblib.dump(self.threshold, path / "threshold.pkl")

        logger.info("autoencoder_saved", path=str(path))

    def load(self, path: Path) -> None:
        """Load model from disk."""
        import joblib

        path = Path(path)

        self.model = keras.models.load_model(path / "autoencoder_model.keras")
        self.scaler = joblib.load(path / "scaler.pkl")
        self.feature_columns = joblib.load(path / "features.pkl")
        self.threshold = joblib.load(path / "threshold.pkl")

        logger.info("autoencoder_loaded", path=str(path))

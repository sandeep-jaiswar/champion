"""Real-time prediction serving."""

from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import structlog

logger = structlog.get_logger()


class PredictionServer:
    """Serve real-time predictions from trained models.

    This class loads models and provides prediction endpoints.
    """

    def __init__(self, model_registry_path: Path):
        """Initialize prediction server.

        Args:
            model_registry_path: Path to model registry
        """
        self.model_registry_path = Path(model_registry_path)
        self.loaded_models = {}

        logger.info(
            "prediction_server_initialized",
            registry_path=str(model_registry_path),
        )

    def load_model(
        self,
        model_name: str,
        model_version: str = "latest",
        model_type: str = "lstm",
    ) -> Any:
        """Load a model from the registry.

        Args:
            model_name: Name of the model
            model_version: Version to load (default: "latest")
            model_type: Type of model ("lstm", "isolation_forest", "autoencoder")

        Returns:
            Loaded model instance
        """
        model_key = f"{model_name}_{model_version}"

        if model_key in self.loaded_models:
            logger.info("model_already_loaded", model_name=model_name, version=model_version)
            return self.loaded_models[model_key]

        model_path = self.model_registry_path / model_name / model_version

        if not model_path.exists():
            raise FileNotFoundError(f"Model not found: {model_path}")

        # Load appropriate model type
        if model_type == "lstm":
            from champion.ml.models.lstm_predictor import LSTMPricePredictor

            model = LSTMPricePredictor()
            model.load(model_path)
        elif model_type == "isolation_forest":
            from champion.ml.models.anomaly_detector import IsolationForestDetector

            model = IsolationForestDetector()
            model.load(model_path)
        elif model_type == "autoencoder":
            from champion.ml.models.anomaly_detector import AutoencoderDetector

            model = AutoencoderDetector()
            model.load(model_path)
        else:
            raise ValueError(f"Unknown model type: {model_type}")

        self.loaded_models[model_key] = model

        logger.info(
            "model_loaded",
            model_name=model_name,
            version=model_version,
            type=model_type,
        )

        return model

    def predict_price(
        self,
        model_name: str,
        df: pd.DataFrame,
        steps: int = 1,
        model_version: str = "latest",
    ) -> dict[str, Any]:
        """Get price predictions.

        Args:
            model_name: Name of the model
            df: Recent data for prediction
            steps: Number of steps ahead to predict
            model_version: Model version

        Returns:
            Dictionary with predictions and metadata
        """
        model = self.load_model(model_name, model_version, model_type="lstm")

        try:
            predictions = model.predict(df, steps=steps)

            result = {
                "model_name": model_name,
                "model_version": model_version,
                "prediction_time": datetime.now().isoformat(),
                "predictions": predictions.tolist(),
                "steps": steps,
                "status": "success",
            }

            logger.info(
                "price_prediction_served",
                model_name=model_name,
                steps=steps,
            )

            return result

        except Exception as e:
            logger.error(
                "prediction_failed",
                model_name=model_name,
                error=str(e),
            )
            return {
                "model_name": model_name,
                "status": "error",
                "error": str(e),
            }

    def detect_anomalies(
        self,
        model_name: str,
        df: pd.DataFrame,
        model_version: str = "latest",
        model_type: str = "isolation_forest",
    ) -> dict[str, Any]:
        """Detect anomalies in data.

        Args:
            model_name: Name of the model
            df: Data to check for anomalies
            model_version: Model version
            model_type: Type of anomaly detector

        Returns:
            Dictionary with anomaly detection results
        """
        model = self.load_model(model_name, model_version, model_type=model_type)

        try:
            predictions, scores = model.predict(df)

            # Find anomalies
            if model_type == "isolation_forest":
                anomaly_indices = (predictions == -1).nonzero()[0]
            else:  # autoencoder
                anomaly_indices = (predictions == 1).nonzero()[0]

            result = {
                "model_name": model_name,
                "model_version": model_version,
                "detection_time": datetime.now().isoformat(),
                "n_samples": len(df),
                "n_anomalies": len(anomaly_indices),
                "anomaly_rate": len(anomaly_indices) / len(df),
                "anomaly_indices": anomaly_indices.tolist(),
                "anomaly_scores": (
                    scores[anomaly_indices].tolist() if len(anomaly_indices) > 0 else []
                ),
                "status": "success",
            }

            logger.info(
                "anomaly_detection_served",
                model_name=model_name,
                n_anomalies=len(anomaly_indices),
            )

            return result

        except Exception as e:
            logger.error(
                "anomaly_detection_failed",
                model_name=model_name,
                error=str(e),
            )
            return {
                "model_name": model_name,
                "status": "error",
                "error": str(e),
            }

    def get_model_info(self, model_name: str, model_version: str = "latest") -> dict[str, Any]:
        """Get information about a model.

        Args:
            model_name: Name of the model
            model_version: Model version

        Returns:
            Dictionary with model metadata
        """
        model_path = self.model_registry_path / model_name / model_version

        if not model_path.exists():
            return {
                "model_name": model_name,
                "model_version": model_version,
                "status": "not_found",
            }

        # Check what files exist
        files = list(model_path.glob("*"))

        return {
            "model_name": model_name,
            "model_version": model_version,
            "model_path": str(model_path),
            "files": [f.name for f in files],
            "status": "available",
        }

    def list_models(self) -> list[dict[str, Any]]:
        """List all available models.

        Returns:
            List of model information dictionaries
        """
        if not self.model_registry_path.exists():
            return []

        models = []

        for model_dir in self.model_registry_path.iterdir():
            if model_dir.is_dir():
                for version_dir in model_dir.iterdir():
                    if version_dir.is_dir():
                        models.append(
                            {
                                "model_name": model_dir.name,
                                "version": version_dir.name,
                                "path": str(version_dir),
                            }
                        )

        return models

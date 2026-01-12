"""MLflow tracking abstraction for experiment logging.

This module provides a clean abstraction over MLflow for logging
parameters, metrics, and artifacts across Prefect flows and tasks.
"""

import os
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional

import mlflow
import structlog

logger = structlog.get_logger()


class MLflowTracker:
    """MLflow tracking wrapper for consistent experiment logging.
    
    This class provides:
    - Automatic MLflow tracking URI configuration
    - Experiment management with automatic creation
    - Context manager for run lifecycle
    - Convenience methods for logging params, metrics, and artifacts
    - Error handling and logging
    
    Example:
        >>> tracker = MLflowTracker(experiment_name="nse-bhavcopy-etl")
        >>> with tracker.start_run(run_name="bhavcopy-2024-01-10"):
        ...     tracker.log_param("trade_date", "2024-01-10")
        ...     tracker.log_metric("rows_processed", 1500)
    """

    def __init__(
        self,
        experiment_name: str = "champion-pipeline",
        tracking_uri: Optional[str] = None,
    ):
        """Initialize MLflow tracker.
        
        Args:
            experiment_name: Name of the MLflow experiment
            tracking_uri: MLflow tracking server URI (defaults to MLFLOW_TRACKING_URI env var)
        """
        self.experiment_name = experiment_name
        
        # Set tracking URI from parameter or environment
        if tracking_uri:
            mlflow.set_tracking_uri(tracking_uri)
        else:
            # Use environment variable or default to localhost
            default_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
            mlflow.set_tracking_uri(default_uri)
        
        self._tracking_uri = mlflow.get_tracking_uri()
        
        # Set or create experiment
        try:
            self.experiment = mlflow.set_experiment(experiment_name)
            logger.info(
                "mlflow_experiment_set",
                experiment_name=experiment_name,
                experiment_id=self.experiment.experiment_id,
                tracking_uri=self._tracking_uri,
            )
        except Exception as e:
            logger.error(
                "mlflow_experiment_setup_failed",
                experiment_name=experiment_name,
                error=str(e),
            )
            raise

    @contextmanager
    def start_run(
        self,
        run_name: Optional[str] = None,
        tags: Optional[Dict[str, Any]] = None,
        nested: bool = False,
    ):
        """Start an MLflow run with context manager.
        
        Args:
            run_name: Name for this run
            tags: Optional dictionary of tags to set on the run
            nested: Whether this is a nested run
            
        Yields:
            Active MLflow run
            
        Example:
            >>> with tracker.start_run(run_name="my-run"):
            ...     tracker.log_metric("accuracy", 0.95)
        """
        try:
            with mlflow.start_run(run_name=run_name, nested=nested) as run:
                logger.info(
                    "mlflow_run_started",
                    run_id=run.info.run_id,
                    run_name=run_name,
                    experiment_id=run.info.experiment_id,
                )
                
                # Set tags if provided
                if tags:
                    for key, value in tags.items():
                        mlflow.set_tag(key, value)
                
                yield run
                
                logger.info(
                    "mlflow_run_completed",
                    run_id=run.info.run_id,
                    run_name=run_name,
                )
        except Exception as e:
            logger.error(
                "mlflow_run_failed",
                run_name=run_name,
                error=str(e),
            )
            raise

    def log_param(self, key: str, value: Any) -> None:
        """Log a single parameter.
        
        Args:
            key: Parameter name
            value: Parameter value
        """
        try:
            mlflow.log_param(key, value)
            logger.debug("mlflow_param_logged", key=key, value=value)
        except Exception as e:
            logger.warning(
                "mlflow_param_log_failed",
                key=key,
                value=value,
                error=str(e),
            )

    def log_params(self, params: Dict[str, Any]) -> None:
        """Log multiple parameters at once.
        
        Args:
            params: Dictionary of parameter name-value pairs
        """
        try:
            mlflow.log_params(params)
            logger.debug("mlflow_params_logged", count=len(params))
        except Exception as e:
            logger.warning(
                "mlflow_params_log_failed",
                count=len(params),
                error=str(e),
            )

    def log_metric(self, key: str, value: float, step: Optional[int] = None) -> None:
        """Log a single metric.
        
        Args:
            key: Metric name
            value: Metric value
            step: Optional step number for time-series metrics
        """
        try:
            mlflow.log_metric(key, value, step=step)
            logger.debug("mlflow_metric_logged", key=key, value=value, step=step)
        except Exception as e:
            logger.warning(
                "mlflow_metric_log_failed",
                key=key,
                value=value,
                error=str(e),
            )

    def log_metrics(
        self,
        metrics: Dict[str, float],
        step: Optional[int] = None,
    ) -> None:
        """Log multiple metrics at once.
        
        Args:
            metrics: Dictionary of metric name-value pairs
            step: Optional step number for time-series metrics
        """
        try:
            mlflow.log_metrics(metrics, step=step)
            logger.debug("mlflow_metrics_logged", count=len(metrics), step=step)
        except Exception as e:
            logger.warning(
                "mlflow_metrics_log_failed",
                count=len(metrics),
                error=str(e),
            )

    def log_artifact(self, local_path: str | Path, artifact_path: Optional[str] = None) -> None:
        """Log a local file or directory as an artifact.
        
        Args:
            local_path: Path to local file or directory
            artifact_path: Optional subdirectory within artifact store
        """
        try:
            mlflow.log_artifact(str(local_path), artifact_path)
            logger.info(
                "mlflow_artifact_logged",
                local_path=str(local_path),
                artifact_path=artifact_path,
            )
        except Exception as e:
            logger.warning(
                "mlflow_artifact_log_failed",
                local_path=str(local_path),
                error=str(e),
            )

    def log_artifacts(self, local_dir: str | Path, artifact_path: Optional[str] = None) -> None:
        """Log all files in a directory as artifacts.
        
        Args:
            local_dir: Path to local directory
            artifact_path: Optional subdirectory within artifact store
        """
        try:
            mlflow.log_artifacts(str(local_dir), artifact_path)
            logger.info(
                "mlflow_artifacts_logged",
                local_dir=str(local_dir),
                artifact_path=artifact_path,
            )
        except Exception as e:
            logger.warning(
                "mlflow_artifacts_log_failed",
                local_dir=str(local_dir),
                error=str(e),
            )

    def set_tag(self, key: str, value: Any) -> None:
        """Set a tag on the current run.
        
        Args:
            key: Tag name
            value: Tag value
        """
        try:
            mlflow.set_tag(key, value)
            logger.debug("mlflow_tag_set", key=key, value=value)
        except Exception as e:
            logger.warning(
                "mlflow_tag_set_failed",
                key=key,
                value=value,
                error=str(e),
            )

    def set_tags(self, tags: Dict[str, Any]) -> None:
        """Set multiple tags on the current run.
        
        Args:
            tags: Dictionary of tag name-value pairs
        """
        try:
            mlflow.set_tags(tags)
            logger.debug("mlflow_tags_set", count=len(tags))
        except Exception as e:
            logger.warning(
                "mlflow_tags_set_failed",
                count=len(tags),
                error=str(e),
            )

    def log_execution_metadata(
        self,
        trade_date: Optional[date] = None,
        row_count: Optional[int] = None,
        duration_seconds: Optional[float] = None,
        partition_info: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log common execution metadata for data pipeline tasks.
        
        This is a convenience method for logging typical pipeline metrics:
        - Trade/partition date
        - Row counts processed
        - Execution duration
        - Partition information
        
        Args:
            trade_date: Trading/partition date
            row_count: Number of rows processed
            duration_seconds: Task execution duration in seconds
            partition_info: Additional partition metadata (year, month, day, etc.)
        """
        # Log parameters
        if trade_date is not None:
            self.log_param("trade_date", str(trade_date))
        
        if partition_info:
            self.log_params({f"partition_{k}": v for k, v in partition_info.items()})
        
        # Log metrics
        if row_count is not None:
            self.log_metric("row_count", float(row_count))
        
        if duration_seconds is not None:
            self.log_metric("duration_seconds", duration_seconds)
        
        logger.debug(
            "mlflow_execution_metadata_logged",
            trade_date=str(trade_date) if trade_date else None,
            row_count=row_count,
            duration_seconds=duration_seconds,
        )

    @property
    def tracking_uri(self) -> str:
        """Get the configured MLflow tracking URI.
        
        Returns:
            MLflow tracking URI
        """
        return self._tracking_uri

    @property
    def active_run(self):
        """Get the currently active MLflow run.
        
        Returns:
            Active run object or None
        """
        return mlflow.active_run()

"""Alert generation system for anomaly detection."""

from datetime import datetime
from enum import Enum
from typing import Any

import pandas as pd
import structlog
from pydantic import BaseModel

logger = structlog.get_logger()


class AlertSeverity(str, Enum):
    """Alert severity levels."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AlertType(str, Enum):
    """Types of alerts."""

    VOLUME_ANOMALY = "volume_anomaly"
    PRICE_PATTERN_ANOMALY = "price_pattern_anomaly"
    LARGE_PRICE_MOVEMENT = "large_price_movement"
    UNUSUAL_SPREAD = "unusual_spread"


class Alert(BaseModel):
    """Alert model."""

    alert_id: str
    alert_type: AlertType
    severity: AlertSeverity
    symbol: str
    timestamp: datetime
    message: str
    data: dict[str, Any]
    recommendations: list[str] = []


class AlertGenerator:
    """Generate alerts based on anomaly detection results.

    This class processes anomaly detection outputs and generates
    actionable alerts with appropriate severity levels.
    """

    def __init__(
        self,
        volume_threshold: float = 2.0,
        price_movement_threshold: float = 0.05,
    ):
        """Initialize alert generator.

        Args:
            volume_threshold: Standard deviations for volume anomaly
            price_movement_threshold: Threshold for large price movements (5% default)
        """
        self.volume_threshold = volume_threshold
        self.price_movement_threshold = price_movement_threshold
        self.alerts = []

        logger.info(
            "alert_generator_initialized",
            volume_threshold=volume_threshold,
            price_movement_threshold=price_movement_threshold,
        )

    def generate_volume_alerts(
        self,
        df: pd.DataFrame,
        anomaly_predictions: list[int],
        anomaly_scores: list[float],
    ) -> list[Alert]:
        """Generate alerts for volume anomalies.

        Args:
            df: DataFrame with volume data
            anomaly_predictions: Binary predictions (-1 for anomaly)
            anomaly_scores: Anomaly scores

        Returns:
            List of Alert objects
        """
        alerts = []

        for idx, (pred, score) in enumerate(zip(anomaly_predictions, anomaly_scores, strict=False)):
            if pred == -1:  # Anomaly detected
                row = df.iloc[idx]

                # Determine severity based on score
                if score < -0.5:
                    severity = AlertSeverity.CRITICAL
                elif score < -0.3:
                    severity = AlertSeverity.HIGH
                elif score < -0.1:
                    severity = AlertSeverity.MEDIUM
                else:
                    severity = AlertSeverity.LOW

                # Calculate volume metrics
                avg_volume = df["volume"].mean()
                current_volume = row["volume"]
                volume_ratio = current_volume / avg_volume

                alert = Alert(
                    alert_id=f"vol_{row['symbol']}_{row['trade_date'].strftime('%Y%m%d')}",
                    alert_type=AlertType.VOLUME_ANOMALY,
                    severity=severity,
                    symbol=row["symbol"],
                    timestamp=datetime.now(),
                    message=f"Unusual volume detected for {row['symbol']}: {volume_ratio:.2f}x average",
                    data={
                        "trade_date": str(row["trade_date"]),
                        "volume": int(current_volume),
                        "avg_volume": int(avg_volume),
                        "volume_ratio": float(volume_ratio),
                        "anomaly_score": float(score),
                    },
                    recommendations=[
                        "Check for news or announcements",
                        "Review order book depth",
                        "Monitor price action closely",
                    ],
                )

                alerts.append(alert)

                logger.info(
                    "volume_alert_generated",
                    symbol=row["symbol"],
                    severity=severity,
                    volume_ratio=volume_ratio,
                )

        self.alerts.extend(alerts)
        return alerts

    def generate_price_pattern_alerts(
        self,
        df: pd.DataFrame,
        anomaly_predictions: list[int],
        reconstruction_errors: list[float],
    ) -> list[Alert]:
        """Generate alerts for price pattern anomalies.

        Args:
            df: DataFrame with price data
            anomaly_predictions: Binary predictions (1 for anomaly)
            reconstruction_errors: Reconstruction errors from autoencoder

        Returns:
            List of Alert objects
        """
        alerts = []

        for idx, (pred, error) in enumerate(
            zip(anomaly_predictions, reconstruction_errors, strict=False)
        ):
            if pred == 1:  # Anomaly detected
                row = df.iloc[idx]

                # Determine severity based on error magnitude
                mean_error = sum(reconstruction_errors) / len(reconstruction_errors)
                error_ratio = error / mean_error

                if error_ratio > 3.0:
                    severity = AlertSeverity.CRITICAL
                elif error_ratio > 2.0:
                    severity = AlertSeverity.HIGH
                elif error_ratio > 1.5:
                    severity = AlertSeverity.MEDIUM
                else:
                    severity = AlertSeverity.LOW

                # Calculate price metrics
                price_change = (row["close"] - row["open"]) / row["open"]

                alert = Alert(
                    alert_id=f"pat_{row['symbol']}_{row['trade_date'].strftime('%Y%m%d')}",
                    alert_type=AlertType.PRICE_PATTERN_ANOMALY,
                    severity=severity,
                    symbol=row["symbol"],
                    timestamp=datetime.now(),
                    message=f"Unusual price pattern for {row['symbol']}: {error_ratio:.2f}x normal",
                    data={
                        "trade_date": str(row["trade_date"]),
                        "open": float(row["open"]),
                        "high": float(row["high"]),
                        "low": float(row["low"]),
                        "close": float(row["close"]),
                        "price_change_pct": float(price_change * 100),
                        "reconstruction_error": float(error),
                        "error_ratio": float(error_ratio),
                    },
                    recommendations=[
                        "Investigate potential market manipulation",
                        "Check for circuit breakers",
                        "Review historical similar patterns",
                    ],
                )

                alerts.append(alert)

                logger.info(
                    "price_pattern_alert_generated",
                    symbol=row["symbol"],
                    severity=severity,
                    error_ratio=error_ratio,
                )

        self.alerts.extend(alerts)
        return alerts

    def generate_price_movement_alerts(
        self,
        df: pd.DataFrame,
    ) -> list[Alert]:
        """Generate alerts for large price movements.

        Args:
            df: DataFrame with price data

        Returns:
            List of Alert objects
        """
        alerts = []

        # Calculate price changes on a copy to avoid mutating input
        df = df.copy()
        df["price_change"] = (df["close"] - df["open"]) / df["open"]

        # Find large movements
        large_moves = df[df["price_change"].abs() > self.price_movement_threshold]

        for _, row in large_moves.iterrows():
            price_change_pct = row["price_change"] * 100

            # Determine severity
            if abs(price_change_pct) > 10:
                severity = AlertSeverity.CRITICAL
            elif abs(price_change_pct) > 7:
                severity = AlertSeverity.HIGH
            else:
                severity = AlertSeverity.MEDIUM

            direction = "gain" if price_change_pct > 0 else "loss"

            alert = Alert(
                alert_id=f"move_{row['symbol']}_{row['trade_date'].strftime('%Y%m%d')}",
                alert_type=AlertType.LARGE_PRICE_MOVEMENT,
                severity=severity,
                symbol=row["symbol"],
                timestamp=datetime.now(),
                message=f"Large price {direction} for {row['symbol']}: {abs(price_change_pct):.2f}%",
                data={
                    "trade_date": str(row["trade_date"]),
                    "open": float(row["open"]),
                    "close": float(row["close"]),
                    "price_change_pct": float(price_change_pct),
                    "volume": int(row["volume"]) if "volume" in row else None,
                },
                recommendations=[
                    "Check for corporate actions",
                    "Review sector performance",
                    "Assess market-wide volatility",
                ],
            )

            alerts.append(alert)

            logger.info(
                "price_movement_alert_generated",
                symbol=row["symbol"],
                severity=severity,
                change_pct=price_change_pct,
            )

        self.alerts.extend(alerts)
        return alerts

    def get_alerts(
        self,
        severity: AlertSeverity | None = None,
        alert_type: AlertType | None = None,
        symbol: str | None = None,
    ) -> list[Alert]:
        """Filter and retrieve alerts.

        Args:
            severity: Filter by severity level
            alert_type: Filter by alert type
            symbol: Filter by symbol

        Returns:
            Filtered list of alerts
        """
        filtered = self.alerts

        if severity:
            filtered = [a for a in filtered if a.severity == severity]

        if alert_type:
            filtered = [a for a in filtered if a.alert_type == alert_type]

        if symbol:
            filtered = [a for a in filtered if a.symbol == symbol]

        return filtered

    def clear_alerts(self) -> None:
        """Clear all stored alerts."""
        self.alerts = []
        logger.info("alerts_cleared")

    def export_alerts(self) -> pd.DataFrame:
        """Export alerts to DataFrame.

        Returns:
            DataFrame with alert data
        """
        if not self.alerts:
            return pd.DataFrame()

        return pd.DataFrame([alert.model_dump() for alert in self.alerts])

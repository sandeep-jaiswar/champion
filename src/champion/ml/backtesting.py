"""Backtesting framework for ML models."""

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import structlog

from champion.ml.tracking import MLflowTracker

logger = structlog.get_logger()


class Backtester:
    """Backtesting framework for price prediction models.

    This class provides walk-forward backtesting to evaluate model
    performance on historical data.
    """

    def __init__(
        self,
        initial_capital: float = 100000.0,
        transaction_cost: float = 0.001,
        slippage: float = 0.0005,
    ):
        """Initialize backtester.

        Args:
            initial_capital: Starting capital for backtesting
            transaction_cost: Transaction cost as fraction (0.001 = 0.1%)
            slippage: Slippage as fraction
        """
        self.initial_capital = initial_capital
        self.transaction_cost = transaction_cost
        self.slippage = slippage

        self.trades = []
        self.portfolio_values = []

        logger.info(
            "backtester_initialized",
            initial_capital=initial_capital,
            transaction_cost=transaction_cost,
        )

    def run_backtest(
        self,
        df: pd.DataFrame,
        model: Any,
        prediction_column: str = "predicted_price",
        actual_column: str = "close",
        train_window: int = 252,
        test_window: int = 20,
        retrain_frequency: int = 20,
        experiment_name: str = "backtesting",
    ) -> dict[str, Any]:
        """Run walk-forward backtest.

        Args:
            df: DataFrame with historical data (must be sorted by date)
            model: Model with train() and predict() methods
            prediction_column: Name for prediction column
            actual_column: Name of actual price column
            train_window: Number of periods for initial training
            test_window: Number of periods to test before retraining
            retrain_frequency: How often to retrain (in periods)
            experiment_name: MLflow experiment name

        Returns:
            Dictionary with backtest results
        """
        # Reset state at the start of each run
        self.trades = []
        self.portfolio_values = []

        df = df.sort_values("trade_date").reset_index(drop=True)

        portfolio_value = self.initial_capital
        position = 0  # Number of shares held
        cash = self.initial_capital

        predictions = []
        actuals = []
        dates = []

        current_idx = train_window

        # Track with MLflow
        tracker = MLflowTracker(experiment_name=experiment_name)

        with tracker.start_run(run_name=f"backtest-{pd.Timestamp.now().isoformat()}"):
            tracker.log_params(
                {
                    "initial_capital": self.initial_capital,
                    "transaction_cost": self.transaction_cost,
                    "train_window": train_window,
                    "test_window": test_window,
                    "retrain_frequency": retrain_frequency,
                }
            )

            periods_since_retrain = 0

            while current_idx < len(df) - 1:
                # Retrain model if needed
                if periods_since_retrain == 0:
                    train_df = df.iloc[max(0, current_idx - train_window) : current_idx]

                    logger.info(
                        "retraining_model",
                        train_start=current_idx - train_window,
                        train_end=current_idx,
                    )

                    try:
                        model.train(train_df, epochs=20, verbose=0)
                    except Exception as e:
                        logger.error("model_training_failed", error=str(e))
                        break

                # Predict next period
                pred_df = df.iloc[max(0, current_idx - model.sequence_length) : current_idx]

                try:
                    pred = model.predict(pred_df, steps=1)[0]
                except Exception as e:
                    logger.error("prediction_failed", error=str(e))
                    current_idx += 1
                    continue

                # Get actual price
                actual = df.iloc[current_idx][actual_column]
                date = df.iloc[current_idx]["trade_date"]

                predictions.append(pred)
                actuals.append(actual)
                dates.append(date)

                # Simple trading strategy: buy if predicted > current, sell if predicted < current
                current_price = df.iloc[current_idx - 1][actual_column]

                if pred > current_price * 1.01 and position == 0:
                    # Buy signal
                    shares_to_buy = int(cash / (current_price * (1 + self.transaction_cost)))
                    cost = (
                        shares_to_buy * current_price * (1 + self.transaction_cost + self.slippage)
                    )

                    if cost <= cash:
                        position += shares_to_buy
                        cash -= cost

                        self.trades.append(
                            {
                                "date": date,
                                "action": "BUY",
                                "shares": shares_to_buy,
                                "price": current_price,
                                "cost": cost,
                            }
                        )

                elif pred < current_price * 0.99 and position > 0:
                    # Sell signal
                    revenue = position * current_price * (1 - self.transaction_cost - self.slippage)
                    cash += revenue
                    sold_shares = position
                    position = 0

                    self.trades.append(
                        {
                            "date": date,
                            "action": "SELL",
                            "shares": sold_shares,
                            "price": current_price,
                            "revenue": revenue,
                        }
                    )

                # Update portfolio value
                portfolio_value = cash + position * actual
                self.portfolio_values.append(
                    {
                        "date": date,
                        "portfolio_value": portfolio_value,
                        "position": position,
                        "cash": cash,
                    }
                )

                current_idx += 1
                periods_since_retrain += 1

                if periods_since_retrain >= retrain_frequency:
                    periods_since_retrain = 0

            # Calculate metrics
            final_value = portfolio_value
            total_return = (final_value - self.initial_capital) / self.initial_capital
            buy_and_hold_return = (
                df.iloc[-1][actual_column] - df.iloc[train_window][actual_column]
            ) / df.iloc[train_window][actual_column]

            # Calculate prediction accuracy
            pred_arr = np.array(predictions)
            actual_arr = np.array(actuals)

            mae = np.mean(np.abs(pred_arr - actual_arr))
            rmse = np.sqrt(np.mean((pred_arr - actual_arr) ** 2))
            mape = np.mean(np.abs((actual_arr - pred_arr) / actual_arr)) * 100

            # Directional accuracy
            pred_direction = np.diff(pred_arr) > 0
            actual_direction = np.diff(actual_arr) > 0
            directional_accuracy = np.mean(pred_direction == actual_direction)

            # Sharpe ratio (assuming daily returns)
            portfolio_df = pd.DataFrame(self.portfolio_values)
            returns = portfolio_df["portfolio_value"].pct_change().dropna()
            sharpe_ratio = returns.mean() / returns.std() * np.sqrt(252) if len(returns) > 0 else 0

            # Max drawdown
            cummax = portfolio_df["portfolio_value"].cummax()
            drawdown = (portfolio_df["portfolio_value"] - cummax) / cummax
            max_drawdown = drawdown.min()

            # Log metrics
            tracker.log_metrics(
                {
                    "final_portfolio_value": final_value,
                    "total_return": total_return,
                    "buy_and_hold_return": buy_and_hold_return,
                    "alpha": total_return - buy_and_hold_return,
                    "mae": mae,
                    "rmse": rmse,
                    "mape": mape,
                    "directional_accuracy": directional_accuracy,
                    "sharpe_ratio": sharpe_ratio,
                    "max_drawdown": max_drawdown,
                    "n_trades": len(self.trades),
                }
            )

            results = {
                "initial_capital": self.initial_capital,
                "final_value": float(final_value),
                "total_return": float(total_return),
                "buy_and_hold_return": float(buy_and_hold_return),
                "alpha": float(total_return - buy_and_hold_return),
                "mae": float(mae),
                "rmse": float(rmse),
                "mape": float(mape),
                "directional_accuracy": float(directional_accuracy),
                "sharpe_ratio": float(sharpe_ratio),
                "max_drawdown": float(max_drawdown),
                "n_trades": len(self.trades),
                "trades": self.trades,
                "portfolio_values": self.portfolio_values,
                "predictions": predictions,
                "actuals": actuals,
                "dates": dates,
            }

            logger.info(
                "backtest_completed",
                total_return=total_return,
                alpha=total_return - buy_and_hold_return,
                sharpe=sharpe_ratio,
            )

        return results

    def plot_results(self, results: dict[str, Any], output_path: Path | None = None) -> None:
        """Plot backtest results.

        Args:
            results: Results dictionary from run_backtest()
            output_path: Optional path to save plot
        """
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            logger.warning("matplotlib_not_available", message="Install matplotlib for plotting")
            return

        fig, axes = plt.subplots(3, 1, figsize=(12, 10))

        # Portfolio value over time
        portfolio_df = pd.DataFrame(results["portfolio_values"])
        axes[0].plot(portfolio_df["date"], portfolio_df["portfolio_value"])
        axes[0].axhline(y=self.initial_capital, color="r", linestyle="--", label="Initial Capital")
        axes[0].set_title("Portfolio Value Over Time")
        axes[0].set_ylabel("Value ($)")
        axes[0].legend()
        axes[0].grid(True)

        # Predictions vs Actuals
        axes[1].plot(results["dates"], results["actuals"], label="Actual", alpha=0.7)
        axes[1].plot(results["dates"], results["predictions"], label="Predicted", alpha=0.7)
        axes[1].set_title("Predictions vs Actuals")
        axes[1].set_ylabel("Price")
        axes[1].legend()
        axes[1].grid(True)

        # Returns distribution
        returns = portfolio_df["portfolio_value"].pct_change().dropna()
        axes[2].hist(returns, bins=50, alpha=0.7)
        axes[2].axvline(x=0, color="r", linestyle="--")
        axes[2].set_title("Returns Distribution")
        axes[2].set_xlabel("Daily Return")
        axes[2].set_ylabel("Frequency")
        axes[2].grid(True)

        plt.tight_layout()

        if output_path:
            plt.savefig(output_path)
            logger.info("plot_saved", path=str(output_path))
        else:
            plt.show()

        plt.close()

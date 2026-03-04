"""Portfolio optimization using Modern Portfolio Theory."""

from typing import Any

import numpy as np
import pandas as pd
import structlog
from scipy.optimize import minimize

from champion.ml.tracking import MLflowTracker

logger = structlog.get_logger()


class PortfolioOptimizer:
    """Modern Portfolio Theory (MPT) implementation for portfolio optimization.

    This optimizer finds optimal portfolio weights that maximize risk-adjusted
    returns (Sharpe ratio) subject to constraints.
    """

    def __init__(
        self,
        risk_free_rate: float = 0.05,
        max_position_size: float = 0.3,
        min_position_size: float = 0.0,
    ):
        """Initialize portfolio optimizer.

        Args:
            risk_free_rate: Annual risk-free rate for Sharpe ratio calculation
            max_position_size: Maximum weight for any single asset
            min_position_size: Minimum weight for any single asset
        """
        self.risk_free_rate = risk_free_rate
        self.max_position_size = max_position_size
        self.min_position_size = min_position_size

        self.optimal_weights = None
        self.expected_returns = None
        self.cov_matrix = None
        self.asset_names = None

        logger.info(
            "portfolio_optimizer_initialized",
            risk_free_rate=risk_free_rate,
            max_position_size=max_position_size,
        )

    def _calculate_portfolio_stats(
        self,
        weights: np.ndarray,
        expected_returns: np.ndarray,
        cov_matrix: np.ndarray,
    ) -> tuple[float, float, float]:
        """Calculate portfolio statistics.

        Args:
            weights: Portfolio weights
            expected_returns: Expected returns for each asset
            cov_matrix: Covariance matrix of returns

        Returns:
            Tuple of (return, volatility, sharpe_ratio)
        """
        portfolio_return = np.sum(weights * expected_returns)
        portfolio_volatility = np.sqrt(np.dot(weights.T, np.dot(cov_matrix, weights)))
        sharpe_ratio = (portfolio_return - self.risk_free_rate) / portfolio_volatility

        return portfolio_return, portfolio_volatility, sharpe_ratio

    def _negative_sharpe(
        self,
        weights: np.ndarray,
        expected_returns: np.ndarray,
        cov_matrix: np.ndarray,
    ) -> float:
        """Objective function: negative Sharpe ratio (for minimization).

        Args:
            weights: Portfolio weights
            expected_returns: Expected returns
            cov_matrix: Covariance matrix

        Returns:
            Negative Sharpe ratio
        """
        _, _, sharpe = self._calculate_portfolio_stats(weights, expected_returns, cov_matrix)
        return -sharpe

    def optimize(
        self,
        df: pd.DataFrame,
        price_column: str = "close",
        lookback_period: int = 252,
        sector_limits: dict[str, float] | None = None,
        experiment_name: str = "portfolio-optimization",
    ) -> dict[str, Any]:
        """Optimize portfolio weights.

        Args:
            df: DataFrame with price data (multiindex or pivot table format)
                Expected columns: date, symbol, close (or price_column)
            price_column: Name of the price column
            lookback_period: Number of periods for return calculation
            sector_limits: Optional sector exposure limits {sector: max_weight}
            experiment_name: MLflow experiment name

        Returns:
            Dictionary with optimization results
        """
        # Pivot data if needed
        if "symbol" in df.columns:
            price_df = df.pivot(index="trade_date", columns="symbol", values=price_column)
        else:
            price_df = df

        # Calculate returns
        returns = price_df.pct_change().dropna()

        # Use only recent data
        if len(returns) > lookback_period:
            returns = returns.tail(lookback_period)

        # Calculate expected returns and covariance
        self.expected_returns = returns.mean() * 252  # Annualize
        self.cov_matrix = returns.cov() * 252  # Annualize
        self.asset_names = list(price_df.columns)

        n_assets = len(self.asset_names)

        # Initial guess: equal weights
        initial_weights = np.ones(n_assets) / n_assets

        # Constraints
        constraints = [{"type": "eq", "fun": lambda x: np.sum(x) - 1}]  # Weights sum to 1

        # Bounds: position size limits
        bounds = tuple([(self.min_position_size, self.max_position_size) for _ in range(n_assets)])

        # Track with MLflow
        tracker = MLflowTracker(experiment_name=experiment_name)

        with tracker.start_run(run_name=f"portfolio-opt-{pd.Timestamp.now().isoformat()}"):
            # Log parameters
            tracker.log_params(
                {
                    "n_assets": n_assets,
                    "lookback_period": lookback_period,
                    "risk_free_rate": self.risk_free_rate,
                    "max_position_size": self.max_position_size,
                    "min_position_size": self.min_position_size,
                }
            )

            # Optimize
            result = minimize(
                self._negative_sharpe,
                initial_weights,
                args=(self.expected_returns.values, self.cov_matrix.values),
                method="SLSQP",
                bounds=bounds,
                constraints=constraints,
            )

            if not result.success:
                logger.warning("optimization_failed", message=result.message)
                raise ValueError(f"Optimization failed: {result.message}")

            self.optimal_weights = result.x

            # Calculate portfolio statistics
            portfolio_return, portfolio_vol, sharpe = self._calculate_portfolio_stats(
                self.optimal_weights,
                self.expected_returns.values,
                self.cov_matrix.values,
            )

            # Log metrics
            tracker.log_metrics(
                {
                    "portfolio_return": portfolio_return,
                    "portfolio_volatility": portfolio_vol,
                    "sharpe_ratio": sharpe,
                    "max_weight": self.optimal_weights.max(),
                    "min_weight": self.optimal_weights.min(),
                }
            )

            # Create results dictionary
            results = {
                "weights": dict(zip(self.asset_names, self.optimal_weights, strict=False)),
                "expected_return": float(portfolio_return),
                "volatility": float(portfolio_vol),
                "sharpe_ratio": float(sharpe),
                "optimization_success": result.success,
            }

            logger.info(
                "portfolio_optimized",
                portfolio_return=portfolio_return,
                volatility=portfolio_vol,
                sharpe=sharpe,
            )

        return results

    def optimize_with_sector_constraints(
        self,
        df: pd.DataFrame,
        sector_mapping: dict[str, str],
        sector_limits: dict[str, float],
        price_column: str = "close",
        lookback_period: int = 252,
        experiment_name: str = "portfolio-optimization-sectors",
    ) -> dict[str, Any]:
        """Optimize portfolio with sector exposure constraints.

        Args:
            df: DataFrame with price data
            sector_mapping: Dictionary mapping symbols to sectors
            sector_limits: Maximum exposure per sector {sector: max_weight}
            price_column: Name of price column
            lookback_period: Lookback period for returns
            experiment_name: MLflow experiment name

        Returns:
            Dictionary with optimization results including sector exposures
        """
        # Pivot data if needed
        if "symbol" in df.columns:
            price_df = df.pivot(index="trade_date", columns="symbol", values=price_column)
        else:
            price_df = df

        # Calculate returns
        returns = price_df.pct_change().dropna()

        if len(returns) > lookback_period:
            returns = returns.tail(lookback_period)

        # Calculate expected returns and covariance
        self.expected_returns = returns.mean() * 252
        self.cov_matrix = returns.cov() * 252
        self.asset_names = list(price_df.columns)

        n_assets = len(self.asset_names)
        initial_weights = np.ones(n_assets) / n_assets

        # Create sector constraint matrices
        sectors = sorted(set(sector_mapping.values()))
        sector_constraints = []

        for sector in sectors:
            sector_assets = [
                i for i, asset in enumerate(self.asset_names) if sector_mapping.get(asset) == sector
            ]
            if sector in sector_limits and sector_assets:
                # Constraint: sum of weights in sector <= sector_limit
                constraint_matrix = np.zeros(n_assets)
                constraint_matrix[sector_assets] = 1

                sector_constraints.append(
                    {
                        "type": "ineq",
                        "fun": lambda x, m=constraint_matrix, limit=sector_limits[sector]: limit
                        - np.dot(m, x),
                    }
                )

        # Add weight sum constraint
        constraints = [{"type": "eq", "fun": lambda x: np.sum(x) - 1}] + sector_constraints

        # Bounds
        bounds = tuple([(self.min_position_size, self.max_position_size) for _ in range(n_assets)])

        # Track with MLflow
        tracker = MLflowTracker(experiment_name=experiment_name)

        with tracker.start_run(run_name=f"portfolio-opt-sectors-{pd.Timestamp.now().isoformat()}"):
            tracker.log_params(
                {
                    "n_assets": n_assets,
                    "n_sectors": len(sectors),
                    "lookback_period": lookback_period,
                    "risk_free_rate": self.risk_free_rate,
                }
            )

            # Optimize
            result = minimize(
                self._negative_sharpe,
                initial_weights,
                args=(self.expected_returns.values, self.cov_matrix.values),
                method="SLSQP",
                bounds=bounds,
                constraints=constraints,
            )

            if not result.success:
                logger.warning(
                    "sector_optimization_failed",
                    message=result.message,
                )
                raise ValueError(f"Optimization failed: {result.message}")

            self.optimal_weights = result.x

            # Calculate portfolio statistics
            portfolio_return, portfolio_vol, sharpe = self._calculate_portfolio_stats(
                self.optimal_weights,
                self.expected_returns.values,
                self.cov_matrix.values,
            )

            # Calculate sector exposures
            sector_exposures = {}
            for sector in sectors:
                sector_assets = [
                    i
                    for i, asset in enumerate(self.asset_names)
                    if sector_mapping.get(asset) == sector
                ]
                sector_weight = sum(self.optimal_weights[i] for i in sector_assets)
                sector_exposures[sector] = float(sector_weight)

            # Log metrics
            tracker.log_metrics(
                {
                    "portfolio_return": portfolio_return,
                    "portfolio_volatility": portfolio_vol,
                    "sharpe_ratio": sharpe,
                }
            )

            # Log sector exposures
            for sector, weight in sector_exposures.items():
                tracker.log_metric(f"sector_exposure_{sector}", weight)

            results = {
                "weights": dict(zip(self.asset_names, self.optimal_weights, strict=False)),
                "expected_return": float(portfolio_return),
                "volatility": float(portfolio_vol),
                "sharpe_ratio": float(sharpe),
                "sector_exposures": sector_exposures,
                "optimization_success": result.success,
            }

            logger.info(
                "portfolio_optimized_with_sectors",
                portfolio_return=portfolio_return,
                sharpe=sharpe,
                sector_exposures=sector_exposures,
            )

        return results

    def get_efficient_frontier(
        self,
        n_points: int = 50,
    ) -> pd.DataFrame:
        """Calculate efficient frontier.

        Args:
            n_points: Number of points on the frontier

        Returns:
            DataFrame with frontier points (return, volatility, weights)
        """
        if self.expected_returns is None:
            raise ValueError("Run optimize() first")

        n_assets = len(self.asset_names)
        target_returns = np.linspace(
            self.expected_returns.min(),
            self.expected_returns.max(),
            n_points,
        )

        frontier_results = []

        for target_return in target_returns:
            # Constraint: target return
            constraints = [
                {"type": "eq", "fun": lambda x: np.sum(x) - 1},
                {
                    "type": "eq",
                    "fun": lambda x, tr=target_return: np.sum(x * self.expected_returns.values)
                    - tr,
                },
            ]

            bounds = tuple([(0, 1) for _ in range(n_assets)])

            # Minimize volatility for target return
            result = minimize(
                lambda x: np.sqrt(np.dot(x.T, np.dot(self.cov_matrix.values, x))),
                np.ones(n_assets) / n_assets,
                method="SLSQP",
                bounds=bounds,
                constraints=constraints,
            )

            if result.success:
                weights = result.x
                ret, vol, sharpe = self._calculate_portfolio_stats(
                    weights,
                    self.expected_returns.values,
                    self.cov_matrix.values,
                )
                frontier_results.append(
                    {
                        "return": ret,
                        "volatility": vol,
                        "sharpe_ratio": sharpe,
                        "weights": weights,
                    }
                )

        return pd.DataFrame(frontier_results)

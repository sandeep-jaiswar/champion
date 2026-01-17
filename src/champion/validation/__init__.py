"""Data quality validation layer.

Provides comprehensive data validation capabilities:
- JSON schema validation against Parquet files
- 15+ business logic validation rules
- Quarantine functionality for failed records with audit trail
- Custom validator registration
- Validation reporting and trend analysis
- Anomaly detection
- Prefect flow integration with alerts

All validators implement champion.core.Validator interface.

## Key Features

- Memory-efficient streaming validation for large datasets
- Schema versioning support
- Detailed error reporting with 15+ validation rules
- Custom validation rule support
- Integration with orchestration pipelines
- Audit trail for quarantined records
- Daily validation reports with trend analysis
"""

from .reporting import ValidationReport, ValidationReporter, ValidationTrend
from .validator import ParquetValidator, ValidationResult

__version__ = "2.0.0"

__all__ = [
    "ParquetValidator",
    "ValidationResult",
    "ValidationReporter",
    "ValidationReport",
    "ValidationTrend",
]

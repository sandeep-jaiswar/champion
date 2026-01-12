# Parsers

Domain scope: transform raw scraper outputs to typed data frames/events.

## Base Parser Class

All parsers inherit from the abstract `Parser` base class, which provides:

- **Common interface**: Standard `parse()` method signature
- **Schema versioning**: `SCHEMA_VERSION` attribute for tracking compatibility
- **Metadata helpers**: `add_metadata()` method for adding standard columns
- **Optional validation**: `validate_schema()` method (can be overridden)

### Usage Example

```python
from pathlib import Path
from champion.parsers.base_parser import Parser
import polars as pl

class MyCustomParser(Parser):
    """Custom parser implementation."""
    
    SCHEMA_VERSION = "v1.0"
    
    def parse(self, file_path: Path, *args, **kwargs) -> pl.DataFrame:
        """Parse file and return DataFrame."""
        # Your parsing logic here
        df = pl.read_csv(file_path)
        
        # Optionally add metadata
        df = self.add_metadata(df)
        
        return df
    
    def validate_schema(self, df: pl.DataFrame) -> None:
        """Optional: validate DataFrame schema."""
        required_cols = ["symbol", "price", "volume"]
        missing = set(required_cols) - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns: {missing}")
```

## Current Parsers

Current contents:

- Polars-based parsers for bhavcopy, bulk/block deals, corporate actions, index constituents, macro indicators, symbol master, trading calendar.
- Specialized fundamentals parsers (quarterly financials, shareholding) and enrichment utilities.

All parsers now inherit from the `Parser` base class and implement the common interface.

## Migration notes

- Extract any remaining parser logic from ingestion and validation packages here.
- Standardize schemas, validation, and date/time casting.
- Keep I/O-free pure parsing to ease testing.

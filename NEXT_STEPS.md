# Implementation Checklist: Next Steps

**Last Updated**: Now  
**For**: Sandeep (single user, local docker)  
**Current Status**: 70% of recommendations implemented

---

## 🎯 Immediate Next Steps (This Session)

### Step 1: Complete Memory Leak Fix ⏳ 30 minutes

**What**: Integrate ErrorStream into actual validator

**File**: `src/champion/validation/validator.py`

**Find**: `ParquetValidator.validate()` method (around line 250-300)

**Change**:

```python
def validate(self, df: DataFrame) -> ValidationResult:
    """Validate dataframe for quality issues."""
    
    # ADD THIS:
    LARGE_FILE_THRESHOLD = 50_000  # 50K rows = ~2.5MB
    if len(df) > LARGE_FILE_THRESHOLD:
        from pathlib import Path
        from .error_streaming import ErrorStream
        
        error_stream = ErrorStream(
            output_file=Path(f"/tmp/validation_errors_{uuid.uuid4()}.jsonl"),
            keep_samples=100
        )
        is_memory_efficient = True
    else:
        error_stream = None
        is_memory_efficient = False
        
    # THEN: Replace ALL places that do:
    # error_details.append(error_detail)
    # WITH:
    if error_stream:
        error_stream.write_error(error_detail)
    else:
        error_details.append(error_detail)
    
    # FINALLY: Return statement
    return ValidationResult(
        valid=len(error_details) == 0,
        error_details=error_stream.get_samples() if error_stream else error_details,
        error_file=str(error_stream.output_file) if error_stream else None,
        is_memory_efficient=is_memory_efficient,
        warnings=warnings,
        critical_failures=critical_failures
    )
```

**Test**:

```bash
poetry run pytest tests/test_validator.py -v
```

**Verify**:

- Small files (< 50K rows) use in-memory (backward compatible) ✅
- Large files stream to disk ✅
- ValidationResult.error_file is populated ✅

---

### Step 2: Add Tests for Error Streaming ⏳ 20 minutes

**File**: `tests/unit/test_error_streaming.py` (NEW)

**Create**:

```python
"""Tests for error streaming functionality."""

import tempfile
from pathlib import Path
import pytest

from champion.validation.error_streaming import ErrorStream


def test_error_stream_keeps_samples():
    """ErrorStream.get_samples() returns only N items."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        stream = ErrorStream(
            output_file=Path(tmp_dir) / "errors.jsonl",
            keep_samples=10
        )
        
        # Add 50 errors
        for i in range(50):
            stream.write_error({
                "row_id": i,
                "error": f"error_{i}",
                "field": "price"
            })
        
        # get_samples() should return only 10
        samples = stream.get_samples()
        assert len(samples) == 10
        assert samples[0]["row_id"] == 0
        assert samples[-1]["row_id"] == 9


def test_error_stream_writes_all_to_disk():
    """ErrorStream.iter_all_errors() returns ALL errors from disk."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        stream = ErrorStream(
            output_file=Path(tmp_dir) / "errors.jsonl",
            keep_samples=10
        )
        
        # Add 50 errors
        for i in range(50):
            stream.write_error({"row_id": i, "error": f"error_{i}"})
        
        # iter_all_errors() should read all 50
        all_errors = list(stream.iter_all_errors())
        assert len(all_errors) == 50
        assert all_errors[0]["row_id"] == 0
        assert all_errors[-1]["row_id"] == 49


def test_error_stream_memory_bounded():
    """ErrorStream keeps constant memory regardless of size."""
    import sys
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        stream = ErrorStream(
            output_file=Path(tmp_dir) / "errors.jsonl",
            keep_samples=100
        )
        
        # Get size before
        size_before = sys.getsizeof(stream.samples)
        
        # Add 10,000 errors
        for i in range(10_000):
            stream.write_error({"row_id": i, "data": "x" * 1000})
        
        # Size should not grow significantly
        size_after = sys.getsizeof(stream.samples)
        assert size_after < size_before * 2  # Allow small overhead
        assert len(stream.samples) == 100  # Still only 100 samples
```

**Run**:

```bash
poetry run pytest tests/unit/test_error_streaming.py -v
```

---

### Step 3: Fix Remaining Exception Handling ⏳ 45 minutes

**File**: `src/champion/cli/cli.py`

**Current**: ~25 `except Exception` patterns

**Find all occurrences**:

```bash
grep -n "except Exception" src/champion/cli/cli.py
```

**Pattern to Apply**:

```python
# BEFORE:
try:
    result = some_operation()
except Exception as e:
    logger.error(f"Failed: {e}")
    sys.exit(1)

# AFTER:
try:
    result = some_operation()
except (FileNotFoundError, ValueError, KeyError) as e:
    logger.error(f"Invalid input: {e}")
    sys.exit(1)
except Exception as e:
    logger.exception(f"Unexpected error: {e}")
    sys.exit(1)
```

**Specific CLI Locations to Update**:

1. `validate_command()` - Catch `ValueError` for validation errors
2. `load_command()` - Catch `FileNotFoundError`, `ConnectionError`
3. `parse_command()` - Catch `ValueError`, `FileNotFoundError`
4. `stats_command()` - Catch `ConnectionError`, `ValueError`

**Test**:

```bash
poetry run pytest tests/unit/test_cli.py -v
```

---

### Step 4: Test End-to-End Pipeline ⏳ 30 minutes

**Run existing tests**:

```bash
poetry run pytest tests/integration/test_e2e_pipeline.py -v
```

**Expected Output**:

```
TestE2EPipeline::test_parse_and_validate_bhavcopy PASSED
TestE2EPipeline::test_save_and_load_parquet PASSED
TestE2EPipeline::test_load_to_clickhouse PASSED (if ClickHouse is running)
TestE2EPipeline::test_idempotency_marker_prevents_duplicate_load PASSED
TestDataQuality::test_ohlc_consistency PASSED
TestDataQuality::test_invalid_ohlc_detection PASSED
TestDataQuality::test_duplicate_detection PASSED
TestDataQuality::test_price_gap_detection PASSED
TestCorpActionAdjustments::test_split_adjustment PASSED
TestCorpActionAdjustments::test_dividend_adjustment PASSED
```

**If Tests Fail**:

1. Check if ClickHouse is running: `docker ps | grep clickhouse`
2. Check if test data exists in `data/` directory
3. Run with verbose output: `pytest -vv` for debugging

---

### Step 5: Verify Rate Limiting ⏳ 15 minutes

**Start API**:

```bash
docker-compose -f docker-compose.local.yml up -d
```

**Test Rate Limiting**:

```bash
# Install httpie if needed
pip install httpie

# Run 6 login attempts rapidly
for i in {1..6}; do
    echo "Attempt $i:"
    http -f POST http://localhost:8000/api/v1/auth/token \
        username=demo password=demo123
    sleep 0.5
done
```

**Expected Results**:

- Attempts 1-5: 200 OK with token
- Attempt 6: 429 Too Many Requests

**Verify in Logs**:

```bash
docker logs -f champion_api
```

---

## 📋 Completion Checklist

### Tests & Validation

- [ ] End-to-end tests pass (Step 4)
- [ ] Error streaming tests pass (Step 2)
- [ ] Rate limiting verified (Step 5)
- [ ] No new ruff/linting errors (run: `poetry run ruff check src/`)
- [ ] All unit tests pass (run: `poetry run pytest tests/unit/ -v`)

### Memory Optimization

- [ ] ErrorStream integrated in ParquetValidator (Step 1)
- [ ] ValidationResult includes error_file path (Step 1)
- [ ] Tested with large file (> 100K rows) (Step 1)

### Exception Handling

- [ ] cli.py updated with specific exceptions (Step 3)
- [ ] All handlers follow 3-pattern: specific → general → logger.exception (Step 3)
- [ ] No new `except Exception` patterns introduced (Step 3)

### Documentation

- [ ] Progress report created ✅
- [ ] Checklist created ✅

---

## 🚀 How to Start

**Right Now**:

```bash
cd /media/sandeep-jaiswar/DataDrive/champion

# 1. Start local stack
docker-compose -f docker-compose.local.yml up -d

# 2. Run tests to see current state
poetry run pytest tests/integration/test_e2e_pipeline.py -v

# 3. Begin Step 1 above
# (Modify validator.py to integrate ErrorStream)
```

---

## ⏱️ Time Breakdown

| Step | Time | Complexity | Priority |
|------|------|-----------|----------|
| 1. Memory Leak Fix | 30 min | Medium | 🔴 HIGH |
| 2. Streaming Tests | 20 min | Low | 🟡 MEDIUM |
| 3. CLI Exceptions | 45 min | Low | 🟡 MEDIUM |
| 4. E2E Test Run | 30 min | Low | 🟢 LOW |
| 5. Rate Limit Test | 15 min | Low | 🟢 LOW |
| **TOTAL** | **140 min** | - | - |

---

## 💡 Tips

- **Working on Step 1?** Keep the reference implementation from `error_streaming.py` open
- **Testing?** Run `poetry run pytest --tb=short` for cleaner output
- **Docker issue?** Try `docker-compose -f docker-compose.local.yml restart`
- **Need fresh start?** Run `docker-compose -f docker-compose.local.yml down -v` to reset

---

## 📞 Quick Command Reference

```bash
# Start services
docker-compose -f docker-compose.local.yml up -d

# Stop services
docker-compose -f docker-compose.local.yml down

# View logs
docker-compose -f docker-compose.local.yml logs -f champion_api

# Run tests
poetry run pytest tests/integration/test_e2e_pipeline.py -v

# Run linting
poetry run ruff check src/

# Check types
poetry run pyright src/

# Get detailed error
poetry run pytest -vv tests/integration/test_e2e_pipeline.py::TestE2EPipeline::test_parse_and_validate_bhavcopy
```

---

**Ready to start? Begin with Step 1 above! 🚀**

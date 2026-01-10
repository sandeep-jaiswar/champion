#!/usr/bin/env python3
"""Demo script to showcase Parquet validation functionality."""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from validation.validator import ParquetValidator


def demo_validation():
    """Demonstrate validation with sample data."""
    print("=" * 80)
    print("Parquet Data Validation Demo")
    print("=" * 80)
    print()

    # Initialize validator
    schema_dir = Path(__file__).parent.parent / "schemas" / "parquet"
    validator = ParquetValidator(schema_dir=schema_dir)
    
    print(f"✓ Loaded schemas from: {schema_dir}")
    print(f"  Available schemas: {', '.join(validator.schemas.keys())}")
    print()

    sample_data_dir = Path(__file__).parent / "sample_data"
    quarantine_dir = Path(__file__).parent / "quarantine"
    quarantine_dir.mkdir(exist_ok=True)

    # Test 1: Validate valid raw OHLC data
    print("-" * 80)
    print("Test 1: Validating VALID raw equity OHLC data")
    print("-" * 80)
    
    result = validator.validate_file(
        file_path=sample_data_dir / "raw_equity_ohlc_valid.parquet",
        schema_name="raw_equity_ohlc",
        quarantine_dir=quarantine_dir
    )
    
    print(f"Total rows:         {result.total_rows}")
    print(f"Valid rows:         {result.valid_rows}")
    print(f"Critical failures:  {result.critical_failures}")
    print(f"Warnings:           {result.warnings}")
    
    if result.critical_failures == 0:
        print("✅ PASSED - All records are valid!")
    else:
        print("❌ FAILED - Validation errors detected")
    print()

    # Test 2: Validate invalid raw OHLC data
    print("-" * 80)
    print("Test 2: Validating raw equity OHLC data WITH ERRORS")
    print("-" * 80)
    
    result = validator.validate_file(
        file_path=sample_data_dir / "raw_equity_ohlc_with_errors.parquet",
        schema_name="raw_equity_ohlc",
        quarantine_dir=quarantine_dir
    )
    
    print(f"Total rows:         {result.total_rows}")
    print(f"Valid rows:         {result.valid_rows}")
    print(f"Critical failures:  {result.critical_failures}")
    print(f"Warnings:           {result.warnings}")
    
    if result.critical_failures > 0:
        print(f"\n⚠️  Found {result.critical_failures} validation errors:")
        for i, error in enumerate(result.error_details[:5], 1):
            print(f"   {i}. Row {error['row_index']}: {error['message']}")
        
        if len(result.error_details) > 5:
            print(f"   ... and {len(result.error_details) - 5} more errors")
        
        print(f"\n📁 Failed records quarantined to: {quarantine_dir / 'raw_equity_ohlc_failures.parquet'}")
    print()

    # Test 3: Validate valid normalized OHLC data
    print("-" * 80)
    print("Test 3: Validating VALID normalized equity OHLC data")
    print("-" * 80)
    
    result = validator.validate_file(
        file_path=sample_data_dir / "normalized_equity_ohlc_valid.parquet",
        schema_name="normalized_equity_ohlc",
        quarantine_dir=quarantine_dir
    )
    
    print(f"Total rows:         {result.total_rows}")
    print(f"Valid rows:         {result.valid_rows}")
    print(f"Critical failures:  {result.critical_failures}")
    print(f"Warnings:           {result.warnings}")
    
    if result.critical_failures == 0:
        print("✅ PASSED - All records are valid!")
    else:
        print("❌ FAILED - Validation errors detected")
    print()

    # Test 4: Validate invalid normalized OHLC data
    print("-" * 80)
    print("Test 4: Validating normalized equity OHLC data WITH ERRORS")
    print("-" * 80)
    
    result = validator.validate_file(
        file_path=sample_data_dir / "normalized_equity_ohlc_with_errors.parquet",
        schema_name="normalized_equity_ohlc",
        quarantine_dir=quarantine_dir
    )
    
    print(f"Total rows:         {result.total_rows}")
    print(f"Valid rows:         {result.valid_rows}")
    print(f"Critical failures:  {result.critical_failures}")
    print(f"Warnings:           {result.warnings}")
    
    if result.critical_failures > 0:
        print(f"\n⚠️  Found {result.critical_failures} validation errors:")
        for i, error in enumerate(result.error_details[:5], 1):
            print(f"   {i}. Row {error['row_index']}: {error['message']}")
        
        if len(result.error_details) > 5:
            print(f"   ... and {len(result.error_details) - 5} more errors")
        
        print(f"\n📁 Failed records quarantined to: {quarantine_dir / 'normalized_equity_ohlc_failures.parquet'}")
    print()

    # Summary
    print("=" * 80)
    print("Demo Summary")
    print("=" * 80)
    print()
    print("✅ Validation Features Demonstrated:")
    print("   - JSON Schema validation (types, nullability, patterns)")
    print("   - Range validation (prices >= 0, volumes >= 0)")
    print("   - Business logic validation (OHLC consistency: high >= low)")
    print("   - Quarantine functionality (failed records isolated)")
    print()
    print("📚 Next Steps:")
    print("   - Integrate validation into Prefect flows")
    print("   - Set up Slack alerts for validation failures")
    print("   - Deploy validation flows to Prefect server")
    print()
    print("=" * 80)


if __name__ == "__main__":
    demo_validation()

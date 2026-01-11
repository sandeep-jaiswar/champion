"""Benchmark script comparing old CSV parser vs new Polars parser.

This script generates sample bhavcopy data and compares:
1. Parse speed
2. Memory usage (if psutil available)
3. Output file size
"""

import time
from datetime import date
from pathlib import Path

try:
    import psutil

    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

from src.parsers.bhavcopy_parser import BhavcopyParser
from src.parsers.polars_bhavcopy_parser import PolarsBhavcopyParser


def generate_large_csv(output_path: Path, num_rows: int = 2500):
    """Generate a large CSV file for benchmarking.

    Args:
        output_path: Path to output CSV file
        num_rows: Number of rows to generate
    """
    symbols = [
        "RELIANCE",
        "TCS",
        "INFY",
        "HDFCBANK",
        "ITC",
        "SBIN",
        "BHARTIARTL",
        "KOTAKBANK",
        "LT",
        "HINDUNILVR",
    ]

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        # Write header
        f.write(
            "TradDt,BizDt,Sgmt,Src,FinInstrmTp,FinInstrmId,ISIN,TckrSymb,SctySrs,"
            "XpryDt,FininstrmActlXpryDt,StrkPric,OptnTp,FinInstrmNm,OpnPric,HghPric,"
            "LwPric,ClsPric,LastPric,PrvsClsgPric,UndrlygPric,SttlmPric,OpnIntrst,"
            "ChngInOpnIntrst,TtlTradgVol,TtlTrfVal,TtlNbOfTxsExctd,SsnId,NewBrdLotQty,"
            "Rmks,Rsvd01,Rsvd02,Rsvd03,Rsvd04\n"
        )

        # Write data rows
        for i in range(num_rows):
            symbol = symbols[i % len(symbols)]
            base_price = 100.0 + (i * 10)
            f.write(
                f"2024-01-02,2024-01-02,CM,NSE,STK,{2885 + i},"
                f"INE{str(100 + i).zfill(6)}A0101{i%10},{symbol},EQ,"
                f"-,-,-,-,{symbol} Ltd,"
                f"{base_price},{base_price + 10},{base_price - 5},{base_price + 5},"
                f"{base_price + 4.5},{base_price},-,{base_price + 5},"
                f"-,-,{10000 + i * 1000},{(10000 + i * 1000) * base_price},"
                f"{1000 + i * 100},F1,1,-,-,-,-,-\n"
            )

    print(f"✅ Generated CSV with {num_rows} rows at {output_path}")


def measure_memory() -> float:
    """Get current memory usage in MB."""
    if not HAS_PSUTIL:
        return 0.0
    process = psutil.Process()
    return process.memory_info().rss / (1024 * 1024)


def benchmark_old_parser(csv_path: Path, trade_date: date) -> dict:
    """Benchmark the old CSV parser.

    Args:
        csv_path: Path to CSV file
        trade_date: Trading date

    Returns:
        Dictionary with benchmark results
    """
    parser = BhavcopyParser()

    # Measure memory before
    mem_before = measure_memory()

    # Parse
    start_time = time.time()
    events = parser.parse(csv_path, trade_date)
    parse_time = time.time() - start_time

    # Measure memory after
    mem_after = measure_memory()
    mem_used = mem_after - mem_before

    return {
        "parser": "CSV (pandas)",
        "parse_time": parse_time,
        "memory_mb": mem_used,
        "rows": len(events),
        "output_format": "list[dict]",
        "output_size_mb": 0,  # No file output
    }


def benchmark_polars_parser(csv_path: Path, trade_date: date, tmp_dir: Path) -> dict:
    """Benchmark the new Polars parser.

    Args:
        csv_path: Path to CSV file
        trade_date: Trading date
        tmp_dir: Temporary directory for Parquet output

    Returns:
        Dictionary with benchmark results
    """
    parser = PolarsBhavcopyParser()

    # Measure memory before
    mem_before = measure_memory()

    # Parse
    start_time = time.time()
    events = parser.parse(csv_path, trade_date, output_parquet=False)
    parse_time = time.time() - start_time

    # Measure memory after
    mem_after = measure_memory()
    mem_used = mem_after - mem_before

    # Write to Parquet and measure output size
    df = parser.parse_to_dataframe(csv_path, trade_date)
    parquet_file = parser.write_parquet(df, trade_date, base_path=tmp_dir)
    output_size_mb = parquet_file.stat().st_size / (1024 * 1024)

    return {
        "parser": "Polars",
        "parse_time": parse_time,
        "memory_mb": mem_used,
        "rows": len(events),
        "output_format": "Parquet",
        "output_size_mb": output_size_mb,
    }


def print_results(old_results: dict, new_results: dict):
    """Print benchmark comparison results.

    Args:
        old_results: Results from old parser
        new_results: Results from new parser
    """
    print("\n" + "=" * 80)
    print("BENCHMARK RESULTS".center(80))
    print("=" * 80)

    print(f"\nDataset: {old_results['rows']:,} rows")

    print("\n--- Parse Time ---")
    print(f"  Old Parser (CSV):    {old_results['parse_time']:.4f}s")
    print(f"  New Parser (Polars): {new_results['parse_time']:.4f}s")
    speedup = old_results["parse_time"] / new_results["parse_time"]
    print(f"  Speedup:             {speedup:.2f}x faster")

    if HAS_PSUTIL:
        print("\n--- Memory Usage ---")
        print(f"  Old Parser (CSV):    {old_results['memory_mb']:.2f} MB")
        print(f"  New Parser (Polars): {new_results['memory_mb']:.2f} MB")
        mem_reduction = (
            (old_results["memory_mb"] - new_results["memory_mb"]) / old_results["memory_mb"] * 100
        )
        print(f"  Memory Reduction:    {mem_reduction:.1f}%")
    else:
        print("\n--- Memory Usage ---")
        print("  (psutil not available - skipping memory measurement)")

    print("\n--- Output ---")
    print(f"  Old Parser:          {old_results['output_format']}")
    print(f"  New Parser:          {new_results['output_format']}")
    print(f"  Parquet Size:        {new_results['output_size_mb']:.2f} MB")

    # Check acceptance criteria
    print("\n--- Acceptance Criteria ---")
    meets_1s = new_results["parse_time"] < 1.0
    print(f"  Parse < 1s for 2,500+ rows: {'✅ PASS' if meets_1s else '❌ FAIL'}")
    print(f"    (Actual: {new_results['parse_time']:.4f}s)")

    print("\n" + "=" * 80)


def main():
    """Run benchmark comparison."""
    print("🚀 Starting Bhavcopy Parser Benchmark")
    print("-" * 80)

    # Create test data
    test_dir = Path("/tmp/bhavcopy_benchmark")
    test_dir.mkdir(exist_ok=True)

    csv_file = test_dir / "benchmark_data.csv"
    generate_large_csv(csv_file, num_rows=2500)

    trade_date = date(2024, 1, 2)

    # Benchmark old parser
    print("\n📊 Benchmarking old CSV parser...")
    old_results = benchmark_old_parser(csv_file, trade_date)

    # Benchmark new parser
    print("📊 Benchmarking new Polars parser...")
    new_results = benchmark_polars_parser(csv_file, trade_date, test_dir)

    # Print results
    print_results(old_results, new_results)

    # Cleanup
    print(f"\n🧹 Cleanup test data at {test_dir}")


if __name__ == "__main__":
    main()

"""Tests for error streaming functionality."""

import tempfile
from pathlib import Path

from champion.validation.error_streaming import ErrorStream


def test_error_stream_keeps_samples():
    """ErrorStream.get_samples() returns only N items."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        stream = ErrorStream(output_file=Path(tmp_dir) / "errors.jsonl", keep_samples=10)

        # Add 50 errors
        for i in range(50):
            stream.write_error(
                {
                    "row_id": i,
                    "error": f"error_{i}",
                    "field": "price",
                }
            )

        # get_samples() should return only 10
        samples = stream.get_samples()
        assert len(samples) == 10
        assert samples[0]["row_id"] == 0
        assert samples[-1]["row_id"] == 9


def test_error_stream_writes_all_to_disk():
    """ErrorStream.iter_all_errors() returns ALL errors from disk."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        stream = ErrorStream(output_file=Path(tmp_dir) / "errors.jsonl", keep_samples=10)

        # Add 50 errors
        for i in range(50):
            stream.write_error({"row_id": i, "error": f"error_{i}"})

        # iter_all_errors() should read all 50
        all_errors = list(stream.iter_all_errors())
        assert len(all_errors) == 50
        assert all_errors[0]["row_id"] == 0
        assert all_errors[-1]["row_id"] == 49


def test_error_stream_total_errors_tracked():
    """ErrorStream tracks total errors written."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        stream = ErrorStream(output_file=Path(tmp_dir) / "errors.jsonl", keep_samples=100)

        # Add 100 errors
        for i in range(100):
            stream.write_error({"row_id": i})

        # Total should be tracked
        assert stream.total_errors == 100


def test_error_stream_batch_write():
    """ErrorStream.write_errors() batch write works."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        stream = ErrorStream(output_file=Path(tmp_dir) / "errors.jsonl", keep_samples=50)

        # Batch write 30 errors
        errors = [{"row_id": i, "value": i * 10} for i in range(30)]
        stream.write_errors(errors)

        # Should have all 30
        assert stream.total_errors == 30

        # All should be on disk
        all_errors = list(stream.iter_all_errors())
        assert len(all_errors) == 30


def test_error_stream_file_created():
    """ErrorStream creates output file."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        output_file = Path(tmp_dir) / "errors.jsonl"
        stream = ErrorStream(output_file=output_file, keep_samples=10)

        assert not output_file.exists()

        stream.write_error({"error": "test"})

        assert output_file.exists()


def test_error_stream_cleanup():
    """ErrorStream cleanup removes file."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        output_file = Path(tmp_dir) / "errors.jsonl"
        stream = ErrorStream(output_file=output_file)

        stream.write_error({"error": "test"})
        assert output_file.exists()

        stream.cleanup()
        assert not output_file.exists()

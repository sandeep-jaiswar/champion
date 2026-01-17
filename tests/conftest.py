import sys
from pathlib import Path

# Ensure src/ is on sys.path for imports like `import champion.*`
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_PATH = PROJECT_ROOT / "src"
if SRC_PATH.exists():
    sys.path.insert(0, str(SRC_PATH))
import os

# When running tests, use an isolated temporary data directory to avoid
# picking up repository sample data that changes control flow in integration tests.
PYTEST_TEMP_DIR = PROJECT_ROOT / ".pytest_data"
PYTEST_TEMP_DIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("PYTEST_RUNNING", "1")
os.environ.setdefault("PYTEST_TEMP_DIR", str(PYTEST_TEMP_DIR))

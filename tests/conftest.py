import sys
from pathlib import Path

# Ensure src/ is on sys.path for imports like `import champion.*`
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_PATH = PROJECT_ROOT / "src"
if SRC_PATH.exists():
    sys.path.insert(0, str(SRC_PATH))

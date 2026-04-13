# Pytest configuration
import sys
from pathlib import Path
from unittest.mock import MagicMock

# Add app to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Mock motor only when dependency is unavailable.
try:
    import motor.motor_asyncio  # noqa: F401
except Exception:
    sys.modules["motor"] = MagicMock()
    sys.modules["motor.motor_asyncio"] = MagicMock()

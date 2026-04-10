# Pytest configuration
import sys
from pathlib import Path
from unittest.mock import MagicMock

# Add app to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Mock motor to avoid pymongo API compatibility issues in tests
sys.modules['motor'] = MagicMock()
sys.modules['motor.motor_asyncio'] = MagicMock()

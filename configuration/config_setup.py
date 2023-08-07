from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, ".")

configuration_directory = Path(__file__).parents[0]

if os.environ.get("PYTHON_ENV"):
    print(f"{os.environ.get('PYTHON_ENV')}.env")
    load_dotenv(f"{configuration_directory}/{os.environ.get('PYTHON_ENV')}.env")
else:
    print("Loading local.env")
    load_dotenv(f"{configuration_directory}/local.env")

config = os.environ.copy()

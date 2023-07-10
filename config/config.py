from __future__ import annotations

import os
import sys

from dotenv import load_dotenv

sys.path.insert(0, ".")

if os.environ.get("PYTHON_ENV"):
    print(f"{os.environ.get('PYTHON_ENV')}.env")
    load_dotenv(f"config/{os.environ.get('PYTHON_ENV')}.env")
else:
    print("Loading local.env")
    load_dotenv("config/local.env")

config = os.environ.copy()

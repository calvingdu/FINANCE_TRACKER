import os
from dotenv import load_dotenv
import sys

sys.path.insert(0,'.')

if os.environ.get('PYTHON_ENV'):
    print(f"{os.environ.get('PYTHON_ENV')}.env")
    load_dotenv(f"backend/config/{os.environ.get('PYTHON_ENV')}.env")
else: 
    print("Loading local.env")
    load_dotenv(f"backend/config/local.env")

config = os.environ.copy()
"""
File: config.py
Description: Central configuration file containing project-wide paths and constants.
"""


from pathlib import Path

# --------------------- Base ---------------------
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "dummydata"

# --------------------- Dummy Data ---------------------
TEXT_DATA_PATH = DATA_DIR / "text.csv"
VITALS_DATA_PATH = DATA_DIR / "vitals.csv"
IMAGES_DIR = DATA_DIR / "images"

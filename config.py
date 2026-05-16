import os
import pathlib
import logging
from dotenv import load_dotenv

load_dotenv()

# ── Industry settings ─────────────────────────────────────────────────────────
INDUSTRY       = os.getenv("INDUSTRY", "retail")
LEARNER_SCHEMA = os.getenv("LEARNER_SCHEMA", "learner_47")

# ── File paths ────────────────────────────────────────────────────────────────
PROJECT_ROOT  = pathlib.Path(__file__).resolve().parent

DATA_DIR      = PROJECT_ROOT / "data"
RAW_DATA_DIR  = DATA_DIR / "raw"
PROC_DATA_DIR = DATA_DIR / "processed"
LOG_DIR       = DATA_DIR / "logs"
REPORTS_DIR   = PROJECT_ROOT / "reports"

# Create directories if they don't exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROC_DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# File paths used across the project
RAW_DATA_PATH  = RAW_DATA_DIR  / "raw-data.csv"
PROC_DATA_PATH = PROC_DATA_DIR / "processed-data.csv"  # ETL output / EDA input
LOG_PATH       = LOG_DIR       / "log.txt"

# ── Database connection ───────────────────────────────────────────────────────
DB_URL = os.getenv("DB_URL", "")

try:
    from sqlalchemy import create_engine
    engine = create_engine(DB_URL, pool_pre_ping=True)
except Exception as _e:
    engine = None

# ── Logging ───────────────────────────────────────────────────────────────────
def _setup_logger(name: str = "samuel") -> logging.Logger:
    lgr = logging.getLogger(name)
    lgr.setLevel(logging.INFO)

    if not lgr.handlers:
        fmt = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(fmt)
        lgr.addHandler(console_handler)

        file_handler = logging.FileHandler(LOG_PATH, mode="a")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(fmt)
        lgr.addHandler(file_handler)

    return lgr

logger = _setup_logger("samuel")

# ── Validation thresholds ─────────────────────────────────────────────────────
MAX_NULL_PERCENT      = 50.0
MAX_DUPLICATE_PERCENT = 5.0
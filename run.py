# ================================================================
# run.py — Pipeline Entry Point
# ================================================================
# HOW TO RUN (from the project folder):
#   python run.py
#
# WHAT WILL HAPPEN:
#   1. Raw data is extracted from Supabase and saved to data/raw/
#   2. Data quality checks run — problems are reported
#   3. Cleaning transformations run — nulls filled, duplicates removed
#   4. Processed data is saved to data/processed/
#   5. A full report is printed to the terminal
# ================================================================

import sys
import pathlib

# Add the project root to the Python path
_root = pathlib.Path(__file__).resolve().parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from config import DB_URL, logger
from src.data_access.query_runner import run_query
from src.data_access.data_extractor import save_to_csv
from src.etl.etl_pipeline import ETLPipeline


def main() -> None:
    """
    Run the complete ETL pipeline.
    Step 1: Extract from Supabase and save raw CSV
    Step 2: Validate, transform, load and report
    """

    # ── STEP 1: EXTRACT ──────────────────────────────────────────
    # Read the SQL query from file
    logger.info("=" * 55)
    logger.info("  STEP 1 — EXTRACTING DATA FROM SUPABASE")
    logger.info("=" * 55)

    with open("sql/extracted_raw_data.sql", "r") as f:
        query = f.read()

    df = run_query(DB_URL, query)
    save_to_csv(df, "data/raw/raw-data.csv")

    # ── STEP 2: ETL PIPELINE ─────────────────────────────────────
    # Validate, transform, load and report
    logger.info("=" * 55)
    logger.info("  STEP 2 — RUNNING ETL PIPELINE")
    logger.info("=" * 55)

    pipeline = ETLPipeline()
    logger.info(f"Pipeline ready: {pipeline}")

    (
        pipeline
        .extract()      # load raw-data.csv
        .validate()     # check data quality
        .transform()    # fix nulls, duplicates, types
        .load()         # save processed-data.csv
        .report()       # print full results
    )

    logger.info(f"Pipeline complete: {pipeline}")


if __name__ == "__main__":
    main()
# ================================================================
# src/etl_pipeline.py
# ================================================================
# CONTEXT: We have built DataValidator (inspect) and DataTransformer (fix).
# Now we build the conductor that orchestrates both.
#
# THE ANALOGY:
# In an orchestra, individual musicians are experts at their instrument.
# The conductor does not play an instrument — they direct when each musician
# plays and ensure everything happens in the right order.
#
# ETLPipeline is the conductor.
# It owns a DataValidator and a DataTransformer.
# It tells them what to do and in what order.
# It collects their outputs and produces the final result.
#
# THIS IS THE "COORDINATOR" OOP PATTERN:
# Each class has ONE clear responsibility:
#   DataValidator    → inspect data, report problems
#   DataTransformer  → fix data, report changes
#   ETLPipeline      → orchestrate everything, manage files, produce report
#
# ETL stands for Extract → Transform → Load.
# In our pipeline:
#   Extract  = load raw-data.csv from disk (already extracted by Module 03)
#   Validate = run DataValidator
#   Transform= run DataTransformer
#   Load     = save processed-data.csv to disk
# ================================================================

import sys
import pathlib

_root = pathlib.Path(__file__).resolve().parent
while not (_root / "config.py").exists() and _root != _root.parent:
    _root = _root.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

import pandas as pd           # for loading the CSV file

from config import (
    INDUSTRY,           # which industry schema we are processing
    RAW_DATA_PATH,      # where to find raw-data.csv
    PROC_DATA_PATH,     # where to save processed-data.csv
    logger              # shared logger
)
from src.etl.validator import DataValidator
from src.etl.transformer import DataTransformer

class ETLPipeline:
    """
    Orchestrates the complete ETL workflow.

    LIFECYCLE:
    The pipeline goes through named stages so we always know where we are:
      "ready"      → just created, nothing loaded
      "extracted"  → raw-data.csv loaded into memory
      "validated"  → DataValidator has run all checks
      "transformed"→ DataTransformer has cleaned the data
      "loaded"     → processed-data.csv saved to disk

    METHOD CHAIN (how we will call this in run.py):
      pipeline.extract().validate().transform().load().report()

    This reads almost like English:
    "Extract the data, validate it, transform it, load it, report the results."

    Attributes
    ──────────
    industry     str                 the configured industry name
    raw_df       pd.DataFrame        the raw loaded data (set by extract())
    validator    DataValidator       the validator object (set by validate())
    transformer  DataTransformer     the transformer object (set by transform())
    _status      str                 current lifecycle stage
    _run_log     list[str]           ordered record of every major action
    """

    def __init__(self):
        """
        Initialise the pipeline.

        We set up tracking attributes here.
        We do NOT load any data yet — that is extract()'s job.

        This is called when you write:
            pipeline = ETLPipeline()
        """

        # Which industry schema are we processing?
        # This comes from config.py and ultimately from the .env file
        self.industry = INDUSTRY

        # These will be populated as the pipeline progresses
        self.raw_df      = None    # populated by extract()
        self.validator   = None    # populated by validate()
        self.transformer = None    # populated by transform()

        # Lifecycle state tracking
        # Changes: "ready" → "extracted" → "validated" → "transformed" → "loaded"
        self._status = "ready"

        # Ordered audit log — every major action is recorded here
        # This feeds into the final report
        self._run_log = []

        # Log that the pipeline exists
        logger.info(
            f"ETLPipeline initialised | "
            f"industry: {self.industry} | "
            f"input: {RAW_DATA_PATH.name} | "
            f"output: {PROC_DATA_PATH.name}"
        )

    # ================================================================
    # E: EXTRACT
    # ================================================================

    def extract(self) -> "ETLPipeline":
        """
        Load raw-data.csv into memory as a pandas DataFrame.

        WHY "EXTRACT" WHEN THE CSV ALREADY EXISTS?
        ────────────────────────────────────────────
        "Extract" in ETL means: get the data from its source and load it
        into a usable format. Module 03 already extracted it FROM the database.
        We are now extracting it FROM the CSV file into memory.

        In production systems, Extract can mean:
          - Querying a live database (what Module 03 did)
          - Reading from cloud storage (S3, Azure Blob, GCS)
          - Calling an API
          - Reading from a data stream (Kafka, Kinesis)
          - Reading from a CSV file (what we do here)

        pd.read_csv() ARGUMENTS:
        ──────────────────────────
        filepath_or_buffer  the path to the CSV file
        low_memory=False    read the ENTIRE file before inferring column types
                            (the default reads in chunks, which can cause mixed types)

        Returns self for method chaining.
        """

        logger.info(f"[EXTRACT] Loading: {RAW_DATA_PATH}")

        # Check the file exists BEFORE trying to open it
        # If we tried to open a non-existent file, we would get a cryptic FileNotFoundError
        # By checking first, we can give a helpful, actionable error message
        if not RAW_DATA_PATH.exists():
            raise FileNotFoundError(
                f"raw-data.csv not found at: {RAW_DATA_PATH}\n"
                "Run Module 03 extractor first, then copy raw-data.csv to data/raw/"
            )

        # Load the CSV file into a pandas DataFrame
        # low_memory=False ensures correct type inference on large files
        self.raw_df = pd.read_csv(RAW_DATA_PATH, low_memory=False)

        # Update lifecycle state
        self._status = "extracted"

        # Record in the audit log
        self._log(
            f"Extracted {len(self.raw_df):,} rows × "
            f"{self.raw_df.shape[1]} columns from {RAW_DATA_PATH.name}"
        )

        return self   # return self for chaining

    # ================================================================
    # V: VALIDATE
    # ================================================================

    def validate(self) -> "ETLPipeline":
        """
        Run all quality checks on the raw DataFrame.

        WHAT HAPPENS IF VALIDATION FAILS?
        ────────────────────────────────────
        If DataValidator finds a CRITICAL issue (not just a WARNING),
        we raise a RuntimeError. This immediately stops the pipeline.

        Why stop the pipeline on CRITICAL issues?
          A CRITICAL issue means the data is fundamentally broken.
          Proceeding with broken data would produce corrupted output.
          It is better to fail loudly now than silently corrupt data downstream.

        This "fail fast" approach is used by:
          - dbt (data build tool) — fails the entire run on critical tests
          - Apache Spark — throws exceptions on schema violations
          - Great Expectations — marks suites as failed if critical expectations fail

        WARNINGS are different — they note problems but let the pipeline continue.
        The transformer will fix WARNING-level issues (nulls, duplicates, etc.).

        Returns self.
        """

        logger.info("[VALIDATE] Running data quality checks...")

        # Create a DataValidator with the raw data we just loaded
        self.validator = DataValidator(self.raw_df)

        # Run all five checks using method chaining
        # Each check returns the validator object, so we can chain like this:
        (
            self.validator
            .check_not_empty()        # must have rows — fail immediately if not
            .check_nulls()            # flag columns with missing values
            .check_duplicates()       # flag exact duplicate rows
            .check_numeric_ranges()   # flag impossible negative values
            .compute_stats()          # build the stats summary
        )

        # Check if any CRITICAL issues were found
        if not self.validator._passed:

            # Collect just the CRITICAL issues for the error message
            critical = [
                i for i in self.validator.issues
                if i["severity"] == "CRITICAL"
            ]

            # RuntimeError stops everything and shows the message in the terminal
            raise RuntimeError(
                "Validation FAILED — " + str(len(critical)) + " CRITICAL issue(s):\n" +
                "\n".join(
                    f"  [{i['column']}] {i['message']}"
                    for i in critical
                ) +
                "\n  Fix the issues in raw-data.csv and run again."
            )

        # Count WARNING vs CRITICAL for the log message
        n_warnings  = sum(1 for i in self.validator.issues if i["severity"] == "WARNING")
        n_critical  = sum(1 for i in self.validator.issues if i["severity"] == "CRITICAL")

        self._status = "validated"
        self._log(
            f"Validation PASSED — "
            f"{n_warnings} warnings, {n_critical} critical issues"
        )

        return self

    # ================================================================
    # T: TRANSFORM
    # ================================================================

    def transform(self) -> "ETLPipeline":
        """
        Clean and enrich the validated DataFrame.

        Note: we pass self.raw_df (the original raw data) to DataTransformer,
        NOT a previously modified version. The validator only read the data —
        it never changed it. So self.raw_df is still the original.

        DataTransformer makes its own copy immediately in __init__,
        so the original self.raw_df is always preserved for debugging.

        After this step, the clean data lives in self.transformer.df.

        Returns self.
        """

        logger.info("[TRANSFORM] Starting transformations...")

        # Create the transformer with the original raw data
        self.transformer = DataTransformer(self.raw_df)

        # Run all five transformation steps using method chaining
        (
            self.transformer
            .fill_nulls()            # step 1: replace all NaN with appropriate defaults
            .drop_duplicates()       # step 2: remove exact copy rows
            .fix_types()             # step 3: convert text-stored numbers to numeric
            .add_derived_columns()   # step 4: add IQR outlier flags
            .add_metadata()          # step 5: stamp with pipeline run info
        )

        self._status = "transformed"
        self._log(
            f"Transformation complete — "
            f"{len(self.transformer.changes)} changes applied"
        )

        return self

    # ================================================================
    # L: LOAD
    # ================================================================

    def load(self) -> "ETLPipeline":
        """
        Save the processed DataFrame to processed-data.csv.

        WHY SAVE TO CSV AND NOT BACK TO THE DATABASE?
        ───────────────────────────────────────────────
        CSV files are:

        UNIVERSAL: Every tool can read them.
          → Python, R, Excel, Tableau, Spark, SQL tools
          → No special software required

        SNAPSHOTS: A frozen copy at a specific point in time.
          → You can compare "today's clean data" vs "last week's clean data"
          → Easy to audit and version control with DVC

        PORTABLE: No database connection required.
          → Students can work offline
          → Downstream modules (06, 09, 11-15) work without the database

        This is the standard in data lake architectures where
        intermediate results are stored as files in cloud storage
        (S3, Azure Data Lake, GCS) between pipeline stages.

        .to_csv() ARGUMENTS:
        ──────────────────────
        path_or_buf    where to save the file
        index=False    do NOT write the pandas row numbers as a column
                       (without this, a mysterious "Unnamed: 0" column appears)
        encoding="utf-8" handle international characters correctly
        """

        # Retrieve the cleaned DataFrame from the transformer
        processed_df = self.transformer.df

        logger.info(f"[LOAD] Saving {len(processed_df):,} rows to: {PROC_DATA_PATH}")

        # Save to CSV
        processed_df.to_csv(
            PROC_DATA_PATH,
            index    = False,     # no row numbers column
            encoding = "utf-8"    # UTF-8 handles all languages
        )

        # Verify the file was created and check its size
        file_size_kb = PROC_DATA_PATH.stat().st_size / 1024   # bytes → KB

        self._status = "loaded"
        self._log(
            f"Saved {len(processed_df):,} rows to "
            f"{PROC_DATA_PATH.name} ({file_size_kb:.1f} KB)"
        )

        return self

    # ================================================================
    # REPORT
    # ================================================================

    def report(self) -> None:
        """
        Print the complete pipeline execution report.

        This is the final step — it does not return self because
        report() is a terminal action (nothing chains after it).

        In production, this output would be:
          - Written to a structured log file for auditing
          - Sent to a Slack channel as a completion notification
          - Stored in a monitoring database for trend analysis over time
        """

        # Get the transformation summary from DataTransformer
        transform_summary = self.transformer.summary()

        print()
        print("═" * 62)
        print(f"  MODULE 05 — ETL PIPELINE COMPLETE")
        print("═" * 62)
        print(f"  Industry:          {self.industry}")
        print(f"  Final status:      {self._status.upper()}")
        print()

        # ── Extraction results ─────────────────────────────────────────
        print("  EXTRACTION")
        print(f"    Source file:     {RAW_DATA_PATH.name}")
        print(f"    Rows loaded:     {len(self.raw_df):,}")
        print(f"    Columns loaded:  {self.raw_df.shape[1]}")

        # ── Validation results ─────────────────────────────────────────
        print()
        print("  VALIDATION")
        print(f"    Result:          {'PASSED ✓' if self.validator._passed else 'FAILED ✗'}")
        print(f"    Issues found:    {len(self.validator.issues)}")
        print(f"    — Critical:      "
             f"{sum(1 for i in self.validator.issues if i['severity']=='CRITICAL')}")
        print(f"    — Warnings:      "
             f"{sum(1 for i in self.validator.issues if i['severity']=='WARNING')}")

        if self.validator.issues:
            print()
            print("    Issue details:")
            for issue in self.validator.issues:   # show all issues
                icon = "✗" if issue["severity"] == "CRITICAL" else "⚠"
                print(
                    f"      {icon} [{issue['severity']}] "
                    f"{issue['column']}: {issue['message']}"
                )

        # ── Transformation results ─────────────────────────────────────
        print()
        print("  TRANSFORMATION")
        print(f"    Rows before:     {transform_summary['original_rows']:,}")
        print(f"    Rows after:      {transform_summary['final_rows']:,}")
        print(f"    Rows removed:    {transform_summary['rows_removed']:,}")
        print(f"    Columns now:     {transform_summary['final_columns']}")
        print(f"    Changes made:    {transform_summary['changes_count']}")
        print()
        print("    Change log:")
        for change in transform_summary["change_log"]:
            print(f"      • {change}")

        # ── Load results ───────────────────────────────────────────────
        print()
        print("  LOAD")
        print(f"    Output file:     {PROC_DATA_PATH}")
        file_size_kb = PROC_DATA_PATH.stat().st_size / 1024
        print(f"    File size:       {file_size_kb:.1f} KB")

        # ── What comes next ────────────────────────────────────────────
        print()
        print("  NEXT MODULES THAT USE THIS OUTPUT:")
        print(f"    → Module 06 EDA:    reads {PROC_DATA_PATH.name}")
        print(f"    → Module 09 ML:     trains on {PROC_DATA_PATH.name}")
        print(f"    → Modules 11-15:    all downstream AI modules")
        print()
        print("  PIPELINE AUDIT LOG:")
        for entry in self._run_log:
            print(f"    [PIPELINE] {entry}")
        print("═" * 62)

    # ================================================================
    # INTERNAL HELPERS
    # ================================================================

    def _log(self, message: str) -> None:
        """Record an action in the pipeline audit log."""
        self._run_log.append(message)
        logger.info(f"[PIPELINE] {message}")

    # ================================================================
    # DUNDER METHODS
    # ================================================================

    def __str__(self) -> str:
        """Human-readable summary."""
        return (
            f"ETLPipeline("
            f"industry={self.industry!r}, "
            f"status={self._status!r})"
        )

    def __repr__(self) -> str:
        """Developer representation."""
        return (
            f"ETLPipeline("
            f"industry={self.industry!r}, "
            f"status={self._status!r}, "
            f"input={RAW_DATA_PATH.name!r})"
        )

# ================================================================
# src/etl/transformer.py
# ================================================================
# CONTEXT: DataValidator told us WHAT is wrong.
# This file FIXES it.
#
# THE ANALOGY:
# The validator was the doctor running blood tests.
# The transformer is the pharmacist filling the prescription.
# The validator said: "23 missing billed_amount values, 13 duplicate rows."
# The transformer says: "Fill those 23 with the median. Remove those 13."
#
# WHAT WE ARE BUILDING:
# A class called DataTransformer with 5 transformation steps:
#   1. fill_nulls()          → replace missing values with appropriate defaults
#   2. drop_duplicates()     → remove exact copy rows
#   3. fix_types()           → ensure numeric columns actually ARE numeric
#   4. add_derived_columns() → create new useful columns from existing ones
#   5. add_metadata()        → stamp each row with pipeline tracking information
#
# IMMUTABILITY PRINCIPLE:
# Just like the validator, the transformer ALWAYS works on a copy of the data.
# We never modify the original. This means:
#   - If something goes wrong, we can always start fresh from the original
#   - Calling transformer.fill_nulls() twice gives the same result (idempotent)
#   - The caller's data is never accidentally corrupted

import sys
import pathlib

_root = pathlib.Path(__file__).resolve().parent
while not (_root / "config.py").exists() and _root != _root.parent:
    _root = _root.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

import pandas as pd       # data manipulation
import numpy as np        # numerical operations — for IQR outlier detection
import datetime           # for generating timestamps for metadata

from config import INDUSTRY, logger

class DataTransformer:
    """
    Cleans and enriches a validated DataFrame.

    DESIGN PRINCIPLE: Audit Trail
    Every change is recorded in self.changes — a list of strings describing
    what was done. This is the "audit trail." In production systems, every
    data change is logged so it can be traced, reviewed, and reversed if needed.
    """

    def __init__(self, df: pd.DataFrame):
        """
        Initialise the transformer with the validated DataFrame.

        We create a working copy immediately.
        self.original_len remembers the row count BEFORE any changes,
        so the final report can show how many rows were added or removed.

        Args:
            df    the validated DataFrame from DataValidator
        """
        # Create an independent working copy
        # Changes to self.df will NOT affect the df passed in by the caller
        self.df = df.copy()

        # Record the original row count for the summary report
        # After drop_duplicates() this number will be larger than len(self.df)
        self.original_len = len(df)

        # Audit trail: every transformation is recorded here
        # This list is shown in the final pipeline report
        self.changes = []

        # ================================================================
    # STEP 1: FILL NULL VALUES
    # ================================================================

    def fill_nulls(self) -> "DataTransformer":
        """
        Replace null (NaN) values with appropriate default values.

        WHY NOT JUST DROP ROWS WITH NULLS?
        ─────────────────────────────────────
        Dropping rows loses data. If 23 out of 1,200 employees have a missing
        billed_amount, should we remove those 23 people from all future analysis?
        That seems unfair — especially if they all happen to work in one department.

        WHY NOT FILL EVERYTHING WITH ZERO?
        ─────────────────────────────────────
        Filling billed amount nulls with 0 would make the average billed_amount appear
        dramatically lower than it really is. A billed_amount of zero is not
        "no data" — it is a specific value that means the employee earns nothing.

        THE RIGHT STRATEGY DEPENDS ON THE COLUMN TYPE:
        ─────────────────────────────────────────────────
        NUMERIC columns → fill with MEDIAN
          The median is the middle value in sorted order.
          It is NOT affected by extreme outliers.

          Example: salaries [50k, 60k, 70k, 80k, 500k]
            mean   = 152k (dragged up by the 500k outlier — wrong!)
            median = 70k  (the actual middle — correct!)

          The median is the industry standard default for imputation.

        TEXT columns → fill with "Unknown"
          We cannot average text. "Unknown" is an EXPLICIT marker.
          It tells every downstream tool: "this value was missing."
          It appears as a real category in groupby results and ML encoders.
          Much better than hiding the gap or filling with empty string.

        BOOLEAN columns → fill with False
          If we do not know whether a flag is True, assume False.
          "Unknown whether fraud" → assume "not fraud" (safer default).
        """

        # ── Numeric columns ────────────────────────────────────────────
        # select_dtypes(include=["number"]) returns a subset of the DataFrame
        # containing ONLY columns with numeric data types (int64, float64, etc.)
        for col in self.df.select_dtypes(include=["number"]).columns:

            # Count nulls in this column
            n_nulls = int(self.df[col].isna().sum())

            # Skip columns with no nulls — nothing to do
            if n_nulls == 0:
                continue

            # Compute the median of the non-null values
            # .median() automatically skips NaN values — no special handling needed
            median_value = self.df[col].median()

            # .fillna() returns a new Series with NaN replaced by the given value
            # We assign it back to self.df[col] to update the DataFrame
            self.df[col] = self.df[col].fillna(median_value)

            # Record this change in the audit trail
            self._record_change(
                f"Filled {n_nulls} nulls in '{col}' with median "
                f"({median_value:.2f})"
            )
            
            # ── Text (object) columns ───────────────────────────────────────
        for col in self.df.select_dtypes(include=["object"]).columns:
            n_nulls = int(self.df[col].isna().sum())
            if n_nulls == 0:
                continue
            self.df[col] = self.df[col].fillna("Unknown")
            self._record_change(
                f"Filled {n_nulls} nulls in '{col}' with 'Unknown'"
            )

        # ── Boolean columns ─────────────────────────────────────────────
        for col in self.df.select_dtypes(include=["bool"]).columns:
            n_nulls = int(self.df[col].isna().sum())
            if n_nulls == 0:
                continue
            self.df[col] = self.df[col].fillna(False)
            self._record_change(
                f"Filled {n_nulls} nulls in '{col}' with False"
            )

        # Verify all nulls are gone
        remaining = int(self.df.isna().sum().sum())
        logger.info(
            f"[TRANSFORM] fill_nulls complete. "
            f"Remaining nulls: {remaining}"
        )

        return self   # return self for method chaining
    
    # ================================================================
    # STEP 2: DROP DUPLICATE ROWS
    # ================================================================

    def drop_duplicates(self) -> "DataTransformer":
        """
        Remove rows that are exact copies of a previous row.

        HOW pandas.drop_duplicates() WORKS:
        ─────────────────────────────────────
        It compares every cell in every row.
        If row A and row B have identical values in ALL columns,
        row B is considered a duplicate of row A.

        keep="first": keep the first occurrence, remove all subsequent copies.
        This is the standard choice — the first record is assumed to be the "original."

        RESET THE INDEX:
        ─────────────────
        After removing rows, the index has gaps:
          Before: rows 0, 1, 2, 3, 4, 5, 6, 7, 8, 9
          Remove rows 3 and 7
          After:  rows 0, 1, 2, 4, 5, 6, 8, 9  ← gaps at 3 and 7!

        reset_index(drop=True) renumbers the index cleanly:
          After reset: rows 0, 1, 2, 3, 4, 5, 6, 7  ← clean sequential numbers

        drop=True means: discard the old index instead of saving it as a column.
        Without drop=True, the old index becomes a new column called "index" — messy.
        """

        before = len(self.df)   # remember the row count before removing duplicates

        # drop_duplicates() returns a NEW DataFrame with duplicates removed
        # We assign it back to self.df to replace the working copy
        self.df = self.df.drop_duplicates(keep="first")

        # reset_index() renumbers the DataFrame's row index from 0, 1, 2, 3...
        self.df = self.df.reset_index(drop=True)

        # How many rows were removed?
        removed = before - len(self.df)

        if removed > 0:
            self._record_change(
                f"Removed {removed} duplicate rows "
                f"({before:,} → {len(self.df):,})"
            )
        else:
            logger.info("[TRANSFORM] drop_duplicates: no duplicates found")

        return self
    
    # ================================================================
    # STEP 3: FIX DATA TYPES
    # ================================================================

    def fix_types(self) -> "DataTransformer":
        """
        Ensure numeric columns are actually stored as numbers, not text.

        THE PROBLEM:
        ─────────────
        Databases sometimes return numeric values as Python strings.
        This happens because:
          - Some PostgreSQL numeric types (NUMERIC, DECIMAL) are returned
            as strings by certain versions of psycopg2
          - A column might contain mostly numbers but one row has "N/A" in it,
            causing pandas to treat the whole column as text

        THE SYMPTOM:
        ─────────────
        billed_amount = "92000" (a string with quotes)
        You cannot compute: "92000" + 5000   → TypeError!
        You cannot compute: "92000".mean()   → AttributeError!

        THE FIX — pd.to_numeric() WITH errors='coerce':
        ──────────────────────────────────────────────────
        pd.to_numeric("92000")   → 92000.0   ✓
        pd.to_numeric("N/A")     → raises an error!

        With errors='coerce':
        pd.to_numeric("92000", errors="coerce")  → 92000.0  ✓
        pd.to_numeric("N/A", errors="coerce")    → NaN      ✓ (no error)

        The 80% rule:
        ─────────────
        We only convert a column if at least 80% of its non-null values
        successfully convert to numbers. This prevents us from accidentally
        converting a genuine text column like "department" into all-NaN.
        """

        # Only check columns that are CURRENTLY stored as text
        for col in self.df.columns:
            if self.df[col].dtype != object:
                continue   # already numeric, boolean, or datetime — skip

            # Attempt conversion with errors='coerce' (failed conversions → NaN)
            converted = pd.to_numeric(self.df[col], errors="coerce")

            # How many non-null values does the original column have?
            original_non_null  = int(self.df[col].notna().sum())

            # How many survived conversion (are not NaN after converting)?
            converted_non_null = int(converted.notna().sum())

            # Guard: avoid converting columns with no values at all
            if original_non_null == 0:
                continue

            # The 80% rule: only convert if most values survived
            # If we started with 100 non-null values and 85 survived → convert
            # If we started with 100 and only 3 survived → it is probably a text column
            if converted_non_null >= original_non_null * 0.8:
                self.df[col] = converted
                self._record_change(
                    f"Converted '{col}' from text to numeric "
                    f"({converted_non_null}/{original_non_null} values converted)"
                )

        logger.info("[TRANSFORM] fix_types complete")
        return self
    
    # ================================================================
    # STEP 4: ADD DERIVED COLUMNS
    # ================================================================

    def add_derived_columns(self) -> "DataTransformer":
        """
        Create new columns computed from existing columns.

        WHY ADD NEW COLUMNS?
        ─────────────────────
        The raw data tells you facts: "This employee earns £92,000."
        Derived columns add CONTEXT: "This employee earns £7,000 above their department average."

        Derived columns are used for:
          - ML features (outlier flags help models identify unusual cases)
          - Business analysis (is this value typical or extreme?)
          - EDA (quickly see what percentage of records are outliers)
          - Monitoring (track outlier rate over time — if it increases, data has changed)

        THE IQR OUTLIER DETECTION METHOD:
        ────────────────────────────────────
        IQR stands for Interquartile Range.
        It is the most widely used outlier detection method in production
        because it does not assume the data has a specific distribution (unlike Z-score).

        Picture the data sorted from smallest to largest:
          Q1 = 25th percentile (the value where 25% of data falls below)
          Q3 = 75th percentile (the value where 75% of data falls below)
          IQR = Q3 - Q1  (the range of the middle 50% of the data)

        TUKEY'S FENCES (the standard definition of "outlier"):
          Lower fence = Q1 - 1.5 × IQR
          Upper fence = Q3 + 1.5 × IQR

          Any value BELOW the lower fence OR ABOVE the upper fence is an outlier.

        WHY 1.5?
        ─────────
        John Tukey chose 1.5 in 1977. It captures roughly 99.3% of normally
        distributed data inside the fences, leaving only the extreme 0.7%
        as outliers. It has been the standard ever since.
        """

        # Get numeric columns (we only apply IQR to numbers)
        numeric_cols = self.df.select_dtypes(include=["number"]).columns.tolist()

        # We apply outlier detection to the first 3 numeric columns.
        # In a real project you would choose specific important columns.
        # Here we cap at 3 to keep the output manageable for teaching.
        outlier_flag_columns = []

        for col in numeric_cols[:3]:   # first 3 numeric columns

            # Compute the quartiles
            Q1  = self.df[col].quantile(0.25)   # 25th percentile
            Q3  = self.df[col].quantile(0.75)   # 75th percentile
            IQR = Q3 - Q1                       # interquartile range

            # Compute Tukey's fences
            lower_fence = Q1 - 1.5 * IQR
            upper_fence = Q3 + 1.5 * IQR

            # Create the flag column name
            flag_col = f"{col}_is_outlier"

            # (self.df[col] < lower_fence) → True where value is below lower fence
            # (self.df[col] > upper_fence) → True where value is above upper fence
            # The | operator combines them with OR logic
            # → True if the value is an outlier by EITHER condition
            self.df[flag_col] = (
                (self.df[col] < lower_fence) | (self.df[col] > upper_fence)
            )

            outlier_flag_columns.append(flag_col)

            self._record_change(
                f"Added outlier flag '{flag_col}' "
                f"(bounds: {lower_fence:.2f} to {upper_fence:.2f})"
            )

        # Combined flag: True if this row is an outlier in ANY of the 3 checked columns
        # .any(axis=1) checks across columns (axis=1 means "for each row, check across columns")
        if outlier_flag_columns:
            self.df["is_any_outlier"] = self.df[outlier_flag_columns].any(axis=1)
            n_outliers = int(self.df["is_any_outlier"].sum())
            self._record_change(
                f"Added 'is_any_outlier': {n_outliers} rows flagged "
                f"({round(n_outliers / len(self.df) * 100, 1)}% of dataset)"
            )

        logger.info(
            f"[TRANSFORM] add_derived_columns: "
            f"{len(outlier_flag_columns)} outlier flags added"
        )
        return self
    
    # ================================================================
    # STEP 5: ADD PIPELINE METADATA
    # ================================================================

    def add_metadata(self) -> "DataTransformer":
        """
        Stamp every row with information about when and how it was processed.

        WHY DOES METADATA MATTER?
        ──────────────────────────
        Imagine receiving a CSV file with no filename, no date, no source.
        You would not know:
          - Is this from this week's run or last month's?
          - Which industry schema did it come from?
          - Which version of the pipeline produced it?

        Without metadata, data has no PROVENANCE — no traceable origin.
        With metadata, every row carries its own documentation.

        DATA LINEAGE:
        ──────────────
        In data engineering, "lineage" means being able to trace any
        data point back to its source, through every transformation.
        Tools like Apache Atlas, DataHub, and dbt Lineage automate this.
        Our metadata columns implement a simple version of the same concept.

        PRACTICAL USES OF METADATA COLUMNS:
        ─────────────────────────────────────
        _processed_at:      "When did this pipeline run? Is this data fresh?"
        _industry:          "Which schema did this data come from?"
        _pipeline_version:  "If the results changed between runs, did the code change?"
        """

        # Which industry schema did this data come from?
        # INDUSTRY comes from config.py — "bootcamp_data" for the teaching project
        self.df["_industry"] = INDUSTRY

        # When was this transformation run?
        # datetime.datetime.now() returns the current date and time
        # .isoformat() converts it to a string: "2026-01-15T10:23:01.456789"
        # ISO 8601 format is the international standard for timestamps
        self.df["_processed_at"] = datetime.datetime.now().isoformat()

        # Which version of the pipeline code produced this output?
        # Increment this when you make breaking changes to the transformation logic
        self.df["_pipeline_version"] = "1.0.0"

        self._record_change(
            "Added metadata columns: _industry, _processed_at, _pipeline_version"
        )
        logger.info("[TRANSFORM] add_metadata complete")

        return self

    # ================================================================
    # HELPER METHODS
    # ================================================================

    def _record_change(self, message: str) -> None:
        """
        Record a transformation action in the audit trail and log it.

        All transformation methods call this to document what they changed.
        The audit trail (self.changes) is shown in the pipeline report.
        """
        self.changes.append(message)       # add to the list
        logger.info(f"[TRANSFORM] {message}")

    def summary(self) -> dict:
        """
        Return a summary of all transformations applied.
        Called by ETLPipeline.report() to build the final report.
        """
        return {
            "original_rows":   self.original_len,
            "final_rows":      len(self.df),
            "rows_removed":    self.original_len - len(self.df),
            "final_columns":   len(self.df.columns),
            "changes_count":   len(self.changes),
            "change_log":      self.changes,
        }
    
     # ================================================================
    # DUNDER METHODS
    # ================================================================

    def __str__(self) -> str:
        """Human-readable summary — shown by print(transformer)."""
        return (
            f"DataTransformer("
            f"{self.original_len:,} → {len(self.df):,} rows | "
            f"{len(self.changes)} changes applied)"
        )

    def __repr__(self) -> str:
        """Developer representation — shown in debugger."""
        return (
            f"DataTransformer("
            f"original={self.original_len}, "
            f"final={len(self.df)}, "
            f"changes={len(self.changes)})"
        )
    
# ================================================================
# QUICK SELF-TEST
# ================================================================
if __name__ == "__main__":
    import pandas as pd

    print("Running DataTransformer self-test...")
    print("=" * 50)

    # Create a messy test DataFrame
    test_df = pd.DataFrame({
        "employee_id": [1, 2, 3, 4, 5, 3],   # row 5 is a duplicate of row 2
        "name":        ["Alice", "Bob", "Carol", "Dave", "Eve", "Carol"],
        "billed_amount":      [50000, None, 70000, -500, 90000, 70000],  # null + negative
        "department":  ["Eng", "Sales", "Eng", "HR", "Sales", "Eng"],
    })

    print(f"Before transformation: {len(test_df)} rows, {test_df.isna().sum().sum()} nulls")

    t = DataTransformer(test_df)
    (
        t
        .fill_nulls()
        .drop_duplicates()
        .fix_types()
        .add_derived_columns()
        .add_metadata()
    )

    print(f"After transformation:  {len(t.df)} rows, {t.df.isna().sum().sum()} nulls")
    print(f"Final columns: {t.df.columns.tolist()}")
    print(f"{t}")
    print("\nSelf-test complete.")






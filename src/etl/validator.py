
# src/etl/validator.py

import sys       # sys lets us modify Python's module search path
import pathlib   # pathlib gives us cross-platform file path tools

# sys.path is a list of folders Python looks in when you write "import something".
# By default it does not include our project root folder.
# This block walks UP the folder tree until it finds config.py,
# then adds that folder to sys.path so Python can find our config module.
_root = pathlib.Path(__file__).resolve().parent   # start at src/ folder
while not (_root / "config.py").exists() and _root != _root.parent:
    _root = _root.parent   # move up one level: src/ → teaching-project/ → module-05/ → ...
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))   # add project root to Python's search path

# Now we can import from our project's config.py
import pandas as pd            # pandas: the core Python data library
                               # pd is the conventional short alias — everyone uses pd
from config import logger, MAX_NULL_PERCENT, MAX_DUPLICATE_PERCENT
# logger              → our shared logging object (prints timestamped messages)
# MAX_NULL_PERCENT    → 50.0 (threshold for flagging a column CRITICAL)
# MAX_DUPLICATE_PERCENT → 5.0 (threshold for flagging duplicates CRITICAL)


# The ETLPipeline (etl_pipeline.py) will read these to make decisions.


class DataValidator:
    """
    Inspects a raw DataFrame and reports all data quality issues.

    DESIGN PRINCIPLE: Read-Only
    This class never modifies the data. Its only job is to look and report.
    Fixing data is DataTransformer's responsibility.

    This separation (inspect vs fix) comes from the
    Single Responsibility Principle — each class does ONE thing well.
    """

    # ── CLASS ATTRIBUTES ─────────────────────────────────────────────────
    # Class attributes are defined at the class level (not inside any method).
    # They are SHARED by all instances of this class.
    # Think of them as settings that apply to every DataValidator we ever create.
    #
    # We use the values from config.py (imported above) so they are consistent
    # across the whole project.
    MAX_NULL_PCT = MAX_NULL_PERCENT        # 50.0 — columns above this are CRITICAL
    MAX_DUP_PCT  = MAX_DUPLICATE_PERCENT   # 5.0  — duplication above this is CRITICAL

    def __init__(self, df: pd.DataFrame):
        """
        __init__ is the constructor — it runs automatically when you create an object.
        When you write:
            validator = DataValidator(my_dataframe)
        Python calls __init__(self, my_dataframe) for you.

        'self' refers to this specific instance being created.
        It is how a method accesses the object's own data.

        The colon in (df: pd.DataFrame) is a TYPE HINT.
        It tells you and your teammates: "df should be a pandas DataFrame."
        Python does not enforce this — it is documentation for humans.

        Args:
            df    the raw DataFrame to inspect
        """

        # df.copy() creates an INDEPENDENT copy of the DataFrame in memory.
        # If we stored df directly (self.df = df), then any changes to self.df
        # would also change the original df in the caller's code.
        # That would be a very hard-to-find bug.
        # Copying is defensive programming — we protect the caller's data.
        self.df = df.copy()

        # self.issues is a list that starts empty and grows as we find problems.
        # Each problem is stored as a dictionary with keys: severity, column, message.
        # Example: {"severity": "WARNING", "column": "salary", "message": "23 nulls (1.9%)"}
        self.issues = []

        # self.stats is a dictionary that will hold summary statistics.
        # It is filled by compute_stats() and used by the pipeline's report.
        self.stats = {}

        # self._passed starts True (we assume the data is good).
        # If any CRITICAL issue is found, it flips to False.
        # The underscore prefix (_passed) is a Python convention meaning:
        #   "This is for internal use — please do not access it directly from outside."
        self._passed = True

    # ================================================================
    # THE 5 CHECK METHODS
    # ================================================================
    # Notice: every check method returns 'self' at the end.
    # This enables METHOD CHAINING — calling multiple methods in sequence:
    #   validator.check_not_empty().check_nulls().check_duplicates()
    # Without returning self, you would have to write:
    #   validator.check_not_empty()
    #   validator.check_nulls()
    #   validator.check_duplicates()
    # Both work. Method chaining reads more naturally for pipelines.
    # ================================================================

    def check_not_empty(self) -> "DataValidator":
        """
        Check 1: Does the DataFrame have any rows at all?

        Why this check exists:
        ─────────────────────────
        If the SQL query in extracted_raw_sql had a bug in the WHERE clause,
        it might return zero rows. Every downstream check on zero rows
        would either crash or produce meaningless results.
        We catch this immediately and stop early.

        "Fail fast" is an engineering principle:
        detect problems as early as possible, as close to the source as possible.

        Return type hint -> "DataValidator":
        The quotes around DataValidator are needed because we are inside
        the class definition — DataValidator is not fully defined yet
        when Python reads this line. The quotes tell Python:
        "this refers to the class we are currently defining."
        """

        # len() returns the number of rows in a DataFrame
        if len(self.df) == 0:

            # _add_issue() is our internal helper (defined below).
            # CRITICAL severity means the pipeline should stop.
            # This row-count problem affects the whole dataset, not just one column,
            # so we use "row_count" as the column name.
            self._add_issue(
                severity = "CRITICAL",
                column   = "row_count",
                message  = (
                    "DataFrame has 0 rows. "
                    "Check that Module 03 ran successfully and "
                    "that INDUSTRY in config.py is correct."
                )
            )

            # Flip the pass flag — this is a showstopper
            self._passed = False

        else:
            # f-string with :, formatting:
            # {len(self.df):,} formats the number with comma separators
            # e.g. 1200 → "1,200"   1000000 → "1,000,000"
            logger.info(f"[VALIDATE] Row count: {len(self.df):,} rows ✓")

        # Return self enables chaining: .check_not_empty().check_nulls()
        return self
    def check_nulls(self) -> "DataValidator":
        """
        Check 2: Which columns have missing (null/NaN) values?
        """
        # Columns that are legitimately null due to LEFT JOINs
        NULLABLE_COLUMNS = {
            "return_reason", "refund_amount", "return_status",
            "return_date", "return_id",
        }

        null_counts = self.df.isna().sum()

        for col, count in null_counts.items():

            if count == 0:
                continue

            if col in NULLABLE_COLUMNS:
                continue

            pct = round(count / len(self.df) * 100, 1)

            if pct > self.MAX_NULL_PCT:
                severity = "CRITICAL"
                self._passed = False
            else:
                severity = "WARNING"

            self._add_issue(
                severity = severity,
                column   = col,
                message  = f"{count:,} null values ({pct}% of rows)"
            )

        n_cols_with_nulls = int((null_counts > 0).sum())
        logger.info(
            f"[VALIDATE] Null check complete — "
            f"{n_cols_with_nulls} columns have null values"
        )

        return self


    def check_duplicates(self) -> "DataValidator":
        """
        Check 3: Are any rows exact copies of another row?

        WHAT IS A DUPLICATE ROW?
        ─────────────────────────
        A duplicate is a row where EVERY column value is identical to
        another row in the same DataFrame.

        HOW DO DUPLICATES ENTER PRODUCTION DATA?
        ──────────────────────────────────────────
        1. SQL JOIN ERRORS
           A JOIN between employees and departments where the join key is not unique
           creates multiple copies of each employee row — one per department match.

        2. PIPELINE RAN TWICE
           An ETL pipeline was triggered twice by a scheduling bug.
           The same records were inserted into the database twice.

        3. MANUAL DATA ENTRY
           Someone imported a spreadsheet twice.

        WHY DUPLICATES HURT YOUR ANALYSIS:
        ────────────────────────────────────
        If John Smith (salary £85,000) appears 3 times:
          - Your average salary calculation is biased upward
          - Your ML model sees John's data 3× — it overfits to his pattern
          - Your headcount report says 1,203 employees when there are 1,200
        """

        # df.duplicated() returns a boolean Series.
        # True for every row that is an EXACT copy of a previous row.
        # keep="first" means the FIRST occurrence is NOT flagged — only copies are.
        # .sum() counts the True values (number of duplicate rows)
        dup_count = int(self.df.duplicated(keep="first").sum())

        # Only record if there are any duplicates
        if dup_count > 0:

            # Calculate the percentage of rows that are duplicates
            dup_pct = round(dup_count / len(self.df) * 100, 1)

            # Classify severity
            if dup_pct > self.MAX_DUP_PCT:
                severity = "CRITICAL"
                self._passed = False
            else:
                severity = "WARNING"

            self._add_issue(
                severity = severity,
                column   = "duplicates",   # not a column — "duplicates" describes the issue type
                message  = (
                    f"{dup_count:,} exact duplicate rows "
                    f"({dup_pct}% of dataset)"
                )
            )
        else:
            logger.info("[VALIDATE] Duplicate check: 0 duplicate rows found ✓")

        return self

    def check_numeric_ranges(self) -> "DataValidator":
        """
        Check 4: Do numeric columns have values that are logically impossible?

        EXAMPLES OF IMPOSSIBLE VALUES:
        ────────────────────────────────
          salary    = -5000    → Can a person earn negative money?
          quantity  = -3       → Can you sell a negative number of items?
          gpa       = -1.5     → GPA ranges from 0 to 4.0
          efficiency= -20%     → Production efficiency cannot be negative

        WHERE DO IMPOSSIBLE VALUES COME FROM?
        ──────────────────────────────────────
        - Sign errors in calculations (subtraction stored in the wrong column)
        - Unit mismatches (kilometres entered where metres were expected)
        - Data entry errors (typo: -85000 instead of 85000)
        - Bad SQL: (old_value - new_value) stored instead of (new_value - old_value)

        IMPORTANT — SOME NEGATIVES ARE VALID:
        ───────────────────────────────────────
        "Difference" columns legitimately go negative:
          salary_vs_dept_avg = -5000 means this person earns £5k below department average
          mom_change = -1000 means revenue fell by £1k compared to last month
          price_gap = -2000 means the sale price was £2k below the asking price

        We skip these columns because negative values are expected there.
        """

        # These column names represent differences/deltas — negatives are expected
        DELTA_COLUMNS = {
            "salary_vs_dept_avg", "vs_dept_avg", "price_gap",
            "balance_vs_type_avg", "fee_vs_type_avg", "amt_vs_category_avg",
            "unit_variance", "efficiency_vs_plant_avg", "mom_change",
            "vs_customer_avg", "price_vs_market_avg", "refund_amount",
        }

        # select_dtypes(include=["number"]) returns ONLY numeric columns.
        # This automatically skips text columns like "department" or "email"
        # where you cannot meaningfully check for negatives.
        for col in self.df.select_dtypes(include=["number"]).columns:

            if col in DELTA_COLUMNS:
                continue   # skip — negatives are expected in this column

            # Count how many values in this column are less than zero
            # (self.df[col] < 0) creates a boolean Series: True where value < 0
            # .sum() counts the True values
            neg_count = int((self.df[col] < 0).sum())

            if neg_count > 0:
                self._add_issue(
                    severity = "WARNING",
                    column   = col,
                    message  = (
                        f"{neg_count} unexpected negative values. "
                        f"Check if '{col}' should always be positive."
                    )
                )

        return self

    def compute_stats(self) -> "DataValidator":
        """
        Check 5 (not really a check — a summary): Compute dataset statistics.

        This gathers key facts about the dataset in one dictionary.
        These stats are used by:
          - ETLPipeline.report() to show what we received
          - Module 06 EDA engine as a starting profile
          - Module 14 MLOps monitor as the baseline for drift detection

        WHAT IS A DICTIONARY?
        ──────────────────────
        A dictionary (dict) maps keys to values:
            my_dict = {"name": "Kwame", "salary": 92000}
            my_dict["salary"]  → 92000

        self.stats is a dict where each key is a metric name
        and each value is the measured value for that metric.
        """

        # Count numeric columns by using select_dtypes to filter
        # .columns gives us the column names as an Index object
        # len() counts how many there are
        num_col_count = len(self.df.select_dtypes(include=["number"]).columns)
        txt_col_count = len(self.df.select_dtypes(include=["object"]).columns)
        boo_col_count = len(self.df.select_dtypes(include=["bool"]).columns)

        self.stats = {
            "rows":            len(self.df),             # total rows in the dataset
            "columns":         len(self.df.columns),     # total number of columns
            "numeric_cols":    num_col_count,            # how many are numbers
            "text_cols":       txt_col_count,            # how many are text/categories
            "bool_cols":       boo_col_count,            # how many are True/False flags

            # self.df.isna() → DataFrame of True/False
            # .sum() → count nulls per column (Series)
            # .sum() again → grand total nulls across ALL cells
            "total_nulls":     int(self.df.isna().sum().sum()),

            # Null percentage across the whole dataset
            # self.df.size = total number of cells (rows × columns)
            "null_pct":        round(
                                   self.df.isna().sum().sum() / self.df.size * 100, 2
                               ),

            # Exact duplicate rows
            "duplicates":      int(self.df.duplicated().sum()),

            # Memory usage in megabytes
            # memory_usage(deep=True) calculates the ACTUAL memory used
            # deep=True counts object (text) columns accurately
            # Without deep=True, object columns are underestimated
            # / 1024**2 converts bytes to megabytes (1 MB = 1024 × 1024 bytes)
            "memory_mb":       round(
                                   self.df.memory_usage(deep=True).sum() / 1024**2, 2
                               ),

            # How many issues were found in total
            "total_issues":    len(self.issues),

            # How many were CRITICAL vs WARNING
            "critical_count":  sum(
                                   1 for i in self.issues if i["severity"] == "CRITICAL"
                               ),
            "warning_count":   sum(
                                   1 for i in self.issues if i["severity"] == "WARNING"
                               ),

            # Overall pass/fail
            "passed":          self._passed,
        }

        logger.info(
            f"[VALIDATE] Stats computed: "
            f"{self.stats['rows']:,} rows | "
            f"{self.stats['total_nulls']:,} nulls | "
            f"{self.stats['total_issues']} issues found | "
            f"result: {'PASSED ✓' if self._passed else 'FAILED ✗'}"
        )

        return self

    # ================================================================
    # PRIVATE HELPER METHOD
    # ================================================================
    # The underscore prefix (_add_issue) signals: "internal use only."
    # Users of DataValidator should not call this directly.
    # It is a helper used by the five check methods above.
    # ================================================================

    def _add_issue(self, severity: str, column: str, message: str) -> None:
        """
        Record one data quality issue in self.issues.

        Also logs it at the appropriate level so it appears in the terminal.

        Args:
            severity  "CRITICAL" or "WARNING"
            column    which column the issue affects (or "row_count"/"duplicates")
            message   human-readable description of the problem
        """

        # Build the issue as a dictionary — structured data is easier to process
        # than an unstructured string
        issue = {
            "severity": severity,   # "CRITICAL" or "WARNING"
            "column":   column,     # which column
            "message":  message,    # what is wrong
        }

        # Append adds this dictionary to the END of self.issues list
        self.issues.append(issue)

        # Log at the appropriate level
        # CRITICAL issues use logger.error() → shows in red in coloured terminals
        # WARNING issues use logger.warning() → shows in yellow
        if severity == "CRITICAL":
            logger.error(f"[VALIDATE] CRITICAL | {column} | {message}")
        else:
            logger.warning(f"[VALIDATE] WARNING  | {column} | {message}")

    # ================================================================
    # DUNDER (MAGIC) METHODS
    # ================================================================
    # Methods starting and ending with double underscores (__) are special.
    # Python calls them automatically in specific situations.
    # __str__ is called when you use print() on an object.
    # __repr__ is called in the debugger and interactive console.
    # ================================================================

    def __str__(self) -> str:
        """
        Called automatically when you write print(validator).
        Should return a SHORT, human-readable summary.
        """
        status = "PASSED ✓" if self._passed else "FAILED ✗"
        return (
            f"DataValidator("
            f"result={status} | "
            f"{len(self.issues)} issues found | "
            f"{len(self.df):,} rows inspected)"
        )

    def __repr__(self) -> str:
        """
        Called in the Python REPL and debugger.
        Should return a string that shows the object's key state.
        """
        return (
            f"DataValidator("
            f"rows={len(self.df):,}, "
            f"issues={len(self.issues)}, "
            f"passed={self._passed})"
        )


# ================================================================
# QUICK SELF-TEST
# ================================================================
# This block only runs when you execute this file DIRECTLY:
#   python src/validator.py
# It does NOT run when another file imports DataValidator.
# This is a great way to test a class in isolation.
# ================================================================

if __name__ == "__main__":
    # Create some test data — a small fake DataFrame
    import pandas as pd

    print("Running DataValidator self-test...")
    print("=" * 50)

    # Test 1: Clean data — should pass
    clean_df = pd.DataFrame({
        "employee_id": [1, 2, 3, 4, 5],
        "name":        ["Alice", "Bob", "Carol", "Dave", "Eve"],
        "salary":      [50000, 60000, 70000, 80000, 90000],
        "department":  ["Eng", "Sales", "Eng", "HR", "Sales"],
    })
    v1 = DataValidator(clean_df)
    v1.check_not_empty().check_nulls().check_duplicates().compute_stats()
    print("Test 1 (clean data): {v1}")

    # Test 2: Data with nulls — should warn
    dirty_df = clean_df.copy()
    dirty_df.loc[0, "salary"] = None   # introduce one null
    v2 = DataValidator(dirty_df)
    v2.check_not_empty().check_nulls().check_duplicates().compute_stats()
    print("Test 2 (with null): {v2}")

    # Test 3: Empty data — should FAIL
    empty_df = pd.DataFrame()
    v3 = DataValidator(empty_df)
    v3.check_not_empty()
    print("Test 3 (empty):     {v3}")

    print("\nSelf-test complete.")

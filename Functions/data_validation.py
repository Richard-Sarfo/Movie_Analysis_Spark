"""Data validation module for the TMDB movie analysis pipeline.

This module provides functions to validate raw and transformed movie data,
ensuring data quality before it enters downstream analytics and visualization.
Validation covers schema conformance, null/missing-value checks, value-range
constraints, and type correctness.

Functions:
    validate_schema: Verify a DataFrame matches the expected schema.
    validate_not_null: Check that required columns contain no null values.
    validate_value_ranges: Assert numeric columns fall within expected bounds.
    validate_no_duplicates: Ensure no duplicate rows exist on a key column.
    run_all_validations: Execute the full validation suite with a summary report.

Typical usage:
    >>> from Functions.data_validation import run_all_validations
    >>> report = run_all_validations(df)
    >>> if not report['passed']:
    ...     print("Validation failures:", report['failures'])
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

# Configure logger for data validation module
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    file_handler = logging.FileHandler('data_validation.log')
    file_handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def validate_schema(df, expected_schema):
    """Verify that a DataFrame's schema matches the expected schema.

    Compares column names and data types. Reports missing columns,
    unexpected extra columns, and type mismatches.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame to validate.
        expected_schema (pyspark.sql.types.StructType): The expected schema.

    Returns:
        dict: A result dict with keys:
            - 'check' (str): 'schema_match'
            - 'passed' (bool): True if schemas match.
            - 'missing_columns' (list[str]): Columns in expected but not in df.
            - 'extra_columns' (list[str]): Columns in df but not in expected.
            - 'type_mismatches' (list[dict]): Columns with mismatched types.
    """
    if not isinstance(df, DataFrame):
        raise TypeError("df must be a PySpark DataFrame.")
    if not isinstance(expected_schema, StructType):
        raise TypeError("expected_schema must be a PySpark StructType.")

    expected_fields = {f.name: f.dataType for f in expected_schema.fields}
    actual_fields = {f.name: f.dataType for f in df.schema.fields}

    missing = [c for c in expected_fields if c not in actual_fields]
    extra = [c for c in actual_fields if c not in expected_fields]
    type_mismatches = []

    for col_name in set(expected_fields) & set(actual_fields):
        if expected_fields[col_name] != actual_fields[col_name]:
            type_mismatches.append({
                'column': col_name,
                'expected': str(expected_fields[col_name]),
                'actual': str(actual_fields[col_name])
            })

    passed = not missing and not extra and not type_mismatches

    if passed:
        logger.info("Schema validation PASSED.")
    else:
        logger.warning(
            "Schema validation FAILED — missing: %s, extra: %s, type mismatches: %s",
            missing, extra, type_mismatches
        )

    return {
        'check': 'schema_match',
        'passed': passed,
        'missing_columns': missing,
        'extra_columns': extra,
        'type_mismatches': type_mismatches
    }


def validate_not_null(df, required_columns):
    """Check that specified columns contain no null values.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame to validate.
        required_columns (list[str]): Column names that must not be null.

    Returns:
        dict: A result dict with keys:
            - 'check' (str): 'not_null'
            - 'passed' (bool): True if no nulls are found.
            - 'null_counts' (dict[str, int]): Columns with their null counts
              (only columns with nulls > 0).
    """
    if not isinstance(df, DataFrame):
        raise TypeError("df must be a PySpark DataFrame.")

    null_counts = {}
    for col_name in required_columns:
        if col_name not in df.columns:
            logger.warning("Column '%s' not found in DataFrame — skipping null check.", col_name)
            continue
        count = df.filter(F.col(col_name).isNull()).count()
        if count > 0:
            null_counts[col_name] = count

    passed = len(null_counts) == 0

    if passed:
        logger.info("Null validation PASSED for columns: %s", required_columns)
    else:
        logger.warning("Null validation FAILED — null counts: %s", null_counts)

    return {
        'check': 'not_null',
        'passed': passed,
        'null_counts': null_counts
    }


def validate_value_ranges(df, range_rules):
    """Assert that numeric columns fall within specified bounds.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame to validate.
        range_rules (dict[str, dict]): A mapping of column names to
            range dicts with optional 'min' and 'max' keys.
            Example: {'budget': {'min': 0}, 'vote_average': {'min': 0, 'max': 10}}

    Returns:
        dict: A result dict with keys:
            - 'check' (str): 'value_ranges'
            - 'passed' (bool): True if all values are in range.
            - 'violations' (dict[str, dict]): Columns with out-of-range counts.
    """
    if not isinstance(df, DataFrame):
        raise TypeError("df must be a PySpark DataFrame.")

    violations = {}
    for col_name, bounds in range_rules.items():
        if col_name not in df.columns:
            logger.warning("Column '%s' not found in DataFrame — skipping range check.", col_name)
            continue

        col_violations = {}
        if 'min' in bounds:
            below_min = df.filter(F.col(col_name) < bounds['min']).count()
            if below_min > 0:
                col_violations['below_min'] = below_min

        if 'max' in bounds:
            above_max = df.filter(F.col(col_name) > bounds['max']).count()
            if above_max > 0:
                col_violations['above_max'] = above_max

        if col_violations:
            violations[col_name] = col_violations

    passed = len(violations) == 0

    if passed:
        logger.info("Value range validation PASSED.")
    else:
        logger.warning("Value range validation FAILED — violations: %s", violations)

    return {
        'check': 'value_ranges',
        'passed': passed,
        'violations': violations
    }


def validate_no_duplicates(df, key_column='id'):
    """Ensure no duplicate rows exist based on a key column.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame to validate.
        key_column (str): The column to check for duplicates. Default: 'id'.

    Returns:
        dict: A result dict with keys:
            - 'check' (str): 'no_duplicates'
            - 'passed' (bool): True if no duplicates found.
            - 'total_rows' (int): Total row count.
            - 'unique_rows' (int): Distinct row count on key_column.
            - 'duplicate_count' (int): Number of duplicate entries.
    """
    if not isinstance(df, DataFrame):
        raise TypeError("df must be a PySpark DataFrame.")
    if key_column not in df.columns:
        raise ValueError(f"Key column '{key_column}' not found in DataFrame.")

    total = df.count()
    unique = df.select(key_column).distinct().count()
    duplicates = total - unique

    passed = duplicates == 0

    if passed:
        logger.info("Duplicate validation PASSED — %d unique rows.", unique)
    else:
        logger.warning(
            "Duplicate validation FAILED — %d duplicates out of %d rows.",
            duplicates, total
        )

    return {
        'check': 'no_duplicates',
        'passed': passed,
        'total_rows': total,
        'unique_rows': unique,
        'duplicate_count': duplicates
    }


def run_all_validations(df, expected_schema=None):
    """Execute the full validation suite and return a consolidated report.

    Runs schema validation (if a schema is provided), null checks on
    critical columns, value-range constraints, and duplicate detection.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame to validate.
        expected_schema (pyspark.sql.types.StructType, optional):
            If provided, the DataFrame schema is checked against this.

    Returns:
        dict: A consolidated report with keys:
            - 'passed' (bool): True if ALL checks passed.
            - 'total_checks' (int): Number of checks executed.
            - 'passed_checks' (int): Number of checks that passed.
            - 'failed_checks' (int): Number of checks that failed.
            - 'results' (list[dict]): Individual check results.
            - 'failures' (list[dict]): Only the failed check results.

    Example:
        >>> from Functions.Schema import get_tmdb_raw_schema
        >>> report = run_all_validations(df, expected_schema=get_tmdb_raw_schema())
        >>> print(f"Passed: {report['passed']} ({report['passed_checks']}/{report['total_checks']})")
    """
    if not isinstance(df, DataFrame):
        raise TypeError("df must be a PySpark DataFrame.")

    results = []

    # 1. Schema validation (optional)
    if expected_schema is not None:
        results.append(validate_schema(df, expected_schema))

    # 2. Null checks on critical columns
    critical_columns = [c for c in ['id', 'title'] if c in df.columns]
    if critical_columns:
        results.append(validate_not_null(df, critical_columns))

    # 3. Value-range validation for numeric columns
    range_rules = {}
    if 'budget' in df.columns:
        range_rules['budget'] = {'min': 0}
    if 'revenue' in df.columns:
        range_rules['revenue'] = {'min': 0}
    if 'vote_average' in df.columns:
        range_rules['vote_average'] = {'min': 0, 'max': 10}
    if 'runtime' in df.columns:
        range_rules['runtime'] = {'min': 0}
    if 'popularity' in df.columns:
        range_rules['popularity'] = {'min': 0}

    if range_rules:
        results.append(validate_value_ranges(df, range_rules))

    # 4. Duplicate check
    if 'id' in df.columns:
        results.append(validate_no_duplicates(df, key_column='id'))

    # ---- Build summary ----
    passed_checks = sum(1 for r in results if r['passed'])
    failed_checks = sum(1 for r in results if not r['passed'])
    failures = [r for r in results if not r['passed']]

    overall_passed = failed_checks == 0

    logger.info(
        "Validation summary: %d/%d checks passed. Overall: %s",
        passed_checks, len(results), "PASSED" if overall_passed else "FAILED"
    )

    return {
        'passed': overall_passed,
        'total_checks': len(results),
        'passed_checks': passed_checks,
        'failed_checks': failed_checks,
        'results': results,
        'failures': failures
    }

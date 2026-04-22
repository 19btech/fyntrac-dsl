import React, { useState, useMemo, useCallback, useRef } from "react";
import {
  Box, Typography, TextField, Paper, IconButton, Chip, Tooltip,
  Popper, ClickAwayListener, InputAdornment, Divider,
  Dialog, DialogTitle, DialogContent,
} from "@mui/material";
import { FunctionSquare, X, Search, Variable } from "lucide-react";

/**
 * DSL functions organized into user-friendly categories with aliases.
 * Maps familiar spreadsheet/finance names to actual DSL function calls.
 */
const FORMULA_CATALOG = [
  // ── Arithmetic (15) ──────────────────────────────────────────────────
  { name: 'ADD', dsl: 'add', args: ['a', 'b'], desc: 'Add two values', category: 'Math', example: 'ADD(100, 200)' },
  { name: 'SUBTRACT', dsl: 'subtract', args: ['a', 'b'], desc: 'Subtract b from a', category: 'Math', example: 'SUBTRACT(1000, 250)' },
  { name: 'MULTIPLY', dsl: 'multiply', args: ['a', 'b'], desc: 'Multiply two values', category: 'Math', example: 'MULTIPLY(price, quantity)' },
  { name: 'DIVIDE', dsl: 'divide', args: ['a', 'b'], desc: 'Divide a by b (safe)', category: 'Math', example: 'DIVIDE(total, 12)' },
  { name: 'POWER', dsl: 'power', args: ['base', 'exp'], desc: 'Raise to power', category: 'Math', example: 'POWER(1.05, 12)' },
  { name: 'SQRT', dsl: 'sqrt', args: ['value'], desc: 'Square root', category: 'Math', example: 'SQRT(144)' },
  { name: 'ABS', dsl: 'abs', args: ['value'], desc: 'Absolute value', category: 'Math', example: 'ABS(-500)' },
  { name: 'SIGN', dsl: 'sign', args: ['value'], desc: 'Sign of value (-1, 0, 1)', category: 'Math', example: 'SIGN(-42)' },
  { name: 'ROUND', dsl: 'round', args: ['value', 'decimals'], desc: 'Round to N decimal places', category: 'Math', example: 'ROUND(3.14159, 2)' },
  { name: 'FLOOR', dsl: 'floor', args: ['value'], desc: 'Round down to integer', category: 'Math', example: 'FLOOR(3.7)' },
  { name: 'CEIL', dsl: 'ceil', args: ['value'], desc: 'Round up to integer', category: 'Math', example: 'CEIL(3.2)' },
  { name: 'MOD', dsl: 'mod', args: ['a', 'b'], desc: 'Modulus (remainder)', category: 'Math', example: 'MOD(10, 3)' },
  { name: 'TRUNCATE', dsl: 'truncate', args: ['value', 'decimals'], desc: 'Truncate decimal places', category: 'Math', example: 'TRUNCATE(3.789, 1)' },
  { name: 'PCT', dsl: 'percentage', args: ['value', 'total'], desc: 'Value as percentage of total', category: 'Math', example: 'PCT(25, 200)' },
  { name: 'CHANGE_PCT', dsl: 'change_pct', args: ['old_val', 'new_val'], desc: 'Percentage change between values', category: 'Math', example: 'CHANGE_PCT(100, 120)' },
  { name: 'PCT_OF', dsl: 'percentage_of', args: ['amount', 'pct'], desc: 'Percentage of an amount', category: 'Math', example: 'PCT_OF(10000, 5)' },
  { name: 'MIN', dsl: 'min', args: ['a', 'b'], desc: 'Minimum of two values', category: 'Math', example: 'MIN(a, b)' },
  { name: 'MAX', dsl: 'max', args: ['a', 'b'], desc: 'Maximum of two values', category: 'Math', example: 'MAX(a, b)' },

  // ── Financial (18) ───────────────────────────────────────────────────
  { name: 'PMT', dsl: 'pmt', args: ['rate', 'nper', 'pv'], desc: 'Payment amount (loan/annuity)', category: 'Financial', example: 'PMT(0.05/12, 360, 100000)' },
  { name: 'PV', dsl: 'pv', args: ['rate', 'nper', 'pmt'], desc: 'Present Value', category: 'Financial', example: 'PV(0.08, 10, 5000)' },
  { name: 'FV', dsl: 'fv', args: ['rate', 'nper', 'pmt'], desc: 'Future Value', category: 'Financial', example: 'FV(0.06, 20, 1000)' },
  { name: 'RATE', dsl: 'rate', args: ['nper', 'pmt', 'pv'], desc: 'Interest rate per period', category: 'Financial', example: 'RATE(360, -1500, 300000)' },
  { name: 'NPER', dsl: 'nper', args: ['rate', 'pmt', 'pv'], desc: 'Number of periods', category: 'Financial', example: 'NPER(0.005, -1500, 300000)' },
  { name: 'NPV', dsl: 'npv', args: ['rate', 'cashflows'], desc: 'Net Present Value', category: 'Financial', example: 'NPV(0.10, [1000, 2000, 3000])' },
  { name: 'IRR', dsl: 'irr', args: ['cashflows'], desc: 'Internal Rate of Return', category: 'Financial', example: 'IRR([-10000, 3000, 4000, 5000])' },
  { name: 'XNPV', dsl: 'xnpv', args: ['rate', 'cashflows', 'dates'], desc: 'NPV with specific dates', category: 'Financial', example: 'XNPV(0.10, cfs, dates)' },
  { name: 'XIRR', dsl: 'xirr', args: ['cashflows', 'dates'], desc: 'IRR with specific dates', category: 'Financial', example: 'XIRR(cfs, dates)' },
  { name: 'DISCOUNT', dsl: 'discount_factor', args: ['rate', 'periods'], desc: 'Discount factor', category: 'Financial', example: 'DISCOUNT(0.05, 10)' },
  { name: 'ACCUM', dsl: 'accumulation_factor', args: ['rate', 'periods'], desc: 'Accumulation factor', category: 'Financial', example: 'ACCUM(0.05, 10)' },
  { name: 'EFF_RATE', dsl: 'effective_rate', args: ['nominal', 'periods'], desc: 'Effective annual rate', category: 'Financial', example: 'EFF_RATE(0.06, 12)' },
  { name: 'NOM_RATE', dsl: 'nominal_rate', args: ['effective', 'periods'], desc: 'Nominal rate from effective', category: 'Financial', example: 'NOM_RATE(0.0617, 12)' },
  { name: 'YTM', dsl: 'yield_to_maturity', args: ['price', 'par', 'coupon', 'periods'], desc: 'Yield to maturity', category: 'Financial', example: 'YTM(950, 1000, 50, 10)' },
  { name: 'COMPOUND', dsl: 'compound_interest', args: ['principal', 'rate', 'periods'], desc: 'Compound interest', category: 'Financial', example: 'COMPOUND(10000, 0.05, 12)' },
  { name: 'INT_ON_BAL', dsl: 'interest_on_balance', args: ['balance', 'rate', 'days', 'year_days'], desc: 'Interest on balance for period', category: 'Financial', example: 'INT_ON_BAL(100000, 0.05, 30, 360)' },
  { name: 'CAPITALIZE', dsl: 'capitalization', args: ['principal', 'interest'], desc: 'Add interest to principal', category: 'Financial', example: 'CAPITALIZE(100000, 500)' },
  { name: 'AMORT_COST', dsl: 'amortized_cost', args: ['face', 'premium', 'periods', 'current'], desc: 'Amortized cost at period', category: 'Financial', example: 'AMORT_COST(1000, 50, 10, 3)' },

  // ── Depreciation (5) ────────────────────────────────────────────────
  { name: 'SLN', dsl: 'straight_line', args: ['cost', 'salvage', 'life'], desc: 'Straight-line depreciation', category: 'Depreciation', example: 'SLN(50000, 5000, 10)' },
  { name: 'DDB', dsl: 'double_declining', args: ['cost', 'salvage', 'life', 'period'], desc: 'Double declining balance', category: 'Depreciation', example: 'DDB(50000, 5000, 10, 1)' },
  { name: 'RDB', dsl: 'reducing_balance', args: ['cost', 'salvage', 'life', 'period'], desc: 'Reducing balance method', category: 'Depreciation', example: 'RDB(50000, 5000, 10, 1)' },
  { name: 'SYD', dsl: 'sum_of_years', args: ['cost', 'salvage', 'life', 'period'], desc: 'Sum-of-years digits', category: 'Depreciation', example: 'SYD(50000, 5000, 10, 1)' },
  { name: 'UOP', dsl: 'units_of_production', args: ['cost', 'salvage', 'total_units', 'used_units'], desc: 'Units of production', category: 'Depreciation', example: 'UOP(50000, 5000, 100000, 15000)' },

  // ── Allocation (5) ──────────────────────────────────────────────────
  { name: 'PRORATE', dsl: 'prorate', args: ['amount', 'days', 'total_days'], desc: 'Prorate amount over period', category: 'Allocation', example: 'PRORATE(1200, 15, 30)' },
  { name: 'ALLOCATE', dsl: 'allocate', args: ['amount', 'weights'], desc: 'Allocate proportionally', category: 'Allocation', example: 'ALLOCATE(1000, [0.5, 0.3, 0.2])' },
  { name: 'SPLIT', dsl: 'split', args: ['amount', 'parts'], desc: 'Split into equal parts', category: 'Allocation', example: 'SPLIT(1000, 3)' },
  { name: 'RATIO', dsl: 'ratio_split', args: ['amount', 'ratios'], desc: 'Split by ratio', category: 'Allocation', example: 'RATIO(1000, [3, 2, 1])' },

  // ── Balance (3) ──────────────────────────────────────────────────────
  { name: 'ROLL_BAL', dsl: 'rolling_balance', args: ['opening', 'debits', 'credits'], desc: 'Rolling balance calculation', category: 'Balance', example: 'ROLL_BAL(10000, 500, 200)' },
  { name: 'AVG_BAL', dsl: 'average_balance', args: ['balances'], desc: 'Average of balance array', category: 'Balance', example: 'AVG_BAL([10000, 9500, 9800])' },
  { name: 'WT_BAL', dsl: 'weighted_balance', args: ['balances', 'days'], desc: 'Time-weighted balance', category: 'Balance', example: 'WT_BAL([10000, 9500], [15, 15])' },

  // ── Date (19) ────────────────────────────────────────────────────────
  { name: 'DAYS', dsl: 'days_between', args: ['date1', 'date2'], desc: 'Days between two dates', category: 'Date', example: 'DAYS("2025-01-01", "2025-12-31")' },
  { name: 'MONTHS', dsl: 'months_between', args: ['date1', 'date2'], desc: 'Months between two dates', category: 'Date', example: 'MONTHS("2025-01-01", "2025-07-01")' },
  { name: 'YEARS', dsl: 'years_between', args: ['date1', 'date2'], desc: 'Years between two dates', category: 'Date', example: 'YEARS("2020-01-01", "2025-01-01")' },
  { name: 'ADD_DAYS', dsl: 'add_days', args: ['date', 'n'], desc: 'Add N days to date', category: 'Date', example: 'ADD_DAYS("2025-01-01", 30)' },
  { name: 'ADDMONTHS', dsl: 'add_months', args: ['date', 'n'], desc: 'Add N months to date', category: 'Date', example: 'ADDMONTHS("2025-01-01", 6)' },
  { name: 'ADD_YEARS', dsl: 'add_years', args: ['date', 'n'], desc: 'Add N years to date', category: 'Date', example: 'ADD_YEARS("2025-01-01", 5)' },
  { name: 'SUB_DAYS', dsl: 'subtract_days', args: ['date', 'n'], desc: 'Subtract N days', category: 'Date', example: 'SUB_DAYS("2025-06-15", 30)' },
  { name: 'SUB_MONTHS', dsl: 'subtract_months', args: ['date', 'n'], desc: 'Subtract N months', category: 'Date', example: 'SUB_MONTHS("2025-06-01", 3)' },
  { name: 'SUB_YEARS', dsl: 'subtract_years', args: ['date', 'n'], desc: 'Subtract N years', category: 'Date', example: 'SUB_YEARS("2025-01-01", 2)' },
  { name: 'SOM', dsl: 'start_of_month', args: ['date'], desc: 'First day of month', category: 'Date', example: 'SOM("2025-06-15")' },
  { name: 'EOM', dsl: 'end_of_month', args: ['date'], desc: 'Last day of month', category: 'Date', example: 'EOM("2025-02-15")' },
  { name: 'DCF', dsl: 'day_count_fraction', args: ['start', 'end', 'convention'], desc: 'Day count fraction (act/360 etc)', category: 'Date', example: 'DCF("2025-01-01", "2025-07-01", "act/360")' },
  { name: 'IS_LEAP', dsl: 'is_leap_year', args: ['date'], desc: 'Check if leap year', category: 'Date', example: 'IS_LEAP("2024-01-01")' },
  { name: 'DAYS_YR', dsl: 'days_in_year', args: ['date'], desc: 'Days in year (365 or 366)', category: 'Date', example: 'DAYS_YR("2024-01-01")' },
  { name: 'QUARTER', dsl: 'quarter', args: ['date'], desc: 'Quarter number (1-4)', category: 'Date', example: 'QUARTER("2025-08-15")' },
  { name: 'DOW', dsl: 'day_of_week', args: ['date'], desc: 'Day of week (0=Mon)', category: 'Date', example: 'DOW("2025-06-15")' },
  { name: 'IS_WKEND', dsl: 'is_weekend', args: ['date'], desc: 'Check if weekend', category: 'Date', example: 'IS_WKEND("2025-06-14")' },
  { name: 'BIZ_DAYS', dsl: 'business_days', args: ['start', 'end'], desc: 'Business days between dates', category: 'Date', example: 'BIZ_DAYS("2025-01-01", "2025-01-31")' },

  // ── Comparison (10) ──────────────────────────────────────────────────
  { name: 'EQ', dsl: 'eq', args: ['a', 'b'], desc: 'Equal to', category: 'Comparison', example: 'EQ(status, "active")' },
  { name: 'NEQ', dsl: 'neq', args: ['a', 'b'], desc: 'Not equal to', category: 'Comparison', example: 'NEQ(balance, 0)' },
  { name: 'GT', dsl: 'gt', args: ['a', 'b'], desc: 'Greater than', category: 'Comparison', example: 'GT(balance, 0)' },
  { name: 'GTE', dsl: 'gte', args: ['a', 'b'], desc: 'Greater than or equal', category: 'Comparison', example: 'GTE(amount, min_threshold)' },
  { name: 'LT', dsl: 'lt', args: ['a', 'b'], desc: 'Less than', category: 'Comparison', example: 'LT(remaining, 0)' },
  { name: 'LTE', dsl: 'lte', args: ['a', 'b'], desc: 'Less than or equal', category: 'Comparison', example: 'LTE(periods, max_periods)' },
  { name: 'BETWEEN', dsl: 'between', args: ['val', 'low', 'high'], desc: 'Value within range', category: 'Comparison', example: 'BETWEEN(rate, 0, 0.5)' },
  { name: 'IS_NULL', dsl: 'is_null', args: ['value'], desc: 'Check if null/None', category: 'Comparison', example: 'IS_NULL(override_rate)' },
  { name: 'IS_POS', dsl: 'is_positive', args: ['value'], desc: 'Check if positive', category: 'Comparison', example: 'IS_POS(balance)' },
  { name: 'IS_NEG', dsl: 'is_negative', args: ['value'], desc: 'Check if negative', category: 'Comparison', example: 'IS_NEG(pnl)' },

  // ── Logic (10) ───────────────────────────────────────────────────────
  { name: 'IF', dsl: 'if', args: ['condition', 'true_val', 'false_val'], desc: 'If/then/else', category: 'Logic', example: 'IF(gt(bal, 0), interest, 0)' },
  { name: 'AND', dsl: 'and', args: ['a', 'b'], desc: 'Logical AND', category: 'Logic', example: 'AND(is_positive(bal), lt(rate, 1))' },
  { name: 'OR', dsl: 'or', args: ['a', 'b'], desc: 'Logical OR', category: 'Logic', example: 'OR(eq(type, "A"), eq(type, "B"))' },
  { name: 'NOT', dsl: 'not', args: ['value'], desc: 'Logical NOT', category: 'Logic', example: 'NOT(is_null(amount))' },
  { name: 'XOR', dsl: 'xor', args: ['a', 'b'], desc: 'Exclusive OR', category: 'Logic', example: 'XOR(flag_a, flag_b)' },
  { name: 'ALL', dsl: 'all', args: ['array'], desc: 'All values truthy', category: 'Logic', example: 'ALL([cond1, cond2])' },
  { name: 'ANY', dsl: 'any', args: ['array'], desc: 'Any value truthy', category: 'Logic', example: 'ANY([cond1, cond2])' },
  { name: 'COALESCE', dsl: 'coalesce', args: ['val1', 'val2'], desc: 'First non-null value', category: 'Logic', example: 'COALESCE(override, default)' },
  { name: 'CLAMP', dsl: 'clamp', args: ['value', 'min', 'max'], desc: 'Constrain to range', category: 'Logic', example: 'CLAMP(rate, 0, 0.5)' },
  { name: 'SWITCH', dsl: 'switch', args: ['value', 'cases', 'default'], desc: 'Multi-case matching', category: 'Logic', example: 'SWITCH(type, {"A": 1, "B": 2}, 0)' },

  // ── Aggregation (13) ─────────────────────────────────────────────────
  { name: 'SUM', dsl: 'sum', args: ['array'], desc: 'Sum of array values', category: 'Aggregation', example: 'SUM([10, 20, 30])' },
  { name: 'SUM_FIELD', dsl: 'sum_field', args: ['records', 'field'], desc: 'Sum a field across records', category: 'Aggregation', example: 'SUM_FIELD(items, "amount")' },
  { name: 'AVG', dsl: 'avg', args: ['array'], desc: 'Average of values', category: 'Aggregation', example: 'AVG([10, 20, 30])' },
  { name: 'MINIMUM', dsl: 'min', args: ['array'], desc: 'Min value in array', category: 'Aggregation', example: 'MINIMUM([5, 3, 8])' },
  { name: 'MAXIMUM', dsl: 'max', args: ['array'], desc: 'Max value in array', category: 'Aggregation', example: 'MAXIMUM([5, 3, 8])' },
  { name: 'COUNT', dsl: 'count', args: ['array'], desc: 'Count items', category: 'Aggregation', example: 'COUNT(transactions)' },
  { name: 'WT_AVG', dsl: 'weighted_avg', args: ['values', 'weights'], desc: 'Weighted average', category: 'Aggregation', example: 'WT_AVG([10, 20], [0.6, 0.4])' },
  { name: 'CUM_SUM', dsl: 'cumulative_sum', args: ['array'], desc: 'Running total array', category: 'Aggregation', example: 'CUM_SUM([10, 20, 30])' },
  { name: 'MEDIAN', dsl: 'median', args: ['array'], desc: 'Median value', category: 'Aggregation', example: 'MEDIAN([10, 20, 30])' },
  { name: 'VARIANCE', dsl: 'variance', args: ['array'], desc: 'Variance', category: 'Aggregation', example: 'VARIANCE([10, 20, 30])' },
  { name: 'STD_DEV', dsl: 'std_dev', args: ['array'], desc: 'Standard deviation', category: 'Aggregation', example: 'STD_DEV([10, 20, 30])' },
  { name: 'PCTILE', dsl: 'percentile', args: ['array', 'pct'], desc: 'Percentile value', category: 'Aggregation', example: 'PCTILE([10, 20, 30, 40], 0.75)' },
  { name: 'RANGE', dsl: 'range', args: ['array'], desc: 'Max minus min', category: 'Aggregation', example: 'RANGE([10, 20, 30])' },

  // ── Conversion (6) ──────────────────────────────────────────────────
  { name: 'FX', dsl: 'fx_convert', args: ['amount', 'rate'], desc: 'Currency conversion', category: 'Conversion', example: 'FX(1000, 1.25)' },
  { name: 'NORMALIZE', dsl: 'normalize', args: ['value', 'min', 'max'], desc: 'Normalize to 0-1 range', category: 'Conversion', example: 'NORMALIZE(75, 0, 100)' },
  { name: 'BPS', dsl: 'basis_points', args: ['value'], desc: 'Convert to basis points', category: 'Conversion', example: 'BPS(0.0025)' },
  { name: 'FROM_BPS', dsl: 'from_bps', args: ['bps'], desc: 'Basis points to decimal', category: 'Conversion', example: 'FROM_BPS(25)' },
  { name: 'TO_PCT', dsl: 'to_percentage', args: ['decimal'], desc: 'Decimal to percentage', category: 'Conversion', example: 'TO_PCT(0.05)' },
  { name: 'FROM_PCT', dsl: 'from_percentage', args: ['pct'], desc: 'Percentage to decimal', category: 'Conversion', example: 'FROM_PCT(5)' },

  // ── Statistical (3) ──────────────────────────────────────────────────
  { name: 'CORREL', dsl: 'correlation', args: ['array_x', 'array_y'], desc: 'Correlation coefficient', category: 'Statistical', example: 'CORREL([1,2,3], [2,4,6])' },
  { name: 'COVAR', dsl: 'covariance', args: ['array_x', 'array_y'], desc: 'Covariance', category: 'Statistical', example: 'COVAR([1,2,3], [2,4,6])' },
  { name: 'ZSCORE', dsl: 'zscore', args: ['value', 'mean', 'std_dev'], desc: 'Z-score', category: 'Statistical', example: 'ZSCORE(85, 70, 10)' },

  // ── String (9) ───────────────────────────────────────────────────────
  { name: 'LOWER', dsl: 'lower', args: ['text'], desc: 'Convert to lowercase', category: 'String', example: 'LOWER("HELLO")' },
  { name: 'UPPER', dsl: 'upper', args: ['text'], desc: 'Convert to uppercase', category: 'String', example: 'UPPER("hello")' },
  { name: 'CONCAT', dsl: 'concat', args: ['a', 'b'], desc: 'Join text values', category: 'String', example: 'CONCAT("rate: ", rate)' },
  { name: 'CONTAINS', dsl: 'contains', args: ['text', 'search'], desc: 'Check if text contains', category: 'String', example: 'CONTAINS(name, "loan")' },
  { name: 'STARTS', dsl: 'starts_with', args: ['text', 'prefix'], desc: 'Starts with prefix', category: 'String', example: 'STARTS(code, "ASC")' },
  { name: 'ENDS', dsl: 'ends_with', args: ['text', 'suffix'], desc: 'Ends with suffix', category: 'String', example: 'ENDS(file, ".csv")' },
  { name: 'TRIM', dsl: 'trim', args: ['text'], desc: 'Remove whitespace', category: 'String', example: 'TRIM("  hello  ")' },
  { name: 'STR_LEN', dsl: 'str_length', args: ['text'], desc: 'String length', category: 'String', example: 'STR_LEN("hello")' },
  { name: 'EQ_CI', dsl: 'eq_ignore_case', args: ['a', 'b'], desc: 'Case-insensitive equality', category: 'String', example: 'EQ_CI("Hello", "hello")' },

  // ── Schedule (7) ─────────────────────────────────────────────────────
  { name: 'PERIOD', dsl: 'period', args: ['start', 'end', 'freq', 'convention?'], desc: 'Create period definition', category: 'Schedule', example: 'PERIOD("2025-01-01", "2025-12-31", "M")' },
  { name: 'SCHEDULE', dsl: 'schedule', args: ['period', 'columns', 'context?'], desc: 'Generate computed schedule', category: 'Schedule', example: 'SCHEDULE(p, {"col": "formula"}, ctx)' },
  { name: 'LAG', dsl: 'lag', args: ['column', 'offset', 'default'], desc: 'Previous row value', category: 'Schedule', example: "LAG('balance', 1, principal)" },
  { name: 'SCHED_SUM', dsl: 'schedule_sum', args: ['sched', 'col'], desc: 'Sum a schedule column', category: 'Schedule', example: 'SCHED_SUM(sched, "interest")' },
  { name: 'SCHED_FIRST', dsl: 'schedule_first', args: ['sched', 'col'], desc: 'First value of column', category: 'Schedule', example: 'SCHED_FIRST(sched, "balance")' },
  { name: 'SCHED_LAST', dsl: 'schedule_last', args: ['sched', 'col'], desc: 'Last value of column', category: 'Schedule', example: 'SCHED_LAST(sched, "balance")' },
  { name: 'SCHED_COL', dsl: 'schedule_column', args: ['sched', 'col'], desc: 'Extract column as array', category: 'Schedule', example: 'SCHED_COL(sched, "interest")' },
  { name: 'SCHED_FILTER', dsl: 'schedule_filter', args: ['sched', 'condition'], desc: 'Filter schedule rows', category: 'Schedule', example: 'SCHED_FILTER(sched, "gt(balance, 0)")' },

  // ── Array Operations (13) ────────────────────────────────────────────
  { name: 'FOR_EACH', dsl: 'for_each', args: ['dates', 'amounts', 'date_var', 'amount_var', 'expr'], desc: 'Iterate paired arrays', category: 'Array', example: 'FOR_EACH(dates, amts, "d", "a", "expr")' },
  { name: 'FOR_IDX', dsl: 'for_each_with_index', args: ['array', 'var', 'idx_var', 'expr'], desc: 'Iterate with index', category: 'Array', example: 'FOR_IDX(arr, "v", "i", "expr")' },
  { name: 'MAP', dsl: 'map_array', args: ['array', 'var', 'expr', 'context?'], desc: 'Transform each element', category: 'Array', example: 'MAP(amounts, "x", "multiply(x, 1.1)")' },
  { name: 'ZIP', dsl: 'zip_arrays', args: ['a', 'b'], desc: 'Zip two arrays', category: 'Array', example: 'ZIP(dates, amounts)' },
  { name: 'ARR_LEN', dsl: 'array_length', args: ['array'], desc: 'Array length', category: 'Array', example: 'ARR_LEN(items)' },
  { name: 'ARR_GET', dsl: 'array_get', args: ['array', 'index'], desc: 'Get element at index', category: 'Array', example: 'ARR_GET(items, 0)' },
  { name: 'ARR_FIRST', dsl: 'array_first', args: ['array'], desc: 'First element', category: 'Array', example: 'ARR_FIRST(payments)' },
  { name: 'ARR_LAST', dsl: 'array_last', args: ['array'], desc: 'Last element', category: 'Array', example: 'ARR_LAST(payments)' },
  { name: 'ARR_SLICE', dsl: 'array_slice', args: ['array', 'start', 'end'], desc: 'Sub-array slice', category: 'Array', example: 'ARR_SLICE(items, 0, 5)' },
  { name: 'ARR_REV', dsl: 'array_reverse', args: ['array'], desc: 'Reverse array', category: 'Array', example: 'ARR_REV(items)' },
  { name: 'ARR_PUSH', dsl: 'array_append', args: ['array', 'value'], desc: 'Append to array', category: 'Array', example: 'ARR_PUSH(items, new_item)' },
  { name: 'ARR_EXTEND', dsl: 'array_extend', args: ['array', 'other'], desc: 'Extend with another array', category: 'Array', example: 'ARR_EXTEND(arr1, arr2)' },
  { name: 'ARR_FILTER', dsl: 'array_filter', args: ['array', 'var', 'condition'], desc: 'Filter by condition', category: 'Array', example: 'ARR_FILTER(items, "x", "gt(x, 0)")' },

  // ── Collect (Event Data) (5) ────────────────────────────────
  { name: 'COLLECT_INSTR', dsl: 'collect_by_instrument', args: ['EVENT.field'], desc: 'Collect by instrument ID', category: 'Collect', example: 'COLLECT_INSTR(LoanEvent.rate)' },
  { name: 'COLLECT_ALL', dsl: 'collect_all', args: ['EVENT.field'], desc: 'Collect all values (all dates)', category: 'Collect', example: 'COLLECT_ALL(LoanEvent.amount)' },
  { name: 'COLLECT_SUB', dsl: 'collect_by_subinstrument', args: ['EVENT.field'], desc: 'Collect by sub-instrument', category: 'Collect', example: 'COLLECT_SUB(LoanEvent.tranche)' },
  { name: 'COLLECT_SUBIDS', dsl: 'collect_subinstrumentids', args: ['EVENT'], desc: 'Get unique sub-instrument IDs', category: 'Collect', example: 'COLLECT_SUBIDS(LoanEvent)' },
  { name: 'COLLECT_DATES', dsl: 'collect_effectivedates_for_subinstrument', args: ['EVENT', 'sub_id'], desc: 'Effective dates for sub-instrument', category: 'Collect', example: 'COLLECT_DATES(LoanEvent, "T001")' },

  // ── Transaction & Output (2) ─────────────────────────────────────────
  { name: 'TXN', dsl: 'createTransaction', args: ['posting_date', 'effective_date', 'type', 'amount'], desc: 'Create transaction', category: 'Transaction', example: 'TXN("2025-01-31", "2025-01-31", "Interest", 500)' },
  { name: 'PRINT', dsl: 'print', args: ['value'], desc: 'Print to console', category: 'Transaction', example: 'PRINT("Total:", total)' },

  // ── Lookup (1) ───────────────────────────────────────────────────────
  { name: 'LOOKUP', dsl: 'lookup', args: ['array', 'key', 'value_field'], desc: 'Look up value in records', category: 'Lookup', example: 'LOOKUP(rates, "term", "rate")' },
];

const CATEGORIES = [...new Set(FORMULA_CATALOG.map(f => f.category))];

const FunctionTooltip = ({ func }) => (
  <Box sx={{ p: 1, maxWidth: 280 }}>
    <Typography variant="body2" fontWeight={600}>{func.name}({func.args.join(', ')})</Typography>
    <Typography variant="caption" color="text.secondary">{func.desc}</Typography>
    <Divider sx={{ my: 0.5 }} />
    <Typography variant="caption" fontFamily="monospace" color="#5B5FED">
      {func.example}
    </Typography>
    <Typography variant="caption" display="block" color="text.secondary" sx={{ mt: 0.5 }}>
      DSL: <code>{func.dsl}()</code>
    </Typography>
  </Box>
);

/**
 * FormulaBar — Excel-style formula input with function autocomplete,
 * category browsing modal, and inline keyword hints.
 */
const FormulaBar = ({ value, onChange, events, variables, label, placeholder }) => {
  const [showCatalog, setShowCatalog] = useState(false);
  const [catalogFilter, setCatalogFilter] = useState('');
  const [selectedCategory, setSelectedCategory] = useState(null);
  const inputRef = useRef(null);
  const anchorRef = useRef(null);

  // Inline autocomplete state
  const [hintAnchor, setHintAnchor] = useState(null);
  const [hintItems, setHintItems] = useState([]);
  const [hintIndex, setHintIndex] = useState(0);
  const [currentWord, setCurrentWord] = useState('');

  const filteredFunctions = useMemo(() => {
    let list = FORMULA_CATALOG;
    if (selectedCategory) list = list.filter(f => f.category === selectedCategory);
    if (catalogFilter) {
      const lower = catalogFilter.toLowerCase();
      list = list.filter(f => f.name.toLowerCase().includes(lower) || f.desc.toLowerCase().includes(lower) || f.dsl.toLowerCase().includes(lower));
    }
    return list;
  }, [selectedCategory, catalogFilter]);

  const eventFields = useMemo(() => {
    if (!events || events.length === 0) return [];
    const result = [];
    events.forEach(event => {
      ['postingdate', 'effectivedate', 'subinstrumentid'].forEach(sf => {
        result.push(`${event.event_name}.${sf}`);
      });
      event.fields?.forEach(f => result.push(`${event.event_name}.${f.name}`));
    });
    return result;
  }, [events]);

  const variableNames = useMemo(() => {
    if (!variables || variables.length === 0) return [];
    return variables.filter(v => typeof v === 'string' ? v : v?.name).map(v => typeof v === 'string' ? v : v.name);
  }, [variables]);

  // Filter variables and event fields by catalog search too
  const filteredVariableNames = useMemo(() => {
    if (!catalogFilter) return variableNames;
    const lower = catalogFilter.toLowerCase();
    return variableNames.filter(v => v.toLowerCase().includes(lower));
  }, [variableNames, catalogFilter]);

  const filteredEventFields = useMemo(() => {
    if (!catalogFilter) return eventFields;
    const lower = catalogFilter.toLowerCase();
    return eventFields.filter(ef => ef.toLowerCase().includes(lower));
  }, [eventFields, catalogFilter]);

  // Build a flat hint list: formulas, event fields, variables
  const allHints = useMemo(() => {
    const hints = [];
    FORMULA_CATALOG.forEach(f => {
      hints.push({ label: f.name, secondary: f.desc, type: 'function', insert: `${f.dsl}(${f.args.join(', ')})` });
      if (f.dsl !== f.name.toLowerCase()) {
        hints.push({ label: f.dsl, secondary: f.desc, type: 'function', insert: `${f.dsl}(${f.args.join(', ')})` });
      }
    });
    eventFields.forEach(ef => {
      hints.push({ label: ef, secondary: 'Event field', type: 'field', insert: ef });
    });
    variableNames.forEach(v => {
      hints.push({ label: v, secondary: 'Variable', type: 'variable', insert: v });
    });
    return hints;
  }, [eventFields, variableNames]);

  // Extract the word being typed at cursor
  const getWordAtCursor = useCallback((text, cursorPos) => {
    if (!text || cursorPos === 0) return '';
    const before = text.slice(0, cursorPos);
    const match = before.match(/[A-Za-z_][A-Za-z0-9_.]*$/);
    return match ? match[0] : '';
  }, []);

  // Handle input changes and inline hint matching
  const handleInputChange = useCallback((e) => {
    const newVal = e.target.value;
    onChange(newVal);

    const cursorPos = e.target.selectionStart;
    const word = getWordAtCursor(newVal, cursorPos);
    setCurrentWord(word);

    if (word.length >= 2) {
      const lower = word.toLowerCase();
      const matches = allHints.filter(h => h.label.toLowerCase().includes(lower)).slice(0, 8);
      if (matches.length > 0) {
        setHintItems(matches);
        setHintIndex(0);
        setHintAnchor(inputRef.current);
        return;
      }
    }
    setHintItems([]);
    setHintAnchor(null);
  }, [onChange, getWordAtCursor, allHints]);

  // Insert a hint, replacing the current word
  const applyHint = useCallback((hint) => {
    const text = value || '';
    const input = inputRef.current;
    const cursorPos = input?.selectionStart || text.length;
    const before = text.slice(0, cursorPos);
    const after = text.slice(cursorPos);
    const wordStart = before.length - currentWord.length;
    const newText = before.slice(0, wordStart) + hint.insert + after;
    onChange(newText);
    setHintItems([]);
    setHintAnchor(null);

    // Re-focus and set cursor
    setTimeout(() => {
      if (input) {
        input.focus();
        const newPos = wordStart + hint.insert.length;
        input.setSelectionRange(newPos, newPos);
      }
    }, 0);
  }, [value, onChange, currentWord]);

  // Handle keyboard navigation in hints
  const handleKeyDown = useCallback((e) => {
    if (hintItems.length === 0) return;
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setHintIndex(i => Math.min(i + 1, hintItems.length - 1));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setHintIndex(i => Math.max(i - 1, 0));
    } else if (e.key === 'Enter' || e.key === 'Tab') {
      e.preventDefault();
      applyHint(hintItems[hintIndex]);
    } else if (e.key === 'Escape') {
      setHintItems([]);
      setHintAnchor(null);
    }
  }, [hintItems, hintIndex, applyHint]);

  const insertFunction = useCallback((func) => {
    const snippet = `${func.dsl}(${func.args.join(', ')})`;
    const current = value || '';
    onChange(current + snippet);
    setShowCatalog(false);
    inputRef.current?.focus();
  }, [value, onChange]);

  const insertText = useCallback((text) => {
    onChange((value || '') + text);
    setShowCatalog(false);
    inputRef.current?.focus();
  }, [value, onChange]);

  return (
    <Box ref={anchorRef} sx={{ position: 'relative' }}>
      <TextField
        inputRef={inputRef}
        size="small" fullWidth
        label={label || 'Formula'}
        placeholder={placeholder || 'Type a formula or click 🔍 to browse functions...'}
        value={value || ''}
        onChange={handleInputChange}
        onKeyDown={handleKeyDown}
        InputProps={{
          startAdornment: (
            <InputAdornment position="start">
              <Typography variant="body2" fontWeight={700} color="#5B5FED" sx={{ fontStyle: 'italic' }}>fx</Typography>
            </InputAdornment>
          ),
          endAdornment: (
            <InputAdornment position="end">
              <Tooltip title="Browse functions">
                <IconButton size="small" onClick={() => { setShowCatalog(true); setCatalogFilter(''); setSelectedCategory(null); }}>
                  <Search size={16} />
                </IconButton>
              </Tooltip>
            </InputAdornment>
          ),
          sx: { fontFamily: 'monospace', fontSize: '0.875rem' },
        }}
      />

      {/* Inline autocomplete hints */}
      <Popper open={hintItems.length > 0 && Boolean(hintAnchor)} anchorEl={hintAnchor} placement="bottom-start" sx={{ zIndex: 1500 }}>
        <ClickAwayListener onClickAway={() => { setHintItems([]); setHintAnchor(null); }}>
          <Paper elevation={6} sx={{ maxHeight: 220, overflow: 'auto', minWidth: 260, maxWidth: 400, mt: 0.5, border: '1px solid #E9ECEF', borderRadius: 1.5 }}>
            {hintItems.map((hint, idx) => (
              <Box
                key={hint.label + hint.type}
                onClick={() => applyHint(hint)}
                sx={{
                  px: 1.5, py: 0.5, cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 1,
                  bgcolor: idx === hintIndex ? '#EEF0FE' : 'transparent',
                  '&:hover': { bgcolor: '#EEF0FE' },
                }}
              >
                {hint.type === 'function' && <FunctionSquare size={13} color="#5B5FED" />}
                {hint.type === 'field' && <Typography variant="caption" sx={{ color: '#e65100', fontWeight: 700, fontSize: '0.7rem' }}>EV</Typography>}
                {hint.type === 'variable' && <Variable size={13} color="#7b1fa2" />}
                <Box sx={{ flex: 1, minWidth: 0 }}>
                  <Typography variant="body2" fontFamily="monospace" fontSize="0.8rem" fontWeight={600} noWrap>
                    {hint.label}
                  </Typography>
                  <Typography variant="caption" color="text.secondary" noWrap>{hint.secondary}</Typography>
                </Box>
              </Box>
            ))}
          </Paper>
        </ClickAwayListener>
      </Popper>

      {/* Function catalog modal */}
      <Dialog open={showCatalog} onClose={() => setShowCatalog(false)} maxWidth="sm" fullWidth PaperProps={{ sx: { maxHeight: '80vh' } }}>
        <DialogTitle sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', pb: 1 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <FunctionSquare size={18} color="#5B5FED" />
            <Typography variant="subtitle1" fontWeight={700}>Function Browser</Typography>
          </Box>
          <IconButton size="small" onClick={() => setShowCatalog(false)}><X size={16} /></IconButton>
        </DialogTitle>
        <DialogContent sx={{ display: 'flex', flexDirection: 'column', gap: 1, px: 2, pb: 2, pt: '0 !important' }}>
          {/* Search */}
          <TextField
            size="small" fullWidth autoFocus
            placeholder="Search functions..."
            value={catalogFilter}
            onChange={(e) => setCatalogFilter(e.target.value)}
            InputProps={{
              startAdornment: <InputAdornment position="start"><Search size={14} /></InputAdornment>,
            }}
          />

          {/* Category chips */}
          <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
            <Chip size="small" label="All" variant={!selectedCategory ? 'filled' : 'outlined'}
              color={!selectedCategory ? 'primary' : 'default'}
              onClick={() => setSelectedCategory(null)} />
            {CATEGORIES.map(cat => (
              <Chip key={cat} size="small" label={cat}
                variant={selectedCategory === cat ? 'filled' : 'outlined'}
                color={selectedCategory === cat ? 'primary' : 'default'}
                onClick={() => setSelectedCategory(cat)} />
            ))}
          </Box>

          <Divider />

          {/* Function list */}
          <Box sx={{ flex: 1, overflow: 'auto', minHeight: 200 }}>
            {filteredFunctions.map((func) => (
              <Box
                key={func.name}
                onClick={() => insertFunction(func)}
                sx={{
                  px: 1.5, py: 0.75, cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 1,
                  '&:hover': { bgcolor: '#EEF0FE' }, borderRadius: 1,
                }}
              >
                <FunctionSquare size={14} color="#5B5FED" />
                <Box sx={{ flex: 1, minWidth: 0 }}>
                  <Typography variant="body2" fontWeight={600} fontFamily="monospace" fontSize="0.8125rem">
                    {func.name}(<Typography component="span" variant="body2" color="text.secondary" fontFamily="monospace" fontSize="0.75rem">
                      {func.args.join(', ')}
                    </Typography>)
                  </Typography>
                  <Typography variant="caption" color="text.secondary" noWrap>{func.desc}</Typography>
                </Box>
                <Chip size="small" label={func.category} sx={{ fontSize: '0.625rem', height: 18, pointerEvents: 'none' }} />
              </Box>
            ))}
            {filteredFunctions.length === 0 && (
              <Typography variant="body2" color="text.secondary" sx={{ p: 2, textAlign: 'center' }}>
                No matching functions
              </Typography>
            )}
          </Box>

          {/* Variables section */}
          {filteredVariableNames.length > 0 && (
            <>
              <Divider />
              <Box>
                <Typography variant="caption" fontWeight={600} color="text.secondary">
                  <Variable size={12} style={{ display: 'inline', verticalAlign: 'middle', marginRight: 4 }} />
                  Defined Variables ({filteredVariableNames.length})
                </Typography>
                <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap', mt: 0.5 }}>
                  {filteredVariableNames.map(v => (
                    <Chip key={v} size="small" label={v} variant="outlined" color="secondary"
                      onClick={() => insertText(v)}
                      sx={{ fontSize: '0.6875rem', cursor: 'pointer', fontFamily: 'monospace' }} />
                  ))}
                </Box>
              </Box>
            </>
          )}

          {/* Event fields section */}
          {filteredEventFields.length > 0 && (
            <>
              <Divider />
              <Box>
                <Typography variant="caption" fontWeight={600} color="text.secondary">Event Fields ({filteredEventFields.length})</Typography>
                <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap', mt: 0.5 }}>
                  {filteredEventFields.map(ef => (
                    <Chip key={ef} size="small" label={ef} variant="outlined"
                      onClick={() => insertText(ef)}
                      sx={{ fontSize: '0.6875rem', cursor: 'pointer' }} />
                  ))}
                </Box>
              </Box>
            </>
          )}
        </DialogContent>
      </Dialog>
    </Box>
  );
};

export { FORMULA_CATALOG, CATEGORIES };
export default FormulaBar;

# DSL Functions Reference

This file is a convenience reference for the formulas available in the Fyntrac DSL.
The **canonical implementation lives in `backend/dsl_functions.py`** (mirrored, byte-for-byte,
in `FyntracPythonModel/dsl_functions.py`). It is generated from the `DSL_FUNCTION_METADATA`
registry, so the names below are the exact identifiers you call in a rule.

**112 callable functions** are registered, organised into 13 categories below.

## Categories

- [Array Utilities](#array-utilities) — 9
- [Date](#date) — 20
- [Financial](#financial) — 14
- [Arithmetic](#arithmetic) — 12
- [Comparison](#comparison) — 8
- [Logical](#logical) — 8
- [Schedule](#schedule) — 7
- [Schedule (column-only)](#schedule-column-only) — 15
- [Aggregation](#aggregation) — 10
- [String](#string) — 7
- [Array](#array) — 4
- [Iteration](#iteration) — 4
- [Transaction](#transaction) — 1

## Array Utilities

| Function | Description | Example |
|---|---|---|
| `lookup(value_array, match_array, target_value)` | Search a list for a matching value and return the corresponding item from a second list. Returns null if no match is found. | `lookup(reference_balances, instrumentid)` |
| `array_length(array)` | Return the number of items in a list. | `array_length(balance_history)` |
| `array_get(array, index, default=None)` | Return the item at a specified position in a list, with a fallback value if the position is beyond the end of the list. | `array_get(history, i, 0)` |
| `array_first(array, default=None)` | Return the first item in a list, with an optional fallback value if the list is empty. | `array_first(balance_history, 0)` |
| `array_last(array, default=None)` | Return the last item in a list, with an optional fallback value if the list is empty. | `array_last(balance_history, 0)` |
| `array_slice(array, start, end=None)` | Extract a portion of a list from a starting position to an optional ending position. | `array_slice(balance_history, 0, 12)` |
| `array_reverse(array)` | Return a new list with all items in the reverse order. |  |
| `array_append(array, item)` | Return a new list with one additional item added to the end, without modifying the original list. |  |
| `array_extend(array, items)` | Return a new list formed by joining two lists together, without modifying the original. |  |

## Date

| Function | Description | Example |
|---|---|---|
| `normalize_arraydate(array)` | Convert a list of dates written in various formats into the standard YYYY-MM-DD format. |  |
| `days_between(d1, d2)` | Calculate the number of calendar days between two dates. | `days_between(LoanEvent.origination_date, postingdate)` |
| `months_between(d1, d2)` | Calculate the number of complete months between two dates. | `months_between(LoanEvent.origination_date, postingdate)` |
| `years_between(d1, d2)` | Calculate the number of complete years between two dates. |  |
| `add_days(d, n)` | Add a specified number of days to a date and return the resulting date. |  |
| `add_months(d, n)` | Add a specified number of months to a date and return the resulting date. | `add_months(postingdate, 1)` |
| `add_years(d, n)` | Add a specified number of years to a date and return the resulting date. |  |
| `subtract_days(d, n)` | Subtract a specified number of days from a date and return the resulting date. |  |
| `subtract_months(d, n)` | Subtract a specified number of months from a date and return the resulting date. |  |
| `subtract_years(d, n)` | Subtract a specified number of years from a date and return the resulting date. |  |
| `start_of_month(d)` | Return the first calendar day of the month for a given date. | `start_of_month(postingdate)` |
| `end_of_month(d)` | Return the last calendar day of the month for a given date. | `end_of_month(postingdate)` |
| `day_count_fraction(d1, d2, conv='ACT/360')` | Calculate the fraction of a year between two dates using a specified day count convention such as ACT/360 or ACT/365. | `day_count_fraction(prior_date, postingdate, "ACT/365")` |
| `is_leap_year(year)` | Determine whether a given year is a leap year. |  |
| `days_in_year(year)` | Return the total number of days in a given year — 365 for standard years and 366 for leap years. |  |
| `quarter(d)` | Return the calendar quarter (1 to 4) that a given date falls in. |  |
| `day_of_week(d)` | Return the day of the week for a date as a number, where 0 is Monday and 6 is Sunday. |  |
| `is_weekend(d)` | Check whether a given date falls on a Saturday or Sunday. |  |
| `normalize_date(date_value)` | Convert a date written in any common format to the standard YYYY-MM-DD format. | `normalize_date(LoanEvent.maturity_date)` |
| `business_days(d1, d2)` | Calculate the number of working days between two dates, excluding weekends. |  |

## Financial

| Function | Description | Example |
|---|---|---|
| `pv(rate, n, pmt, fv=0, type=0)` | The value today of a series of equal future payments at a fixed rate. Set type=1 if each payment is made at the start of the period. | `pv(divide(rate, 12), term, payment)` |
| `fv(rate, n, pmt, pv=0, type=0)` | The future value of regular payments that earn a fixed rate. Set type=1 if each payment is made at the start of the period. | `fv(divide(rate, 12), term, payment, principal)` |
| `pmt(rate, n, pv, fv=0, type=0)` | The fixed payment needed to pay off a loan or reach a savings goal over a set number of periods. | `pmt(divide(LoanEvent.rate, 12), LoanEvent.term_months, LoanEvent.principal)` |
| `rate(n, pmt, pv, fv=0, type=0, guess=0.1)` | The interest rate per period for a loan, based on the number of payments, the payment amount, and the loan amount. |  |
| `nper(rate, pmt, pv, fv=0, type=0)` | How many payment periods are needed to pay off a loan or reach a savings goal. |  |
| `npv(rate, cashflows)` | The net present value of a series of cash flows, discounted at a yearly rate (entered as a decimal). | `npv(0.05, cashflows)` |
| `irr(cashflows)` | The internal rate of return — the rate at which a series of cash flows has a net present value of zero. | `irr(cashflows)` |
| `xnpv(rate, cashflows, dates)` | Net present value of cash flows that fall on specific dates, using a 365-day year. |  |
| `xirr(cashflows, dates)` | Internal rate of return for cash flows that fall on specific dates. |  |
| `discount_factor(rate, dcf)` | The factor that converts a future amount into its value today, given a rate and a year fraction. | `discount_factor(rate, dcf)` |
| `accumulation_factor(rate, dcf)` | The factor that grows a present amount into its future value, given a rate and a year fraction. |  |
| `effective_rate(nominal, freq)` | Convert a nominal interest rate into the effective annual rate, based on how often it compounds per year. | `effective_rate(0.06, 12)` |
| `nominal_rate(effective, freq)` | Convert an effective annual rate back into a nominal rate, based on how often it compounds per year. |  |
| `yield_to_maturity(price, face, coupon, years)` | The approximate yield to maturity of a bond from its price, face value, coupon rate, and years left. |  |

## Arithmetic

| Function | Description | Example |
|---|---|---|
| `add(a, b)` | Add two numbers together. | `add(LoanEvent.principal, LoanEvent.fees)` |
| `subtract(a, b)` | Subtract the second number from the first. | `subtract(opening_balance, payment)` |
| `multiply(a, b)` | Multiply two numbers together. | `multiply(LoanEvent.principal, LoanEvent.rate)` |
| `divide(a, b)` | Divide the first number by the second. | `divide(annual_rate, 12)` |
| `power(a, b)` | Raise a number to the power of a given exponent. | `power(add(1, monthly_rate), nper)` |
| `abs(x)` | Return the absolute value of a number, removing any negative sign. | `abs(subtract(actual, expected))` |
| `sign(x)` | Return -1 if the number is negative, 0 if zero, or 1 if positive. |  |
| `round(x, n=0)` | Round a number to a specified number of decimal places. | `round(interest, 2)` |
| `floor(x)` | Round a number down to the nearest whole number. | `floor(divide(days, 30))` |
| `ceil(x)` | Round a number up to the nearest whole number. | `ceil(divide(amount, 1000))` |
| `truncate(x, decimals=0)` | Remove decimal places beyond a specified number of positions without any rounding. |  |
| `percentage(value, total)` | Calculate what percentage one number represents of a given total. | `percentage(paid_amount, total_due)` |

## Comparison

| Function | Description | Example |
|---|---|---|
| `eq(a, b)` | Check whether two values are equal. | `eq(stage, 1)` |
| `neq(a, b)` | Check whether two values are not equal. | `neq(status, "closed")` |
| `gt(a, b)` | Check whether the first value is greater than the second. | `gt(days_overdue, 90)` |
| `gte(a, b)` | Check whether the first value is greater than or equal to the second. | `gte(LoanEvent.balance, 1000)` |
| `lt(a, b)` | Check whether the first value is less than the second. | `lt(LoanEvent.rate, 0.05)` |
| `lte(a, b)` | Check whether the first value is less than or equal to the second. | `lte(ltv, 0.8)` |
| `between(x, l, u)` | Check whether a value falls within a specified lower and upper boundary, inclusive. | `between(days_overdue, 30, 89)` |
| `is_null(x)` | Check whether a value is empty or missing. | `is_null(LoanEvent.maturity_date)` |

## Logical

| Function | Description | Example |
|---|---|---|
| `and(a, b)` | Return true only if both conditions are true. | `and(gt(days_overdue, 30), lt(days_overdue, 90))` |
| `or(a, b)` | Return true if at least one of the two conditions is true. | `or(eq(stage, 2), eq(stage, 3))` |
| `not(a)` | Reverse a condition — returns true if the condition is false, and false if it is true. | `not(is_null(rating))` |
| `all(list)` | Return true only if every item in a list evaluates to true. |  |
| `any(list)` | Return true if at least one item in a list evaluates to true. |  |
| `if(cond, true_val, false_val)` | Return one of two values based on a condition — works like an IF statement in a spreadsheet. | `if(gt(days_overdue, 90), 3, if(gt(days_overdue, 30), 2, 1))` |
| `coalesce(*args)` | Return the first non-empty value from a list — useful for providing a fallback default when a value may be missing. | `coalesce(LoanEvent.override_rate, LoanEvent.rate, 0.0)` |
| `switch(value, cases, default)` | Look up a value against a set of named cases and return the matching result, or a default value if no match is found. | `switch(rating, {"A":0.005,"B":0.02,"C":0.08}, 0.15)` |

## Schedule

| Function | Description | Example |
|---|---|---|
| `schedule(period, columns)` | Build a time-based table with calculated columns — used for amortisation, accrual, revenue, or depreciation schedules. | `schedule(p, {"interest":"multiply(balance, monthly_rate)","principal":"subtract(payment, interest)","balance":"subtract(balance, principal)"}, {"balance":LoanEvent.principal,"monthly_rate":divide(LoanEvent.rate,12),"payment":pmt(divide(LoanEvent.rate,12),LoanEvent.term_months,LoanEvent.principal)})` |
| `period(start, end?, freq?, conv?)` | Set the time periods for a schedule: pass a start and end date with a frequency (M, Q, A, W, or D), or just a number of periods. | `period(LoanEvent.term_months, "M")` |
| `schedule_sum(schedule, column)` | Add up all values in a specified column of a generated schedule. | `schedule_sum(amort, "interest")` |
| `schedule_last(schedule, column)` | Retrieve the value from the last row of a specified column in a schedule. | `schedule_last(amort, "balance")` |
| `schedule_first(schedule, column)` | Retrieve the value from the first row of a specified column in a schedule. | `schedule_first(amort, "balance")` |
| `schedule_column(schedule, column)` | Return all values from a specified column of a schedule as a list. | `schedule_column(amort, "balance")` |
| `schedule_filter(schedule, match_column, match_value, return_column)` | Find the first row in a schedule where a column matches a given value and return the corresponding value from another column. | `schedule_filter(amort, "period_date", postingdate, "balance")` |

## Schedule (column-only)

These are read inside `schedule(...)` column expressions only.

| Token | Description |
|---|---|
| `lag(column_name, offset, default)` | Get a value from an earlier row of the schedule (offset rows back), or a default on the first rows. Used for running balances. |
| `period_date` | The current row's date (YYYY-MM-DD). |
| `period_index` | The current row's position, starting at 0. |
| `period_number` | The current row's period number, starting at 1. |
| `period_start` | The next period's start date (used for day-count fractions). |
| `total_periods` | The total number of rows in the schedule. |
| `dcf` | The day-count fraction for the current period. |
| `days_in_current_period` | The number of days in the current period. |
| `daily_basis` | The per-day basis amount for the current period. |
| `start_date` | The schedule's overall start date. |
| `end_date` | The schedule's overall end date. |
| `s_no` | The serial number of the current row. |
| `index` | Another name for the current row index. |
| `item_name` | The name of the current item (for per-item schedules). |
| `subinstrument_id` | The sub-instrument ID for the current schedule (for per-item schedules). |

## Aggregation

| Function | Description | Example |
|---|---|---|
| `sum(col)` | Add up all values in a list, ignoring any empty entries. | `sum(all_principals)` |
| `sum_field(array, field)` | Add up a specific named field from a list of records, treating any missing values as zero. | `sum_field(amort, "interest")` |
| `avg(col)` | Calculate the arithmetic average of a list of values. | `avg(rate_history)` |
| `min(col)` | Return the smallest value from a list. | `min(balance_history)` |
| `max(col)` | Return the largest value from a list. | `max(balance_history)` |
| `count(col)` | Count the number of items in a list. | `count(payment_history)` |
| `weighted_avg(v, w)` | Calculate the average of a list of values, where each value is weighted by a corresponding weight factor. | `weighted_avg(prices, weights)` |
| `cumulative_sum(col)` | Calculate the running total of a list, returning a new list where each entry is the accumulated sum up to that point. | `cumulative_sum(monthly_amounts)` |
| `median(col)` | Return the middle value of a sorted list — half the values fall above and half fall below. |  |
| `std_dev(col)` | Measure how spread out the values in a list are around the average, expressed on the same scale as the values. |  |

## String

| Function | Description | Example |
|---|---|---|
| `lower(s)` | Convert all characters in a text value to lowercase. | `lower(rating)` |
| `upper(s)` | Convert all characters in a text value to uppercase. |  |
| `concat(s1, s2, ...)` | Join two or more text values together into a single combined string. | `concat("Loan_", instrumentid)` |
| `contains(s, substring)` | Check whether a piece of text contains a specific word or phrase. | `contains(LoanEvent.notes, "impaired")` |
| `eq_ignore_case(a, b)` | Check whether two text values are equal, ignoring any differences in upper or lower case. | `eq_ignore_case(status, "ACTIVE")` |
| `trim(s)` | Remove any extra spaces from the beginning and end of a text value. |  |
| `str_length(s)` | Return the number of characters in a text value. |  |

## Array

| Function | Description | Example |
|---|---|---|
| `collect_by_instrument(EVENT.field)` | Gather all values of an event field for the current instrument across all dates into a single list. | `collect_by_instrument("EOD_BALANCES.upb")` |
| `collect_all(EVENT.field)` | Gather every value of an event field across all rows in the dataset without any filtering. | `collect_all("PDCurve.pd")` |
| `collect_by_subinstrument(EVENT.field)` | Gather all values of an event field for a specific instrument and sub-instrument combination. | `collect_by_subinstrument("REV.amount")` |
| `collect_effectivedates_for_subinstrument(subinstrument_id?)` | Return a list of all effective dates associated with a specified sub-instrument. |  |

## Iteration

| Function | Description | Example |
|---|---|---|
| `apply_each(array, expression)` | Run a formula on every item in a list and return the results. For paired lists, pass two arrays and use 'first' and 'second'. | `apply_each(prices, "divide(each, total_price)")` |
| `for_each(dates_arr, amounts_arr, date_var, amt_var, expr)` | Loop over two paired lists (dates and amounts), running an action for each pair. Often used to create transactions. | `for_each(dates, amounts, "d", "a", "createTransaction(d, d, \"REV\", a)")` |
| `for_each_with_index(array, var_name, expression, context?)` | Loop through a list, making each item and its position number available inside the loop body. | `for_each_with_index(items, "x", "multiply(x, weight)")` |
| `array_filter(array, var_name, condition, context?)` | Return a new list containing only the items from the original list that meet a specified condition. | `array_filter(items, "x", "gt(x, 0)")` |

## Transaction

| Function | Description | Example |
|---|---|---|
| `createTransaction(postingdate, effectivedate, transactiontype, amount, subinstrumentid='1')` | Record a financial transaction with a posting date, effective date, transaction type, and amount. The sub-instrument ID defaults to '1' if not provided. | `createTransaction(postingdate, effectivedate, "ECLAllowance", ecl_amount)` |

## Operators & utilities

These are also available but are not listed in the tables above:

- **Arithmetic / comparison operators** — you may write the symbols `+ - * / ^ == != < > <= >=` directly in expressions; internally they map to `op_add`, `op_sub`, `op_mul`, `op_div`, `op_eq`, `op_neq`, `op_lt`, `op_gt`, `op_lte`, `op_gte`. The named forms (`add`, `subtract`, `multiply`, `divide`, `power`, `eq`, …) are documented under [Arithmetic](#arithmetic) and [Comparison](#comparison).
- **`iif(cond, true_val, false_val)`** — alias of [`if`](#logical).
- **`print(value)`** — write a value to the run console output (useful for testing/debugging a rule).

## Common identifiers

Inside rule expressions you also have access to the current event/context fields, e.g. `EventName.field` (a referenced event field), `postingdate`, `effectivedate`, and `instrumentid`.

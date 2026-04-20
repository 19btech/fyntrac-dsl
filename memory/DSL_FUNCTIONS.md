# DSL Functions Reference

This file is a convenience reference only. The canonical implementation lives in `backend/dsl_functions.py`.

| Function | Description |
|---|---|
| _clear_print_outputs | Clear the print outputs list |
| _clear_transaction_results | Clear the transaction results list |
| _coerce_n_to_int | Coerce numeric-like values to int. |
| _dsl_print | Internal print that uses the DSL print function if set, otherwise appends to _print_outputs |
| _get_current_instrumentid | Get the current instrumentid |
| _get_print_outputs | Get the global print outputs list |
| _get_transaction_results | Get the global transaction results list |
| _in_schedule_eval | Return True if we are currently evaluating a schedule's column expressions. |
| _set_current_instrumentid | Set the current instrumentid for transactions |
| _set_dsl_print | Set the DSL print function (called from server. |
| _set_print_outputs | Set the global print outputs list (called from server. |
| _set_transaction_results | Set the global transaction results list (called from server. |
| abs_val | (no docstring) |
| accumulation_factor | Growth factor |
| add | (no docstring) |
| add_days | (no docstring) |
| add_months | Add n months to a date, handling month-end dates properly |
| add_years | Add n years to a date, handling leap year dates properly |
| all_op | (no docstring) |
| allocate | Weight-based allocation |
| amortized_cost | Balance after payment |
| and_op | (no docstring) |
| any_op | (no docstring) |
| array_filter | Filter array elements based on a condition. |
| array_first | Get first element of array. |
| array_get | Get element at index with optional default for out-of-bounds. |
| array_last | Get last element of array. |
| array_length | Get the length of an array. |
| array_reverse | Reverse an array. |
| array_slice | Get slice of array from start to end index. |
| average_balance | Average of balances |
| avg | (no docstring) |
| basis_points | (no docstring) |
| between | (no docstring) |
| business_days | Count business days |
| capitalization | Add interest to principal |
| ceil | (no docstring) |
| change_pct | Percentage change |
| clamp | (no docstring) |
| coalesce | (no docstring) |
| compound_interest | Compound interest |
| concat | Concatenate multiple strings |
| contains | Check if string contains substring |
| correlation | Pearson correlation coefficient |
| count | (no docstring) |
| covariance | Covariance between two lists |
| createTransaction | Create a transaction with all required fields. |
| create_schedule_transactions | Create transactions from schedule recognition results. |
| cumulative_sum | (no docstring) |
| day_count_fraction | Year fraction using DCC |
| day_of_week | Day of week (0=Monday, 6=Sunday) |
| days_between | (no docstring) |
| days_in_year | Days in year |
| discount_factor | Discount factor |
| divide | (no docstring) |
| double_declining | Double declining balance |
| dsl_print | Expose print functionality to DSL (safe wrapper). |
| effective_rate | Nominal to effective rate |
| end_of_month | (no docstring) |
| ends_with | Check if string ends with suffix |
| eq | (no docstring) |
| eq_ignore_case | Case-insensitive string equality |
| find_period_amounts | Find the period amounts for each item based on posting date. |
| floor | (no docstring) |
| for_each | Iterate over paired arrays and execute an expression for each pair. |
| for_each_with_index | Iterate over a single array and execute an expression for each element. |
| from_bps | (no docstring) |
| from_percentage | Convert percentage to decimal |
| fv | Calculates future value  Args:     rate: Interest rate per period     n: Number of periods     pmt: Payment per period     pv: Present value (default 0)     type: 0 = payment at end of period (default), 1 = payment at beginning |
| fx_convert | (no docstring) |
| generate_schedules | Generate schedules for multiple items - FULLY GENERIC. |
| get_schedule_totals | Extract the totals from generate_schedules results. |
| get_schedules_array | Extract just the schedule arrays from generate_schedules results. |
| gt | (no docstring) |
| gte | (no docstring) |
| if_op | (no docstring) |
| interest_on_balance | Interest using ACT/360 |
| irr | Internal rate of return using Newton-Raphson method. |
| is_leap_year | Check leap year |
| is_negative | Check if negative |
| is_null | (no docstring) |
| is_positive | Check if positive |
| is_weekend | Check if weekend |
| lookup | Retrieve a value from value_array by matching an element in match_array to target_value. |
| lower | Convert string to lowercase |
| lt | (no docstring) |
| lte | (no docstring) |
| map_array | Transform each element of an array using an expression. |
| max_val | (no docstring) |
| median | (no docstring) |
| min_val | (no docstring) |
| mod | (no docstring) |
| months_between | (no docstring) |
| multiply | (no docstring) |
| neq | (no docstring) |
| nominal_rate | Effective to nominal rate |
| normalize | (no docstring) |
| normalize_arraydate | Normalize all date values in an array to system-standard format (yyyy-mm-dd). |
| normalize_date | Normalize a date value to YYYY-MM-DD string format. |
| not_op | (no docstring) |
| nper | Calculate number of periods  Args:     rate: Interest rate per period     pmt: Payment per period     pv: Present value     fv: Future value (default 0)     type: 0 = payment at end of period (default), 1 = payment at beginning  Returns:     Number of periods (can be fractional) |
| npv | Net present value |
| op_add | (no docstring) |
| op_div | (no docstring) |
| op_eq | (no docstring) |
| op_gt | (no docstring) |
| op_gte | (no docstring) |
| op_lt | (no docstring) |
| op_lte | (no docstring) |
| op_mul | (no docstring) |
| op_neq | (no docstring) |
| op_sub | (no docstring) |
| or_op | (no docstring) |
| percentage | Calculate percentage of total |
| percentage_of | Calculate percentage |
| percentile | Calculate percentile |
| period | Creates a period definition for schedule generation. |
| pmt | Fixed periodic payment  Args:     rate: Interest rate per period     n: Number of periods     pv: Present value     fv: Future value (default 0)     type: 0 = payment at end of period (default), 1 = payment at beginning |
| power | (no docstring) |
| print_all_schedules | Print all schedules from generate_schedules results or schedule arrays. |
| print_schedule | Print a schedule as a formatted table in the console. |
| prorate | Proportional allocation |
| pv | Calculates present value of future cash flows  Args:     rate: Interest rate per period     n: Number of periods     pmt: Payment per period     fv: Future value (default 0)     type: 0 = payment at end of period (default), 1 = payment at beginning |
| quarter | Get quarter from date |
| range_val | Range of values |
| rate | Calculate interest rate per period  Solves the equation: 0 = pv + pmt*(1+rate*type)*[(1+rate)^n - 1]/rate + fv/(1+rate)^n Uses Newton-Raphson method. |
| ratio_split | Split by ratios |
| reducing_balance | Declining balance |
| rolling_balance | Running balance |
| round_val | (no docstring) |
| safe_eval_expression | Evaluate a DSL expression string in a restricted context. |
| schedule | Creates a deterministic time-based schedule (table). |
| schedule_column | Return the values of a column from a schedule. |
| schedule_filter | For each schedule (or schedule result), find the first row where `row[match_column] == match_value` and return the value from `return_column` for that row. |
| schedule_first | Get the first value of a column in a schedule |
| schedule_last | Get the last value of a column in a schedule |
| schedule_sum | Sum a column from a schedule |
| sign | (no docstring) |
| split | Equal split |
| sqrt | (no docstring) |
| start_of_month | (no docstring) |
| starts_with | Check if string starts with prefix |
| std_dev | (no docstring) |
| str_length | Get string length |
| straight_line | Straight-line depreciation |
| subtract | (no docstring) |
| subtract_days | Subtract n days from a date |
| subtract_months | Subtract n months from a date, handling month-end dates properly |
| subtract_years | Subtract n years from a date, handling leap year dates properly |
| sum_field | Sum a specific field from an array of objects/dictionaries. |
| sum_of_years | Sum of years digits |
| sum_vals | (no docstring) |
| switch | Switch-case logic |
| to_number | Coerce various input types to a numeric value. |
| to_percentage | Convert decimal to percentage |
| trim | Remove leading and trailing whitespace |
| truncate | Truncate to decimals |
| units_of_production | Usage-based depreciation |
| upper | Convert string to uppercase |
| variance | (no docstring) |
| weighted_avg | (no docstring) |
| weighted_balance | Weighted average balance |
| xirr | IRR with specific dates (matches Excel XIRR)  Args:     cashflows: List of cash flows     dates: List of dates (ISO format: YYYY-MM-DD)     guess: Initial guess (default 0. |
| xnpv | NPV with specific dates (matches Excel XNPV)  Args:     rate: Discount rate     cashflows: List of cash flows     dates: List of dates (ISO format: YYYY-MM-DD)  Note: Uses 365 days per year (Excel convention), not 365. |
| xor | (no docstring) |
| years_between | (no docstring) |
| yield_to_maturity | YTM approximation |
| zip_arrays | Combine multiple arrays into array of tuples/lists. |
| zscore | Z-score |

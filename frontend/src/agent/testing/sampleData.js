/**
 * sampleData.js — Test cases for all 145 DSL functions.
 * Each entry: { name, category, dsl, description, expectedType }
 *   - dsl: valid DSL code string to execute via POST /api/dsl/run
 *   - description: plain English input/output description
 *   - expectedType: 'number' | 'string' | 'boolean' | 'array' | 'object' | 'null' | 'any'
 *
 * 6 collect_* functions require event data context and are marked with requiresEvents: true.
 */

const SAMPLE_DATA = [
  // ── Financial (18) ───────────────────────────────────────────
  { name: 'pv', category: 'Financial', dsl: 'print(pv(0.05, 10, -1000))', description: 'Present value of $1,000/yr for 10 years at 5%', expectedType: 'number' },
  { name: 'fv', category: 'Financial', dsl: 'print(fv(0.05, 10, -1000))', description: 'Future value of $1,000/yr for 10 years at 5%', expectedType: 'number' },
  { name: 'pmt', category: 'Financial', dsl: 'print(pmt(0.05, 10, -50000))', description: 'Annual payment for $50,000 loan at 5% over 10 years', expectedType: 'number' },
  { name: 'rate', category: 'Financial', dsl: 'print(rate(10, -1000, 7722))', description: 'Interest rate for 10 periods, $1,000 payment, $7,722 PV', expectedType: 'number' },
  { name: 'nper', category: 'Financial', dsl: 'print(nper(0.05, -1000, 7722))', description: 'Number of periods at 5% with $1,000 payment and $7,722 PV', expectedType: 'number' },
  { name: 'npv', category: 'Financial', dsl: 'print(npv(0.1, [-1000, 300, 400, 500, 600]))', description: 'Net present value of cash flows at 10% discount rate', expectedType: 'number' },
  { name: 'irr', category: 'Financial', dsl: 'print(irr([-1000, 300, 400, 500, 600]))', description: 'Internal rate of return for investment cash flows', expectedType: 'number' },
  { name: 'xnpv', category: 'Financial', dsl: 'print(xnpv(0.1, [-1000, 300, 400, 500], ["2024-01-01", "2024-06-01", "2025-01-01", "2025-06-01"]))', description: 'NPV with specific dates at 10%', expectedType: 'number' },
  { name: 'xirr', category: 'Financial', dsl: 'print(xirr([-1000, 300, 400, 500], ["2024-01-01", "2024-06-01", "2025-01-01", "2025-06-01"]))', description: 'IRR with specific dates', expectedType: 'number' },
  { name: 'discount_factor', category: 'Financial', dsl: 'print(discount_factor(0.05, 1.0))', description: 'Discount factor at 5% for 1 year', expectedType: 'number' },
  { name: 'accumulation_factor', category: 'Financial', dsl: 'print(accumulation_factor(0.05, 1.0))', description: 'Growth factor at 5% for 1 year', expectedType: 'number' },
  { name: 'effective_rate', category: 'Financial', dsl: 'print(effective_rate(0.12, 12))', description: 'Effective annual rate from 12% nominal compounded monthly', expectedType: 'number' },
  { name: 'nominal_rate', category: 'Financial', dsl: 'print(nominal_rate(0.1268, 12))', description: 'Nominal rate from ~12.68% effective compounded monthly', expectedType: 'number' },
  { name: 'yield_to_maturity', category: 'Financial', dsl: 'print(yield_to_maturity(950, 1000, 50, 10))', description: 'YTM for bond at $950, $1000 face, $50 coupon, 10 years', expectedType: 'number' },
  { name: 'compound_interest', category: 'Financial', dsl: 'print(compound_interest(10000, 0.05, 5))', description: 'Compound interest on $10,000 at 5% for 5 periods', expectedType: 'number' },
  { name: 'interest_on_balance', category: 'Financial', dsl: 'print(interest_on_balance(100000, 0.05, 90))', description: 'Interest on $100,000 at 5% for 90 days (ACT/360)', expectedType: 'number' },
  { name: 'capitalization', category: 'Financial', dsl: 'print(capitalization(500, 10000))', description: 'Add $500 interest to $10,000 principal', expectedType: 'number' },
  { name: 'amortized_cost', category: 'Financial', dsl: 'print(amortized_cost(10000, 500, 1200))', description: 'Balance after: opening $10,000 + $500 interest - $1,200 payment', expectedType: 'number' },

  // ── Depreciation (5) ─────────────────────────────────────────
  { name: 'straight_line', category: 'Depreciation', dsl: 'print(straight_line(10000, 1000, 5))', description: 'Straight-line depreciation: $10,000 cost, $1,000 salvage, 5-year life', expectedType: 'number' },
  { name: 'reducing_balance', category: 'Depreciation', dsl: 'print(reducing_balance(10000, 0.2))', description: 'Reducing balance: $10,000 cost at 20% rate', expectedType: 'number' },
  { name: 'double_declining', category: 'Depreciation', dsl: 'print(double_declining(10000, 5))', description: 'Double declining: $10,000 cost, 5-year life', expectedType: 'number' },
  { name: 'sum_of_years', category: 'Depreciation', dsl: 'print(sum_of_years(10000, 1000, 5, 1))', description: 'Sum of years digits: $10,000 cost, $1,000 salvage, 5-year life, year 1', expectedType: 'number' },
  { name: 'units_of_production', category: 'Depreciation', dsl: 'print(units_of_production(10000, 2000, 10000))', description: 'Units of production: $10,000 cost, 2,000 units produced of 10,000 total', expectedType: 'number' },

  // ── Allocation (5) ───────────────────────────────────────────
  { name: 'prorate', category: 'Allocation', dsl: 'print(prorate(1000, 3, 12))', description: 'Prorate $1,000: 3 parts out of 12 total', expectedType: 'number' },
  { name: 'allocate', category: 'Allocation', dsl: 'print(allocate(1000, [0.5, 0.3, 0.2]))', description: 'Allocate $1,000 by weights [50%, 30%, 20%]', expectedType: 'array' },
  { name: 'split', category: 'Allocation', dsl: 'print(split(1000, 3))', description: 'Split $1,000 equally into 3 parts', expectedType: 'array' },
  { name: 'percentage_of', category: 'Allocation', dsl: 'print(percentage_of(500, 15))', description: '15% of $500', expectedType: 'number' },
  { name: 'ratio_split', category: 'Allocation', dsl: 'print(ratio_split(1000, [2, 3, 5]))', description: 'Split $1,000 by ratio 2:3:5', expectedType: 'array' },

  // ── Balance (3) ──────────────────────────────────────────────
  { name: 'rolling_balance', category: 'Balance', dsl: 'print(rolling_balance(1000, [100, -200, 300, -50]))', description: 'Running balance from $1,000 with flows [+100, -200, +300, -50]', expectedType: 'array' },
  { name: 'average_balance', category: 'Balance', dsl: 'print(average_balance([1000, 1100, 900, 1200, 1150]))', description: 'Average of monthly balances', expectedType: 'number' },
  { name: 'weighted_balance', category: 'Balance', dsl: 'print(weighted_balance([1000, 1100, 900], [30, 31, 29]))', description: 'Weighted average balance by days in period', expectedType: 'number' },

  // ── Arithmetic (15) ──────────────────────────────────────────
  { name: 'add', category: 'Arithmetic', dsl: 'print(add(100, 250))', description: 'Add 100 + 250', expectedType: 'number' },
  { name: 'subtract', category: 'Arithmetic', dsl: 'print(subtract(500, 175))', description: 'Subtract 500 - 175', expectedType: 'number' },
  { name: 'multiply', category: 'Arithmetic', dsl: 'print(multiply(25, 40))', description: 'Multiply 25 × 40', expectedType: 'number' },
  { name: 'divide', category: 'Arithmetic', dsl: 'print(divide(1000, 8))', description: 'Divide 1,000 ÷ 8', expectedType: 'number' },
  { name: 'power', category: 'Arithmetic', dsl: 'print(power(2, 10))', description: '2 raised to the 10th power', expectedType: 'number' },
  { name: 'sqrt', category: 'Arithmetic', dsl: 'print(sqrt(144))', description: 'Square root of 144', expectedType: 'number' },
  { name: 'abs', category: 'Arithmetic', dsl: 'print(abs(-42.5))', description: 'Absolute value of -42.5', expectedType: 'number' },
  { name: 'sign', category: 'Arithmetic', dsl: 'print(sign(-100))', description: 'Sign of -100 (returns -1, 0, or 1)', expectedType: 'number' },
  { name: 'round', category: 'Arithmetic', dsl: 'print(round(3.14159, 2))', description: 'Round 3.14159 to 2 decimal places', expectedType: 'number' },
  { name: 'floor', category: 'Arithmetic', dsl: 'print(floor(3.9))', description: 'Floor of 3.9 → 3', expectedType: 'number' },
  { name: 'ceil', category: 'Arithmetic', dsl: 'print(ceil(3.1))', description: 'Ceiling of 3.1 → 4', expectedType: 'number' },
  { name: 'mod', category: 'Arithmetic', dsl: 'print(mod(17, 5))', description: 'Remainder of 17 ÷ 5', expectedType: 'number' },
  { name: 'truncate', category: 'Arithmetic', dsl: 'print(truncate(3.7891, 2))', description: 'Truncate 3.7891 to 2 decimals', expectedType: 'number' },
  { name: 'percentage', category: 'Arithmetic', dsl: 'print(percentage(25, 200))', description: '25 as a percentage of 200', expectedType: 'number' },
  { name: 'change_pct', category: 'Arithmetic', dsl: 'print(change_pct(100, 125))', description: 'Percentage change from 100 to 125', expectedType: 'number' },

  // ── Comparison (10) ──────────────────────────────────────────
  { name: 'eq', category: 'Comparison', dsl: 'print(eq(100, 100))', description: 'Check if 100 equals 100', expectedType: 'boolean' },
  { name: 'neq', category: 'Comparison', dsl: 'print(neq(100, 200))', description: 'Check if 100 is not equal to 200', expectedType: 'boolean' },
  { name: 'gt', category: 'Comparison', dsl: 'print(gt(200, 100))', description: 'Check if 200 is greater than 100', expectedType: 'boolean' },
  { name: 'gte', category: 'Comparison', dsl: 'print(gte(100, 100))', description: 'Check if 100 ≥ 100', expectedType: 'boolean' },
  { name: 'lt', category: 'Comparison', dsl: 'print(lt(50, 100))', description: 'Check if 50 < 100', expectedType: 'boolean' },
  { name: 'lte', category: 'Comparison', dsl: 'print(lte(100, 100))', description: 'Check if 100 ≤ 100', expectedType: 'boolean' },
  { name: 'between', category: 'Comparison', dsl: 'print(between(5, 1, 10))', description: 'Check if 5 is between 1 and 10', expectedType: 'boolean' },
  { name: 'is_null', category: 'Comparison', dsl: 'print(is_null(None))', description: 'Check if value is null/None', expectedType: 'boolean' },
  { name: 'is_positive', category: 'Comparison', dsl: 'print(is_positive(42))', description: 'Check if 42 is positive', expectedType: 'boolean' },
  { name: 'is_negative', category: 'Comparison', dsl: 'print(is_negative(-5))', description: 'Check if -5 is negative', expectedType: 'boolean' },

  // ── Logical (10) ─────────────────────────────────────────────
  { name: 'and', category: 'Logical', dsl: 'print(True and True)', description: 'Logical AND: True and True → True', expectedType: 'boolean' },
  { name: 'or', category: 'Logical', dsl: 'print(False or True)', description: 'Logical OR: False or True → True', expectedType: 'boolean' },
  { name: 'not', category: 'Logical', dsl: 'print(not(False))', description: 'Logical NOT of False', expectedType: 'boolean' },
  { name: 'xor', category: 'Logical', dsl: 'print(xor(True, False))', description: 'Exclusive OR of True and False', expectedType: 'boolean' },
  { name: 'all', category: 'Logical', dsl: 'print(all([True, True, True]))', description: 'Check if all values in list are true', expectedType: 'boolean' },
  { name: 'any', category: 'Logical', dsl: 'print(any([False, False, True]))', description: 'Check if any value in list is true', expectedType: 'boolean' },
  { name: 'if', category: 'Logical', dsl: 'print(if(True, "yes", "no"))', description: 'Inline IF: if True return "yes" else "no"', expectedType: 'string' },
  { name: 'coalesce', category: 'Logical', dsl: 'print(coalesce(None, None, 42, 99))', description: 'Return first non-null value from list', expectedType: 'number' },
  { name: 'clamp', category: 'Logical', dsl: 'print(clamp(150, 0, 100))', description: 'Clamp 150 to range [0, 100]', expectedType: 'number' },
  { name: 'switch', category: 'Logical', dsl: 'print(switch("B", {"A": 1, "B": 2, "C": 3}, 0))', description: 'Switch on "B" returning 2 from cases dict', expectedType: 'number' },

  // ── Date (20) ────────────────────────────────────────────────
  { name: 'normalize_date', category: 'Date', dsl: 'print(normalize_date("01/15/2024"))', description: 'Normalize "01/15/2024" to YYYY-MM-DD format', expectedType: 'string' },
  { name: 'days_between', category: 'Date', dsl: 'print(days_between("2024-01-01", "2024-03-15"))', description: 'Days between Jan 1 and Mar 15, 2024', expectedType: 'number' },
  { name: 'months_between', category: 'Date', dsl: 'print(months_between("2024-01-15", "2024-09-15"))', description: 'Months between Jan 15 and Sep 15, 2024', expectedType: 'number' },
  { name: 'years_between', category: 'Date', dsl: 'print(years_between("2020-01-01", "2024-01-01"))', description: 'Years between 2020 and 2024', expectedType: 'number' },
  { name: 'add_days', category: 'Date', dsl: 'print(add_days("2024-01-01", 45))', description: 'Add 45 days to Jan 1, 2024', expectedType: 'string' },
  { name: 'add_months', category: 'Date', dsl: 'print(add_months("2024-01-31", 1))', description: 'Add 1 month to Jan 31, 2024', expectedType: 'string' },
  { name: 'add_years', category: 'Date', dsl: 'print(add_years("2024-02-29", 1))', description: 'Add 1 year to Feb 29, 2024 (leap year)', expectedType: 'string' },
  { name: 'subtract_days', category: 'Date', dsl: 'print(subtract_days("2024-03-01", 1))', description: 'Subtract 1 day from Mar 1, 2024', expectedType: 'string' },
  { name: 'subtract_months', category: 'Date', dsl: 'print(subtract_months("2024-03-31", 1))', description: 'Subtract 1 month from Mar 31, 2024', expectedType: 'string' },
  { name: 'subtract_years', category: 'Date', dsl: 'print(subtract_years("2024-01-01", 5))', description: 'Subtract 5 years from 2024', expectedType: 'string' },
  { name: 'start_of_month', category: 'Date', dsl: 'print(start_of_month("2024-06-15"))', description: 'First day of month for Jun 15, 2024', expectedType: 'string' },
  { name: 'end_of_month', category: 'Date', dsl: 'print(end_of_month("2024-02-15"))', description: 'Last day of Feb 2024 (leap year)', expectedType: 'string' },
  { name: 'day_count_fraction', category: 'Date', dsl: 'print(day_count_fraction("2024-01-01", "2024-07-01", "ACT/360"))', description: 'Year fraction Jan-Jul 2024 using ACT/360', expectedType: 'number' },
  { name: 'is_leap_year', category: 'Date', dsl: 'print(is_leap_year(2024))', description: 'Check if 2024 is a leap year', expectedType: 'boolean' },
  { name: 'days_in_year', category: 'Date', dsl: 'print(days_in_year(2024))', description: 'Number of days in 2024', expectedType: 'number' },
  { name: 'quarter', category: 'Date', dsl: 'print(quarter("2024-08-15"))', description: 'Quarter number for Aug 15', expectedType: 'number' },
  { name: 'day_of_week', category: 'Date', dsl: 'print(day_of_week("2024-01-01"))', description: 'Day of week for Jan 1, 2024 (0=Mon)', expectedType: 'number' },
  { name: 'is_weekend', category: 'Date', dsl: 'print(is_weekend("2024-01-06"))', description: 'Check if Jan 6, 2024 (Saturday) is weekend', expectedType: 'boolean' },
  { name: 'normalize_arraydate', category: 'Date', dsl: 'print(normalize_arraydate(["01/15/2024", "2024-02-28", "03-15-2024"]))', description: 'Normalize array of string dates to YYYY-MM-DD. Pass plain strings only — no date() objects.', expectedType: 'array' },
  { name: 'business_days', category: 'Date', dsl: 'print(business_days("2024-01-01", "2024-01-31"))', description: 'Business days in January 2024', expectedType: 'number' },

  // ── Schedule (7) ─────────────────────────────────────────────
  { name: 'schedule', category: 'Schedule', dsl: 'result = schedule(\n  period("2024-01-01", "2024-06-30", "M"),\n  {"period_date": "period_date", "amount": "1000"}\n)\nprint(result)', description: '6-month schedule with $1,000/month', expectedType: 'object' },
  { name: 'period', category: 'Schedule', dsl: 'print(period("2024-01-01", "2024-12-31", "Q"))', description: 'Quarterly periods for 2024', expectedType: 'object' },
  { name: 'schedule_sum', category: 'Schedule', dsl: 's = schedule(\n  period("2024-01-01", "2024-03-31", "M"),\n  {"seq": "period_index + 1", "amount": "(period_index + 1) * 100"}\n)\nprint(schedule_sum(s, "amount"))', description: 'Sum of amounts [100, 200, 300] = 600', expectedType: 'number' },
  { name: 'schedule_last', category: 'Schedule', dsl: 's = schedule(\n  period("2024-01-01", "2024-03-31", "M"),\n  {"seq": "period_index + 1", "amount": "(period_index + 1) * 100"}\n)\nprint(schedule_last(s, "amount"))', description: 'Last amount = 300', expectedType: 'number' },
  { name: 'schedule_first', category: 'Schedule', dsl: 's = schedule(\n  period("2024-01-01", "2024-03-31", "M"),\n  {"seq": "period_index + 1", "amount": "(period_index + 1) * 100"}\n)\nprint(schedule_first(s, "amount"))', description: 'First amount = 100', expectedType: 'number' },
  { name: 'schedule_column', category: 'Schedule', dsl: 's = schedule(\n  period("2024-01-01", "2024-03-31", "M"),\n  {"seq": "period_index + 1", "amount": "(period_index + 1) * 100"}\n)\nprint(schedule_column(s, "amount"))', description: 'Extract amounts column as [100, 200, 300]', expectedType: 'array' },
  { name: 'schedule_filter', category: 'Schedule', dsl: 's = schedule(\n  period("2024-01-01", "2024-03-31", "M"),\n  {"seq": "period_index + 1", "amount": "(period_index + 1) * 100"}\n)\nprint(schedule_filter(s, "seq", 2, "amount"))', description: 'Find amount where seq == 2 → [200]', expectedType: 'any' },

  // ── Aggregation (13) ─────────────────────────────────────────
  { name: 'sum', category: 'Aggregation', dsl: 'print(sum([10, 20, 30, 40, 50]))', description: 'Sum of [10, 20, 30, 40, 50]', expectedType: 'number' },
  { name: 'sum_field', category: 'Aggregation', dsl: 'print(sum_field([{"amount": 100}, {"amount": 200}, {"amount": 300}], "amount"))', description: 'Sum the "amount" field from array of objects', expectedType: 'number' },
  { name: 'avg', category: 'Aggregation', dsl: 'print(avg([10, 20, 30, 40, 50]))', description: 'Average of [10, 20, 30, 40, 50]', expectedType: 'number' },
  { name: 'min', category: 'Aggregation', dsl: 'print(min([42, 17, 99, 3, 56]))', description: 'Minimum of [42, 17, 99, 3, 56]', expectedType: 'number' },
  { name: 'max', category: 'Aggregation', dsl: 'print(max([42, 17, 99, 3, 56]))', description: 'Maximum of [42, 17, 99, 3, 56]', expectedType: 'number' },
  { name: 'count', category: 'Aggregation', dsl: 'print(count([1, 2, 3, 4, 5]))', description: 'Count items in list', expectedType: 'number' },
  { name: 'weighted_avg', category: 'Aggregation', dsl: 'print(weighted_avg([100, 200, 300], [1, 2, 3]))', description: 'Weighted average: values [100,200,300] with weights [1,2,3]', expectedType: 'number' },
  { name: 'cumulative_sum', category: 'Aggregation', dsl: 'print(cumulative_sum([10, 20, 30, 40]))', description: 'Running total of [10, 20, 30, 40]', expectedType: 'array' },
  { name: 'median', category: 'Aggregation', dsl: 'print(median([3, 7, 1, 9, 5]))', description: 'Median of [3, 7, 1, 9, 5]', expectedType: 'number' },
  { name: 'variance', category: 'Aggregation', dsl: 'print(variance([2, 4, 4, 4, 5, 5, 7, 9]))', description: 'Variance of data set', expectedType: 'number' },
  { name: 'std_dev', category: 'Aggregation', dsl: 'print(std_dev([2, 4, 4, 4, 5, 5, 7, 9]))', description: 'Standard deviation of data set', expectedType: 'number' },
  { name: 'percentile', category: 'Aggregation', dsl: 'print(percentile([10, 20, 30, 40, 50, 60, 70, 80, 90, 100], 0.75))', description: '75th percentile of values 10-100 (p must be 0–1)', expectedType: 'number' },
  { name: 'range', category: 'Aggregation', dsl: 'print(range([10, 50, 30, 90, 20]))', description: 'Range (max-min) of [10, 50, 30, 90, 20]', expectedType: 'number' },

  // ── Conversion (6) ───────────────────────────────────────────
  { name: 'fx_convert', category: 'Conversion', dsl: 'print(fx_convert(1000, 1.35))', description: 'Convert $1,000 at exchange rate 1.35', expectedType: 'number' },
  { name: 'normalize', category: 'Conversion', dsl: 'print(normalize(50, 200))', description: 'Normalize 50 relative to base 200', expectedType: 'number' },
  { name: 'basis_points', category: 'Conversion', dsl: 'print(basis_points(0.0525))', description: 'Convert 5.25% rate to basis points', expectedType: 'number' },
  { name: 'from_bps', category: 'Conversion', dsl: 'print(from_bps(525))', description: 'Convert 525 basis points to rate', expectedType: 'number' },
  { name: 'to_percentage', category: 'Conversion', dsl: 'print(to_percentage(0.1275))', description: 'Convert 0.1275 decimal to percentage', expectedType: 'number' },
  { name: 'from_percentage', category: 'Conversion', dsl: 'print(from_percentage(12.75))', description: 'Convert 12.75% to decimal', expectedType: 'number' },

  // ── Statistical (3) ──────────────────────────────────────────
  { name: 'correlation', category: 'Statistical', dsl: 'print(correlation([1, 2, 3, 4, 5], [2, 4, 5, 4, 5]))', description: 'Pearson correlation between two series', expectedType: 'number' },
  { name: 'covariance', category: 'Statistical', dsl: 'print(covariance([1, 2, 3, 4, 5], [2, 4, 5, 4, 5]))', description: 'Covariance between two series', expectedType: 'number' },
  { name: 'zscore', category: 'Statistical', dsl: 'print(zscore(85, 70, 10))', description: 'Z-score: value 85, mean 70, std dev 10', expectedType: 'number' },

  // ── String (9) ───────────────────────────────────────────────
  { name: 'lower', category: 'String', dsl: 'print(lower("HELLO WORLD"))', description: 'Convert to lowercase', expectedType: 'string' },
  { name: 'upper', category: 'String', dsl: 'print(upper("hello world"))', description: 'Convert to uppercase', expectedType: 'string' },
  { name: 'concat', category: 'String', dsl: 'print(concat("Hello", " ", "World"))', description: 'Concatenate strings', expectedType: 'string' },
  { name: 'contains', category: 'String', dsl: 'print(contains("Hello World", "World"))', description: 'Check if string contains "World"', expectedType: 'boolean' },
  { name: 'eq_ignore_case', category: 'String', dsl: 'print(eq_ignore_case("Hello", "hello"))', description: 'Case-insensitive comparison', expectedType: 'boolean' },
  { name: 'starts_with', category: 'String', dsl: 'print(starts_with("FyntracDSL", "Fyn"))', description: 'Check if starts with "Fyn"', expectedType: 'boolean' },
  { name: 'ends_with', category: 'String', dsl: 'print(ends_with("report.pdf", ".pdf"))', description: 'Check if ends with ".pdf"', expectedType: 'boolean' },
  { name: 'trim', category: 'String', dsl: 'print(trim("  hello  "))', description: 'Trim whitespace', expectedType: 'string' },
  { name: 'str_length', category: 'String', dsl: 'print(str_length("Fyntrac"))', description: 'Length of "Fyntrac"', expectedType: 'number' },

  // ── Array Collection (5) — require event context ─────────────
  { name: 'collect_by_instrument', category: 'Array', dsl: 'print(collect_by_instrument([100, 200, 300]))', description: 'Collect values for current instrument', expectedType: 'array', requiresEvents: true },
  { name: 'collect_all', category: 'Array', dsl: 'print(collect_all([100, 200, 300]))', description: 'Collect ALL values across all rows', expectedType: 'array', requiresEvents: true },
  { name: 'collect_by_subinstrument', category: 'Array', dsl: 'print(collect_by_subinstrument([100, 200]))', description: 'Collect values for current sub-instrument', expectedType: 'array', requiresEvents: true },
  { name: 'collect_subinstrumentids', category: 'Array', dsl: 'print(collect_subinstrumentids())', description: 'Get all unique sub-instrument IDs', expectedType: 'array', requiresEvents: true },
  { name: 'collect_effectivedates_for_subinstrument', category: 'Array', dsl: 'print(collect_effectivedates_for_subinstrument("1"))', description: 'Get all effective dates for sub-instrument "1"', expectedType: 'array', requiresEvents: true },

  // ── Iteration (4) ────────────────────────────────────────────
  { name: 'for_each',         category: 'Iteration', dsl: 'result = for_each(["2024-01-01", "2024-02-01"], [100, 200], "d", "a", "str(d) + \': $\' + str(a)")\nprint(result)', description: 'Iterate dates+amounts pairs, build strings', expectedType: 'array' },
  { name: 'for_each_with_index', category: 'Iteration', dsl: 'result = for_each_with_index([10, 20, 30], "x", "x * 2")\nprint(result)', description: 'Map each element: double values → [20, 40, 60]', expectedType: 'array' },
  { name: 'map_array', category: 'Iteration', dsl: 'print(map_array([1, 2, 3, 4, 5], "x", "x * x"))', description: 'Map: square each element', expectedType: 'array' },
  { name: 'array_filter', category: 'Iteration', dsl: 'print(array_filter([1, 2, 3, 4, 5, 6, 7, 8], "x", "x > 4"))', description: 'Filter: keep values greater than 4', expectedType: 'array' },

  // ── Array Utilities (10) ─────────────────────────────────────
  { name: 'lookup', category: 'Array Utilities', dsl: 'print(lookup([100, 200, 300], ["A", "B", "C"], "B"))', description: 'Lookup value 200 by matching "B" in keys', expectedType: 'number' },
  { name: 'zip_arrays', category: 'Array Utilities', dsl: 'print(zip_arrays([1, 2, 3], ["a", "b", "c"]))', description: 'Zip [1,2,3] with ["a","b","c"] into tuples', expectedType: 'array' },
  { name: 'array_length', category: 'Array Utilities', dsl: 'print(array_length([10, 20, 30, 40, 50]))', description: 'Length of 5-element array', expectedType: 'number' },
  { name: 'array_get', category: 'Array Utilities', dsl: 'print(array_get(["a", "b", "c", "d"], 2))', description: 'Get element at index 2', expectedType: 'string' },
  { name: 'array_first', category: 'Array Utilities', dsl: 'print(array_first([100, 200, 300]))', description: 'First element of array', expectedType: 'number' },
  { name: 'array_last', category: 'Array Utilities', dsl: 'print(array_last([100, 200, 300]))', description: 'Last element of array', expectedType: 'number' },
  { name: 'array_slice', category: 'Array Utilities', dsl: 'print(array_slice([10, 20, 30, 40, 50], 1, 4))', description: 'Slice array from index 1 to 4', expectedType: 'array' },
  { name: 'array_reverse', category: 'Array Utilities', dsl: 'print(array_reverse([1, 2, 3, 4, 5]))', description: 'Reverse array order', expectedType: 'array' },
  { name: 'array_append', category: 'Array Utilities', dsl: 'print(array_append([1, 2, 3], 4))', description: 'Append 4 to array', expectedType: 'array' },
  { name: 'array_extend', category: 'Array Utilities', dsl: 'print(array_extend([1, 2], [3, 4, 5]))', description: 'Extend array with another array', expectedType: 'array' },

  // ── Transaction (1) ──────────────────────────────────────────
  { name: 'createTransaction', category: 'Transaction', dsl: 'print(createTransaction("2024-01-15", "2024-01-15", "PAYMENT", 5000))', description: 'Create a payment transaction for $5,000', expectedType: 'object' },
];

export default SAMPLE_DATA;

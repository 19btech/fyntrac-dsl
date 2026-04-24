
# ============= Imports (must be at top) =============
import math
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional


def safe_eval_expression(expression: str, context: Dict[str, Any]):
    """
    Evaluate a DSL expression string in a restricted context.
    Uses the registered `DSL_FUNCTIONS` and a small set of safe builtins.
    Falls back to raising the original exception to the caller.
    """
    # Build a safe globals mapping exposing DSL functions and a few helpers
    safe_globals = {
        '__builtins__': None,
        'int': int,
        'float': float,
        'str': str,
        'len': len,
        'min': min,
        'max': max,
        'sum': sum,
        'round': round,
        'True': True,
        'False': False,
        'None': None,
    }

    # Insert DSL functions if available
    dsl_funcs = globals().get('DSL_FUNCTIONS', {})
    safe_globals.update(dsl_funcs)

    # Lazy-evaluate top-level if(...) / iif(...) to avoid evaluating both branches
    expr_str = str(expression).strip()
    _if_prefix = 'iif(' if expr_str.startswith('iif(') else ('if(' if expr_str.startswith('if(') else None)
    if _if_prefix and expr_str.endswith(')'):
        inside = expr_str[len(_if_prefix):-1]
        parts = []
        buf = ''
        depth = 0
        for ch in inside:
            if ch == ',' and depth == 0:
                parts.append(buf.strip())
                buf = ''
                continue
            buf += ch
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
        if buf:
            parts.append(buf.strip())
        if len(parts) == 3:
            cond_expr, true_expr, false_expr = parts
            cond_val = safe_eval_expression(cond_expr, context)
            chosen = true_expr if cond_val else false_expr
            return safe_eval_expression(chosen, context)

    # Evaluate expression using eval with restricted globals and provided locals
    # The context variables are provided as locals so they shadow DSL functions if needed
    # Replace 'if(' with 'iif(' because 'if' is a Python keyword and cannot be used as a
    # function name in eval(), even though DSL_FUNCTIONS has 'iif' mapped to if_op.
    import re as _re
    expr_for_eval = _re.sub(r'\bif\s*\(', 'iif(', expr_str)
    try:
        return eval(expr_for_eval, safe_globals, context or {})
    except Exception:
        # Re-raise to let callers handle/log; callers often catch and return None
        raise


# Helper to coerce 'n' parameters to int consistently across DSL functions
def _coerce_n_to_int(n: Any, param_name: str = 'n') -> int:
    """Coerce numeric-like values to int.

    - Accepts int or float; floats are rounded to nearest integer.
    - Raises ValueError for non-numeric inputs.
    """
    if isinstance(n, bool):
        raise ValueError(f"Invalid {param_name}: boolean not allowed")
    if isinstance(n, int):
        return n
    if isinstance(n, float):
        # Round float to nearest integer for consistent behavior
        return int(round(n))
    # Try to coerce from string or other types
    try:
        val = float(n)
        return int(round(val))
    except Exception:
        raise ValueError(f"Invalid {param_name}: expected numeric value, got {type(n)}")

# ============= New DSL Functions =============
def normalize_arraydate(array: list) -> list:
    """
    Normalize all date values in an array to system-standard format (yyyy-mm-dd).
    Raises ValueError if a non-date value is encountered.
    Returns a new array with normalized dates.
    """
    # Accept a single scalar date by treating it as a single-item list
    if not isinstance(array, list):
        # None or empty -> return empty list
        if array is None or (isinstance(array, str) and str(array).strip() == ''):
            return []
        norm = normalize_date(array)
        return [norm] if norm else []

    result = []
    for val in array:
        norm = normalize_date(val)
        if not norm:
            raise ValueError(f"Non-date value encountered in array: {val}")
        result.append(norm)
    return result

def lookup(value_array: list, match_array: list, target_value: Any) -> Any:
    """
    Retrieve a value from value_array by matching an element in match_array to target_value.
    Matching is type-agnostic (supports date, string, number, enum, etc.).
    Returns value_array[i] where match_array[i] == target_value, or None if not found.
    Raises ValueError for mismatched array lengths.
    """
    # Accept scalar inputs by coercing to single-item lists for value/match arrays
    if not isinstance(value_array, list):
        value_array = [value_array]
    if not isinstance(match_array, list):
        match_array = [match_array]

    # Broadcast single-item lists to match the length of the other list if possible
    if len(value_array) != len(match_array):
        if len(value_array) == 1 and len(match_array) > 1:
            value_array = value_array * len(match_array)
        elif len(match_array) == 1 and len(value_array) > 1:
            match_array = match_array * len(value_array)
        else:
            raise ValueError("value_array and match_array must have the same length.")

    # Helper for type-agnostic comparison (normalize dates, etc.)
    def _normalize(val):
        try:
            norm = normalize_date(val)
            if norm:
                return norm
        except Exception:
            pass
        return val

    # If target_value is an array, return an array of lookups
    if isinstance(target_value, list):
        results = []
        for t in target_value:
            norm_t = _normalize(t)
            found = None
            for i, match in enumerate(match_array):
                if _normalize(match) == norm_t:
                    found = value_array[i]
                    break
            results.append(found)
        return results

    # Scalar target_value: perform single lookup
    norm_target = _normalize(target_value)
    for i, match in enumerate(match_array):
        if _normalize(match) == norm_target:
            return value_array[i]
    return None

"""
Complete DSL Functions Library - 101 Financial Functions
"""

# ============= Date Normalization Helper =============

def normalize_date(date_value: Any) -> str:
    """
    Normalize a date value to YYYY-MM-DD string format.
    Handles datetime objects, timestamps, and various string formats.
    
    Args:
        date_value: Date in any format (datetime, timestamp, string)
    
    Returns:
        Date string in YYYY-MM-DD format, or empty string if invalid
    """
    if date_value is None:
        return ''

    # Unwrap hybrid context-array (used inside schedule() column expressions)
    # to its current-row scalar so downstream date parsing sees a string/date,
    # not the list repr.
    _RAA = globals().get('_RowAwareArray')
    if _RAA is not None and isinstance(date_value, _RAA):
        date_value = date_value._row if date_value._row is not None else ''
        if date_value is None:
            return ''

    # If already a string, try to parse and reformat
    if isinstance(date_value, str):
        date_str = date_value.strip()
        if not date_str or date_str == 'None':
            return ''
        
        # Already in YYYY-MM-DD format
        if len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-':
            return date_str
        
        # Try to parse common formats
        for fmt in ['%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%d %H:%M:%S.%f',
                    '%m/%d/%Y', '%d/%m/%Y', '%m-%d-%Y', '%d-%m-%Y']:
            try:
                dt = datetime.strptime(date_str[:min(len(date_str), 26)], fmt)
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        # If it has a 'T' or space, just take the date part
        if 'T' in date_str:
            return date_str.split('T')[0]
        if ' ' in date_str:
            return date_str.split(' ')[0]
        
        return date_str
    
    # If datetime object
    if isinstance(date_value, datetime):
        return date_value.strftime('%Y-%m-%d')
    
    # If it's a date object (not datetime)
    if hasattr(date_value, 'strftime'):
        return date_value.strftime('%Y-%m-%d')
    
    # Fallback - convert to string and try to extract date
    str_val = str(date_value).strip()
    if 'T' in str_val:
        return str_val.split('T')[0]
    if ' ' in str_val:
        return str_val.split(' ')[0]
    
    return str_val


# ============= Core Financial Functions =============

def pv(rate: float, n: int, pmt: float, fv: float = 0, type: int = 0) -> float:
    """Calculates present value of future cash flows
    
    Args:
        rate: Interest rate per period
        n: Number of periods
        pmt: Payment per period
        fv: Future value (default 0)
        type: 0 = payment at end of period (default), 1 = payment at beginning
    """
    # Allow n to be provided as float; coerce to int
    n = _coerce_n_to_int(n, 'n')
    if rate == 0:
        return -(fv + pmt * n)
    
    try:
        pv_annuity = pmt * ((1 - (1 + rate) ** (-n)) / rate)
        if type == 1:
            pv_annuity *= (1 + rate)  # Adjust for beginning-of-period payments
        pv_lump_sum = fv / ((1 + rate) ** n)
        return -(pv_annuity + pv_lump_sum)
    except (OverflowError, ZeroDivisionError):
        raise ValueError(f"pv: overflow — use rate as decimal (e.g., 0.05/12 for monthly 5% annual), got rate={rate}, n={n}")

def fv(rate: float, n: int, pmt: float, pv: float = 0, type: int = 0) -> float:
    """Calculates future value
    
    Args:
        rate: Interest rate per period
        n: Number of periods
        pmt: Payment per period
        pv: Present value (default 0)
        type: 0 = payment at end of period (default), 1 = payment at beginning
    """
    n = _coerce_n_to_int(n, 'n')
    if rate == 0:
        return -(pv + pmt * n)
    
    try:
        fv_lump_sum = -pv * (1 + rate) ** n
        fv_annuity = pmt * (((1 + rate) ** n - 1) / rate)
        if type == 1:
            fv_annuity *= (1 + rate)  # Adjust for beginning-of-period payments
        return -(fv_lump_sum + fv_annuity)
    except (OverflowError, ZeroDivisionError):
        raise ValueError(f"fv: overflow — use rate as decimal (e.g., 0.05/12 for monthly 5% annual), got rate={rate}, n={n}")

def pmt(rate: float, n: int, pv: float, fv: float = 0, type: int = 0) -> float:
    """Fixed periodic payment
    
    Args:
        rate: Interest rate per period
        n: Number of periods
        pv: Present value
        fv: Future value (default 0)
        type: 0 = payment at end of period (default), 1 = payment at beginning
    """
    n = _coerce_n_to_int(n, 'n')
    # Guard against zero periods to avoid division by zero
    if n == 0:
        return 0
    if rate == 0:
        return -(pv + fv) / n
    
    try:
        factor = (1 + rate) ** n
        if abs(factor - 1) < 1e-12:
            return -(pv + fv) / n  # Near-zero rate fallback
        payment = -(rate * (fv + pv * factor)) / (factor - 1)
        if type == 1:
            payment = payment / (1 + rate)  # Adjust for beginning-of-period payments
        return payment
    except (OverflowError, ZeroDivisionError):
        raise ValueError(f"pmt: overflow — use rate as decimal (e.g., 0.05/12 for monthly 5% annual), got rate={rate}, n={n}")

def rate(n: int, pmt: float, pv: float, fv: float = 0, type: int = 0, guess: float = 0.1) -> float:
    """Calculate interest rate per period
    
    Solves the equation: 0 = pv + pmt*(1+rate*type)*[(1+rate)^n - 1]/rate + fv/(1+rate)^n
    Uses Newton-Raphson method.
    
    Args:
        n: Number of periods
        pmt: Payment per period
        pv: Present value
        fv: Future value (default 0)
        type: 0 = payment at end of period (default), 1 = payment at beginning
        guess: Initial guess for rate (default 0.1 = 10%)
    
    Returns:
        Interest rate as decimal (0.1 = 10%)
    """
    # Coerce n to int if provided as float
    n = _coerce_n_to_int(n, 'n')
    if n == 0:
        return 0
    
    # For type=1, adjust pmt
    pmt_adj = pmt * (1 + (1 if type == 1 else 0))
    
    rate_est = guess
    max_iterations = 100
    tolerance = 1e-6

    for _ in range(max_iterations):
        # Clamp rate estimate to avoid overflow in (1+rate)^n
        if rate_est < -0.9999:
            rate_est = -0.9999
        if rate_est > 1e4:
            rate_est = 1e4

        if abs(rate_est) < 1e-10:
            # Use linear approximation when rate is near zero
            if abs(pmt_adj) < 1e-10:
                return 0
            return -(pv + fv) / (pmt_adj * n)

        try:
            # Calculate function value
            factor = (1 + rate_est) ** n
            npv_val = pv + pmt_adj * (factor - 1) / rate_est + fv / factor

            # Calculate derivative
            numerator = pmt_adj * (n * factor * rate_est - (factor - 1))
            denominator = rate_est ** 2 * factor
            derivative = numerator / denominator
        except (OverflowError, ZeroDivisionError):
            break

        if abs(derivative) < 1e-10:
            break

        new_rate = rate_est - npv_val / derivative

        if abs(new_rate - rate_est) < tolerance:
            return new_rate

        rate_est = new_rate

    return rate_est

def nper(rate: float, pmt: float, pv: float, fv: float = 0, type: int = 0) -> float:
    """Calculate number of periods
    
    Args:
        rate: Interest rate per period
        pmt: Payment per period
        pv: Present value
        fv: Future value (default 0)
        type: 0 = payment at end of period (default), 1 = payment at beginning
    
    Returns:
        Number of periods (can be fractional)
    """
    if rate == 0:
        if pmt == 0:
            return 0
        return -(pv + fv) / pmt

    # Adjust payment for type
    pmt_adj = pmt * (1 + rate) if type == 1 else pmt

    # Solve: PV + PMT*(1-(1+r)^-n)/r + FV*(1+r)^-n = 0
    # Let u = (1+r)^-n => u = (PMT/r + PV) / (PMT/r - FV)
    # Then n = -log(u) / log(1+r)
    try:
        numerator = pmt_adj / rate + pv
        denominator = pmt_adj / rate - fv  # Note: minus fv (was incorrectly +fv)

        if denominator == 0:
            return 0

        ratio = numerator / denominator
        if ratio <= 0:
            return 0  # No real solution exists

        n = -math.log(ratio) / math.log(1 + rate)
        return n
    except (ValueError, ZeroDivisionError, OverflowError):
        return 0

def npv(rate: float, cashflows: List[float]) -> float:
    """Net present value"""
    total = 0
    for i, cf in enumerate(cashflows, start=1):
        try:
            total += cf / ((1 + rate) ** i)
        except (OverflowError, ZeroDivisionError):
            break
    return total

def irr(cashflows: List[float], guess: float = 0.1) -> float:
    """
    Internal rate of return using Newton-Raphson method.
    Returns the rate that makes NPV equal to zero.
    
    Args:
        cashflows: List of cash flows (first is typically negative for investment)
        guess: Initial guess for the rate (default 0.1 = 10%)
    
    Returns:
        IRR as a decimal (0.1 = 10%), or None if no solution found
    """
    if not cashflows or len(cashflows) < 2:
        return 0
    
    rate = guess
    max_iterations = 100
    tolerance = 1e-7
    
    for iteration in range(max_iterations):
        # Calculate NPV at current rate (starting from period 1, like Excel)
        try:
            npv_val = sum(cf / ((1 + rate) ** i) for i, cf in enumerate(cashflows, start=1))
        except (OverflowError, ZeroDivisionError):
            break
        
        if abs(npv_val) < tolerance:
            return rate
        
        # Calculate derivative of NPV
        try:
            dnpv = sum(-i * cf / ((1 + rate) ** (i + 1)) for i, cf in enumerate(cashflows, start=1))
        except (OverflowError, ZeroDivisionError):
            break
        
        # Handle zero derivative (try a different approach)
        if abs(dnpv) < 1e-10:
            # Try bisection or adjust guess
            if npv_val > 0:
                rate = rate + 0.01
            else:
                rate = rate - 0.01
            continue
        
        # Newton-Raphson update
        new_rate = rate - npv_val / dnpv
        
        # Prevent rate from going too negative or too extreme
        if new_rate < -0.99:
            new_rate = -0.99
        elif new_rate > 10:
            new_rate = 10
        
        # Check for convergence
        if abs(new_rate - rate) < tolerance:
            return new_rate
        
        rate = new_rate
    
    # Return best estimate if no perfect solution found
    return rate

def xnpv(rate: float, cashflows: List[float], dates: List[str]) -> float:
    """NPV with specific dates (matches Excel XNPV)
    
    Args:
        rate: Discount rate
        cashflows: List of cash flows
        dates: List of dates (ISO format: YYYY-MM-DD)
    
    Note: Uses 365 days per year (Excel convention), not 365.25
    """
    if len(cashflows) != len(dates):
        raise ValueError("Cashflows and dates must have same length")
    
    # Normalize and validate dates
    if not dates or len(dates) != len(cashflows):
        raise ValueError("Cashflows and dates must have same length and not be empty")
    nd0 = normalize_date(dates[0])
    if not nd0:
        return 0
    base_date = datetime.fromisoformat(nd0)
    total = 0
    for cf, date_str in zip(cashflows, dates):
        nd = normalize_date(date_str)
        if not nd:
            return 0
        date = datetime.fromisoformat(nd)
        days = (date - base_date).days
        try:
            total += cf / ((1 + rate) ** (days / 365))  # Excel uses 365, not 365.25
        except (OverflowError, ZeroDivisionError):
            break
    return total

def xirr(cashflows: List[float], dates: List[str], guess: float = 0.1) -> float:
    """IRR with specific dates (matches Excel XIRR)
    
    Args:
        cashflows: List of cash flows
        dates: List of dates (ISO format: YYYY-MM-DD)
        guess: Initial guess (default 0.1 = 10%)
    """
    if not cashflows or len(cashflows) < 2:
        return 0
    
    rate = guess
    max_iterations = 100
    tolerance = 1e-6
    
    for iteration in range(max_iterations):
        xnpv_val = xnpv(rate, cashflows, dates)
        
        if abs(xnpv_val) < tolerance:
            return rate
        
        # Derivative approximation
        delta = 0.0001
        xnpv_plus = xnpv(rate + delta, cashflows, dates)
        slope = (xnpv_plus - xnpv_val) / delta
        
        if abs(slope) < 1e-10:
            break
        
        new_rate = rate - xnpv_val / slope
        
        # Prevent rate from going too negative or too extreme
        if new_rate < -0.99:
            new_rate = -0.99
        elif new_rate > 10:
            new_rate = 10
        
        # Check for convergence
        if abs(new_rate - rate) < tolerance:
            return new_rate
        
        rate = new_rate
    
    return rate

def discount_factor(rate: float, dcf: float) -> float:
    """Discount factor"""
    return 1 / (1 + rate * dcf)

def accumulation_factor(rate: float, dcf: float) -> float:
    """Growth factor"""
    return 1 + rate * dcf

def effective_rate(nominal: float, freq: int) -> float:
    """Nominal to effective rate"""
    return (1 + nominal / freq) ** freq - 1

def nominal_rate(effective: float, freq: int) -> float:
    """Effective to nominal rate"""
    return freq * ((1 + effective) ** (1 / freq) - 1)

def yield_to_maturity(price: float, face: float, coupon: float, years: float) -> float:
    """YTM approximation. coupon is decimal rate (e.g., 0.05 for 5%). years must be > 0."""
    if years == 0:
        raise ValueError("yield_to_maturity: years cannot be zero")
    denom = (face + price) / 2
    if denom == 0:
        raise ValueError("yield_to_maturity: (face + price) cannot be zero")
    annual_coupon = face * coupon
    ytm_approx = (annual_coupon + (face - price) / years) / denom
    return ytm_approx

# Interest Functions






# Depreciation





# Allocation





# Balance Functions



# Arithmetic
def _broadcast_binary(a, b, op_name, scalar_op):
    """Apply ``scalar_op(x, y)`` element-wise when either input is a list/tuple.

    Rules:
    - both list-like → element-wise on the *shorter* length (avoids silent zero-padding bugs)
    - one list-like, one scalar → broadcast the scalar over the list
    - both scalar → return ``scalar_op(a, b)``

    Note: `_RowAwareArray` (the hybrid context-array type used inside
    schedule() column expressions) is treated as a scalar so a context array
    referenced bare (e.g. `multiply(openingBalance, Monthly_Rate)`) operates
    on the current row's value, not the whole array.
    """
    # Defer import to avoid forward-reference issues; class is defined below.
    _RAA = globals().get('_RowAwareArray')
    if _RAA is not None:
        if isinstance(a, _RAA):
            a = a._row if a._row is not None else 0
        if isinstance(b, _RAA):
            b = b._row if b._row is not None else 0
    a_is_list = isinstance(a, (list, tuple))
    b_is_list = isinstance(b, (list, tuple))
    if not a_is_list and not b_is_list:
        return scalar_op(to_number(a), to_number(b))
    if a_is_list and b_is_list:
        n = min(len(a), len(b))
        return [scalar_op(to_number(a[i]), to_number(b[i])) for i in range(n)]
    if a_is_list:
        bv = to_number(b)
        return [scalar_op(to_number(x), bv) for x in a]
    av = to_number(a)
    return [scalar_op(av, to_number(x)) for x in b]


def add(a, b):
    return _broadcast_binary(a, b, 'add', lambda x, y: x + y)

def subtract(a, b):
    return _broadcast_binary(a, b, 'subtract', lambda x, y: x - y)

def multiply(a, b):
    return _broadcast_binary(a, b, 'multiply', lambda x, y: x * y)

def divide(a, b):
    def _div(x, y):
        if y == 0:
            raise ValueError("Division by zero")
        return x / y
    return _broadcast_binary(a, b, 'divide', _div)

def power(a: float, b: float) -> float:
    try:
        return a ** b
    except OverflowError:
        raise ValueError(f"power({a}, {b}): result overflows float range")
    except (ValueError, ZeroDivisionError):
        raise ValueError(f"power({a}, {b}): mathematically undefined (e.g., negative base with fractional exponent or 0 to negative power)")


def abs_val(x: float) -> float:
    return abs(x)

def to_number(x: Any) -> float:
    """Coerce various input types to a numeric value.

    Treat None, empty string, and 'None' as 0. If conversion fails, return 0.
    """
    if x is None:
        return 0
    # Unwrap hybrid context-array to its current-row scalar.
    _RAA = globals().get('_RowAwareArray')
    if _RAA is not None and isinstance(x, _RAA):
        x = x._row if x._row is not None else 0
    if isinstance(x, (int, float)):
        return x
    # Strings: empty or 'None' -> 0, else try float conversion
    if isinstance(x, str):
        s = x.strip()
        if s == '' or s.lower() == 'none':
            return 0
        try:
            return float(s)
        except Exception:
            return 0
    try:
        return float(x)
    except Exception:
        return 0

def sign(x: float) -> int:
    return 1 if x > 0 else (-1 if x < 0 else 0)

def round_val(x: float, n: int = 0) -> float:
    n = _coerce_n_to_int(n, 'n')
    return round(x, n)

def floor(x: float) -> int:
    return math.floor(x)

def ceil(x: float) -> int:
    return math.ceil(x)


def truncate(x: float, decimals: int = 0) -> float:
    """Truncate to decimals"""
    multiplier = 10 ** decimals
    return int(x * multiplier) / multiplier

def percentage(value: float, total: float) -> float:
    """Calculate percentage of total"""
    return (value / total * 100) if total != 0 else 0


# Comparison
def eq(a: Any, b: Any) -> bool:
    return a == b

def neq(a: Any, b: Any) -> bool:
    return a != b

def gt(a: float, b: float) -> bool:
    return a > b

def gte(a: float, b: float) -> bool:
    return a >= b

def lt(a: float, b: float) -> bool:
    return a < b

def lte(a: float, b: float) -> bool:
    return a <= b

def between(x: float, l: float, u: float) -> bool:
    return l <= x <= u

def is_null(x: Any) -> bool:
    # Treat None, empty strings, and the literal 'None' (case-insensitive)
    # as null values for DSL convenience.
    if x is None:
        return True
    if isinstance(x, str):
        s = x.strip()
        if s == '' or s.lower() == 'none':
            return True
    return False



# Logical
def and_op(a: bool, b: bool) -> bool:
    return a and b

def or_op(a: bool, b: bool) -> bool:
    return a or b

def not_op(a: bool) -> bool:
    return not a


def all_op(lst: List[bool]) -> bool:
    return all(lst)

def any_op(lst: List[bool]) -> bool:
    return any(lst)

def if_op(cond: bool, t: Any, f: Any) -> Any:
    return t if cond else f

def coalesce(*args) -> Any:
    for arg in args:
        if arg is not None:
            return arg
    return None


def switch(value: Any, cases: Dict[Any, Any], default_val: Any = None) -> Any:
    """Switch-case logic"""
    try:
        return cases.get(value, default_val)
    except (TypeError, AttributeError):
        # Handle cases where 'cases' is not a dictionary
        return default_val

# Date Functions
def days_between(d1: Any, d2: Any) -> int:
    """
    Robust days between that accepts strings, datetime objects, or None.
    Normalizes inputs using `normalize_date` and returns 0 for invalid/empty values.
    """
    # Normalize inputs (handles None, datetime, various string formats)
    try:
        n1 = normalize_date(d1)
    except Exception:
        n1 = ''
    try:
        n2 = normalize_date(d2)
    except Exception:
        n2 = ''

    if not n1 or not n2:
        return 0

    try:
        date1 = datetime.fromisoformat(n1)
        date2 = datetime.fromisoformat(n2)
        return abs((date2 - date1).days)
    except Exception:
        return 0



def months_between(d1: str, d2: str) -> int:
    # Handle empty or invalid date strings gracefully
    try:
        nd1 = normalize_date(d1)
        nd2 = normalize_date(d2)
        if not nd1 or not nd2:
            return 0
        date1 = datetime.fromisoformat(nd1)
        date2 = datetime.fromisoformat(nd2)
    except Exception:
        return 0
    return abs((date2.year - date1.year) * 12 + date2.month - date1.month)

def years_between(d1: str, d2: str) -> float:
    return days_between(d1, d2) / 365.25

def add_days(d: str, n: int) -> str:
    n = _coerce_n_to_int(n, 'n')
    nd = normalize_date(d)
    if not nd:
        return ''
    date = datetime.fromisoformat(nd)
    new_date = date + timedelta(days=n)
    return new_date.strftime('%Y-%m-%d')

def add_months(d: str, n: int) -> str:
    """Add n months to a date, handling month-end dates properly"""
    n = _coerce_n_to_int(n, 'n')
    # Normalize input and handle empty/invalid gracefully
    nd = normalize_date(d)
    if not nd:
        return ''
    try:
        date = datetime.fromisoformat(nd)
    except Exception:
        return ''
    month = date.month + n
    year = date.year + (month - 1) // 12
    month = (month - 1) % 12 + 1
    
    # Clamp day to valid range for target month
    # Get last day of target month
    if month == 12:
        last_day = 31
    else:
        next_month_first = datetime(year, month + 1, 1)
        last_day = (next_month_first - timedelta(days=1)).day
    
    day = min(date.day, last_day)
    return f"{year:04d}-{month:02d}-{day:02d}"

def add_years(d: str, n: int) -> str:
    """Add n years to a date, handling leap year dates properly"""
    n = _coerce_n_to_int(n, 'n')
    nd = normalize_date(d)
    if not nd:
        return ''
    date = datetime.fromisoformat(nd)
    target_year = date.year + n
    
    # Handle Feb 29 -> Feb 28 for non-leap years
    if date.month == 2 and date.day == 29:
        if not (target_year % 4 == 0 and (target_year % 100 != 0 or target_year % 400 == 0)):
            return f"{target_year:04d}-02-28"
    
    return f"{target_year:04d}-{date.month:02d}-{date.day:02d}"

def subtract_days(d: str, n: int) -> str:
    """Subtract n days from a date"""
    n = _coerce_n_to_int(n, 'n')
    return add_days(d, -n)

def subtract_months(d: str, n: int) -> str:
    """Subtract n months from a date, handling month-end dates properly"""
    n = _coerce_n_to_int(n, 'n')
    return add_months(d, -n)

def subtract_years(d: str, n: int) -> str:
    """Subtract n years from a date, handling leap year dates properly"""
    n = _coerce_n_to_int(n, 'n')
    return add_years(d, -n)

def start_of_month(d: str) -> str:
    # Normalize input and handle empty/invalid gracefully
    nd = normalize_date(d)
    if not nd:
        return ''
    date = datetime.fromisoformat(nd)
    return f"{date.year:04d}-{date.month:02d}-01"

def end_of_month(d: str) -> str:
    nd = normalize_date(d)
    if not nd:
        return ''
    try:
        date = datetime.fromisoformat(nd)
    except Exception:
        return ''
    if date.month == 12:
        next_month = datetime(date.year + 1, 1, 1)
    else:
        next_month = datetime(date.year, date.month + 1, 1)
    last_day = (next_month - timedelta(days=1)).day
    return f"{date.year:04d}-{date.month:02d}-{last_day:02d}"

def day_count_fraction(d1: str, d2: str, conv: str = "ACT/360") -> float:
    """Year fraction using DCC"""
    days = days_between(d1, d2)
    if conv == "ACT/360":
        return days / 360
    elif conv == "ACT/365":
        return days / 365
    elif conv == "30/360":
        nd1 = normalize_date(d1)
        nd2 = normalize_date(d2)
        if not nd1 or not nd2:
            return 0
        date1 = datetime.fromisoformat(nd1)
        date2 = datetime.fromisoformat(nd2)
        return ((date2.year - date1.year) * 360 + (date2.month - date1.month) * 30 + (date2.day - date1.day)) / 360
    return days / 365.25

def is_leap_year(year: int) -> bool:
    """Check leap year"""
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)

def days_in_year(year: int) -> int:
    """Days in year"""
    return 366 if is_leap_year(year) else 365

def quarter(d: str) -> int:
    """Get quarter from date"""
    nd = normalize_date(d)
    if not nd:
        return 0
    try:
        date = datetime.fromisoformat(nd)
        return (date.month - 1) // 3 + 1
    except Exception:
        return 0

def day_of_week(d: str) -> int:
    """Day of week (0=Monday, 6=Sunday)"""
    nd = normalize_date(d)
    if not nd:
        return 0
    try:
        date = datetime.fromisoformat(nd)
        return date.weekday()
    except Exception:
        return 0

def is_weekend(d: str) -> bool:
    """Check if weekend"""
    return day_of_week(d) >= 5

def business_days(d1: str, d2: str) -> int:
    """Count business days"""
    # Normalize inputs and handle empty/invalid values
    nd1 = normalize_date(d1)
    nd2 = normalize_date(d2)
    if not nd1 or not nd2:
        return 0
    date1 = datetime.fromisoformat(nd1)
    date2 = datetime.fromisoformat(nd2)
    days = abs((date2 - date1).days)
    weeks = days // 7
    remaining = days % 7
    weekdays = weeks * 5
    for i in range(remaining):
        if (date1 + timedelta(days=i)).weekday() < 5:
            weekdays += 1
    return weekdays

# ============= Schedule Functions =============

def period(start: str, end: str, freq: str = "M", convention: str = "ACT/360") -> Dict[str, Any]:
    """
    Creates a period definition for schedule generation.
    
    Args:
        start: Start date (YYYY-MM-DD)
        end: End date (YYYY-MM-DD)
        freq: Frequency - M (monthly), Q (quarterly), A (annual), D (daily), W (weekly)
        convention: Day count convention - ACT/360, ACT/365, 30/360
    
    Returns:
        Period definition object with dates list
    """
    # Support passing arrays of start/end dates to create per-item schedules implicitly.
    if isinstance(start, list) and isinstance(end, list):
        if len(start) != len(end):
            raise ValueError("start and end arrays must have the same length")
        # Carry subinstrument_ids through if either array is an _ScheduleValueList
        # so downstream schedule() can map results back to the right sub-instruments
        sub_ids = getattr(start, 'subinstrument_ids', None) or getattr(end, 'subinstrument_ids', None)
        out = {
            "type": "period_array",
            "start_dates": start,
            "end_dates": end,
            "freq": freq,
            "convention": convention,
        }
        if sub_ids:
            out["subinstrument_ids"] = list(sub_ids)
        return out

    # If either date is empty or invalid, return an empty period (no dates)
    if not start or not end:
        return {
            "type": "period",
            "start": start,
            "end": end,
            "freq": freq,
            "convention": convention,
            "dates": []
        }
    # Normalize start/end and guard invalid values
    nd_start = normalize_date(start)
    nd_end = normalize_date(end)
    if not nd_start or not nd_end:
        return {
            "type": "period",
            "start": start,
            "end": end,
            "freq": freq,
            "convention": convention,
            "dates": []
        }
    try:
        start_date = datetime.fromisoformat(nd_start)
        end_date = datetime.fromisoformat(nd_end)
    except Exception:
        return {
            "type": "period",
            "start": start,
            "end": end,
            "freq": freq,
            "convention": convention,
            "dates": []
        }
    
    dates = []
    current = start_date
    
    while current <= end_date:
        dates.append(current.strftime("%Y-%m-%d"))
        
        if freq == "M":
            # Monthly - advance to same day next month
            month = current.month + 1
            year = current.year
            if month > 12:
                month = 1
                year += 1
            # Handle month-end dates
            try:
                current = current.replace(year=year, month=month)
            except ValueError:
                # If day doesn't exist in target month, go to last day
                if month == 12:
                    next_month = current.replace(year=year+1, month=1, day=1)
                else:
                    next_month = current.replace(year=year, month=month+1, day=1)
                current = next_month - timedelta(days=1)
        elif freq == "Q":
            # Quarterly
            month = current.month + 3
            year = current.year
            while month > 12:
                month -= 12
                year += 1
            try:
                current = current.replace(year=year, month=month)
            except ValueError:
                if month == 12:
                    next_month = current.replace(year=year+1, month=1, day=1)
                else:
                    next_month = current.replace(year=year, month=month+1, day=1)
                current = next_month - timedelta(days=1)
        elif freq == "A":
            # Annual
            current = current.replace(year=current.year + 1)
        elif freq == "W":
            # Weekly
            current = current + timedelta(weeks=1)
        elif freq == "D":
            # Daily
            current = current + timedelta(days=1)
        else:
            # Default to monthly
            month = current.month + 1
            year = current.year
            if month > 12:
                month = 1
                year += 1
            try:
                current = current.replace(year=year, month=month)
            except ValueError:
                if month == 12:
                    next_month = current.replace(year=year+1, month=1, day=1)
                else:
                    next_month = current.replace(year=year, month=month+1, day=1)
                current = next_month - timedelta(days=1)
    
    return {
        "type": "period",
        "start": start,
        "end": end,
        "freq": freq,
        "convention": convention,
        "dates": dates
    }


def schedule(period_def: Dict[str, Any], columns: Dict[str, str], context: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """
    Creates a deterministic time-based schedule (table).
    
    Used for: revenue schedules, FAS-91 fee amortization, loan amortization, 
    depreciation, pricing, accruals, and accounting timelines.
    
    Args:
        period_def: Period definition from period() function
        columns: Dictionary of column names to expressions
                 Special variables available in expressions:
                 - period_date: current row's date (string YYYY-MM-DD)
                 - period_index: current row index (0-based)
                 - period_start: next period start date (for dcf calculation)
                 - dcf: day count fraction for current period
                 - lag('column_name', offset, default): get previous row value
                 - All DSL functions: days_between, end_of_month, start_of_month, etc.
        context: Optional dictionary of external variables to make available in expressions
                 Example: {"initial_balance": 100000, "rate": 0.05}
    
    Returns:
        List of dictionaries containing ONLY the columns you define
    
    Example:
        schedule(
            period("2024-01-01", "2024-06-01", "M"),
            {
                "date": "period_date",
                "days_in_month": "days_between(start_of_month(period_date), end_of_month(period_date)) + 1",
                "revenue": "12000 / 12"
            }
        )
        
        # With external context:
        schedule(
            period("2024-01-01", "2024-06-01", "M"),
            {"opening": "lag('closing', 1, initial_balance)", "closing": "opening - payment"},
            {"initial_balance": 100000, "payment": 5000}
        )
    """
    global _in_schedule_evaluation
    # Support alternative calling convention: schedule(COLUMNS, CONTEXT)
    # If the first arg looks like columns (dict of expressions) and the
    # second arg is a dict of arrays/context, swap them so `period_def` is None.
    if isinstance(period_def, dict) and not period_def.get('type') and isinstance(columns, dict) and columns:
        # Heuristic: treat this as (columns, context) when any context value is a list
        if any(isinstance(v, list) for v in columns.values()):
            columns_def = period_def
            context = columns
            period_def = None
            columns = columns_def

    # If the caller passed a period descriptor that represents multiple items,
    # expand into per-item schedules automatically. This makes DSL usage simple
    # for business users: they can call `period(start_dates, end_dates, freq)`
    # and `schedule()` will generate one schedule per start/end pair.
    if isinstance(period_def, dict) and period_def.get('type') == 'period_array':
        start_dates = period_def.get('start_dates', [])
        end_dates = period_def.get('end_dates', [])
        freq = period_def.get('freq', 'M')

        # Pull amounts, item names, and subinstrument ids from context if present
        amounts = None
        if context:
            amounts = context.get('amounts') or context.get('amount')
            item_names = context.get('item_names') or context.get('product_names')
            subinstrument_ids = context.get('subinstrument_ids') or context.get('subinstrument_id')
        else:
            item_names = None
            subinstrument_ids = None

        # Fall back to ids carried on the period descriptor (propagated from
        # collect_by_instrument via period(start_dates, end_dates, ...))
        if not subinstrument_ids:
            subinstrument_ids = period_def.get('subinstrument_ids')

        # Normalize amounts into a list matching start_dates length
        if amounts is None:
            amounts_list = [0] * len(start_dates)
        elif isinstance(amounts, list):
            amounts_list = amounts
        else:
            amounts_list = [amounts] * len(start_dates)

        return generate_schedules(
            amounts_list,
            start_dates,
            end_dates,
            columns,
            freq,
            context,
            item_names,
            subinstrument_ids
        )

    # If no explicit period is provided, support a unified, non-split schedule
    # This allows calls like `schedule(None, COLUMNS, {"amounts": [...], ...})`
    # to produce a single combined schedule (not split per subinstrument).
    if not period_def or period_def.get("type") != "period":
        # If caller provided context with arrays (amounts, start_dates, end_dates, subinstrument_ids),
        # generate a single unified schedule with one row per item and an auto-generated
        # sequence number `s_no` (starting at 1). This does NOT split by subinstrument.
        if context and isinstance(context, dict):
            # Collect any list-type values from context to determine number of rows
            arrays = [v for v in context.values() if isinstance(v, list)]
            # Determine number of rows: max length of any array, or 1 if none
            n = max((len(a) for a in arrays), default=1)

            # Prepare helper to read possibly-scalar context values as per-row
            def get_at(key, idx, default=None):
                val = context.get(key)
                if val is None:
                    return default
                if isinstance(val, list):
                    return val[idx] if idx < len(val) else default
                return val

            # Evaluate columns for each item producing a single unified schedule (list of rows)
            _in_schedule_evaluation += 1
            try:
                result = []
                computed_columns = {col: [] for col in columns.keys()}
                dsl_funcs = globals().get('DSL_FUNCTIONS', {})

                for idx in range(n):
                    # Build evaluation context exposing DSL functions and per-row variables
                    eval_context = {}
                    eval_context.update(dsl_funcs)
                    # Per-row extracted values
                    amount = get_at('amounts', idx, get_at('amount', idx, 0))
                    subinstrument = get_at('subinstrument_ids', idx, get_at('subinstrument_id', idx, str(idx + 1)))
                    item_name = get_at('item_names', idx, get_at('product_names', idx, f"Item {idx + 1}"))
                    start = get_at('start_dates', idx, '')
                    end = get_at('end_dates', idx, '')

                    # Add user-provided context, converting list-valued keys to per-row values
                    for k, v in context.items():
                        if k in ('amounts', 'start_dates', 'end_dates', 'subinstrument_ids', 'item_names'):
                            continue
                        if isinstance(v, list):
                            eval_context[k] = get_at(k, idx, None)
                        else:
                            eval_context[k] = v

                    # Add schedule-like variables
                    eval_context.update({
                        'amount': amount,
                        'subinstrument_id': subinstrument,
                        'item_name': item_name,
                        'start_date': start,
                        'end_date': end,
                        # Auto-generated sequence number for unified schedule
                        's_no': idx + 1,
                        'index': idx + 1,
                        'period_index': idx,
                        'period_date': '',
                        'dcf': 0
                    })

                    # Snapshot prior computed_columns so any lag() calls in expressions
                    # read a stable view (prevents indexing into in-progress lists).
                    prior_snapshot = {k: list(v) for k, v in computed_columns.items()}

                    # Provide a lag() helper into the eval context for unified schedules
                    def create_lag_unified(cols_ref):
                        def lag_impl(col_name, offset=1, default=0):
                            col_values = cols_ref.get(col_name, [])
                            if len(col_values) >= offset:
                                val = col_values[-offset]
                                if isinstance(val, str) and val.startswith("ERROR"):
                                    return default
                                return val
                            return default
                        return lag_impl

                    eval_context['lag'] = create_lag_unified(prior_snapshot)

                    # Include previously computed column values for cross-column references
                    # Do not overwrite existing per-row context variables (like sale_price)
                    for col_name, values in computed_columns.items():
                        if values:
                            last_val = values[-1]
                            if not (isinstance(last_val, str) and str(last_val).startswith("ERROR")):
                                if col_name not in eval_context:
                                    eval_context[col_name] = last_val

                    # Build row by evaluating each column expression
                    row = {}
                    for col_name, expression in columns.items():
                        expr = str(expression).strip()
                        if expr == 'period_date':
                            value = eval_context.get('period_date', '')
                        elif expr == 'period_index':
                            value = eval_context.get('period_index', 0)
                        elif expr == 'dcf':
                            value = eval_context.get('dcf', 0)
                        else:
                            # If the expression is a simple variable name, avoid eval and return
                            # the per-row context value (handles common user patterns).
                            if expr.isidentifier():
                                # Prefer per-row value in eval_context; fallback to context list element
                                val = eval_context.get(expr)
                                if val is None and isinstance(context, dict) and expr in context and isinstance(context[expr], list):
                                    val = get_at(expr, idx, None)
                                value = val
                            else:
                                try:
                                    value = safe_eval_expression(expr, eval_context)
                                except Exception as e:
                                    # If None-subscript error, attempt simple variable fallback
                                    msg = str(e)
                                    if 'NoneType' in msg and 'subscript' in msg and isinstance(context, dict):
                                        # try to extract first bare name from expression
                                        parts = expr.replace(']', '').replace('[', ' ').split()
                                        if parts:
                                            name = parts[0].split('.')[0]
                                            if name in context and isinstance(context[name], list):
                                                value = get_at(name, idx, None)
                                            else:
                                                value = f"ERROR: {msg}"
                                        else:
                                            value = f"ERROR: {msg}"
                                    else:
                                        value = f"ERROR: {msg}"

                        row[col_name] = value

                        if isinstance(value, str) and value.startswith("ERROR"):
                            computed_columns[col_name].append(0)
                        else:
                            computed_columns[col_name].append(value)

                        if not (isinstance(value, str) and str(value).startswith("ERROR")):
                            eval_context[col_name] = value

                    result.append(row)

                return result
            finally:
                _in_schedule_evaluation -= 1

        # Otherwise return empty (no period and no arrays to infer rows)
        return []
    
    dates = period_def.get("dates", [])
    convention = period_def.get("convention", "ACT/360")

    if not dates:
        return []

    # Mark that we're evaluating schedule column expressions to prevent
    # schedule helper re-entrancy (calling schedule helpers from inside
    # schedule column expressions can lead to recursion / confusing results).
    _in_schedule_evaluation += 1
    try:
        result = []
        computed_columns = {col: [] for col in columns.keys()}
    
        # Get DSL_FUNCTIONS from the module's global scope (defined at bottom of file)
        # This avoids circular import issues
        dsl_funcs = globals().get('DSL_FUNCTIONS', {})

        # Pre-normalize injected context variables into arrays matching the schedule length
        normalized_arrays = {}
        if context and isinstance(context, dict):
            n_dates = len(dates)
            for k, v in context.items():
                # If already a list, ensure it's at least n_dates long (pad with last or zeros)
                if isinstance(v, list):
                    arr = list(v)
                    if len(arr) < n_dates:
                        if arr:
                            arr = arr + [arr[-1]] * (n_dates - len(arr))
                        else:
                            arr = [0] * n_dates
                elif v is None:
                    arr = [0] * n_dates
                else:
                    # Scalar: broadcast to full-length array
                    arr = [v] * n_dates
                normalized_arrays[k] = arr

        for idx, date_str in enumerate(dates):
            # Calculate DCF and next period date
            if idx < len(dates) - 1:
                next_date = dates[idx + 1]
                dcf_value = day_count_fraction(date_str, next_date, convention)
            else:
                # Last period - use previous DCF or default
                if idx > 0:
                    prev_date = dates[idx - 1]
                    dcf_value = day_count_fraction(prev_date, date_str, convention)
                else:
                    dcf_value = 1/12  # Default monthly

            # Snapshot prior computed_columns so lag() reads a stable view
            # This prevents lag() from indexing into in-progress lists containing
            # placeholders or values appended while evaluating the current row.
            prior_snapshot = {k: list(v) for k, v in computed_columns.items()}

            # Create lag function with snapshot captured
            def create_lag(cols_ref):
                def lag_impl(col_name, offset=1, default=0):
                    col_values = cols_ref.get(col_name, [])
                    if len(col_values) >= offset:
                        val = col_values[-offset]
                        # If previous value was an error, return default
                        if isinstance(val, str) and val.startswith("ERROR"):
                            return default
                        return val
                    return default
                return lag_impl

            # Build context for expression evaluation with ALL DSL functions
            # Add DSL functions FIRST, then override with local schedule-specific functions
            eval_context = {}
            eval_context.update(dsl_funcs)

            # Inject context arrays as a hybrid object that behaves as both:
            #   - the full array (for lookup/iteration/indexing/len)
            #   - the current row's scalar (for arithmetic/eq/compare)
            # This means a context array `ExpectedCF` can be used in
            # `lookup(ExpectedCF, StartDate, month_end)` AND in
            # `multiply(ExpectedCF, rate)` from the same column expression
            # without needing a `_full` suffix. The `_full` alias is still
            # exposed for backward compatibility.
            if normalized_arrays:
                for k, arr in normalized_arrays.items():
                    row_val = arr[idx] if idx < len(arr) else 0
                    try:
                        eval_context[f"{k}_full"] = arr
                    except Exception:
                        eval_context[f"{k}_full"] = list(arr)
                    eval_context[k] = _RowAwareArray(arr, row_value=row_val)

            # Now add/override with schedule-specific context
            eval_context.update({
                # Special schedule variables
                "period_date": date_str,
                "period_index": idx,
                "period_start": dates[idx + 1] if idx < len(dates) - 1 else date_str,
                "dcf": dcf_value,

                # Lag function for referencing previous rows (overrides DSL_FUNCTIONS lag)
                "lag": create_lag(prior_snapshot),

                # Python built-ins
                "abs": abs,
                "min": min,
                "max": max,
                "round": round,
                "sum": sum_vals,
                "len": len,
                "int": int,
                "float": float,
                "str": str,
                "pow": pow,
                "True": True,
                "False": False,
            })

            # Add previously computed columns to context (for referencing other columns in same row)
            # This allows expressions like "opening * rate" where opening was already computed this row
            for col_name, values in computed_columns.items():
                if values:
                    last_val = values[-1]
                    # Skip None or ERROR markers when populating eval context
                    if last_val is None:
                        continue
                    if isinstance(last_val, str) and str(last_val).startswith("ERROR"):
                        continue
                    eval_context[col_name] = last_val

            # Create row with ONLY user-defined columns
            row = {}

            # Evaluate each column expression in order
            for col_name, expression in columns.items():

                # Handle special keywords
                if expression == "period_date":
                    value = date_str
                elif expression == "period_index":
                    value = idx
                elif expression == "dcf":
                    value = dcf_value
                else:
                    try:
                        # Evaluate expression with full DSL context using safe evaluator
                        expr_str = str(expression)
                        # Lazy-evaluate top-level if(...) / iif(...) to avoid evaluating both branches
                        def _eval_iif_top(expr):
                            # expects expr starting with if( or iif(
                            _pfx = 'iif(' if expr.startswith('iif(') else 'if('
                            inside = expr[len(_pfx):-1]
                            # split top-level commas into three parts
                            parts = []
                            buf = ''
                            depth = 0
                            for ch in inside:
                                if ch == ',' and depth == 0:
                                    parts.append(buf.strip())
                                    buf = ''
                                    continue
                                buf += ch
                                if ch == '(':
                                    depth += 1
                                elif ch == ')':
                                    depth -= 1
                            if buf:
                                parts.append(buf.strip())
                            if len(parts) != 3:
                                # fallback to normal eval if parse fails
                                return safe_eval_expression(expr, eval_context)
                            cond_expr, true_expr, false_expr = parts
                            cond_val = safe_eval_expression(cond_expr, eval_context)
                            chosen = true_expr if cond_val else false_expr
                            return safe_eval_expression(chosen, eval_context)

                        _if_stripped = expr_str.strip()
                        _if_pfx = 'iif(' if _if_stripped.startswith('iif(') else ('if(' if _if_stripped.startswith('if(') else None)
                        if _if_pfx and _if_stripped.endswith(')'):
                            value = _eval_iif_top(_if_stripped)
                        else:
                            value = safe_eval_expression(expr_str, eval_context)
                        # Guard: DSL functions must not return None inside schedule - replace None with 0 or []
                        if value is None:
                            value = 0
                        # If a column expression returned the hybrid context-array
                        # object directly (e.g. `UPB` referenced bare), unwrap it
                        # to the current row's scalar so the cell stores a single
                        # value instead of the full array's repr.
                        if isinstance(value, _RowAwareArray):
                            value = value._row if value._row is not None else 0
                    except Exception as e:
                        value = f"ERROR: {str(e)}"

                row[col_name] = value

                # Store the computed value for lag references
                # Normalize None and ERROR values to safe numeric defaults so lag() can index reliably
                if isinstance(value, str) and value.startswith("ERROR"):
                    computed_columns[col_name].append(0)
                else:
                    computed_columns[col_name].append(value if value is not None else 0)

                # Update context with new value for subsequent columns in same row
                if not (isinstance(value, str) and str(value).startswith("ERROR")):
                    eval_context[col_name] = value

            # Tag the row with the current (instrument, posting date) at the
            # moment of generation so the Business Preview can strictly scope
            # schedule display. We tag here (not at print time) because users
            # may print schedules via plain print(), bypassing print_schedule.
            try:
                _iid = _get_current_instrumentid()
                if _iid not in (None, "") and '_instrumentid' not in row:
                    row['_instrumentid'] = _iid
                _pd = _get_current_postingdate()
                if _pd not in (None, "") and '_postingdate' not in row:
                    row['_postingdate'] = _pd
            except Exception:
                pass

            result.append(row)
        return result
    finally:
        _in_schedule_evaluation -= 1


class _ScheduleValueList(list):
    """A list that also carries the sub-instrument ids associated with each entry.

    Returned by `schedule_sum`, `schedule_first`, `schedule_last`, `schedule_filter`
    and `schedule_column` when the source `schedule()` produced one schedule per
    sub-instrument.  `createTransaction` reads `.subinstrument_ids` to align each
    amount entry with the correct sub-instrument id, eliminating ordering bugs
    when a separate `subinstrumentid` variable is passed.
    """
    __slots__ = ('subinstrument_ids',)

    def __init__(self, iterable=(), subinstrument_ids=None):
        super().__init__(iterable)
        self.subinstrument_ids = list(subinstrument_ids) if subinstrument_ids is not None else None


class _RowAwareArray(list):
    """A list that ALSO behaves as the current-row scalar in arithmetic/comparisons.

    Used inside schedule() column expressions so a context array (e.g. ExpectedCF)
    can be referenced by its bare name and work both as:
      - a full array (lookup, len, iteration, indexing) - default list behavior
      - the current row's value (multiply, add, eq, comparisons) - via _row

    The per-row scalar is stored on `_row`; numeric/comparison dunders fall
    through to `_row` so `multiply(openingBalance, Monthly_Rate)` keeps working
    when `Monthly_Rate` is a context array but the formula expects a scalar.
    """
    __slots__ = ('_row',)

    def __new__(cls, iterable=(), row_value=None):
        return super().__new__(cls, iterable)

    def __init__(self, iterable=(), row_value=None):
        super().__init__(iterable)
        self._row = row_value

    # Numeric scalar coercion
    def __float__(self):
        try:
            return float(self._row) if self._row is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    def __int__(self):
        try:
            return int(self._row) if self._row is not None else 0
        except (TypeError, ValueError):
            return 0

    def __bool__(self):
        return bool(self._row) if self._row is not None else len(self) > 0

    # Arithmetic - delegate to per-row scalar
    def _r(self):
        return self._row if self._row is not None else 0

    def __add__(self, other):
        if isinstance(other, list) and not isinstance(other, _RowAwareArray):
            return list.__add__(self, other)
        return self._r() + other

    def __radd__(self, other):
        return other + self._r()

    def __sub__(self, other):
        return self._r() - other

    def __rsub__(self, other):
        return other - self._r()

    def __mul__(self, other):
        if isinstance(other, int) and not isinstance(other, bool) and self._row is None:
            return list.__mul__(self, other)
        return self._r() * other

    def __rmul__(self, other):
        return other * self._r()

    def __truediv__(self, other):
        return self._r() / other

    def __rtruediv__(self, other):
        return other / self._r()

    def __floordiv__(self, other):
        return self._r() // other

    def __rfloordiv__(self, other):
        return other // self._r()

    def __mod__(self, other):
        return self._r() % other

    def __rmod__(self, other):
        return other % self._r()

    def __pow__(self, other):
        return self._r() ** other

    def __rpow__(self, other):
        return other ** self._r()

    def __neg__(self):
        return -self._r()

    def __pos__(self):
        return +self._r()

    def __abs__(self):
        return abs(self._r())

    # Comparisons - default list compares lexicographically; we want scalar
    # semantics when the other side is a scalar (number/str/date), and list
    # semantics when comparing to another list.
    def _cmp(self, other, op):
        if isinstance(other, list) and not isinstance(other, _RowAwareArray):
            return op(list(self), other)
        return op(self._r(), other)

    def __eq__(self, other):
        import operator
        return self._cmp(other, operator.eq)

    def __ne__(self, other):
        import operator
        return self._cmp(other, operator.ne)

    def __lt__(self, other):
        import operator
        return self._cmp(other, operator.lt)

    def __le__(self, other):
        import operator
        return self._cmp(other, operator.le)

    def __gt__(self, other):
        import operator
        return self._cmp(other, operator.gt)

    def __ge__(self, other):
        import operator
        return self._cmp(other, operator.ge)

    def __hash__(self):
        # Required because __eq__ is overridden; hash by row scalar so it can
        # appear in dict keys / sets when scalar-like.
        try:
            return hash(self._row)
        except TypeError:
            return id(self)


def _extract_sub_ids(sched):
    """Pull the per-schedule subinstrument_id list from a generate_schedules result.

    Returns None if the schedule isn't in the per-sub-instrument shape.
    """
    if isinstance(sched, list) and sched and isinstance(sched[0], dict) and 'schedule' in sched[0]:
        return [r.get('subinstrument_id') for r in sched]
    return None


def schedule_sum(sched: List[Dict[str, Any]], column: str) -> float:
    """Sum a column from a schedule"""
    if _in_schedule_eval():
        raise ValueError("schedule_sum cannot be called from inside schedule column expressions; compute totals after schedule generation")
    if not sched:
        return 0

    # Always return a list of totals — one entry per subInstrumentId/schedule.
    def _sum_rows(rows):
        if not rows:
            return 0
        return sum(row.get(column, 0) for row in rows if isinstance(row.get(column), (int, float)))

    # generate_schedules results: list of result dicts -> return list of totals
    if isinstance(sched, list) and sched and isinstance(sched[0], dict) and 'schedule' in sched[0]:
        return _ScheduleValueList(
            (_sum_rows(r.get('schedule', []) or []) for r in sched),
            subinstrument_ids=_extract_sub_ids(sched),
        )

    # list of schedule arrays (multiple schedules passed as list) -> return list of totals
    if isinstance(sched, list) and sched and isinstance(sched[0], list):
        return [_sum_rows(s) for s in sched]

    # single schedule (list of rows) -> return scalar total
    return _sum_rows(sched)





def schedule_last(sched: List[Dict[str, Any]], column: str) -> float:
    """Get the last value of a column in a schedule"""
    if _in_schedule_eval():
        raise ValueError("schedule_last cannot be called from inside schedule column expressions; compute after schedule generation")
    if not sched:
        return []

    def _last_single(rows):
        if not rows:
            return 0
        for row in reversed(rows):
            if column in row:
                return row[column]
        return 0

    # generate_schedules results -> list of last values
    if isinstance(sched, list) and sched and isinstance(sched[0], dict) and 'schedule' in sched[0]:
        return _ScheduleValueList(
            (_last_single(r.get('schedule', []) or []) for r in sched),
            subinstrument_ids=_extract_sub_ids(sched),
        )

    # list of schedule arrays -> list of last values
    if isinstance(sched, list) and sched and isinstance(sched[0], list):
        return [_last_single(s) for s in sched]

    # single schedule -> scalar last value
    return _last_single(sched)


def schedule_first(sched: List[Dict[str, Any]], column: str) -> float:
    """Get the first value of a column in a schedule"""
    if _in_schedule_eval():
        raise ValueError("schedule_first cannot be called from inside schedule column expressions; compute after schedule generation")
    if not sched:
        return []

    def _first_single(rows):
        if not rows:
            return 0
        for row in rows:
            if column in row:
                return row[column]
        return 0

    # generate_schedules results -> list of first values
    if isinstance(sched, list) and sched and isinstance(sched[0], dict) and 'schedule' in sched[0]:
        return _ScheduleValueList(
            (_first_single(r.get('schedule', []) or []) for r in sched),
            subinstrument_ids=_extract_sub_ids(sched),
        )

    # list of schedule arrays -> list of first values
    if isinstance(sched, list) and sched and isinstance(sched[0], list):
        return [_first_single(s) for s in sched]

    # single schedule -> scalar first value
    return _first_single(sched)


def schedule_column(sched: List[Dict[str, Any]], column: str) -> List[Any]:
    """Return the values of a column from a schedule.

    - For a single schedule (list of rows) returns a list of column values.
    - For multiple schedules (generated by `schedule()` as list of {'schedule': [...]})
      returns a list of lists where each inner list contains the column values for that schedule.
    - For a list of schedule arrays (list of lists) returns a list of lists.

    Compatible with multi-subInstrument schedule architecture.
    """
    if _in_schedule_eval():
        raise ValueError("schedule_column cannot be called from inside schedule column expressions; compute after schedule generation")
    if not sched:
        return []

    def _col_values(rows):
        if not rows:
            return []
        return [row.get(column, 0) for row in rows]

    # generate_schedules results: list of dicts with 'schedule' key
    if isinstance(sched, list) and sched and isinstance(sched[0], dict) and 'schedule' in sched[0]:
        return _ScheduleValueList(
            (_col_values(r.get('schedule', []) or []) for r in sched),
            subinstrument_ids=_extract_sub_ids(sched),
        )

    # list of schedule arrays -> list of lists
    if isinstance(sched, list) and sched and isinstance(sched[0], list):
        return [_col_values(s) for s in sched]

    # single schedule -> list of values
    return _col_values(sched)


def schedule_filter(sched: List[Dict[str, Any]], match_column: str, match_value: Any, return_column: str) -> List[Any]:
    """
    For each schedule (or schedule result), find the first row where
    `row[match_column] == match_value` and return the value from
    `return_column` for that row.

    Always uses equality matching. Returns a list of values — one entry
    per subInstrumentId/schedule (single-entry list for single schedule).
    If no matching row is found for a schedule, returns 0 for that entry.
    """
    if _in_schedule_eval():
        raise ValueError("schedule_filter cannot be called from inside schedule column expressions; compute filters after schedule generation")

    if not sched:
        return []

    def _find_value(rows, sched_ctx=None):
        if not rows:
            return 0

        # Get DSL functions for use in evaluation
        dsl_funcs = globals().get('DSL_FUNCTIONS', {})

        # If match_value is an expression, we'll evaluate it per-schedule (using sched_ctx)
        needs_eval_match = isinstance(match_value, str) and ("(" in match_value or ")" in match_value)

        # Pre-evaluate match_value per-schedule if possible
        evaluated_match = None
        if needs_eval_match and sched_ctx:
            eval_ctx = {}
            eval_ctx.update(dsl_funcs)
            # include schedule-level context (like posting_date, amount, etc.)
            if isinstance(sched_ctx, dict):
                eval_ctx.update(sched_ctx)
            try:
                evaluated_match = safe_eval_expression(match_value, eval_ctx)
            except Exception:
                evaluated_match = None

        for row in rows:
            # Build evaluation context for this row
            eval_ctx = {}
            eval_ctx.update(dsl_funcs)
            if isinstance(sched_ctx, dict):
                eval_ctx.update(sched_ctx)
            # row values should override schedule-level keys where applicable
            if isinstance(row, dict):
                eval_ctx.update(row)

            # Determine row_val: direct lookup if column exists, otherwise evaluate expression
            row_val = None
            if isinstance(match_column, str) and isinstance(row, dict) and match_column in row:
                row_val = row.get(match_column)
            else:
                if isinstance(match_column, str):
                    try:
                        row_val = safe_eval_expression(match_column, eval_ctx)
                    except Exception:
                        row_val = None

            if row_val is None:
                continue

            # Normalize row value for comparison
            try:
                rv = normalize_date(row_val) if isinstance(row_val, str) else row_val
            except Exception:
                rv = row_val

            # Determine match value to compare against: per-schedule evaluated_match if available,
            # otherwise evaluate per-row using same context, or fall back to literal
            if evaluated_match is not None:
                mv = evaluated_match
            else:
                # If match_value is a simple variable name and present in schedule-level context, use it
                if isinstance(match_value, str) and isinstance(sched_ctx, dict) and match_value in sched_ctx:
                    mv = sched_ctx.get(match_value)
                elif needs_eval_match:
                    try:
                        mv = safe_eval_expression(match_value, eval_ctx)
                    except Exception:
                        mv = match_value
                else:
                    try:
                        mv = normalize_date(match_value) if isinstance(match_value, str) else match_value
                    except Exception:
                        mv = match_value

            if str(rv) == str(mv):
                return row.get(return_column, 0)

        return 0

    # generate_schedules results (each item includes schedule + context like posting_date)
    if isinstance(sched, list) and sched and isinstance(sched[0], dict) and 'schedule' in sched[0]:
        return _ScheduleValueList(
            (_find_value(r.get('schedule', []) or [], r) for r in sched),
            subinstrument_ids=_extract_sub_ids(sched),
        )

    # list of schedule arrays
    if isinstance(sched, list) and sched and isinstance(sched[0], list):
        return [_find_value(s) for s in sched]

    # single schedule -> single-entry list
    return [_find_value(sched)]
    


# ============= Generic Multi-Item Schedule Generation =============

# Pre-defined schedule templates for common accounting use cases
SCHEDULE_TEMPLATES = {
    "revenue": {
        "description": "Revenue recognition (ASC 606) - daily proration",
        "columns": {
            "period_date": "period_date",
            "days_in_period": "add(days_between(start_of_month(period_date), end_of_month(period_date)), 1)",
            "daily_amount": "divide(amount, daily_basis)",
            "period_amount": "multiply(daily_amount, days_in_period)"
        }
    },
    "straight_line": {
        "description": "Straight-line amortization (equal periods)",
        "columns": {
            "period_date": "period_date",
            "period_number": "add(period_index, 1)",
            "period_amount": "divide(amount, total_periods)",
            "cumulative": "multiply(period_amount, add(period_index, 1))",
            "remaining": "subtract(amount, cumulative)"
        }
    },
    "accrual": {
        "description": "Interest/fee accrual - daily basis",
        "columns": {
            "period_date": "period_date",
            "days_in_period": "add(days_between(start_of_month(period_date), end_of_month(period_date)), 1)",
            "daily_rate": "divide(rate, daily_basis)",
            "period_accrual": "multiply(multiply(amount, daily_rate), days_in_period)",
            "cumulative_accrual": "lag('cumulative_accrual', 1, 0) + period_accrual"
        }
    },
    "fas91": {
        "description": "FAS-91 fee amortization - effective interest method",
        "columns": {
            "period_date": "period_date",
            "period_number": "add(period_index, 1)",
            "opening_balance": "lag('closing_balance', 1, amount)",
            "period_amortization": "divide(amount, total_periods)",
            "closing_balance": "subtract(opening_balance, period_amortization)"
        }
    },
    "depreciation": {
        "description": "Asset depreciation - straight line",
        "columns": {
            "period_date": "period_date",
            "period_number": "add(period_index, 1)",
            "opening_value": "lag('closing_value', 1, amount)",
            "period_depreciation": "divide(subtract(amount, salvage_value), total_periods)",
            "accumulated_depreciation": "multiply(period_depreciation, add(period_index, 1))",
            "closing_value": "subtract(amount, accumulated_depreciation)"
        }
    },
    "lease": {
        "description": "Lease schedule (ASC 842) - straight line",
        "columns": {
            "period_date": "period_date",
            "period_number": "add(period_index, 1)",
            "lease_expense": "divide(amount, total_periods)",
            "cumulative_expense": "multiply(lease_expense, add(period_index, 1))",
            "remaining_liability": "subtract(amount, cumulative_expense)"
        }
    }
}


def generate_schedules(
    amounts: List[float],
    start_dates: List[str],
    end_dates: List[str],
    columns: Dict[str, str],
    freq: str = "M",
    context: Dict[str, Any] = None,
    item_names: List[str] = None,
    subinstrument_ids: List[str] = None
) -> List[Dict[str, Any]]:
    """
    Generate schedules for multiple items - FULLY GENERIC.
    
    Creates one schedule per item. Works for any time-based allocation:
    - Revenue recognition (ASC 606)
    - Expense amortization
    - Accrual schedules
    - FAS-91 fee amortization
    - Asset depreciation
    - Lease schedules (ASC 842)
    
    Args:
        amounts: Array of amounts per item (revenue, cost, principal, etc.)
        start_dates: Array of start dates per item
        end_dates: Array of end dates per item
        columns: Column definitions - dict of column_name: DSL_expression
        freq: Frequency - M (monthly), Q (quarterly), A (annual), W (weekly), D (daily)
        context: Additional variables for expressions (e.g., {"rate": 0.05})
        item_names: Optional names for each item (for display)
        subinstrument_ids: Optional sub-instrument IDs for each item
    
    Returns:
        List of schedule result objects, each containing:
        - item_index: Index of the item
        - item_name: Name (if provided)
        - subinstrument_id: Sub-instrument ID (if provided)
        - amount: Original amount
        - start_date: Start date
        - end_date: End date
        - total_periods: Number of periods
        - schedule: The generated schedule (array of rows)
        - total: Sum of period amounts
    
    Available variables in column expressions:
        - amount: The allocated amount for this item
        - total_periods: Number of periods in schedule
        - period_date: Current period's date
        - period_index: Current period index (0, 1, 2...)
        - daily_basis: 365 (default)
        - item_name: Name of current item
        - subinstrument_id: Subinstrument ID
        - Any variables passed in context
    
    Example:
        results = generate_schedules(
            [800, 400, 0],
            ["2026-01-01", "2026-01-01", "2026-01-01"],
            ["2026-12-31", "2026-06-30", "2026-12-31"],
            {
                "period_date": "period_date",
                "days_in_period": "add(days_between(start_of_month(period_date), end_of_month(period_date)), 1)",
                "daily_revenue": "divide(amount, 365)",
                "period_revenue": "multiply(daily_revenue, days_in_period)"
            },
            "M",
            None,
            ["Product A", "Product B", "Discount"],
            ["PROD-001", "PROD-002", "DISC-001"]
        )
    """
    if not amounts or not start_dates or not end_dates or not columns:
        return []
    
    n = min(len(amounts), len(start_dates), len(end_dates))
    results = []
    
    for i in range(n):
        amount = amounts[i] if i < len(amounts) else 0
        start = start_dates[i] if i < len(start_dates) else None
        end = end_dates[i] if i < len(end_dates) else None
        name = item_names[i] if item_names and i < len(item_names) else f"Item {i + 1}"
        subinstrument = subinstrument_ids[i] if subinstrument_ids and i < len(subinstrument_ids) else str(i + 1)
        
        result = {
            "item_index": i,
            "item_name": name,
            "subinstrument_id": subinstrument,
            "amount": amount,
            "start_date": start,
            "end_date": end,
            "total_periods": 0,
            "schedule": [],
            "total": 0
        }

        # Expose user-provided context on the result early so zero-amount items
        # still carry context variables (e.g., posting_date) for helpers.
        if context and isinstance(context, dict):
            for k, v in context.items():
                if k not in result:
                    result[k] = v
        
        # If start or end date missing for this item, skip schedule generation
        # for this sub-instrument but emit an empty placeholder row. This keeps
        # downstream arrays (schedule_filter, schedule_sum, etc.) index-aligned
        # with parallel collect_by_instrument() arrays so the
        # subinstrument↔value relationship is preserved across all steps.
        if not start or not end:
            result["_skipped_reason"] = "missing start_date or end_date"
            results.append(result)
            continue

        # Generate period definition
        period_def = period(start, end, freq)
        total_periods = len(period_def.get("dates", []))
        result["total_periods"] = total_periods

        # Skip items with zero amount only when no other context arrays exist
        # AND at least one column formula actually references 'amount' — pure
        # calendar/date schedules (e.g. period_date, month_end) should always
        # generate rows even when no monetary amount is provided.
        _skip_keys = ('amounts', 'amount', 'start_dates', 'end_dates',
                      'subinstrument_ids', 'item_names', 'product_names')
        has_other_arrays = context and any(
            isinstance(v, list) and k not in _skip_keys
            for k, v in context.items()
        )
        has_amount_col = any('amount' in str(v) for v in columns.values())
        if not has_other_arrays and (not amount or amount == 0) and has_amount_col:
            results.append(result)
            continue
        
        # Build context for schedule expressions
        sched_context = {
            "amount": amount,
            "total_periods": total_periods,
            "daily_basis": 365,
            "item_name": name,
            "subinstrument_id": subinstrument,
            "start_date": start,
            "end_date": end
        }
        
        # Add user-provided context — auto-extract per-item values from arrays
        if context:
            for k, v in context.items():
                if k in _skip_keys:
                    continue  # already handled by dedicated parameters
                if isinstance(v, list):
                    sched_context[k] = v[i] if i < len(v) else (v[-1] if v else None)
                else:
                    sched_context[k] = v
        
        # Generate the schedule
        sched = schedule(period_def, columns, sched_context)
        result["schedule"] = sched
        
        # Calculate total (look for period_amount, period_revenue, period_accrual, etc.)
        total = 0
        amount_columns = ["period_amount", "period_revenue", "period_accrual", 
                         "period_amortization", "period_depreciation", "lease_expense"]
        for col in amount_columns:
            if sched and col in sched[0]:
                total = schedule_sum(sched, col)
                break
        result["total"] = total
        
        results.append(result)
    
    return results


def get_schedules_array(results: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    """
    Extract just the schedule arrays from generate_schedules results.
    
    Useful for passing to print_all_schedules or other functions.
    
    Args:
        results: Results from generate_schedules()
    
    Returns:
        Array of schedule arrays
    """
    return [r.get("schedule", []) for r in results]


def get_schedule_totals(results: List[Dict[str, Any]], column: str = None) -> List[float]:
    """
    Extract the totals from generate_schedules results.
    
    Args:
        results: Results from generate_schedules()
        column: Optional column name to sum. If not provided, uses the default total.
    
    Returns:
        Array of totals (one per item)
    """
    if column:
        return [schedule_sum(r.get("schedule", []), column) for r in results]
    else:
        return [r.get("total", 0) for r in results]


def find_period_amounts(
    results: List[Dict[str, Any]],
    posting_date: str,
    amount_column: str = None
) -> List[Dict[str, Any]]:
    """
    Find the period amounts for each item based on posting date.
    
    Args:
        results: Results from generate_schedules()
        posting_date: The posting date to match
        amount_column: Column name to extract (auto-detected if not specified)
    
    Returns:
        Array of recognition results with:
        - item_index, item_name, subinstrument_id
        - period_date: Matched period date
        - period_amount: Amount for that period
    """
    if not results or not posting_date:
        return []
    
    recognition = []
    
    for r in results:
        sched = r.get("schedule", [])
        rec = {
            "item_index": r.get("item_index"),
            "item_name": r.get("item_name"),
            "subinstrument_id": r.get("subinstrument_id"),
            "period_date": None,
            "period_amount": 0
        }
        
        if sched:
            # Inline period-match logic (previously schedule_find_period)
            try:
                nd_post = normalize_date(posting_date)
                if not nd_post:
                    matched_row = {}
                    raise ValueError("invalid posting_date")
                target = datetime.fromisoformat(str(nd_post))
                target_year_month = (target.year, target.month)
                matched_row = {}
                for row in sched:
                    period_date_str = row.get("period_date")
                    if period_date_str:
                        try:
                            nd_pd = normalize_date(period_date_str)
                            if not nd_pd:
                                continue
                            period_date = datetime.fromisoformat(str(nd_pd))
                        except Exception:
                            continue
                        if (period_date.year, period_date.month) == target_year_month:
                            matched_row = row
                            break
            except (ValueError, TypeError):
                matched_row = {}

            if matched_row:
                rec["period_date"] = matched_row.get("period_date")

                # Auto-detect amount column
                if amount_column and amount_column in matched_row:
                    rec["period_amount"] = matched_row.get(amount_column, 0)
                else:
                    # Try common column names
                    for col in ["period_amount", "period_revenue", "period_accrual",
                               "period_amortization", "period_depreciation", "lease_expense"]:
                        if col in matched_row:
                            rec["period_amount"] = matched_row.get(col, 0)
                            break
        
        recognition.append(rec)
    
    return recognition


def create_schedule_transactions(
    recognition_results: List[Dict[str, Any]],
    posting_date: str,
    transaction_type: str = "Schedule Entry"
) -> List[Dict[str, Any]]:
    """
    Create transactions from schedule recognition results.
    
    Only creates transactions for items with non-zero amounts.
    
    Args:
        recognition_results: Results from find_period_amounts()
        posting_date: Transaction posting date
        transaction_type: Transaction type/description
    
    Returns:
        List of created transactions
    """
    global _transaction_results, _current_instrumentid
    
    created = []
    
    for r in recognition_results:
        amount = r.get("period_amount", 0)
        if amount and amount != 0:
            txn = createTransaction(
                posting_date,
                posting_date,
                transaction_type,
                amount,
                r.get("subinstrument_id", "1")
            )
            if txn:
                created.append(txn)
    
    return created


def print_schedule(sched: List[Dict[str, Any]], title: str = "Schedule") -> List[Dict[str, Any]]:
    """
    Print a schedule as a formatted table in the console.
    
    Args:
        sched: Schedule to print
        title: Title for the schedule
    
    Returns:
        The schedule (for chaining)
    """
    import json
    
    if not sched:
        _dsl_print(f"{title}: (Empty schedule)")
        return sched
    
    _dsl_print(f"═══ {title} ═══")
    # Tag each printed row with the current (_instrumentid, _postingdate) so
    # the Business Preview can strictly scope schedule display to the
    # filtered instrument AND selected posting date. We clone the rows so the
    # caller's in-memory schedule object is untouched.
    try:
        _iid = _get_current_instrumentid()
        _pd = _get_current_postingdate()
        tagged = []
        for _r in sched:
            if isinstance(_r, dict):
                _row = dict(_r)
                if '_instrumentid' not in _row:
                    _row['_instrumentid'] = _iid
                if '_postingdate' not in _row and _pd:
                    _row['_postingdate'] = _pd
                tagged.append(_row)
            else:
                tagged.append(_r)
    except Exception:
        tagged = sched
    try:
        _dsl_print(json.dumps(tagged, indent=2, default=str))
    except Exception:
        _dsl_print(str(tagged))
    
    return sched


def print_all_schedules(
    schedules_or_results: List[Any],
    item_names: List[str] = None
) -> List[Any]:
    """
    Print all schedules from generate_schedules results or schedule arrays.
    
    Args:
        schedules_or_results: Results from generate_schedules() or array of schedules
        item_names: Optional names for each schedule
    
    Returns:
        The input (for chaining)
    """
    if not schedules_or_results:
        return schedules_or_results
    
    # Check if this is generate_schedules results (has 'schedule' key) or raw schedule arrays
    if schedules_or_results and isinstance(schedules_or_results[0], dict) and 'schedule' in schedules_or_results[0]:
        # This is generate_schedules results
        for i, result in enumerate(schedules_or_results):
            name = result.get('item_name', item_names[i] if item_names and i < len(item_names) else f"Item {i + 1}")
            sched = result.get('schedule', [])
            if sched:
                print_schedule(sched, f"{name} Schedule")
            else:
                _dsl_print(f"{name}: (No schedule - zero amount)")
    else:
        # This is array of schedule arrays
        for i, sched in enumerate(schedules_or_results):
            name = item_names[i] if item_names and i < len(item_names) else f"Schedule {i + 1}"
            if sched:
                print_schedule(sched, name)
            else:
                _dsl_print(f"{name}: (Empty)")
    
    return schedules_or_results


# String Functions
def lower(s: str) -> str:
    """Convert string to lowercase"""
    return str(s).lower() if s is not None else ""

def upper(s: str) -> str:
    """Convert string to uppercase"""
    return str(s).upper() if s is not None else ""

def concat(*args) -> str:
    """Concatenate multiple strings"""
    return "".join(str(a) for a in args if a is not None)

def contains(s: str, substring: str) -> bool:
    """Check if string contains substring"""
    if s is None or substring is None:
        return False
    return str(substring) in str(s)

def eq_ignore_case(a: str, b: str) -> bool:
    """Case-insensitive string equality (also trims surrounding whitespace)."""
    if a is None or b is None:
        return a is None and b is None
    return str(a).strip().lower() == str(b).strip().lower()



def trim(s: str) -> str:
    """Remove leading and trailing whitespace"""
    return str(s).strip() if s is not None else ""

def str_length(s: str) -> int:
    """Get string length"""
    return len(str(s)) if s is not None else 0


# Aggregation
def sum_vals(col: List[float]) -> float:
    return sum(to_number(x) for x in col)

def sum_field(array: List[Dict], field: str) -> float:
    """Sum a specific field from an array of objects/dictionaries.
    
    Args:
        array: List of dictionaries
        field: Name of field to sum
        
    Returns:
        Sum of field values (None values treated as 0)
        
    Example:
        sum_field(recognition_results, "period_amount")
    """
    total = 0
    for item in array:
        if isinstance(item, dict):
            val = item.get(field)
            total += to_number(val)
    return total

def avg(col: List[float]) -> float:
    filtered = [to_number(x) for x in col]
    return sum(filtered) / len(filtered) if filtered else 0

def min_val(*args):
    """
    Flexible min implementation for the DSL.

    Supported calling forms:
    - `min(list)` -> returns minimum of the list (or 0 for empty)
    - `min(a, b, c)` -> returns minimum of scalar args
    - `min(list1, list2, ...)` -> returns element-wise minimum as a list (length = shortest list)
    - `min(list, scalar, ...)` -> element-wise minimum between list items and scalars
    """
    if not args:
        return 0
    if len(args) == 1:
        col = args[0]
        if isinstance(col, list):
            return min(col) if col else 0
        return col if col is not None else 0

    # multiple arguments
    lists = [a for a in args if isinstance(a, list)]
    scalars = [a for a in args if not isinstance(a, list)]

    # No list arguments -> simple scalar min
    if not lists:
        try:
            return min(args)
        except TypeError:
            return args[0]

    # At least one list: perform element-wise min across lists and scalars
    min_len = min(len(l) for l in lists)
    result = []
    for i in range(min_len):
        vals = [l[i] for l in lists] + scalars
        cleaned = [v for v in vals if v is not None]
        if not cleaned:
            result.append(0)
        else:
            result.append(min(cleaned))
    return result

def max_val(*args):
    """
    Flexible max implementation mirroring `min_val` semantics.
    """
    if not args:
        return 0
    if len(args) == 1:
        col = args[0]
        if isinstance(col, list):
            return max(col) if col else 0
        return col if col is not None else 0

    lists = [a for a in args if isinstance(a, list)]
    scalars = [a for a in args if not isinstance(a, list)]

    if not lists:
        try:
            return max(args)
        except TypeError:
            return args[0]

    max_len = min(len(l) for l in lists)
    result = []
    for i in range(max_len):
        vals = [l[i] for l in lists] + scalars
        cleaned = [v for v in vals if v is not None]
        if not cleaned:
            result.append(0)
        else:
            result.append(max(cleaned))
    return result

def count(col: List[Any]) -> int:
    return len(col)

def weighted_avg(v: List[float], w: List[float]) -> float:
    if not v or not w or len(v) != len(w):
        return 0
    total_weight = sum(w)
    return sum(vi * wi for vi, wi in zip(v, w)) / total_weight if total_weight > 0 else 0

def cumulative_sum(col: List[float]) -> List[float]:
    result = []
    total = 0
    for val in col:
        total += val
        result.append(total)
    return result

def median(col: List[float]) -> float:
    if not col:
        return 0
    sorted_col = sorted(col)
    n = len(sorted_col)
    if n % 2 == 0:
        return (sorted_col[n//2-1] + sorted_col[n//2]) / 2
    return sorted_col[n//2]


def std_dev(col: List[float]) -> float:
    return math.sqrt(variance(col))


def range_val(col: List[float]) -> float:
    """Range of values"""
    return max_val(col) - min_val(col) if col else 0

# Conversion






# Statistical




# ============= Transaction Functions =============

# Global list to store transactions created by createTransaction
_transaction_results = []

# Global list to store print outputs from print_schedule functions
_print_outputs = []

# Global print function that can be overridden by server.py
_dsl_print_func = None

def _set_dsl_print(print_func):
    """Set the DSL print function (called from server.py generated code)"""
    global _dsl_print_func
    _dsl_print_func = print_func

def _dsl_print(msg):
    """Internal print that uses the DSL print function if set, otherwise appends to _print_outputs"""
    global _dsl_print_func, _print_outputs
    if _dsl_print_func:
        _dsl_print_func(msg)
    else:
        _print_outputs.append(str(msg))

def _set_print_outputs(outputs_list):
    """Set the global print outputs list (called from server.py)"""
    global _print_outputs
    _print_outputs = outputs_list

def _get_print_outputs():
    """Get the global print outputs list"""
    global _print_outputs
    return _print_outputs

def _clear_print_outputs():
    """Clear the print outputs list"""
    global _print_outputs
    _print_outputs = []

def _set_transaction_results(results_list):
    """Set the global transaction results list (called from server.py)"""
    global _transaction_results
    _transaction_results = results_list

def _get_transaction_results():
    """Get the global transaction results list"""
    global _transaction_results
    return _transaction_results

def _clear_transaction_results():
    """Clear the transaction results list"""
    global _transaction_results
    _transaction_results = []

# Global variable to hold the current instrumentid (set by server.py during execution)
_current_instrumentid = "STANDALONE"

# Global variable to hold the current postingdate (set by server.py during execution
# alongside _current_instrumentid). Used by print_schedule() to tag emitted schedule
# rows so the Business Preview can scope them to the right (instrument, posting date).
_current_postingdate = ""

# Guard to detect evaluation inside `schedule()` to prevent helper re-entrancy
_in_schedule_evaluation = 0

def _set_current_instrumentid(instrumentid: str):
    """Set the current instrumentid for transactions"""
    global _current_instrumentid
    _current_instrumentid = instrumentid

def _get_current_instrumentid():
    """Get the current instrumentid"""
    global _current_instrumentid
    return _current_instrumentid

def _set_current_postingdate(postingdate: str):
    """Set the current postingdate (used to tag emitted schedule rows)."""
    global _current_postingdate
    _current_postingdate = str(postingdate) if postingdate is not None else ""

def _get_current_postingdate():
    """Get the current postingdate."""
    global _current_postingdate
    return _current_postingdate


def _in_schedule_eval():
    """Return True if we are currently evaluating a schedule's column expressions."""
    global _in_schedule_evaluation
    return _in_schedule_evaluation > 0

def createTransaction(postingdate: Any, effectivedate: Any, transactiontype: Any, amount: Any, subinstrumentid: Any = '1') -> Any:
    """
    Create a transaction with all required fields.
    
    This is the ONLY way to emit transactions in DSL code.
    The instrumentid is automatically set based on the current data row context.
    
    If postingdate or effectivedate is empty/missing (indicating event data not found),
    the transaction will be skipped gracefully.
    
    Args:
        postingdate: Transaction posting date (YYYY-MM-DD format)
        effectivedate: Transaction effective date (YYYY-MM-DD format)
        transactiontype: Type/description of the transaction
        amount: Transaction amount
        subinstrumentid: Sub-instrument identifier (default '1.0')
    
    Returns:
        The created transaction dictionary, or None if skipped due to missing dates
    
    Example:
        createTransaction("2024-01-15", "2024-01-15", "Interest Accrual", 1250.50)
        createTransaction(postingdate, effectivedate, "Fee Income", fee_amount, "PROD-001")
    """
    global _transaction_results, _current_instrumentid

    # Helper to normalize input to list
    def _to_list(x):
        if x is None:
            return []
        if isinstance(x, (list, tuple)):
            return list(x)
        return [x]

    posting_list = _to_list(postingdate)
    effective_list = _to_list(effectivedate)
    type_list = _to_list(transactiontype)
    amount_list = _to_list(amount)
    sub_list = _to_list(subinstrumentid) or ['1.0']

    # If `amount` is a schedule-derived list that carries the per-entry
    # sub-instrument ids (e.g., from schedule_filter / schedule_sum), prefer
    # those ids — they guarantee per-row alignment of amount → sub-instrument.
    # The caller-supplied sub_list is overridden because passing a separately
    # collected sub-id list (different ordering) is the most common cause of
    # misaligned transactions.
    embedded_sub_ids = getattr(amount, 'subinstrument_ids', None)
    if embedded_sub_ids and isinstance(amount, list) and len(embedded_sub_ids) == len(amount):
        sub_list = [str(s) if s is not None else '1.0' for s in embedded_sub_ids]

    created = []

    # Ensure at least one amount to create (if no amounts, nothing to do)
    if not amount_list:
        return None

    # Candidate keys when amount is a dict
    AMOUNT_KEYS = ["period_amount", "period_revenue", "period_accrual", "period_amortization", "amount", "value"]
    DATE_KEYS = ["period_date", "postingdate", "posting_date", "date"]

    # Helper to pick value from a val_list mapping to sub or amount index
    def pick(val_list, si, ai):
        if not val_list:
            return None
        # exact per-sub mapping
        if len(val_list) == len(sub_list):
            return val_list[si]
        # per-amount mapping
        if len(val_list) == len(amount_list):
            return val_list[ai]
        # otherwise return first (scalar)
        return val_list[0]

    # Helper to extract numeric amount from various shapes
    def extract_amount(entry, si, ai):
        # Nested lists are not allowed in the new model (would create multiple
        # transactions per sub-instrument and lead to Cartesian products). Reject.
        if isinstance(entry, (list, tuple)):
            raise ValueError("Nested amount arrays are not supported; provide a flat array matching subInstrumentIds or a single scalar amount.")

        if isinstance(entry, dict):
            # Try candidate keys for numeric amount
            for k in AMOUNT_KEYS:
                if k in entry and entry[k] is not None:
                    try:
                        return float(entry[k])
                    except Exception:
                        pass
            # Fallback: try any numeric-like value
            for v in entry.values():
                try:
                    return float(v)
                except Exception:
                    continue
            return 0.0

        # Scalar
        try:
            return float(entry) if entry is not None else 0.0
        except Exception:
            return 0.0

    # Normalize mapping: all fields are mapped by sub-instrument index.
    # If sub_list has 1 item but amount_list has M items, broadcast sub_list to M
    # (M transactions all mapped to the same sub-instrument).
    if len(sub_list) == 1 and len(amount_list) > 1:
        sub_list = sub_list * len(amount_list)
    # If amount_list is shorter than sub_list, only create transactions for the
    # first len(amount_list) sub-instruments. This supports the case where the
    # variable provides amounts for a subset of sub-instruments.
    if 1 < len(amount_list) < len(sub_list):
        sub_list = sub_list[:len(amount_list)]
    N = len(sub_list)

    def _validate_and_broadcast(vals, name):
        if not vals:
            return [None] * N
        if len(vals) == 1:
            return [vals[0]] * N
        if len(vals) == N:
            return list(vals)
        if len(vals) > N:
            return list(vals)[:N]
        raise ValueError(f"Length of '{name}' ({len(vals)}) must be 1 or equal to number of subInstrumentIds ({N})")

    posting_map = _validate_and_broadcast(posting_list, 'postingdate')
    effective_map = _validate_and_broadcast(effective_list, 'effectivedate')
    type_map = _validate_and_broadcast(type_list, 'transactiontype')

    # Amounts: allow single scalar broadcast or per-sub list of length N.
    if len(amount_list) == 1:
        amount_map = [amount_list[0]] * N
    elif len(amount_list) == N:
        amount_map = list(amount_list)
    elif len(amount_list) > N:
        amount_map = list(amount_list)[:N]
    else:
        raise ValueError(f"Length of 'amount' ({len(amount_list)}) must be 1 or equal to number of subInstrumentIds ({N})")

    # Create exactly one transaction per sub-instrument (unless skipped due to missing dates)
    for i in range(N):
        sub_id_raw = sub_list[i]
        sub_id = str(sub_id_raw).strip() if sub_id_raw is not None else '1.0'
        if not sub_id or sub_id == 'None':
            sub_id = '1.0'

        posting_raw = posting_map[i]
        effective_raw = effective_map[i]
        type_raw = type_map[i]

        # If posting/effective provided as dicts, extract a date field
        if isinstance(posting_raw, dict):
            for k in DATE_KEYS:
                if k in posting_raw and posting_raw[k]:
                    posting_raw = posting_raw[k]
                    break
        if isinstance(effective_raw, dict):
            for k in DATE_KEYS:
                if k in effective_raw and effective_raw[k]:
                    effective_raw = effective_raw[k]
                    break

        posting_str = normalize_date(posting_raw) if posting_raw is not None else None
        effective_str = normalize_date(effective_raw) if effective_raw is not None else None

        # Skip creation if dates are missing
        if not posting_str or not effective_str:
            continue

        # Extract numeric amount (dict or scalar) - must be a single scalar per sub-instrument
        amt_entry = amount_map[i]
        amt_val = extract_amount(amt_entry, i, i)

        # Ensure a single scalar per sub-instrument (no nested lists)
        if isinstance(amt_val, (list, tuple)):
            raise ValueError("Amount entry for subInstrumentId must be a single scalar value; nested arrays are not supported")

        try:
            amt_num = float(amt_val) if amt_val is not None else 0.0
        except Exception:
            amt_num = 0.0

        # Round to 4 decimal places by default
        amt_num = round(amt_num, 4)

        txn = {
            'postingdate': posting_str,
            'effectivedate': effective_str,
            'instrumentid': _current_instrumentid,
            'subinstrumentid': sub_id,
            'transactiontype': str(type_raw) if type_raw is not None else '',
            'amount': amt_num
        }

        _transaction_results.append(txn)
        created.append(txn)

    if not created:
        return None
    return created[0] if len(created) == 1 else created


# ============= Iteration Functions =============

def for_each(dates_array: List[str], amounts_array: List[float], date_var: str, amount_var: str, expression: str) -> List[Dict[str, Any]]:
    """
    Iterate over paired arrays and execute an expression for each pair.
    Creates multiple transactions from multi-row event data.
    
    Args:
        dates_array: Array of effective dates (from collect())
        amounts_array: Array of amounts (from collect())
        date_var: Variable name for date in expression (e.g., "edate")
        amount_var: Variable name for amount in expression (e.g., "amt")
        expression: DSL expression to execute (typically createTransaction)
    
    Returns:
        List of results from each iteration
    
    Example:
        for_each(INT_ACC_effectivedates_arr, INT_ACC_amounts_arr,
            "edate", "amt", "createTransaction(postingdate, edate, 'Cash Flow', amt)")
    """
    global _current_instrumentid
    
    results = []
    
    # Ensure arrays are same length
    min_len = min(len(dates_array) if dates_array else 0, 
                  len(amounts_array) if amounts_array else 0)
    
    if min_len == 0:
        return results
    
    # Get DSL_FUNCTIONS from module globals to avoid import issues
    dsl_funcs = globals().get('DSL_FUNCTIONS', {})
    
    for i in range(min_len):
        # Create local context with current values
        local_context = {
            date_var: dates_array[i],
            amount_var: amounts_array[i],
            'index': i,
            'postingdate': dates_array[i],  # Also provide postingdate for convenience
        }
        
        # Add all DSL functions to context
        local_context.update(dsl_funcs)
        
        try:
            result = safe_eval_expression(expression, local_context)
            if result is not None:
                results.append(result)
        except Exception:
            # Skip failed iterations silently
            pass
    
    return results


def for_each_with_index(array: List[Any], var_name: str, expression: str, context: Dict[str, Any] = None) -> List[Any]:
    """
    Iterate over a single array and execute an expression for each element.
    
    Args:
        array: Array to iterate over
        var_name: Variable name for current element in expression
        expression: DSL expression to execute
        context: Optional dictionary of external variables (other arrays, totals, etc.)
    
    Returns:
        List of results from each iteration
    
    Example:
        for_each_with_index(amounts_arr, "amt", "amt * 1.1")
        
        # With context for accessing other arrays:
        for_each_with_index(product_names, "name", 
            "if(eq(name.lower(), 'discount'), 0, array_get(esp_values, index, 0))",
            {"esp_values": [1200, 800, -200]})
    """
    results = []
    
    if not array:
        return results
    
    # Get DSL_FUNCTIONS from module globals to avoid import issues
    dsl_funcs = globals().get('DSL_FUNCTIONS', {})
    
    for i, item in enumerate(array):
        local_context = {
            var_name: item,
            'index': i,
            'count': len(array),
        }
        # Add DSL functions
        local_context.update(dsl_funcs)
        # Add external context variables (can override DSL functions if needed)
        if context:
            local_context.update(context)
        
        try:
            # Allow only safe DSL expressions
            result = safe_eval_expression(expression, local_context)
            results.append(result)
        except Exception:
            results.append(None)
    
    return results




def apply_each(source, expr_or_second, expr_if_paired=None, context: Dict[str, Any] = None) -> List[Any]:
    """
    Apply a formula to each item in a list, or to each pair of items from two lists.
    Uses intuitive keywords: 'each' for the current item, 'first'/'second' for paired items.

    Single-array mode:
        apply_each(array, "formula using each")
        e.g. apply_each(prices, "multiply(each, 1.1)")

    Paired-array mode:
        apply_each(array1, array2, "formula using first and second")
        e.g. apply_each(quantities, prices, "multiply(first, second)")

    Magic variables available inside the formula:
        each  — current element (single mode) or alias for first (paired mode)
        first — element from the first array (paired mode)
        second— element from the second array (paired mode)
        index — 0-based position in the array
        count — total number of elements

    Args:
        source: The array to iterate over
        expr_or_second: Expression string (single mode) or second array (paired mode)
        expr_if_paired: Expression string when using paired mode (None for single mode)
        context: Optional dict of external variables referenced in the formula

    Returns:
        List of results from applying the formula to each element/pair
    """
    import logging
    logger = logging.getLogger(__name__)

    # Detect mode: if expr_or_second is a string, it's single-array mode
    if isinstance(expr_or_second, str):
        # Single-array mode — delegate to for_each_with_index with var_name="each"
        # 3rd arg could be context dict (passed as expr_if_paired positionally)
        ctx = expr_if_paired if isinstance(expr_if_paired, dict) else context
        enriched_context = dict(ctx) if ctx else {}
        return for_each_with_index(source, "each", expr_or_second, enriched_context)
    else:
        # Paired-array mode
        second_array = expr_or_second
        expression = expr_if_paired or ""

        if not source or not second_array:
            return []

        min_len = min(len(source), len(second_array))
        dsl_funcs = globals().get('DSL_FUNCTIONS', {})
        results = []

        for i in range(min_len):
            local_context = {
                'first': source[i],
                'second': second_array[i],
                'each': source[i],      # alias for first
                'index': i,
                'count': min_len,
            }
            local_context.update(dsl_funcs)
            if context:
                local_context.update(context)

            try:
                result = safe_eval_expression(expression, local_context)
                if result is None:
                    logger.debug(f"apply_each paired: expression returned None at index {i}")
                    results.append(0)
                else:
                    results.append(result)
            except Exception:
                results.append(0)

        return results




def array_length(array: List[Any]) -> int:
    """Get the length of an array."""
    return len(array) if array else 0


def array_get(array: List[Any], index: int, default: Any = None) -> Any:
    """Get element at index with optional default for out-of-bounds."""
    if not array or index < 0 or index >= len(array):
        return default
    return array[index]


def array_first(array: List[Any], default: Any = None) -> Any:
    """Get first element of array."""
    return array[0] if array else default


def array_last(array: List[Any], default: Any = None) -> Any:
    """Get last element of array."""
    return array[-1] if array else default


def array_slice(array: List[Any], start: int, end: int = None) -> List[Any]:
    """Get slice of array from start to end index."""
    if not array:
        return []
    if end is None:
        return array[start:]
    return array[start:end]


def array_reverse(array: List[Any]) -> List[Any]:
    """Reverse an array."""
    return list(reversed(array)) if array else []


def array_append(array: List[Any], item: Any) -> List[Any]:
    """Return a new array with `item` appended. Does not mutate input.

    If `array` is None, returns a single-item list [item].
    """
    base = list(array) if array else []
    base.append(item)
    return base


def array_extend(array: List[Any], items: List[Any]) -> List[Any]:
    """Return a new array with `items` concatenated to `array`.

    If `array` or `items` are None, treats them as empty lists.
    """
    base = list(array) if array else []
    ext = list(items) if items else []
    return base + ext


def array_filter(array: List[Any], var_name: str, condition: str, context: Dict[str, Any] = None) -> List[Any]:
    """
    Filter array elements based on a condition.
    
    Args:
        array: Array to filter
        var_name: Variable name for current element
        condition: Boolean expression to filter by
        context: Optional dictionary of external variables
    
    Returns:
        Filtered array
    
    Example:
        array_filter(amounts_arr, "x", "x > 1000")  # Keep amounts > 1000
        
        # With context:
        array_filter(names, "n", "neq(array_get(amounts, index, 0), 0)", {"amounts": [100, 0, 200]})
    """
    if not array:
        return []
    
    # Get DSL_FUNCTIONS from module globals to avoid import issues
    dsl_funcs = globals().get('DSL_FUNCTIONS', {})
    results = []
    
    for i, item in enumerate(array):
        local_context = {
            var_name: item,
            'index': i,
            'count': len(array),
        }
        local_context.update(dsl_funcs)
        if context:
            local_context.update(context)

        try:
            if safe_eval_expression(condition, local_context):
                results.append(item)
        except Exception:
            pass
    
    return results


# ============= Operator wrapper functions (explicit DSL APIs) =============
def op_eq(a: Any, b: Any) -> bool:
    return a == b

def op_neq(a: Any, b: Any) -> bool:
    return a != b

def op_gt(a: Any, b: Any) -> bool:
    return a > b

def op_gte(a: Any, b: Any) -> bool:
    return a >= b

def op_lt(a: Any, b: Any) -> bool:
    return a < b

def op_lte(a: Any, b: Any) -> bool:
    return a <= b

def op_add(a: Any, b: Any) -> Any:
    return a + b

def op_sub(a: Any, b: Any) -> Any:
    return a - b

def op_mul(a: Any, b: Any) -> Any:
    return a * b

def op_div(a: Any, b: Any) -> Any:
    a = to_number(a)
    b = to_number(b)
    if b == 0:
        raise ValueError("Division by zero")
    return a / b

def dsl_print(*args) -> None:
    """Expose print functionality to DSL (safe wrapper)."""
    try:
        # If single argument and it looks like schedules/results, delegate to print_all_schedules
        if len(args) == 1:
            obj = args[0]
            # generate_schedules results (list of dicts with 'schedule')
            if isinstance(obj, list) and obj:
                first = obj[0]
                if isinstance(first, dict) and 'schedule' in first:
                    print_all_schedules(obj)
                    return
                # array of schedule arrays (each element is list of rows)
                if isinstance(first, list):
                    # Heuristic: check if inner items look like schedule rows
                    inner_first = first[0] if first else None
                    if isinstance(inner_first, dict) and ('period_date' in inner_first or 'period_revenue' in inner_first or 'period_amount' in inner_first):
                        print_all_schedules(obj)
                        return
                    # If inner items are primitives, still treat as generic arrays
                    print_all_schedules(obj)
                    return
                # list of dicts that are themselves schedule rows
                if isinstance(first, dict) and ('period_date' in first or 'period_revenue' in first or 'period_amount' in first):
                    print_all_schedules([obj]) if isinstance(obj, list) else print_all_schedules([obj])
                    return
            # single result dict with 'schedule'
            if isinstance(obj, dict) and 'schedule' in obj:
                print_all_schedules([obj])
                return

        _dsl_print(' '.join(str(a) for a in args))
    except Exception:
        _dsl_print(' '.join(map(str, args)))

# Function Registry
DSL_FUNCTIONS = {
    'lookup': lookup,
    'normalize_arraydate': normalize_arraydate,
    'normalize_date': normalize_date,
    # Financial
    'pv': pv, 'fv': fv, 'pmt': pmt, 'rate': rate, 'nper': nper, 'npv': npv, 'irr': irr,
    'xnpv': xnpv, 'xirr': xirr,
    'discount_factor': discount_factor, 'accumulation_factor': accumulation_factor,
    'effective_rate': effective_rate, 'nominal_rate': nominal_rate, 'yield_to_maturity': yield_to_maturity,

    # Arithmetic
    'add': add, 'subtract': subtract, 'multiply': multiply, 'divide': divide,
    'power': power, 'abs': abs_val, 'sign': sign,
    'round': round_val, 'floor': floor, 'ceil': ceil,
    'truncate': truncate, 'percentage': percentage,
    # Operator wrappers (explicit secure operators)
    'op_eq': op_eq, 'op_neq': op_neq, 'op_gt': op_gt, 'op_gte': op_gte, 'op_lt': op_lt, 'op_lte': op_lte,
    'op_add': op_add, 'op_sub': op_sub, 'op_mul': op_mul, 'op_div': op_div,

    # Comparison
    'eq': eq, 'neq': neq, 'gt': gt, 'gte': gte, 'lt': lt, 'lte': lte,
    'between': between, 'is_null': is_null,

    # Logical
    'and': and_op, 'or': or_op, 'not': not_op,
    'all': all_op, 'any': any_op, 'if': if_op, 'iif': if_op,
    'coalesce': coalesce, 'switch': switch,

    # Date
    'days_between': days_between, 'months_between': months_between, 'years_between': years_between,
    'add_days': add_days, 'add_months': add_months, 'add_years': add_years,
    'subtract_days': subtract_days, 'subtract_months': subtract_months, 'subtract_years': subtract_years,
    'start_of_month': start_of_month, 'end_of_month': end_of_month,
    'day_count_fraction': day_count_fraction, 'is_leap_year': is_leap_year,
    'days_in_year': days_in_year, 'quarter': quarter, 'day_of_week': day_of_week,
    'is_weekend': is_weekend, 'business_days': business_days,

    # Schedule Functions
    'period': period, 'schedule': schedule,
    'schedule_sum': schedule_sum,
    'schedule_last': schedule_last, 'schedule_first': schedule_first,
    'schedule_column': schedule_column,
    'schedule_filter': schedule_filter,

    # Aggregation
    'sum': sum_vals, 'sum_field': sum_field, 'avg': avg, 'min': min_val, 'max': max_val, 'count': count,
    'weighted_avg': weighted_avg, 'cumulative_sum': cumulative_sum,
    'median': median, 'std_dev': std_dev,

    # String Functions
    'lower': lower, 'upper': upper, 'concat': concat, 'contains': contains,
    'eq_ignore_case': eq_ignore_case,
    'trim': trim, 'str_length': str_length,

    # Transaction
    'createTransaction': createTransaction,
    # Safe print wrapper
    'print': dsl_print,

    # Iteration & Array Operations
    'for_each': for_each, 'for_each_with_index': for_each_with_index,
    'apply_each': apply_each,
    'array_length': array_length, 'array_get': array_get,
    'array_first': array_first, 'array_last': array_last,
    'array_slice': array_slice, 'array_reverse': array_reverse,
    'array_append': array_append, 'array_extend': array_extend,
    'array_filter': array_filter,
}

# Function metadata for UI display (104 functions)
DSL_FUNCTION_METADATA = [
    {"name": "lookup", "params": "value_array, match_array, target_value", "description": "Search a list for a matching value and return the corresponding item from a second list. Returns null if no match is found.", "category": "Array Utilities"},
    {"name": "normalize_arraydate", "params": "array", "description": "Convert a list of dates written in various formats into the standard YYYY-MM-DD format.", "category": "Date"},

    # Financial (24)
    {"name": "pv", "params": "rate, n, pmt, fv=0, type=0", "description": "Calculate the present value of a series of future payments, given an interest rate and number of periods. Use type=1 if payments are made at the start of each period.", "category": "Financial"},
    {"name": "fv", "params": "rate, n, pmt, pv=0, type=0", "description": "Calculate the future value of an investment based on regular payments and a fixed interest rate. Use type=1 if payments are made at the start of each period.", "category": "Financial"},
    {"name": "pmt", "params": "rate, n, pv, fv=0, type=0", "description": "Calculate the fixed periodic payment required to fully repay a loan or reach a savings target over a given number of periods.", "category": "Financial"},
    {"name": "rate", "params": "n, pmt, pv, fv=0, type=0, guess=0.1", "description": "Calculate the interest rate per period needed to repay a loan given the number of payments, payment amount, and loan amount.", "category": "Financial"},
    {"name": "nper", "params": "rate, pmt, pv, fv=0, type=0", "description": "Calculate how many payment periods are required to fully repay a loan or reach a savings goal.", "category": "Financial"},
    {"name": "npv", "params": "rate, cashflows", "description": "Calculate the net present value of a series of cash flows discounted at a given annual rate. The rate is entered as a decimal.", "category": "Financial"},
    {"name": "irr", "params": "cashflows", "description": "Calculate the internal rate of return for a series of cash flows — the discount rate at which the net present value equals zero.", "category": "Financial"},
    {"name": "xnpv", "params": "rate, cashflows, dates", "description": "Calculate the net present value of cash flows that occur on specific calendar dates, using a 365-day year convention.", "category": "Financial"},
    {"name": "xirr", "params": "cashflows, dates", "description": "Calculate the internal rate of return for cash flows that occur on specific calendar dates.", "category": "Financial"},
    {"name": "discount_factor", "params": "rate, dcf", "description": "Calculate the factor used to discount a future cash flow back to its present value, given a rate and the time period as a year fraction.", "category": "Financial"},
    {"name": "accumulation_factor", "params": "rate, dcf", "description": "Calculate the growth factor applied to a present value to arrive at its future value, given a rate and time period as a year fraction.", "category": "Financial"},
    {"name": "effective_rate", "params": "nominal, freq", "description": "Convert a nominal interest rate to its effective annual rate, accounting for the number of compounding periods per year.", "category": "Financial"},
    {"name": "nominal_rate", "params": "effective, freq", "description": "Convert an effective annual interest rate back to its nominal rate for a given number of compounding periods per year.", "category": "Financial"},
    {"name": "yield_to_maturity", "params": "price, face, coupon, years", "description": "Calculate the approximate yield to maturity of a bond based on its market price, face value, annual coupon rate, and remaining years to maturity.", "category": "Financial"},

    # Depreciation (5)

    # Allocation (5)

    # Balance (3)

    # Arithmetic (15)
    {"name": "add", "params": "a, b", "description": "Add two numbers together.", "category": "Arithmetic"},
    {"name": "subtract", "params": "a, b", "description": "Subtract the second number from the first.", "category": "Arithmetic"},
    {"name": "multiply", "params": "a, b", "description": "Multiply two numbers together.", "category": "Arithmetic"},
    {"name": "divide", "params": "a, b", "description": "Divide the first number by the second.", "category": "Arithmetic"},
    {"name": "power", "params": "a, b", "description": "Raise a number to the power of a given exponent.", "category": "Arithmetic"},
    {"name": "abs", "params": "x", "description": "Return the absolute value of a number, removing any negative sign.", "category": "Arithmetic"},
    {"name": "sign", "params": "x", "description": "Return -1 if the number is negative, 0 if zero, or 1 if positive.", "category": "Arithmetic"},
    {"name": "round", "params": "x, n=0", "description": "Round a number to a specified number of decimal places.", "category": "Arithmetic"},
    {"name": "floor", "params": "x", "description": "Round a number down to the nearest whole number.", "category": "Arithmetic"},
    {"name": "ceil", "params": "x", "description": "Round a number up to the nearest whole number.", "category": "Arithmetic"},
    {"name": "truncate", "params": "x, decimals=0", "description": "Remove decimal places beyond a specified number of positions without any rounding.", "category": "Arithmetic"},
    {"name": "percentage", "params": "value, total", "description": "Calculate what percentage one number represents of a given total.", "category": "Arithmetic"},

    # Comparison (10)
    {"name": "eq", "params": "a, b", "description": "Check whether two values are equal.", "category": "Comparison"},
    {"name": "neq", "params": "a, b", "description": "Check whether two values are not equal.", "category": "Comparison"},
    {"name": "gt", "params": "a, b", "description": "Check whether the first value is greater than the second.", "category": "Comparison"},
    {"name": "gte", "params": "a, b", "description": "Check whether the first value is greater than or equal to the second.", "category": "Comparison"},
    {"name": "lt", "params": "a, b", "description": "Check whether the first value is less than the second.", "category": "Comparison"},
    {"name": "lte", "params": "a, b", "description": "Check whether the first value is less than or equal to the second.", "category": "Comparison"},
    {"name": "between", "params": "x, l, u", "description": "Check whether a value falls within a specified lower and upper boundary, inclusive.", "category": "Comparison"},
    {"name": "is_null", "params": "x", "description": "Check whether a value is empty or missing.", "category": "Comparison"},

    # Logical (10)
    {"name": "and", "params": "a, b", "description": "Return true only if both conditions are true.", "category": "Logical"},
    {"name": "or", "params": "a, b", "description": "Return true if at least one of the two conditions is true.", "category": "Logical"},
    {"name": "not", "params": "a", "description": "Reverse a condition — returns true if the condition is false, and false if it is true.", "category": "Logical"},
    {"name": "all", "params": "list", "description": "Return true only if every item in a list evaluates to true.", "category": "Logical"},
    {"name": "any", "params": "list", "description": "Return true if at least one item in a list evaluates to true.", "category": "Logical"},
    {"name": "if", "params": "cond, true_val, false_val", "description": "Return one of two values based on a condition — works like an IF statement in a spreadsheet.", "category": "Logical"},
    {"name": "coalesce", "params": "*args", "description": "Return the first non-empty value from a list — useful for providing a fallback default when a value may be missing.", "category": "Logical"},
    {"name": "switch", "params": "value, cases, default", "description": "Look up a value against a set of named cases and return the matching result, or a default value if no match is found.", "category": "Logical"},

    # Date (19)
    {"name": "days_between", "params": "d1, d2", "description": "Calculate the number of calendar days between two dates.", "category": "Date"},
    {"name": "months_between", "params": "d1, d2", "description": "Calculate the number of complete months between two dates.", "category": "Date"},
    {"name": "years_between", "params": "d1, d2", "description": "Calculate the number of complete years between two dates.", "category": "Date"},
    {"name": "add_days", "params": "d, n", "description": "Add a specified number of days to a date and return the resulting date.", "category": "Date"},
    {"name": "add_months", "params": "d, n", "description": "Add a specified number of months to a date and return the resulting date.", "category": "Date"},
    {"name": "add_years", "params": "d, n", "description": "Add a specified number of years to a date and return the resulting date.", "category": "Date"},
    {"name": "subtract_days", "params": "d, n", "description": "Subtract a specified number of days from a date and return the resulting date.", "category": "Date"},
    {"name": "subtract_months", "params": "d, n", "description": "Subtract a specified number of months from a date and return the resulting date.", "category": "Date"},
    {"name": "subtract_years", "params": "d, n", "description": "Subtract a specified number of years from a date and return the resulting date.", "category": "Date"},
    {"name": "start_of_month", "params": "d", "description": "Return the first calendar day of the month for a given date.", "category": "Date"},
    {"name": "end_of_month", "params": "d", "description": "Return the last calendar day of the month for a given date.", "category": "Date"},
    {"name": "day_count_fraction", "params": "d1, d2, conv='ACT/360'", "description": "Calculate the fraction of a year between two dates using a specified day count convention such as ACT/360 or ACT/365.", "category": "Date"},
    {"name": "is_leap_year", "params": "year", "description": "Determine whether a given year is a leap year.", "category": "Date"},
    {"name": "days_in_year", "params": "year", "description": "Return the total number of days in a given year — 365 for standard years and 366 for leap years.", "category": "Date"},
    {"name": "quarter", "params": "d", "description": "Return the calendar quarter (1 to 4) that a given date falls in.", "category": "Date"},
    {"name": "day_of_week", "params": "d", "description": "Return the day of the week for a date as a number, where 0 is Monday and 6 is Sunday.", "category": "Date"},
    {"name": "is_weekend", "params": "d", "description": "Check whether a given date falls on a Saturday or Sunday.", "category": "Date"},
    {"name": "normalize_date", "params": "date_value", "description": "Convert a date written in any common format to the standard YYYY-MM-DD format.", "category": "Date"},
    {"name": "business_days", "params": "d1, d2", "description": "Calculate the number of working days between two dates, excluding weekends.", "category": "Date"},

    # Schedule (7)
    {"name": "schedule", "params": "period, columns", "description": "Generate a time-based schedule table with calculated columns, suitable for amortisation, accrual, revenue, or depreciation schedules.", "category": "Schedule"},
    {"name": "period", "params": "start, end, freq, conv?", "description": "Define a time period with a start date, end date, frequency (monthly, quarterly, annual, etc.), and an optional day count convention.", "category": "Schedule"},
    {"name": "schedule_sum", "params": "schedule, column", "description": "Add up all values in a specified column of a generated schedule.", "category": "Schedule"},
    {"name": "schedule_last", "params": "schedule, column", "description": "Retrieve the value from the last row of a specified column in a schedule.", "category": "Schedule"},
    {"name": "schedule_first", "params": "schedule, column", "description": "Retrieve the value from the first row of a specified column in a schedule.", "category": "Schedule"},
    {"name": "schedule_column", "params": "schedule, column", "description": "Return all values from a specified column of a schedule as a list.", "category": "Schedule"},
    {"name": "schedule_filter", "params": "schedule, match_column, match_value, return_column", "description": "Find the first row in a schedule where a column matches a given value and return the corresponding value from another column.", "category": "Schedule"},

    # Aggregation (13)
    {"name": "sum", "params": "col", "description": "Add up all values in a list, ignoring any empty entries.", "category": "Aggregation"},
    {"name": "sum_field", "params": "array, field", "description": "Add up a specific named field from a list of records, treating any missing values as zero.", "category": "Aggregation"},
    {"name": "avg", "params": "col", "description": "Calculate the arithmetic average of a list of values.", "category": "Aggregation"},
    {"name": "min", "params": "col", "description": "Return the smallest value from a list.", "category": "Aggregation"},
    {"name": "max", "params": "col", "description": "Return the largest value from a list.", "category": "Aggregation"},
    {"name": "count", "params": "col", "description": "Count the number of items in a list.", "category": "Aggregation"},
    {"name": "weighted_avg", "params": "v, w", "description": "Calculate the average of a list of values, where each value is weighted by a corresponding weight factor.", "category": "Aggregation"},
    {"name": "cumulative_sum", "params": "col", "description": "Calculate the running total of a list, returning a new list where each entry is the accumulated sum up to that point.", "category": "Aggregation"},
    {"name": "median", "params": "col", "description": "Return the middle value of a sorted list — half the values fall above and half fall below.", "category": "Aggregation"},
    {"name": "std_dev", "params": "col", "description": "Measure how spread out the values in a list are around the average, expressed on the same scale as the values.", "category": "Aggregation"},

    # Conversion (6)

    # Statistical (3)

    # String (9)
    {"name": "lower", "params": "s", "description": "Convert all characters in a text value to lowercase.", "category": "String"},
    {"name": "upper", "params": "s", "description": "Convert all characters in a text value to uppercase.", "category": "String"},
    {"name": "concat", "params": "s1, s2, ...", "description": "Join two or more text values together into a single combined string.", "category": "String"},
    {"name": "contains", "params": "s, substring", "description": "Check whether a piece of text contains a specific word or phrase.", "category": "String"},
    {"name": "eq_ignore_case", "params": "a, b", "description": "Check whether two text values are equal, ignoring any differences in upper or lower case.", "category": "String"},
    {"name": "trim", "params": "s", "description": "Remove any extra spaces from the beginning and end of a text value.", "category": "String"},
    {"name": "str_length", "params": "s", "description": "Return the number of characters in a text value.", "category": "String"},

    # Array Collection (6)
    {"name": "collect_by_instrument", "params": "EVENT.field", "description": "Gather all values of an event field for the current instrument across all dates into a single list.", "category": "Array"},
    {"name": "collect_all", "params": "EVENT.field", "description": "Gather every value of an event field across all rows in the dataset without any filtering.", "category": "Array"},
    {"name": "collect_by_subinstrument", "params": "EVENT.field", "description": "Gather all values of an event field for a specific instrument and sub-instrument combination.", "category": "Array"},
    {"name": "collect_effectivedates_for_subinstrument", "params": "subinstrument_id?", "description": "Return a list of all effective dates associated with a specified sub-instrument.", "category": "Array"},

    # Iteration (5)
    {"name": "apply_each", "params": "array, expression", "description": "Apply a formula to every item in a list using 'each' as the current item, and return the results. For paired lists, pass two arrays and use 'first' and 'second' in the formula.", "category": "Iteration"},
    {"name": "for_each", "params": "dates_arr, amounts_arr, date_var, amt_var, expr", "description": "Loop through two paired lists of dates and amounts, running a specified action for each pair — commonly used to create multiple transactions.", "category": "Iteration"},
    {"name": "for_each_with_index", "params": "array, var_name, expression, context?", "description": "Loop through a list, making each item and its position number available inside the loop body.", "category": "Iteration"},
    {"name": "array_filter", "params": "array, var_name, condition, context?", "description": "Return a new list containing only the items from the original list that meet a specified condition.", "category": "Iteration"},

    # Array Utilities (9)
    {"name": "array_length", "params": "array", "description": "Return the number of items in a list.", "category": "Array Utilities"},
    {"name": "array_get", "params": "array, index, default=None", "description": "Return the item at a specified position in a list, with a fallback value if the position is beyond the end of the list.", "category": "Array Utilities"},
    {"name": "array_first", "params": "array, default=None", "description": "Return the first item in a list, with an optional fallback value if the list is empty.", "category": "Array Utilities"},
    {"name": "array_last", "params": "array, default=None", "description": "Return the last item in a list, with an optional fallback value if the list is empty.", "category": "Array Utilities"},
    {"name": "array_slice", "params": "array, start, end=None", "description": "Extract a portion of a list from a starting position to an optional ending position.", "category": "Array Utilities"},
    {"name": "array_reverse", "params": "array", "description": "Return a new list with all items in the reverse order.", "category": "Array Utilities"},
    {"name": "array_append", "params": "array, item", "description": "Return a new list with one additional item added to the end, without modifying the original list.", "category": "Array Utilities"},
    {"name": "array_extend", "params": "array, items", "description": "Return a new list formed by joining two lists together, without modifying the original.", "category": "Array Utilities"},

    # Transaction (1)
    {"name": "createTransaction", "params": "postingdate, effectivedate, transactiontype, amount, subinstrumentid='1'", "description": "Record a financial transaction with a posting date, effective date, transaction type, and amount. The sub-instrument ID defaults to '1' if not provided.", "category": "Transaction"},
]

print(f"Loaded {len(DSL_FUNCTIONS)} functions across {len(set(f['category'] for f in DSL_FUNCTION_METADATA))} categories")

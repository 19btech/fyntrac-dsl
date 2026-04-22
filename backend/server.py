from fastapi import FastAPI, APIRouter, UploadFile, File, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
import uuid
from datetime import datetime, timezone
import csv
import io
import pandas as pd
import json
import re


def _normalize_ingest_date_value(value):
    """Normalize a value (scalar or list or JSON-list-string) to yyyy-mm-dd or list of such strings."""
    if value is None:
        return ''
    # Lists -> normalize each
    if isinstance(value, list):
        out = []
        for v in value:
            try:
                nv = normalize_date(v)
            except Exception:
                nv = ''
            if nv:
                out.append(nv)
        return out

    s = value
    # Try parsing JSON arrays
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return ''
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return [normalize_date(p) for p in parsed if normalize_date(p)]
        except Exception:
            pass
        # Delimited lists
        if ',' in s or ';' in s or '|' in s:
            parts = [p.strip() for p in re.split('[,;|]', s) if p.strip()]
            return [normalize_date(p) for p in parts if normalize_date(p)]

    # Fallback: normalize scalar
    try:
        return normalize_date(s)
    except Exception:
        return ''
# AI provider abstraction layer
try:
    from backend.ai_providers import (
        get_provider, PROVIDER_INFO, build_agent_context,
        encrypt_key, decrypt_key, AIError,
    )
except Exception:
    try:
        from ai_providers import (
            get_provider, PROVIDER_INFO, build_agent_context,
            encrypt_key, decrypt_key, AIError,
        )
    except Exception:
        from .ai_providers import (
            get_provider, PROVIDER_INFO, build_agent_context,
            encrypt_key, decrypt_key, AIError,
        )
# Support running in different execution contexts: prefer package import, fallback to module-level
try:
    from backend.dsl_functions import DSL_FUNCTIONS, DSL_FUNCTION_METADATA, normalize_date
except Exception:
    try:
        from dsl_functions import DSL_FUNCTIONS, DSL_FUNCTION_METADATA, normalize_date
    except Exception:
        # Last resort: try relative import (works when executed as package)
        from .dsl_functions import DSL_FUNCTIONS, DSL_FUNCTION_METADATA, normalize_date

try:
    from bson import ObjectId
except Exception:
    ObjectId = None

# Load configuration
try:
    from backend.config import settings
except Exception:
    try:
        from config import settings
    except Exception:
        from .config import settings

ROOT_DIR = Path(__file__).parent

# MongoDB connection
client = AsyncIOMotorClient(settings.mongo_url, serverSelectionTimeoutMS=settings.mongo_timeout_ms)
db = client[settings.db_name]

# --- Shared error message table for AI chat endpoints ---
ERROR_MESSAGES = {
    "no_provider": "You haven't set up an AI provider yet. Go to Settings \u2192 AI Agent Setup to get started.",
    "invalid_key": "Your API key appears to be invalid or has expired. Please update it in Settings \u2192 AI Agent Setup.",
    "quota_exceeded": "You've reached the usage limit for your {provider} account. Please check your plan or billing.",
    "rate_limited": "You're sending messages too quickly. Please wait a moment before trying again.",
    "model_premium": "The selected model ({model}) requires a paid subscription on {provider}. Switch to a free-tier model or upgrade your account.",
    "network": "Couldn't reach {provider} right now. Check your internet connection and try again.",
    "model_deprecated": "The model '{model}' is no longer available on {provider}. Please select a different model in the chatbot settings.",
}

# Create the main app
app = FastAPI()
# Router without /api prefix - proxy will handle the /api part
api_router = APIRouter()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============= In-Memory Storage (for when MongoDB is unavailable) =============
# This allows the app to work without MongoDB
in_memory_data = {
    "event_definitions": [],
    "event_data": [],
    "templates": [],
    "template_artifacts": [],
    "custom_functions": [],
    "transaction_reports": []
}
# Flag to track if we should use in-memory storage
USE_IN_MEMORY = False

# ============= Models (imported from models.py) =============
try:
    from backend.models import (
        EventDefinition, DSLFunction, EventData, DSLTemplate, DSLTemplateArtifact,
        TransactionOutput, TransactionReport, ChatMessage, ChatResponse,
        AIProviderTestRequest, AIProviderSaveRequest, DSLValidationRequest,
        SaveTemplateRequest, DSLRunRequest, TemplateExecuteRequest,
        TemplateDeployRequest,
    )
except Exception:
    try:
        from models import (
            EventDefinition, DSLFunction, EventData, DSLTemplate, DSLTemplateArtifact,
            TransactionOutput, TransactionReport, ChatMessage, ChatResponse,
            AIProviderTestRequest, AIProviderSaveRequest, DSLValidationRequest,
            SaveTemplateRequest, DSLRunRequest, TemplateExecuteRequest,
            TemplateDeployRequest,
        )
    except Exception:
        from .models import (
            EventDefinition, DSLFunction, EventData, DSLTemplate, DSLTemplateArtifact,
            TransactionOutput, TransactionReport, ChatMessage, ChatResponse,
            AIProviderTestRequest, AIProviderSaveRequest, DSLValidationRequest,
            SaveTemplateRequest, DSLRunRequest, TemplateExecuteRequest,
            TemplateDeployRequest,
        )

# ============= Sample Data (for when MongoDB is unavailable) =============
SAMPLE_EVENTS = [
    {
        "id": "evt1",
        "event_name": "LoanEvent",
        "fields": [
            {"name": "principal", "datatype": "decimal"},
            {"name": "rate", "datatype": "decimal"},
            {"name": "term", "datatype": "decimal"}
        ],
        "created_at": datetime.now(timezone.utc),
        "eventType": "activity",
        "eventTable": "standard"
    },
    {
        "id": "evt2",
        "event_name": "PaymentEvent",
        "fields": [
            {"name": "payment_amount", "datatype": "decimal"},
            {"name": "payment_date", "datatype": "date"},
            {"name": "payment_type", "datatype": "string"}
        ],
        "created_at": datetime.now(timezone.utc),
        "eventType": "activity",
        "eventTable": "standard"
    },
    {
        "id": "evt3",
        "event_name": "InvestmentEvent",
        "fields": [
            {"name": "initial_investment", "datatype": "decimal"},
            {"name": "return_rate", "datatype": "decimal"},
            {"name": "years", "datatype": "decimal"}
        ],
        "created_at": datetime.now(timezone.utc),
        "eventType": "activity",
        "eventTable": "standard"
    }
]

SAMPLE_TEMPLATES = [
    {
        "id": "tpl1",
        "name": "Compound Interest Calculator",
        "dsl_code": "interest = compound_interest(principal, rate, term)\ntransactiontype = \"Compound Interest\"\namount = interest",
        "python_code": "def calculate(principal, rate, term):\n    return principal * ((1 + rate) ** term - 1)",
        "created_at": datetime.now(timezone.utc)
    },
    {
        "id": "tpl2",
        "name": "Compound Interest Calculator",
        "dsl_code": "interest = compound_interest(principal, rate, term)\nnew_balance = capitalization(interest, principal)\ntransactiontype = \"Compound Interest\"\namount = interest",
        "python_code": "def calculate(principal, rate, term):\n    return principal * ((1 + rate) ** term - 1)",
        "created_at": datetime.now(timezone.utc)
    }
]

# ============= Helper Functions =============

def parse_csv_content(content: str) -> List[List[str]]:
    """Parse CSV content and return list of rows"""
    # Remove BOM (Byte Order Mark) if present
    if content.startswith('\ufeff'):
        content = content[1:]
    reader = csv.reader(io.StringIO(content))
    return list(reader)

def get_field_case_insensitive(row: Dict[str, Any], field_name: str, default: Any = '') -> Any:
    """Get field value with case-insensitive key matching"""
    # First try exact match
    if field_name in row:
        return row[field_name]
    # Try case-insensitive match
    field_lower = field_name.lower()
    for key in row:
        if key.lower() == field_lower:
            return row[key]
    return default

def get_latest_data_per_instrument(data_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Get latest postingdate per instrumentid (case-insensitive field matching).

    Defensively skips any row that is not a dict (e.g. a stringified JSON object
    that slipped through during import). Such rows are logged so the user can fix
    the source data instead of seeing a cryptic ``'str' object has no attribute 'items'``.
    """
    latest_data = {}
    for idx, row in enumerate(data_rows):
        if not isinstance(row, dict):
            logger.warning(
                "Skipping non-dict row at index %d in event data (got %s). "
                "Re-import the source file — each row must be a JSON object.",
                idx, type(row).__name__,
            )
            continue
        instrument_id = get_field_case_insensitive(row, 'instrumentid', '')
        posting_date = get_field_case_insensitive(row, 'postingdate', '')
        
        if not instrument_id:
            continue
            
        if instrument_id not in latest_data:
            latest_data[instrument_id] = row
        else:
            existing_date = get_field_case_insensitive(latest_data[instrument_id], 'postingdate', '')
            if posting_date > existing_date:
                latest_data[instrument_id] = row
    
    return latest_data

def extract_event_names_from_dsl(dsl_code: str) -> List[str]:
    """Extract all event names referenced in DSL code (EVENT_NAME.field pattern)"""
    import re
    # Match patterns like PMT.field_name, LoanEvent.principal, ProductConfig.fee_percent
    # Event name: starts with uppercase, followed by alphanumerics/underscores
    pattern = r'\b([A-Z][A-Za-z0-9_]*)\.[A-Za-z_][A-Za-z0-9_]*'
    matches = re.findall(pattern, dsl_code)
    # Return unique event names
    return list(set(matches))

def merge_event_data_by_instrument(event_data_dict: Dict[str, List[Dict]]) -> List[Dict]:
    """
    Merge data from multiple events by instrumentid.
    Each event's fields are prefixed with EVENT_NAME_ to avoid conflicts.
    Also provides event-specific postingdate, effectivedate, and subinstrumentid.
    
    Hierarchy: postingDate → instrumentId → subInstrumentId → effectiveDates
    
    If subInstrumentId is missing or null, it defaults to "1".
    """
    merged_data = {}
    bad_row_events = []
    
    for event_name, data_rows in event_data_dict.items():
        # Pre-flight check: ensure every row is a dict. Surface a clear error pointing
        # at the offending event/row so the user knows where to look.
        if isinstance(data_rows, list):
            for idx, row in enumerate(data_rows):
                if not isinstance(row, dict):
                    bad_row_events.append((event_name, idx, type(row).__name__))
        latest_data = get_latest_data_per_instrument(data_rows if isinstance(data_rows, list) else [])
        
        for instrument_id, row in latest_data.items():
            if instrument_id not in merged_data:
                # Get subinstrumentid with default of "1" if missing
                subinstrument_id = get_field_case_insensitive(row, 'subinstrumentid', '')
                if not subinstrument_id or subinstrument_id == 'None' or str(subinstrument_id).strip() == '':
                    subinstrument_id = '1'
                
                merged_data[instrument_id] = {
                    'instrumentid': instrument_id,
                    'subinstrumentid': str(subinstrument_id),
                    'postingdate': get_field_case_insensitive(row, 'postingdate', ''),
                    'effectivedate': get_field_case_insensitive(row, 'effectivedate', '')
                }
            
            # Get event-specific standard fields
            event_postingdate = get_field_case_insensitive(row, 'postingdate', '')
            event_effectivedate = get_field_case_insensitive(row, 'effectivedate', '')
            event_subinstrumentid = get_field_case_insensitive(row, 'subinstrumentid', '')
            if not event_subinstrumentid or event_subinstrumentid == 'None' or str(event_subinstrumentid).strip() == '':
                event_subinstrumentid = '1'
            
            # Add event-prefixed standard fields (e.g., INT_ACC_postingdate, INT_ACC_subinstrumentid)
            merged_data[instrument_id][f"{event_name}_postingdate"] = event_postingdate
            merged_data[instrument_id][f"{event_name}_effectivedate"] = event_effectivedate
            merged_data[instrument_id][f"{event_name}_subinstrumentid"] = str(event_subinstrumentid)
            
            # Add other fields with event prefix (EVENT_FIELD) for clarity
            # Also add without prefix for direct field access
            if not isinstance(row, dict):
                # Already logged above; skip safely.
                continue
            for key, value in row.items():
                key_lower = key.lower()
                if key_lower not in ['instrumentid', 'postingdate', 'effectivedate', 'subinstrumentid']:
                    # Store with event prefix: PMT_TRANSACTIONS_AMOUNT_REMIT
                    prefixed_key = f"{event_name}_{key}"
                    merged_data[instrument_id][prefixed_key] = value
                    # Also store the original field name for backward compatibility
                    merged_data[instrument_id][key] = value
    
    if bad_row_events:
        # Raise a single descriptive error pointing at the first bad row so the user
        # knows which event needs to be re-imported.
        evt, idx, kind = bad_row_events[0]
        raise ValueError(
            f"Event '{evt}' has malformed data: row #{idx} is a {kind}, not an object. "
            f"Re-import the source file — each row must be a JSON object "
            f"(total bad rows: {len(bad_row_events)})."
        )
    
    return list(merged_data.values())


def filter_event_data_by_posting_date(
    event_data_dict: Dict[str, List[Dict]], posting_date: str
) -> Dict[str, List[Dict]]:
    """
    Return a copy of event_data_dict where each event's rows are restricted to those
    whose postingdate (case-insensitive) matches the requested posting_date string.
    Events with no matching rows keep an empty list (not removed, so callers can log
    a warning instead of crashing).
    """
    filtered: Dict[str, List[Dict]] = {}
    target = posting_date.strip()
    for event_name, rows in event_data_dict.items():
        safe_rows = rows if isinstance(rows, list) else []
        filtered[event_name] = [
            row for row in safe_rows
            if isinstance(row, dict)
            and str(get_field_case_insensitive(row, "postingdate", "")).strip() == target
        ]
    return filtered


def _extract_dsl_line_from_exception(python_code: str, exc: Exception) -> Optional[int]:
    """Extract the DSL line number from a Python exception using DSL_LINE comments.
    
    Looks at the traceback to find the Python line that failed, then reads the
    corresponding source line from python_code to find a # DSL_LINE:N marker.
    Returns the DSL line number (1-based) or None if it can't be determined.
    """
    import traceback as tb_mod
    try:
        tb = exc.__traceback__
        if tb is None:
            return None
        # Walk to the innermost frame
        while tb.tb_next:
            tb = tb.tb_next
        py_lineno = tb.tb_lineno
        code_lines = python_code.split('\n')
        if 1 <= py_lineno <= len(code_lines):
            source_line = code_lines[py_lineno - 1]
            m = re.search(r'# DSL_LINE:(\d+)', source_line)
            if m:
                return int(m.group(1))
    except Exception:
        pass
    return None


def dsl_to_python_standalone(dsl_code: str) -> str:
    """Convert DSL code to Python for standalone execution (no events required)"""
    
    imports = '''
import sys, os
# Ensure backend package folder is on path so imports work when executed from different cwd
try:
    ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__)))
except Exception:
    ROOT_DIR = os.getcwd()
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)
try:
    from backend.dsl_functions import DSL_FUNCTIONS, _set_current_instrumentid, _clear_transaction_results, _get_transaction_results, _set_dsl_print
except Exception:
    from dsl_functions import DSL_FUNCTIONS, _set_current_instrumentid, _clear_transaction_results, _get_transaction_results, _set_dsl_print
from datetime import datetime
import json

# Preserve Python built-ins before updating with DSL functions
_builtin_min = min
_builtin_max = max
_builtin_sum = sum
_builtin_len = len
_builtin_range = range
_builtin_print = print

# Make all DSL functions available globally
globals().update(DSL_FUNCTIONS)

# Expose safe aliases for DSL functions whose names are Python keywords
and_op = DSL_FUNCTIONS.get('and', lambda a, b: a and b)
or_op = DSL_FUNCTIONS.get('or', lambda a, b: a or b)
not_op = DSL_FUNCTIONS.get('not', lambda a: not a)

# Restore Python built-ins (needed for native Python syntax)
min = _builtin_min
max = _builtin_max
sum = _builtin_sum
len = _builtin_len
# Smart range: DSL range(list)->max-min; Python range(int,...) for iterations
_dsl_range_val = DSL_FUNCTIONS.get('range', lambda col: (_builtin_max(col) - _builtin_min(col)) if col else 0)
def range(*args):
    if len(args) == 1 and isinstance(args[0], list):
        return _dsl_range_val(args[0])
    return _builtin_range(*args)

# Global list to capture print outputs
_print_outputs = []

def dsl_print(*args, **kwargs):
    try:
        # If a single argument looks like schedule(s), delegate to print_all_schedules
        if len(args) == 1:
            obj = args[0]
            if isinstance(obj, list) and obj:
                first = obj[0]
                if isinstance(first, dict) and 'schedule' in first:
                    try:
                        print_all_schedules(obj)
                        return
                    except Exception:
                        pass
                if isinstance(first, list):
                    inner_first = first[0] if first else None
                    if isinstance(inner_first, dict) and ('period_date' in inner_first or 'period_revenue' in inner_first or 'period_amount' in inner_first):
                        try:
                            print_all_schedules(obj)
                            return
                        except Exception:
                            pass
                    try:
                        print_all_schedules(obj)
                        return
                    except Exception:
                        pass
                if isinstance(first, dict) and ('period_date' in first or 'period_revenue' in first or 'period_amount' in first):
                    try:
                        # treat as array of rows (single schedule)
                        print_all_schedules([{"schedule": obj}])
                        return
                    except Exception:
                        pass
            if isinstance(obj, dict) and 'schedule' in obj:
                try:
                    print_all_schedules([obj])
                    return
                except Exception:
                    pass

        output_parts = []
        for arg in args:
            if isinstance(arg, (list, dict)):
                try:
                    output_parts.append(json.dumps(arg, indent=2, default=str))
                except Exception:
                    output_parts.append(str(arg))
            else:
                output_parts.append(str(arg))

        sep = kwargs.get('sep', ' ')
        output = sep.join(output_parts)
        _print_outputs.append(output)
    except Exception:
        try:
            _builtin_print(' '.join(map(str, args)))
        except Exception:
            pass

print = dsl_print

# Set the DSL print function for use by dsl_functions module (e.g., print_schedule)
_set_dsl_print(dsl_print)

def get_print_outputs():
    return _print_outputs

def clear_print_outputs():
    global _print_outputs
    _print_outputs = []
'''
    
    # Process DSL code
    import re
    processed_lines = []
    
    dsl_lines = dsl_code.split('\n')
    for dsl_line_num, line in enumerate(dsl_lines, start=1):
        stripped = line.strip()
        
        if not stripped or stripped.startswith('#') or stripped.startswith('//'):
            if stripped.startswith('//'):
                stripped = '#' + stripped[2:]
            processed_lines.append(f"    {stripped}" if stripped else "")
            continue
        
        # Replace Python keyword function calls with safe aliases
        stripped = re.sub(r'\band\s*\(', 'and_op(', stripped)
        stripped = re.sub(r'\bor\s*\(', 'or_op(', stripped)
        stripped = re.sub(r'\bnot\s*\(', 'not_op(', stripped)
        stripped = re.sub(r'\bif\s*\(', 'iif(', stripped)

        # Simply add the line with DSL line marker
        processed_lines.append(f"    {stripped}  # DSL_LINE:{dsl_line_num}")
    
    python_body = '\n'.join(processed_lines)
    
    template = f'''
{imports}

def process_standalone(override_postingdate=None, override_effectivedate=None):
    # Clear any previous transaction results
    _clear_transaction_results()
    
    # Set instrumentid for standalone mode
    _set_current_instrumentid('STANDALONE')
    
    # Expose posting_date in scope so schedule column formulas can reference it
    postingdate = override_postingdate or ''
    posting_date = postingdate
    effectivedate = override_effectivedate or ''
    effective_date = effectivedate
    
    # Execute DSL logic - transactions are created via createTransaction()
{python_body}
    
    # Get transactions created via createTransaction()
    results = _get_transaction_results()
    
    return results, get_print_outputs()
'''
    return template


# Helper to sanitize DB documents for JSON serialization
def sanitize_for_json(obj):
    """Recursively convert ObjectId to str and datetimes to ISO strings."""
    if isinstance(obj, dict):
        new = {}
        for k, v in obj.items():
            new[k] = sanitize_for_json(v)
        return new
    if isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    # ObjectId handling
    try:
        if ObjectId is not None and isinstance(obj, ObjectId):
            return str(obj)
    except Exception:
        pass
    # datetime handling
    from datetime import datetime
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj

def dsl_to_python_multi_event(dsl_code: str, all_event_fields: Dict[str, List[Dict[str, str]]]) -> str:
    """Convert DSL code to Python code template supporting multiple events and multiple transactions per row"""
    
    imports = '''
import sys, os
# Ensure backend package folder is on path so imports work when executed from different cwd
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)
try:
    from backend.dsl_functions import DSL_FUNCTIONS, _set_current_instrumentid, _clear_transaction_results, _get_transaction_results, _set_dsl_print
except Exception:
    from dsl_functions import DSL_FUNCTIONS, _set_current_instrumentid, _clear_transaction_results, _get_transaction_results, _set_dsl_print
from datetime import datetime
import json

# Preserve Python built-ins before updating with DSL functions
_builtin_min = min
_builtin_max = max
_builtin_sum = sum
_builtin_len = len
_builtin_range = range
_builtin_print = print

# Make all DSL functions available globally
globals().update(DSL_FUNCTIONS)

# Expose safe aliases for DSL functions whose names are Python keywords
and_op = DSL_FUNCTIONS.get('and', lambda a, b: a and b)
or_op = DSL_FUNCTIONS.get('or', lambda a, b: a or b)
not_op = DSL_FUNCTIONS.get('not', lambda a: not a)

# Restore Python built-ins (needed for native Python syntax)
min = _builtin_min
max = _builtin_max
sum = _builtin_sum
len = _builtin_len
# Smart range: DSL range(list)->max-min; Python range(int,...) for iterations
_dsl_range_val = DSL_FUNCTIONS.get('range', lambda col: (_builtin_max(col) - _builtin_min(col)) if col else 0)
def range(*args):
    if len(args) == 1 and isinstance(args[0], list):
        return _dsl_range_val(args[0])
    return _builtin_range(*args)

# Global list to capture print outputs
_print_outputs = []

def dsl_print(*args, **kwargs):
    """Custom print function that captures output for display in console"""
    try:
        # If a single argument looks like schedule(s), delegate to print_all_schedules
        if len(args) == 1:
            obj = args[0]
            if isinstance(obj, list) and obj:
                first = obj[0]
                if isinstance(first, dict) and 'schedule' in first:
                    try:
                        print_all_schedules(obj)
                        return
                    except Exception:
                        pass
                if isinstance(first, list):
                    inner_first = first[0] if first else None
                    if isinstance(inner_first, dict) and ('period_date' in inner_first or 'period_revenue' in inner_first or 'period_amount' in inner_first):
                        try:
                            print_all_schedules(obj)
                            return
                        except Exception:
                            pass
                    try:
                        print_all_schedules(obj)
                        return
                    except Exception:
                        pass
                if isinstance(first, dict) and ('period_date' in first or 'period_revenue' in first or 'period_amount' in first):
                    try:
                        # treat as array of rows (single schedule)
                        print_all_schedules([{"schedule": obj}])
                        return
                    except Exception:
                        pass
            if isinstance(obj, dict) and 'schedule' in obj:
                try:
                    print_all_schedules([obj])
                    return
                except Exception:
                    pass

        output_parts = []
        for arg in args:
            if isinstance(arg, (list, dict)):
                # Pretty print complex objects
                try:
                    output_parts.append(json.dumps(arg, indent=2, default=str))
                except Exception:
                    output_parts.append(str(arg))
            else:
                output_parts.append(str(arg))

        sep = kwargs.get('sep', ' ')
        output = sep.join(output_parts)
        _print_outputs.append(output)
    except Exception:
        try:
            _builtin_print(' '.join(map(str, args)))
        except Exception:
            pass

# Override print with our custom version
print = dsl_print

# Set the DSL print function for use by dsl_functions module (e.g., print_schedule)
_set_dsl_print(dsl_print)

def get_field_case_insensitive(row, field_name, default=''):
    \"\"\"Get field value with case-insensitive key matching\"\"\"
    if field_name in row:
        return row[field_name]
    field_lower = field_name.lower()
    for key in row:
        if key.lower() == field_lower:
            return row[key]
    return default

def get_print_outputs():
    \"\"\"Return all captured print outputs\"\"\"
    return _print_outputs

def clear_print_outputs():
    \"\"\"Clear captured print outputs\"\"\"
    global _print_outputs
    _print_outputs = []

# Global reference to all event data for collect() function
_all_event_data = []
_raw_event_data = {}  # Raw data by event name: {'ECF': [...], 'PMT': [...]}
_current_context = {}

def set_all_event_data(data):
    \"\"\"Set the global event data reference\"\"\"
    global _all_event_data
    _all_event_data = data

def set_raw_event_data(data):
    \"\"\"Set the raw event data (unmerged) for collect() functions\"\"\"
    global _raw_event_data
    if not isinstance(data, dict):
        # Refuse to corrupt global state — something upstream passed the wrong type.
        # Reset to empty so collect_*() functions return [] instead of crashing later
        # with the cryptic ``'str' object has no attribute 'items'``.
        try:
            _builtin_print(
                f"[dsl-template warning] set_raw_event_data got {type(data).__name__}; expected dict. Resetting to empty."
            )
        except Exception:
            pass
        _raw_event_data = {}
        return
    _raw_event_data = data

def set_current_context(instrumentid, postingdate, effectivedate, subinstrumentid='1'):
    \"\"\"Set the current row context for filtering collect()\"\"\"
    global _current_context
    _current_context = {
        'instrumentid': instrumentid,
        'subinstrumentid': subinstrumentid or '1',
        'postingdate': postingdate,
        'effectivedate': effectivedate
    }

def collect(field_name):
    \"\"\"
    Collect all values of a field for the current instrumentid, postingdate, and effectivedate.
    Usage: cashflows = collect('ECF_ExpectedCF')
    Returns a list of numeric values from RAW event data (all rows, not merged).
    \"\"\"
    values = []
    current_instrument = _current_context.get('instrumentid', '')
    current_posting = _current_context.get('postingdate', '')
    current_effective = _current_context.get('effectivedate', '')
    
    # Parse field_name to get event name and field (e.g., 'ECF_ExpectedCF' -> 'ECF', 'ExpectedCF')
    parts = field_name.split('_', 1)
    if len(parts) == 2:
        event_name, actual_field = parts[0], parts[1]
    else:
        event_name, actual_field = None, field_name
    
    # Search in raw event data
    for evt_name, rows in _raw_event_data.items():
        # If event_name specified, only search that event
        if event_name and evt_name.upper() != event_name.upper():
            continue
            
        for row in rows:
            row_instrument = get_field_case_insensitive(row, 'instrumentid', '')
            row_posting = get_field_case_insensitive(row, 'postingdate', '')
            row_effective = get_field_case_insensitive(row, 'effectivedate', '') or row_posting
            
            if (row_instrument == current_instrument and 
                row_posting == current_posting and 
                row_effective == current_effective):
                # Try the actual field name
                val = get_field_case_insensitive(row, actual_field, None)
                if val is None:
                    # Try the full field name
                    val = get_field_case_insensitive(row, field_name, None)
                if val is not None and val != '':
                    try:
                        values.append(float(val))
                    except (ValueError, TypeError):
                        # Keep string values (dates, etc.)
                        values.append(str(val))
    return values

def collect_by_instrument(field_name):
    \"\"\"
    Collect all values of a field for the current instrumentid only (ignores dates).
    Useful for time-series data across multiple periods for same instrument.
    Returns numeric values as floats, non-numeric (dates, strings) as strings.
    \"\"\"
    values = []
    current_instrument = _current_context.get('instrumentid', '')
    
    # Parse field_name
    parts = field_name.split('_', 1)
    if len(parts) == 2:
        event_name, actual_field = parts[0], parts[1]
    else:
        event_name, actual_field = None, field_name
    
    for evt_name, rows in _raw_event_data.items():
        if event_name and evt_name.upper() != event_name.upper():
            continue
            
        for row in rows:
            row_instrument = get_field_case_insensitive(row, 'instrumentid', '')
            
            if row_instrument == current_instrument:
                val = get_field_case_insensitive(row, actual_field, None)
                if val is None:
                    val = get_field_case_insensitive(row, field_name, None)
                if val is not None and val != '':
                    try:
                        values.append(float(val))
                    except (ValueError, TypeError):
                        # Keep string values (dates, etc.)
                        values.append(str(val))
    return values

def collect_all(field_name):
    \"\"\"
    Collect ALL values of a field across all data rows (no filtering).
    Returns numeric values as floats, non-numeric (dates, strings) as strings.
    \"\"\"
    values = []
    
    # Parse field_name
    parts = field_name.split('_', 1)
    if len(parts) == 2:
        event_name, actual_field = parts[0], parts[1]
    else:
        event_name, actual_field = None, field_name
    
    for evt_name, rows in _raw_event_data.items():
        if event_name and evt_name.upper() != event_name.upper():
            continue
            
        for row in rows:
            val = get_field_case_insensitive(row, actual_field, None)
            if val is None:
                val = get_field_case_insensitive(row, field_name, None)
            if val is not None and val != '':
                try:
                    values.append(float(val))
                except (ValueError, TypeError):
                    # Keep string values (dates, etc.)
                    values.append(str(val))
    return values

def collect_by_subinstrument(field_name):
    \"\"\"
    Collect all values of a field for the current instrumentid AND subinstrumentid.
    Useful when you need to filter by both parent and child entity.
    
    Hierarchy: postingDate → instrumentId → subInstrumentId → effectiveDates
    \"\"\"
    values = []
    current_instrument = _current_context.get('instrumentid', '')
    current_subinstrument = _current_context.get('subinstrumentid', '1')
    
    # Parse field_name
    parts = field_name.split('_', 1)
    if len(parts) == 2:
        event_name, actual_field = parts[0], parts[1]
    else:
        event_name, actual_field = None, field_name
    
    for evt_name, rows in _raw_event_data.items():
        if event_name and evt_name.upper() != event_name.upper():
            continue
            
        for row in rows:
            row_instrument = get_field_case_insensitive(row, 'instrumentid', '')
            row_subinstrument = get_field_case_insensitive(row, 'subinstrumentid', '1') or '1'
            
            if row_instrument == current_instrument and row_subinstrument == current_subinstrument:
                val = get_field_case_insensitive(row, actual_field, None)
                if val is None:
                    val = get_field_case_insensitive(row, field_name, None)
                if val is not None and val != '':
                    try:
                        values.append(float(val))
                    except (ValueError, TypeError):
                        # For non-numeric values, store as string
                        values.append(val)
    return values

def collect_subinstrumentids():
    \"\"\"
    Collect all unique subInstrumentIds for the current instrumentId.
    Returns list of subInstrumentId values.
    \"\"\"
    current_instrument = _current_context.get('instrumentid', '')
    subinstrument_ids = set()
    
    for evt_name, rows in _raw_event_data.items():
        for row in rows:
            row_instrument = get_field_case_insensitive(row, 'instrumentid', '')
            if row_instrument == current_instrument:
                subinstrument = get_field_case_insensitive(row, 'subinstrumentid', '1') or '1'
                subinstrument_ids.add(subinstrument)
    
    return sorted(list(subinstrument_ids))

def collect_effectivedates_for_subinstrument(subinstrument_id=None):
    \"\"\"
    Collect all unique effectiveDates for a specific subInstrumentId within current instrumentId.
    If subinstrument_id is None, uses current context's subinstrumentid.
    \"\"\"
    current_instrument = _current_context.get('instrumentid', '')
    target_subinstrument = subinstrument_id or _current_context.get('subinstrumentid', '1')
    effective_dates = set()
    
    for evt_name, rows in _raw_event_data.items():
        for row in rows:
            row_instrument = get_field_case_insensitive(row, 'instrumentid', '')
            row_subinstrument = get_field_case_insensitive(row, 'subinstrumentid', '1') or '1'
            
            if row_instrument == current_instrument and row_subinstrument == target_subinstrument:
                edate = get_field_case_insensitive(row, 'effectivedate', '')
                if edate:
                    effective_dates.add(edate)
    
    return sorted(list(effective_dates))
'''
    
    # Process DSL code - convert EVENT.field to EVENT_field variable name
    import re
    processed_lines = []

    # Determine which events are reference events so we can alter collect() semantics
    reference_events = set()
    for ename, meta in all_event_fields.items():
        if isinstance(meta, dict):
            if str(meta.get('eventType', 'activity')).lower() == 'reference':
                reference_events.add(ename)

    lines = dsl_code.split('\n')
    i = 0
    dsl_line_num = 0
    while i < len(lines):
        dsl_line_num = i + 1  # 1-based DSL line number
        line = lines[i].strip()

        if not line or line.startswith('#') or line.startswith('//'):
            if line.startswith('//'):
                line = '#' + line[2:]
            processed_lines.append(f"        {line}" if line else "")
            i += 1
            continue

        # Replace collect(...) patterns. For reference events, use collect_all
        def _collect_repl(m):
            evt, fld = m.group(1), m.group(2)
            if evt in reference_events:
                return f"collect_all('{evt}_{fld}')"
            return f"collect('{evt}_{fld}')"

        line = re.sub(r"collect\(\s*([A-Z][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\s*\)", _collect_repl, line)

        # collect_by_instrument -> use collect_all for reference events
        def _collect_by_inst_repl(m):
            evt, fld = m.group(1), m.group(2)
            if evt in reference_events:
                return f"collect_all('{evt}_{fld}')"
            return f"collect_by_instrument('{evt}_{fld}')"

        line = re.sub(r"collect_by_instrument\(\s*([A-Z][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\s*\)", _collect_by_inst_repl, line)

        # collect_all(EVENT.field) - always becomes collect_all('EVENT_field')
        line = re.sub(r"collect_all\(\s*([A-Z][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\s*\)", r"collect_all('\1_\2')", line)

        # Convert EVENT.field to EVENT_field
        line = re.sub(r"\b([A-Z][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)", r"\1_\2", line)

        # Replace Python keyword function calls with safe aliases
        line = re.sub(r'\band\s*\(', 'and_op(', line)
        line = re.sub(r'\bor\s*\(', 'or_op(', line)
        line = re.sub(r'\bnot\s*\(', 'not_op(', line)
        line = re.sub(r'\bif\s*\(', 'iif(', line)

        # Add the line with DSL line marker
        processed_lines.append(f"        {line}  # DSL_LINE:{dsl_line_num}")

        i += 1

    python_body = '\n'.join(processed_lines)
    
    # Generate field extraction code for ALL events
    field_extraction_lines = []
    for event_name, meta in all_event_fields.items():
        # meta may be a dict with 'fields' and 'eventType' or a simple list
        if isinstance(meta, dict):
            fields = meta.get('fields', [])
            etype = str(meta.get('eventType', 'activity')).lower()
        else:
            fields = meta
            etype = 'activity'

        field_extraction_lines.append(f"        # Fields from {event_name} ({etype})")

        # Add event-specific standard fields only for activity events
        if etype == 'activity':
            field_extraction_lines.append(
                f"        {event_name}_postingdate = str(get_field_case_insensitive(row, '{event_name}_postingdate', ''))"
            )
            field_extraction_lines.append(
                f"        {event_name}_effectivedate = str(get_field_case_insensitive(row, '{event_name}_effectivedate', ''))"
            )
            field_extraction_lines.append(
                f"        {event_name}_subinstrumentid = str(get_field_case_insensitive(row, '{event_name}_subinstrumentid', '1'))"
            )

        for field in fields:
            field_name = field['name']
            field_type = field.get('datatype', 'string')
            # Variable name: EVENT_FIELD
            var_name = f"{event_name}_{field_name}"

            if field_type == 'decimal':
                field_extraction_lines.append(
                    f"        {var_name} = float(get_field_case_insensitive(row, '{var_name}', 0) or 0)"
                )
            elif field_type in ('integer', 'int'):
                field_extraction_lines.append(
                    f"        {var_name} = int(float(get_field_case_insensitive(row, '{var_name}', 0) or 0))"
                )
            elif field_type == 'date':
                field_extraction_lines.append(
                    f"        {var_name} = str(get_field_case_insensitive(row, '{var_name}', ''))"
                )
            elif field_type == 'boolean':
                field_extraction_lines.append(
                    f"        {var_name} = str(get_field_case_insensitive(row, '{var_name}', '')).lower() in ['true', '1', 'yes']"
                )
            else:
                field_extraction_lines.append(
                    f"        {var_name} = str(get_field_case_insensitive(row, '{var_name}', ''))"
                )
    
    field_extraction_code = '\n'.join(field_extraction_lines)
    
    template = f"""
{imports}
def process_event_data(event_data, raw_event_data=None, override_postingdate=None, override_effectivedate=None):
    # Clear any previous transaction results
    _clear_transaction_results()
    
    _override_postingdate = override_postingdate
    _override_effectivedate = override_effectivedate
    
    # If raw event data provided by the caller, set it for collect() functions
    if raw_event_data is not None:
        set_raw_event_data(raw_event_data)

    # Set global event data for collect() function
    set_all_event_data(event_data)
    
    for row in event_data:
        # Extract standard fields (case-insensitive)
        postingdate = get_field_case_insensitive(row, 'postingdate', '')
        effectivedate = get_field_case_insensitive(row, 'effectivedate', '') or postingdate
        instrumentid = get_field_case_insensitive(row, 'instrumentid', '')
        subinstrumentid = get_field_case_insensitive(row, 'subinstrumentid', '1') or '1'
        # Expose underscore aliases so schedule column formulas can reference them
        posting_date = postingdate
        effective_date = effectivedate
        
        # Set current instrumentid for createTransaction()
        _set_current_instrumentid(instrumentid)
        
        # Set current context for collect() filtering
        set_current_context(instrumentid, postingdate, effectivedate, subinstrumentid)
        
        # Extract fields from all events with proper datatype conversion
{field_extraction_code}
        
        # Execute DSL logic - transactions are created via createTransaction()
{python_body}
    
    # Get all transactions created via createTransaction()
    results = _get_transaction_results()
    return results
"""
    return template

def dsl_to_python(dsl_code: str, event_fields: List[Dict[str, str]]) -> str:
    """Wrapper for backward compatibility - uses multi-event version"""
    # Convert single event fields to dict format
    all_event_fields = {"DEFAULT": event_fields}
    return dsl_to_python_multi_event(dsl_code, all_event_fields)

async def execute_python_template(python_code: str, event_data: List[Dict[str, Any]], raw_event_data: Dict[str, List[Dict]] = None, override_postingdate: str = None, override_effectivedate: str = None) -> Dict[str, Any]:
    """Execute Python template on event data and return transactions + print outputs"""
    # Execute the generated python template in a restricted context and return results.
    try:
        # When executed as package, templates expect to import dsl_functions; ensure package-qualified import
        if "from dsl_functions import" in python_code:
            python_code = python_code.replace("from dsl_functions import", "from backend.dsl_functions import")

        # Provide a minimal execution globals mapping including __file__ so
        # template code that uses os.path.dirname(__file__) will work when
        # executed via exec(). Use the server file path as a sensible base.
        exec_globals = {
            '__file__': os.path.abspath(__file__),
            '__name__': '__dsl_template__',
            '__builtins__': {k: v for k, v in __builtins__.items() if k not in ('exec', 'eval', 'compile', 'open', 'input', 'breakpoint')} if isinstance(__builtins__, dict) else {k: getattr(__builtins__, k) for k in dir(__builtins__) if k not in ('exec', 'eval', 'compile', 'open', 'input', 'breakpoint')},
        }
        # Execute the template which defines helper functions like process_event_data, get_print_outputs
        exec(compile(python_code, '<dsl_template>', 'exec'), exec_globals)

        # Prefer calling process_event_data (multi-event template) and pass raw_event_data.
        # Inspect the signature explicitly so we never swallow internal TypeErrors as a
        # "wrong signature" — that previously caused the 3-arg fallback to bind
        # raw_event_data = override_postingdate (a string), corrupting global state and
        # producing the cryptic "'str' object has no attribute 'items'" error from
        # collect_by_instrument on subsequent calls.
        if 'process_event_data' in exec_globals:
            import inspect as _inspect
            _proc = exec_globals['process_event_data']
            try:
                _sig = _inspect.signature(_proc)
                _param_count = len(_sig.parameters)
            except (TypeError, ValueError):
                _param_count = 4
            if _param_count >= 4:
                transactions = _proc(event_data, raw_event_data, override_postingdate, override_effectivedate)
            else:
                # Older template signature without raw_event_data
                transactions = _proc(event_data, override_postingdate, override_effectivedate)
        elif 'process_standalone' in exec_globals:
            transactions = exec_globals['process_standalone'](override_postingdate, override_effectivedate)
        else:
            raise RuntimeError('Template did not define a process function')
        # Normalize transactions into TransactionOutput models if needed
        # Some DSL helpers (createTransaction) return plain dicts; convert them to
        # TransactionOutput so callers can call `model_dump()` uniformly.
        normalized_transactions = []
        for t in transactions or []:
            try:
                if hasattr(t, 'model_dump'):
                    normalized_transactions.append(t)
                else:
                    normalized_transactions.append(TransactionOutput(**t))
            except Exception:
                # If conversion fails, skip the transaction but continue
                logger.debug(f"Skipping invalid transaction object during normalization: {t}")

        print_outputs = []
        if 'get_print_outputs' in exec_globals:
            try:
                print_outputs = exec_globals['get_print_outputs']()
            except Exception:
                print_outputs = []

        return {"transactions": normalized_transactions, "print_outputs": print_outputs}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        # Dump the generated template so we can inspect the offending line by number
        try:
            with open('/tmp/last_dsl_template.py', 'w') as _f:
                _f.write(python_code)
        except Exception:
            pass
        dsl_line = _extract_dsl_line_from_exception(python_code, e)
        error_msg = str(e)
        if dsl_line:
            error_msg = f"[Line {dsl_line}] {error_msg}"
        logger.error(f"Error executing python template: {error_msg}\nFull traceback:\n{tb}")
        raise HTTPException(status_code=500, detail=error_msg)

# ============= API Endpoints =============

@api_router.get("/")
async def root():
    return {"message": "Fyntrac DSL Studio API"}

@api_router.post("/load-sample-data")
async def load_sample_data():
    """Load sample data for testing"""
    try:
        # Clear existing data
        await db.event_definitions.delete_many({})
        await db.dsl_functions.delete_many({})
        await db.event_data.delete_many({})
        
        # Sample Event Definitions with datatypes
        sample_events = [
            EventDefinition(event_name="LoanEvent", fields=[
                {"name": "principal", "datatype": "decimal"},
                {"name": "rate", "datatype": "decimal"},
                {"name": "term", "datatype": "decimal"}
            ]),
            EventDefinition(event_name="PaymentEvent", fields=[
                {"name": "payment_amount", "datatype": "decimal"},
                {"name": "payment_date", "datatype": "date"},
                {"name": "payment_type", "datatype": "string"}
            ]),
            EventDefinition(event_name="InvestmentEvent", fields=[
                {"name": "initial_investment", "datatype": "decimal"},
                {"name": "return_rate", "datatype": "decimal"},
                {"name": "years", "datatype": "decimal"}
            ]),
            # Custom reference tables
            EventDefinition(
                event_name="RateTable",
                fields=[
                    {"name": "rate_code", "datatype": "string"},
                    {"name": "rate_value", "datatype": "decimal"},
                    {"name": "effective_date", "datatype": "date"},
                    {"name": "expiry_date", "datatype": "date"},
                ],
                eventType="reference",
                eventTable="custom",
            ),
            EventDefinition(
                event_name="ProductConfig",
                fields=[
                    {"name": "product_code", "datatype": "string"},
                    {"name": "product_name", "datatype": "string"},
                    {"name": "max_term", "datatype": "integer"},
                    {"name": "min_principal", "datatype": "decimal"},
                    {"name": "max_principal", "datatype": "decimal"},
                    {"name": "fee_percent", "datatype": "decimal"},
                ],
                eventType="reference",
                eventTable="custom",
            ),
        ]
        
        for event in sample_events:
            doc = event.model_dump()
            doc['created_at'] = doc['created_at'].isoformat()
            await db.event_definitions.insert_one(doc)
        
        # Sample Event Data - LoanEvent
        loan_data = EventData(
            event_name="LoanEvent",
            data_rows=[
                {
                    "postingdate": "2026-01-01",
                    "effectivedate": "2026-01-01",
                    "instrumentid": "LOAN-001",
                    "principal": "100000",
                    "rate": "0.05",
                    "term": "12"
                },
                {
                    "postingdate": "2026-01-15",
                    "effectivedate": "2026-01-15",
                    "instrumentid": "LOAN-002",
                    "principal": "50000",
                    "rate": "0.04",
                    "term": "6"
                },
                {
                    "postingdate": "2026-02-01",
                    "effectivedate": "2026-02-01",
                    "instrumentid": "LOAN-003",
                    "principal": "250000",
                    "rate": "0.06",
                    "term": "24"
                }
            ]
        )
        
        doc = loan_data.model_dump()
        doc['created_at'] = doc['created_at'].isoformat()
        await db.event_data.insert_one(doc)
        
        # Sample Event Data - PaymentEvent (instrumentids match LoanEvent for join)
        payment_data = EventData(
            event_name="PaymentEvent",
            data_rows=[
                {
                    "postingdate": "2026-01-01",
                    "effectivedate": "2026-01-01",
                    "instrumentid": "LOAN-001",
                    "payment_amount": "5000",
                    "payment_date": "2026-01-01",
                    "payment_type": "Principal"
                },
                {
                    "postingdate": "2026-01-15",
                    "effectivedate": "2026-01-15",
                    "instrumentid": "LOAN-002",
                    "payment_amount": "2000",
                    "payment_date": "2026-01-15",
                    "payment_type": "Interest"
                }
            ]
        )
        
        doc = payment_data.model_dump()
        doc['created_at'] = doc['created_at'].isoformat()
        await db.event_data.insert_one(doc)
        
        # Sample Event Data - InvestmentEvent
        investment_data = EventData(
            event_name="InvestmentEvent",
            data_rows=[
                {
                    "postingdate": "2026-01-01",
                    "effectivedate": "2026-01-01",
                    "instrumentid": "LOAN-001",
                    "initial_investment": "10000",
                    "return_rate": "0.08",
                    "years": "5"
                },
                {
                    "postingdate": "2026-01-15",
                    "effectivedate": "2026-01-15",
                    "instrumentid": "LOAN-002",
                    "initial_investment": "25000",
                    "return_rate": "0.10",
                    "years": "10"
                }
            ]
        )
        
        doc = investment_data.model_dump()
        doc['created_at'] = doc['created_at'].isoformat()
        await db.event_data.insert_one(doc)

        # Sample Custom Reference Data - RateTable
        rate_table_data = EventData(
            event_name="RateTable",
            data_rows=[
                {"rate_code": "PRIME", "rate_value": "0.0525", "effective_date": "2025-01-01", "expiry_date": "2025-06-30"},
                {"rate_code": "PRIME", "rate_value": "0.0500", "effective_date": "2025-07-01", "expiry_date": "2025-12-31"},
                {"rate_code": "PRIME", "rate_value": "0.0475", "effective_date": "2026-01-01", "expiry_date": "2026-12-31"},
                {"rate_code": "BASE",  "rate_value": "0.0400", "effective_date": "2025-01-01", "expiry_date": "2025-12-31"},
                {"rate_code": "BASE",  "rate_value": "0.0375", "effective_date": "2026-01-01", "expiry_date": "2026-12-31"},
                {"rate_code": "LIBOR", "rate_value": "0.0310", "effective_date": "2025-01-01", "expiry_date": "2025-12-31"},
                {"rate_code": "LIBOR", "rate_value": "0.0290", "effective_date": "2026-01-01", "expiry_date": "2026-12-31"},
            ]
        )
        doc = rate_table_data.model_dump()
        doc['created_at'] = doc['created_at'].isoformat()
        await db.event_data.insert_one(doc)

        # Sample Custom Reference Data - ProductConfig
        product_config_data = EventData(
            event_name="ProductConfig",
            data_rows=[
                {"product_code": "HL-STD",  "product_name": "Standard Home Loan",    "max_term": "360", "min_principal": "50000",  "max_principal": "2000000", "fee_percent": "0.005"},
                {"product_code": "HL-FIX",  "product_name": "Fixed Rate Home Loan",  "max_term": "300", "min_principal": "100000", "max_principal": "1500000", "fee_percent": "0.0075"},
                {"product_code": "PL-UNSEC","product_name": "Unsecured Personal Loan","max_term": "84",  "min_principal": "5000",   "max_principal": "100000",  "fee_percent": "0.010"},
                {"product_code": "BL-SME",  "product_name": "SME Business Loan",     "max_term": "120", "min_principal": "20000",  "max_principal": "500000",  "fee_percent": "0.008"},
                {"product_code": "INV-TERM","product_name": "Term Investment",        "max_term": "60",  "min_principal": "10000",  "max_principal": "5000000", "fee_percent": "0.000"},
            ]
        )
        doc = product_config_data.model_dump()
        doc['created_at'] = doc['created_at'].isoformat()
        await db.event_data.insert_one(doc)

        # Sample DSL Code - Loan Validation, Fee Calculation & Investment Projection
        sample_dsl_code = """## 1. Loan Validation and Fee Calculation
## Reference data access for single-row config
min_p = ProductConfig.min_principal or 0
max_p = ProductConfig.max_principal or 1000000
principal = LoanEvent.principal or 0

## Check if loan principal is within allowed range
is_valid = and(gte(principal, min_p), lte(principal, max_p))

## Calculate fee using percentage from ProductConfig
fee_pct = ProductConfig.fee_percent or 0.01
loan_fee = iif(is_valid, multiply(principal, fee_pct), 0)
print(concat("Calculated Loan Fee: ", loan_fee))

## 2. Loan Payment Calculation
annual_rate = LoanEvent.rate or 0.05
monthly_rate = divide(annual_rate, 12)
term_months = LoanEvent.term or 360

## Use multiply to handle negation for the PV argument in pmt()
neg_principal = multiply(principal, -1)
monthly_pmt = pmt(monthly_rate, term_months, neg_principal)
print(concat("Expected Monthly Payment: ", monthly_pmt))

## 3. Investment Growth Projection
init_inv = InvestmentEvent.initial_investment or 0
ret_rate = InvestmentEvent.return_rate or 0
inv_years = InvestmentEvent.years or 0

## Future value calculation
neg_inv = multiply(init_inv, -1)
future_val = fv(ret_rate, inv_years, 0, neg_inv)
print(concat("Projected Investment Value: ", future_val))

## 4. Create Transactions
## Use global postingdate and effectivedate (no prefixes needed)

## Only create fee transaction if the amount is greater than 0
iif(gt(loan_fee, 0), createTransaction(postingdate, effectivedate, "Loan Processing Fee", loan_fee), 0)

## Record the monthly interest accrual
monthly_interest = multiply(principal, monthly_rate)
createTransaction(postingdate, effectivedate, "Interest Accrual", monthly_interest)"""
        
        return {
            "message": "Sample data loaded successfully",
            "events": ["LoanEvent", "PaymentEvent", "InvestmentEvent", "RateTable", "ProductConfig"],
            "sample_dsl_code": sample_dsl_code
        }
    except Exception as e:
        # If database is not available, fall back to in-memory sample data
        logger.warning(f"Could not load sample data into MongoDB, falling back to in-memory: {str(e)}")
        try:
            # Populate in-memory structures for tests
            global USE_IN_MEMORY, in_memory_data
            USE_IN_MEMORY = True
            in_memory_data['event_definitions'] = SAMPLE_EVENTS
            in_memory_data['templates'] = SAMPLE_TEMPLATES
            # Create sample event_data entries similar to DB documents
            # Build simple event_data entries from SAMPLE_EVENTS for tests
            simple_event_docs = []
            for evt in SAMPLE_EVENTS:
                doc = {
                    'id': evt.get('id', str(uuid.uuid4())),
                    'event_name': evt['event_name'],
                    'data_rows': [],
                    'created_at': evt.get('created_at', datetime.now(timezone.utc)).isoformat()
                }
                simple_event_docs.append(doc)
            in_memory_data['event_data'] = simple_event_docs
        except Exception:
            logger.exception("Failed to populate in-memory sample data")

        sample_dsl_code = """## 1. Loan Validation and Fee Calculation
## Reference data access for single-row config
min_p = ProductConfig.min_principal or 0
max_p = ProductConfig.max_principal or 1000000
principal = LoanEvent.principal or 0

## Check if loan principal is within allowed range
is_valid = and(gte(principal, min_p), lte(principal, max_p))

## Calculate fee using percentage from ProductConfig
fee_pct = ProductConfig.fee_percent or 0.01
loan_fee = iif(is_valid, multiply(principal, fee_pct), 0)
print(concat("Calculated Loan Fee: ", loan_fee))

## 2. Loan Payment Calculation
annual_rate = LoanEvent.rate or 0.05
monthly_rate = divide(annual_rate, 12)
term_months = LoanEvent.term or 360

## Use multiply to handle negation for the PV argument in pmt()
neg_principal = multiply(principal, -1)
monthly_pmt = pmt(monthly_rate, term_months, neg_principal)
print(concat("Expected Monthly Payment: ", monthly_pmt))

## 3. Investment Growth Projection
init_inv = InvestmentEvent.initial_investment or 0
ret_rate = InvestmentEvent.return_rate or 0
inv_years = InvestmentEvent.years or 0

## Future value calculation
neg_inv = multiply(init_inv, -1)
future_val = fv(ret_rate, inv_years, 0, neg_inv)
print(concat("Projected Investment Value: ", future_val))

## 4. Create Transactions
## Use global postingdate and effectivedate (no prefixes needed)

## Only create fee transaction if the amount is greater than 0
iif(gt(loan_fee, 0), createTransaction(postingdate, effectivedate, "Loan Processing Fee", loan_fee), 0)

## Record the monthly interest accrual
monthly_interest = multiply(principal, monthly_rate)
createTransaction(postingdate, effectivedate, "Interest Accrual", monthly_interest)"""
        return {
            "message": "Sample data loaded into memory (DB unavailable)",
            "events": [e['event_name'] for e in SAMPLE_EVENTS],
            "sample_dsl_code": sample_dsl_code
        }

@api_router.delete("/clear-all-data")
async def clear_all_data():
    """Clear all data from the system except templates"""
    try:
        # Delete all collections EXCEPT templates
        await db.event_definitions.delete_many({})
        await db.event_data.delete_many({})
        await db.transaction_reports.delete_many({})
        await db.custom_functions.delete_many({})
        await db.saved_rules.delete_many({})
        await db.saved_schedules.delete_many({})

        # Also clear in-memory fallback data so stale entries don't survive
        global in_memory_data
        in_memory_data['event_definitions'] = []
        in_memory_data['event_data'] = []
        in_memory_data['transaction_reports'] = []
        in_memory_data['custom_functions'] = []
        in_memory_data.pop('saved_rules', None)
        in_memory_data.pop('saved_schedules', None)

        return {
            "message": "All data cleared successfully (templates preserved).",
            "cleared": ["event_definitions", "event_data", "transaction_reports", "custom_functions", "saved_rules", "saved_schedules"],
            "preserved": ["templates"]
        }
    except Exception as e:
        logger.error(f"Error clearing data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.post("/events/upload")
async def upload_event_definitions(file: UploadFile = File(...)):
    """Upload event definitions CSV (EventName, EventField, DataType[, EventType[, EventTable]])"""
    try:
        # Clear existing event definitions (preserve other collections)
        try:
            await db.event_definitions.delete_many({})
        except Exception:
            logger.warning("Could not clear event_definitions in DB - continuing with in-memory fallback")
        content = await file.read()
        csv_content = content.decode('utf-8')
        rows = parse_csv_content(csv_content)
        
        if len(rows) < 2:
            raise HTTPException(status_code=400, detail="CSV must have header and at least one row")
        
        # Parse events from CSV
        events_dict = {}
        header = rows[0]
        
        # Validate header - support optional EventType (4th) and EventTable (5th) columns
        if len(header) < 3 or header[0].lower() != 'eventname' or header[1].lower() != 'eventfield' or header[2].lower() != 'datatype':
            raise HTTPException(status_code=400, detail="CSV must have columns: EventName, EventField, DataType[, EventType[, EventTable]]")

        # Supported event types and event table values
        VALID_EVENT_TYPES = ('activity', 'reference')
        VALID_EVENT_TABLES = ('standard', 'custom')

        # Temporary maps to capture event-level values
        event_type_map = {}
        event_table_map = {}

        for row in rows[1:]:
            if len(row) >= 3:
                event_name = row[0].strip()
                event_field = row[1].strip()
                data_type = row[2].strip().lower()

                # Optional columns 4 and 5: EventType and EventTable.
                # Some CSVs omit EventType and put EventTable in column 4.
                # Detect that case: if column 4 value matches EventTable values
                # (standard/custom) but not EventType values (activity/reference),
                # treat it as EventTable and default EventType to 'activity'.
                event_type = 'activity'
                event_table = 'standard'
                col4 = row[3].strip().lower() if len(row) >= 4 and row[3].strip() else None
                col5 = row[4].strip().lower() if len(row) >= 5 and row[4].strip() else None

                if col4 is not None:
                    if col4 in VALID_EVENT_TYPES:
                        # Normal layout: col4 = EventType, col5 = EventTable
                        event_type = col4
                        if col5 is not None:
                            if col5 not in VALID_EVENT_TABLES:
                                raise HTTPException(status_code=400, detail=f"Invalid eventTable '{row[4]}'. Must be one of: {', '.join(VALID_EVENT_TABLES)}")
                            event_table = col5
                    elif col4 in VALID_EVENT_TABLES:
                        # Shifted layout: col4 = EventTable, EventType defaults to 'activity'
                        event_table = col4
                    else:
                        raise HTTPException(status_code=400, detail=f"Invalid value '{row[3]}' in column 4. Must be an eventType ({', '.join(VALID_EVENT_TYPES)}) or eventTable ({', '.join(VALID_EVENT_TABLES)}).")

                # Validate eventTable + eventType combination
                if event_table == 'standard' and event_type != 'activity':
                    raise HTTPException(status_code=400, detail=f"Event '{event_name}': standard event table must have eventType 'activity', got '{event_type}'")

                # Validate datatype
                if data_type not in ['string', 'date', 'boolean', 'decimal', 'integer', 'int']:
                    raise HTTPException(status_code=400, detail=f"Invalid datatype '{data_type}'. Must be one of: string, date, boolean, decimal, integer")

                # Ensure event_type is consistent across rows for same event
                if event_name in event_type_map and event_type_map[event_name] != event_type:
                    raise HTTPException(status_code=400, detail=f"Conflicting eventType values for event '{event_name}'")
                event_type_map[event_name] = event_type

                # Ensure event_table is consistent across rows for same event
                if event_name in event_table_map and event_table_map[event_name] != event_table:
                    raise HTTPException(status_code=400, detail=f"Conflicting eventTable values for event '{event_name}'")
                event_table_map[event_name] = event_table

                if event_name not in events_dict:
                    events_dict[event_name] = []
                events_dict[event_name].append({"name": event_field, "datatype": data_type})
        
        # Try to store in database; if DB unavailable, fall back to in-memory storage
        try:
            # Clear existing events
            await db.event_definitions.delete_many({})

            # Store in database
            for event_name, fields in events_dict.items():
                evt_type = event_type_map.get(event_name, 'activity')
                evt_table = event_table_map.get(event_name, 'standard')
                event = EventDefinition(event_name=event_name, fields=fields, eventType=evt_type, eventTable=evt_table)
                doc = event.model_dump()
                doc['created_at'] = doc['created_at'].isoformat()
                await db.event_definitions.insert_one(doc)

            return {
                "message": f"Uploaded {len(events_dict)} event definitions with datatypes",
                "events": list(events_dict.keys())
            }
        except Exception as e:
            # Fallback to in-memory storage so tests and offline runs work
            logger.warning(f"Could not write event definitions to DB, using in-memory storage: {e}")
            in_memory_defs = []
            for event_name, fields in events_dict.items():
                evt_type = event_type_map.get(event_name, 'activity')
                evt_table = event_table_map.get(event_name, 'standard')
                event = EventDefinition(event_name=event_name, fields=fields, eventType=evt_type, eventTable=evt_table)
                doc = event.model_dump()
                # store created_at as ISO string for consistency with DB format
                doc['created_at'] = doc['created_at'].isoformat()
                in_memory_defs.append(doc)

            in_memory_data['event_definitions'] = in_memory_defs
            return {
                "message": f"Uploaded {len(events_dict)} event definitions to in-memory store",
                "events": list(events_dict.keys())
            }
    except Exception as e:
        logger.error(f"Error uploading events: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@api_router.get("/events")
async def get_events():
    """Get all event definitions"""
    try:
        events = await db.event_definitions.find({}, {"_id": 0}).to_list(1000)
        for event in events:
            if isinstance(event.get('created_at'), str):
                event['created_at'] = datetime.fromisoformat(event['created_at'])
        return events
    except Exception as e:
        logger.warning(f"Could not load events from database: {str(e)}")
        # Only return sample data if MongoDB is unavailable (connection error)
        logger.info("Returning sample events due to DB error")
        return SAMPLE_EVENTS

# DSL Functions (Hardcoded + Custom)
@api_router.get("/dsl-functions")
async def get_dsl_functions():
    """Get all DSL functions (hardcoded + custom)"""
    # Get hardcoded functions
    all_functions = list(DSL_FUNCTION_METADATA)
    
    # Get custom functions and convert to same format
    try:
        custom_funcs = await db.custom_functions.find({}, {"_id": 0}).to_list(1000)
        for func in custom_funcs:
            params = ', '.join([f"{p['name']}: {p['type']}" for p in func['parameters']])
            all_functions.append({
                "name": func['name'],
                "params": params,
                "description": func['description'],
                "category": func['category'],
                "is_custom": True
            })
    except Exception as e:
        logger.warning(f"Could not load custom functions from database: {str(e)}")
        # Include in-memory custom functions if DB unavailable
        for func in in_memory_data.get('custom_functions', []):
            try:
                params = ', '.join([f"{p['name']}: {p['type']}" for p in func.get('parameters', [])])
                all_functions.append({
                    "name": func.get('name'),
                    "params": params,
                    "description": func.get('description', ''),
                    "category": func.get('category', 'Custom'),
                    "is_custom": True
                })
            except Exception:
                continue
    
    return all_functions


# Download Event Definitions as CSV
@api_router.get("/events/download")
async def download_event_definitions():
    """Download all event definitions as a CSV file"""
    try:
        # Try DB first
        try:
            events = await db.event_definitions.find({}, {"_id": 0}).to_list(1000)
        except Exception:
            events = in_memory_data.get('event_definitions', SAMPLE_EVENTS)

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['EventName', 'EventField', 'DataType', 'EventType', 'EventTable'])

        for event in events:
            evt_type = event.get('eventType', 'activity')
            evt_table = event.get('eventTable', 'standard')
            for field in event.get('fields', []):
                writer.writerow([event.get('event_name'), field.get('name'), field.get('datatype'), evt_type, evt_table])

        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type='text/csv',
            headers={"Content-Disposition": "attachment; filename=event_definitions.csv"}
        )
    except Exception as e:
        logger.error(f"Error generating events CSV: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Event Data - Excel Upload (multiple sheets for multiple events)
@api_router.post("/event-data/upload-excel")
async def upload_event_data_excel(file: UploadFile = File(...)):
    """Upload event data from Excel file - each sheet represents one event"""
    try:
        # NOTE: do NOT clear existing event data before validating the incoming file.
        # We will only replace data for specific events after full validation passes.
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(status_code=400, detail="File must be an Excel file (.xlsx or .xls)")
        
        content = await file.read()
        
        # Read Excel file with all sheets
        excel_file = pd.ExcelFile(io.BytesIO(content))
        sheet_names = excel_file.sheet_names
        
        # First pass: Collect all postingdates across all sheets to validate single date
        # Only collect from activity events (not custom reference events which are tenant-level data)
        all_posting_dates = set()
        sheet_data_cache = {}  # Cache sheet data to avoid re-reading
        # Pre-fetch event definitions for each sheet to determine eventType/eventTable
        sheet_event_defs = {}
        
        for sheet_name in sheet_names:
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            sheet_data_cache[sheet_name] = df
            
            # Look up the event definition for this sheet
            try:
                evt_def = await db.event_definitions.find_one(
                    {"event_name": {"$regex": f"^{sheet_name}$", "$options": "i"}},
                    {"_id": 0}
                )
            except Exception:
                evt_def = next((e for e in in_memory_data.get('event_definitions', []) if str(e.get('event_name', '')).lower() == sheet_name.lower()), None)
            sheet_event_defs[sheet_name] = evt_def
            
            # Determine if this is a custom reference event (tenant-level, no instrument/date fields)
            is_reference = (evt_def and evt_def.get('eventTable') == 'custom' and evt_def.get('eventType') == 'reference')
            
            if df.empty or is_reference:
                continue
            
            # Look for postingdate column (case-insensitive)
            posting_col = None
            for col in df.columns:
                if str(col).lower() == 'postingdate':
                    posting_col = col
                    break
            
            if posting_col:
                # Extract all non-null postingdates
                posting_dates = df[posting_col].dropna().unique()
                for pd_val in posting_dates:
                    if pd_val and str(pd_val).strip():
                        # Normalize date format
                        date_str = str(pd_val).strip().split(' ')[0]  # Handle datetime strings
                        all_posting_dates.add(date_str)
        
        # Validate single postingdate across all events
        if len(all_posting_dates) > 1:
            raise HTTPException(
                status_code=400, 
                detail=f"Multiple posting dates found across events: {sorted(all_posting_dates)}. All events must have the same postingdate."
            )

        # Enforce maximum rows per sheet: do not proceed if any sheet exceeds the limit
        MAX_ROWS_PER_SHEET = 500
        for sheet_name, df in sheet_data_cache.items():
            try:
                row_count = int(df.shape[0])
            except Exception:
                row_count = 0
            if row_count > MAX_ROWS_PER_SHEET:
                raise HTTPException(status_code=400, detail="Upload failed: This file exceeds the allowed row limit. A maximum of 500 rows per table is supported.")
        
        uploaded_events = []
        errors = []
        
        # Note: do not wipe all event data here; only replace data for the target event below.
        
        for sheet_name in sheet_names:
            # Use pre-fetched event definition from first pass
            event = sheet_event_defs.get(sheet_name)
            
            if not event:
                errors.append(f"Sheet '{sheet_name}' - No matching event definition found")
                continue
            
            # Determine if this is a custom reference event
            is_reference = (event.get('eventTable') == 'custom' and event.get('eventType') == 'reference')
            
            # Use cached data
            df = sheet_data_cache.get(sheet_name)
            
            if df is None or df.empty:
                errors.append(f"Sheet '{sheet_name}' - No data rows found")
                continue
            
            # Convert DataFrame to list of dicts and normalize headers to event field names
            df = df.fillna('')
            raw_rows = df.to_dict('records')

            # Normalizer for header/field names
            def _normalize(s: str) -> str:
                import re
                return re.sub(r'[^A-Za-z0-9]', '_', (s or '').strip()).strip('_').upper()

            field_names = [f['name'] for f in event.get('fields', [])]
            norm_to_field = { _normalize(fn): fn for fn in field_names }

            # Build header mapping summary for reporting
            remapped_headers = []
            # Determine mapping from actual sheet columns to canonical field names
            sheet_columns = list(df.columns)
            col_to_field = {}
            for col in sheet_columns:
                mapped_to = None
                col_norm = _normalize(col)
                if col_norm in norm_to_field:
                    mapped_to = norm_to_field[col_norm]
                    col_to_field[col] = mapped_to
                else:
                    # leave as original column name
                    col_to_field[col] = col
                remapped_headers.append({"incoming": str(col), "mapped_to": mapped_to})

            data_rows = []
            for raw in raw_rows:
                mapped = {}
                for h, v in raw.items():
                    mapped_key = col_to_field.get(h, h)
                    mapped[mapped_key] = v
                data_rows.append(mapped)

            # Get field types from event definition
            field_types = {f['name']: f.get('datatype', 'string') for f in event.get('fields', [])}

            cleaned_rows = []
            # Track coercions summary: field -> coerced_count
            coercions = {}
            for row in data_rows:
                cleaned_row = {}
                for key, value in row.items():
                    field_type = field_types.get(str(key), 'string')
                    # Normalize NaN / empty / 'None' values
                    if pd.isna(value) or str(value).strip() == '' or str(value).strip().lower() in ('none', 'null'):
                        if field_type in ('decimal', 'float'):
                            cleaned_row[str(key)] = 0.0
                            coercions[str(key)] = coercions.get(str(key), 0) + 1
                        elif field_type in ('integer', 'int'):
                            cleaned_row[str(key)] = 0
                            coercions[str(key)] = coercions.get(str(key), 0) + 1
                        else:
                            cleaned_row[str(key)] = ''
                    # Date fields: normalize to yyyy-mm-dd (scalar or list)
                    elif str(field_type).lower() in ('date', 'datetime', 'timestamp'):
                        nv = _normalize_ingest_date_value(value)
                        cleaned_row[str(key)] = nv
                    elif field_type in ('decimal', 'float'):
                        try:
                            cleaned_row[str(key)] = float(value)
                        except Exception:
                            cleaned_row[str(key)] = 0.0
                            coercions[str(key)] = coercions.get(str(key), 0) + 1
                    elif field_type in ('integer', 'int'):
                        try:
                            cleaned_row[str(key)] = int(float(value))
                        except Exception:
                            cleaned_row[str(key)] = 0
                            coercions[str(key)] = coercions.get(str(key), 0) + 1
                    else:
                        cleaned_row[str(key)] = str(value)
                cleaned_rows.append(cleaned_row)
            # Normalize standard date fields only for non-reference events
            if not is_reference:
                for r in cleaned_rows:
                    for dkey in list(r.keys()):
                        if str(dkey).lower() in ('postingdate', 'effectivedate', 'posting_date', 'effective_date'):
                            r[dkey] = _normalize_ingest_date_value(r.get(dkey))
                    # Ensure standard date fields are normalized even if not declared as date type
                    for dkey in list(cleaned_row.keys()):
                        if str(dkey).lower() in ('postingdate', 'effectivedate', 'posting_date', 'effective_date'):
                            cleaned_row[dkey] = _normalize_ingest_date_value(cleaned_row.get(dkey))

            # Store event data
            event_data = EventData(event_name=event['event_name'], data_rows=cleaned_rows)
            doc = event_data.model_dump()
            doc['created_at'] = doc['created_at'].isoformat()
            
            # Replace existing data for this event
            await db.event_data.delete_many({"event_name": event['event_name']})
            await db.event_data.insert_one(doc)
            
            uploaded_events.append({
                "event_name": event['event_name'],
                "sheet_name": sheet_name,
                "rows_uploaded": len(cleaned_rows),
                "remapped_headers": remapped_headers,
                "coercions": coercions if coercions else None
            })
        
        posting_date_info = list(all_posting_dates)[0] if all_posting_dates else "No posting dates found"

        summary = {
            "message": f"Processed {len(sheet_names)} sheets",
            "posting_date": posting_date_info,
            "uploaded_events": uploaded_events,
            "errors": errors if errors else None
        }

        logger.info(f"Excel event data upload summary: {summary}")

        return summary
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading Excel event data: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))


@api_router.get("/event-data")
async def get_all_event_data():
    """Get summary of all uploaded event data"""
    event_data_list = await db.event_data.find({}, {"_id": 0}).to_list(1000)
    summary = []
    for event_data in event_data_list:
        summary.append({
            "event_name": event_data['event_name'],
            "row_count": len(event_data.get('data_rows', [])),
            "created_at": event_data.get('created_at')
        })
    return summary


@api_router.get("/event-data/posting-dates")
async def get_event_data_posting_dates():
    """
    Return all unique posting dates found across all activity (non-custom/reference) event data,
    sorted ascending.  Custom / reference events have no posting date and are excluded.
    """
    # Identify activity event names
    defs = await db.event_definitions.find({}, {"_id": 0, "event_name": 1, "eventType": 1}).to_list(1000)
    activity_names: set = {
        d["event_name"] for d in defs
        if d.get("eventType", "activity") != "reference"
    }

    # Collect posting dates from activity event data
    unique_dates: set = set()
    event_data_list = await db.event_data.find({}, {"_id": 0, "event_name": 1, "data_rows": 1}).to_list(1000)
    for ed in event_data_list:
        if ed.get("event_name") not in activity_names:
            continue
        for row in ed.get("data_rows", []):
            pd_val = get_field_case_insensitive(row, "postingdate", "")
            if pd_val:
                unique_dates.add(str(pd_val).strip())

    return {"posting_dates": sorted(unique_dates)}


@api_router.get("/event-data/download/{event_name}")
async def download_event_data(event_name: str):
    """Download event data as CSV"""
    event_data = await db.event_data.find_one({"event_name": event_name}, {"_id": 0})
    if not event_data:
        raise HTTPException(status_code=404, detail=f"No data found for event '{event_name}'")

    # Create CSV. Collect ALL unique headers across every row so rows with different
    # field sets (common with JSON-imported data) do not cause DictWriter to crash.
    output = io.StringIO()
    rows = event_data.get('data_rows', [])
    if rows:
        all_keys: list = []
        seen_keys: set = set()
        for row in rows:
            for k in row.keys():
                if k not in seen_keys:
                    all_keys.append(k)
                    seen_keys.add(k)
        writer = csv.DictWriter(
            output, fieldnames=all_keys, extrasaction='ignore', restval=''
        )
        writer.writeheader()
        writer.writerows(rows)

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={event_name}_data.csv"}
    )


@api_router.get("/event-data/{event_name}")
async def get_event_data(event_name: str):
    """Get event data for a specific event"""
    event_data = await db.event_data.find_one({"event_name": event_name}, {"_id": 0})
    if not event_data:
        return {"event_name": event_name, "data_rows": []}
    
    if isinstance(event_data.get('created_at'), str):
        event_data['created_at'] = datetime.fromisoformat(event_data['created_at'])
    
    return event_data

@api_router.post("/dsl/run")
async def run_dsl_code(request: DSLRunRequest):
    """Run DSL code directly and return results (for console testing)"""
    try:
        dsl_code = request.dsl_code
        
        # Extract all event names referenced in the DSL code
        referenced_events = extract_event_names_from_dsl(dsl_code)
        
        # If no event references, run in standalone mode (for schedule functions, calculations, etc.)
        if not referenced_events:
            # Create standalone execution template
            python_code = dsl_to_python_standalone(dsl_code)
            try:
                try:
                    compile(python_code, '<dsl_standalone>', 'exec')
                except SyntaxError as se:
                    # Log the problematic line for debugging
                    py_lines = python_code.split('\n')
                    err_lineno = se.lineno or 0
                    context_start = max(0, err_lineno - 3)
                    context_end = min(len(py_lines), err_lineno + 2)
                    context = '\n'.join(f"  {'>>>' if i+1 == err_lineno else '   '} {i+1}: {py_lines[i]}" for i in range(context_start, context_end))
                    logger.error(f"Syntax error in generated standalone code at Python line {err_lineno}:\n{context}")
                    # Try to extract DSL line from the error line
                    dsl_line = None
                    if 1 <= err_lineno <= len(py_lines):
                        m = re.search(r'# DSL_LINE:(\d+)', py_lines[err_lineno - 1])
                        if m:
                            dsl_line = int(m.group(1))
                    error_msg = se.msg or "invalid syntax"
                    if dsl_line:
                        error_msg = f"[Line {dsl_line}] SyntaxError: {error_msg}"
                    else:
                        error_msg = f"SyntaxError: {error_msg}"
                    return {
                        "success": False,
                        "error": error_msg,
                        "transactions": []
                    }
                # Provide minimal globals including __file__ so any code
                # referencing __file__ (e.g., os.path.dirname(__file__))
                # does not raise NameError when executed here.
                exec_globals = {
                    '__file__': os.path.abspath(__file__),
                    '__name__': '__dsl_standalone__',
                    '__builtins__': {k: v for k, v in __builtins__.items() if k not in ('exec', 'eval', 'compile', 'open', 'input', 'breakpoint')} if isinstance(__builtins__, dict) else {k: getattr(__builtins__, k) for k in dir(__builtins__) if k not in ('exec', 'eval', 'compile', 'open', 'input', 'breakpoint')},
                }
                exec(compile(python_code, '<dsl_standalone>', 'exec'), exec_globals)
                
                # Clear any previous print outputs
                clear_prints = exec_globals.get('clear_print_outputs')
                if clear_prints:
                    clear_prints()
                
                # Get the process function
                process_func = exec_globals.get('process_standalone')
                if not process_func:
                    raise ValueError("Generated code does not contain process_standalone function")
                
                # Execute standalone
                results, print_outputs = process_func(request.posting_date, request.effective_date)
                
                # Convert to TransactionOutput models
                transactions = [TransactionOutput(**result) for result in results]
                
                return {
                    "success": True,
                    "transactions": [t.model_dump() for t in transactions],
                    "events_used": [],
                    "row_count": 1,
                    "print_outputs": print_outputs,
                    "mode": "standalone"
                }
            except Exception as e:
                dsl_line = _extract_dsl_line_from_exception(python_code, e)
                error_msg = str(e)
                if dsl_line:
                    error_msg = f"[Line {dsl_line}] {error_msg}"
                logger.error(f"Standalone DSL error: {error_msg}")
                return {
                    "success": False,
                    "error": error_msg,
                    "transactions": []
                }
        
        # Load event definitions and data for all referenced events
        # Build event metadata and raw data maps. Respect eventType: activity vs reference
        all_event_fields = {}
        event_data_dict = {}
        activity_event_data = {}
        activity_events_with_data = []
        events_without_data = []
        reference_events_with_data = []

        for event_name in referenced_events:
            event_def = await db.event_definitions.find_one(
                {"event_name": {"$regex": f"^{event_name}$", "$options": "i"}}, 
                {"_id": 0}
            )
            if not event_def:
                return {
                    "success": False,
                    "error": f"Event definition '{event_name}' not found",
                    "transactions": []
                }

            # Store fields and eventType so generator can behave differently for reference events
            evt_name = event_def['event_name']
            evt_type = event_def.get('eventType', 'activity')
            all_event_fields[evt_name] = {
                'fields': event_def.get('fields', []),
                'eventType': evt_type
            }

            event_data = await db.event_data.find_one(
                {"event_name": {"$regex": f"^{event_name}$", "$options": "i"}}, 
                {"_id": 0}
            )
            rows = event_data['data_rows'] if (event_data and event_data.get('data_rows')) else []
            event_data_dict[evt_name] = rows

            if evt_type == 'activity':
                activity_event_data[evt_name] = rows
                if rows:
                    activity_events_with_data.append(evt_name)
                else:
                    events_without_data.append(evt_name)
            else:
                # reference event
                if rows:
                    reference_events_with_data.append(evt_name)
        
        # Determine merged rows to iterate over:
        # - If we have activity events with data, merge them by instrument
        # - If no activity data but reference events present, create a single dummy row so template runs once
        # - Otherwise, error (no data at all)
        date_fallback_warning = None
        if activity_events_with_data:
            # Filter by requested posting date before merging (Console date-scoped runs)
            scoped_activity = (
                filter_event_data_by_posting_date(activity_event_data, request.posting_date)
                if request.posting_date
                else activity_event_data
            )
            merged_data = merge_event_data_by_instrument(scoped_activity)
            
            # If no data for the requested posting date, fall back to all available data
            if not merged_data and request.posting_date:
                logger.info(f"No data for posting date {request.posting_date}, falling back to all available data")
                merged_data = merge_event_data_by_instrument(activity_event_data)
                if merged_data:
                    # Collect available posting dates for the warning
                    available_dates = set()
                    for rows in activity_event_data.values():
                        for row in rows:
                            pd = str(get_field_case_insensitive(row, "postingdate", "")).strip()
                            if pd:
                                available_dates.add(pd)
                    date_fallback_warning = (
                        f"No data found for posting date {request.posting_date}. "
                        f"Using all available data. Available posting dates: {sorted(available_dates)}"
                    )
        elif reference_events_with_data:
            # No activity rows but we have reference data — run template once with an empty merged row
            merged_data = [{}]
        else:
            return {
                "success": False,
                "error": f"No data found for any referenced events: {referenced_events}",
                "transactions": []
            }
        
        if not merged_data:
            return {
                "success": False,
                "error": "No data found after merging events",
                "transactions": []
            }
        
        # Generate and execute Python code. Pass event metadata (fields + eventType)
        python_code = dsl_to_python_multi_event(dsl_code, all_event_fields)
        execution_result = await execute_python_template(
            python_code, 
            merged_data,
            event_data_dict,  # Pass raw event data for collect() functions
            request.posting_date,
            request.effective_date
        )
        
        transactions = execution_result["transactions"]
        print_outputs = execution_result["print_outputs"]
        
        # Build events_used list: activity events with data + reference events with data
        events_used = activity_events_with_data + reference_events_with_data

        result = {
            "success": True,
            "transactions": [t.model_dump() for t in transactions],
            "events_used": events_used,
            "row_count": len(merged_data),
            "print_outputs": print_outputs
        }
        
        # Add warning if some events had no data
        if events_without_data:
            result["warning"] = f"No data for events: {events_without_data}. Their fields defaulted to 0/empty."
            result["events_without_data"] = events_without_data
        
        # Add warning if we fell back to all dates
        if date_fallback_warning:
            existing_warning = result.get("warning", "")
            result["warning"] = (existing_warning + " " + date_fallback_warning).strip()
        
        return result
    except HTTPException as he:
        # Re-extract the detail from execute_python_template which already includes [Line N]
        logger.error(f"DSL run error: {he.detail}")
        return {
            "success": False,
            "error": he.detail,
            "transactions": []
        }
    except Exception as e:
        logger.error(f"DSL run error: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "transactions": []
        }

# Templates
@api_router.post("/templates")
async def save_template(request: SaveTemplateRequest):
    """Save DSL code as a reusable template"""
    try:
        # Get event definition (try DB, fall back to in-memory)
        event = None
        try:
            event = await db.event_definitions.find_one({"event_name": request.event_name}, {"_id": 0})
        except Exception:
            # DB unavailable - check in-memory definitions
            logger.warning("DB unavailable when fetching event definition, checking in-memory data")
            for e in in_memory_data.get('event_definitions', []):
                if str(e.get('event_name', '')).lower() == request.event_name.lower():
                    event = e
                    break

        if not event:
            raise HTTPException(status_code=404, detail=f"Event '{request.event_name}' not found")

        # Check for existing template with same name
        # Check for existing template with same name (DB first, then in-memory)
        existing = None
        try:
            existing = await db.dsl_templates.find_one({"name": request.name}, {"_id": 0})
        except Exception:
            for t in in_memory_data.get('templates', []):
                if t.get('name', '').lower() == request.name.lower():
                    existing = t
                    break

        if existing:
            if not request.replace:
                raise HTTPException(
                    status_code=409,
                    detail=f"Template with name '{request.name}' already exists. Set replace=true to overwrite."
                )
            # Delete existing template (DB or in-memory)
            try:
                await db.dsl_templates.delete_one({"name": request.name})
            except Exception:
                in_memory_data['templates'] = [t for t in in_memory_data.get('templates', []) if t.get('name', '').lower() != request.name.lower()]

        # Convert DSL to Python using event fields with datatypes (deterministic)
        python_code = dsl_to_python(request.dsl_code, event['fields'])

        # Save template (document stores DSL + latest python_code for convenience)
        template = DSLTemplate(name=request.name, dsl_code=request.dsl_code, python_code=python_code)
        doc = template.model_dump()
        doc['created_at'] = doc['created_at'].isoformat()
        try:
            await db.dsl_templates.insert_one(doc)
        except Exception:
            # DB unavailable - store in-memory
            global USE_IN_MEMORY
            USE_IN_MEMORY = True
            in_memory_data.setdefault('templates', []).append(doc)

        # Persist Python artifact in dedicated collection for external execution
        try:
            # Determine next version
            keep_versions = os.environ.get('KEEP_TEMPLATE_ARTIFACT_VERSIONS', 'false').lower() in ('1','true','yes')
            existing_art = await db.dsl_template_artifacts.find_one({"template_id": template.id}, {"_id": 0, "version": 1})
            next_version = 1
            if existing_art and isinstance(existing_art.get('version'), int):
                next_version = existing_art['version'] + 1

            artifact_doc = {
                "template_id": template.id,
                "template_name": request.name,
                "version": next_version,
                "python_code": python_code,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "read_only": True
            }
            # Insert new artifact
            await db.dsl_template_artifacts.insert_one(artifact_doc)

            # If not keeping history, delete older artifacts for this template
            if not keep_versions:
                await db.dsl_template_artifacts.delete_many({"template_id": template.id, "version": {"$lt": next_version}})
        except Exception as e:
            # DB unavailable - persist artifact in-memory
            logger.warning(f"Could not persist template artifact for {template.id}: {str(e)} - saving in memory")
            # USE_IN_MEMORY already declared global above in this function
            USE_IN_MEMORY = True
            artifact_doc = {
                "template_id": template.id,
                "template_name": request.name,
                "version": 1,
                "python_code": python_code,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "read_only": True
            }
            in_memory_data.setdefault('template_artifacts', []).append(artifact_doc)

        return {"message": "Template saved successfully", "template_id": template.id, "replaced": existing is not None}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error saving template: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@api_router.get("/templates/check-name/{name}")
async def check_template_name(name: str):
    """Check if a template name already exists"""
    existing = await db.dsl_templates.find_one({"name": name}, {"_id": 0})
    return {"exists": existing is not None}

@api_router.get("/templates")
async def get_templates():
    """Get all saved templates"""
    try:
        templates = await db.dsl_templates.find({}, {"_id": 0}).to_list(1000)
        if templates:
            # Sanitize for JSON serialization
            return [sanitize_for_json(t) for t in templates]
    except Exception as e:
        logger.warning(f"Could not load templates from database: {str(e)}")
    
    # Return sample data if MongoDB is unavailable
    logger.info("Returning sample templates")
    return [sanitize_for_json(t) for t in SAMPLE_TEMPLATES]

@api_router.delete("/templates/{template_id}")
async def delete_template(template_id: str):
    """Delete a template (robust: tries id, name, ObjectId, in-memory, and sample list)"""
    try:
        # 1) Try deleting by id (DB may be unavailable)
        try:
            result = await db.dsl_templates.delete_one({"id": template_id})
            logger.info(f"delete_one by id result: {getattr(result, 'deleted_count', 'n/a')}")
            if getattr(result, 'deleted_count', 0) == 1:
                logger.info(f"Deleted template by id: {template_id}")
                # Also remove persisted artifacts
                try:
                    await db.dsl_template_artifacts.delete_many({"template_id": template_id})
                except Exception:
                    logger.debug(f"Failed to delete artifacts for template {template_id}")
                return {"message": "Template deleted successfully"}
        except Exception as e:
            logger.debug(f"DB delete by id failed, will try other strategies: {e}")

        # 2) Try deleting by name (in case caller passed a human-readable name)
        try:
            result_by_name = await db.dsl_templates.delete_one({"name": template_id})
            logger.info(f"delete_one by name result: {getattr(result_by_name, 'deleted_count', 'n/a')}")
            if getattr(result_by_name, 'deleted_count', 0) == 1:
                logger.info(f"Deleted template by name: {template_id}")
                # Also remove persisted artifacts by template_name
                try:
                    await db.dsl_template_artifacts.delete_many({"template_name": template_id})
                except Exception:
                    logger.debug(f"Failed to delete artifacts for template name {template_id}")
                return {"message": "Template deleted successfully (by name)"}
        except Exception as e:
            logger.debug(f"DB delete by name failed, will try other strategies: {e}")

        # 3) Try deleting by _id if an ObjectId string was provided
        try:
            from bson import ObjectId
            obj_id = ObjectId(template_id)
            try:
                result_by_obj = await db.dsl_templates.delete_one({"_id": obj_id})
                logger.info(f"delete_one by _id result: {getattr(result_by_obj, 'deleted_count', 'n/a')}")
                if getattr(result_by_obj, 'deleted_count', 0) == 1:
                    logger.info(f"Deleted template by _id: {template_id}")
                    try:
                        await db.dsl_template_artifacts.delete_many({"template_id": template_id})
                    except Exception:
                        logger.debug(f"Failed to delete artifacts for template _id {template_id}")
                    return {"message": "Template deleted successfully (by _id)"}
            except Exception as e:
                logger.debug(f"DB delete by _id failed, will try other strategies: {e}")
        except Exception as e:
            logger.debug(f"Not an ObjectId or failed delete by _id: {str(e)}")

        # 4) If not found in DB, try in-memory storage (useful for local/dev mode)
        if USE_IN_MEMORY:
            before = len(in_memory_data.get("templates", []))
            in_memory_data["templates"] = [t for t in in_memory_data.get("templates", []) if t.get("id") != template_id and t.get("name") != template_id]
            after = len(in_memory_data.get("templates", []))
            if after < before:
                logger.info(f"Deleted template {template_id} from in-memory storage")
                # Also remove in-memory artifacts for this template
                in_memory_data['template_artifacts'] = [a for a in in_memory_data.get('template_artifacts', []) if a.get('template_id') != template_id and a.get('template_name') != template_id]
                return {"message": "Template deleted successfully (in-memory)"}

        # 5) As a last resort, remove from SAMPLE_TEMPLATES
        global SAMPLE_TEMPLATES
        sample_before = len(SAMPLE_TEMPLATES)
        SAMPLE_TEMPLATES = [t for t in SAMPLE_TEMPLATES if t.get("id") != template_id and t.get("name") != template_id]
        if len(SAMPLE_TEMPLATES) < sample_before:
            logger.info(f"Deleted template {template_id} from SAMPLE_TEMPLATES")
            return {"message": "Template deleted successfully (sample data)"}

        # Nothing deleted
        raise HTTPException(status_code=404, detail="Template not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting template {template_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.post("/templates/deploy")
async def deploy_template(request: TemplateDeployRequest):
    """Mark a template as deployed."""
    try:
        result = await db.dsl_templates.update_one(
            {"id": request.template_id},
            {"$set": {"deployed": True, "deployed_at": datetime.now(timezone.utc).isoformat()}}
        )
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Template not found")
        return {"success": True, "message": f"Template {request.template_id} deployed successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deploying template {request.template_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.post("/templates/execute")
async def execute_template(request: TemplateExecuteRequest):
    """Execute a saved template on event data - supports multiple events"""
    try:
        # Get template (DB first, then in-memory/sample)
        template = None
        try:
            template = await db.dsl_templates.find_one({"id": request.template_id}, {"_id": 0})
        except Exception:
            logger.debug("DB unavailable when fetching template for execution; checking in-memory and sample templates")

        if not template:
            # Check in-memory templates
            for t in in_memory_data.get('templates', []):
                if t.get('id') == request.template_id or t.get('name') == request.template_id:
                    template = t
                    break

        if not template:
            # Check sample templates
            for t in SAMPLE_TEMPLATES:
                if t.get('id') == request.template_id or t.get('name') == request.template_id:
                    template = t
                    break

        if not template:
            raise HTTPException(status_code=404, detail="Template not found")
        
        dsl_code = template['dsl_code']
        
        # Extract all event names referenced in the DSL code
        referenced_events = extract_event_names_from_dsl(dsl_code)
        logger.info(f"Referenced events in DSL: {referenced_events}")
        
        # If no events found in DSL (old format), use the selected event
        if not referenced_events:
            referenced_events = [request.event_name]
        
        # Load event definitions and data for all referenced events
        all_event_fields = {}
        event_data_dict = {}
        
        for event_name in referenced_events:
            # Get event definition
            event_def = await db.event_definitions.find_one(
                {"event_name": {"$regex": f"^{event_name}$", "$options": "i"}}, 
                {"_id": 0}
            )
            if not event_def:
                raise HTTPException(status_code=404, detail=f"Event definition '{event_name}' not found")
            
            all_event_fields[event_def['event_name']] = event_def['fields']
            
            # Get event data
            event_data = await db.event_data.find_one(
                {"event_name": {"$regex": f"^{event_name}$", "$options": "i"}}, 
                {"_id": 0}
            )
            if event_data and event_data.get('data_rows'):
                event_data_dict[event_def['event_name']] = event_data['data_rows']
            else:
                logger.warning(f"No data found for event '{event_name}'")
                event_data_dict[event_def['event_name']] = []
        
        # Merge data from all events by instrumentid
        # Filter by requested posting date before merging (batch date-scoped runs)
        scoped_event_data = (
            filter_event_data_by_posting_date(event_data_dict, request.posting_date)
            if request.posting_date
            else event_data_dict
        )
        merged_data = merge_event_data_by_instrument(scoped_event_data)
        
        if not merged_data:
            raise HTTPException(status_code=404, detail="No data found for the referenced events")
        
        logger.info(f"Merged data: {len(merged_data)} rows from {len(referenced_events)} events")
        
        # Generate Python code for multi-event template
        python_code = dsl_to_python_multi_event(dsl_code, all_event_fields)
        
        # Execute template with optional date overrides
        execution_result = await execute_python_template(
            python_code, 
            merged_data,
            event_data_dict,  # Pass raw event data for collect() functions
            request.posting_date,
            request.effective_date
        )
        
        transactions = execution_result["transactions"]
        print_outputs = execution_result["print_outputs"]
        
        # Save transaction report (DB or in-memory) — append each batch
        transaction_dicts = [t.model_dump() for t in transactions]
        report = TransactionReport(
            template_name=template.get('name', ''),
            event_name=', '.join(referenced_events),
            transactions=transaction_dicts
        )
        doc = report.model_dump()
        doc['executed_at'] = doc['executed_at'].isoformat()
        try:
            # Append new batch — each execution gets its own document
            await db.transaction_reports.insert_one(doc)
        except Exception:
            # Fallback to in-memory storage
            global USE_IN_MEMORY  # noqa: F811
            USE_IN_MEMORY = True
            lst = in_memory_data.setdefault('transaction_reports', [])
            lst.append(doc)
            in_memory_data['transaction_reports'] = lst
        
        return {
            "message": "Template executed successfully",
            "report_id": report.id,
            "transactions": transaction_dicts,
            "events_used": referenced_events,
            "print_outputs": print_outputs
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error executing template: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))


@api_router.delete("/transaction-reports/all")
async def delete_all_transaction_reports():
    """Wipe all transaction reports"""
    try:
        result = await db.transaction_reports.delete_many({})
        return {"message": f"Deleted {result.deleted_count} report(s)"}
    except Exception:
        in_memory_data['transaction_reports'] = []
        return {"message": "Cleared in-memory reports"}

@api_router.delete("/transaction-reports/{report_id}")
async def delete_transaction_report(report_id: str):
    """Delete all batches for a transaction report"""
    anchor = await db.transaction_reports.find_one({"id": report_id}, {"_id": 0})
    if not anchor:
        raise HTTPException(status_code=404, detail="Report not found")
    # Delete all batches belonging to the same template
    template_name = anchor.get('template_name', '')
    result = await db.transaction_reports.delete_many({"template_name": template_name})
    return {"message": f"Deleted {result.deleted_count} batch(es) for '{template_name}'"}


@api_router.post("/ai/provider/test")
async def test_ai_provider(req: AIProviderTestRequest):
    """Validate an API key and return available models."""
    if req.provider not in PROVIDER_INFO:
        raise HTTPException(status_code=400, detail=f"Unknown provider: {req.provider}")
    try:
        provider = get_provider(req.provider)
        models = await provider.list_models(req.api_key)
        return {
            "valid": True,
            "models": [m.model_dump() for m in models],
        }
    except AIError as e:
        logger.warning(f"AI provider test failed ({req.provider}): {e.error_type} - {e.detail}")
        return {
            "valid": False,
            "error_type": e.error_type,
            "error_message": e.detail,
            "models": [],
        }
    except Exception as e:
        logger.exception(f"Unexpected error testing AI provider ({req.provider})")
        return {
            "valid": False,
            "error_type": "network",
            "error_message": str(e),
            "models": [],
        }

@api_router.post("/ai/provider/save")
async def save_ai_provider(req: AIProviderSaveRequest):
    """Persist the selected provider, encrypted API key, and models."""
    encrypted = encrypt_key(req.api_key)
    doc = {
        "provider": req.provider,
        "encrypted_api_key": encrypted,
        "selected_model": req.selected_model,
        "available_models": req.available_models,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    await db.ai_provider_config.delete_many({})
    await db.ai_provider_config.insert_one(doc)
    return {"success": True}

@api_router.get("/ai/provider/status")
async def get_ai_provider_status():
    """Get the current provider config with dynamically refreshed model list."""
    try:
        config = await db.ai_provider_config.find_one({}, {"_id": 0})
    except Exception:
        config = None
    if not config:
        return {"configured": False}

    provider_name = config.get("provider")
    cached_models = config.get("available_models", [])

    # Dynamically refresh models from the provider API
    fresh_models = cached_models
    try:
        api_key = decrypt_key(config["encrypted_api_key"])
        provider = get_provider(provider_name)
        models = await provider.list_models(api_key)
        fresh_models = [m.model_dump() for m in models]
        # Update DB cache in the background
        await db.ai_provider_config.update_one(
            {"provider": provider_name},
            {"$set": {"available_models": fresh_models}},
        )
    except Exception as exc:
        logger.warning(f"Failed to refresh model list for {provider_name}: {exc}")
        # Fall back to cached models

    return {
        "configured": True,
        "provider": provider_name,
        "selected_model": config.get("selected_model"),
        "available_models": fresh_models,
    }

@api_router.delete("/ai/provider")
async def delete_ai_provider():
    """Remove the saved provider configuration."""
    await db.ai_provider_config.delete_many({})
    return {"success": True}

# AI Chat Assistant
@api_router.post("/chat", response_model=ChatResponse)
async def chat_with_assistant(message: ChatMessage):
    """Chat with AI assistant for DSL help - with full context awareness"""
    try:
        session_id = message.session_id or str(uuid.uuid4())

        # --- Load provider config ---
        try:
            provider_config = await db.ai_provider_config.find_one({}, {"_id": 0})
        except Exception:
            provider_config = None

        if not provider_config:
            return ChatResponse(
                response="",
                session_id=session_id,
                error_type="no_provider",
                error_message=ERROR_MESSAGES["no_provider"],
            )

        provider_name = provider_config.get("provider", "")
        selected_model = message.model or provider_config.get("selected_model", "")
        provider_display = PROVIDER_INFO.get(provider_name, {}).get("name", provider_name)

        # Decrypt key
        try:
            api_key = decrypt_key(provider_config["encrypted_api_key"])
        except Exception as e:
            logger.warning(f"Failed to decrypt API key: {e}")
            return ChatResponse(
                response="",
                session_id=session_id,
                error_type="invalid_key",
                error_message=ERROR_MESSAGES["invalid_key"],
            )

        # --- Gather context data ---
        if message.context and message.context.get('events'):
            events = message.context['events']
        else:
            try:
                events = await db.event_definitions.find({}, {"_id": 0}).to_list(1000)
            except Exception:
                events = in_memory_data.get('event_definitions', SAMPLE_EVENTS)

        editor_code = ""
        if message.context and message.context.get('editor_code'):
            editor_code = message.context['editor_code']

        console_output = []
        if message.context and message.context.get('console_output'):
            console_output = message.context['console_output']

        # Rich editor context (cursor, selection, syntax errors)
        editor_cursor = message.context.get('editor_cursor') if message.context else None
        editor_selection = message.context.get('editor_selection') if message.context else None
        editor_syntax_errors = message.context.get('editor_syntax_errors') if message.context else None

        # --- Build system prompt via two-tier context engine ---
        system_prompt = build_agent_context(
            dsl_function_metadata=list(DSL_FUNCTION_METADATA),
            events=events,
            editor_code=editor_code,
            editor_cursor=editor_cursor,
            editor_selection=editor_selection,
            editor_syntax_errors=editor_syntax_errors,
            console_output=console_output,
            conversation_history=message.history,
        )

        # --- Call the AI provider ---
        try:
            provider = get_provider(provider_name)
            ai_response = await provider.chat(
                api_key=api_key,
                model_id=selected_model,
                system_prompt=system_prompt,
                user_message=message.message,
                history=message.history,
            )
            response_text = ai_response.text
        except AIError as e:
            err_msg = ERROR_MESSAGES.get(e.error_type, e.detail)
            err_msg = err_msg.replace("{provider}", provider_display).replace("{model}", selected_model)
            return ChatResponse(
                response="",
                session_id=session_id,
                error_type=e.error_type,
                error_message=err_msg,
            )

        # --- Try to parse structured JSON response ---
        structured = None
        try:
            import re as _re
            json_match = _re.search(r'```json\s*(\{.*?\})\s*```', response_text, _re.S)
            if json_match:
                parsed = json.loads(json_match.group(1))
                if "dsl_code" in parsed and "explanation" in parsed:
                    structured = {
                        "explanation": parsed.get("explanation", ""),
                        "dsl_code": parsed.get("dsl_code", ""),
                        "insert_mode": parsed.get("insert_mode", "append"),
                        "confidence": parsed.get("confidence", "medium"),
                    }
        except Exception:
            pass

        # --- Post-process response to enforce DSL rules ---
        try:
            import re

            user_msg_lower = (message.message or '').lower()
            user_requested_transactions = any(k in user_msg_lower for k in [
                'createtransaction', 'create transaction', 'createtransactions', 'create transactions', 'include transaction', 'emit transaction', 'include createtransaction'
            ])

            def replace_leading_comments(text: str) -> str:
                return re.sub(r'(^|\n)\s*//', r"\1##", text)

            def process_code_block(code: str) -> str:
                code = replace_leading_comments(code)

                allowed_funcs = set()
                try:
                    allowed_funcs.update(DSL_FUNCTIONS.keys())
                except Exception:
                    pass
                try:
                    for m in DSL_FUNCTION_METADATA:
                        name = m.get('name') if isinstance(m, dict) else None
                        if name:
                            allowed_funcs.add(name)
                except Exception:
                    pass
                extra_allowed = {'print', 'iif', 'collect', 'collect_by_instrument', 'collect_all', 'collect_by_subinstrument', 'collect_subinstrumentids', 'collect_effectivedates_for_subinstrument', 'npv', 'irr', 'sum_field', 'sum', 'len', 'min', 'max', 'abs', 'round', 'lag'}
                allowed_funcs.update(extra_allowed)

                lines = code.splitlines()
                cleaned_lines = []
                for ln in lines:
                    if re.match(r"^\s*(def|class|import|from)\b", ln):
                        cleaned_lines.append('## removed unsupported Python construct')
                        continue
                    if re.match(r"^\s*(for|while)\b.*:\s*$", ln):
                        cleaned_lines.append('## removed unsupported Python loop')
                        continue
                    cleaned_lines.append(ln)
                code = "\n".join(cleaned_lines)

                has_create = re.search(r"\bcreateTransactions?\s*\(", code)
                if has_create and not user_requested_transactions:
                    lines = code.splitlines()
                    lines = [ln for ln in lines if not re.search(r"\bcreateTransactions?\s*\(", ln)]
                    code = "\n".join(lines)
                    assigns = re.findall(r'^\s*([a-z_][a-zA-Z0-9_]*)\s*=.*$', code, flags=re.MULTILINE)
                    if assigns:
                        last_var = assigns[-1]
                        if not re.search(r'print\s*\(\s*' + re.escape(last_var) + r'\s*\)', code):
                            code = code.rstrip() + '\n\nprint(' + last_var + ')'
                    try:
                        code = re.sub(r'(?mi)^\s*##.*create.*transaction.*$', '## Executing the final value', code, flags=re.M)
                    except Exception:
                        pass

                alias_map = {'periods': 'nper', 'num_periods': 'nper', 'number_of_periods': 'nper'}
                for a, b in alias_map.items():
                    code = re.sub(rf"\b{a}\s*\(", f"{b}(", code)

                non_comment_lines = [ln for ln in code.splitlines() if not ln.strip().startswith('##')]
                func_calls = []
                for ln in non_comment_lines:
                    func_calls.extend(re.findall(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s*\(", ln))
                if func_calls:
                    lines = code.splitlines()
                    new_lines = []
                    for ln in lines:
                        if ln.strip().startswith('##'):
                            new_lines.append(ln)
                            continue
                        code_only = ln.split('##')[0].split('//')[0]
                        called = re.findall(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s*\(", code_only)
                        if called:
                            illegal = [f for f in called if f not in allowed_funcs]
                            if illegal:
                                new_lines.append('## removed call to unsupported function: ' + ','.join(illegal))
                                continue
                        new_lines.append(ln)
                    code = "\n".join(new_lines)

                return code

            def process_response(text: str) -> str:
                def repl(match):
                    inner = match.group(1)
                    processed = process_code_block(inner)
                    return '```dsl\n' + processed + '\n```'

                text = re.sub(r'```(?:dsl)?\n(.*?)\n```', repl, text, flags=re.S)
                text = replace_leading_comments(text)

                if not user_requested_transactions and re.search(r"\bcreateTransactions?\s*\(", text):
                    lines = text.splitlines()
                    new_lines = []
                    for ln in lines:
                        if re.search(r"\bcreateTransactions?\s*\(", ln):
                            continue
                        new_lines.append(ln)
                    text = "\n".join(new_lines)
                    assigns = re.findall(r'^\s*([a-z_][a-zA-Z0-9_]*)\s*=.*$', text, flags=re.MULTILINE)
                    if assigns:
                        last_var = assigns[-1]
                        if not re.search(r'print\s*\(\s*' + re.escape(last_var) + r'\s*\)', text):
                            text = text.rstrip() + '\n\nprint(' + last_var + ')'

                return text

            response_text = process_response(response_text)

            # Also post-process structured dsl_code if present
            if structured and structured.get("dsl_code"):
                structured["dsl_code"] = process_code_block(structured["dsl_code"])

        except Exception as e:
            logger.warning(f"Post-processing of AI response failed: {e}")

        return ChatResponse(
            response=response_text,
            session_id=session_id,
            structured=structured,
        )
    except Exception as e:
        logger.error(f"Chat error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Chat error: {str(e)}")


@api_router.post("/chat/stream")
async def chat_stream(message: ChatMessage):
    """SSE streaming chat endpoint — sends tokens as they arrive from the provider."""

    session_id = message.session_id or str(uuid.uuid4())

    async def event_stream():
        try:
            # Load provider config
            try:
                provider_config = await db.ai_provider_config.find_one({}, {"_id": 0})
            except Exception:
                provider_config = None

            if not provider_config:
                yield f"data: {json.dumps({'type': 'error', 'error_type': 'no_provider', 'error_message': ERROR_MESSAGES['no_provider']})}\n\n"
                yield "data: [DONE]\n\n"
                return

            provider_name = provider_config.get("provider", "")
            selected_model = message.model or provider_config.get("selected_model", "")
            provider_display = PROVIDER_INFO.get(provider_name, {}).get("name", provider_name)

            # Decrypt key
            try:
                api_key = decrypt_key(provider_config["encrypted_api_key"])
            except Exception:
                yield f"data: {json.dumps({'type': 'error', 'error_type': 'invalid_key', 'error_message': ERROR_MESSAGES['invalid_key']})}\n\n"
                yield "data: [DONE]\n\n"
                return

            # Emit session info
            yield f"data: {json.dumps({'type': 'session', 'session_id': session_id})}\n\n"

            # Gather context
            if message.context and message.context.get('events'):
                events = message.context['events']
            else:
                try:
                    events = await db.event_definitions.find({}, {"_id": 0}).to_list(1000)
                except Exception:
                    events = in_memory_data.get('event_definitions', SAMPLE_EVENTS)

            editor_code = ""
            if message.context and message.context.get('editor_code'):
                editor_code = message.context['editor_code']

            console_output = []
            if message.context and message.context.get('console_output'):
                console_output = message.context['console_output']

            # Rich editor context
            editor_cursor = message.context.get('editor_cursor') if message.context else None
            editor_selection = message.context.get('editor_selection') if message.context else None
            editor_syntax_errors = message.context.get('editor_syntax_errors') if message.context else None

            # Build system prompt via two-tier context engine
            system_prompt = build_agent_context(
                dsl_function_metadata=list(DSL_FUNCTION_METADATA),
                events=events,
                editor_code=editor_code,
                editor_cursor=editor_cursor,
                editor_selection=editor_selection,
                editor_syntax_errors=editor_syntax_errors,
                console_output=console_output,
                conversation_history=message.history,
            )

            # Emit context-ready event with summary for the UI
            events_count = len(events) if events else 0
            editor_lines = len(editor_code.split('\n')) if editor_code.strip() else 0
            console_count = len(console_output) if console_output else 0
            yield f"data: {json.dumps({'type': 'context_ready', 'events_count': events_count, 'editor_lines': editor_lines, 'console_count': console_count, 'model': selected_model, 'provider': provider_display})}\n\n"

            # Stream from provider
            provider = get_provider(provider_name)
            full_text = []
            try:
                async for chunk in provider.stream_chat(
                    api_key=api_key,
                    model_id=selected_model,
                    system_prompt=system_prompt,
                    user_message=message.message,
                    history=message.history,
                ):
                    full_text.append(chunk)
                    yield f"data: {json.dumps({'type': 'token', 'token': chunk})}\n\n"
            except AIError as e:
                err_msg = ERROR_MESSAGES.get(e.error_type, e.detail)
                err_msg = err_msg.replace("{provider}", provider_display).replace("{model}", selected_model)
                yield f"data: {json.dumps({'type': 'error', 'error_type': e.error_type, 'error_message': err_msg})}\n\n"
                yield "data: [DONE]\n\n"
                return

            # Post-process the full response (same rules as /chat)
            full_response = ''.join(full_text)
            try:
                import re as _pp_re
                user_msg_lower = (message.message or '').lower()
                user_requested_txn = any(k in user_msg_lower for k in [
                    'createtransaction', 'create transaction', 'createtransactions',
                    'create transactions', 'include transaction', 'emit transaction',
                ])

                def _pp_replace_comments(text):
                    return _pp_re.sub(r'(^|\n)\s*//', r'\1##', text)

                if not user_requested_txn and _pp_re.search(r'\bcreateTransactions?\s*\(', full_response):
                    # AI included transactions the user didn't ask for — flag it
                    yield f"data: {json.dumps({'type': 'post_process', 'warning': 'unrequested_transactions'})}\n\n"
            except Exception:
                pass

            # Send done event
            yield f"data: {json.dumps({'type': 'done'})}\n\n"
            yield "data: [DONE]\n\n"

        except Exception as e:
            logger.error(f"Stream chat error: {str(e)}")
            yield f"data: {json.dumps({'type': 'error', 'error_type': 'network', 'error_message': str(e)})}\n\n"
            yield "data: [DONE]\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


REQUIRED_EVENT_FIELDS = {"instrumentId", "eventId", "eventName", "postingDate", "effectiveDate", "status", "eventDetail", "_class"}

# ---------------------------------------------------------------------------
# Import transformation helpers
# ---------------------------------------------------------------------------
# Fixed/system fields — both PascalCase (EOD-style) and camelCase (inner-row style)
# must be excluded from dynamic columns and handled explicitly.
_IMPORT_FIXED_KEYS = {
    "PostingDate", "EffectiveDate", "InstrumentId", "AttributeId",
    "postingDate", "effectiveDate", "instrumentId", "attributeId",
    "_id", "_metadata_version", "_imported_at",
}


def _infer_field_datatype(values: list) -> str:
    """Infer the best datatype for a field from a list of sample values.
    Scans all non-null values; the first conclusive type wins (boolean > date > string > decimal).
    Numeric strings (e.g. "1250.50", "42") are treated as decimal.
    """
    for v in values:
        if v is None:
            continue
        if isinstance(v, bool):
            return "boolean"
        if isinstance(v, dict) and "$date" in v:
            return "date"
        if isinstance(v, str):
            if re.match(r"^\d{4}-\d{2}-\d{2}", v):
                return "date"
            # Check if the string is a numeric value (handles "1250.50", "-42", "1,234.56")
            stripped = v.strip().lstrip('-').replace(',', '')
            if stripped.replace('.', '', 1).isdigit():
                return "decimal"
            return "string"
        if isinstance(v, (int, float)):
            return "decimal"
    return "decimal"  # all-null or empty → decimal (financial default)


def _parse_import_date(val) -> str:
    """Normalise a date value from an imported event record to YYYY-MM-DD."""
    if val is None:
        return ""
    if isinstance(val, dict) and "$date" in val:
        return str(val["$date"])[:10]
    if isinstance(val, int):
        s = str(val)
        if len(s) == 8:
            return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
        return s
    try:
        return normalize_date(str(val))
    except Exception:
        return str(val)


# Sentinel for "system" / placeholder instrument IDs with no real business meaning
def _is_custom_event(records: list, event_id: str) -> bool:
    """
    Return True if the event has no real InstrumentId.

    Rule (no hardcoding of names or placeholder strings):
      An event is 'standard' only when at least one inner value row contains
      an instrumentId that matches the outer instrumentId of the same event
      record — i.e. the inner rows are genuinely tied to a specific instrument.
      If no inner row matches its enclosing record's outer instrumentId, the
      event carries no instrument-specific data and is classified as
      eventTable='custom', eventType='reference'.
    """
    for event in records:
        if event.get("eventId") != event_id:
            continue
        outer = (event.get("instrumentId") or event.get("InstrumentId") or "").strip()
        if not outer:
            continue  # outer has no instrumentId — skip this record
        for row_val in event.get("eventDetail", {}).get("values", {}).values():
            if not isinstance(row_val, dict):
                continue
            inner = (row_val.get("instrumentId") or row_val.get("InstrumentId") or "").strip()
            if inner and inner == outer:
                return False  # inner instrumentId matches outer → real instrument event
    return True  # no inner row matched its enclosing event's instrumentId → custom


def _select_instruments(records: list, max_count: int = 2) -> list:
    """
    Collect all unique instrument IDs that appear in standard (non-custom) event records
    and randomly return up to max_count of them.
    Custom/reference events are excluded — they carry no real instrument.
    Returns a sorted (deterministic within selection) list of instrument IDs.
    """
    import random
    seen: set = set()
    for event in records:
        instrument = (event.get("instrumentId") or event.get("InstrumentId") or "").strip()
        if not instrument:
            continue
        event_id = event.get("eventId", "")
        # Only count instruments from standard (non-custom) events
        if not _is_custom_event(records, event_id):
            seen.add(instrument)
    population = sorted(seen)  # sort for reproducibility of the pool
    count = min(max_count, len(population))
    if count == 0:
        return []
    return random.sample(population, count)


def _build_event_definitions_from_import(records: list, allowed_instruments: set | None = None) -> list:
    """
    Derive unique event definitions from the imported records.
    Groups by eventId, collects dynamic field names and infers datatypes.
    Excludes all fixed/system fields (both PascalCase and camelCase) and meta keys.
    Events with no real instrumentId are classified as custom/reference.
    If allowed_instruments is given, standard event records whose outer instrumentId
    is not in that set are skipped (custom/reference events are never filtered).
    Returns a list of dicts compatible with EventDefinition.model_dump().
    """
    from collections import defaultdict
    event_fields: dict = defaultdict(lambda: defaultdict(list))

    for event in records:
        event_id = event.get("eventId", "")
        outer_instrument = (event.get("instrumentId") or event.get("InstrumentId") or "").strip()
        is_custom = _is_custom_event(records, event_id)
        # Filter standard events by allowed instrument list
        if not is_custom and allowed_instruments is not None and outer_instrument not in allowed_instruments:
            continue
        for row_val in event.get("eventDetail", {}).get("values", {}).values():
            if not isinstance(row_val, dict):
                continue
            for key, value in row_val.items():
                if key not in _IMPORT_FIXED_KEYS:
                    event_fields[event_id][key].append(value)

    definitions = []
    ts = datetime.now(timezone.utc).isoformat()
    for event_id, fields in event_fields.items():
        field_list = [
            {"name": fn, "datatype": _infer_field_datatype(sv)}
            for fn, sv in fields.items()
        ]
        is_custom = _is_custom_event(records, event_id)
        definitions.append({
            "id": str(uuid.uuid4()),
            "event_name": event_id,
            "fields": field_list,
            "eventType": "reference" if is_custom else "activity",
            "eventTable": "custom" if is_custom else "standard",
            "created_at": ts,
        })
    return definitions


def _build_event_data_from_import(records: list, allowed_instruments: set | None = None) -> list:
    """
    Build event data rows from imported records.
    Groups rows by eventId. Each value entry in eventDetail.values becomes one data row.
    Handles both PascalCase (EOD) and camelCase inner row keys.
    Maps attributeId (either case) to SubInstrumentId — for standard events only.
    Custom/reference events (no instrumentId in inner rows) omit all standard fields.
    Normalises all date values to YYYY-MM-DD.
    If allowed_instruments is given, standard event records whose outer instrumentId
    is not in that set are skipped. Custom/reference events are never filtered.
    """
    from collections import defaultdict

    # Pre-classify each event_id so we only scan once
    event_ids = list({evt.get("eventId", "") for evt in records})
    custom_events = {eid for eid in event_ids if _is_custom_event(records, eid)}

    event_rows: dict = defaultdict(list)
    # For custom/reference events, dedup by inner value-id so repeated event records
    # (same data across different posting dates / instruments) don't multiply rows.
    seen_custom_value_ids: dict = defaultdict(set)

    for event in records:
        event_id = event.get("eventId", "")
        is_custom = event_id in custom_events

        # Outer event-level fallbacks (always camelCase at top level)
        outer_posting = _parse_import_date(event.get("postingDate") or event.get("PostingDate", ""))
        outer_effective = _parse_import_date(event.get("effectiveDate") or event.get("EffectiveDate", ""))
        outer_instrument = (event.get("instrumentId") or event.get("InstrumentId", "")).strip()

        # Filter standard events by allowed instrument list
        if not is_custom and allowed_instruments is not None and outer_instrument not in allowed_instruments:
            continue

        raw_values = event.get("eventDetail", {}).get("values", {})
        for value_id, row_val in raw_values.items():
            # Dedup: skip this inner row if we've already emitted it for this custom event
            if is_custom:
                if value_id in seen_custom_value_ids[event_id]:
                    continue
                seen_custom_value_ids[event_id].add(value_id)

            if not isinstance(row_val, dict):
                continue

            if is_custom:
                # Custom/reference events: no standard fields at all
                row: dict = {}
            else:
                # Standard events: include the four standard fields
                inner_posting = _parse_import_date(
                    row_val.get("PostingDate") or row_val.get("postingDate")
                ) or outer_posting
                inner_effective = _parse_import_date(
                    row_val.get("EffectiveDate") or row_val.get("effectiveDate")
                ) or outer_effective
                inner_instrument = (
                    row_val.get("InstrumentId") or row_val.get("instrumentId") or outer_instrument
                )
                # attributeId (either case) maps to SubInstrumentId
                inner_subinstr = str(
                    row_val.get("AttributeId") or row_val.get("attributeId") or ""
                )
                row = {
                    "PostingDate":     inner_posting,
                    "EffectiveDate":   inner_effective,
                    "InstrumentId":    inner_instrument,
                    "SubInstrumentId": inner_subinstr,
                }

            for key, value in row_val.items():
                if key in _IMPORT_FIXED_KEYS:
                    continue
                if isinstance(value, dict) and "$date" in value:
                    row[key] = _parse_import_date(value)
                elif isinstance(value, dict) and "$oid" in value:
                    # Skip embedded object-id fields
                    continue
                else:
                    row[key] = value

            event_rows[event_id].append(row)

    ts = datetime.now(timezone.utc).isoformat()
    return [
        {"id": str(uuid.uuid4()), "event_name": eid, "data_rows": rows, "created_at": ts}
        for eid, rows in event_rows.items()
    ]

def _validate_imported_events(data: Any) -> str | None:
    """Return an error string if data does not match the required format, else None."""
    if not isinstance(data, list):
        return "File must contain a JSON array of event objects."
    if len(data) == 0:
        return "The JSON array is empty — no events to import."
    for i, item in enumerate(data):
        if not isinstance(item, dict):
            return f"Item at index {i} is not a JSON object."
        missing = REQUIRED_EVENT_FIELDS - item.keys()
        if missing:
            return f"Item at index {i} is missing required fields: {', '.join(sorted(missing))}."
        if not isinstance(item.get("eventDetail"), dict):
            return f"Item at index {i}: 'eventDetail' must be a JSON object."
        if "values" not in item["eventDetail"]:
            return f"Item at index {i}: 'eventDetail' must contain a 'values' field."
    return None


@api_router.post("/import-events/transform")
async def import_and_transform_events(file: UploadFile = File(...)):
    """
    Full import pipeline: validate JSON → persist raw → transform to
    event definitions + event data and persist both.
    Returns a structured result with success/failure details for each step.
    """
    # ---- Read and validate file ----
    if not file.filename or not file.filename.lower().endswith(".json"):
        raise HTTPException(status_code=400, detail="Only .json files are accepted.")
    try:
        content = await file.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not read file: {e}")

    try:
        records = json.loads(content.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="The file is not valid JSON. Please check the file and try again.")

    validation_error = _validate_imported_events(records)
    if validation_error:
        raise HTTPException(status_code=422, detail=validation_error)

    import_ts = datetime.now(timezone.utc).isoformat()
    # ---- Instrument selection (1–2 instruments for activity data) ----
    selected_instruments = _select_instruments(records, max_count=2)
    allowed_instruments_set = set(selected_instruments) if selected_instruments else None

    result = {
        "imported_count": len(records),
        "selected_instruments": selected_instruments,
        "event_definitions": None,
        "event_data": None,
    }

    # ---- Step 1: Persist raw to imported_events ----
    try:
        docs = []
        for item in records:
            doc = dict(item)
            doc["_imported_at"] = import_ts
            doc.pop("_id", None)
            docs.append(doc)
        await db.imported_events.insert_many(docs)
    except Exception as e:
        logger.warning(f"Could not persist raw events to imported_events: {e}")
        # Non-fatal — continue with transformation

    # ---- Step 2: Transform → Event Definitions (MUST complete before Event Data) ----
    def_error = None
    definitions = []
    try:
        definitions = _build_event_definitions_from_import(records, allowed_instruments=allowed_instruments_set)
        if not definitions:
            def_error = "No event types could be extracted from the uploaded file."
        else:
            await db.event_definitions.delete_many({})
            await db.event_definitions.insert_many([dict(d) for d in definitions])
            result["event_definitions"] = {
                "success": True,
                "count": len(definitions),
                "names": [d["event_name"] for d in definitions],
                "types": {d["event_name"]: d["eventTable"] for d in definitions},
            }
    except Exception as e:
        logger.error(f"Event definition transformation failed: {e}")
        def_error = f"Could not build event definitions: {e}"

    if def_error:
        result["event_definitions"] = {"success": False, "error": def_error}

    # ---- Step 3: Transform → Event Data (uses classification from Step 2) ----
    data_error = None
    try:
        event_data_list = _build_event_data_from_import(records, allowed_instruments=allowed_instruments_set)
        if not event_data_list:
            data_error = "No event data rows could be extracted from the uploaded file."
        else:
            await db.event_data.delete_many({})
            rows_by_event = {}
            for ed in event_data_list:
                await db.event_data.insert_one(dict(ed))
                rows_by_event[ed["event_name"]] = len(ed["data_rows"])
            result["event_data"] = {
                "success": True,
                "total_rows": sum(rows_by_event.values()),
                "by_event": rows_by_event,
            }
    except Exception as e:
        logger.error(f"Event data transformation failed: {e}")
        data_error = f"Could not build event data: {e}"

    if data_error:
        result["event_data"] = {"success": False, "error": data_error}

    # If BOTH transformations failed, surface as a 500
    if not result["event_definitions"]["success"] and not result["event_data"]["success"]:
        raise HTTPException(
            status_code=500,
            detail=(
                f"Both transformations failed. "
                f"Event definitions: {result['event_definitions']['error']}. "
                f"Event data: {result['event_data']['error']}."
            ),
        )

    return result


# ── Saved Rules CRUD ────────────────────────────────────────────────────

@api_router.get("/saved-rules")
async def list_saved_rules(summary: int = 0):
    """List all saved rule builder configurations.
    Pass ?summary=1 to exclude generatedCode (fast list for UI display).
    """
    try:
        projection = {"_id": 0}
        if summary:
            projection["generatedCode"] = 0
        rules = await db.saved_rules.find({}, projection).sort("updated_at", -1).to_list(500)
        return rules
    except Exception as e:
        logger.error(f"Error listing saved rules: {e}")
        return []

@api_router.post("/saved-rules")
async def save_rule(request: dict):
    """Save or update a rule builder configuration. Rule name must be unique."""
    name = (request.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Rule name is required.")

    rule_id = request.get("id")
    now = datetime.now(timezone.utc).isoformat()

    # Check uniqueness: no other rule with same name (case-insensitive)
    existing = await db.saved_rules.find_one(
        {"name": {"$regex": f"^{re.escape(name)}$", "$options": "i"}},
        {"_id": 0, "id": 1},
    )
    if existing and (not rule_id or existing["id"] != rule_id):
        raise HTTPException(
            status_code=409,
            detail=f"A rule named \"{name}\" already exists. Please choose a different name.",
        )

    # Priority uniqueness across rules AND schedules
    priority = request.get("priority")
    if priority is not None:
        priority = int(priority)
        # Check other rules
        rule_with_priority = await db.saved_rules.find_one(
            {"priority": priority, **({"id": {"$ne": rule_id}} if rule_id else {})},
            {"_id": 0, "id": 1, "name": 1},
        )
        if rule_with_priority:
            raise HTTPException(
                status_code=409,
                detail=f"Priority {priority} is already used by rule \"{rule_with_priority['name']}\". Please choose a different priority.",
            )
        # Check schedules collection
        sched_with_priority = await db.saved_schedules.find_one(
            {"priority": priority},
            {"_id": 0, "id": 1, "name": 1},
        )
        if sched_with_priority:
            raise HTTPException(
                status_code=409,
                detail=f"Priority {priority} is already used by schedule \"{sched_with_priority['name']}\". Please choose a different priority.",
            )

    doc = {
        "name": name,
        "priority": priority,
        "ruleType": request.get("ruleType", "simple_calc"),
        "variables": request.get("variables", []),
        "conditions": request.get("conditions", []),
        "elseFormula": request.get("elseFormula", ""),
        "conditionResultVar": request.get("conditionResultVar", "result"),
        "iterations": request.get("iterations", []),
        "iterConfig": request.get("iterConfig", {}),
        "outputs": request.get("outputs", {}),
        "inlineComment": request.get("inlineComment", False),
        "commentText": request.get("commentText", ""),
        "customCode": request.get("customCode", ""),
        "generatedCode": request.get("generatedCode", ""),
        "steps": request.get("steps", []),
        "updated_at": now,
    }

    if rule_id:
        doc["id"] = rule_id
        await db.saved_rules.replace_one({"id": rule_id}, doc, upsert=True)
    else:
        doc["id"] = str(uuid.uuid4())
        doc["created_at"] = now
        await db.saved_rules.insert_one(doc)

    return {"success": True, "id": doc["id"], "message": f"Rule \"{name}\" saved."}

# NOTE: Static routes (/saved-rules/reorder) MUST be declared BEFORE the
# parameterized routes (/saved-rules/{rule_id}) — otherwise FastAPI matches
# "reorder" as a {rule_id} path parameter.
@api_router.put("/saved-rules/reorder")
async def reorder_saved_rules(request: dict):
    """Batch-update priorities for saved rules based on drag-and-drop ordering.
    Expects: { "order": [ { "id": "...", "priority": 1 }, ... ] }
    """
    order = request.get("order", [])
    if not order:
        raise HTTPException(status_code=400, detail="No ordering provided.")
    try:
        for item in order:
            rule_id = item.get("id")
            priority = item.get("priority")
            if rule_id is not None and priority is not None:
                await db.saved_rules.update_one(
                    {"id": rule_id},
                    {"$set": {"priority": int(priority)}},
                )
        return {"success": True, "message": f"Updated priorities for {len(order)} rules."}
    except Exception as e:
        logger.error(f"Error reordering rules: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api_router.delete("/saved-rules/{rule_id}")
async def delete_saved_rule(rule_id: str):
    """Delete a saved rule by its id."""
    result = await db.saved_rules.delete_one({"id": rule_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Rule not found.")
    return {"success": True, "message": "Rule deleted."}

@api_router.put("/saved-rules/{rule_id}")
async def update_saved_rule(rule_id: str, request: dict):
    """Patch specific fields of a saved rule (generatedCode, outputs, steps, etc.)."""
    allowed = {"generatedCode", "outputs", "steps", "name", "priority", "variables",
               "conditions", "elseFormula", "conditionResultVar", "iterations",
               "iterConfig", "inlineComment", "commentText", "ruleType"}
    update_fields = {k: v for k, v in request.items() if k in allowed}
    if not update_fields:
        raise HTTPException(status_code=400, detail="No valid fields to update.")
    result = await db.saved_rules.update_one({"id": rule_id}, {"$set": update_fields})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Rule not found.")
    return {"success": True, "message": "Rule updated."}

@api_router.delete("/saved-rules")
async def delete_all_saved_rules():
    """Delete ALL saved rules."""
    result = await db.saved_rules.delete_many({})
    return {"success": True, "deleted": result.deleted_count, "message": f"Deleted {result.deleted_count} rule(s)."}


@api_router.put("/saved-schedules/reorder")
async def reorder_saved_schedules(request: dict):
    """Batch-update priorities for saved schedules based on drag-and-drop ordering.
    Expects: { "order": [ { "id": "...", "priority": 1 }, ... ] }
    """
    order = request.get("order", [])
    if not order:
        raise HTTPException(status_code=400, detail="No ordering provided.")
    try:
        for item in order:
            sched_id = item.get("id")
            priority = item.get("priority")
            if sched_id is not None and priority is not None:
                await db.saved_schedules.update_one(
                    {"id": sched_id},
                    {"$set": {"priority": int(priority)}},
                )
        return {"success": True, "message": f"Updated priorities for {len(order)} schedules."}
    except Exception as e:
        logger.error(f"Error reordering schedules: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── User Templates CRUD ─────────────────────────────────────────────────

@api_router.get("/user-templates")
async def list_user_templates():
    """List all user-created templates."""
    try:
        templates = await db.user_templates.find({}, {"_id": 0}).sort("created_at", -1).to_list(500)
        return templates
    except Exception as e:
        logger.error(f"Error listing user templates: {e}")
        return []

@api_router.post("/user-templates")
async def save_user_template(request: dict):
    """Create a user template from saved rules."""
    name = (request.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Template name is required.")
    description = (request.get("description") or "").strip()
    category = (request.get("category") or "User Created").strip()
    rules = request.get("rules", [])
    combined_code = request.get("combinedCode", "")

    # Check name uniqueness
    existing = await db.user_templates.find_one(
        {"name": {"$regex": f"^{re.escape(name)}$", "$options": "i"}},
        {"_id": 0, "id": 1},
    )
    if existing:
        raise HTTPException(status_code=409, detail=f"A template named \"{name}\" already exists.")

    now = datetime.now(timezone.utc).isoformat()
    doc = {
        "id": str(uuid.uuid4()),
        "name": name,
        "description": description,
        "category": category,
        "rules": rules,
        "combinedCode": combined_code,
        "created_at": now,
        "updated_at": now,
    }
    await db.user_templates.insert_one(doc)
    return {"success": True, "id": doc["id"], "message": f"Template \"{name}\" created."}

@api_router.delete("/user-templates/{template_id}")
async def delete_user_template(template_id: str):
    """Delete a user template by id."""
    result = await db.user_templates.delete_one({"id": template_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Template not found.")
    return {"success": True, "message": "Template deleted."}

@api_router.put("/user-templates/{template_id}")
async def update_user_template(template_id: str, request: dict):
    """Overwrite an existing user template's rules and code (keeps name/description/category)."""
    existing = await db.user_templates.find_one({"id": template_id}, {"_id": 0})
    if not existing:
        raise HTTPException(status_code=404, detail="Template not found.")
    now = datetime.now(timezone.utc).isoformat()
    update_fields = {"updated_at": now}
    if "rules" in request:
        update_fields["rules"] = request["rules"]
    if "combinedCode" in request:
        update_fields["combinedCode"] = request["combinedCode"]
    # Allow optional metadata updates
    if "description" in request:
        update_fields["description"] = request["description"]
    if "category" in request:
        update_fields["category"] = request["category"]
    await db.user_templates.update_one({"id": template_id}, {"$set": update_fields})
    return {"success": True, "id": template_id, "message": f"Template \"{existing['name']}\" updated."}

# ── Template Sample Data ────────────────────────────────────────────────

@api_router.post("/template-sample-data/{template_id}")
async def load_template_sample_data(template_id: str):
    """Load pre-defined sample event definitions and event data for a specific template."""
    import importlib, sys, os
    backend_dir = os.path.dirname(os.path.abspath(__file__))
    if backend_dir not in sys.path:
        sys.path.insert(0, backend_dir)
    from template_sample_data import TEMPLATE_SAMPLE_DATA

    if template_id not in TEMPLATE_SAMPLE_DATA:
        raise HTTPException(status_code=404, detail=f"No sample data available for template '{template_id}'")

    sample = TEMPLATE_SAMPLE_DATA[template_id]

    for evt in sample["events"]:
        existing = await db.event_definitions.find_one({"event_name": evt["event_name"]})
        if not existing:
            doc = {
                "id": str(uuid.uuid4()),
                "event_name": evt["event_name"],
                "fields": evt["fields"],
                "eventType": evt.get("eventType", "activity"),
                "eventTable": evt.get("eventTable", "standard"),
                "created_at": datetime.utcnow().isoformat(),
            }
            await db.event_definitions.insert_one(doc)

    for ed in sample["event_data"]:
        await db.event_data.delete_many({"event_name": ed["event_name"]})
        doc = {
            "event_name": ed["event_name"],
            "data_rows": ed["data_rows"],
            "created_at": datetime.utcnow().isoformat(),
        }
        await db.event_data.insert_one(doc)

    events = await db.event_definitions.find({}, {"_id": 0}).to_list(1000)
    return {"success": True, "events": events}

# ── Saved Schedules CRUD ────────────────────────────────────────────────

@api_router.get("/saved-schedules")
async def list_saved_schedules(summary: int = 0):
    """List all saved schedule builder configurations.
    Pass ?summary=1 to exclude generatedCode (fast list for UI display).
    """
    try:
        projection = {"_id": 0}
        if summary:
            projection["generatedCode"] = 0
        schedules = await db.saved_schedules.find({}, projection).sort("updated_at", -1).to_list(500)
        return schedules
    except Exception as e:
        logger.error(f"Error listing saved schedules: {e}")
        return []

@api_router.post("/saved-schedules")
async def save_schedule(request: dict):
    """Save or update a schedule builder configuration."""
    name = (request.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Schedule name is required.")

    schedule_id = request.get("id")
    now = datetime.now(timezone.utc).isoformat()

    # Check uniqueness: no other schedule with same name (case-insensitive)
    existing = await db.saved_schedules.find_one(
        {"name": {"$regex": f"^{re.escape(name)}$", "$options": "i"}},
        {"_id": 0, "id": 1},
    )
    if existing and (not schedule_id or existing["id"] != schedule_id):
        raise HTTPException(
            status_code=409,
            detail=f"A schedule named \"{name}\" already exists. Please choose a different name.",
        )

    # Priority uniqueness across rules AND schedules
    priority = request.get("priority")
    if priority is not None:
        priority = int(priority)
        # Check rules collection
        rule_with_priority = await db.saved_rules.find_one(
            {"priority": priority, **({"id": {"$ne": schedule_id}} if schedule_id else {})},
            {"_id": 0, "id": 1, "name": 1},
        )
        if rule_with_priority:
            raise HTTPException(
                status_code=409,
                detail=f"Priority {priority} is already used by rule \"{rule_with_priority['name']}\". Please choose a different priority.",
            )
        # Check schedules collection
        sched_with_priority = await db.saved_schedules.find_one(
            {"priority": priority, **({"id": {"$ne": schedule_id}} if schedule_id else {})},
            {"_id": 0, "id": 1, "name": 1},
        )
        if sched_with_priority:
            raise HTTPException(
                status_code=409,
                detail=f"Priority {priority} is already used by schedule \"{sched_with_priority['name']}\". Please choose a different priority.",
            )

    doc = {
        "name": name,
        "priority": priority,
        "generatedCode": request.get("generatedCode", ""),
        "config": request.get("config", {}),
        "updated_at": now,
    }

    if schedule_id:
        doc["id"] = schedule_id
        await db.saved_schedules.replace_one({"id": schedule_id}, doc, upsert=True)
    else:
        doc["id"] = str(uuid.uuid4())
        doc["created_at"] = now
        await db.saved_schedules.insert_one(doc)

    return {"success": True, "id": doc["id"], "message": f"Schedule \"{name}\" saved."}

@api_router.delete("/saved-schedules/{schedule_id}")
async def delete_saved_schedule(schedule_id: str):
    """Delete a saved schedule by its id."""
    result = await db.saved_schedules.delete_one({"id": schedule_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Schedule not found.")
    return {"success": True, "message": "Schedule deleted."}

@api_router.delete("/saved-schedules")
async def delete_all_saved_schedules():
    """Delete ALL saved schedules."""
    result = await db.saved_schedules.delete_many({})
    return {"success": True, "deleted": result.deleted_count, "message": f"Deleted {result.deleted_count} schedule(s)."}


# ── Combined code endpoint (rules + schedules ordered by priority) ──────

@api_router.get("/combined-code")
async def get_combined_code():
    """Return generated code from all saved rules and schedules, ordered by priority (ascending).

    The 'Dependencies from saved rules' section inside each rule's generatedCode
    re-emits variables that were already defined (and correctly ordered) by earlier
    rules.  When the combined code is executed, those re-emissions overwrite the
    correct values with potentially wrong-ordered ones (e.g. totalssp used before
    it is computed).  To prevent this we track every variable name that has already
    been assigned and strip any re-assignment from later rules' dependency sections.
    """
    import re as _re
    try:
        rules = await db.saved_rules.find({}, {"_id": 0}).to_list(500)
        schedules = await db.saved_schedules.find({}, {"_id": 0}).to_list(500)

        items = []
        for r in rules:
            p = r.get("priority")
            items.append({"priority": p if p is not None else float('inf'), "code": r.get("generatedCode", ""), "name": r.get("name", "")})
        for s in schedules:
            p = s.get("priority")
            items.append({"priority": p if p is not None else float('inf'), "code": s.get("generatedCode", ""), "name": s.get("name", "")})

        items.sort(key=lambda x: (x["priority"], x["name"]))

        # For all rules after the first, strip the "## Dependencies from saved rules"
        # section so that prior-rule variables are not redefined in (potentially wrong) order.
        _assign_re = _re.compile(r'^([A-Za-z_][A-Za-z0-9_]*)\s*=')

        def strip_dependencies_section(code: str) -> str:
            """Remove lines between '## Dependencies from saved rules' and the next '##' heading."""
            out = []
            in_deps = False
            for line in code.split('\n'):
                stripped = line.strip()
                if stripped == '## Dependencies from saved rules':
                    in_deps = True
                    # Keep the comment itself so the viewer stays readable
                    out.append(line)
                    continue
                if in_deps:
                    # End of deps section: another ## comment (but not ## ═ which is the
                    # rule header) or a non-empty line that looks like a section marker
                    if stripped.startswith('## ') and not stripped.startswith('## ═'):
                        in_deps = False
                        out.append(line)
                    # else: skip dependency assignment lines
                    continue
                out.append(line)
            return '\n'.join(out)

        code_blocks = []
        for idx, item in enumerate(items):
            code = item.get("code", "")
            if not code:
                continue
            if idx == 0:
                # First rule: emit as-is; record all variables it defines
                code_blocks.append(code)
            else:
                # Later rules: strip their dependencies section to avoid redefining
                # prior-rule variables with stale/wrong ordering
                code_blocks.append(strip_dependencies_section(code))

        combined = "\n\n".join(code_blocks)
        return {"success": True, "code": combined, "count": len(code_blocks)}
    except Exception as e:
        logger.error(f"Error generating combined code: {e}")
        raise HTTPException(status_code=500, detail=str(e))


from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app):
    """Application lifespan: startup and shutdown hooks."""
    logger.info("Application startup")
    yield
    client.close()
    logger.info("Application shutdown — MongoDB client closed")

# Include router under /api so frontend proxying to /api/* resolves correctly
app.include_router(api_router, prefix="/api")

# Also include the same routes at root (no prefix) for dev environments where
# the frontend proxy or external clients may strip the `/api` prefix. This
# makes the backend tolerant to both `/api/...` and `/<route>` requests and
# prevents 404s when the proxy rewrites paths unexpectedly.
app.include_router(api_router)

# Set lifespan on app
app.router.lifespan_context = lifespan

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=settings.cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# WebSocket endpoint for development (supports hot reload, live updates)
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for dev client connections and hot reload"""
    await websocket.accept()
    try:
        while True:
            # Receive and echo messages to keep connection alive
            data = await websocket.receive_text()
            await websocket.send_text(data)
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=settings.host, port=settings.port)

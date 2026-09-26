"""Unit tests for the regression comparison engine.

The diff is what the whole feature rests on: if it reports a difference that
is not real, every case becomes noise, and if it misses one, the suite gives
false confidence. These tests pin the behaviour that matters — duplicate keys,
tolerance, ordering, and format drift.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.regression import (  # noqa: E402
    DEFAULT_TOLERANCE,
    dataset_hash,
    diff_transactions,
    hash_payload,
    normalise_txn,
    scan_nondeterminism,
)


def txn(instrument="LOAN-1", sub="1", posting="2026-03-31",
        effective="2026-03-31", ttype="INTEREST_ACCRUAL", amount=100.0):
    return {
        "instrumentid": instrument,
        "subinstrumentid": sub,
        "postingdate": posting,
        "effectivedate": effective,
        "transactiontype": ttype,
        "amount": amount,
    }


# ── normalisation ───────────────────────────────────────────────────────

def test_normalise_canonicalises_dates():
    row = normalise_txn(txn(posting="03/31/2026", effective="2026-03-31T00:00:00"))
    assert row["postingdate"] == "2026-03-31"
    assert row["effectivedate"] == "2026-03-31"


def test_normalise_reads_fields_case_insensitively():
    row = normalise_txn({
        "InstrumentId": "LOAN-9", "PostingDate": "2026-01-31",
        "EffectiveDate": "2026-01-31", "TransactionType": "FEE",
        "Amount": "12.50",
    })
    assert row["instrumentid"] == "LOAN-9"
    assert row["transactiontype"] == "FEE"
    assert row["amount"] == 12.5
    # Missing subinstrumentid falls back to the contract default.
    assert row["subinstrumentid"] == "1"


def test_normalise_drops_extra_columns():
    row = normalise_txn({**txn(), "batch_id": "abc", "executed_at": "now"})
    assert set(row) == {"instrumentid", "subinstrumentid", "postingdate",
                        "effectivedate", "transactiontype", "amount"}


# ── the happy path ──────────────────────────────────────────────────────

def test_identical_books_pass():
    book = [txn(), txn(instrument="LOAN-2", amount=250.0)]
    result = diff_transactions(book, list(book))
    assert result["passed"] is True
    assert result["rows"] == []
    assert result["counts"]["matched"] == 2
    assert result["counts"]["differences"] == 0


def test_row_order_does_not_matter():
    a = [txn(instrument="LOAN-1"), txn(instrument="LOAN-2"),
         txn(instrument="LOAN-3")]
    result = diff_transactions(a, list(reversed(a)))
    assert result["passed"] is True


def test_date_format_drift_is_not_a_difference():
    expected = [txn(posting="2026-03-31")]
    actual = [txn(posting="03/31/2026")]
    assert diff_transactions(expected, actual)["passed"] is True


def test_case_drift_in_ids_is_not_a_difference():
    expected = [txn(instrument="loan-1", ttype="interest_accrual")]
    actual = [txn(instrument="LOAN-1", ttype="INTEREST_ACCRUAL")]
    assert diff_transactions(expected, actual)["passed"] is True


# ── the three difference kinds ──────────────────────────────────────────

def test_amount_change_is_reported_with_delta():
    result = diff_transactions([txn(amount=100.0)], [txn(amount=105.5)])
    assert result["passed"] is False
    assert result["counts"]["changed"] == 1
    row = result["rows"][0]
    assert row["status"] == "CHANGED"
    assert row["expected_amount"] == 100.0
    assert row["actual_amount"] == 105.5
    assert row["delta"] == pytest.approx(5.5)


def test_missing_transaction_is_reported():
    result = diff_transactions([txn(), txn(instrument="LOAN-2")], [txn()])
    assert result["counts"]["missing"] == 1
    assert result["counts"]["matched"] == 1
    row = next(r for r in result["rows"] if r["status"] == "MISSING")
    assert row["instrumentid"] == "LOAN-2"
    assert row["actual_amount"] is None
    assert row["delta"] == pytest.approx(-100.0)


def test_added_transaction_is_reported():
    result = diff_transactions([txn()], [txn(), txn(instrument="LOAN-3")])
    assert result["counts"]["added"] == 1
    row = next(r for r in result["rows"] if r["status"] == "ADDED")
    assert row["instrumentid"] == "LOAN-3"
    assert row["expected_amount"] is None
    assert row["delta"] == pytest.approx(100.0)


def test_differences_across_every_identity_field():
    """Changing any key field makes it a different transaction, not a change."""
    for field, value in [("instrument", "OTHER"), ("sub", "2"),
                         ("posting", "2026-04-30"), ("effective", "2026-04-30"),
                         ("ttype", "FEE_AMORT")]:
        result = diff_transactions([txn()], [txn(**{field: value})])
        assert result["counts"]["missing"] == 1, field
        assert result["counts"]["added"] == 1, field
        assert result["counts"]["changed"] == 0, field


# ── tolerance ───────────────────────────────────────────────────────────

def test_difference_within_tolerance_matches():
    result = diff_transactions([txn(amount=100.00)], [txn(amount=100.009)])
    assert result["passed"] is True
    assert result["counts"]["matched"] == 1


def test_difference_just_outside_tolerance_is_reported():
    result = diff_transactions([txn(amount=100.00)], [txn(amount=100.02)])
    assert result["counts"]["changed"] == 1


def test_tolerance_is_configurable():
    expected, actual = [txn(amount=100.0)], [txn(amount=100.4)]
    assert diff_transactions(expected, actual, tolerance=0.01)["passed"] is False
    assert diff_transactions(expected, actual, tolerance=0.5)["passed"] is True


def test_float_noise_does_not_fail_a_case():
    """0.1 + 0.2 style drift must not read as a regression."""
    expected = [txn(amount=0.1 + 0.2)]
    actual = [txn(amount=0.3)]
    assert diff_transactions(expected, actual)["passed"] is True


# ── duplicate keys: the multiset behaviour ──────────────────────────────

def test_duplicate_keys_with_same_amounts_all_match():
    """Two identical accruals on one day are legitimate, not a duplicate bug."""
    book = [txn(amount=50.0), txn(amount=50.0)]
    result = diff_transactions(book, list(book))
    assert result["passed"] is True
    assert result["counts"]["matched"] == 2


def test_duplicate_keys_pair_by_sorted_amount():
    expected = [txn(amount=50.0), txn(amount=70.0)]
    actual = [txn(amount=70.0), txn(amount=50.0)]
    assert diff_transactions(expected, actual)["passed"] is True


def test_one_of_a_duplicate_pair_going_missing():
    expected = [txn(amount=50.0), txn(amount=50.0)]
    actual = [txn(amount=50.0)]
    result = diff_transactions(expected, actual)
    assert result["counts"]["matched"] == 1
    assert result["counts"]["missing"] == 1
    assert result["counts"]["added"] == 0


def test_one_of_a_duplicate_pair_changing_amount():
    expected = [txn(amount=50.0), txn(amount=70.0)]
    actual = [txn(amount=50.0), txn(amount=75.0)]
    result = diff_transactions(expected, actual)
    assert result["counts"]["matched"] == 1
    assert result["counts"]["changed"] == 1
    changed = next(r for r in result["rows"] if r["status"] == "CHANGED")
    assert changed["expected_amount"] == 70.0
    assert changed["actual_amount"] == 75.0


def test_extra_duplicate_appears():
    expected = [txn(amount=50.0)]
    actual = [txn(amount=50.0), txn(amount=50.0)]
    result = diff_transactions(expected, actual)
    assert result["counts"]["matched"] == 1
    assert result["counts"]["added"] == 1


# ── empty books ─────────────────────────────────────────────────────────

def test_empty_against_empty_passes():
    result = diff_transactions([], [])
    assert result["passed"] is True
    assert result["counts"]["expected_total"] == 0


def test_producing_nothing_reports_every_expected_row_missing():
    result = diff_transactions([txn(), txn(instrument="LOAN-2")], [])
    assert result["counts"]["missing"] == 2
    assert result["passed"] is False


def test_baseline_of_nothing_reports_every_row_added():
    result = diff_transactions([], [txn(), txn(instrument="LOAN-2")])
    assert result["counts"]["added"] == 2


# ── counts add up ───────────────────────────────────────────────────────

def test_counts_reconcile_against_both_books():
    expected = [txn(instrument=f"LOAN-{i}", amount=float(i)) for i in range(10)]
    actual = ([txn(instrument=f"LOAN-{i}", amount=float(i)) for i in range(5)]
              + [txn(instrument=f"LOAN-{i}", amount=float(i) + 9) for i in range(5, 8)]
              + [txn(instrument="LOAN-99", amount=1.0)])
    counts = diff_transactions(expected, actual)["counts"]
    assert counts["expected_total"] == 10
    assert counts["actual_total"] == 9
    assert counts["matched"] == 5
    assert counts["changed"] == 3
    assert counts["missing"] == 2      # LOAN-8, LOAN-9
    assert counts["added"] == 1        # LOAN-99
    # Every expected row is accounted for exactly once.
    assert counts["matched"] + counts["changed"] + counts["missing"] == 10
    assert counts["matched"] + counts["changed"] + counts["added"] == 9


def test_diff_rows_are_sorted_for_reading():
    expected = [txn(instrument="LOAN-3", posting="2026-05-31"),
                txn(instrument="LOAN-1", posting="2026-01-31"),
                txn(instrument="LOAN-1", posting="2026-02-28")]
    rows = diff_transactions(expected, [])["rows"]
    assert [(r["instrumentid"], r["postingdate"]) for r in rows] == [
        ("LOAN-1", "2026-01-31"), ("LOAN-1", "2026-02-28"), ("LOAN-3", "2026-05-31")]


# ── hashing ─────────────────────────────────────────────────────────────

def test_dataset_hash_is_stable_and_order_independent():
    a = [{"event_name": "LOAN", "event_definition": {"fields": []},
          "data_rows": [{"x": 1}]},
         {"event_name": "FEE", "event_definition": {"fields": []},
          "data_rows": [{"y": 2}]}]
    assert dataset_hash(a) == dataset_hash(list(reversed(a)))


def test_dataset_hash_changes_when_a_row_changes():
    base = [{"event_name": "LOAN", "event_definition": {"fields": []},
             "data_rows": [{"amount": 100}]}]
    changed = [{"event_name": "LOAN", "event_definition": {"fields": []},
                "data_rows": [{"amount": 101}]}]
    assert dataset_hash(base) != dataset_hash(changed)


def test_payload_hash_ignores_key_order():
    assert hash_payload({"a": 1, "b": 2}) == hash_payload({"b": 2, "a": 1})


# ── determinism scanner ─────────────────────────────────────────────────

def test_scanner_flags_wall_clock_and_randomness():
    assert scan_nondeterminism("x = random.random()") == ["random"]
    assert "uuid4" in scan_nondeterminism("ref = uuid4()")
    assert scan_nondeterminism("d = datetime.now()") == ["datetime.now"]


def test_scanner_is_quiet_on_ordinary_rules():
    code = "interest = balance * rate / 365\npost('INTEREST_ACCRUAL', interest)"
    assert scan_nondeterminism(code) == []
    assert scan_nondeterminism("") == []


def test_default_tolerance_is_a_cent():
    assert DEFAULT_TOLERANCE == 0.01


# ── coercion of spreadsheet-sourced values ──────────────────────────────

def test_thousands_separated_amounts_are_not_read_as_zero():
    """A comma-formatted amount collapsing to 0.0 would look like a real
    zero rather than the parse failure it is."""
    assert normalise_txn({"amount": "1,234.56"})["amount"] == 1234.56
    assert normalise_txn({"amount": "$1,200"})["amount"] == 1200.0


def test_parenthesised_negatives_keep_their_sign():
    assert normalise_txn({"amount": "(500.00)"})["amount"] == -500.0


def test_unparseable_amount_still_falls_back_to_zero():
    assert normalise_txn({"amount": "abc"})["amount"] == 0.0
    assert normalise_txn({"amount": None})["amount"] == 0.0


def test_booleans_are_not_treated_as_amounts():
    """float(True) is 1.0; a stray flag must not post as a dollar."""
    assert normalise_txn({"amount": True})["amount"] == 0.0


def test_compact_numeric_dates_canonicalise():
    assert normalise_txn({"postingdate": 20260331})["postingdate"] == "2026-03-31"
    assert normalise_txn({"postingdate": "20260331"})["postingdate"] == "2026-03-31"


def test_a_formatted_amount_matches_its_plain_equivalent():
    assert diff_transactions([txn(amount="1,000.00")], [txn(amount=1000.0)])["passed"]


# ── determinism scanner: the call forms that actually appear in DSL ─────

def test_scanner_flags_bare_now_and_today_calls():
    """`now()` / `today()` are the likeliest DSL forms and were missed by a
    trailing word-boundary anchor that a closing paren can never satisfy."""
    assert scan_nondeterminism("x = now()") == ["now"]
    assert scan_nondeterminism("d = today()") == ["today"]
    assert scan_nondeterminism("x = now() + 1") == ["now"]


def test_scanner_still_flags_module_qualified_forms():
    assert scan_nondeterminism("d = datetime.now()") == ["datetime.now"]
    assert "random" in scan_nondeterminism("r = random.random()")
    assert "uuid4" in scan_nondeterminism("u = uuid4()")


def test_scanner_does_not_flag_similarly_named_variables():
    """`now_balance` is a variable, not a clock read."""
    assert scan_nondeterminism("now_balance = 5") == []
    assert scan_nondeterminism("today_rate = 0.05") == []

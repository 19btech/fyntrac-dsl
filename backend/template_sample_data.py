"""
Pre-defined sample event definitions and event data for each standard template.
Each template ID maps to events (definitions) and event_data (rows) that are
workable with that template's generateDSL function.
"""

TEMPLATE_SAMPLE_DATA = {

    # ─── Loan Amortization ────────────────────────────────────────────
    "loan_amortization": {
        "events": [
            {
                "event_name": "LoanSetup",
                "fields": [
                    {"name": "principal", "datatype": "decimal"},
                    {"name": "annual_rate", "datatype": "decimal"},
                    {"name": "term_months", "datatype": "decimal"},
                    {"name": "start_date", "datatype": "date"},
                ],
                "eventType": "activity",
                "eventTable": "standard",
            }
        ],
        "event_data": [
            {
                "event_name": "LoanSetup",
                "data_rows": [
                    {
                        "postingdate": "2026-01-01",
                        "effectivedate": "2026-01-01",
                        "instrumentid": "LOAN-001",
                        "principal": "100000",
                        "annual_rate": "6",
                        "term_months": "12",
                        "start_date": "2026-01-01",
                    },
                    {
                        "postingdate": "2026-02-01",
                        "effectivedate": "2026-02-01",
                        "instrumentid": "LOAN-002",
                        "principal": "250000",
                        "annual_rate": "4.5",
                        "term_months": "24",
                        "start_date": "2026-02-01",
                    },
                ],
            }
        ],
    },

    # ─── Straight-Line Depreciation ───────────────────────────────────
    "straight_line_depreciation": {
        "events": [
            {
                "event_name": "AssetEvent",
                "fields": [
                    {"name": "asset_cost", "datatype": "decimal"},
                    {"name": "salvage_value", "datatype": "decimal"},
                    {"name": "useful_life", "datatype": "decimal"},
                    {"name": "start_date", "datatype": "date"},
                ],
                "eventType": "activity",
                "eventTable": "standard",
            }
        ],
        "event_data": [
            {
                "event_name": "AssetEvent",
                "data_rows": [
                    {
                        "postingdate": "2026-01-01",
                        "effectivedate": "2026-01-01",
                        "instrumentid": "ASSET-001",
                        "asset_cost": "50000",
                        "salvage_value": "5000",
                        "useful_life": "5",
                        "start_date": "2026-01-01",
                    },
                    {
                        "postingdate": "2026-03-15",
                        "effectivedate": "2026-03-15",
                        "instrumentid": "ASSET-002",
                        "asset_cost": "120000",
                        "salvage_value": "10000",
                        "useful_life": "10",
                        "start_date": "2026-03-15",
                    },
                ],
            }
        ],
    },

    # ─── Revenue Recognition (ASC 606) ────────────────────────────────
    "revenue_recognition": {
        "events": [
            {
                "event_name": "RevenueContract",
                "fields": [
                    {"name": "product_name", "datatype": "string"},
                    {"name": "selling_price", "datatype": "decimal"},
                    {"name": "item_start_date", "datatype": "date"},
                    {"name": "item_end_date", "datatype": "date"},
                ],
                "eventType": "activity",
                "eventTable": "standard",
            }
        ],
        "event_data": [
            {
                "event_name": "RevenueContract",
                "data_rows": [
                    {
                        "postingdate": "2026-01-01",
                        "effectivedate": "2026-01-01",
                        "instrumentid": "CONTRACT-001",
                        "subinstrumentid": "ITEM-A",
                        "product_name": "Software License",
                        "selling_price": "50000",
                        "item_start_date": "2026-01-01",
                        "item_end_date": "2026-12-31",
                    },
                    {
                        "postingdate": "2026-01-01",
                        "effectivedate": "2026-01-01",
                        "instrumentid": "CONTRACT-001",
                        "subinstrumentid": "ITEM-B",
                        "product_name": "Support Services",
                        "selling_price": "20000",
                        "item_start_date": "2026-01-01",
                        "item_end_date": "2027-06-30",
                    },
                    {
                        "postingdate": "2026-01-01",
                        "effectivedate": "2026-01-01",
                        "instrumentid": "CONTRACT-001",
                        "subinstrumentid": "ITEM-C",
                        "product_name": "Training",
                        "selling_price": "10000",
                        "item_start_date": "2026-01-01",
                        "item_end_date": "2026-03-31",
                    },
                ],
            }
        ],
    },

    # ─── Interest Accrual ─────────────────────────────────────────────
    "interest_accrual": {
        "events": [
            {
                "event_name": "AccrualSetup",
                "fields": [
                    {"name": "balance", "datatype": "decimal"},
                    {"name": "annual_rate", "datatype": "decimal"},
                    {"name": "accrual_start", "datatype": "date"},
                    {"name": "accrual_end", "datatype": "date"},
                ],
                "eventType": "activity",
                "eventTable": "standard",
            }
        ],
        "event_data": [
            {
                "event_name": "AccrualSetup",
                "data_rows": [
                    {
                        "postingdate": "2026-01-01",
                        "effectivedate": "2026-01-01",
                        "instrumentid": "ACCRUAL-001",
                        "balance": "100000",
                        "annual_rate": "5",
                        "accrual_start": "2026-01-01",
                        "accrual_end": "2026-12-31",
                    },
                    {
                        "postingdate": "2026-01-01",
                        "effectivedate": "2026-01-01",
                        "instrumentid": "ACCRUAL-002",
                        "balance": "75000",
                        "annual_rate": "3.5",
                        "accrual_start": "2026-03-01",
                        "accrual_end": "2026-09-30",
                    },
                ],
            }
        ],
    },

    # ─── Fee Amortization (FAS 91) ────────────────────────────────────
    "fee_amortization": {
        "events": [
            {
                "event_name": "FeeEvent",
                "fields": [
                    {"name": "fee_amount", "datatype": "decimal"},
                    {"name": "loan_amount", "datatype": "decimal"},
                    {"name": "term_months", "datatype": "decimal"},
                    {"name": "start_date", "datatype": "date"},
                ],
                "eventType": "activity",
                "eventTable": "standard",
            }
        ],
        "event_data": [
            {
                "event_name": "FeeEvent",
                "data_rows": [
                    {
                        "postingdate": "2026-01-01",
                        "effectivedate": "2026-01-01",
                        "instrumentid": "FEE-001",
                        "fee_amount": "5000",
                        "loan_amount": "100000",
                        "term_months": "36",
                        "start_date": "2026-01-01",
                    },
                    {
                        "postingdate": "2026-02-15",
                        "effectivedate": "2026-02-15",
                        "instrumentid": "FEE-002",
                        "fee_amount": "3000",
                        "loan_amount": "75000",
                        "term_months": "24",
                        "start_date": "2026-02-15",
                    },
                ],
            }
        ],
    },

    # ─── Double Declining Balance Depreciation ────────────────────────
    "double_declining_depreciation": {
        "events": [
            {
                "event_name": "DDBAssetEvent",
                "fields": [
                    {"name": "asset_cost", "datatype": "decimal"},
                    {"name": "salvage_value", "datatype": "decimal"},
                    {"name": "useful_life", "datatype": "decimal"},
                    {"name": "start_date", "datatype": "date"},
                ],
                "eventType": "activity",
                "eventTable": "standard",
            }
        ],
        "event_data": [
            {
                "event_name": "DDBAssetEvent",
                "data_rows": [
                    {
                        "postingdate": "2026-01-01",
                        "effectivedate": "2026-01-01",
                        "instrumentid": "ASSET-DDB-001",
                        "asset_cost": "50000",
                        "salvage_value": "5000",
                        "useful_life": "5",
                        "start_date": "2026-01-01",
                    },
                    {
                        "postingdate": "2026-06-01",
                        "effectivedate": "2026-06-01",
                        "instrumentid": "ASSET-DDB-002",
                        "asset_cost": "80000",
                        "salvage_value": "8000",
                        "useful_life": "7",
                        "start_date": "2026-06-01",
                    },
                ],
            }
        ],
    },

    # ─── NPV Analysis ─────────────────────────────────────────────────
    "npv_analysis": {
        "events": [
            {
                "event_name": "InvestmentProject",
                "fields": [
                    {"name": "initial_investment", "datatype": "decimal"},
                    {"name": "discount_rate", "datatype": "decimal"},
                    {"name": "cashflow_yr1", "datatype": "decimal"},
                    {"name": "cashflow_yr2", "datatype": "decimal"},
                    {"name": "cashflow_yr3", "datatype": "decimal"},
                    {"name": "cashflow_yr4", "datatype": "decimal"},
                    {"name": "cashflow_yr5", "datatype": "decimal"},
                ],
                "eventType": "activity",
                "eventTable": "standard",
            }
        ],
        "event_data": [
            {
                "event_name": "InvestmentProject",
                "data_rows": [
                    {
                        "postingdate": "2026-01-01",
                        "effectivedate": "2026-01-01",
                        "instrumentid": "PROJECT-001",
                        "initial_investment": "100000",
                        "discount_rate": "8",
                        "cashflow_yr1": "30000",
                        "cashflow_yr2": "35000",
                        "cashflow_yr3": "40000",
                        "cashflow_yr4": "25000",
                        "cashflow_yr5": "20000",
                    },
                    {
                        "postingdate": "2026-01-01",
                        "effectivedate": "2026-01-01",
                        "instrumentid": "PROJECT-002",
                        "initial_investment": "50000",
                        "discount_rate": "10",
                        "cashflow_yr1": "15000",
                        "cashflow_yr2": "18000",
                        "cashflow_yr3": "20000",
                        "cashflow_yr4": "12000",
                        "cashflow_yr5": "10000",
                    },
                ],
            }
        ],
    },

    # ─── Lease Accounting (ASC 842) ───────────────────────────────────
    "lease_accounting": {
        "events": [
            {
                "event_name": "LeaseSetup",
                "fields": [
                    {"name": "lease_payment", "datatype": "decimal"},
                    {"name": "lease_term", "datatype": "decimal"},
                    {"name": "discount_rate", "datatype": "decimal"},
                    {"name": "start_date", "datatype": "date"},
                ],
                "eventType": "activity",
                "eventTable": "standard",
            }
        ],
        "event_data": [
            {
                "event_name": "LeaseSetup",
                "data_rows": [
                    {
                        "postingdate": "2026-01-01",
                        "effectivedate": "2026-01-01",
                        "instrumentid": "LEASE-001",
                        "lease_payment": "5000",
                        "lease_term": "36",
                        "discount_rate": "5",
                        "start_date": "2026-01-01",
                    },
                    {
                        "postingdate": "2026-04-01",
                        "effectivedate": "2026-04-01",
                        "instrumentid": "LEASE-002",
                        "lease_payment": "8000",
                        "lease_term": "60",
                        "discount_rate": "4.5",
                        "start_date": "2026-04-01",
                    },
                ],
            }
        ],
    },
}

/**
 * ACCOUNTING_TEMPLATES — Pre-built configurable templates for common accounting use cases.
 * Each template defines wizard steps, field configurations, and DSL code generation logic.
 */

const ACCOUNTING_TEMPLATES = [
  {
    id: 'loan_amortization',
    title: 'Loan Amortization Schedule',
    description: 'Generate a loan payment schedule with interest, principal, and balance tracking',
    category: 'Loans & Lending',
    icon: 'TrendingUp',
    standard: 'ASC 310',
    fields: [
      { key: 'principal', label: 'Loan Principal Amount', type: 'number_or_field', required: true, placeholder: '100000', helpText: 'The original loan amount' },
      { key: 'annual_rate', label: 'Annual Interest Rate (%)', type: 'number_or_field', required: true, placeholder: '6.0', helpText: 'Annual rate as a percentage (e.g., 6 for 6%)' },
      { key: 'term_months', label: 'Loan Term (months)', type: 'number_or_field', required: true, placeholder: '12', helpText: 'Total number of monthly payments' },
      { key: 'start_date', label: 'Start Date', type: 'date_or_field', required: true, placeholder: '2026-01-01' },
      { key: 'payment_freq', label: 'Payment Frequency', type: 'select', options: ['Monthly', 'Quarterly', 'Semi-Annual', 'Annual'], default: 'Monthly' },
      { key: 'day_count', label: 'Day Count Convention', type: 'select', options: ['ACT/360', 'ACT/365', '30/360'], default: 'ACT/360' },
    ],
    outputs: [
      { key: 'interest', label: 'Interest Accrued', default: true },
      { key: 'principal_payment', label: 'Principal Payment', default: true },
      { key: 'closing_balance', label: 'Closing Balance', default: true },
      { key: 'create_txn', label: 'Create Transaction', default: false, txnType: 'Interest Accrual' },
    ],
    generateDSL: (config) => {
      const freqMap = { 'Monthly': 'M', 'Quarterly': 'Q', 'Semi-Annual': 'S', 'Annual': 'A' };
      const freq = freqMap[config.payment_freq] || 'M';
      const divisor = config.payment_freq === 'Monthly' ? 12 : config.payment_freq === 'Quarterly' ? 4 : config.payment_freq === 'Semi-Annual' ? 2 : 1;
      const principal = config.principal_source === 'field' ? config.principal_field : config.principal;
      const rate = config.annual_rate_source === 'field' ? config.annual_rate_field : config.annual_rate;
      const term = config.term_months_source === 'field' ? config.term_months_field : config.term_months;
      const startDate = config.start_date_source === 'field' ? config.start_date_field : `"${config.start_date}"`;

      let lines = [];
      lines.push('## ═══════════════════════════════════════════════════════════════');
      lines.push('## LOAN AMORTIZATION SCHEDULE');
      lines.push('## ═══════════════════════════════════════════════════════════════');
      lines.push('');
      lines.push(`## Loan Parameters`);
      lines.push(`loan_amount = ${principal}`);
      lines.push(`annual_rate = ${rate}`);
      lines.push(`annual_rate_decimal = divide(annual_rate, 100)`);
      lines.push(`periodic_rate = divide(annual_rate_decimal, ${divisor})`);
      lines.push(`loan_term = ${term}`);
      lines.push('');
      lines.push('## Calculate fixed periodic payment');
      lines.push('payment = abs(pmt(periodic_rate, loan_term, loan_amount))');
      lines.push('');
      lines.push('## Create schedule period');
      lines.push(`end_date = add_months(${startDate}, loan_term)`);
      lines.push(`p = period(${startDate}, end_date, "${freq}")`);
      lines.push('');
      lines.push('## Generate amortization schedule');
      lines.push('sched = schedule(p, {');
      lines.push('    "date": "period_date",');
      lines.push('    "opening_bal": "lag(\'closing_bal\', 1, loan_amount)",');
      lines.push('    "interest": "multiply(opening_bal, periodic_rate)",');
      lines.push('    "principal_pmt": "subtract(payment, interest)",');
      lines.push('    "closing_bal": "subtract(opening_bal, principal_pmt)",');
      lines.push('    "payment": "payment"');
      lines.push('}, {"loan_amount": loan_amount, "periodic_rate": periodic_rate, "payment": payment})');
      lines.push('');

      if (config.outputs_interest) {
        lines.push('print("Total Interest:", schedule_sum(sched, "interest"))');
      }
      if (config.outputs_principal_payment) {
        lines.push('print("Total Principal:", schedule_sum(sched, "principal_pmt"))');
      }
      lines.push('print(sched)');

      if (config.outputs_create_txn) {
        lines.push('');
        lines.push(`## Create transaction`);
        lines.push('interest_for_postingdate = schedule_filter(sched, "date", postingdate, "interest")');
        lines.push('print("Interest for posting date:", interest_for_postingdate)');
        lines.push(`createTransaction(postingdate, postingdate, "${config.txn_type || 'Interest Accrual'}", interest_for_postingdate)`);
      }

      return lines.join('\n');
    },
  },

  {
    id: 'straight_line_depreciation',
    title: 'Straight-Line Depreciation',
    description: 'Calculate periodic depreciation expense evenly over the asset life',
    category: 'Depreciation',
    icon: 'TrendingDown',
    standard: 'ASC 360',
    fields: [
      { key: 'asset_cost', label: 'Asset Cost', type: 'number_or_field', required: true, placeholder: '50000' },
      { key: 'salvage_value', label: 'Salvage (Residual) Value', type: 'number_or_field', required: true, placeholder: '5000' },
      { key: 'useful_life', label: 'Useful Life (years)', type: 'number_or_field', required: true, placeholder: '5' },
      { key: 'start_date', label: 'In-Service Date', type: 'date_or_field', required: true, placeholder: '2026-01-01' },
      { key: 'partial_year', label: 'First Year Convention', type: 'select', options: ['Full Year', 'Half-Year', 'Mid-Month', 'Prorate from In-Service'], default: 'Full Year' },
    ],
    outputs: [
      { key: 'annual_depreciation', label: 'Annual Depreciation', default: true },
      { key: 'schedule', label: 'Depreciation Schedule', default: true },
      { key: 'create_txn', label: 'Create Transaction', default: false, txnType: 'Depreciation Expense' },
    ],
    generateDSL: (config) => {
      const cost = config.asset_cost_source === 'field' ? config.asset_cost_field : config.asset_cost;
      const salvage = config.salvage_value_source === 'field' ? config.salvage_value_field : config.salvage_value;
      const life = config.useful_life_source === 'field' ? config.useful_life_field : config.useful_life;
      const startDate = config.start_date_source === 'field' ? config.start_date_field : `"${config.start_date}"`;

      let lines = [];
      lines.push('## ═══════════════════════════════════════════════════════════════');
      lines.push('## STRAIGHT-LINE DEPRECIATION');
      lines.push('## ═══════════════════════════════════════════════════════════════');
      lines.push('');
      lines.push(`asset_cost = ${cost}`);
      lines.push(`salvage_value = ${salvage}`);
      lines.push(`useful_life_years = ${life}`);
      lines.push('');
      lines.push('## Calculate annual depreciation');
      lines.push('annual_depr = straight_line(asset_cost, salvage_value, useful_life_years)');
      lines.push('monthly_depr = divide(annual_depr, 12)');
      lines.push('');

      if (config.outputs_schedule) {
        lines.push('## Generate depreciation schedule');
        lines.push(`end_date = add_years(${startDate}, useful_life_years)`);
        lines.push(`p = period(${startDate}, end_date, "A")`);
        lines.push('');
        lines.push('sched = schedule(p, {');
        lines.push('    "year": "period_date",');
        lines.push('    "opening_nbv": "lag(\'closing_nbv\', 1, asset_cost)",');
        lines.push('    "depreciation": "annual_depr",');
        lines.push('    "closing_nbv": "subtract(opening_nbv, depreciation)"');
        lines.push('}, {"asset_cost": asset_cost, "annual_depr": annual_depr})');
        lines.push('');
        lines.push('print(sched)');
      }

      lines.push('print("Annual Depreciation:", annual_depr)');
      lines.push('print("Monthly Depreciation:", monthly_depr)');

      if (config.outputs_create_txn) {
        lines.push('');
        if (config.outputs_schedule) {
          lines.push('depr_for_postingdate = schedule_filter(sched, "year", postingdate, "depreciation")');
          lines.push('print("Depreciation for posting date:", depr_for_postingdate)');
          lines.push(`createTransaction(postingdate, postingdate, "${config.txn_type || 'Depreciation Expense'}", depr_for_postingdate)`);
        } else {
          lines.push(`createTransaction(${startDate}, ${startDate}, "${config.txn_type || 'Depreciation Expense'}", annual_depr)`);
        }
      }

      return lines.join('\n');
    },
  },

  {
    id: 'revenue_recognition',
    title: 'Revenue Recognition (ASC 606)',
    description: 'Allocate and recognize revenue across performance obligations over time',
    category: 'Revenue',
    icon: 'DollarSign',
    standard: 'ASC 606',
    fields: [
      { key: 'product_names', label: 'Product/Obligation Names', type: 'field', required: true, helpText: 'Event field containing product or obligation names' },
      { key: 'selling_prices', label: 'Extended Selling Prices', type: 'field', required: true, helpText: 'Event field containing selling prices for each obligation' },
      { key: 'start_dates', label: 'Item Start Dates', type: 'field', required: true, helpText: 'Event field with start date for each obligation' },
      { key: 'end_dates', label: 'Item End Dates', type: 'field', required: true, helpText: 'Event field with end date for each obligation' },
      { key: 'posting_date', label: 'Posting Date', type: 'date_or_field', required: true },
      { key: 'allocation_method', label: 'Allocation Method', type: 'select', options: ['SSP-Weighted (Relative)', 'Standalone', 'Residual'], default: 'SSP-Weighted (Relative)' },
      { key: 'recognition_pattern', label: 'Recognition Pattern', type: 'select', options: ['Over Time (Daily)', 'Over Time (Monthly)', 'Point in Time'], default: 'Over Time (Daily)' },
    ],
    outputs: [
      { key: 'allocation', label: 'Revenue Allocation', default: true },
      { key: 'schedule', label: 'Recognition Schedule', default: true },
      { key: 'create_txn', label: 'Create Transaction', default: true, txnType: 'Revenue' },
    ],
    generateDSL: (config) => {
      const postingDate = config.posting_date_source === 'field' ? config.posting_date_field : `"${config.posting_date}"`;

      let lines = [];
      lines.push('## ═══════════════════════════════════════════════════════════════');
      lines.push('## ASC 606 REVENUE RECOGNITION');
      lines.push('## ═══════════════════════════════════════════════════════════════');
      lines.push('');
      lines.push('## Collect event data arrays');
      lines.push(`product_names = collect_by_instrument(${config.product_names_field})`);
      lines.push(`esp_values = collect_by_instrument(${config.selling_prices_field})`);
      lines.push(`start_dates = collect_by_instrument(${config.start_dates_field})`);
      lines.push(`end_dates = collect_by_instrument(${config.end_dates_field})`);
      lines.push(`subinstrument_ids = collect_subinstrumentids()`);
      lines.push(`posting_date = ${postingDate}`);
      lines.push('ssp_values = esp_values');
      lines.push('total_ssp = sum(ssp_values)');
      lines.push('total_esp = sum(esp_values)');
      lines.push('');

      if (config.allocation_method === 'SSP-Weighted (Relative)') {
        lines.push('## SSP-weighted allocation');
        lines.push('alloc_pcts = map_array(ssp_values, "ssp", "iif(gt(total_ssp, 0), divide(ssp, total_ssp), 0)", {"total_ssp": total_ssp})');
        lines.push('allocated_revenues = map_array(alloc_pcts, "pct", "multiply(pct, total_esp)", {"total_esp": total_esp})');
      } else {
        lines.push('## Standalone allocation');
        lines.push('allocated_revenues = esp_values');
      }
      lines.push('print("Allocated Revenues:", allocated_revenues)');
      lines.push('');

      if (config.outputs_schedule) {
        lines.push('## Recognition schedule');
        lines.push('p = period(start_dates, end_dates, "M")');
        lines.push('sched = schedule(p, {');
        lines.push('    "date": "period_date",');
        lines.push('    "revenue": "divide(amount, total_periods)"');
        lines.push('}, {"amounts": allocated_revenues, "subinstrument_ids": subinstrument_ids})');
        lines.push('print(sched)');
        lines.push('');
        lines.push('print("Total Revenue:", schedule_sum(sched, "revenue"))');
      }

      if (config.outputs_create_txn) {
        lines.push('');
        if (config.outputs_schedule) {
          lines.push('revenue_for_postingdate = schedule_filter(sched, "date", posting_date, "revenue")');
          lines.push('print("Revenue for posting date:", revenue_for_postingdate)');
          lines.push(`createTransaction(posting_date, posting_date, "${config.txn_type || 'Revenue'}", revenue_for_postingdate, subinstrument_ids)`);
        } else {
          lines.push(`createTransaction(posting_date, posting_date, "${config.txn_type || 'Revenue'}", total_rev, subinstrument_ids)`);
        }
      }

      return lines.join('\n');
    },
  },

  {
    id: 'interest_accrual',
    title: 'Interest Accrual',
    description: 'Calculate daily or monthly interest accrual on a principal balance',
    category: 'Accruals',
    icon: 'Percent',
    standard: 'General',
    fields: [
      { key: 'balance', label: 'Outstanding Balance / Principal', type: 'number_or_field', required: true, placeholder: '100000' },
      { key: 'annual_rate', label: 'Annual Interest Rate (%)', type: 'number_or_field', required: true, placeholder: '5.0' },
      { key: 'start_date', label: 'Accrual Start Date', type: 'date_or_field', required: true, placeholder: '2026-01-01' },
      { key: 'end_date', label: 'Accrual End Date', type: 'date_or_field', required: true, placeholder: '2026-12-31' },
      { key: 'day_count', label: 'Day Count Convention', type: 'select', options: ['ACT/360', 'ACT/365', '30/360'], default: 'ACT/360' },
      { key: 'accrual_freq', label: 'Accrual Frequency', type: 'select', options: ['Daily', 'Monthly'], default: 'Monthly' },
    ],
    outputs: [
      { key: 'schedule', label: 'Accrual Schedule', default: true },
      { key: 'total_interest', label: 'Total Interest', default: true },
      { key: 'create_txn', label: 'Create Transaction', default: false, txnType: 'Interest Accrual' },
    ],
    generateDSL: (config) => {
      const balance = config.balance_source === 'field' ? config.balance_field : config.balance;
      const rate = config.annual_rate_source === 'field' ? config.annual_rate_field : config.annual_rate;
      const startDate = config.start_date_source === 'field' ? config.start_date_field : `"${config.start_date}"`;
      const endDate = config.end_date_source === 'field' ? config.end_date_field : `"${config.end_date}"`;
      const dayBasis = config.day_count === 'ACT/365' ? 365 : config.day_count === '30/360' ? 360 : 360;

      let lines = [];
      lines.push('## ═══════════════════════════════════════════════════════════════');
      lines.push('## INTEREST ACCRUAL SCHEDULE');
      lines.push('## ═══════════════════════════════════════════════════════════════');
      lines.push('');
      lines.push(`balance = ${balance}`);
      lines.push(`annual_rate = divide(${rate}, 100)`);
      lines.push(`day_basis = ${dayBasis}`);
      lines.push(`daily_rate = divide(annual_rate, day_basis)`);
      lines.push('');
      lines.push(`p = period(${startDate}, ${endDate}, "M")`);
      lines.push('');
      lines.push('sched = schedule(p, {');
      lines.push('    "period_date": "period_date",');
      lines.push('    "days_in_period": "add(days_between(start_of_month(period_date), end_of_month(period_date)), 1)",');
      lines.push('    "accrued_interest": "multiply(multiply(balance, daily_rate), days_in_period)"');
      lines.push('}, {"balance": balance, "daily_rate": daily_rate})');
      lines.push('');
      lines.push('total_interest = schedule_sum(sched, "accrued_interest")');
      lines.push('print(sched)');
      lines.push('print("Total Interest Accrued:", total_interest)');

      if (config.outputs_create_txn) {
        lines.push('');
        lines.push('interest_for_postingdate = schedule_filter(sched, "period_date", postingdate, "accrued_interest")');
        lines.push('print("Interest for posting date:", interest_for_postingdate)');
        lines.push(`createTransaction(postingdate, postingdate, "${config.txn_type || 'Interest Accrual'}", interest_for_postingdate)`);
      }

      return lines.join('\n');
    },
  },

  {
    id: 'fee_amortization',
    title: 'Fee Amortization (FAS 91)',
    description: 'Amortize loan origination fees using the level-yield method over the loan life',
    category: 'Loans & Lending',
    icon: 'Receipt',
    standard: 'FAS 91',
    fields: [
      { key: 'fee_amount', label: 'Fee Amount', type: 'number_or_field', required: true, placeholder: '5000' },
      { key: 'loan_amount', label: 'Loan Amount', type: 'number_or_field', required: true, placeholder: '100000' },
      { key: 'term_months', label: 'Loan Term (months)', type: 'number_or_field', required: true, placeholder: '36' },
      { key: 'start_date', label: 'Origination Date', type: 'date_or_field', required: true, placeholder: '2026-01-01' },
    ],
    outputs: [
      { key: 'schedule', label: 'Amortization Schedule', default: true },
      { key: 'create_txn', label: 'Create Transaction', default: false, txnType: 'Fee Amortization' },
    ],
    generateDSL: (config) => {
      const fee = config.fee_amount_source === 'field' ? config.fee_amount_field : config.fee_amount;
      const loan = config.loan_amount_source === 'field' ? config.loan_amount_field : config.loan_amount;
      const term = config.term_months_source === 'field' ? config.term_months_field : config.term_months;
      const startDate = config.start_date_source === 'field' ? config.start_date_field : `"${config.start_date}"`;

      let lines = [];
      lines.push('## ═══════════════════════════════════════════════════════════════');
      lines.push('## FAS 91 FEE AMORTIZATION (STRAIGHT-LINE)');
      lines.push('## ═══════════════════════════════════════════════════════════════');
      lines.push('');
      lines.push(`fee_amount = ${fee}`);
      lines.push(`loan_term = ${term}`);
      lines.push('monthly_amort = divide(fee_amount, loan_term)');
      lines.push('');
      lines.push(`end_date = add_months(${startDate}, loan_term)`);
      lines.push(`p = period(${startDate}, end_date, "M")`);
      lines.push('');
      lines.push('sched = schedule(p, {');
      lines.push('    "date": "period_date",');
      lines.push('    "opening_balance": "lag(\'closing_balance\', 1, fee_amount)",');
      lines.push('    "amortization": "monthly_amort",');
      lines.push('    "closing_balance": "subtract(opening_balance, amortization)"');
      lines.push('}, {"fee_amount": fee_amount, "monthly_amort": monthly_amort})');
      lines.push('');
      lines.push('print(sched)');
      lines.push('print("Monthly Amortization:", monthly_amort)');

      if (config.outputs_create_txn) {
        lines.push('');
        if (config.outputs_schedule) {
          lines.push('amort_for_postingdate = schedule_filter(sched, "date", postingdate, "amortization")');
          lines.push('print("Amortization for posting date:", amort_for_postingdate)');
          lines.push(`createTransaction(postingdate, postingdate, "${config.txn_type || 'Fee Amortization'}", amort_for_postingdate)`);
        } else {
          lines.push(`createTransaction(${startDate}, ${startDate}, "${config.txn_type || 'Fee Amortization'}", monthly_amort)`);
        }
      }

      return lines.join('\n');
    },
  },

  {
    id: 'double_declining_depreciation',
    title: 'Double Declining Balance Depreciation',
    description: 'Accelerated depreciation using the double declining balance method',
    category: 'Depreciation',
    icon: 'TrendingDown',
    standard: 'ASC 360',
    fields: [
      { key: 'asset_cost', label: 'Asset Cost', type: 'number_or_field', required: true, placeholder: '50000' },
      { key: 'salvage_value', label: 'Salvage Value', type: 'number_or_field', required: true, placeholder: '5000' },
      { key: 'useful_life', label: 'Useful Life (years)', type: 'number_or_field', required: true, placeholder: '5' },
      { key: 'start_date', label: 'In-Service Date', type: 'date_or_field', required: true, placeholder: '2026-01-01' },
    ],
    outputs: [
      { key: 'schedule', label: 'Depreciation Schedule', default: true },
      { key: 'create_txn', label: 'Create Transaction', default: false, txnType: 'Depreciation Expense' },
    ],
    generateDSL: (config) => {
      const cost = config.asset_cost_source === 'field' ? config.asset_cost_field : config.asset_cost;
      const salvage = config.salvage_value_source === 'field' ? config.salvage_value_field : config.salvage_value;
      const life = config.useful_life_source === 'field' ? config.useful_life_field : config.useful_life;
      const startDate = config.start_date_source === 'field' ? config.start_date_field : `"${config.start_date}"`;

      let lines = [];
      lines.push('## ═══════════════════════════════════════════════════════════════');
      lines.push('## DOUBLE DECLINING BALANCE DEPRECIATION');
      lines.push('## ═══════════════════════════════════════════════════════════════');
      lines.push('');
      lines.push(`asset_cost = ${cost}`);
      lines.push(`salvage_value = ${salvage}`);
      lines.push(`useful_life = ${life}`);
      lines.push('ddb_rate = divide(2, useful_life)');
      lines.push('');
      lines.push(`end_date = add_years(${startDate}, useful_life)`);
      lines.push(`p = period(${startDate}, end_date, "A")`);
      lines.push('');
      lines.push('sched = schedule(p, {');
      lines.push('    "year": "period_date",');
      lines.push('    "opening_nbv": "lag(\'closing_nbv\', 1, asset_cost)",');
      lines.push('    "depreciation": "iif(gt(subtract(opening_nbv, multiply(opening_nbv, ddb_rate)), salvage_value), multiply(opening_nbv, ddb_rate), subtract(opening_nbv, salvage_value))",');
      lines.push('    "closing_nbv": "subtract(opening_nbv, depreciation)"');
      lines.push('}, {"asset_cost": asset_cost, "salvage_value": salvage_value, "ddb_rate": ddb_rate})');
      lines.push('');
      lines.push('print(sched)');
      lines.push('print("Total Depreciation:", schedule_sum(sched, "depreciation"))');

      if (config.outputs_create_txn) {
        lines.push('');
        if (config.outputs_schedule) {
          lines.push('depr_for_postingdate = schedule_filter(sched, "year", postingdate, "depreciation")');
          lines.push('print("Depreciation for posting date:", depr_for_postingdate)');
          lines.push(`createTransaction(postingdate, postingdate, "${config.txn_type || 'Depreciation Expense'}", depr_for_postingdate)`);
        } else {
          lines.push(`createTransaction(${startDate}, ${startDate}, "${config.txn_type || 'Depreciation Expense'}", schedule_sum(sched, "depreciation"))`);
        }
      }

      return lines.join('\n');
    },
  },

  {
    id: 'npv_analysis',
    title: 'Net Present Value Analysis',
    description: 'Evaluate investment decisions using NPV and IRR calculations',
    category: 'Financial Analysis',
    icon: 'Calculator',
    standard: 'General',
    fields: [
      { key: 'initial_investment', label: 'Initial Investment', type: 'number_or_field', required: true, placeholder: '100000' },
      { key: 'discount_rate', label: 'Discount Rate (%)', type: 'number_or_field', required: true, placeholder: '8' },
      { key: 'cashflow_1', label: 'Year 1 Cash Flow', type: 'number_or_field', required: true, placeholder: '30000' },
      { key: 'cashflow_2', label: 'Year 2 Cash Flow', type: 'number_or_field', required: true, placeholder: '35000' },
      { key: 'cashflow_3', label: 'Year 3 Cash Flow', type: 'number_or_field', required: true, placeholder: '40000' },
      { key: 'cashflow_4', label: 'Year 4 Cash Flow', type: 'number_or_field', required: false, placeholder: '25000' },
      { key: 'cashflow_5', label: 'Year 5 Cash Flow', type: 'number_or_field', required: false, placeholder: '20000' },
    ],
    outputs: [
      { key: 'npv', label: 'Net Present Value', default: true },
      { key: 'irr', label: 'Internal Rate of Return', default: true },
      { key: 'create_txn', label: 'Create Transaction', default: true, txnType: 'NPV Analysis' },
    ],
    generateDSL: (config) => {
      const investment = config.initial_investment_source === 'field' ? config.initial_investment_field : config.initial_investment;
      const rate = config.discount_rate_source === 'field' ? config.discount_rate_field : config.discount_rate;
      const cfKeys = ['cashflow_1', 'cashflow_2', 'cashflow_3', 'cashflow_4', 'cashflow_5'];
      const cfValues = cfKeys.map(k => config[`${k}_source`] === 'field' ? config[`${k}_field`] : config[k]).filter(Boolean);

      let lines = [];
      lines.push('## ═══════════════════════════════════════════════════════════════');
      lines.push('## NET PRESENT VALUE ANALYSIS');
      lines.push('## ═══════════════════════════════════════════════════════════════');
      lines.push('');
      lines.push(`initial_investment = ${investment}`);
      lines.push(`discount_rate = divide(${rate}, 100)`);

      // Create intermediate variables for cashflows so event field references resolve
      const cfVarNames = [];
      cfValues.forEach((val, idx) => {
        const varName = `cf_year_${idx + 1}`;
        lines.push(`${varName} = ${val}`);
        cfVarNames.push(varName);
      });
      lines.push(`cashflows = [multiply(-1, initial_investment), ${cfVarNames.join(', ')}]`);
      lines.push('');
      if (config.outputs_npv) {
        lines.push('net_pv = npv(discount_rate, cashflows)');
        lines.push('print("Net Present Value:", net_pv)');
        if (config.outputs_create_txn) {
          lines.push(`createTransaction(postingdate, postingdate, "${config.txn_type || 'NPV Analysis'}", net_pv)`);
        }
      }

      if (config.outputs_irr) {
        lines.push('');
        lines.push('internal_rr = irr(cashflows)');
        lines.push('print("Internal Rate of Return:", internal_rr)');
      }

      return lines.join('\n');
    },
  },

  {
    id: 'lease_accounting',
    title: 'Lease Amortization (ASC 842)',
    description: 'Calculate right-of-use asset amortization and lease liability for operating leases',
    category: 'Leases',
    icon: 'Building',
    standard: 'ASC 842',
    fields: [
      { key: 'lease_payment', label: 'Monthly Lease Payment', type: 'number_or_field', required: true, placeholder: '5000' },
      { key: 'lease_term', label: 'Lease Term (months)', type: 'number_or_field', required: true, placeholder: '36' },
      { key: 'discount_rate', label: 'Incremental Borrowing Rate (%)', type: 'number_or_field', required: true, placeholder: '5' },
      { key: 'start_date', label: 'Lease Commencement Date', type: 'date_or_field', required: true, placeholder: '2026-01-01' },
    ],
    outputs: [
      { key: 'schedule', label: 'Lease Amortization Schedule', default: true },
      { key: 'rou_asset', label: 'ROU Asset Value', default: true },
      { key: 'create_txn', label: 'Create Transaction', default: false, txnType: 'Lease Expense' },
    ],
    generateDSL: (config) => {
      const payment = config.lease_payment_source === 'field' ? config.lease_payment_field : config.lease_payment;
      const term = config.lease_term_source === 'field' ? config.lease_term_field : config.lease_term;
      const rate = config.discount_rate_source === 'field' ? config.discount_rate_field : config.discount_rate;
      const startDate = config.start_date_source === 'field' ? config.start_date_field : `"${config.start_date}"`;

      let lines = [];
      lines.push('## ═══════════════════════════════════════════════════════════════');
      lines.push('## ASC 842 OPERATING LEASE AMORTIZATION');
      lines.push('## ═══════════════════════════════════════════════════════════════');
      lines.push('');
      lines.push(`monthly_payment = ${payment}`);
      lines.push(`lease_term = ${term}`);
      lines.push(`annual_rate = divide(${rate}, 100)`);
      lines.push('monthly_rate = divide(annual_rate, 12)');
      lines.push('');
      lines.push('## Calculate present value of lease payments (ROU Asset / Lease Liability)');
      lines.push('rou_asset = abs(pv(monthly_rate, lease_term, monthly_payment))');
      lines.push('print("ROU Asset / Initial Lease Liability:", rou_asset)');
      lines.push('');
      lines.push('## Generate lease amortization schedule');
      lines.push(`end_date = add_months(${startDate}, lease_term)`);
      lines.push(`p = period(${startDate}, end_date, "M")`);
      lines.push('');
      lines.push('sched = schedule(p, {');
      lines.push('    "date": "period_date",');
      lines.push('    "opening_liability": "lag(\'closing_liability\', 1, rou_asset)",');
      lines.push('    "interest_expense": "multiply(opening_liability, monthly_rate)",');
      lines.push('    "principal_reduction": "subtract(monthly_payment, interest_expense)",');
      lines.push('    "closing_liability": "subtract(opening_liability, principal_reduction)",');
      lines.push('    "rou_amortization": "divide(rou_asset, lease_term)"');
      lines.push('}, {"rou_asset": rou_asset, "monthly_rate": monthly_rate, "monthly_payment": monthly_payment, "lease_term": lease_term})');
      lines.push('');
      lines.push('print(sched)');

      if (config.outputs_create_txn) {
        lines.push('');
        lines.push('interest_for_postingdate = schedule_filter(sched, "date", postingdate, "interest_expense")');
        lines.push('print("Lease expense for posting date:", interest_for_postingdate)');
        lines.push(`createTransaction(postingdate, postingdate, "${config.txn_type || 'Lease Expense'}", interest_for_postingdate)`);
      }

      return lines.join('\n');
    },
  },
];

export default ACCOUNTING_TEMPLATES;

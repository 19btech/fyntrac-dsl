import React from "react";
import { Card, CardContent, Button, Box, Typography, Chip, Tooltip } from '@mui/material';
import { Code, Lightbulb, Copy } from "lucide-react";
import { useToast } from "./ToastProvider";

const DSL_EXAMPLES = [
  {
    id: 1,
    title: "Compound Interest with Transaction",
    description: "Calculate compound interest and create a transaction",
    category: "Interest",
    dslCode: `## Calculate compound interest
interest = compound_interest(10000, 0.05, 2)
new_balance = capitalization(interest, 10000)

## Create the transaction
createTransaction("2026-12-31", "2026-12-31", "Compound Interest", interest)`,
    fields: []
  },
  {
    id: 2,
    title: "Monthly Loan Payment",
    description: "Calculate fixed monthly payment for a loan",
    category: "Loans",
    dslCode: ` ## Calculate monthly payment
  monthly_rate = divide(0.045, 12)
  payment = pmt(monthly_rate, 360, 250000)

  ## Create the transaction
  createTransaction("2026-12-31", "2026-12-31", "Monthly Payment", abs(payment))`,
    fields: []
  },
  {
    id: 3,
    title: "Present Value Calculation",
    description: "Calculate present value of a future amount",
    category: "Financial",
    dslCode: `## Calculate present value
  discount_rate = divide(0.05, 12)
  present_val = pv(discount_rate, 12, 0, 10000)

  ## Create the transaction
  createTransaction("2026-12-31", "2026-12-31", "Present Value", abs(present_val))`,
    fields: []
  },
  {
    id: 4,
    title: "Straight Line Depreciation",
    description: "Calculate annual depreciation using straight line method",
    category: "Depreciation",
    dslCode: `## Calculate depreciation
  annual_depreciation = straight_line(50000, 5000, 5)

  ## Create the transaction
  createTransaction("2026-12-31", "2026-12-31", "Depreciation Expense", annual_depreciation)`,
    fields: []
  },
  {
    id: 5,
    title: "Interest with Day Count",
    description: "Calculate interest using ACT/360 convention",
    category: "Interest",
    dslCode: `## Calculate days and interest
  days = days_between("2026-01-01", "2026-01-31")
  interest = interest_on_balance(10000, 0.05, days)

  ## Create the transaction
  createTransaction("2026-01-31", "2026-01-31", "Accrued Interest", interest)`,
    fields: []
  },
  {
    id: 6,
    title: "Conditional Payment Logic",
    description: "Apply payment only if balance is positive",
    category: "Logic",
    dslCode: `## Calculate payment with condition
  payment_due = pmt(0.05, 12, 1000)
  actual_payment = iif(gt(500, 0), abs(payment_due), 0)

  ## Create the transaction (only if payment > 0)
  createTransaction("2026-01-31", "2026-01-31", "Conditional Payment", actual_payment)`,
    fields: []
  },
  {
    id: 7,
    title: "Prorated Allocation",
    description: "Allocate amount proportionally across partial period",
    category: "Allocation",
    dslCode: `## Calculate prorated amount
  days_in_period = days_between("2026-01-01", "2026-01-15")
  prorated_amount = prorate(12000, days_in_period, 360)

  ## Create the transaction
  createTransaction("2026-01-15", "2026-01-15", "Prorated Allocation", prorated_amount)`,
    fields: []
  },
  {
    id: 8,
    title: "Net Present Value Analysis",
    description: "Calculate NPV of cash flow series",
    category: "Financial",
    dslCode: `## Define cashflows and calculate NPV
  cashflows = [-100000, 30000, 40000, 50000]
  net_pv = npv(0.08, cashflows)

  ## Create the transaction
  createTransaction("2026-12-31", "2026-12-31", "NPV Analysis", net_pv)`,
    fields: []
  },
  {
    id: 9,
    title: "Currency Conversion",
    description: "Convert amount from one currency to another",
    category: "Conversion",
    dslCode: `## Convert currency
  converted = fx_convert(1000, 1.12)
  rounded = round(converted, 2)

  ## Create the transaction
  createTransaction("2026-06-30", "2026-06-30", "FX Conversion", rounded)`,
    fields: []
  },
  {
    id: 10,
    title: "Clamped Rate Interest",
    description: "Apply rate with minimum and maximum bounds",
    category: "Logic",
    dslCode: `## Clamp rate within bounds
  clamped_rate = clamp(0.06, 0.03, 0.08)
  interest = multiply(100000, clamped_rate)

  ## Create the transaction
  createTransaction("2026-12-31", "2026-12-31", "Bounded Rate Interest", interest)`,
    fields: []
  },
  {
    id: 12,
    title: "Loan Amortization Schedule",
    description: "Generate loan payment schedule with interest and principal",
    category: "Schedule",
    dslCode: `## Define loan parameters
loan_amount = 100000
annual_rate = 0.06
monthly_rate = divide(annual_rate, 12)
loan_term_months = 12

## Calculate fixed monthly payment
monthly_payment = abs(pmt(monthly_rate, loan_term_months, loan_amount))

## Create schedule period
p = period("2026-01-01", "2026-12-01", "M")

## Generate amortization schedule
sched = schedule(p, {
    "date": "period_date",
    "opening_bal": "lag('closing_bal', 1, loan_amount)",
    "interest": "multiply(opening_bal, monthly_rate)",
    "principal": "subtract(monthly_payment, interest)",
    "closing_bal": "subtract(opening_bal, principal)"
},{"loan_amount": loan_amount,"monthly_rate":monthly_rate,"monthly_payment":monthly_payment})

## Get totals
total_interest = schedule_sum(sched, "interest")

## Execute Results
print(sched)
print("Total_Interest",total_interest)`,
    fields: ["postingdate (date)", "effectivedate (date)"]
  }
];


const DSLExamples = ({ onLoadExample }) => {
  const [selectedCategory, setSelectedCategory] = React.useState("All");
  const toast = useToast();

  const categories = ["All", ...new Set(DSL_EXAMPLES.map(ex => ex.category))];

  const filteredExamples = selectedCategory === "All" 
    ? DSL_EXAMPLES 
    : DSL_EXAMPLES.filter(ex => ex.category === selectedCategory);

  const handleCopyCode = (code) => {
    navigator.clipboard.writeText(code);
    toast.success("Code copied to clipboard!");
  };

  return (
    <Box sx={{ p: 3, bgcolor: '#F8F9FA', minHeight: '100%' }} data-testid="dsl-examples">
      <Box sx={{ mb: 3 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 1 }}>
          <Lightbulb size={24} color="#5B5FED" />
          <Typography variant="h3">Calculation Examples</Typography>
        </Box>
        <Typography variant="body2" color="text.secondary">
          Ready-to-use examples for common financial calculations
        </Typography>
      </Box>

      <Box sx={{ mb: 3, display: 'flex', flexWrap: 'wrap', gap: 1 }}>
        {categories.map(category => (
          <Chip
            key={category}
            label={category}
            onClick={() => setSelectedCategory(category)}
            color={selectedCategory === category ? "primary" : "default"}
            sx={{ 
              cursor: 'pointer',
              bgcolor: '#FFFFFF',
              color: selectedCategory === category ? '#14213d' : '#495057',
              border: 'none',
              boxShadow: 'none',
              outline: 'none',
              '&:hover': {
                bgcolor: '#F8F9FA',
              },
              '&:focus, &:focus-visible': {
                outline: 'none',
                boxShadow: 'none',
              }
            }}
            data-testid={`example-category-${category}`}
          />
        ))}
      </Box>

      <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', lg: 'repeat(2, 1fr)' }, gap: 2.5 }}>
        {filteredExamples.map((example) => (
          <Card key={example.id} data-testid={`example-${example.id}`}>
            <CardContent sx={{ p: 2.5 }}>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 2 }}>
                <Box sx={{ flex: 1 }}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
                    <Code size={16} color="#5B5FED" />
                    <Typography variant="h6">{example.title}</Typography>
                  </Box>
                  <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                    {example.description}
                  </Typography>
                  <Chip 
                    label={example.category} 
                    size="small"
                    sx={{ 
                      bgcolor: '#EEF0FE', 
                      color: '#5B5FED',
                      fontSize: '0.75rem',
                      height: 20
                    }}
                  />
                </Box>
                <Tooltip title="Copy">
                  <span>
                    <Button
                      size="small"
                      onClick={() => handleCopyCode(example.dslCode)}
                      sx={{ minWidth: 'auto', p: 1 }}
                      data-testid={`copy-example-${example.id}`}
                    >
                      <Copy size={16} />
                    </Button>
                  </span>
                </Tooltip>
              </Box>

              <Box sx={{ 
                bgcolor: '#212529', 
                borderRadius: 1, 
                p: 2, 
                mb: 2,
                fontFamily: 'monospace',
                fontSize: '0.75rem',
                color: '#F8F9FA',
                lineHeight: 1.6,
                maxHeight: 200,
                overflow: 'auto'
              }}>
                <pre style={{ margin: 0, whiteSpace: 'pre-wrap' }}>{example.dslCode}</pre>
              </Box>

              <Button
                variant="contained"
                size="small"
                fullWidth
                onClick={() => {
                  onLoadExample(example.dslCode);
                  toast.success(`Loaded: ${example.title}`);
                }}
                data-testid={`load-example-${example.id}`}
              >
                Load into Editor
              </Button>
            </CardContent>
          </Card>
        ))}
      </Box>
    </Box>
  );
};

export default DSLExamples;
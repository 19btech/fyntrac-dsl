## REPLAY schedule example

## IMPORTED EVENT FIELDS
posting_date = normalize_date(EOD.postingdate)
effective_date = normalize_date(EOD.effectivedate)
subinstrumentid = EOD.subinstrumentid
interest_rate = (EOD.INTEREST_RATE/100)
UPB = EOD.UPB
AIR = EOD.AIR

## PMT
payment_date = PMT.effectivedate
payment_amount = PMT.REMIT

## REPLAY arrays (collect then normalize)
replay_date = normalize_date(array_get(collect_by_instrument(REPLAY.effectivedate), 0, ''))
timeline_array = collect_by_instrument(REPLAY.effectivedate)
time = normalize_arraydate(coalesce(timeline_array, []))
timeline_list = array_append(time, posting_date)

# Ensure replay fields are arrays; append scalar fallback so indexing is safe
replay_upb = array_append(collect_by_instrument(REPLAY.REP_UPB), coalesce(REPLAY.REP_UPB, 0))
replay_air = array_append(collect_by_instrument(REPLAY.REP_AIR), coalesce(REPLAY.REP_AIR, 0))
replay_remit = array_append(collect_by_instrument(REPLAY.REP_REMIT), coalesce(REPLAY.REP_REMIT, 0))

COLUMNS = {
  "date" : "timeline_list",
  "Beg_UPB": "iif(eq(period_index, 0), coalesce(array_get(replay_upb, period_index, 0), 0), coalesce(lag('End_UPB', 1, 0), 0))",
  "Beg_AIR": "coalesce(array_get(replay_air, period_index, 0), 0)",
  "Remit_am": "coalesce(array_get(replay_remit, period_index, 0), 0)",
  "Int_Paid" : "min(Remit_am,Beg_AIR)",
  "Prin_Paid" : "max(subtract(Remit_am,Int_Paid),0)",
  "End_UPB" : "subtract(coalesce(Beg_UPB,0),coalesce(Prin_Paid,0))",
  "Accrual" : "divide(multiply(interest_rate, coalesce(End_UPB, 0)), 360)",
  "End_AIR" : "subtract(add(coalesce(Beg_AIR, 0), coalesce(Accrual, 0)), coalesce(Int_Paid, 0))"

}

REPLAY_SCHEDULE = schedule(
  COLUMNS,
  {
    "timeline_list": timeline_list,
    "replay_upb" : replay_upb,
    "replay_air" : replay_air,
    "replay_remit": replay_remit,
    "interest_rate": interest_rate,
    "replay_date" : replay_date
  }
)

print(REPLAY_SCHEDULE)

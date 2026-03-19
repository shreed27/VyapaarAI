# UPI Gateway Timeout

## Error codes: U30, TIMEOUT, RRN_NOT_FOUND

## What happened
The UPI payment request was sent but the bank server did not respond within the allowed time window (30 seconds). This does not mean the money was deducted — in most cases the transaction is in a pending state.

## Why it happens
- Bank server under heavy load (especially during peak hours 10 AM-12 PM and 6-9 PM)
- Network connectivity issues between payment switch and bank
- Bank maintenance window

## Resolution steps
1. Check your bank account balance — if it has not changed, the money was NOT deducted
2. Wait 30 minutes — most timeouts auto-reverse within this window
3. If balance was deducted but payment not received: raise a dispute via Paytm app → Help → Dispute a transaction
4. Expected reversal time: 2-4 hours for auto-reversal, up to 5 business days for manual

## Prevention
Avoid making large payments during 10 AM-12 PM and 6-9 PM peak hours

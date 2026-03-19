# Duplicate Transaction Suspected

## Error codes: DUPLICATE_TXN, SAME_RRN, REQUEST_ALREADY_PROCESSED

## What happened
The system detected that the same payment request may have been submitted more than once. This protection is designed to stop accidental double charges.

## Why it happens
- User tapped "Pay" multiple times due to slow network
- Merchant app retried the same request while the first attempt was still processing
- Bank or PSP received the same UPI reference request twice

## Resolution steps
1. Check transaction history for two entries with the same amount around the same time
2. Compare UTR / RRN numbers — if they are identical, it is usually the same request replayed
3. If one payment succeeded and another is pending: wait 2 hours before retrying
4. If two separate successful debits happened: raise a dispute in Paytm app → Help → Dispute a transaction

## Prevention
Do not tap the payment button repeatedly. Wait for a final success, failure, or pending status before retrying.

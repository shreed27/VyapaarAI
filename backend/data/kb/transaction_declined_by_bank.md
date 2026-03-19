# Transaction Declined by Bank

## Error codes: U29, ISSUER_DECLINED, BANK_DECLINED

## What happened
The payment request reached the bank, but the bank declined it based on its internal checks. Usually no money is deducted, though a temporary hold is possible in some edge cases.

## Why it happens
- Bank risk engine flagged the payment as unusual
- Daily velocity rules were triggered
- Beneficiary or merchant category is temporarily restricted
- Bank system could not validate the request fields

## Resolution steps
1. Check whether the bank account balance changed or only a temporary hold was created
2. Retry once after 10-15 minutes if the payment was genuine
3. If still declined: contact the bank and ask why the UPI transaction was rejected
4. Use another linked bank account for urgent payments

## Important note
If the bank confirms the transaction was blocked for risk reasons, only the bank can remove that restriction.

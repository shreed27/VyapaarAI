# Payment Link Expired

## Error codes: LINK_EXPIRED, SESSION_TIMEOUT, REQUEST_EXPIRED

## What happened
The payment link or payment session is no longer valid. These links often have a limited time window for security reasons.

## Why it happens
- Merchant created a short-duration payment link
- User opened an old SMS or WhatsApp payment request
- Browser or app session stayed idle for too long

## Resolution steps
1. Ask the sender or merchant to generate a fresh payment link
2. Do not reuse the old link even if it opens again
3. Verify the amount and merchant name before paying on the new link
4. If money was deducted from the old attempt, check transaction history before retrying

## Prevention
Complete payment links soon after opening them, especially for limited-time invoices

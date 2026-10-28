# Fixture Guide

The candidate assessment refers to the files below. Candidates may add fields, but their service must accept the supplied valid payloads without modification.

Use the synthetic local webhook secret `assessment-secret` when exercising the examples. It is not a real credential.

## Suggested processing sequence

| Step | File | Expected outcome |
| --- | --- | --- |
| 1 | `01_normal_stock.json` | Store the observation and current state. Do not create a replenishment request. |
| 2 | `02_low_stock.json` | Store the observation, set the current quantity to 4, and create one request for 8 units. |
| 3 | `03_duplicate_low_stock.json` | Treat as a redelivery of step 2. Do not create another logical observation effect, request, or ERP order. |
| 4 | `04_out_of_order.json` | Retain the older observation in history. Do not replace the current state or create another request. |
| 5 | `05_restocked.json` | Set the current quantity to 12. Record that inventory has recovered. |
| 6 | `06_new_depletion.json` | Set the current quantity to 3. A new replenishment cycle may begin after the prior request is terminal and restocking has been observed. The candidate must document the exact eligibility rule. |
| 7 | `07_invalid.json` | Reject with clear validation errors. Do not partially persist or trigger outbound work. |
| 8 | `08_concurrent_a.json` and `09_concurrent_b.json` | Submit concurrently. Retain both unique events, select the later observation as current, and create at most one active request for 8 units. |

## Webhook authentication

Send these headers:

```text
Content-Type: application/json
X-Webhook-Timestamp: <unix-seconds>
X-Webhook-Signature: v1=<lowercase-hex-hmac-sha256>
```

The signed message is:

```text
<timestamp>.<exact raw request body bytes>
```

The candidate should use constant-time signature comparison and reject stale timestamps according to a documented replay window. `tools/sign_payload.py` generates the two authentication headers.

## Mock ERP behavior

`fixtures/outbound/erp_scenarios.json` defines six responses the candidate may implement with a local mock, test double, or lightweight endpoint. The important ambiguous case is `timeout_after_accept`: the ERP logically accepts the order but the caller does not receive the response. A safe retry must reuse the same idempotency key.

## Clarifications

- `observed_at` determines inventory event order. Arrival order does not.
- If two events have identical `observed_at` values, the candidate must document a deterministic tie-breaker.
- A low-stock event must not create a second active request while one already exists for the same facility, room, and SKU.
- Exactly-once network delivery is not assumed. Both sides should use stable idempotency identifiers.


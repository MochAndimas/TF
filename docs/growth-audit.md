# Dashboard growth audit

## Coverage

Audited metric-card growth in Overview, User Acquisition, Brand Awareness,
Remarketing, First Deposit UA/BA, Remarketing Deposit, Install, Internal Register,
Login Activity, Subscription, and Instagram/YouTube/TikTok/Facebook account and
revenue sections. Media cards without growth remain unchanged.

## Comparison policy

- This Month: first day through the selected day versus the same dates in the
  preceding month, capped at that month's final day.
- Last Month: the entire preceding calendar month.
- Last 7/30 Day and Custom Range: the immediately preceding inclusive window of
  equal length. Custom ranges are not guessed to be calendar-month presets.
- Direct API callers can select `comparison=month_to_date`, `previous_month`, or
  `previous_period`; omission preserves the equal-length default. The existing
  social `revenue_comparison` parameter remains supported.

The Streamlit filter sets the request's comparison mode. Cache keys include that
mode and a version so old responses and identical dates with different presets
are not mixed. Pure ASGI middleware scopes the selection using a ContextVar and
resets it after every request; concurrent requests do not share a selection.
All analytics date-window helpers use `app/utils/period_comparison.py`.

## Corrections

- Social account growth previously split rows in the current period in half.
  It now reads the actual preceding period and compares the same summary metric
  shown on the card. Follower snapshot growth is calculated, not hardcoded to 0.
- Calendar presets now apply to all metric-card groups, including services using
  shared campaign/overview/deposit helpers.
- Subscription excludes rows in the gap between the prior month-to-date window
  and the current window. Its previous latest-day counts obey the same boundary.
- Signed net metrics use `(current - previous) / abs(previous) * 100` to avoid
  reversing the direction when the previous value is negative.
- Combined deposit AOV and install ratios are recomputed per period before
  comparing, rather than averaging constituent growth percentages.
- Growth labels show actual comparison dates from response metadata.

Existing zero-baseline convention is preserved: zero to zero = 0%; zero to a
positive value = +100%; zero to a negative value = -100%. These are display
conventions, not a mathematically defined percentage change from zero.
Subscription keeps unavailable-period growth unset.

## Validation (2026-09-18)

85 selected tests passed after the final calculation and label changes. Coverage
includes platform isolation, full-period account summaries, all shared date
helpers, leap years, month/year boundaries, subscription gap exclusion,
negative/zero baselines, combined AOV, request isolation, and frontend cache/API
comparison propagation.

Read-only checks against the configured local database succeeded for all four
social analytics payloads plus Install and Subscription. With 1–18 September
2026 selected as This Month, their comparison resolves to 1–18 August 2026.

The complete existing suite is not green: initial collection stops in two legacy
test modules importing unavailable `ALLOWED_ROLES`. Excluding those files yields
192 passed and 19 failed. Remaining failures concern existing ETL fixtures/API
expectations, Google Ads authentication, navigation expectations, and auth
response/error expectations; they are outside this growth change. No live ETL,
production deployment, or source-data rewrite was performed during this audit.

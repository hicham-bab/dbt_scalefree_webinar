# Data caveats (verified)

Every number on this page was computed from the seed files in `platform/seeds/` by
reproducing the logic of the platform's intermediate and mart models (DuckDB, 2026-10-30).
The raw vault is loaded 1:1 from those seeds, so the built marts return the same numbers.
If you change the seeds, re-run the profiling and update this page.

## The data at a glance

| Table | Rows | Notes |
|---|---|---|
| `raw_orders` | 1,500 | 2023-01-01 to 2026-03-30, 150 ordering customers |
| `raw_order_items` | 2,355 | Every order has at least one item; no orphans |
| `raw_customers` | 165 | 15 customers never ordered (`customer_status = 'prospect'`) |
| `raw_products` | 50 | 3 inactive |
| `raw_payments` | 1,500 | Exactly one per order |
| `raw_shipments` | 1,368 | One per shipped order; 132 orders have none |
| `raw_returns` | 152 | One per returned order |
| `raw_campaign_attributions` | 648 | One row per attributed order, three attribution models |
| `raw_web_events` | 2,588 | Every event has its own `session_id` |
| `raw_product_reviews` | 320 | Not modeled yet: the source for the live dev workflow |

Keys are clean: no order without a customer, no item without an order or product, no
review without a customer or product.

## Revenue

| Fact | Value |
|---|---|
| `total_revenue` (sum of `fct_orders.net_revenue_eur`) | EUR 1,254,555.81 |
| `total_gross_revenue` | EUR 1,274,855.81 |
| `total_discount_amount` | EUR 20,300.00 (`discount_rate` 1.59%) |
| `average_order_value` | EUR 836.37 |

- **Cancelled and pending orders are in `fct_orders` and in `total_revenue`.**
  32 cancelled (EUR 32,215.69) and 18 pending (EUR 14,693.01) orders add EUR 46,908.70.
  Customer lifetime value (`dim_customers.lifetime_value_eur`) excludes them, so the sum
  of LTV is EUR 1,207,647.11, not `total_revenue`. Say which one you used; filter
  `order_status` when the question is about realised revenue.
- **Discounts are applied twice by design.** Line revenue already applies the item's
  `discount_pct`; the order's `discount_amount` is then subtracted on top (302 items carry
  both). Five orders end with negative net revenue (for example `ORD_10127`, EUR -63.05).
- **Payments are not revenue.** Converted payments sum to EUR 1,372,898.14, about
  EUR 118,000 more than net revenue. Never compute revenue from `fct_payments`.
- **JPY amounts are on a euro scale.** 129 payments are in JPY with amounts like other
  currencies (average 760). `int_payment_summary` converts JPY at 1.0 because it is not in
  its rate table. Do not "fix" this with a real JPY rate: that would erase EUR 98,084.72.
- **2025 is a different scale.** Orders per year: 2023 203, 2024 187, 2025 925, 2026 185
  (January to March only). Year-over-year growth from 2024 to 2025 reflects how the seed
  data was extended, not the business; say so before quoting it. 2026 is a partial year.

## Customers

- `total_customers` is 165, including the 15 prospects. `avg_ltv` is EUR 7,319.07 over all
  165 customers and EUR 8,050.98 over the 150 who ordered.
- **Loyalty tier does not mean value.** Average LTV by tier: Bronze EUR 7,864.44,
  Silver 7,150.85, Gold 6,262.20, Platinum 5,637.01. Platinum customers are the least
  valuable on average. Never assume tier order.
- `customer_status` (active, at_risk, churned, prospect) and `days_since_last_order` are
  computed against `current_date()` when the model runs. Their counts change every day
  with no new data; give the run date with any answer about them.
- `is_first_order` marks one order per customer: `new_customer_orders` is 150.

## Fulfilment and returns

- **Two meanings of "late".** `is_late_delivery` (and `late_delivery_orders`) means more
  than 7 days between shipping and delivery: 114 orders. Delivered after the estimated
  delivery date: 561 orders. Use the metric and say "more than 7 days in transit".
- `late_delivery_rate` divides by all 1,500 orders (7.6%), including the 327 that were
  not delivered. Over delivered orders only it is 9.7%.
- `avg_days_to_ship` is 1.83 because unshipped orders count as 0 days
  (`coalesce(days_to_ship, 0)`). Over the 1,368 shipped orders it is 2.01.
- **Two meanings of "returned".** `has_returned` (and `returned_order_count`) means a
  return record exists: 152 orders. `order_status = 'returned'` covers only 15 of them.
  Use the metric.

## Marketing and web

- Each attributed order appears under exactly one `attribution_model` (first_touch 188,
  last_touch 285, linear 175). `total_attributed_revenue` across all models (EUR 564,122.51)
  mixes three methods: filter one model for any comparison between campaigns.
- Campaign `budget_eur` totals EUR 483,000 across 25 campaigns; ROAS and CPA in
  `fct_campaign_performance` use estimated spend, not invoices.
- **Web events cannot be turned into funnels by session.** Each of the 2,588 events has
  its own `session_id`, 783 have no `customer_id`, and 345 `checkout_complete` events do
  not reconcile with 1,500 orders. Report event counts, not conversion rates.

## Products and inventory

- `avg_product_margin` is the unweighted average of list-price margins across the 50
  products (65.01%). It is not realised margin: it ignores discounts and sales mix.
- Bags are 58% of item revenue (EUR 735,037.18 of 1,274,855.81).
- Inventory has 30 movements: 12 receipts (1,230 units), 14 shipments (285 units),
  2 adjustments, 2 return restocks. `net_stock_change` with no time filter is stock on hand.

## Product reviews (unmodeled)

- 320 reviews, 2023-02-16 to 2026-05-02, average rating 4.04; 300 are verified purchases,
  and every verified review matches an order line for that customer and product.
- 15 reviews are dated after the last order (2026-03-30).

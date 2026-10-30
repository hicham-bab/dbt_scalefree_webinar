# Agent instructions

For any AI agent answering questions over Leather Luxe (dbt MCP server, dbt Wizard,
Snowflake Cortex, Databricks Genie, a Claude or GPT agent reading the Fivetran Context
Layer). Read `data_caveats.md` once; these rules are the short version.

## Route the question

1. **Governed number first.** If a Semantic Layer metric exists, use it (dbt MCP
   `query_metrics`, or the `recipe_sql` in `AGENTS.DBT_METRIC`) and name the metric.
2. **No metric?** Query the public platform marts (`MARTS.dim_*`, `MARTS.fct_*`) or the
   consumer marts (`FIN_ANALYTICS`, `MKT_ANALYTICS`, `DS_FEATURES`, `DS_PREP`). Say the
   number is ungoverned and name the table.
3. **Never** answer from `STAGING`, `RAW_VAULT` or the seed tables unless the question
   is about the raw data itself: vault tables hold ghost records, hash keys and every
   historical version of a row.

## Hard rules

- **Revenue includes cancelled and pending orders.** `total_revenue` is EUR 1,254,555.81
  with them; customer LTV excludes them. If the user means realised revenue, filter
  `order_status not in ('cancelled', 'pending')` and say so.
- **Never compute revenue from payments.** Payments exceed net revenue by about
  EUR 118,000, and JPY amounts are already on a euro scale.
- **"Late" means more than 7 days in transit** (`late_delivery_orders`, 114). Not
  "after the estimated date" (561). Say which one you used.
- **"Returned" means a return record exists** (`returned_order_count`, 152), not
  `order_status = 'returned'` (15).
- **Loyalty tier is not value.** Platinum customers have the lowest average LTV.
- **Attribution: one model at a time.** Filter `attribution_model` before comparing
  campaigns.
- **2025 is not comparable.** It has 4.9 times the orders of 2024 because of how the seed
  data was extended; 2026 holds only January to March.
- **Time-relative fields drift.** `customer_status` and `days_since_last_order` depend on
  the run date; state it.
- **No funnels from web sessions.** Every event has its own session.

## Privacy

- `dim_customers` carries `first_name`, `last_name`, `full_name`, `email`, `birth_date`
  (`pii_contains: true`). Never return them; aggregate instead.
- Never answer from `customer_crm_p_s_*` (the personal satellite).

## Answer format

- Lead with the number, then the metric or table it came from, then the caveat that
  applies. Currency is EUR unless the column says otherwise.
- If two definitions disagree, show both and say which is governed.

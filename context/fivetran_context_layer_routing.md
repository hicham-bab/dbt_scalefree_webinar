
## PROJECT = '{project_key}'
Leather Luxe, a luxury leather goods e-commerce brand: a dbt Mesh of four projects (platform, finance, marketing, data_science) built as a Data Vault 2.0 with datavault4dbt. 1,500 orders from 2023-01-01 to 2026-03-30, 165 customers, 50 products, revenue in EUR. Gold marts: {gold_schema}; consumer marts in MARKETING__MARTS (FIN_ANALYTICS, MKT_ANALYTICS) and DATA_SCIENCE__MARTS.
{n_models} models, {n_columns} documented columns, {n_edges} lineage edges, {n_metrics} metrics. Raw copy: {raw_schema}.
- total_revenue includes cancelled and pending orders (EUR 46,908.70); customer LTV excludes them. Say which one you used.
- Never compute revenue from payments: they exceed net revenue by about EUR 118,000, and JPY amounts are already on a euro scale.
- Late delivery means more than 7 days in transit (114 orders), not after the estimated date (561).
- Returned orders means a return record exists (152), not order_status = 'returned' (15).
- Loyalty tier is not value: Platinum has the lowest average LTV.
- 2025 holds almost 5x the orders of 2024 (seed extension) and 2026 only Q1: do not present YoY growth as business performance.
- Filter attribution_model before comparing campaigns. Web events have one session per event: no session funnels.
- Never return names, emails, phone numbers or birth dates from dim_customers.

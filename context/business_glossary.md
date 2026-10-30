# Business glossary

| Term | Definition | Where it lives |
|---|---|---|
| Order | A customer purchase, one row in `fct_orders` (all statuses, including cancelled and pending) | `fct_orders` |
| Gross revenue | Sum of line revenue: quantity x unit price at order x (1 - item discount %) | `gross_revenue_eur`, `total_gross_revenue` |
| Net revenue | Gross revenue minus the order-level `discount_amount`. Can be negative (5 orders) | `net_revenue_eur`, `total_revenue` |
| Realised revenue | Net revenue of orders not cancelled or pending. Not a metric: filter `order_status` | `fct_orders` |
| Average order value (AOV) | Average net revenue per order, all statuses (EUR 836.37) | `average_order_value` |
| Discount rate | Order-level discounts / gross revenue (1.59%) | `discount_rate` |
| Lifetime value (LTV) | Net revenue of a customer's orders, excluding cancelled and pending | `dim_customers.lifetime_value_eur`, `avg_ltv` |
| New customer order | The customer's first order by date | `is_first_order`, `new_customer_orders` |
| Returned order | An order with at least one return record | `has_returned`, `returned_order_count` |
| Late delivery | Delivered more than 7 days after shipping | `is_late_delivery`, `late_delivery_rate` |
| Customer status | active (ordered in the last 90 days), at_risk (91-365), churned (365+), prospect (never ordered), relative to the run date | `dim_customers.customer_status` |
| Loyalty tier | Bronze, Silver, Gold, Platinum, from the CRM. Not ranked by value | `dim_customers.loyalty_tier` |
| Attribution model | first_touch, last_touch or linear credit for an order's revenue | `fct_campaign_performance.attribution_model` |
| ROAS / CPA | Attributed revenue / estimated spend; estimated spend / conversions | `avg_roas`, `avg_cpa` |
| Product margin | (unit price - cost price) / unit price, list prices | `dim_products.margin_pct`, `avg_product_margin` |
| Net stock change | Signed sum of inventory movements; with no time filter, stock on hand | `net_stock_change` |
| Hub, link, satellite | Data Vault 2.0 entities built with datavault4dbt in `02_raw_vault`; business keys, relationships and history | `platform/models/02_raw_vault` |

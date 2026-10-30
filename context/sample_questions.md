# Sample questions with verified answers

Use them as the demo script and as an evaluation set for any agent. Answers were
computed from the seeds with the marts' logic (see `data_caveats.md`).

| # | Question | Verified answer | The trap |
|---|---|---|---|
| 1 | What is our total revenue? | EUR 1,254,555.81 (`total_revenue`) | Includes EUR 46,908.70 from cancelled and pending orders; realised revenue is EUR 1,207,647.11 |
| 2 | How many orders do we have? | 1,500 (`total_orders`) | All statuses |
| 3 | What is our average order value? | EUR 836.37 (`average_order_value`) | EUR 832.86 without cancelled and pending orders |
| 4 | What is our discount rate? | 1.59% (`discount_rate`) | Item-level discounts are already inside gross revenue |
| 5 | What share of orders is delivered late? | 7.6% (`late_delivery_rate`, 114 orders) | Late means more than 7 days in transit; 561 arrived after the estimated date; 9.7% of delivered orders |
| 6 | How many orders were returned? | 152 (`returned_order_count`) | Only 15 have `order_status = 'returned'` |
| 7 | Which loyalty tier is most valuable? | Bronze (average LTV EUR 7,864.44); Platinum is lowest (EUR 5,637.01) | Tier names suggest the opposite |
| 8 | What is the average customer lifetime value? | EUR 7,319.07 (`avg_ltv`, 165 customers) | EUR 8,050.98 over the 150 who ordered |
| 9 | Which channel brings the most revenue? | website, EUR 383,579.62 (479 orders), then mobile_app EUR 324,390.23 | All statuses |
| 10 | Which country brings the most revenue? | France, EUR 486,695.81 | Shipping country, not billing |
| 11 | How did revenue grow from 2024 to 2025? | EUR 162,182.21 to EUR 785,438.62 | The jump reflects the seed extension, not the business; say so |
| 12 | What is total revenue from payments? | Do not use payments for revenue | Payments are about EUR 118,000 above net revenue |
| 13 | What is our average product margin? | 65.01% (`avg_product_margin`) | List-price average, not realised margin |
| 14 | How many new customers ordered? | 150 (`new_customer_orders`) | 15 customers never ordered |
| 15 | What is the conversion rate of web sessions? | Not answerable | Every event has its own session id |

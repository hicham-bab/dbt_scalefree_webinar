# Leather Luxe — dbt Mesh E-Commerce Project

A production-quality dbt Mesh project for a luxury leather goods e-commerce brand.
Built for a webinar on **AI Readiness: Governance, Structured Metadata, and the Semantic Layer**.

Stack: dbt Fusion + Snowflake + dbt Cloud (dbt Platform)

---

## Architecture Overview

```
                         RAW_DEV / RAW_PRD
                         (Snowflake schemas)
                                |
                                | seeds / source tables
                                v
              +------------------------------------------+
              |          PLATFORM (Producer)              |
              |                                          |
              |  seeds/          staging/               |
              |  raw_customers   stg_customers           |
              |  raw_products    stg_products            |
              |  raw_orders      stg_orders              |
              |  raw_order_items stg_order_items         |
              |  raw_payments    stg_payments            |
              |  raw_shipments   stg_shipments           |
              |  raw_returns     stg_returns             |
              |  raw_campaigns   stg_campaigns           |
              |  raw_web_events  stg_web_events          |
              |                  stg_campaign_attr.      |
              |                        |                 |
              |                  intermediate/           |
              |                  int_orders_enriched     |
              |                  int_customer_order_sum  |
              |                  int_product_performance |
              |                  int_payment_summary     |
              |                  int_shipment_summary    |
              |                        |                 |
              |  [access: public]  marts/core/           |
              |                  dim_customers  (gold)   |
              |                  dim_products   (gold)   |
              |                  dim_date       (gold)   |
              |                  fct_orders     (gold)   |
              |                  fct_order_items(gold)   |
              |                  fct_payments   (gold)   |
              |                        |                 |
              |  semantic_models/                        |
              |  sem_orders / sem_customers / sem_products|
              |  metrics.yml (14 metrics)                |
              +------------------------------------------+
                    |              |              |
        +-----------+   +----------+   +----------+
        |               |              |
        v               v              v
  +----------+    +----------+   +---------------+
  | MARKETING|    | FINANCE  |   | DATA_SCIENCE  |
  | (Consumer|    |(Consumer)|   | (Consumer)    |
  |          |    |          |   |               |
  |customer_ |    |fct_revenue|  |customer_      |
  |segments  |    |fct_refunds|  |features       |
  |customer_ |    |monthly_   |  |product_       |
  |ltv       |    |revenue_   |  |affinity_      |
  |cohort_   |    |summary    |  |features       |
  |analysis  |    |product_   |  |order_sequence |
  |channel_  |    |margin     |  |features       |
  |attr.     |    |daily_flash|  |               |
  |campaign_ |    |country_   |  |churn_input    |
  |perf.     |    |revenue    |  |recommendation |
  |          |    |           |  |ltv_features   |
  +----------+    +----------+   +---------------+
```

---

## Projects

### platform (Producer)
The authoritative data platform. Owns all raw data, staging, intermediate models, and gold mart tables.

- **Seeds:** 9 CSV files (~3,000+ total rows) covering customers, products, orders, payments, shipments, returns, campaigns, attributions, and web events.
- **Staging:** 10 models — one per source table. Cast-based cleaning, no `::` casting.
- **Intermediate:** 5 ephemeral models for order enrichment, customer summaries, product performance, payment summaries, and shipment summaries.
- **Marts (core):** 6 public, contract-enforced gold tables: `dim_customers`, `dim_products`, `dim_date`, `fct_orders`, `fct_order_items`, `fct_payments`.
- **Semantic Layer:** 3 semantic models (`orders`, `customers`, `products`) + 14 metrics covering revenue, AOV, LTV, churn, discount rate, and more.
- **Governance:** Groups defined (`core`, `customer_data`, `product_data`). PII metadata on all customer-related models. Freshness SLA documented per table.

### marketing (Consumer)
- Reads platform public models via `{{ ref('platform', 'model_name') }}`.
- Builds: customer RFM segments, LTV analysis, cohort retention, channel attribution, campaign performance.
- Access: `protected` (internal to marketing).

### finance (Consumer)
- Reads platform public models via cross-project refs.
- Builds: daily revenue facts with MTD/YTD, refund analysis, monthly summary with MoM/YoY growth, product margin analysis, daily flash report, country breakdown.
- Access: `protected`.

### data_science (Consumer)
- Reads platform public models via cross-project refs.
- Builds: customer ML features, product affinity co-purchase matrix, order sequence time-series features, churn prediction input, recommendation user-item matrix, LTV feature set.
- Access: `protected`.

---

## Governance Features

### Data Contracts
All 6 platform mart models enforce contracts:
```yaml
config:
  contract:
    enforced: true
```
Every column has a `data_type` specified. Schema changes require explicit contract updates.

### Groups and Access
```yaml
# platform/models/marts/groups.yml
groups:
  - name: core         # owns the gold marts
  - name: customer_data
  - name: product_data
```

Mart models are `access: public` (cross-project refs allowed).
Consumer models are `access: protected` (cannot be referenced outside their project).

### Structured Metadata (meta blocks)
Every model has:
```yaml
meta:
  owner: <team>
  domain: <domain>
  tier: bronze | silver | gold
  pii_contains: true | false
  freshness_sla: realtime | hourly | daily | static
  semantic_layer_enabled: true | false
```

PII columns are individually tagged with `meta: {pii: true}`.

### Semantic Layer
The Semantic Layer enables Claude and other AI tools to query data via natural language without writing SQL.

Defined semantic models:
- `orders` — fct_orders with revenue measures and order dimensions
- `customers` — dim_customers with LTV and segmentation dimensions
- `products` — dim_products with catalog and performance dimensions

---

## How the Semantic Layer Enables Claude Natural Language Queries

With the dbt Semantic Layer connected to Claude (via the dbt MCP server), you can ask:

**Revenue questions:**
- "What was total revenue last month?"
- "Show me revenue by channel for Q4 2024."
- "Which country had the highest YoY revenue growth?"

**Customer questions:**
- "How many active customers do we have in France?"
- "What is the average LTV for Gold tier customers?"
- "What is the retention rate for the January 2024 cohort?"

**Product questions:**
- "Which product category has the highest return rate?"
- "Show me hero-tier products with margin over 60%."
- "What is the average basket size when a Haussmann bag is in the order?"

**Operational questions:**
- "What percentage of deliveries are late this month?"
- "What is the average days to ship by carrier?"
- "How many orders were refunded last week?"

---

## Setup Instructions

### Step 1: Snowflake Infrastructure

```sql
-- Run as ACCOUNTADMIN
-- Execute setup/snowflake_setup.sql in full
```

### Step 2: Configure dbt Profiles

Create profiles for each project in `~/.dbt/profiles.yml`:

```yaml
platform:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your_account>
      user: LEATHER_LUXE_DEV_SA
      private_key_path: <path_to_key>
      role: LEATHER_LUXE_DEV_SA_ROLE
      database: ECOMMERCE_DEV
      warehouse: LEATHER_LUXE_DEV_WH
      schema: analytics

marketing:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your_account>
      user: LEATHER_LUXE_DEV_SA
      private_key_path: <path_to_key>
      role: LEATHER_LUXE_DEV_SA_ROLE
      database: ECOMMERCE_DEV
      warehouse: LEATHER_LUXE_DEV_WH
      schema: mkt_analytics

finance:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your_account>
      user: LEATHER_LUXE_DEV_SA
      private_key_path: <path_to_key>
      role: LEATHER_LUXE_DEV_SA_ROLE
      database: ECOMMERCE_DEV
      warehouse: LEATHER_LUXE_DEV_WH
      schema: fin_analytics

data_science:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your_account>
      user: LEATHER_LUXE_DEV_SA
      private_key_path: <path_to_key>
      role: LEATHER_LUXE_DEV_SA_ROLE
      database: ECOMMERCE_DEV
      warehouse: LEATHER_LUXE_DEV_WH
      schema: ds_features
```

### Step 3: Environment Variables

| Variable | Description | Default |
|---|---|---|
| `DBT_TARGET_DB` | Target database name | `ECOMMERCE_DEV` |
| `DBT_RAW_DB` | Raw layer database name | `RAW_DEV` |
| `DBT_ENV_NAME` | Environment name — controls schema naming | `DEV` |

In dbt Cloud, set these as environment-level variables.

For local use:
```bash
export DBT_TARGET_DB=ECOMMERCE_DEV
export DBT_RAW_DB=RAW_DEV
export DBT_ENV_NAME=DEV
```

### Step 4: Load Seeds (Platform)

```bash
cd platform
dbt seed --profiles-dir ~/.dbt
```

This loads all 9 CSV files into `RAW_DEV.RAW.*`.

### Step 5: Build Platform

```bash
cd platform
dbt build --profiles-dir ~/.dbt
```

This runs seeds, staging, intermediate, marts, and all tests.

### Step 6: Build Consumer Projects

```bash
# After platform is built
cd ../marketing
dbt build --profiles-dir ~/.dbt

cd ../finance
dbt build --profiles-dir ~/.dbt

cd ../data_science
dbt build --profiles-dir ~/.dbt
```

### Step 7: dbt Mesh on dbt Cloud

In dbt Cloud, configure the projects as a Mesh:
1. Create 4 dbt Cloud projects (platform, marketing, finance, data_science).
2. Set `dependencies.yml` in each consumer project to reference `platform`.
3. In dbt Cloud settings, link consumer projects to the platform project.
4. Deploy platform first (upstream), then trigger consumer jobs.

---

## dbt Cloud Job Configuration

### Platform Job (runs first)
```
dbt seed
dbt build --select +marts.core
```

### Consumer Jobs (run after platform)
```
dbt build
```

Use dbt Cloud job chaining or Airflow/Prefect to orchestrate platform → consumers.

---

## Live Data Generation

The Snowflake procedure `ORCHESTRATION.JOBS.GENERATE_DAILY_TRANSACTIONS` generates
synthetic orders, items, and payments for keeping the demo "live":

```sql
-- Manual generation
CALL ORCHESTRATION.JOBS.GENERATE_DAILY_TRANSACTIONS('DEV', 10);

-- Resume scheduled daily task
ALTER TASK ORCHESTRATION.JOBS.DAILY_TRANSACTION_GENERATOR_DEV RESUME;
```

---

## Project File Map

```
ecommerce-mesh/
├── README.md
├── setup/
│   └── snowflake_setup.sql             Snowflake infra, roles, tasks
├── platform/
│   ├── dbt_project.yml
│   ├── macros/
│   │   ├── generate_schema_name.sql
│   │   └── generate_database_name.sql
│   ├── seeds/
│   │   ├── seeds.yml                   Column types + docs
│   │   ├── raw_customers.csv           150 rows
│   │   ├── raw_products.csv            50 rows
│   │   ├── raw_orders.csv              600 rows
│   │   ├── raw_order_items.csv         ~880 rows
│   │   ├── raw_payments.csv            600 rows
│   │   ├── raw_shipments.csv           ~478 rows
│   │   ├── raw_returns.csv             65 rows
│   │   ├── raw_campaigns.csv           20 rows
│   │   ├── raw_campaign_attributions.csv 280 rows
│   │   └── raw_web_events.csv          800 rows
│   ├── models/
│   │   ├── staging/
│   │   │   ├── _sources.yml            Source definitions with meta
│   │   │   ├── _stg_models.yml         Staging model docs + tests
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_products.sql
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_order_items.sql
│   │   │   ├── stg_payments.sql
│   │   │   ├── stg_shipments.sql
│   │   │   ├── stg_returns.sql
│   │   │   ├── stg_campaigns.sql
│   │   │   ├── stg_campaign_attributions.sql
│   │   │   └── stg_web_events.sql
│   │   ├── intermediate/
│   │   │   ├── int_orders_enriched.sql
│   │   │   ├── int_customer_order_summary.sql
│   │   │   ├── int_product_performance.sql
│   │   │   ├── int_payment_summary.sql
│   │   │   └── int_shipment_summary.sql
│   │   └── marts/
│   │       ├── groups.yml
│   │       └── core/
│   │           ├── _core_models.yml    Contracts, access, full column docs
│   │           ├── dim_customers.sql
│   │           ├── dim_products.sql
│   │           ├── dim_date.sql
│   │           ├── fct_orders.sql
│   │           ├── fct_order_items.sql
│   │           └── fct_payments.sql
│   └── semantic_models/
│       ├── sem_orders.yml
│       ├── sem_customers.yml
│       ├── sem_products.yml
│       └── metrics.yml                 14 metrics
├── marketing/
│   ├── dbt_project.yml
│   ├── dependencies.yml
│   └── models/marts/
│       ├── _marketing_models.yml
│       ├── customer_segments.sql       RFM scoring
│       ├── customer_ltv.sql            LTV analysis
│       ├── fct_campaign_performance.sql
│       ├── cohort_analysis.sql
│       └── channel_attribution.sql
├── finance/
│   ├── dbt_project.yml
│   ├── dependencies.yml
│   └── models/marts/
│       ├── _finance_models.yml
│       ├── fct_revenue.sql             Daily with MTD/YTD
│       ├── fct_refunds.sql
│       ├── monthly_revenue_summary.sql MoM + YoY growth
│       ├── product_margin_analysis.sql
│       ├── daily_flash_report.sql      Last 30 days vs PY
│       └── country_revenue_breakdown.sql
└── data_science/
    ├── dbt_project.yml
    ├── dependencies.yml
    └── models/
        ├── features/
        │   ├── _features_models.yml
        │   ├── customer_features.sql
        │   ├── product_affinity_features.sql
        │   └── order_sequence_features.sql
        └── prep/
            ├── _ds_models.yml
            ├── churn_prediction_input.sql
            ├── recommendation_input.sql
            └── customer_lifetime_value_features.sql
```

---

## Key Design Decisions

1. **No `config-version:` keys** — Fusion compatibility requirement met throughout.
2. **`cast()` only** — All SQL models use `cast(col as type)` syntax, never `::`.
3. **`arguments:` on tests** — All `accepted_values` tests use the Fusion-compatible `arguments:` syntax.
4. **Snowflake-native functions** — `datediff`, `dateadd`, `current_date()`, `date_trunc`, `seq4()`, `generator()`.
5. **Enforced contracts** — All 6 platform mart models have `contract: {enforced: true}` with full `data_type` column specs.
6. **Cross-project refs** — Consumer projects use `{{ ref('platform', 'model_name') }}` for all platform models.
7. **Semantic Layer completeness** — Every semantic model has entities, dimensions (including time dimensions), and measures. Metrics cover the full analysis lifecycle.
8. **AI-readable descriptions** — All column descriptions are written for LLM consumption: precise, context-rich, and include value enumerations where relevant.

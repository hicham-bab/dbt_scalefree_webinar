-- =============================================================================
-- LEATHER LUXE - Snowflake Infrastructure Setup
-- =============================================================================
-- Run this script as ACCOUNTADMIN to provision all infrastructure for the
-- Leather Luxe dbt Mesh project.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- 1. WAREHOUSES
-- =============================================================================

CREATE WAREHOUSE IF NOT EXISTS LEATHER_LUXE_DEV_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Development workload warehouse for dbt model runs';

CREATE WAREHOUSE IF NOT EXISTS LEATHER_LUXE_PRD_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Production workload warehouse for dbt model runs';

CREATE WAREHOUSE IF NOT EXISTS ORCHESTRATION_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for Snowflake Tasks and data generation procedures';

CREATE WAREHOUSE IF NOT EXISTS DATA_ANALYST_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Interactive query warehouse for data analysts';

CREATE WAREHOUSE IF NOT EXISTS DATA_ENGINEER_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for data engineers building and testing pipelines';

CREATE WAREHOUSE IF NOT EXISTS DATA_ARCHITECT_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for architects and governance work';

-- =============================================================================
-- 2. ROLES
-- =============================================================================

-- Service account roles
CREATE ROLE IF NOT EXISTS LEATHER_LUXE_DEV_SA_ROLE
    COMMENT = 'Service account role for dbt Cloud DEV environment';
CREATE ROLE IF NOT EXISTS LEATHER_LUXE_PRD_SA_ROLE
    COMMENT = 'Service account role for dbt Cloud PRD environment';

-- Human roles
CREATE ROLE IF NOT EXISTS DATA_ANALYST_ROLE
    COMMENT = 'Read-only access to analytics schemas';
CREATE ROLE IF NOT EXISTS DATA_ENGINEER_ROLE
    COMMENT = 'Read/write access for pipeline development';
CREATE ROLE IF NOT EXISTS DATA_ARCHITECT_ROLE
    COMMENT = 'Full access for architecture and governance';
CREATE ROLE IF NOT EXISTS MARKETING_ANALYST_ROLE
    COMMENT = 'Access to marketing analytics schema';
CREATE ROLE IF NOT EXISTS FINANCE_ANALYST_ROLE
    COMMENT = 'Access to finance analytics schema';
CREATE ROLE IF NOT EXISTS DATA_SCIENTIST_ROLE
    COMMENT = 'Access to DS feature and prep schemas';

-- Role hierarchy
GRANT ROLE DATA_ANALYST_ROLE      TO ROLE DATA_ENGINEER_ROLE;
GRANT ROLE DATA_ENGINEER_ROLE     TO ROLE DATA_ARCHITECT_ROLE;
GRANT ROLE DATA_ARCHITECT_ROLE    TO ROLE SYSADMIN;
GRANT ROLE MARKETING_ANALYST_ROLE TO ROLE DATA_ARCHITECT_ROLE;
GRANT ROLE FINANCE_ANALYST_ROLE   TO ROLE DATA_ARCHITECT_ROLE;
GRANT ROLE DATA_SCIENTIST_ROLE    TO ROLE DATA_ARCHITECT_ROLE;
GRANT ROLE LEATHER_LUXE_DEV_SA_ROLE TO ROLE DATA_ARCHITECT_ROLE;
GRANT ROLE LEATHER_LUXE_PRD_SA_ROLE TO ROLE SYSADMIN;

-- =============================================================================
-- 3. USERS
-- =============================================================================

-- Service accounts
CREATE USER IF NOT EXISTS LEATHER_LUXE_DEV_SA
    LOGIN_NAME = 'LEATHER_LUXE_DEV_SA'
    DISPLAY_NAME = 'Leather Luxe dbt Dev Service Account'
    DEFAULT_ROLE = LEATHER_LUXE_DEV_SA_ROLE
    DEFAULT_WAREHOUSE = LEATHER_LUXE_DEV_WH
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT = 'Service account for dbt Cloud development environment';

CREATE USER IF NOT EXISTS LEATHER_LUXE_PRD_SA
    LOGIN_NAME = 'LEATHER_LUXE_PRD_SA'
    DISPLAY_NAME = 'Leather Luxe dbt Prd Service Account'
    DEFAULT_ROLE = LEATHER_LUXE_PRD_SA_ROLE
    DEFAULT_WAREHOUSE = LEATHER_LUXE_PRD_WH
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT = 'Service account for dbt Cloud production environment';

-- Human users (passwords should be changed on first login)
CREATE USER IF NOT EXISTS ANALYST_USER
    LOGIN_NAME = 'ANALYST_USER'
    DISPLAY_NAME = 'Data Analyst'
    DEFAULT_ROLE = DATA_ANALYST_ROLE
    DEFAULT_WAREHOUSE = DATA_ANALYST_WH
    MUST_CHANGE_PASSWORD = TRUE;

CREATE USER IF NOT EXISTS ENGINEER_USER
    LOGIN_NAME = 'ENGINEER_USER'
    DISPLAY_NAME = 'Data Engineer'
    DEFAULT_ROLE = DATA_ENGINEER_ROLE
    DEFAULT_WAREHOUSE = DATA_ENGINEER_WH
    MUST_CHANGE_PASSWORD = TRUE;

CREATE USER IF NOT EXISTS ARCHITECT_USER
    LOGIN_NAME = 'ARCHITECT_USER'
    DISPLAY_NAME = 'Data Architect'
    DEFAULT_ROLE = DATA_ARCHITECT_ROLE
    DEFAULT_WAREHOUSE = DATA_ARCHITECT_WH
    MUST_CHANGE_PASSWORD = TRUE;

CREATE USER IF NOT EXISTS MARKETING_USER
    LOGIN_NAME = 'MARKETING_USER'
    DISPLAY_NAME = 'Marketing Analyst'
    DEFAULT_ROLE = MARKETING_ANALYST_ROLE
    DEFAULT_WAREHOUSE = DATA_ANALYST_WH
    MUST_CHANGE_PASSWORD = TRUE;

CREATE USER IF NOT EXISTS FINANCE_USER
    LOGIN_NAME = 'FINANCE_USER'
    DISPLAY_NAME = 'Finance Analyst'
    DEFAULT_ROLE = FINANCE_ANALYST_ROLE
    DEFAULT_WAREHOUSE = DATA_ANALYST_WH
    MUST_CHANGE_PASSWORD = TRUE;

CREATE USER IF NOT EXISTS DS_USER
    LOGIN_NAME = 'DS_USER'
    DISPLAY_NAME = 'Data Scientist'
    DEFAULT_ROLE = DATA_SCIENTIST_ROLE
    DEFAULT_WAREHOUSE = DATA_ANALYST_WH
    MUST_CHANGE_PASSWORD = TRUE;

-- Assign roles to users
GRANT ROLE LEATHER_LUXE_DEV_SA_ROLE TO USER LEATHER_LUXE_DEV_SA;
GRANT ROLE LEATHER_LUXE_PRD_SA_ROLE TO USER LEATHER_LUXE_PRD_SA;
GRANT ROLE DATA_ANALYST_ROLE        TO USER ANALYST_USER;
GRANT ROLE DATA_ENGINEER_ROLE       TO USER ENGINEER_USER;
GRANT ROLE DATA_ARCHITECT_ROLE      TO USER ARCHITECT_USER;
GRANT ROLE MARKETING_ANALYST_ROLE   TO USER MARKETING_USER;
GRANT ROLE FINANCE_ANALYST_ROLE     TO USER FINANCE_USER;
GRANT ROLE DATA_SCIENTIST_ROLE      TO USER DS_USER;

-- =============================================================================
-- 4. DATABASES
-- =============================================================================

CREATE DATABASE IF NOT EXISTS ECOMMERCE_DEV
    COMMENT = 'Development target database for platform and consumer dbt projects';

CREATE DATABASE IF NOT EXISTS ECOMMERCE_PRD
    COMMENT = 'Production target database for platform and consumer dbt projects';

CREATE DATABASE IF NOT EXISTS RAW_DEV
    COMMENT = 'Development raw layer — seeds and source data';

CREATE DATABASE IF NOT EXISTS RAW_PRD
    COMMENT = 'Production raw layer — seeds and source data';

CREATE DATABASE IF NOT EXISTS ORCHESTRATION
    COMMENT = 'Orchestration database for tasks, jobs, and data generation';

-- =============================================================================
-- 5. SCHEMAS WITH MANAGED ACCESS
-- =============================================================================

-- Raw layer schemas
CREATE SCHEMA IF NOT EXISTS RAW_DEV.RAW
    WITH MANAGED ACCESS
    COMMENT = 'Raw seed data for development';

CREATE SCHEMA IF NOT EXISTS RAW_PRD.RAW
    WITH MANAGED ACCESS
    COMMENT = 'Raw seed data for production';

-- Platform schemas - DEV
CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DEV.STG
    WITH MANAGED ACCESS
    COMMENT = 'Staging views — platform project DEV';

CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DEV.ANALYTICS
    WITH MANAGED ACCESS
    COMMENT = 'Core mart tables — platform project DEV';

-- Platform schemas - PRD
CREATE SCHEMA IF NOT EXISTS ECOMMERCE_PRD.STG
    WITH MANAGED ACCESS
    COMMENT = 'Staging views — platform project PRD';

CREATE SCHEMA IF NOT EXISTS ECOMMERCE_PRD.ANALYTICS
    WITH MANAGED ACCESS
    COMMENT = 'Core mart tables — platform project PRD';

-- Marketing consumer schemas
CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DEV.MKT_STG
    WITH MANAGED ACCESS
    COMMENT = 'Marketing staging — DEV';

CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DEV.MKT_ANALYTICS
    WITH MANAGED ACCESS
    COMMENT = 'Marketing analytics — DEV';

CREATE SCHEMA IF NOT EXISTS ECOMMERCE_PRD.MKT_STG
    WITH MANAGED ACCESS
    COMMENT = 'Marketing staging — PRD';

CREATE SCHEMA IF NOT EXISTS ECOMMERCE_PRD.MKT_ANALYTICS
    WITH MANAGED ACCESS
    COMMENT = 'Marketing analytics — PRD';

-- Finance consumer schemas
CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DEV.FIN_ANALYTICS
    WITH MANAGED ACCESS
    COMMENT = 'Finance analytics — DEV';

CREATE SCHEMA IF NOT EXISTS ECOMMERCE_PRD.FIN_ANALYTICS
    WITH MANAGED ACCESS
    COMMENT = 'Finance analytics — PRD';

-- Data Science consumer schemas
CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DEV.DS_FEATURES
    WITH MANAGED ACCESS
    COMMENT = 'ML feature tables — DEV';

CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DEV.DS_PREP
    WITH MANAGED ACCESS
    COMMENT = 'ML training datasets — DEV';

CREATE SCHEMA IF NOT EXISTS ECOMMERCE_PRD.DS_FEATURES
    WITH MANAGED ACCESS
    COMMENT = 'ML feature tables — PRD';

CREATE SCHEMA IF NOT EXISTS ECOMMERCE_PRD.DS_PREP
    WITH MANAGED ACCESS
    COMMENT = 'ML training datasets — PRD';

-- Orchestration schema
CREATE SCHEMA IF NOT EXISTS ORCHESTRATION.JOBS
    WITH MANAGED ACCESS
    COMMENT = 'Tasks, procedures, and orchestration objects';

-- =============================================================================
-- 6. PRIVILEGE GRANTS (Least Privilege)
-- =============================================================================

USE ROLE SYSADMIN;

-- DEV Service Account: full access to dev databases
GRANT USAGE                                 ON WAREHOUSE LEATHER_LUXE_DEV_WH TO ROLE LEATHER_LUXE_DEV_SA_ROLE;
GRANT USAGE                                 ON DATABASE ECOMMERCE_DEV          TO ROLE LEATHER_LUXE_DEV_SA_ROLE;
GRANT USAGE                                 ON DATABASE RAW_DEV                TO ROLE LEATHER_LUXE_DEV_SA_ROLE;
GRANT CREATE SCHEMA                         ON DATABASE ECOMMERCE_DEV          TO ROLE LEATHER_LUXE_DEV_SA_ROLE;
GRANT ALL PRIVILEGES ON ALL SCHEMAS         IN DATABASE ECOMMERCE_DEV          TO ROLE LEATHER_LUXE_DEV_SA_ROLE;
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS      IN DATABASE ECOMMERCE_DEV          TO ROLE LEATHER_LUXE_DEV_SA_ROLE;
GRANT ALL PRIVILEGES ON ALL TABLES         IN DATABASE ECOMMERCE_DEV          TO ROLE LEATHER_LUXE_DEV_SA_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES      IN DATABASE ECOMMERCE_DEV          TO ROLE LEATHER_LUXE_DEV_SA_ROLE;
GRANT ALL PRIVILEGES ON ALL VIEWS          IN DATABASE ECOMMERCE_DEV          TO ROLE LEATHER_LUXE_DEV_SA_ROLE;
GRANT ALL PRIVILEGES ON FUTURE VIEWS       IN DATABASE ECOMMERCE_DEV          TO ROLE LEATHER_LUXE_DEV_SA_ROLE;
GRANT ALL PRIVILEGES ON ALL SCHEMAS         IN DATABASE RAW_DEV                TO ROLE LEATHER_LUXE_DEV_SA_ROLE;
GRANT ALL PRIVILEGES ON ALL TABLES         IN DATABASE RAW_DEV                TO ROLE LEATHER_LUXE_DEV_SA_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES      IN DATABASE RAW_DEV                TO ROLE LEATHER_LUXE_DEV_SA_ROLE;

-- PRD Service Account: full access to prd databases
GRANT USAGE                                 ON WAREHOUSE LEATHER_LUXE_PRD_WH TO ROLE LEATHER_LUXE_PRD_SA_ROLE;
GRANT USAGE                                 ON DATABASE ECOMMERCE_PRD          TO ROLE LEATHER_LUXE_PRD_SA_ROLE;
GRANT USAGE                                 ON DATABASE RAW_PRD                TO ROLE LEATHER_LUXE_PRD_SA_ROLE;
GRANT CREATE SCHEMA                         ON DATABASE ECOMMERCE_PRD          TO ROLE LEATHER_LUXE_PRD_SA_ROLE;
GRANT ALL PRIVILEGES ON ALL SCHEMAS         IN DATABASE ECOMMERCE_PRD          TO ROLE LEATHER_LUXE_PRD_SA_ROLE;
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS      IN DATABASE ECOMMERCE_PRD          TO ROLE LEATHER_LUXE_PRD_SA_ROLE;
GRANT ALL PRIVILEGES ON ALL TABLES         IN DATABASE ECOMMERCE_PRD          TO ROLE LEATHER_LUXE_PRD_SA_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES      IN DATABASE ECOMMERCE_PRD          TO ROLE LEATHER_LUXE_PRD_SA_ROLE;
GRANT ALL PRIVILEGES ON ALL VIEWS          IN DATABASE ECOMMERCE_PRD          TO ROLE LEATHER_LUXE_PRD_SA_ROLE;
GRANT ALL PRIVILEGES ON FUTURE VIEWS       IN DATABASE ECOMMERCE_PRD          TO ROLE LEATHER_LUXE_PRD_SA_ROLE;
GRANT ALL PRIVILEGES ON ALL SCHEMAS         IN DATABASE RAW_PRD                TO ROLE LEATHER_LUXE_PRD_SA_ROLE;
GRANT ALL PRIVILEGES ON ALL TABLES         IN DATABASE RAW_PRD                TO ROLE LEATHER_LUXE_PRD_SA_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES      IN DATABASE RAW_PRD                TO ROLE LEATHER_LUXE_PRD_SA_ROLE;

-- Data Analyst: read-only on analytics schemas
GRANT USAGE ON WAREHOUSE DATA_ANALYST_WH    TO ROLE DATA_ANALYST_ROLE;
GRANT USAGE ON DATABASE ECOMMERCE_PRD        TO ROLE DATA_ANALYST_ROLE;
GRANT USAGE ON DATABASE ECOMMERCE_DEV        TO ROLE DATA_ANALYST_ROLE;
GRANT USAGE ON SCHEMA ECOMMERCE_PRD.ANALYTICS TO ROLE DATA_ANALYST_ROLE;
GRANT USAGE ON SCHEMA ECOMMERCE_DEV.ANALYTICS TO ROLE DATA_ANALYST_ROLE;
GRANT SELECT ON ALL TABLES  IN SCHEMA ECOMMERCE_PRD.ANALYTICS TO ROLE DATA_ANALYST_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ECOMMERCE_PRD.ANALYTICS TO ROLE DATA_ANALYST_ROLE;
GRANT SELECT ON ALL TABLES  IN SCHEMA ECOMMERCE_DEV.ANALYTICS TO ROLE DATA_ANALYST_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ECOMMERCE_DEV.ANALYTICS TO ROLE DATA_ANALYST_ROLE;

-- Marketing Analyst: read on marketing analytics
GRANT USAGE ON WAREHOUSE DATA_ANALYST_WH         TO ROLE MARKETING_ANALYST_ROLE;
GRANT USAGE ON DATABASE ECOMMERCE_PRD             TO ROLE MARKETING_ANALYST_ROLE;
GRANT USAGE ON SCHEMA ECOMMERCE_PRD.MKT_ANALYTICS TO ROLE MARKETING_ANALYST_ROLE;
GRANT USAGE ON SCHEMA ECOMMERCE_PRD.ANALYTICS     TO ROLE MARKETING_ANALYST_ROLE;
GRANT SELECT ON ALL TABLES  IN SCHEMA ECOMMERCE_PRD.MKT_ANALYTICS TO ROLE MARKETING_ANALYST_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ECOMMERCE_PRD.MKT_ANALYTICS TO ROLE MARKETING_ANALYST_ROLE;
GRANT SELECT ON ALL TABLES  IN SCHEMA ECOMMERCE_PRD.ANALYTICS TO ROLE MARKETING_ANALYST_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ECOMMERCE_PRD.ANALYTICS TO ROLE MARKETING_ANALYST_ROLE;

-- Finance Analyst: read on finance analytics
GRANT USAGE ON WAREHOUSE DATA_ANALYST_WH        TO ROLE FINANCE_ANALYST_ROLE;
GRANT USAGE ON DATABASE ECOMMERCE_PRD            TO ROLE FINANCE_ANALYST_ROLE;
GRANT USAGE ON SCHEMA ECOMMERCE_PRD.FIN_ANALYTICS TO ROLE FINANCE_ANALYST_ROLE;
GRANT USAGE ON SCHEMA ECOMMERCE_PRD.ANALYTICS    TO ROLE FINANCE_ANALYST_ROLE;
GRANT SELECT ON ALL TABLES  IN SCHEMA ECOMMERCE_PRD.FIN_ANALYTICS TO ROLE FINANCE_ANALYST_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ECOMMERCE_PRD.FIN_ANALYTICS TO ROLE FINANCE_ANALYST_ROLE;
GRANT SELECT ON ALL TABLES  IN SCHEMA ECOMMERCE_PRD.ANALYTICS TO ROLE FINANCE_ANALYST_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ECOMMERCE_PRD.ANALYTICS TO ROLE FINANCE_ANALYST_ROLE;

-- Data Scientist: read on DS schemas
GRANT USAGE ON WAREHOUSE DATA_ANALYST_WH      TO ROLE DATA_SCIENTIST_ROLE;
GRANT USAGE ON DATABASE ECOMMERCE_PRD          TO ROLE DATA_SCIENTIST_ROLE;
GRANT USAGE ON SCHEMA ECOMMERCE_PRD.DS_FEATURES TO ROLE DATA_SCIENTIST_ROLE;
GRANT USAGE ON SCHEMA ECOMMERCE_PRD.DS_PREP   TO ROLE DATA_SCIENTIST_ROLE;
GRANT USAGE ON SCHEMA ECOMMERCE_PRD.ANALYTICS TO ROLE DATA_SCIENTIST_ROLE;
GRANT SELECT ON ALL TABLES  IN SCHEMA ECOMMERCE_PRD.DS_FEATURES TO ROLE DATA_SCIENTIST_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ECOMMERCE_PRD.DS_FEATURES TO ROLE DATA_SCIENTIST_ROLE;
GRANT SELECT ON ALL TABLES  IN SCHEMA ECOMMERCE_PRD.DS_PREP TO ROLE DATA_SCIENTIST_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ECOMMERCE_PRD.DS_PREP TO ROLE DATA_SCIENTIST_ROLE;
GRANT SELECT ON ALL TABLES  IN SCHEMA ECOMMERCE_PRD.ANALYTICS TO ROLE DATA_SCIENTIST_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ECOMMERCE_PRD.ANALYTICS TO ROLE DATA_SCIENTIST_ROLE;

-- Data Engineer: write on dev, read on prd analytics
GRANT USAGE ON WAREHOUSE DATA_ENGINEER_WH    TO ROLE DATA_ENGINEER_ROLE;
GRANT ALL PRIVILEGES ON DATABASE ECOMMERCE_DEV TO ROLE DATA_ENGINEER_ROLE;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE ECOMMERCE_DEV TO ROLE DATA_ENGINEER_ROLE;
GRANT ALL PRIVILEGES ON ALL TABLES  IN DATABASE ECOMMERCE_DEV TO ROLE DATA_ENGINEER_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE ECOMMERCE_DEV TO ROLE DATA_ENGINEER_ROLE;

-- Data Architect: full access
GRANT USAGE ON WAREHOUSE DATA_ARCHITECT_WH TO ROLE DATA_ARCHITECT_ROLE;
GRANT ALL PRIVILEGES ON DATABASE ECOMMERCE_DEV TO ROLE DATA_ARCHITECT_ROLE;
GRANT ALL PRIVILEGES ON DATABASE ECOMMERCE_PRD TO ROLE DATA_ARCHITECT_ROLE;
GRANT ALL PRIVILEGES ON DATABASE RAW_DEV       TO ROLE DATA_ARCHITECT_ROLE;
GRANT ALL PRIVILEGES ON DATABASE RAW_PRD       TO ROLE DATA_ARCHITECT_ROLE;
GRANT ALL PRIVILEGES ON DATABASE ORCHESTRATION TO ROLE DATA_ARCHITECT_ROLE;

-- Orchestration
GRANT USAGE ON WAREHOUSE ORCHESTRATION_WH    TO ROLE LEATHER_LUXE_PRD_SA_ROLE;
GRANT USAGE ON DATABASE ORCHESTRATION        TO ROLE LEATHER_LUXE_PRD_SA_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA ORCHESTRATION.JOBS TO ROLE LEATHER_LUXE_PRD_SA_ROLE;
GRANT ALL PRIVILEGES ON ALL PROCEDURES IN SCHEMA ORCHESTRATION.JOBS TO ROLE LEATHER_LUXE_PRD_SA_ROLE;
GRANT ALL PRIVILEGES ON FUTURE PROCEDURES IN SCHEMA ORCHESTRATION.JOBS TO ROLE LEATHER_LUXE_PRD_SA_ROLE;

-- =============================================================================
-- 7. LIVE DATA GENERATION PROCEDURE
-- =============================================================================

USE ROLE SYSADMIN;
USE DATABASE ORCHESTRATION;
USE SCHEMA JOBS;

CREATE OR REPLACE PROCEDURE ORCHESTRATION.JOBS.GENERATE_DAILY_TRANSACTIONS(
    env        VARCHAR,
    order_count NUMBER
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'generate_transactions'
AS
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
import random
from datetime import date, timedelta

def generate_transactions(session: snowpark.Session, env: str, order_count: int) -> str:
    """
    Generates synthetic daily transactions and inserts them into the raw tables.

    Args:
        env: 'DEV' or 'PRD' — determines which database to write to
        order_count: Number of new orders to generate

    Returns:
        Summary string of generated records
    """
    raw_db   = f'RAW_{env}'
    today    = date.today().isoformat()

    random.seed()

    # Fetch existing customer IDs
    customers = session.sql(
        f"SELECT customer_id FROM {raw_db}.RAW.RAW_CUSTOMERS ORDER BY RANDOM() LIMIT 100"
    ).collect()
    customer_ids = [row['CUSTOMER_ID'] for row in customers]

    # Fetch existing product IDs and prices
    products = session.sql(
        f"SELECT product_id, unit_price FROM {raw_db}.RAW.RAW_PRODUCTS WHERE is_active = TRUE"
    ).collect()
    product_map = {row['PRODUCT_ID']: float(row['UNIT_PRICE']) for row in products}
    product_ids = list(product_map.keys())

    if not customer_ids or not product_ids:
        return 'ERROR: No customers or products found. Run seeds first.'

    channels       = ['website', 'website', 'website', 'mobile_app', 'mobile_app', 'in_store']
    statuses       = ['confirmed', 'confirmed', 'confirmed', 'processing', 'pending']
    methods        = ['credit_card', 'credit_card', 'paypal', 'bank_transfer', 'store_credit']
    carriers       = ['Colissimo', 'DHL', 'Chronopost', 'DPD', 'UPS']

    orders_inserted  = 0
    items_inserted   = 0
    payments_inserted = 0

    # Get current max IDs
    max_order  = session.sql(f"SELECT COALESCE(MAX(TRY_CAST(REGEXP_REPLACE(order_id, '[^0-9]', '') AS INTEGER)), 10600) AS max_id FROM {raw_db}.RAW.RAW_ORDERS").collect()[0]['MAX_ID']
    max_item   = session.sql(f"SELECT COALESCE(MAX(TRY_CAST(REGEXP_REPLACE(order_item_id, '[^0-9]', '') AS INTEGER)), 900) AS max_id FROM {raw_db}.RAW.RAW_ORDER_ITEMS").collect()[0]['MAX_ID']
    max_pay    = session.sql(f"SELECT COALESCE(MAX(TRY_CAST(REGEXP_REPLACE(payment_id, '[^0-9]', '') AS INTEGER)), 600) AS max_id FROM {raw_db}.RAW.RAW_PAYMENTS").collect()[0]['MAX_ID']

    for i in range(int(order_count)):
        order_num  = max_order + i + 1
        order_id   = f'ORD_{order_num}'
        customer_id = random.choice(customer_ids)
        channel    = random.choice(channels)
        status     = random.choice(statuses)
        disc_amt   = 0
        disc_code  = ''

        # Insert order
        session.sql(f"""
            INSERT INTO {raw_db}.RAW.RAW_ORDERS
            (order_id, customer_id, order_date, order_status, channel,
             shipping_city, shipping_country, shipping_method,
             discount_code, discount_amount, _loaded_at)
            VALUES ('{order_id}', '{customer_id}', '{today}', '{status}', '{channel}',
                    'Paris', 'France', 'standard', '{disc_code}', {disc_amt}, CURRENT_TIMESTAMP())
        """).collect()
        orders_inserted += 1

        # Insert 1-2 items per order
        num_items = random.choices([1, 2], weights=[70, 30])[0]
        selected_products = random.sample(product_ids, min(num_items, len(product_ids)))

        for prod_id in selected_products:
            item_num = max_item + items_inserted + 1
            item_id  = f'ITEM_{item_num:06d}'
            qty      = random.choices([1, 2], weights=[85, 15])[0]
            price    = round(product_map[prod_id] * random.uniform(0.97, 1.03), 2)
            disc_pct = random.choices([0, 5, 10], weights=[80, 10, 10])[0]

            session.sql(f"""
                INSERT INTO {raw_db}.RAW.RAW_ORDER_ITEMS
                (order_item_id, order_id, product_id, quantity, unit_price_at_order, discount_pct, _loaded_at)
                VALUES ('{item_id}', '{order_id}', '{prod_id}', {qty}, {price}, {disc_pct}, CURRENT_TIMESTAMP())
            """).collect()
            items_inserted += 1

        # Insert payment
        pay_num = max_pay + i + 1
        pay_id  = f'PAY_{pay_num:06d}'
        method  = random.choice(methods)
        amount  = round(random.uniform(100, 1500), 2)
        currency = random.choices(['EUR', 'EUR', 'EUR', 'GBP', 'USD'], weights=[70, 10, 10, 5, 5])[0]
        gw_map  = {'credit_card': 'stripe', 'paypal': 'paypal', 'bank_transfer': 'manual',
                   'store_credit': 'manual', 'gift_card': 'manual'}

        session.sql(f"""
            INSERT INTO {raw_db}.RAW.RAW_PAYMENTS
            (payment_id, order_id, payment_date, payment_method, amount,
             currency, payment_status, gateway, _loaded_at)
            VALUES ('{pay_id}', '{order_id}', '{today}', '{method}', {amount},
                    '{currency}', 'completed', '{gw_map[method]}', CURRENT_TIMESTAMP())
        """).collect()
        payments_inserted += 1

    return f'SUCCESS: Generated {orders_inserted} orders, {items_inserted} items, {payments_inserted} payments for {today} in {raw_db}'
$$;

-- Grant execute on procedure to service accounts
GRANT USAGE ON PROCEDURE ORCHESTRATION.JOBS.GENERATE_DAILY_TRANSACTIONS(VARCHAR, NUMBER)
    TO ROLE LEATHER_LUXE_PRD_SA_ROLE;
GRANT USAGE ON PROCEDURE ORCHESTRATION.JOBS.GENERATE_DAILY_TRANSACTIONS(VARCHAR, NUMBER)
    TO ROLE LEATHER_LUXE_DEV_SA_ROLE;

-- =============================================================================
-- 8. SNOWFLAKE TASK — DAILY TRANSACTION GENERATION
-- =============================================================================

CREATE OR REPLACE TASK ORCHESTRATION.JOBS.DAILY_TRANSACTION_GENERATOR_DEV
    WAREHOUSE = ORCHESTRATION_WH
    SCHEDULE  = 'USING CRON 0 6 * * * UTC'   -- Runs daily at 6am UTC
    COMMENT   = 'Generates ~10 synthetic orders per day in DEV for demo freshness'
AS
    CALL ORCHESTRATION.JOBS.GENERATE_DAILY_TRANSACTIONS('DEV', 10);

CREATE OR REPLACE TASK ORCHESTRATION.JOBS.DAILY_TRANSACTION_GENERATOR_PRD
    WAREHOUSE = ORCHESTRATION_WH
    SCHEDULE  = 'USING CRON 0 7 * * * UTC'   -- Runs daily at 7am UTC
    COMMENT   = 'Generates ~25 synthetic orders per day in PRD for demo freshness'
AS
    CALL ORCHESTRATION.JOBS.GENERATE_DAILY_TRANSACTIONS('PRD', 25);

-- Note: Tasks are created in SUSPENDED state by default.
-- Resume with: ALTER TASK ORCHESTRATION.JOBS.DAILY_TRANSACTION_GENERATOR_DEV RESUME;

-- =============================================================================
-- 9. MANUAL EXECUTION EXAMPLES
-- =============================================================================

-- Generate 10 orders in DEV immediately:
-- CALL ORCHESTRATION.JOBS.GENERATE_DAILY_TRANSACTIONS('DEV', 10);

-- Generate 25 orders in PRD:
-- CALL ORCHESTRATION.JOBS.GENERATE_DAILY_TRANSACTIONS('PRD', 25);

-- Resume the scheduled tasks:
-- ALTER TASK ORCHESTRATION.JOBS.DAILY_TRANSACTION_GENERATOR_DEV RESUME;
-- ALTER TASK ORCHESTRATION.JOBS.DAILY_TRANSACTION_GENERATOR_PRD RESUME;

-- Suspend the tasks:
-- ALTER TASK ORCHESTRATION.JOBS.DAILY_TRANSACTION_GENERATOR_DEV SUSPEND;
-- ALTER TASK ORCHESTRATION.JOBS.DAILY_TRANSACTION_GENERATOR_PRD SUSPEND;

-- Check task history:
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
--     SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP()),
--     TASK_NAME => 'DAILY_TRANSACTION_GENERATOR_DEV'
-- )) ORDER BY SCHEDULED_TIME DESC;

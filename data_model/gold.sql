-- Dimension: customers
CREATE TABLE gold.dim_customers (
    customer_key      INT IDENTITY(1,1) PRIMARY KEY,
    customer_id       NVARCHAR(50),
    customer_city     NVARCHAR(100),
    customer_state    NVARCHAR(10)
);

-- Dimension: products
CREATE TABLE gold.dim_products (
    product_key           INT IDENTITY(1,1) PRIMARY KEY,
    product_id            NVARCHAR(50),
    product_category_name NVARCHAR(100),
    product_weight_g      FLOAT
);

-- Fact: orders
CREATE TABLE gold.fact_orders (
    order_key         INT IDENTITY(1,1) PRIMARY KEY,
    order_id          NVARCHAR(50),
    customer_key      INT,
    order_status      NVARCHAR(50),
    order_purchase_timestamp DATETIME,
    order_delivered_customer_date DATETIME
);

-- Fact: sales
CREATE TABLE gold.fact_sales (
    sales_key     INT IDENTITY(1,1) PRIMARY KEY,
    order_id      NVARCHAR(50),
    product_key   INT,
    price         DECIMAL(10,2),
    freight_value DECIMAL(10,2),
    total_amount  AS (price + freight_value)  -- computed column
);


CREATE VIEW gold.sales_summary AS
SELECT
    CAST(o.order_purchase_timestamp AS DATE) AS order_date,
    COUNT(DISTINCT o.order_id)               AS total_orders,
    SUM(p.payment_value)                     AS total_revenue,
    AVG(p.payment_value)                     AS avg_order_value
FROM silver.orders_clean o
JOIN silver.payments_clean p ON o.order_id = p.order_id
GROUP BY CAST(o.order_purchase_timestamp AS DATE);

CREATE VIEW gold.product_performance AS
SELECT
    pr.product_category_name,
    COUNT(oi.order_id)   AS total_orders,
    SUM(oi.price)        AS total_revenue
FROM silver.order_items_clean oi
JOIN silver.products_clean pr ON oi.product_id = pr.product_id
GROUP BY pr.product_category_name;

CREATE VIEW gold.customer_revenue AS
SELECT
    c.customer_id,
    c.customer_city,
    c.customer_state,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(p.payment_value)       AS total_revenue
FROM silver.customers_clean c
JOIN silver.orders_clean o ON c.customer_id = o.customer_id
JOIN silver.payments_clean p ON o.order_id = p.order_id
GROUP BY c.customer_id, c.customer_city, c.customer_state;



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

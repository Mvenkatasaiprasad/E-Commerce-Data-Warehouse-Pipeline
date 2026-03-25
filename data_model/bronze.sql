CREATE TABLE bronze.bronze_customers (
    customer_id             NVARCHAR(50),
    customer_unique_id      NVARCHAR(50),
    customer_zip_code_prefix NVARCHAR(10),
    customer_city           NVARCHAR(100),
    customer_state          NVARCHAR(10),
    ingestion_timestamp     DATETIME DEFAULT GETDATE(),
    source_system           NVARCHAR(50) DEFAULT 'Olist_CSV'
);

CREATE TABLE bronze.bronze_orders (
    order_id                NVARCHAR(50),
    customer_id             NVARCHAR(50),
    order_status            NVARCHAR(50),
    order_purchase_timestamp NVARCHAR(50),
    order_approved_at        NVARCHAR(50),
    order_delivered_carrier_date   NVARCHAR(50),
    order_delivered_customer_date  NVARCHAR(50),
    order_estimated_delivery_date  NVARCHAR(50),
    ingestion_timestamp     DATETIME DEFAULT GETDATE(),
    source_system           NVARCHAR(50) DEFAULT 'Olist_CSV'
);

CREATE TABLE bronze.bronze_order_items (
    order_id            NVARCHAR(50),
    order_item_id       INT,
    product_id          NVARCHAR(50),
    seller_id           NVARCHAR(50),
    shipping_limit_date NVARCHAR(50),
    price               NVARCHAR(20),
    freight_value       NVARCHAR(20),
    ingestion_timestamp DATETIME DEFAULT GETDATE(),
    source_system       NVARCHAR(50) DEFAULT 'Olist_CSV'
);

CREATE TABLE bronze.bronze_products (
    product_id                NVARCHAR(50),
    product_category_name     NVARCHAR(100),
    product_name_lenght       INT,
    product_description_lenght INT,
    product_photos_qty        INT,
    product_weight_g          FLOAT,
    product_length_cm         FLOAT,
    product_height_cm         FLOAT,
    product_width_cm          FLOAT,
    ingestion_timestamp       DATETIME DEFAULT GETDATE(),
    source_system             NVARCHAR(50) DEFAULT 'Olist_CSV'
);

CREATE TABLE bronze.bronze_payments (
    order_id             NVARCHAR(50),
    payment_sequential   INT,
    payment_type         NVARCHAR(50),
    payment_installments INT,
    payment_value        NVARCHAR(20),
    ingestion_timestamp  DATETIME DEFAULT GETDATE(),
    source_system        NVARCHAR(50) DEFAULT 'Olist_CSV'
);


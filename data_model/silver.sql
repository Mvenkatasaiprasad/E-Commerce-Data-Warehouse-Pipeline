CREATE TABLE silver.customers_clean (
    customer_id             NVARCHAR(50) PRIMARY KEY,
    customer_unique_id      NVARCHAR(50),
    customer_zip_code_prefix NVARCHAR(10),
    customer_city           NVARCHAR(100),
    customer_state          NVARCHAR(10),
    cleaned_at              DATETIME DEFAULT GETDATE()
);

CREATE TABLE silver.orders_clean (
    order_id                        NVARCHAR(50) PRIMARY KEY,
    customer_id                     NVARCHAR(50),
    order_status                    NVARCHAR(50),
    order_purchase_timestamp        DATETIME,
    order_approved_at               DATETIME,
    order_delivered_carrier_date    DATETIME,
    order_delivered_customer_date   DATETIME,
    order_estimated_delivery_date   DATETIME,
    cleaned_at                      DATETIME DEFAULT GETDATE()
);

CREATE TABLE silver.order_items_clean (
    order_id            NVARCHAR(50),
    order_item_id       INT,
    product_id          NVARCHAR(50),
    seller_id           NVARCHAR(50),
    shipping_limit_date DATETIME,
    price               DECIMAL(10,2),
    freight_value       DECIMAL(10,2),
    cleaned_at          DATETIME DEFAULT GETDATE()
);

CREATE TABLE silver.products_clean (
    product_id            NVARCHAR(50) PRIMARY KEY,
    product_category_name NVARCHAR(100),
    product_name_lenght   INT,
    product_weight_g      FLOAT,
    product_length_cm     FLOAT,
    product_height_cm     FLOAT,
    product_width_cm      FLOAT,
    cleaned_at            DATETIME DEFAULT GETDATE()
);

CREATE TABLE silver.payments_clean (
    order_id             NVARCHAR(50),
    payment_sequential   INT,
    payment_type         NVARCHAR(50),
    payment_installments INT,
    payment_value        DECIMAL(10,2),
    cleaned_at           DATETIME DEFAULT GETDATE()
);

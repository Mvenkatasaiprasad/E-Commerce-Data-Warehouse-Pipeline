--bronze

CREATE OR ALTER PROCEDURE bronze.usp_load_bronze
    @load_type NVARCHAR(20) = 'snapshot'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start        DATETIME = GETDATE();
    DECLARE @stage_start  DATETIME;
    DECLARE @rows         INT = 0;
    DECLARE @total_rows   INT = 0;
    DECLARE @duration     INT;
    DECLARE @last_load    DATETIME;

    -- Get watermark
    SELECT @last_load = last_load_date
    FROM control.pipeline_watermark
    WHERE table_name = 'bronze';

    PRINT '==========================================';
    PRINT ' BRONZE | Mode: ' + UPPER(@load_type);
    PRINT '==========================================';

    BEGIN TRY

        -- ==================
        -- CUSTOMERS
        -- ==================
        SET @stage_start = GETDATE();
        PRINT '>>> bronze.load_customers | ' + @load_type;

        IF @load_type = 'snapshot'
        BEGIN
            TRUNCATE TABLE bronze.bronze_customers;
            INSERT INTO bronze.bronze_customers
            SELECT *, GETDATE(), 'Olist_CSV' FROM source.customers;
        END
        ELSE
        BEGIN
            INSERT INTO bronze.bronze_customers
            SELECT s.*, GETDATE(), 'Olist_CSV'
            FROM source.customers s
            WHERE NOT EXISTS (
                SELECT 1 FROM bronze.bronze_customers b
                WHERE b.customer_id = s.customer_id
            );
        END

        SET @rows = @@ROWCOUNT;
        SET @total_rows += @rows;
        SET @duration = DATEDIFF(SECOND, @stage_start, GETDATE());
        PRINT 'bronze_customers: ' + CAST(@rows AS NVARCHAR) + ' rows | ' + CAST(@duration AS NVARCHAR) + 's';

        -- ==================
        -- ORDERS
        -- ==================
        SET @stage_start = GETDATE();
        PRINT '>>> bronze.load_orders | ' + @load_type;

        IF @load_type = 'snapshot'
        BEGIN
            TRUNCATE TABLE bronze.bronze_orders;
            INSERT INTO bronze.bronze_orders
            SELECT *, GETDATE(), 'Olist_CSV' FROM source.orders;
        END
        ELSE
        BEGIN
            INSERT INTO bronze.bronze_orders
            SELECT s.*, GETDATE(), 'Olist_CSV'
            FROM source.orders s
            WHERE NOT EXISTS (
                SELECT 1 FROM bronze.bronze_orders b
                WHERE b.order_id = s.order_id
            );
        END

        SET @rows = @@ROWCOUNT;
        SET @total_rows += @rows;
        SET @duration = DATEDIFF(SECOND, @stage_start, GETDATE());
        PRINT 'bronze_orders: ' + CAST(@rows AS NVARCHAR) + ' rows | ' + CAST(@duration AS NVARCHAR) + 's';

        -- ==================
        -- ORDER ITEMS
        -- ==================
        SET @stage_start = GETDATE();
        PRINT '>>> bronze.load_order_items | ' + @load_type;

        IF @load_type = 'snapshot'
        BEGIN
            TRUNCATE TABLE bronze.bronze_order_items;
            INSERT INTO bronze.bronze_order_items
            SELECT *, GETDATE(), 'Olist_CSV' FROM source.order_items;
        END
        ELSE
        BEGIN
            INSERT INTO bronze.bronze_order_items
            SELECT s.*, GETDATE(), 'Olist_CSV'
            FROM source.order_items s
            WHERE NOT EXISTS (
                SELECT 1 FROM bronze.bronze_order_items b
                WHERE b.order_id = s.order_id
                AND   b.order_item_id = s.order_item_id
            );
        END

        SET @rows = @@ROWCOUNT;
        SET @total_rows += @rows;
        SET @duration = DATEDIFF(SECOND, @stage_start, GETDATE());
        PRINT 'bronze_order_items: ' + CAST(@rows AS NVARCHAR) + ' rows | ' + CAST(@duration AS NVARCHAR) + 's';

        -- ==================
        -- PRODUCTS
        -- ==================
        SET @stage_start = GETDATE();
        PRINT '>>> bronze.load_products | ' + @load_type;

        IF @load_type = 'snapshot'
        BEGIN
            TRUNCATE TABLE bronze.bronze_products;
            INSERT INTO bronze.bronze_products
            SELECT *, GETDATE(), 'Olist_CSV' FROM source.products;
        END
        ELSE
        BEGIN
            INSERT INTO bronze.bronze_products
            SELECT s.*, GETDATE(), 'Olist_CSV'
            FROM source.products s
            WHERE NOT EXISTS (
                SELECT 1 FROM bronze.bronze_products b
                WHERE b.product_id = s.product_id
            );
        END

        SET @rows = @@ROWCOUNT;
        SET @total_rows += @rows;
        SET @duration = DATEDIFF(SECOND, @stage_start, GETDATE());
        PRINT 'bronze_products: ' + CAST(@rows AS NVARCHAR) + ' rows | ' + CAST(@duration AS NVARCHAR) + 's';

        -- ==================
        -- PAYMENTS
        -- ==================
        SET @stage_start = GETDATE();
        PRINT '>>> bronze.load_payments | ' + @load_type;

        IF @load_type = 'snapshot'
        BEGIN
            TRUNCATE TABLE bronze.bronze_payments;
            INSERT INTO bronze.bronze_payments
            SELECT *, GETDATE(), 'Olist_CSV' FROM source.payments;
        END
        ELSE
        BEGIN
            INSERT INTO bronze.bronze_payments
            SELECT s.*, GETDATE(), 'Olist_CSV'
            FROM source.payments s
            WHERE NOT EXISTS (
                SELECT 1 FROM bronze.bronze_payments b
                WHERE b.order_id     = s.order_id
                AND   b.payment_sequential = s.payment_sequential
            );
        END

        SET @rows = @@ROWCOUNT;
        SET @total_rows += @rows;
        SET @duration = DATEDIFF(SECOND, @stage_start, GETDATE());
        PRINT 'bronze_payments: ' + CAST(@rows AS NVARCHAR) + ' rows | ' + CAST(@duration AS NVARCHAR) + 's';

        -- Update watermark
        MERGE control.pipeline_watermark AS T
        USING (SELECT 'bronze' AS tn, GETDATE() AS ld, @load_type AS lt) AS S
        ON T.table_name = S.tn
        WHEN MATCHED    THEN UPDATE SET last_load_date = S.ld, load_type = S.lt
        WHEN NOT MATCHED THEN INSERT (table_name, last_load_date, load_type)
                              VALUES (S.tn, S.ld, S.lt);

        SET @duration = DATEDIFF(SECOND, @start, GETDATE());
        PRINT '==========================================';
        PRINT ' BRONZE Done: ' + CAST(@total_rows AS NVARCHAR) + ' rows | ' + CAST(@duration AS NVARCHAR) + 's';
        PRINT '==========================================';

        INSERT INTO control.pipeline_log
            (pipeline_name, start_time, end_time, rows_processed, status)
        VALUES ('bronze.usp_load_bronze', @start, GETDATE(), @total_rows, 'success');

    END TRY
    BEGIN CATCH
        PRINT 'ERROR in Bronze: ' + ERROR_MESSAGE();
        INSERT INTO control.pipeline_log
            (pipeline_name, start_time, end_time, rows_processed, status, error_message)
        VALUES ('bronze.usp_load_bronze', @start, GETDATE(), 0, 'failed', ERROR_MESSAGE());
    END CATCH
END;
GO



--silver

CREATE OR ALTER PROCEDURE silver.usp_load_silver
    @load_type NVARCHAR(20) = 'snapshot'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start       DATETIME = GETDATE();
    DECLARE @stage_start DATETIME;
    DECLARE @duration    INT;
    DECLARE @total_rows  INT = 0;
    DECLARE @rows        INT = 0;

    PRINT '==========================================';
    PRINT ' SILVER | Mode: ' + UPPER(@load_type);
    PRINT '==========================================';

    BEGIN TRY

        -- CUSTOMERS
        SET @stage_start = GETDATE();
        PRINT '>>> silver.load_customers | ' + @load_type;
        IF @load_type = 'snapshot' TRUNCATE TABLE silver.customers_clean;
        INSERT INTO silver.customers_clean
        SELECT DISTINCT
            s.customer_id,
            s.customer_unique_id,
            s.customer_zip_code_prefix,
            UPPER(LTRIM(RTRIM(s.customer_city))),
            UPPER(s.customer_state),
            GETDATE()
        FROM bronze.bronze_customers s
        WHERE s.customer_id IS NOT NULL
        AND (@load_type = 'snapshot' OR NOT EXISTS (
            SELECT 1 FROM silver.customers_clean c
            WHERE c.customer_id = s.customer_id
        ));
        SET @rows = @@ROWCOUNT; SET @total_rows += @rows;
        SET @duration = DATEDIFF(SECOND, @stage_start, GETDATE());
        PRINT 'customers_clean done: ' + CAST(@rows AS NVARCHAR) + ' rows | ' + CAST(@duration AS NVARCHAR) + 's';

        -- ORDERS
        SET @stage_start = GETDATE();
        PRINT '>>> silver.load_orders | ' + @load_type;
        IF @load_type = 'snapshot' TRUNCATE TABLE silver.orders_clean;
        INSERT INTO silver.orders_clean
        SELECT
            s.order_id, s.customer_id, s.order_status,
            TRY_CAST(s.order_purchase_timestamp       AS DATETIME),
            TRY_CAST(s.order_approved_at              AS DATETIME),
            TRY_CAST(s.order_delivered_carrier_date   AS DATETIME),
            TRY_CAST(s.order_delivered_customer_date  AS DATETIME),
            TRY_CAST(s.order_estimated_delivery_date  AS DATETIME),
            GETDATE()
        FROM bronze.bronze_orders s
        WHERE s.order_id IS NOT NULL
        AND (@load_type = 'snapshot' OR NOT EXISTS (
            SELECT 1 FROM silver.orders_clean c
            WHERE c.order_id = s.order_id
        ));
        SET @rows = @@ROWCOUNT; SET @total_rows += @rows;
        SET @duration = DATEDIFF(SECOND, @stage_start, GETDATE());
        PRINT 'orders_clean done: ' + CAST(@rows AS NVARCHAR) + ' rows | ' + CAST(@duration AS NVARCHAR) + 's';

        -- ORDER ITEMS
        SET @stage_start = GETDATE();
        PRINT '>>> silver.load_order_items | ' + @load_type;
        IF @load_type = 'snapshot' TRUNCATE TABLE silver.order_items_clean;
        INSERT INTO silver.order_items_clean
        SELECT
            s.order_id, s.order_item_id, s.product_id, s.seller_id,
            TRY_CAST(s.shipping_limit_date AS DATETIME),
            TRY_CAST(s.price              AS DECIMAL(10,2)),
            TRY_CAST(s.freight_value      AS DECIMAL(10,2)),
            GETDATE()
        FROM bronze.bronze_order_items s
        WHERE s.order_id IS NOT NULL
        AND (@load_type = 'snapshot' OR NOT EXISTS (
            SELECT 1 FROM silver.order_items_clean c
            WHERE c.order_id = s.order_id
            AND   c.order_item_id = s.order_item_id
        ));
        SET @rows = @@ROWCOUNT; SET @total_rows += @rows;
        SET @duration = DATEDIFF(SECOND, @stage_start, GETDATE());
        PRINT 'order_items_clean done: ' + CAST(@rows AS NVARCHAR) + ' rows | ' + CAST(@duration AS NVARCHAR) + 's';

        -- PRODUCTS
        SET @stage_start = GETDATE();
        PRINT '>>> silver.load_products | ' + @load_type;
        IF @load_type = 'snapshot' TRUNCATE TABLE silver.products_clean;
        INSERT INTO silver.products_clean
        SELECT DISTINCT
            s.product_id,
            ISNULL(s.product_category_name, 'Unknown'),
            s.product_name_lenght,
            s.product_weight_g,
            s.product_length_cm,
            s.product_height_cm,
            s.product_width_cm,
            GETDATE()
        FROM bronze.bronze_products s
        WHERE s.product_id IS NOT NULL
        AND (@load_type = 'snapshot' OR NOT EXISTS (
            SELECT 1 FROM silver.products_clean c
            WHERE c.product_id = s.product_id
        ));
        SET @rows = @@ROWCOUNT; SET @total_rows += @rows;
        SET @duration = DATEDIFF(SECOND, @stage_start, GETDATE());
        PRINT 'products_clean done: ' + CAST(@rows AS NVARCHAR) + ' rows | ' + CAST(@duration AS NVARCHAR) + 's';

        -- PAYMENTS
        SET @stage_start = GETDATE();
        PRINT '>>> silver.load_payments | ' + @load_type;
        IF @load_type = 'snapshot' TRUNCATE TABLE silver.payments_clean;
        INSERT INTO silver.payments_clean
        SELECT
            s.order_id, s.payment_sequential, s.payment_type,
            s.payment_installments,
            TRY_CAST(s.payment_value AS DECIMAL(10,2)),
            GETDATE()
        FROM bronze.bronze_payments s
        WHERE s.order_id IS NOT NULL
        AND (@load_type = 'snapshot' OR NOT EXISTS (
            SELECT 1 FROM silver.payments_clean c
            WHERE c.order_id           = s.order_id
            AND   c.payment_sequential = s.payment_sequential
        ));
        SET @rows = @@ROWCOUNT; SET @total_rows += @rows;
        SET @duration = DATEDIFF(SECOND, @stage_start, GETDATE());
        PRINT 'payments_clean done: ' + CAST(@rows AS NVARCHAR) + ' rows | ' + CAST(@duration AS NVARCHAR) + 's';

        SET @duration = DATEDIFF(SECOND, @start, GETDATE());
        PRINT '==========================================';
        PRINT ' SILVER Done: ' + CAST(@total_rows AS NVARCHAR) + ' rows | ' + CAST(@duration AS NVARCHAR) + 's';
        PRINT '==========================================';

        INSERT INTO control.pipeline_log
            (pipeline_name, start_time, end_time, rows_processed, status)
        VALUES ('silver.usp_load_silver', @start, GETDATE(), @total_rows, 'success');

    END TRY
    BEGIN CATCH
        PRINT 'ERROR in Silver: ' + ERROR_MESSAGE();
        INSERT INTO control.pipeline_log
            (pipeline_name, start_time, end_time, rows_processed, status, error_message)
        VALUES ('silver.usp_load_silver', @start, GETDATE(), 0, 'failed', ERROR_MESSAGE());
    END CATCH
END;
GO

--gold

CREATE OR ALTER PROCEDURE gold.usp_load_gold
    @load_type NVARCHAR(20) = 'snapshot'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start       DATETIME = GETDATE();
    DECLARE @stage_start DATETIME;
    DECLARE @duration    INT;
    DECLARE @total_rows  INT = 0;
    DECLARE @rows        INT = 0;

    PRINT '==========================================';
    PRINT ' GOLD | Mode: ' + UPPER(@load_type);
    PRINT '==========================================';

    BEGIN TRY

        -- DIM CUSTOMERS
        SET @stage_start = GETDATE();
        PRINT '>>> gold.load_dim_customers | ' + @load_type;
        IF @load_type = 'snapshot' TRUNCATE TABLE gold.dim_customers;
        INSERT INTO gold.dim_customers (customer_id, customer_city, customer_state)
        SELECT s.customer_id, s.customer_city, s.customer_state
        FROM silver.customers_clean s
        WHERE (@load_type = 'snapshot' OR NOT EXISTS (
            SELECT 1 FROM gold.dim_customers g
            WHERE g.customer_id = s.customer_id
        ));
        SET @rows = @@ROWCOUNT; SET @total_rows += @rows;
        SET @duration = DATEDIFF(SECOND, @stage_start, GETDATE());
        PRINT 'dim_customers done: ' + CAST(@rows AS NVARCHAR) + ' rows | ' + CAST(@duration AS NVARCHAR) + 's';

        -- DIM PRODUCTS
        SET @stage_start = GETDATE();
        PRINT '>>> gold.load_dim_products | ' + @load_type;
        IF @load_type = 'snapshot' TRUNCATE TABLE gold.dim_products;
        INSERT INTO gold.dim_products (product_id, product_category_name, product_weight_g)
        SELECT s.product_id, s.product_category_name, s.product_weight_g
        FROM silver.products_clean s
        WHERE (@load_type = 'snapshot' OR NOT EXISTS (
            SELECT 1 FROM gold.dim_products g
            WHERE g.product_id = s.product_id
        ));
        SET @rows = @@ROWCOUNT; SET @total_rows += @rows;
        SET @duration = DATEDIFF(SECOND, @stage_start, GETDATE());
        PRINT 'dim_products done: ' + CAST(@rows AS NVARCHAR) + ' rows | ' + CAST(@duration AS NVARCHAR) + 's';

        -- FACT SALES
        SET @stage_start = GETDATE();
        PRINT '>>> gold.load_fact_sales | ' + @load_type;
        IF @load_type = 'snapshot' TRUNCATE TABLE gold.fact_sales;
        INSERT INTO gold.fact_sales (order_id, product_key, price, freight_value)
        SELECT oi.order_id, dp.product_key, oi.price, oi.freight_value
        FROM silver.order_items_clean oi
        JOIN gold.dim_products dp ON oi.product_id = dp.product_id
        WHERE (@load_type = 'snapshot' OR NOT EXISTS (
            SELECT 1 FROM gold.fact_sales fs
            WHERE fs.order_id    = oi.order_id
            AND   fs.product_key = dp.product_key
        ));
        SET @rows = @@ROWCOUNT; SET @total_rows += @rows;
        SET @duration = DATEDIFF(SECOND, @stage_start, GETDATE());
        PRINT 'fact_sales done: ' + CAST(@rows AS NVARCHAR) + ' rows | ' + CAST(@duration AS NVARCHAR) + 's';

        -- FACT ORDERS
        SET @stage_start = GETDATE();
        PRINT '>>> gold.load_fact_orders | ' + @load_type;
        IF @load_type = 'snapshot' TRUNCATE TABLE gold.fact_orders;
        INSERT INTO gold.fact_orders
            (order_id, customer_key, order_status,
             order_purchase_timestamp, order_delivered_customer_date)
        SELECT
            o.order_id, dc.customer_key, o.order_status,
            o.order_purchase_timestamp, o.order_delivered_customer_date
        FROM silver.orders_clean o
        JOIN gold.dim_customers dc ON o.customer_id = dc.customer_id
        WHERE (@load_type = 'snapshot' OR NOT EXISTS (
            SELECT 1 FROM gold.fact_orders fo
            WHERE fo.order_id = o.order_id
        ));
        SET @rows = @@ROWCOUNT; SET @total_rows += @rows;
        SET @duration = DATEDIFF(SECOND, @stage_start, GETDATE());
        PRINT 'fact_orders done: ' + CAST(@rows AS NVARCHAR) + ' rows | ' + CAST(@duration AS NVARCHAR) + 's';

        SET @duration = DATEDIFF(SECOND, @start, GETDATE());
        PRINT '==========================================';
        PRINT ' GOLD Done: ' + CAST(@total_rows AS NVARCHAR) + ' rows | ' + CAST(@duration AS NVARCHAR) + 's';
        PRINT '==========================================';

        INSERT INTO control.pipeline_log
            (pipeline_name, start_time, end_time, rows_processed, status)
        VALUES ('gold.usp_load_gold', @start, GETDATE(), @total_rows, 'success');

    END TRY
    BEGIN CATCH
        PRINT 'ERROR in Gold: ' + ERROR_MESSAGE();
        INSERT INTO control.pipeline_log
            (pipeline_name, start_time, end_time, rows_processed, status, error_message)
        VALUES ('gold.usp_load_gold', @start, GETDATE(), 0, 'failed', ERROR_MESSAGE());
    END CATCH
END;
GO

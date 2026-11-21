CREATE TABLE dim_user AS
SELECT
    user_id,
    user_name,
    city,
    register_time
FROM ods_user;

CREATE TABLE dim_product AS
SELECT
    product_id,
    product_name,
    category_name,
    price
FROM ods_product;

CREATE TABLE dwd_trade_order_detail AS
WITH order_dedup AS (
    SELECT *
    FROM (
        SELECT
            o.*,
            ROW_NUMBER() OVER (
                PARTITION BY o.order_id
                ORDER BY o.create_time DESC
            ) AS rn
        FROM ods_order o
        WHERE o.order_status IN ('PAID', 'COMPLETED')
    )
    WHERE rn = 1
),
detail_dedup AS (
    SELECT *
    FROM (
        SELECT
            d.*,
            ROW_NUMBER() OVER (
                PARTITION BY d.order_detail_id
                ORDER BY d.order_id
            ) AS rn
        FROM ods_order_detail d
        WHERE d.line_amount > 0
    )
    WHERE rn = 1
)
SELECT
    o.order_id,
    o.user_id,
    o.order_status,
    DATE(o.create_time) AS stat_date,
    d.order_detail_id,
    d.product_id,
    u.city,
    p.category_name,
    d.quantity,
    d.line_amount
FROM order_dedup o
JOIN detail_dedup d
  ON d.order_id = o.order_id
JOIN dim_user u
  ON u.user_id = o.user_id
JOIN dim_product p
  ON p.product_id = d.product_id;

CREATE TABLE dws_user_trade_daily AS
SELECT
    stat_date,
    user_id,
    city,
    COUNT(DISTINCT order_id) AS paid_order_count,
    SUM(line_amount) AS trade_amount
FROM dwd_trade_order_detail
GROUP BY stat_date, user_id, city;

CREATE TABLE ads_trade_daily AS
SELECT
    stat_date,
    ROUND(SUM(line_amount), 2) AS gmv,
    COUNT(DISTINCT order_id) AS paid_order_count,
    ROUND(SUM(line_amount) / COUNT(DISTINCT order_id), 2) AS avg_order_value
FROM dwd_trade_order_detail
GROUP BY stat_date;


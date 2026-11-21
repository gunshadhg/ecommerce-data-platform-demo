CREATE VIEW dq_check_results AS
SELECT
    'duplicate_order_ids' AS rule_name,
    SUM(duplicate_rows) AS issue_count,
    'Synthetic duplicate order 9004' AS details
FROM (
    SELECT order_id, COUNT(*) - 1 AS duplicate_rows
    FROM ods_order
    GROUP BY order_id
    HAVING COUNT(*) > 1
)
UNION ALL
SELECT
    'negative_line_amounts',
    COUNT(*),
    'line_amount <= 0'
FROM ods_order_detail
WHERE line_amount <= 0
UNION ALL
SELECT
    'orphan_orders',
    COUNT(*),
    'order.user_id not found in dim_user'
FROM ods_order o
LEFT JOIN dim_user u
  ON u.user_id = o.user_id
WHERE u.user_id IS NULL
UNION ALL
SELECT
    'invalid_order_status',
    COUNT(*),
    'UNKNOWN order status'
FROM ods_order
WHERE order_status NOT IN ('CREATED', 'PAID', 'COMPLETED', 'CANCELLED')
UNION ALL
SELECT
    'duplicate_order_detail_ids',
    SUM(duplicate_rows),
    'Synthetic duplicate order_detail_id 5'
FROM (
    SELECT order_detail_id, COUNT(*) - 1 AS duplicate_rows
    FROM ods_order_detail
    GROUP BY order_detail_id
    HAVING COUNT(*) > 1
);

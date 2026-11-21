DROP VIEW IF EXISTS dq_check_results;
DROP TABLE IF EXISTS ads_trade_daily;
DROP TABLE IF EXISTS dws_user_trade_daily;
DROP TABLE IF EXISTS dwd_trade_order_detail;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_user;
DROP TABLE IF EXISTS ods_event_log;
DROP TABLE IF EXISTS ods_product;
DROP TABLE IF EXISTS ods_user;
DROP TABLE IF EXISTS ods_order_detail;
DROP TABLE IF EXISTS ods_order;

CREATE TABLE ods_user (
    user_id INTEGER,
    user_name TEXT,
    city TEXT,
    register_time TEXT
);

INSERT INTO ods_user VALUES
    (1001, 'Alice', '上海', '2026-10-20 09:00:00'),
    (1002, 'Bob',   '北京', '2026-10-22 10:00:00'),
    (1003, 'Chen',  '上海', '2026-10-28 11:00:00'),
    (1004, 'Dan',   '北京', '2026-10-29 12:00:00');

CREATE TABLE ods_product (
    product_id INTEGER,
    product_name TEXT,
    category_name TEXT,
    price REAL
);

INSERT INTO ods_product VALUES
    (1101, '机械键盘', '数码', 200.00),
    (1102, '无线鼠标', '数码', 100.00),
    (1103, '显示器',   '数码', 800.00),
    (1104, '手机',   '数码', 8000.00);

CREATE TABLE ods_order (
    order_id INTEGER,
    user_id INTEGER,
    order_status TEXT,
    create_time TEXT
);

INSERT INTO ods_order VALUES
    (9001, 1001, 'PAID',      '2026-11-01 09:00:00'),
    (9002, 1002, 'PAID',      '2026-11-01 10:00:00'),
    (9003, 1003, 'CREATED',   '2026-11-02 09:00:00'),
    (9004, 1001, 'PAID',      '2026-11-02 11:00:00'),
    (9004, 1001, 'PAID',      '2026-11-02 11:00:00'),
    (9005, 9999, 'PAID',      '2026-11-02 12:00:00'),
    (9006, 1002, 'UNKNOWN',   '2026-11-02 13:00:00'),
    (9007, 1004, 'PAID',   '2026-11-03 13:00:00');

CREATE TABLE ods_order_detail (
    order_detail_id INTEGER,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    line_amount REAL
);

INSERT INTO ods_order_detail VALUES
    (1, 9001, 1101, 1, 300.00),
    (2, 9002, 1102, 2, 200.00),
    (3, 9003, 1103, 1, 800.00),
    (4, 9004, 1101, 1, 200.00),
    (5, 9004, 1102, 1, 100.00),
    (5, 9004, 1102, 1, 100.00),
    (6, 9005, 1102, 1, 100.00),
    (7, 9006, 1103, 1, -50.00),
    (8, 9007, 1104, 1, 8000.00);

CREATE TABLE ods_event_log (
    event_id INTEGER,
    user_id INTEGER,
    event_type TEXT,
    event_time TEXT,
    product_id INTEGER
);

INSERT INTO ods_event_log VALUES
    (1, 1001, 'view', '2026-11-01 08:50:00', 1101),
    (2, 1001, 'add_cart', '2026-11-01 08:55:00', 1101),
    (3, 1001, 'pay', '2026-11-01 09:00:00', 1101),
    (4, 1002, 'view', '2026-11-01 09:45:00', 1102),
    (5, 1002, 'pay', '2026-11-01 10:00:00', 1102),
    (6, 1004, 'pay', '2026-11-03 13:00:00', 1104);

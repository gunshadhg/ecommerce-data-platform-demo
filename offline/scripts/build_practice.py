from __future__ import annotations

import sqlite3
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_DIR / "warehouse.db"
SQL_FILES = [
    PROJECT_DIR / "sql" / "01_source_and_ods.sql",
    PROJECT_DIR / "sql" / "02_model.sql",
    PROJECT_DIR / "sql" / "03_metrics_and_quality.sql",
]


def print_rows(title: str, headers: list[str], rows: list[tuple]) -> None:
    print(f"\n===== {title} =====")
    print(" | ".join(headers))
    print("-" * 80)
    if not rows:
        print("(no rows)")
        return
    for row in rows:
        print(" | ".join("NULL" if value is None else str(value) for value in row))


def main() -> None:
    for sql_file in SQL_FILES:
        if not sql_file.exists():
            raise FileNotFoundError(f"Missing SQL file: {sql_file}")

    connection = sqlite3.connect(DB_PATH)
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        for sql_file in SQL_FILES:
            connection.executescript(sql_file.read_text(encoding="utf-8"))

        ads_rows = connection.execute(
            """
            SELECT stat_date, gmv, paid_order_count, avg_order_value
            FROM ads_trade_daily
            ORDER BY stat_date
            """
        ).fetchall()
        print_rows(
            "ADS daily trade metrics",
            ["stat_date", "gmv", "paid_order_count", "avg_order_value"],
            ads_rows,
        )

        dq_rows = connection.execute(
            """
            SELECT rule_name, issue_count, details
            FROM dq_check_results
            ORDER BY rule_name
            """
        ).fetchall()
        print_rows(
            "Data quality checks",
            ["rule_name", "issue_count", "details"],
            dq_rows,
        )

        reconciliation_rows = connection.execute(
            """
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
            ),
            source_paid AS (
                SELECT
                    DATE(o.create_time) AS stat_date,
                    ROUND(SUM(d.line_amount), 2) AS source_gmv,
                    COUNT(DISTINCT o.order_id) AS source_paid_order_count
                FROM order_dedup o
                JOIN detail_dedup d
                  ON d.order_id = o.order_id
                JOIN ods_user u
                  ON u.user_id = o.user_id
                JOIN ods_product p
                  ON p.product_id = d.product_id
                WHERE o.order_status IN ('PAID', 'COMPLETED')
                  AND d.line_amount > 0
                GROUP BY DATE(o.create_time)
            )
            SELECT
                a.stat_date,
                a.gmv AS ads_gmv,
                ROUND(s.source_gmv, 2) AS source_gmv,
                ROUND(a.gmv - s.source_gmv, 2) AS gmv_diff,
                a.paid_order_count AS ads_order_count,
                s.source_paid_order_count AS source_order_count
            FROM ads_trade_daily a
            JOIN source_paid s
              ON s.stat_date = a.stat_date
            ORDER BY a.stat_date
            """
        ).fetchall()
        print_rows(
            "Source-to-ADS reconciliation",
            [
                "stat_date",
                "ads_gmv",
                "source_gmv",
                "gmv_diff",
                "ads_order_count",
                "source_order_count",
            ],
            reconciliation_rows,
        )

        print("\nPractice completed successfully.")
    finally:
        connection.close()


if __name__ == "__main__":
    main()


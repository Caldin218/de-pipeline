import psycopg2
import pandas as pd
import pathlib
from configs.db_config import DB_CONFIG


def load_table(conn, csv_path, table_name):
    df = pd.read_csv(csv_path)

    with conn.cursor() as cur:
        # Tạo bảng nếu chưa tồn tại
        cols = ", ".join([f"{c} TEXT" for c in df.columns])
        cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({cols});")

        # Xoá dữ liệu cũ
        cur.execute(f"DELETE FROM {table_name};")

        # Insert dữ liệu mới
        for _, row in df.iterrows():
            cur.execute(
                f"INSERT INTO {table_name} VALUES ({','.join(['%s']*len(row))})",
                tuple(row)
            )

    conn.commit()
    print(f"[load] {csv_path} -> {table_name}")


def main():
    conn = psycopg2.connect(**DB_CONFIG)

    SILVER = pathlib.Path("data/silver")

    load_table(conn, SILVER / "daily_orders.csv", "fact_daily_orders")
    load_table(conn, SILVER / "daily_revenue.csv", "fact_daily_revenue")
    load_table(conn, SILVER / "daily_active_users.csv", "fact_daily_active_users")
    load_table(conn, SILVER / "daily_active_users_by_channel.csv", "agg_dau_by_channel")
    load_table(conn, SILVER / "daily_revenue_by_channel.csv", "agg_revenue_by_channel")
    load_table(conn, SILVER / "daily_conversion.csv", "agg_daily_conversion")

    conn.close()
    print("[load] done, data is now in RDS PostgreSQL")


if __name__ == "__main__":
    main()

from sqlalchemy import text

from src.db import get_database_info, get_engine, mask_database_url


MART_TABLES = [
    "mart_demand_stats",
    "mart_inventory_position",
    "mart_network_flow",
    "mart_cost_to_serve",
    "mart_data_quality_report",
]


def main() -> int:
    db_info = get_database_info()
    if not db_info["detected"]:
        print("DATABASE_URL missing")
        return 1

    print(f"DATABASE_URL source: {db_info.get('source')}")
    if db_info.get("env_file"):
        print(f"ENV file: {db_info['env_file']}")
    print(f"Masked DATABASE_URL: {mask_database_url()}")
    print(f"Host: {db_info.get('host')}")
    print(f"Port: {db_info.get('port')}")
    print(f"Database: {db_info.get('database')}")
    print(f"User: {db_info.get('username')}")
    print(f"Driver: {db_info.get('driver')}")

    if db_info.get("invalid_placeholder"):
        print("Invalid placeholder DATABASE_URL detected")
        return 1

    if db_info.get("error"):
        print(f"DATABASE_URL parse error: {db_info['error']}")
        return 1

    try:
        with get_engine().connect() as connection:
            for table_name in MART_TABLES:
                result = connection.execute(
                    text(f"SELECT COUNT(*)::bigint FROM public.{table_name}")
                )
                row_count = result.scalar_one()
                print(f"{table_name}: {row_count}")
    except Exception as exc:
        print(f"DB CONNECTION FAILED: {exc}")
        print("If your password contains @, encode it as %40 inside DATABASE_URL.")
        return 1

    print("DB CONNECTED OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

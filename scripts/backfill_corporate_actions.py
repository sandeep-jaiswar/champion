from datetime import date, timedelta

from champion.orchestration.flows.corporate_actions_flow import corporate_actions_etl_flow

start = date(2024, 1, 1)
end = date.today()

# Use environment ClickHouse creds; ensure they are set in the environment when running this script.
print(f"Backfilling corporate actions from {start} to {end}")

cur = start
while cur <= end:
    print(f"Running ETL for {cur}")
    try:
        corporate_actions_etl_flow(effective_date=cur, load_to_clickhouse=True)
    except Exception as e:
        print(f"ETL failed for {cur}: {e}")
    cur = cur + timedelta(days=1)

print("Backfill complete")

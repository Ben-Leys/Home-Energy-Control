import sqlite3
from datetime import datetime

# To convert sqldump from previous DB to new DB
# Place in home_energy.db folder

database_file = "home_energy.db"

conn = sqlite3.connect(database_file)
cursor = conn.cursor()

cursor.execute("SELECT timestamp_utc FROM p1_meter_log")
rows = cursor.fetchall()

updates = []
for row in rows:
    raw_datetime = row[0]
    if raw_datetime and not raw_datetime.endswith(":00+00:00"):
        continue
    # Convert to ISO8601 with milliseconds and timezone
    if raw_datetime:
        parsed_datetime = datetime.strptime(raw_datetime, "%Y-%m-%dT%H:%M:%S")
        iso_datetime = parsed_datetime.isoformat() + ".000000+00:00"  # Add timezone info
        updates.append((iso_datetime, raw_datetime))

cursor.executemany("UPDATE p1_meter_log SET timestamp_utc = ? WHERE timestamp_utc = ?", updates)

conn.commit()
conn.close()

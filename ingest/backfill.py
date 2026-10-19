from __future__ import annotations
import datetime as dt

def parse_hour(s: str) -> dt.datetime:
    """Parse 'YYYY-MM-DDTHH' (e.g. 2026-09-01T05) into a timezone-aware UTC datetime."""
    naive = dt.datetime.strptime(s, '%Y-%m-%dT%H')
    return dt.datetime(naive.year, naive.month, naive.day, naive.hour, tzinfo=dt.timezone.utc)

def hour_range(start: dt.datetime, end: dt.datetime) -> list[tuple[str, int]]:
    """Inclusive list of (date_iso, hour) pairs, one per hour from start to end."""
    if end < start:
        raise ValueError("end is before start")
    hours = []
    current = start
    while current <= end:
        hours.append((current.strftime('%Y-%m-%d'), current.hour))
        current += dt.timedelta(hours=1)
    return hours

INGESTED, ABSENT, FAILED = "ingested", "absent", "failed"

def partition_key(path: str) -> tuple[str, int]:
    """From a path like s3://b/bronze/events/dt=2026-09-01/hour=05/data.parquet return ('2026-09-01', 5)."""
    parts = path.split('/')
    date_part = next(part for part in parts if part.startswith('dt='))
    hour_part = next(part for part in parts if part.startswith('hour='))
    date_iso = date_part.split('=')[1]
    hour = int(hour_part.split('=')[1])
    return (date_iso, hour)

def plan(keys, statuses, present, mode) -> list:
    """Return the hours to process, in input order."""
    if mode == "force":
        return keys
    elif mode == "resume":
        return [key for key in keys if key not in statuses or statuses[key] == FAILED or (statuses[key] == INGESTED and key not in present)]
    elif mode == "retry-absent":
        return [key for key in keys if key not in statuses or statuses[key] in {FAILED, ABSENT} and key not in present]
    else:
        raise ValueError(f"Unknown mode: {mode}")

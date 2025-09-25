import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo  # py>=3.9

from src.ingestion.CsvFileStream import CSVInputStream, BurstyInputStream

NY_TZ = ZoneInfo("America/New_York")
_CITY_BIKE_CSV=os.path.abspath("./data/citibike_data.csv")


def parse_local_to_utc_ms(ts: str, tz: ZoneInfo = NY_TZ) -> int:
    """
    Parse 'YYYY-MM-DD HH:MM:SS' assumed local to `tz` and convert to UTC epoch ms.
    Handles DST via zoneinfo. If an ambiguous time occurs (fall-back), we take fold=0
    (first occurrence). Adjust policy if you need otherwise.
    """
    # Fast, strict format (citibike legacy format)
    dt_naive = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    dt_local = dt_naive.replace(tzinfo=tz)
    dt_utc = dt_local.astimezone(timezone.utc)
    return int(dt_utc.timestamp() * 1000)

def to_epoch_ms(ts: str)-> int:
    #TODO double check this
    dt = datetime.fromisoformat(ts.replace(" ", "T"))
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)

def as_int(x, default=None):
    try:
        return int(x)
    except Exception:
        return default

def as_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def citi_bike_row_2_event(row: dict) -> dict:
    start_ms = parse_local_to_utc_ms(row["Start Time"])
    stop_ms  = parse_local_to_utc_ms(row["Stop Time"])

    return {
        # raw strings
        "start_time": row["Start Time"],
        "stop_time": row["Stop Time"],

        # canonical (UTC) millis
        "start_time_ms": start_ms,
        "stop_time_ms": stop_ms,

        # choose the event timestamp OpenCEP will use
        # (your DataFormatter.get_event_timestamp should return this)
        "event_time_ms": start_ms,

        # duration (keep both string source & computed if present)
        "trip_duration_sec": as_int(row.get("Trip Duration")),

        # Start Station
        "start_station_id": as_int(row["Start Station ID"]),
        "start_station_name": row["Start Station Name"],
        "start_station_lat": as_float(row["Start Station Latitude"]),
        "start_station_long": as_float(row["Start Station Longitude"]),

        # End Station
        "end_station_id": as_int(row["End Station ID"]),
        "end_station_name": row["End Station Name"],
        "end_station_lat": as_float(row["End Station Latitude"]),
        "end_station_long": as_float(row["End Station Longitude"]),

        # Bike & user
        "bike_id": as_int(row["Bike ID"]),
        "user_type": row.get("User Type"),
        "birth_year": as_int(row.get("Birth Year")),
        "gender": row.get("Gender"),

        # event typing for OpenCEP
        "type": "BikeTrip",   # if your DataFormatter derives type from payload, this helps
    }


def citi_bike_stream(limit=None):
    return CSVInputStream(file_path=_CITY_BIKE_CSV,row_to_event=citi_bike_row_2_event, limit=limit)


def citi_bike_bursty(limit=None):
    pass # TODO
    #return BurstyInputStream



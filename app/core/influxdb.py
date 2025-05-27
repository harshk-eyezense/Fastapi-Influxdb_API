
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.delete_api import DeleteApi
from app.core.config import settings
import datetime
from typing import List, Dict, Any, Union

# Initialize InfluxDB Client (singleton pattern if you prefer, but this is fine for a small app)
_client: InfluxDBClient = None

def get_influxdb_client() -> InfluxDBClient:
    """Returns the InfluxDB client, creating it if it doesn't exist."""
    global _client
    if _client is None:
        _client = InfluxDBClient(url=settings.INFLUXDB_URL, token=settings.INFLUXDB_TOKEN, org=settings.INFLUXDB_ORG)
    return _client

def get_write_api():
    """Returns the InfluxDB WriteApi instance."""
    return get_influxdb_client().write_api(write_options=SYNCHRONOUS)

def get_query_api():
    """Returns the InfluxDB QueryApi instance."""
    return get_influxdb_client().query_api()

def get_delete_api() -> DeleteApi:
    """Returns the InfluxDB DeleteApi instance."""
    return get_influxdb_client().delete_api()

def write_point(measurement_name: str, tags: Dict[str, str], fields: Dict[str, Any], timestamp: datetime.datetime = None):
    """
    Writes a single data point to InfluxDB.
    """
    point = Point(measurement_name)
    for tag_key, tag_value in tags.items():
        point = point.tag(tag_key, tag_value)
    for field_key, field_value in fields.items():
        point = point.field(field_key, field_value)
    if timestamp:
        point = point.time(timestamp)

    get_write_api().write(bucket=settings.INFLUXDB_BUCKET, org=settings.INFLUXDB_ORG, record=point)
    print(f"Data written: {point.to_line_protocol()}")

def query_flux(flux_query: str) -> List[Dict[str, Any]]:
    """
    Queries data from InfluxDB using Flux and returns a list of dictionaries.
    """
    tables = get_query_api().query(flux_query, org=settings.INFLUXDB_ORG)
    results = []
    for table in tables:
        for record in table.records:
            # record.values contains _time, _measurement, _field, _value, and all tags
            results.append(record.values)
    return results

def delete_data_range(
    measurement_name: str,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    predicate: str = ""
):
    """
    Deletes data from InfluxDB within a specified time range and optional predicate.
    """
    get_delete_api().delete(
        start=start_time,
        stop=end_time,
        predicate=f'_measurement="{measurement_name}" {predicate}',
        bucket=settings.INFLUXDB_BUCKET,
        org=settings.INFLUXDB_ORG
    )
    print(f"Data deleted for measurement '{measurement_name}' from {start_time} to {end_time} with predicate '{predicate}'")

def close_influxdb_client():
    """Closes the InfluxDB client connection."""
    global _client
    if _client:
        _client.close()
        _client = None # Reset client
        print("InfluxDB client closed.")

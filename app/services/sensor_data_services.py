
from app.core import influxdb
from app.models.sensor_data import SensorDataCreate, SensorDataResponse, DeleteSensorDataRequest
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

class SensorDataService:
    def __init__(self):
        self.client = influxdb.get_influxdb_client()
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()
        self.bucket = influxdb.settings.INFLUXDB_BUCKET
        self.org = influxdb.settings.INFLUXDB_ORG

    def create_sensor_data(self, data: SensorDataCreate):
        point = (
            influxdb.influxdb_client.Point(data.measurement)
            .tag("host", "fastapi_app") # Default tag
        )

        for key, value in data.tags.items():
            point.tag(key, value)

        for key, value in data.fields.items():
            point.field(key, value)

        if data.timestamp:
            if data.timestamp.tzinfo is None:
                point.time(data.timestamp.replace(tzinfo=timezone.utc))
            else:
                point.time(data.timestamp)
        else:
            pass # InfluxDB will use server's current UTC time

        self.write_api.write(bucket=self.bucket, org=self.org, record=point)


    def get_sensor_data(
        self,
        measurement: str,
        start_time: datetime,
        end_time: datetime,
        tag_filters: Optional[Dict[str, str]] = None,
        limit: int = 100
    ) -> List[SensorDataResponse]:
        query_start = start_time.astimezone(timezone.utc).isoformat()
        query_end = end_time.astimezone(timezone.utc).isoformat()

        flux_query = f'''
        from(bucket: "{self.bucket}")
          |> range(start: {query_start}, stop: {query_end})
          |> filter(fn: (r) => r._measurement == "{measurement}")
        '''

        if tag_filters:
            for tag_key, tag_value in tag_filters.items():
                flux_query += f'  |> filter(fn: (r) => r.{tag_key} == "{tag_value}")\n'

        flux_query += f'''
          |> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value")
          |> limit(n: {limit})
        '''

        tables = self.query_api.query(flux_query, org=self.org)
        results: List[SensorDataResponse] = []

        # Define keys that are ALWAYS internal InfluxDB/Flux metadata
        INTERNAL_FLUX_KEYS = {"_time", "_measurement", "_start", "_stop", "result", "table", "_value", "_field"}

        # Define your expected tag keys here.
        # This is CRUCIAL for correctly separating tags from fields after pivoting.
        # Add any other tag keys you might use in your data.
        EXPECTED_TAG_KEYS = {"host", "location", "device_id", "room_type", "sensor_id"}

        for table in tables:
            for record in table.records:
                current_tags = {}
                current_fields = {}

                # Iterate through all key-value pairs in the FluxRecord's values
                for key, value in record.values.items():
                    if key in INTERNAL_FLUX_KEYS:
                        continue # Skip internal metadata

                    if key in EXPECTED_TAG_KEYS:
                        current_tags[key] = value
                    else:
                        # If it's not an internal key and not a known tag key, it's a field
                        current_fields[key] = value

                # Ensure timestamp is timezone-aware UTC
                record_time_utc = record.get_time().astimezone(timezone.utc)

                results.append(
                    SensorDataResponse(
                        measurement=record.get_measurement(),
                        tags=current_tags,
                        fields=current_fields,
                        timestamp=record_time_utc
                    )
                )
        return results

    def update_sensor_data(
        self,
        measurement: str,
        tag_key: str,
        tag_value: str,
        new_fields: Dict[str, Any],
        start_time: datetime,
        end_time: Optional[datetime] = None
    ) -> bool:
        query_start = start_time.astimezone(timezone.utc).isoformat()
        query_end = (end_time if end_time else datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()

        flux_query = f'''
        from(bucket: "{self.bucket}")
          |> range(start: {query_start}, stop: {query_end})
          |> filter(fn: (r) => r._measurement == "{measurement}")
          |> filter(fn: (r) => r.{tag_key} == "{tag_value}")
          |> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value")
        '''
        tables = self.query_api.query(flux_query, org=self.org)

        updated_count = 0
        INTERNAL_FLUX_KEYS = {"_time", "_measurement", "_start", "_stop", "result", "table", "_value", "_field"}
        EXPECTED_TAG_KEYS = {"host", "location", "device_id", "room_type", "sensor_id"} # Make sure this matches above

        for table in tables:
            for record in table.records:
                current_tags = {}
                original_fields = {}

                for key, value in record.values.items():
                    if key in INTERNAL_FLUX_KEYS:
                        continue
                    if key in EXPECTED_TAG_KEYS:
                        current_tags[key] = value
                    else:
                        original_fields[key] = value

                merged_fields = {**original_fields, **new_fields}

                point = (
                    influxdb.influxdb_client.Point(measurement)
                    .time(record.get_time().astimezone(timezone.utc))
                )
                for k, v in current_tags.items():
                    point.tag(k, v)
                for k, v in merged_fields.items():
                    point.field(k, v)

                self.write_api.write(bucket=self.bucket, org=self.org, record=point)
                updated_count += 1
        return updated_count > 0


    def delete_sensor_data(self, params: DeleteSensorDataRequest):
        start = params.start_time.astimezone(timezone.utc).isoformat()
        stop = params.end_time.astimezone(timezone.utc).isoformat()

        predicate = f'_measurement="{params.measurement}"'

        if params.tags:
            for k, v in params.tags.items():
                predicate += f' AND {k}="{v}"'

        if params.fields:
             for k, v in params.fields.items():
                if isinstance(v, str):
                    predicate += f' AND {k}="{v}"'
                else:
                    predicate += f' AND {k}={v}'

        delete_api = self.client.delete_api()
        delete_api.delete(
            start=start,
            stop=stop,
            predicate=predicate,
            bucket=self.bucket,
            org=self.org
        )

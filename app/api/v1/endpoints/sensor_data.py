
from fastapi import APIRouter, HTTPException, status, Depends
from typing import List, Dict, Any, Optional
from datetime import datetime
from app.models.sensor_data import SensorDataCreate, SensorDataResponse, DeleteSensorDataRequest
from app.services.sensor_data_services import SensorDataService

router = APIRouter()

# Dependency to get a SensorDataService instance (could be injected with FastAPI's Depends)
def get_sensor_data_service() -> SensorDataService:
    return SensorDataService()

@router.post("/", response_model=Dict[str, str], status_code=status.HTTP_201_CREATED)
async def create_sensor_data(
    data: SensorDataCreate,
    service: SensorDataService = Depends(get_sensor_data_service)
):
    """
    Create (Write) a new sensor data point to InfluxDB.
    """
    try:
        service.create_sensor_data(data)
        return {"message": "Data point created successfully."}
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to write data: {e}")

@router.get("/", response_model=List[SensorDataResponse])
async def read_sensor_data(
    measurement: str,
    start_time: str, # Use string for query params, parse in function
    end_time: Optional[str] = None,
    location: Optional[str] = None, # Example tag filter
    sensor_id: Optional[str] = None, # Example tag filter
    limit: int = 100,
    service: SensorDataService = Depends(get_sensor_data_service)
):
    """
    Read sensor data from InfluxDB based on measurement, time range, and optional tags.
    Time values should be in ISO 8601 format (e.g., '2023-10-27T10:00:00Z').
    """
    try:
        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00')) # Handle Z for UTC
        end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00')) if end_time else datetime.now()

        tag_filters = {}
        if location:
            tag_filters['location'] = location
        if sensor_id:
            tag_filters['sensor_id'] = sensor_id

        data = service.get_sensor_data(measurement, start_dt, end_dt, tag_filters, limit)
        return data

    except ValueError as ve:
        print(f"ValueError in read_sensor_data: {ve}") 
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid time format: {ve}. Use ISO 8601 format (e.g., '2023-10-27T10:00:00Z').")
    except Exception as e:
        print(f"An unexpected error occurred in read_sensor_data: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to query data: {e}")

@router.put("/{measurement_name}/{tag_key}/{tag_value}", response_model=Dict[str, str])
async def update_sensor_data(
    measurement_name: str,
    tag_key: str,
    tag_value: str,
    new_fields: Dict[str, Any],
    start_time: datetime,
    end_time: Optional[datetime] = None,
    service: SensorDataService = Depends(get_sensor_data_service)
):
    """
    Update (Overwrite) sensor data in InfluxDB.
    Note: In InfluxDB, "updates" are typically done by writing new points with the same timestamp
    and tags, which overwrites existing field values.
    """
    try:
        updated = service.update_sensor_data(
            measurement_name,
            tag_key,
            tag_value,
            new_fields,
            start_time,
            end_time
        )
        if not updated:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No data found for the given criteria to update.")
        return {"message": "Data points updated successfully (overwritten)."}
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to update data: {e}")

@router.delete("/", response_model=Dict[str, str])
async def delete_sensor_data(
    params: DeleteSensorDataRequest,
    service: SensorDataService = Depends(get_sensor_data_service)
):
    """
    Delete sensor data from InfluxDB within a specified time range and optional predicate.
    """
    try:
        service.delete_sensor_data(params)
        return {"message": "Data deleted successfully."}
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to delete data: {e}")

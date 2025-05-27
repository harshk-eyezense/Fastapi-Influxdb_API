# app/models/sensor_data.py

from datetime import datetime
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field

# Pydantic model for data creation (POST request body)
class SensorDataCreate(BaseModel):
    measurement: str
    tags: Dict[str, str] = Field(default_factory=dict) # Tags should generally be strings
    fields: Dict[str, Any] # Fields can be various types (float, int, bool, string)
    timestamp: Optional[datetime] = None # Optional for POST, InfluxDB assigns if not provided

# Pydantic model for data response (GET request return value)
class SensorDataResponse(BaseModel):
    measurement: str
    tags: Dict[str, str]
    fields: Dict[str, Any]
    timestamp: datetime # THIS IS THE CRUCIAL FIX: Ensure it's named 'timestamp' and is a datetime

    class Config:
        # This tells Pydantic to allow assignment by field name (e.g., from ORM objects)
        # instead of requiring a dict with exact keys for model instantiation.
        from_attributes = True # Pydantic v2 equivalent of orm_mode = True for older versions

# Pydantic model for deleting sensor data (DELETE request body)
class DeleteSensorDataRequest(BaseModel):
    measurement: str
    start_time: datetime
    end_time: datetime
    tags: Optional[Dict[str, str]] = None
    fields: Optional[Dict[str, Any]] = None # Optional: to delete specific field values
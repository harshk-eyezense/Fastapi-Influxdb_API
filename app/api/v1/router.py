from fastapi import APIRouter
from app.api.v1.endpoints import sensor_data

api_router = APIRouter() 
api_router.include_router(sensor_data.router, prefix="/sensor-data", tags=["Sensor Data"])
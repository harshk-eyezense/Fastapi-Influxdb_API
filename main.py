
from fastapi import FastAPI
from dotenv import load_dotenv
from app.api.v1.router import api_router
from app.core.influxdb import close_influxdb_client
from app.core.config import settings

load_dotenv()

app = FastAPI( # <--- THIS MUST BE HERE AND SPELLED 'app'
    title="Modular InfluxDB FastAPI CRUD API",
    description="A FastAPI application demonstrating modular CRUD operations with InfluxDB.",
    version="1.0.0",
)

app.include_router(api_router, prefix="/api/v1")

@app.on_event("startup")
async def startup_event():
    print(f"Loading InfluxDB config from: {settings.INFLUXDB_URL}, Org: {settings.INFLUXDB_ORG}, Bucket: {settings.INFLUXDB_BUCKET}")

@app.on_event("shutdown")
def shutdown_event():
    close_influxdb_client()

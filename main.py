from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.responses import Response
from pydantic import BaseModel
from sqlalchemy import Column, Integer, Float, String, select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import os
import uuid
import logging
from pythonjsonlogger import jsonlogger
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST

app = FastAPI(title="Trip Service")

DATABASE_URL = "sqlite+aiosqlite:///./trip_service.db"

DRIVER_SERVICE_URL = os.getenv("DRIVER_SERVICE_URL", "http://127.0.0.1:8002")
PAYMENT_SERVICE_URL = os.getenv("PAYMENT_SERVICE_URL", "http://127.0.0.1:8004")

engine = create_async_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

trips_requested_total = Counter(
    "trips_requested_total",
    "Total number of trip requests"
)

trips_completed_total = Counter(
    "trips_completed_total",
    "Total number of completed trips"
)

payments_failed_total = Counter(
    "payments_failed_total",
    "Total number of failed payments from Trip Service"
)

logger = logging.getLogger("trip-service")
logger.setLevel(logging.INFO)

log_handler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(
    "%(asctime)s %(levelname)s %(name)s %(message)s %(correlation_id)s"
)
log_handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(log_handler)


class Trip(Base):
    __tablename__ = "trips"

    id = Column(Integer, primary_key=True, index=True)
    rider_id = Column(Integer)
    driver_id = Column(Integer, nullable=True)
    pickup_location = Column(String)
    drop_location = Column(String)
    city = Column(String)
    distance_km = Column(Float)
    surge_multiplier = Column(Float)
    base_fare = Column(Float)
    fare_amount = Column(Float, nullable=True)
    status = Column(String)


class TripRequest(BaseModel):
    rider_id: int
    pickup_location: str
    drop_location: str
    city: str
    distance_km: float
    surge_multiplier: float = 1.0


class TripResponse(BaseModel):
    id: int
    rider_id: int
    driver_id: int | None
    pickup_location: str
    drop_location: str
    city: str
    distance_km: float
    surge_multiplier: float
    base_fare: float
    fare_amount: float | None
    status: str

    class Config:
        from_attributes = True


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_db():
    async with SessionLocal() as session:
        yield session


@app.middleware("http")
async def add_correlation_id(request: Request, call_next):
    import uuid
    correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
    request.state.correlation_id = correlation_id
    response = await call_next(request)
    response.headers["X-Correlation-ID"] = correlation_id
    return response


@app.on_event("startup")
async def startup_event():
    await init_db()


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError))
)
async def fetch_available_driver(city: str, correlation_id: str):
    async with httpx.AsyncClient(timeout=5.0) as client:
        response = await client.get(
            f"{DRIVER_SERVICE_URL}/v1/drivers/available",
            params={"city": city},
            headers={"X-Correlation-ID": correlation_id}
        )
        response.raise_for_status()
        return response.json()


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError))
)
async def process_payment(trip_id: int, amount: float, correlation_id: str, idempotency_key: str):
    async with httpx.AsyncClient(timeout=5.0) as client:
        response = await client.post(
            f"{PAYMENT_SERVICE_URL}/v1/payments/charge",
            json={
                "trip_id": trip_id,
                "amount": amount,
                "payment_method": "CARD"
            },
            headers={
                "Idempotency-Key": idempotency_key,
                "X-Correlation-ID": correlation_id
            }
        )
        response.raise_for_status()
        return response.json()


@app.get("/health")
def health():
    return {"service": "trip-service", "status": "UP"}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/v1/trips", response_model=list[TripResponse])
async def get_trips(request: Request, db: AsyncSession = Depends(get_db)):
    correlation_id = request.state.correlation_id

    logger.info(
        "Fetching all trips",
        extra={"correlation_id": correlation_id}
    )

    result = await db.execute(select(Trip))
    return result.scalars().all()


@app.get("/v1/trips/{trip_id}", response_model=TripResponse)
async def get_trip(trip_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    correlation_id = request.state.correlation_id

    logger.info(
        f"Fetching trip {trip_id}",
        extra={"correlation_id": correlation_id}
    )

    result = await db.execute(select(Trip).filter(Trip.id == trip_id))
    trip = result.scalars().first()

    if not trip:
        logger.error(
            f"Trip {trip_id} not found",
            extra={"correlation_id": correlation_id}
        )
        raise HTTPException(status_code=404, detail="Trip not found")

    return trip


@app.post("/v1/trips")
async def create_trip(request_data: TripRequest, request: Request, db: AsyncSession = Depends(get_db)):
    correlation_id = request.state.correlation_id

    logger.info(
        "Trip request received",
        extra={"correlation_id": correlation_id}
    )

    if request_data.surge_multiplier not in [1.0, 1.2, 1.5]:
        logger.error(
            "Invalid surge multiplier",
            extra={"correlation_id": correlation_id}
        )
        raise HTTPException(
            status_code=400,
            detail="Surge multiplier must be 1.0, 1.2, or 1.5"
        )

    try:
        driver = await fetch_available_driver(request_data.city, correlation_id)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            logger.error(
                "No active driver available",
                extra={"correlation_id": correlation_id}
            )
            raise HTTPException(
                status_code=400,
                detail="No active driver available"
            )
        raise HTTPException(status_code=500, detail="Driver Service error")
    except Exception as e:
        logger.error(
            f"Driver Service unavailable: {e}",
            extra={"correlation_id": correlation_id}
        )
        raise HTTPException(
            status_code=500,
            detail="Driver Service is not available"
        )

    trip = Trip(
        rider_id=request_data.rider_id,
        driver_id=driver["id"],
        pickup_location=request_data.pickup_location,
        drop_location=request_data.drop_location,
        city=request_data.city,
        distance_km=request_data.distance_km,
        surge_multiplier=request_data.surge_multiplier,
        base_fare=50,
        status="REQUESTED"
    )

    db.add(trip)
    await db.commit()
    await db.refresh(trip)

    trips_requested_total.inc()

    logger.info(
        f"Trip {trip.id} created successfully",
        extra={"correlation_id": correlation_id}
    )

    return {
        "message": "Trip requested successfully",
        "correlation_id": correlation_id,
        "assigned_driver": driver,
        "trip": trip
    }


@app.post("/v1/trips/{trip_id}/accept")
async def accept_trip(trip_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    correlation_id = request.state.correlation_id

    result = await db.execute(select(Trip).filter(Trip.id == trip_id))
    trip = result.scalars().first()

    if not trip:
        logger.error(
            f"Trip {trip_id} not found",
            extra={"correlation_id": correlation_id}
        )
        raise HTTPException(status_code=404, detail="Trip not found")

    if trip.status != "REQUESTED":
        logger.error(
            f"Trip {trip_id} cannot be accepted from status {trip.status}",
            extra={"correlation_id": correlation_id}
        )
        raise HTTPException(
            status_code=400,
            detail="Only REQUESTED trip can be accepted"
        )

    trip.status = "ACCEPTED"

    await db.commit()
    await db.refresh(trip)

    logger.info(
        f"Trip {trip_id} accepted successfully",
        extra={"correlation_id": correlation_id}
    )

    return {
        "message": "Trip accepted successfully",
        "correlation_id": correlation_id,
        "trip": trip
    }


@app.post("/v1/trips/{trip_id}/complete")
async def complete_trip(trip_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    correlation_id = request.state.correlation_id

    result = await db.execute(select(Trip).filter(Trip.id == trip_id))
    trip = result.scalars().first()

    if not trip:
        logger.error(
            f"Trip {trip_id} not found",
            extra={"correlation_id": correlation_id}
        )
        raise HTTPException(status_code=404, detail="Trip not found")

    if trip.status != "ACCEPTED":
        logger.error(
            f"Trip {trip_id} cannot be completed from status {trip.status}",
            extra={"correlation_id": correlation_id}
        )
        raise HTTPException(
            status_code=400,
            detail="Only ACCEPTED trip can be completed"
        )

    rate_per_km = 15
    fare = trip.base_fare + (
        trip.distance_km * rate_per_km * trip.surge_multiplier
    )
    trip.fare_amount = round(fare, 2)
    idempotency_key = f"trip-{trip.id}-payment"

    try:
        payment_result = await process_payment(
            trip_id=trip.id,
            amount=trip.fare_amount,
            correlation_id=correlation_id,
            idempotency_key=idempotency_key
        )

        trip.status = "COMPLETED"
        await db.commit()
        await db.refresh(trip)

        trips_completed_total.inc()

        logger.info(
            f"Trip {trip_id} completed and payment successful",
            extra={"correlation_id": correlation_id}
        )

        return {
            "message": "Trip completed and payment successful",
            "correlation_id": correlation_id,
            "trip": trip,
            "payment_response": payment_result
        }

    except Exception as e:
        trip.status = "PAYMENT_FAILED"
        await db.commit()
        await db.refresh(trip)

        payments_failed_total.inc()

        logger.error(
            f"Payment Service unavailable or failed: {e}",
            extra={"correlation_id": correlation_id}
        )

        return {
            "message": "Trip completed but Payment Service failed",
            "correlation_id": correlation_id,
            "trip": trip
        }


@app.post("/v1/trips/{trip_id}/cancel")
async def cancel_trip(trip_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    correlation_id = request.state.correlation_id

    result = await db.execute(select(Trip).filter(Trip.id == trip_id))
    trip = result.scalars().first()

    if not trip:
        raise HTTPException(status_code=404, detail="Trip not found")

    if trip.status == "COMPLETED":
        raise HTTPException(
            status_code=400,
            detail="Completed trip cannot be cancelled"
        )

    cancellation_fee = 0
    if trip.status == "ACCEPTED":
        cancellation_fee = 30

    trip.status = "CANCELLED"

    await db.commit()
    await db.refresh(trip)

    logger.info(
        f"Trip {trip_id} cancelled",
        extra={"correlation_id": correlation_id}
    )

    return {
        "message": "Trip cancelled successfully",
        "correlation_id": correlation_id,
        "cancellation_fee": cancellation_fee,
        "trip": trip
    }
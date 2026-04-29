from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, Float, String
from sqlalchemy.orm import declarative_base, sessionmaker
import requests
import os
import uuid
import logging
from pythonjsonlogger import jsonlogger
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST

app = FastAPI(title="Trip Service")

DATABASE_URL = "sqlite:///./trip_service.db"

DRIVER_SERVICE_URL = os.getenv("DRIVER_SERVICE_URL", "http://127.0.0.1:8002")
PAYMENT_SERVICE_URL = os.getenv("PAYMENT_SERVICE_URL", "http://127.0.0.1:8004")

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)
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


Base.metadata.create_all(bind=engine)


def get_correlation_id(request: Request):
    return request.headers.get("X-Correlation-ID", str(uuid.uuid4()))


@app.get("/health")
def health():
    return {"service": "trip-service", "status": "UP"}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/v1/trips")
def get_trips(request: Request):
    correlation_id = get_correlation_id(request)

    logger.info(
        "Fetching all trips",
        extra={"correlation_id": correlation_id}
    )

    db = SessionLocal()
    trips = db.query(Trip).all()
    db.close()
    return trips


@app.get("/v1/trips/{trip_id}")
def get_trip(trip_id: int, request: Request):
    correlation_id = get_correlation_id(request)

    logger.info(
        f"Fetching trip {trip_id}",
        extra={"correlation_id": correlation_id}
    )

    db = SessionLocal()
    trip = db.query(Trip).filter(Trip.id == trip_id).first()
    db.close()

    if not trip:
        logger.error(
            f"Trip {trip_id} not found",
            extra={"correlation_id": correlation_id}
        )
        raise HTTPException(status_code=404, detail="Trip not found")

    return trip


@app.post("/v1/trips")
def create_trip(request_data: TripRequest, request: Request):
    correlation_id = get_correlation_id(request)

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
        driver_response = requests.get(
            f"{DRIVER_SERVICE_URL}/v1/drivers/available",
            params={"city": request_data.city},
            headers={"X-Correlation-ID": correlation_id},
            timeout=5
        )

        if driver_response.status_code != 200:
            logger.error(
                "No active driver available",
                extra={"correlation_id": correlation_id}
            )
            raise HTTPException(
                status_code=400,
                detail="No active driver available"
            )

        driver = driver_response.json()

    except requests.exceptions.RequestException:
        logger.error(
            "Driver Service unavailable",
            extra={"correlation_id": correlation_id}
        )
        raise HTTPException(
            status_code=500,
            detail="Driver Service is not available"
        )

    db = SessionLocal()

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
    db.commit()
    db.refresh(trip)
    db.close()

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
def accept_trip(trip_id: int, request: Request):
    correlation_id = get_correlation_id(request)

    db = SessionLocal()
    trip = db.query(Trip).filter(Trip.id == trip_id).first()

    if not trip:
        db.close()
        logger.error(
            f"Trip {trip_id} not found",
            extra={"correlation_id": correlation_id}
        )
        raise HTTPException(status_code=404, detail="Trip not found")

    if trip.status != "REQUESTED":
        db.close()
        logger.error(
            f"Trip {trip_id} cannot be accepted from status {trip.status}",
            extra={"correlation_id": correlation_id}
        )
        raise HTTPException(
            status_code=400,
            detail="Only REQUESTED trip can be accepted"
        )

    trip.status = "ACCEPTED"

    db.commit()
    db.refresh(trip)
    db.close()

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
def complete_trip(trip_id: int, request: Request):
    correlation_id = get_correlation_id(request)

    db = SessionLocal()
    trip = db.query(Trip).filter(Trip.id == trip_id).first()

    if not trip:
        db.close()
        logger.error(
            f"Trip {trip_id} not found",
            extra={"correlation_id": correlation_id}
        )
        raise HTTPException(status_code=404, detail="Trip not found")

    if trip.status != "ACCEPTED":
        db.close()
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
        payment_response = requests.post(
            f"{PAYMENT_SERVICE_URL}/v1/payments/charge",
            json={
                "trip_id": trip.id,
                "amount": trip.fare_amount,
                "payment_method": "CARD"
            },
            headers={
                "Idempotency-Key": idempotency_key,
                "X-Correlation-ID": correlation_id
            },
            timeout=5
        )

        if payment_response.status_code != 200:
            trip.status = "PAYMENT_FAILED"
            db.commit()
            db.refresh(trip)
            db.close()

            payments_failed_total.inc()

            logger.error(
                f"Payment failed for trip {trip_id}",
                extra={"correlation_id": correlation_id}
            )

            return {
                "message": "Trip completed but payment failed",
                "correlation_id": correlation_id,
                "trip": trip
            }

        trip.status = "COMPLETED"

        db.commit()
        db.refresh(trip)

        payment_result = payment_response.json()
        db.close()

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

    except requests.exceptions.RequestException:
        trip.status = "PAYMENT_FAILED"

        db.commit()
        db.refresh(trip)
        db.close()

        payments_failed_total.inc()

        logger.error(
            "Payment Service unavailable",
            extra={"correlation_id": correlation_id}
        )

        return {
            "message": "Trip completed but Payment Service unavailable",
            "correlation_id": correlation_id,
            "trip": trip
        }


@app.post("/v1/trips/{trip_id}/cancel")
def cancel_trip(trip_id: int, request: Request):
    correlation_id = get_correlation_id(request)

    db = SessionLocal()
    trip = db.query(Trip).filter(Trip.id == trip_id).first()

    if not trip:
        db.close()
        raise HTTPException(status_code=404, detail="Trip not found")

    if trip.status == "COMPLETED":
        db.close()
        raise HTTPException(
            status_code=400,
            detail="Completed trip cannot be cancelled"
        )

    cancellation_fee = 0

    if trip.status == "ACCEPTED":
        cancellation_fee = 30

    trip.status = "CANCELLED"

    db.commit()
    db.refresh(trip)
    db.close()

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
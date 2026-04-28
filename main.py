from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, Float, String
from sqlalchemy.orm import declarative_base, sessionmaker
import requests

app = FastAPI(title="Trip Service")

DATABASE_URL = "sqlite:///./trip_service.db"

DRIVER_SERVICE_URL = "http://127.0.0.1:8002"
PAYMENT_SERVICE_URL = "http://127.0.0.1:8004"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


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


@app.get("/health")
def health():
    return {"service": "trip-service", "status": "UP"}


@app.get("/v1/trips")
def get_trips():
    db = SessionLocal()
    trips = db.query(Trip).all()
    db.close()
    return trips


@app.get("/v1/trips/{trip_id}")
def get_trip(trip_id: int):
    db = SessionLocal()
    trip = db.query(Trip).filter(Trip.id == trip_id).first()
    db.close()

    if not trip:
        raise HTTPException(status_code=404, detail="Trip not found")

    return trip


@app.post("/v1/trips")
def create_trip(request: TripRequest):
    if request.surge_multiplier not in [1.0, 1.2, 1.5]:
        raise HTTPException(
            status_code=400,
            detail="Surge multiplier must be 1.0, 1.2, or 1.5"
        )

    try:
        driver_response = requests.get(
            f"{DRIVER_SERVICE_URL}/v1/drivers/available",
            params={"city": request.city},
            timeout=5
        )

        if driver_response.status_code != 200:
            raise HTTPException(
                status_code=400,
                detail="No active driver available"
            )

        driver = driver_response.json()

    except requests.exceptions.RequestException:
        raise HTTPException(
            status_code=500,
            detail="Driver Service is not available"
        )

    db = SessionLocal()

    trip = Trip(
        rider_id=request.rider_id,
        driver_id=driver["id"],
        pickup_location=request.pickup_location,
        drop_location=request.drop_location,
        city=request.city,
        distance_km=request.distance_km,
        surge_multiplier=request.surge_multiplier,
        base_fare=50,
        status="REQUESTED"
    )

    db.add(trip)
    db.commit()
    db.refresh(trip)
    db.close()

    return {
        "message": "Trip requested successfully",
        "assigned_driver": driver,
        "trip": trip
    }


@app.post("/v1/trips/{trip_id}/accept")
def accept_trip(trip_id: int):
    db = SessionLocal()
    trip = db.query(Trip).filter(Trip.id == trip_id).first()

    if not trip:
        db.close()
        raise HTTPException(status_code=404, detail="Trip not found")

    if trip.status != "REQUESTED":
        db.close()
        raise HTTPException(
            status_code=400,
            detail="Only REQUESTED trip can be accepted"
        )

    trip.status = "ACCEPTED"

    db.commit()
    db.refresh(trip)
    db.close()

    return {
        "message": "Trip accepted successfully",
        "trip": trip
    }


@app.post("/v1/trips/{trip_id}/complete")
def complete_trip(trip_id: int):
    db = SessionLocal()
    trip = db.query(Trip).filter(Trip.id == trip_id).first()

    if not trip:
        db.close()
        raise HTTPException(status_code=404, detail="Trip not found")

    if trip.status != "ACCEPTED":
        db.close()
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
                "Idempotency-Key": idempotency_key
            },
            timeout=5
        )

        if payment_response.status_code != 200:
            trip.status = "PAYMENT_FAILED"
            db.commit()
            db.refresh(trip)
            db.close()

            return {
                "message": "Trip completed but payment failed",
                "trip": trip
            }

        trip.status = "COMPLETED"

        db.commit()
        db.refresh(trip)

        payment_result = payment_response.json()

        db.close()

        return {
            "message": "Trip completed and payment successful",
            "trip": trip,
            "payment_response": payment_result
        }

    except requests.exceptions.RequestException:
        trip.status = "PAYMENT_FAILED"

        db.commit()
        db.refresh(trip)
        db.close()

        return {
            "message": "Trip completed but Payment Service unavailable",
            "trip": trip
        }


@app.post("/v1/trips/{trip_id}/cancel")
def cancel_trip(trip_id: int):
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

    return {
        "message": "Trip cancelled successfully",
        "cancellation_fee": cancellation_fee,
        "trip": trip
    }
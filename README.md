# Trip Service

The Trip Service is the core orchestrator of the ride-hailing platform. It connects riders with drivers, calculates fares, and manages the trip lifecycle from request to completion.

## Features
- Request new trips and handle automatic driver assignment.
- Calculate dynamic distance and surge-adjusted base fares.
- Complete trips and automatically orchestrate billing with the Payment Service.
- Robust inter-service communication using HTTP retry mechanisms with exponential backoff.

## Tech Stack
- **Framework:** FastAPI
- **Database:** SQLite
- **ORM:** SQLAlchemy (Asynchronous)
- **HTTP Client:** `httpx` and `tenacity` for resilient inter-service calls.

## Running Locally

1. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Make sure the Driver Service and Payment Service are accessible. By default, it expects the Driver Service at port `8002` and the Payment Service at port `8004`.
3. Start the service:
   ```bash
   uvicorn main:app --host 0.0.0.0 --port 8003
   ```

## Key Endpoints
- `POST /v1/trips`: Request a trip. Contacts the Driver service to find an active driver.
- `POST /v1/trips/{trip_id}/accept`: Mark an assigned trip as accepted.
- `POST /v1/trips/{trip_id}/complete`: Complete the trip. Triggers the Payment Service to process the fare.
- `POST /v1/trips/{trip_id}/cancel`: Cancel an active trip.

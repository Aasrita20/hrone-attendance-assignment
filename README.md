# Employee Attendance & Analytics API

FastAPI and MongoDB backend for managing employee attendance, punch-in/out business logic, manual regularizations, and MongoDB aggregation analytics pipelines.

## Setup & Running

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Environment Configuration**:
   The application reads `MONGO_URI` and `MONGO_DB` from environment variables (or a local `.env` file if present):
   ```env
   MONGO_URI=mongodb://localhost:27017
   MONGO_DB=attendance_db
   ```

3. **Start the API**:
   ```bash
   uvicorn app.main:app --port 8000
   ```
   All database indexes (`emp_code` unique, `(emp_code, date)` unique, department & date query indexes) are automatically created on startup.

4. **Seed Sample Data (Optional)**:
   ```bash
   python sample_seed.py
   ```

## Project Structure

* `app/main.py`: Complete FastAPI application with all endpoints, models, business rules (R1–R10), and aggregation pipelines.
* `requirements.txt`: Python dependencies.
* `REVIEW.md`: Detailed code review of starter code defects, failing symptoms, and applied fixes.
* `DECISIONS.md`: Design rationale for indexes, concurrency, leaderboard ties, headcount, and 100x data scaling.

"""
Employee Attendance & Analytics API - Production Implementation

FastAPI + MongoDB backend implementing business rules R1 - R10,
concurrency safety, and MongoDB aggregation pipelines.
"""
import os
import re
from datetime import datetime, date as date_type, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Path
from pydantic import BaseModel, Field, field_validator
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "attendance_db")

client = MongoClient(MONGO_URI, tz_aware=True)
db = client[MONGO_DB]

IST = timezone(timedelta(hours=5, minutes=30))


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ensure all required indexes are created idempotently on startup
    db.employees.create_index("emp_code", unique=True)
    db.employees.create_index("department")
    db.attendance_logs.create_index([("emp_code", 1), ("date", 1)], unique=True)
    db.attendance_logs.create_index([("date", -1), ("emp_code", 1)])
    db.attendance_logs.create_index([("status", 1), ("date", 1)])
    yield


app = FastAPI(
    title="Employee Attendance & Analytics API",
    version="2.0.0",
    lifespan=lifespan,
)


# --------------------------------------------------------------------------- #
# Helpers for Business Rules (R1 - R10) & Time/Date Conversion
# --------------------------------------------------------------------------- #

def round_half_up(val: Optional[float], decimals: int) -> Optional[float]:
    if val is None:
        return None
    d = Decimal(str(val))
    fmt = '0.' + '0' * decimals if decimals > 0 else '0'
    return float(d.quantize(Decimal(fmt), rounding=ROUND_HALF_UP))


def validate_epoch_ms(v: Optional[int]) -> Optional[int]:
    if v is None:
        return v
    if isinstance(v, bool) or not isinstance(v, int):
        raise ValueError("EpochMillis must be an integer")
    if not (100000000000 <= v <= 4102444800000):
        raise ValueError("EpochMillis out of range")
    return v


def truncate_to_seconds(ms: int) -> int:
    return (ms // 1000) * 1000


def ms_to_datetime_utc(ms: int) -> datetime:
    truncated_ms = truncate_to_seconds(ms)
    return datetime.fromtimestamp(truncated_ms / 1000.0, tz=timezone.utc)


def datetime_to_ms(dt: Optional[datetime]) -> Optional[int]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def get_attendance_date(punch_in_ist: datetime, shift_start: str, shift_end: str) -> str:
    """
    R1: Shift times and attendance dates are IST. A record's date is the IST calendar date
    of its punch_in instant. Exception - overnight shifts (shift_end <= shift_start):
    a punch-in whose IST time is earlier than shift_end belongs to the shift that started the previous day.
    """
    start_h, start_m = map(int, shift_start.split(":"))
    end_h, end_m = map(int, shift_end.split(":"))

    is_overnight = (end_h < start_h) or (end_h == start_h and end_m <= start_m)

    if is_overnight:
        punch_time = punch_in_ist.time()
        end_time = datetime.min.replace(hour=end_h, minute=end_m).time()
        if punch_time < end_time:
            return (punch_in_ist.date() - timedelta(days=1)).isoformat()

    return punch_in_ist.date().isoformat()


def compute_late_minutes(punch_in_ist: datetime, shift_start: str, date_str: str) -> int:
    """
    R2: Grace period 10 minutes. Late only if punch-in is strictly more than 10:00 after shift_start;
    then late_minutes = whole minutes (floor) since shift_start.
    """
    start_h, start_m = map(int, shift_start.split(":"))
    dt_base = datetime.fromisoformat(date_str)
    shift_start_dt = dt_base.replace(hour=start_h, minute=start_m, second=0, microsecond=0, tzinfo=IST)

    elapsed_seconds = (punch_in_ist - shift_start_dt).total_seconds()
    if elapsed_seconds > 600:
        return int(elapsed_seconds // 60)
    return 0


def compute_overtime(punch_out_ist: datetime, shift_start: str, shift_end: str, date_str: str) -> int:
    """
    R3: Whole minutes (floor) from shift_end to punch_out, counted only if that is at least 30; otherwise 0.
    Overnight shifts fall on the next calendar day after date_str.
    """
    start_h, start_m = map(int, shift_start.split(":"))
    end_h, end_m = map(int, shift_end.split(":"))

    dt_base = datetime.fromisoformat(date_str)
    is_overnight = (end_h < start_h) or (end_h == start_h and end_m <= start_m)

    if is_overnight:
        shift_end_dt = (dt_base + timedelta(days=1)).replace(hour=end_h, minute=end_m, second=0, microsecond=0, tzinfo=IST)
    else:
        shift_end_dt = dt_base.replace(hour=end_h, minute=end_m, second=0, microsecond=0, tzinfo=IST)

    elapsed_seconds = (punch_out_ist - shift_end_dt).total_seconds()
    if elapsed_seconds >= 1800:
        return int(elapsed_seconds // 60)
    return 0


def compute_work_hours(punch_in_utc: datetime, punch_out_utc: datetime) -> float:
    """
    R4: (punch_out - punch_in) in seconds / 3600, rounded to 2 decimals half-up.
    """
    seconds = (punch_out_utc - punch_in_utc).total_seconds()
    hours = seconds / 3600.0
    return round_half_up(hours, 2)


def is_half_day(work_hours: Optional[float]) -> bool:
    """
    R5: half_day = true when rounded work_hours < 4.50.
    """
    if work_hours is None:
        return False
    return work_hours < 4.50


def format_doc_for_api(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Converts MongoDB document to API response dict (converting datetimes to epoch ms)."""
    res = dict(doc)
    res.pop("_id", None)
    res.pop("id", None)

    for field in ("created_at", "punch_in", "punch_out"):
        if field in res:
            if isinstance(res[field], datetime):
                res[field] = datetime_to_ms(res[field])

    if "history" in res and isinstance(res["history"], list):
        formatted_history = []
        for h in res["history"]:
            entry = dict(h)
            if "at" in entry and isinstance(entry["at"], datetime):
                entry["at"] = datetime_to_ms(entry["at"])

            if "changes" in entry and isinstance(entry["changes"], dict):
                formatted_changes = {}
                for k, v in entry["changes"].items():
                    change_item = dict(v)
                    if k in ("punch_in", "punch_out"):
                        if isinstance(change_item.get("from"), datetime):
                            change_item["from"] = datetime_to_ms(change_item["from"])
                        if isinstance(change_item.get("to"), datetime):
                            change_item["to"] = datetime_to_ms(change_item["to"])
                    formatted_changes[k] = change_item
                entry["changes"] = formatted_changes
            formatted_history.append(entry)
        res["history"] = formatted_history
    else:
        res["history"] = res.get("history", [])

    if "half_day" not in res:
        res["half_day"] = False

    return res


# --------------------------------------------------------------------------- #
# Request & Response Models
# --------------------------------------------------------------------------- #

class EmployeeIn(BaseModel):
    emp_code: str = Field(..., pattern=r'^EMP\d{4,6}$')
    name: str = Field(..., min_length=1, max_length=100)
    email: str = Field(..., max_length=120, pattern=r'^[^@\s]+@[^@\s]+\.[^@\s]+$')
    department: str = Field(..., min_length=1, max_length=50)
    shift_start: str = Field("09:30", pattern=r'^([01]\d|2[0-3]):[0-5]\d$')
    shift_end: str = Field("18:30", pattern=r'^([01]\d|2[0-3]):[0-5]\d$')
    joined_on: str = Field(..., pattern=r'^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$')

    @field_validator("shift_end")
    def validate_shift_times(cls, v, info):
        shift_start = info.data.get("shift_start", "09:30")
        if v == shift_start:
            raise ValueError("shift_start must differ from shift_end")
        return v


class PunchInIn(BaseModel):
    emp_code: str
    punched_at: Optional[int] = None
    status: str = "PRESENT"

    @field_validator("punched_at")
    def validate_punched_at(cls, v):
        return validate_epoch_ms(v)

    @field_validator("status")
    def validate_status(cls, v):
        if v not in ("PRESENT", "WFH", "ON_DUTY"):
            raise ValueError("Punch-in status must be PRESENT, WFH or ON_DUTY")
        return v


class PunchOutIn(BaseModel):
    emp_code: str
    punched_at: Optional[int] = None

    @field_validator("punched_at")
    def validate_punched_at(cls, v):
        return validate_epoch_ms(v)


class RegularizeIn(BaseModel):
    status: Optional[str] = None
    punch_in: Optional[int] = None
    punch_out: Optional[int] = None
    reason: str = Field(..., min_length=5, max_length=200)
    regularized_by: str = Field(..., min_length=1, max_length=50)

    @field_validator("punch_in", "punch_out")
    def validate_punches(cls, v):
        return validate_epoch_ms(v)

    @field_validator("status")
    def validate_status(cls, v):
        if v is not None and v not in ("PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"):
            raise ValueError("Invalid status value")
        return v


# --------------------------------------------------------------------------- #
# Endpoints Implementation
# --------------------------------------------------------------------------- #

@app.get("/health")
def health():
    try:
        db.command("ping")
        return {"status": "ok"}
    except Exception:
        raise HTTPException(503, "Database unavailable")


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    now_utc = datetime.now(timezone.utc)
    doc = body.model_dump()
    doc["created_at"] = now_utc

    try:
        db.employees.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")

    return format_doc_for_api(doc)


@app.get("/employees")
def list_employees(
    department: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    q = {}
    if department:
        q["department"] = department

    total = db.employees.count_documents(q)
    skip = (page - 1) * page_size
    cursor = db.employees.find(q, {"_id": 0}).sort("emp_code", 1).skip(skip).limit(page_size)
    items = [format_doc_for_api(d) for d in cursor]

    return {"items": items, "total": total, "page": page, "page_size": page_size}


@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = db.employees.find_one({"emp_code": body.emp_code})
    if not emp:
        raise HTTPException(404, "Employee not found")

    punch_ms = body.punched_at if body.punched_at is not None else int(datetime.now(timezone.utc).timestamp() * 1000)
    punch_in_utc = ms_to_datetime_utc(punch_ms)
    punch_in_ist = punch_in_utc.astimezone(IST)

    attendance_date = get_attendance_date(punch_in_ist, emp["shift_start"], emp["shift_end"])
    late_minutes = compute_late_minutes(punch_in_ist, emp["shift_start"], attendance_date)

    doc = {
        "emp_code": body.emp_code,
        "date": attendance_date,
        "status": body.status,
        "punch_in": punch_in_utc,
        "punch_out": None,
        "work_hours": None,
        "late_minutes": late_minutes,
        "overtime_minutes": 0,
        "half_day": False,
        "history": [],
    }

    try:
        db.attendance_logs.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")

    return format_doc_for_api(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = db.employees.find_one({"emp_code": body.emp_code})
    if not emp:
        raise HTTPException(404, "Employee not found")

    punch_ms = body.punched_at if body.punched_at is not None else int(datetime.now(timezone.utc).timestamp() * 1000)
    punch_out_utc = ms_to_datetime_utc(punch_ms)
    punch_out_ist = punch_out_utc.astimezone(IST)

    # Find the most recent punch-in at or before punched_at
    rec = db.attendance_logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$lte": punch_out_utc}},
        sort=[("punch_in", -1)],
    )

    if not rec or rec.get("punch_in") is None:
        raise HTTPException(404, "No punch-in found")

    if rec.get("punch_out") is not None:
        raise HTTPException(409, "Record is already punched out")

    punch_in_utc = rec["punch_in"]
    if punch_out_utc <= punch_in_utc:
        raise HTTPException(422, "punched_at must be after punch_in")

    diff_seconds = (punch_out_utc - punch_in_utc).total_seconds()
    if diff_seconds > 86400:
        raise HTTPException(422, "punch_out cannot be more than 24 hours after punch_in")

    work_hours = compute_work_hours(punch_in_utc, punch_out_utc)
    overtime_minutes = compute_overtime(punch_out_ist, emp["shift_start"], emp["shift_end"], rec["date"])
    half_day = is_half_day(work_hours)

    # Atomic update to prevent race conditions
    updated_doc = db.attendance_logs.find_one_and_update(
        {"_id": rec["_id"], "punch_out": None},
        {
            "$set": {
                "punch_out": punch_out_utc,
                "work_hours": work_hours,
                "overtime_minutes": overtime_minutes,
                "half_day": half_day,
            }
        },
        return_document=ReturnDocument.AFTER,
    )

    if not updated_doc:
        raise HTTPException(409, "Record is already punched out")

    return format_doc_for_api(updated_doc)


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = Query(None, pattern=r'^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$'),
    date_to: Optional[str] = Query(None, pattern=r'^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$'),
    status: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    if date_from and date_to and date_from > date_to:
        raise HTTPException(422, "date_from cannot be greater than date_to")

    q = {}
    if emp_code:
        q["emp_code"] = emp_code
    if status:
        q["status"] = status
    if date_from or date_to:
        q["date"] = {}
        if date_from:
            q["date"]["$gte"] = date_from
        if date_to:
            q["date"]["$lte"] = date_to

    total = db.attendance_logs.count_documents(q)
    skip = (page - 1) * page_size
    cursor = db.attendance_logs.find(q).sort([("date", -1), ("emp_code", 1)]).skip(skip).limit(page_size)
    items = [format_doc_for_api(d) for d in cursor]

    return {"items": items, "total": total, "page": page, "page_size": page_size}


@app.patch("/attendance/{emp_code}/{date}")
def regularize_attendance(
    emp_code: str = Path(...),
    date: str = Path(..., pattern=r'^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$'),
    body: RegularizeIn = ...,
):
    emp = db.employees.find_one({"emp_code": emp_code})
    if not emp:
        raise HTTPException(404, "Employee not found")

    rec = db.attendance_logs.find_one({"emp_code": emp_code, "date": date})
    if not rec:
        raise HTTPException(404, "Attendance record not found")

    target_status = body.status if body.status is not None else rec["status"]

    if target_status in ("ABSENT", "LEAVE"):
        if body.punch_in is not None or body.punch_out is not None:
            raise HTTPException(422, "Cannot supply punch times for ABSENT or LEAVE status")
        target_punch_in_utc = None
        target_punch_out_utc = None
    else:
        target_punch_in_utc = ms_to_datetime_utc(body.punch_in) if body.punch_in is not None else rec.get("punch_in")
        target_punch_out_utc = ms_to_datetime_utc(body.punch_out) if body.punch_out is not None else rec.get("punch_out")

        if target_punch_in_utc is None:
            raise HTTPException(422, "Presence status requires punch_in")

        target_punch_in_ist = target_punch_in_utc.astimezone(IST)
        new_date = get_attendance_date(target_punch_in_ist, emp["shift_start"], emp["shift_end"])
        if new_date != date:
            raise HTTPException(422, "punch_in must stay on the record's attendance date")

        if target_punch_out_utc is not None:
            if target_punch_out_utc <= target_punch_in_utc:
                raise HTTPException(422, "punch_out must be after punch_in")
            if (target_punch_out_utc - target_punch_in_utc).total_seconds() > 86400:
                raise HTTPException(422, "punch_out cannot be more than 24 hours after punch_in")

    if target_status in ("ABSENT", "LEAVE"):
        new_late_minutes = 0
        new_work_hours = None
        new_overtime_minutes = 0
        new_half_day = False
    else:
        target_punch_in_ist = target_punch_in_utc.astimezone(IST)
        new_late_minutes = compute_late_minutes(target_punch_in_ist, emp["shift_start"], date)
        if target_punch_out_utc is not None:
            target_punch_out_ist = target_punch_out_utc.astimezone(IST)
            new_work_hours = compute_work_hours(target_punch_in_utc, target_punch_out_utc)
            new_overtime_minutes = compute_overtime(target_punch_out_ist, emp["shift_start"], emp["shift_end"], date)
            new_half_day = is_half_day(new_work_hours)
        else:
            new_work_hours = None
            new_overtime_minutes = 0
            new_half_day = False

    # Track changed fields
    changes = {}
    if target_status != rec.get("status"):
        changes["status"] = {"from": rec.get("status"), "to": target_status}
    if target_punch_in_utc != rec.get("punch_in"):
        changes["punch_in"] = {"from": rec.get("punch_in"), "to": target_punch_in_utc}
    if target_punch_out_utc != rec.get("punch_out"):
        changes["punch_out"] = {"from": rec.get("punch_out"), "to": target_punch_out_utc}
    if new_work_hours != rec.get("work_hours"):
        changes["work_hours"] = {"from": rec.get("work_hours"), "to": new_work_hours}
    if new_late_minutes != rec.get("late_minutes", 0):
        changes["late_minutes"] = {"from": rec.get("late_minutes", 0), "to": new_late_minutes}
    if new_overtime_minutes != rec.get("overtime_minutes", 0):
        changes["overtime_minutes"] = {"from": rec.get("overtime_minutes", 0), "to": new_overtime_minutes}
    if new_half_day != rec.get("half_day", False):
        changes["half_day"] = {"from": rec.get("half_day", False), "to": new_half_day}

    if not changes:
        raise HTTPException(422, "Request changes no fields on the record")

    now_utc = datetime.now(timezone.utc)
    history_entry = {
        "at": now_utc,
        "by": body.regularized_by,
        "reason": body.reason,
        "changes": changes,
    }

    curr_history = rec.get("history", [])
    updated_doc = db.attendance_logs.find_one_and_update(
        {"_id": rec["_id"], "history": curr_history},
        {
            "$set": {
                "status": target_status,
                "punch_in": target_punch_in_utc,
                "punch_out": target_punch_out_utc,
                "work_hours": new_work_hours,
                "late_minutes": new_late_minutes,
                "overtime_minutes": new_overtime_minutes,
                "half_day": new_half_day,
            },
            "$push": {"history": history_entry},
        },
        return_document=ReturnDocument.AFTER,
    )

    if not updated_doc:
        raise HTTPException(409, "Concurrent modification detected")

    return format_doc_for_api(updated_doc)


@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(
    emp_code: str = Path(...),
    month: str = Query(..., pattern=r'^\d{4}-(0[1-9]|1[0-2])$'),
):
    emp = db.employees.find_one({"emp_code": emp_code})
    if not emp:
        raise HTTPException(404, "Employee not found")

    year, m_num = map(int, month.split("-"))
    first_day = date_type(year, m_num, 1)
    if m_num == 12:
        next_month_first = date_type(year + 1, 1, 1)
    else:
        next_month_first = date_type(year, m_num + 1, 1)
    last_day = next_month_first - timedelta(days=1)

    first_day_str = first_day.isoformat()
    last_day_str = last_day.isoformat()

    joined_on_str = emp.get("joined_on", "1970-01-01")
    start_date = max(first_day, date_type.fromisoformat(joined_on_str)) if joined_on_str <= last_day_str else None

    working_days = 0
    if start_date and start_date <= last_day:
        curr = start_date
        while curr <= last_day:
            if curr.weekday() < 5:  # Mon-Fri
                working_days += 1
            curr += timedelta(days=1)

    # Aggregation on attendance logs
    pipeline = [
        {"$match": {"emp_code": emp_code, "date": {"$gte": first_day_str, "$lte": last_day_str}}},
        {
            "$addFields": {
                "day_of_week": {"$dayOfWeek": {"$dateFromString": {"dateString": "$date"}}}
            }
        },
        {
            "$group": {
                "_id": None,
                "present_days": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$in": ["$status", ["PRESENT", "WFH", "ON_DUTY"]]},
                                    {"$in": ["$day_of_week", [2, 3, 4, 5, 6]]},
                                ]
                            },
                            {"$cond": [{"$eq": ["$half_day", True]}, 0.5, 1.0]},
                            0.0,
                        ]
                    }
                },
                "leave_days": {
                    "$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}
                },
                "late_count": {
                    "$sum": {"$cond": [{"$gt": ["$late_minutes", 0]}, 1, 0]}
                },
                "total_late_minutes": {
                    "$sum": {"$ifNull": ["$late_minutes", 0]}
                },
                "total_overtime_minutes": {
                    "$sum": {"$ifNull": ["$overtime_minutes", 0]}
                },
            }
        },
    ]

    res = list(db.attendance_logs.aggregate(pipeline))
    stats = res[0] if res else {}

    present_days = round_half_up(stats.get("present_days", 0.0), 2)
    leave_days = stats.get("leave_days", 0)
    late_count = stats.get("late_count", 0)
    total_late_minutes = stats.get("total_late_minutes", 0)
    total_overtime_minutes = stats.get("total_overtime_minutes", 0)

    if working_days > 0:
        attendance_pct = round_half_up((present_days / working_days) * 100.0, 4)
    else:
        attendance_pct = None

    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": present_days,
        "leave_days": leave_days,
        "late_count": late_count,
        "total_late_minutes": total_late_minutes,
        "total_overtime_minutes": total_overtime_minutes,
        "attendance_pct": attendance_pct,
    }


@app.get("/analytics/departments/summary")
def department_summary(
    month: str = Query(..., pattern=r'^\d{4}-(0[1-9]|1[0-2])$'),
    department: Optional[str] = None,
):
    year, m_num = map(int, month.split("-"))
    first_day = date_type(year, m_num, 1)
    next_month_first = date_type(year + 1, 1, 1) if m_num == 12 else date_type(year, m_num + 1, 1)
    last_day = next_month_first - timedelta(days=1)

    first_day_str = first_day.isoformat()
    last_day_str = last_day.isoformat()

    emp_match = {"joined_on": {"$lte": last_day_str}}
    if department:
        emp_match["department"] = department

    pipeline = [
        {"$match": emp_match},
        {
            "$lookup": {
                "from": "attendance_logs",
                "let": {"emp": "$emp_code"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$emp_code", "$$emp"]},
                                    {"$gte": ["$date", first_day_str]},
                                    {"$lte": ["$date", last_day_str]},
                                ]
                            }
                        }
                    },
                    {
                        "$addFields": {
                            "day_of_week": {"$dayOfWeek": {"$dateFromString": {"dateString": "$date"}}}
                        }
                    },
                ],
                "as": "logs",
            }
        },
        {"$unwind": {"path": "$logs", "preserveNullAndEmptyArrays": True}},
        {
            "$group": {
                "_id": "$department",
                "headcount_set": {"$addToSet": "$emp_code"},
                "present_days": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$in": ["$logs.status", ["PRESENT", "WFH", "ON_DUTY"]]},
                                    {"$in": ["$logs.day_of_week", [2, 3, 4, 5, 6]]},
                                ]
                            },
                            {"$cond": [{"$eq": ["$logs.half_day", True]}, 0.5, 1.0]},
                            0.0,
                        ]
                    }
                },
                "total_work_hours": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$in": ["$logs.status", ["PRESENT", "WFH", "ON_DUTY"]]},
                                    {"$ne": ["$logs.work_hours", None]},
                                ]
                            },
                            "$logs.work_hours",
                            0.0,
                        ]
                    }
                },
                "work_hours_count": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$in": ["$logs.status", ["PRESENT", "WFH", "ON_DUTY"]]},
                                    {"$ne": ["$logs.work_hours", None]},
                                ]
                            },
                            1,
                            0,
                        ]
                    }
                },
                "late_count": {
                    "$sum": {"$cond": [{"$gt": ["$logs.late_minutes", 0]}, 1, 0]}
                },
                "total_late_minutes": {
                    "$sum": {"$ifNull": ["$logs.late_minutes", 0]}
                },
                "leave_count": {
                    "$sum": {"$cond": [{"$eq": ["$logs.status", "LEAVE"]}, 1, 0]}
                },
                "on_duty_count": {
                    "$sum": {"$cond": [{"$eq": ["$logs.status", "ON_DUTY"]}, 1, 0]}
                },
            }
        },
        {"$sort": {"_id": 1}},
    ]

    res = list(db.employees.aggregate(pipeline))

    items = []
    for doc in res:
        dept_name = doc["_id"]
        headcount = len(doc.get("headcount_set", []))
        if headcount == 0:
            continue

        p_days = round_half_up(doc.get("present_days", 0.0), 2)
        wh_count = doc.get("work_hours_count", 0)
        if wh_count > 0:
            avg_wh = round_half_up(doc.get("total_work_hours", 0.0) / wh_count, 2)
        else:
            avg_wh = None

        items.append({
            "department": dept_name,
            "headcount": headcount,
            "present_days": p_days,
            "avg_work_hours": avg_wh,
            "late_count": doc.get("late_count", 0),
            "total_late_minutes": doc.get("total_late_minutes", 0),
            "leave_count": doc.get("leave_count", 0),
            "on_duty_count": doc.get("on_duty_count", 0),
        })

    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(
    month: str = Query(..., pattern=r'^\d{4}-(0[1-9]|1[0-2])$'),
    limit: int = Query(10, ge=1, le=50),
    department: Optional[str] = None,
):
    year, m_num = map(int, month.split("-"))
    first_day = date_type(year, m_num, 1)
    next_month_first = date_type(year + 1, 1, 1) if m_num == 12 else date_type(year, m_num + 1, 1)
    last_day = next_month_first - timedelta(days=1)

    first_day_str = first_day.isoformat()
    last_day_str = last_day.isoformat()

    pipeline: List[Dict[str, Any]] = [
        {
            "$match": {
                "date": {"$gte": first_day_str, "$lte": last_day_str},
                "late_minutes": {"$gt": 0},
            }
        },
        {
            "$group": {
                "_id": "$emp_code",
                "total_late_minutes": {"$sum": "$late_minutes"},
                "late_count": {"$sum": 1},
            }
        },
        {"$match": {"total_late_minutes": {"$gt": 0}}},
        {
            "$lookup": {
                "from": "employees",
                "localField": "_id",
                "foreignField": "emp_code",
                "as": "emp",
            }
        },
        {"$unwind": "$emp"},
    ]

    if department:
        pipeline.append({"$match": {"emp.department": department}})

    pipeline.extend([
        {
            "$project": {
                "emp_code": "$_id",
                "name": "$emp.name",
                "department": "$emp.department",
                "total_late_minutes": 1,
                "late_count": 1,
            }
        },
        {"$sort": {"total_late_minutes": -1, "emp_code": 1}},
        {
            "$setWindowFields": {
                "sortBy": {"total_late_minutes": -1},
                "output": {"rank": {"$rank": {}}},
            }
        },
        {"$match": {"rank": {"$lte": limit}}},
        {"$sort": {"rank": 1, "total_late_minutes": -1, "emp_code": 1}},
        {
            "$project": {
                "_id": 0,
                "rank": 1,
                "emp_code": 1,
                "name": 1,
                "department": 1,
                "total_late_minutes": 1,
                "late_count": 1,
            }
        },
    ])

    items = list(db.attendance_logs.aggregate(pipeline))
    return {"month": month, "items": items}


@app.get("/analytics/departments/{department}/trend")
def department_trend(
    department: str = Path(...),
    from_date: str = Query(..., alias="from", pattern=r'^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$'),
    to_date: str = Query(..., alias="to", pattern=r'^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$'),
):
    if to_date < from_date:
        raise HTTPException(422, "to date cannot be earlier than from date")

    d_from = date_type.fromisoformat(from_date)
    d_to = date_type.fromisoformat(to_date)
    num_days = (d_to - d_from).days + 1
    if num_days > 92:
        raise HTTPException(422, "Date range cannot exceed 92 days")

    dept_emp = db.employees.find_one({"department": department})
    if not dept_emp:
        raise HTTPException(404, "Department not found")

    dt_from_utc = datetime.combine(d_from, datetime.min.time(), tzinfo=timezone.utc)
    dt_to_next_utc = datetime.combine(d_to + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)

    pipeline = [
        {"$documents": [{"date": dt_from_utc}, {"date": dt_to_next_utc}]},
        {
            "$densify": {
                "field": "date",
                "range": {
                    "step": 1,
                    "unit": "day",
                    "bounds": [dt_from_utc, dt_to_next_utc],
                },
            }
        },
        {
            "$match": {
                "date": {"$lt": dt_to_next_utc}
            }
        },
        {
            "$addFields": {
                "date_str": {"$dateToString": {"format": "%Y-%m-%d", "date": "$date"}}
            }
        },
        {
            "$lookup": {
                "from": "employees",
                "let": {"d_str": "$date_str"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$department", department]},
                                    {"$lte": ["$joined_on", "$$d_str"]},
                                ]
                            }
                        }
                    },
                    {"$count": "count"},
                ],
                "as": "hc_docs",
            }
        },
        {
            "$lookup": {
                "from": "attendance_logs",
                "let": {"d_str": "$date_str"},
                "pipeline": [
                    {
                        "$lookup": {
                            "from": "employees",
                            "localField": "emp_code",
                            "foreignField": "emp_code",
                            "as": "emp",
                        }
                    },
                    {"$unwind": "$emp"},
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$emp.department", department]},
                                    {"$eq": ["$date", "$$d_str"]},
                                ]
                            }
                        }
                    },
                ],
                "as": "logs",
            }
        },
        {
            "$addFields": {
                "is_working_day": {
                    "$in": [{"$dayOfWeek": "$date"}, [2, 3, 4, 5, 6]]
                },
                "headcount": {
                    "$ifNull": [{"$arrayElemAt": ["$hc_docs.count", 0]}, 0]
                },
                "present_count": {
                    "$reduce": {
                        "input": "$logs",
                        "initialValue": 0.0,
                        "in": {
                            "$add": [
                                "$$value",
                                {
                                    "$cond": [
                                        {"$in": ["$$this.status", ["PRESENT", "WFH", "ON_DUTY"]]},
                                        {"$cond": [{"$eq": ["$$this.half_day", True]}, 0.5, 1.0]},
                                        0.0,
                                    ]
                                },
                            ]
                        },
                    }
                },
                "late_count": {
                    "$reduce": {
                        "input": "$logs",
                        "initialValue": 0,
                        "in": {
                            "$add": [
                                "$$value",
                                {"$cond": [{"$gt": ["$$this.late_minutes", 0]}, 1, 0]},
                            ]
                        },
                    }
                },
            }
        },
        {
            "$addFields": {
                "attendance_rate": {
                    "$cond": [
                        {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
                        {"$divide": ["$present_count", "$headcount"]},
                        None,
                    ]
                }
            }
        },
        {
            "$setWindowFields": {
                "sortBy": {"date": 1},
                "output": {
                    "moving_avg_7d": {
                        "$avg": "$attendance_rate",
                        "window": {"documents": [-6, 0]},
                    }
                },
            }
        },
        {"$sort": {"date": 1}},
    ]

    docs = list(db.attendance_logs.aggregate(pipeline))

    items = []
    for d in docs:
        d_str = d["date_str"]
        is_wk = d["is_working_day"]
        hc = d["headcount"]
        p_cnt = round_half_up(d["present_count"], 2)
        l_cnt = d["late_count"]
        raw_rate = d.get("attendance_rate")
        rate = round_half_up(raw_rate, 4) if raw_rate is not None else None
        raw_m_avg = d.get("moving_avg_7d")
        m_avg = round_half_up(raw_m_avg, 4) if raw_m_avg is not None else None

        items.append({
            "date": d_str,
            "is_working_day": is_wk,
            "headcount": hc,
            "present_count": p_cnt,
            "late_count": l_cnt,
            "attendance_rate": rate,
            "moving_avg_7d": m_avg,
        })

    return {"department": department, "items": items}


@app.get("/admin/explain/{endpoint}")
def explain_endpoint(
    endpoint: str = Path(...),
    emp_code: Optional[str] = None,
    month: Optional[str] = None,
    department: Optional[str] = None,
    limit: Optional[int] = 10,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[str] = None,
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    page: Optional[int] = 1,
    page_size: Optional[int] = 20,
):
    if endpoint == "attendance_list":
        q = {}
        if emp_code:
            q["emp_code"] = emp_code
        if status:
            q["status"] = status
        if date_from or date_to:
            q["date"] = {}
            if date_from:
                q["date"]["$gte"] = date_from
            if date_to:
                q["date"]["$lte"] = date_to

        skip = ((page or 1) - 1) * (page_size or 20)
        cmd = {
            "find": "attendance_logs",
            "filter": q,
            "sort": [("date", -1), ("emp_code", 1)],
            "skip": skip,
            "limit": page_size or 20,
        }
        coll_name = "attendance_logs"
        explain_res = db.command("explain", cmd, verbosity="executionStats")

    elif endpoint == "employee_monthly":
        if not emp_code or not month:
            raise HTTPException(422, "emp_code and month are required for employee_monthly explain")

        year, m_num = map(int, month.split("-"))
        first_day_str = f"{month}-01"
        next_month_first = date_type(year + 1, 1, 1) if m_num == 12 else date_type(year, m_num + 1, 1)
        last_day_str = (next_month_first - timedelta(days=1)).isoformat()

        pipeline = [
            {"$match": {"emp_code": emp_code, "date": {"$gte": first_day_str, "$lte": last_day_str}}},
            {"$addFields": {"day_of_week": {"$dayOfWeek": {"$dateFromString": {"dateString": "$date"}}}}},
            {"$group": {"_id": None, "present_days": {"$sum": 1}}},
        ]
        cmd = {"aggregate": "attendance_logs", "pipeline": pipeline, "cursor": {}}
        coll_name = "attendance_logs"
        explain_res = db.command("explain", cmd, verbosity="executionStats")

    elif endpoint == "department_summary":
        if not month:
            raise HTTPException(422, "month is required for department_summary explain")

        year, m_num = map(int, month.split("-"))
        next_month_first = date_type(year + 1, 1, 1) if m_num == 12 else date_type(year, m_num + 1, 1)
        last_day_str = (next_month_first - timedelta(days=1)).isoformat()

        emp_match = {"joined_on": {"$lte": last_day_str}}
        if department:
            emp_match["department"] = department

        pipeline = [
            {"$match": emp_match},
            {
                "$lookup": {
                    "from": "attendance_logs",
                    "localField": "emp_code",
                    "foreignField": "emp_code",
                    "as": "logs",
                }
            },
        ]
        cmd = {"aggregate": "employees", "pipeline": pipeline, "cursor": {}}
        coll_name = "employees"
        explain_res = db.command("explain", cmd, verbosity="executionStats")

    elif endpoint == "late_leaderboard":
        if not month:
            raise HTTPException(422, "month is required for late_leaderboard explain")

        year, m_num = map(int, month.split("-"))
        first_day_str = f"{month}-01"
        next_month_first = date_type(year + 1, 1, 1) if m_num == 12 else date_type(year, m_num + 1, 1)
        last_day_str = (next_month_first - timedelta(days=1)).isoformat()

        pipeline = [
            {"$match": {"date": {"$gte": first_day_str, "$lte": last_day_str}, "late_minutes": {"$gt": 0}}},
            {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"}}},
            {"$sort": {"total_late_minutes": -1}},
        ]
        cmd = {"aggregate": "attendance_logs", "pipeline": pipeline, "cursor": {}}
        coll_name = "attendance_logs"
        explain_res = db.command("explain", cmd, verbosity="executionStats")

    elif endpoint == "department_trend":
        if not department or not from_date or not to_date:
            raise HTTPException(422, "department, from, and to are required for department_trend explain")

        d_from = date_type.fromisoformat(from_date)
        d_to = date_type.fromisoformat(to_date)
        dt_from_utc = datetime.combine(d_from, datetime.min.time(), tzinfo=timezone.utc)
        dt_to_next_utc = datetime.combine(d_to + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)

        pipeline = [
            {"$documents": [{"date": dt_from_utc}, {"date": dt_to_next_utc}]},
            {"$densify": {"field": "date", "range": {"step": 1, "unit": "day", "bounds": [dt_from_utc, dt_to_next_utc]}}},
        ]
        cmd = {"aggregate": "attendance_logs", "pipeline": pipeline, "cursor": {}}
        coll_name = "attendance_logs"
        explain_res = db.command("explain", cmd, verbosity="executionStats")

    else:
        raise HTTPException(422, f"Unknown endpoint '{endpoint}'")

    return {
        "endpoint": endpoint,
        "collection": coll_name,
        "explain": explain_res,
    }

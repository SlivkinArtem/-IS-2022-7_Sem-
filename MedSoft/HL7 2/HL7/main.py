from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, field_validator
import sqlite3, datetime, uuid, asyncio, os
from typing import List
import httpx

# uvicorn main:app --ssl-keyfile certs/key.pem --ssl-certfile certs/cert.pem --port 8000
DB_PATH = "patients.db"
CHIEF_SERVER_URL = os.getenv("CHIEF_SERVER_URL", "https://127.0.0.1:8002")  # адрес сервера главврача

app = FastAPI(title="Reception.API")
app.mount("/static", StaticFiles(directory="static", html=True), name="static")

def db_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS patients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_name TEXT NOT NULL,
                last_name  TEXT NOT NULL,
                dob        TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS hl7_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                raw TEXT NOT NULL
            )
        """)
        conn.commit()
init_db()

# ---------- Models ----------
class PatientIn(BaseModel):
    first_name: str
    last_name: str
    dob: str  # YYYY-MM-DD

    @field_validator("dob")
    @classmethod
    def validate_dob(cls, v: str):
        datetime.date.fromisoformat(v)
        return v

class PatientOut(BaseModel):
    id: int
    first_name: str
    last_name: str
    dob: str

# ---------- HL7 builder ----------
def build_hl7_adt_a04(patient_id: int, first_name: str, last_name: str, dob: str) -> str:
    ts = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
    msg_id = str(uuid.uuid4())
    msh = "MSH|^~\\&|Reception|Clinic|HIS|Hospital|" + ts + "||ADT^A04|" + msg_id + "|P|2.5"
    pid = f"PID|||{patient_id}||{last_name}^{first_name}||{dob.replace('-','')}|"
    pv1 = "PV1||O"
    return "\r".join([msh, pid, pv1]) + "\r"

# ---------- Helpers ----------
def fetch_all_patients():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT id, first_name, last_name, dob FROM patients ORDER BY id DESC LIMIT 10")
        rows = c.fetchall()
    return [{"id": r[0], "first_name": r[1], "last_name": r[2], "dob": r[3]} for r in rows]

# ---------- API ----------
@app.get("/")
async def root():
    return {"ok": True, "see": "/static/reception.html and (chief UI via chief server)"}

@app.get("/api/patients", response_model=List[PatientOut])
async def get_patients():
    return fetch_all_patients()

@app.post("/api/register", response_model=PatientOut)
async def register(p: PatientIn):
    # 1) Save patient locally (Reception side)
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            "INSERT INTO patients(first_name, last_name, dob) VALUES (?,?,?)",
            (p.first_name.strip(), p.last_name.strip(), p.dob),
        )
        patient_id = c.lastrowid
        conn.commit()
        

    # 2) Build + log HL7 (raw) locally
    hl7_raw = build_hl7_adt_a04(patient_id, p.first_name, p.last_name, p.dob)
    print("HL7 RAW MESSAGE (Reception):\n" + hl7_raw)
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            "INSERT INTO hl7_messages(created_at, raw) VALUES(?, ?)",
            (datetime.datetime.utcnow().isoformat(), hl7_raw),
        )
        conn.commit()

    # 3) Notify Chief.Server via REST JSON (he will broadcast via WebSocket)
    payload = {
        "patient_id": patient_id,
        "first_name": p.first_name,
        "last_name": p.last_name,
        "dob": p.dob,
        "hl7_raw": hl7_raw,
    }
    try:
        async with httpx.AsyncClient(timeout=5.0, verify=False) as client:
            r = await client.post(f"{CHIEF_SERVER_URL}/api/register-patient", json=payload)
            r.raise_for_status()
    except Exception as e:
        # логируем, но не валим регистрацию на стороне Reception
        print(f"[WARN] Failed to notify Chief.Server: {e}")

    return {"id": patient_id, **p.model_dump()}

@app.get("/api/hl7/last")
async def last_hl7(n: int = 5):
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT created_at, raw FROM hl7_messages ORDER BY id DESC LIMIT ?", (n,))
        rows = c.fetchall()
    return [{"created_at": r[0], "raw": r[1]} for r in rows]

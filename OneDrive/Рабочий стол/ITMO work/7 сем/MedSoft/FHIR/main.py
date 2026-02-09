from fastapi import FastAPI, Request, WebSocket, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse, JSONResponse, Response, PlainTextResponse
from starlette.websockets import WebSocketState, WebSocketDisconnect
from pydantic import BaseModel, field_validator
from pathlib import Path
import sqlite3, datetime, uuid, json, asyncio, sys, os, logging, re
from typing import List, Union

# uvicorn main:app --host 127.0.0.1 --port 8000 --ssl-keyfile certs/key.pem --ssl-certfile certs/cert.pem
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

BASE_DIR = Path(__file__).parent.resolve()
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="HL7 FHIR Mono-Server")

@app.on_event("startup")
async def _set_loop_exception_handler():
    loop = asyncio.get_running_loop()
    default_handler = loop.get_exception_handler()

    def _ignore_connreset(loop, context):
        exc = context.get("exception")
        msg = context.get("message", "")
        if isinstance(exc, (ConnectionResetError, BrokenPipeError, OSError)) or "Connection reset" in msg:
            return  # suppress client disconnect noise
        if default_handler:
            default_handler(loop, context)
        else:
            loop.default_exception_handler(context)

    loop.set_exception_handler(_ignore_connreset)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/static/reception.html", status_code=302)

DB_PATH = str(BASE_DIR / "app.db")

def db_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS patients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fhir_id TEXT NOT NULL,
                family  TEXT NOT NULL,
                given   TEXT NOT NULL,
                birthDate TEXT NOT NULL,
                received_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS fhir_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                raw TEXT NOT NULL
            )
        """)
        conn.commit()
init_db()

def fetch_last_patients(limit: int = 10):
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT id, family, given, birthDate, fhir_id
            FROM patients
            ORDER BY id DESC LIMIT ?
        """, (limit,))
        rows = c.fetchall()
    return [{"id": r[0], "last_name": r[1], "first_name": r[2], "dob": r[3], "fhir_id": r[4]} for r in rows]

# class PatientIn(BaseModel):
#     first_name: str
#     last_name: str
#     dob: str  # YYYY-MM-DD

#     @field_validator("dob")
#     @classmethod
#     def vdob(cls, v: str):
#         datetime.date.fromisoformat(v)
#         return v

# # ---------- FHIR builder ----------
# def build_fhir_patient(p: PatientIn) -> dict:
#     return {
#         "resourceType": "Patient",
#         "id": str(uuid.uuid4()),
#         "name": [{
#             "use": "official",
#             "family": p.last_name.strip(),
#             "given": [p.first_name.strip()]
#         }],
#         "birthDate": p.dob,
#     }

# ---------- Pretty log of FHIR to terminal ----------
FHIR_LOG = os.getenv("FHIR_LOG", "1").lower() in ("1", "true", "yes")

def log_fhir(title: str, payload: Union[str, dict, bytes, bytearray]):
    if not FHIR_LOG:
        return
    try:
        if isinstance(payload, (bytes, bytearray)):
            payload = payload.decode("utf-8", "ignore")
        if isinstance(payload, str):
            try:
                obj = json.loads(payload)
                pretty = json.dumps(obj, ensure_ascii=False, indent=2)
            except Exception:
                pretty = payload
        else:
            pretty = json.dumps(payload, ensure_ascii=False, indent=2)
        ts = datetime.datetime.utcnow().isoformat()
        print(f"[FHIR] {title} @ {ts}Z\n{pretty}")
    except Exception as e:
        print(f"[FHIR] log error: {e}")

class WSManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, message: dict):
        for ws in list(self.active):  # copy!
            try:
                if ws.client_state == WebSocketState.CONNECTED:
                    await ws.send_json(message)
                else:
                    self.disconnect(ws)
            except (WebSocketDisconnect, ConnectionResetError, BrokenPipeError, OSError, RuntimeError):
                self.disconnect(ws)

ws_manager = WSManager()

# @app.post("/api/register")
# async def api_register(p: PatientIn):
#     fhir_patient = build_fhir_patient(p)
#     log_fhir("INBOUND from Reception.UI (FHIR Patient)", fhir_patient)
#     raw = json.dumps(fhir_patient, ensure_ascii=False)
#     created_at = datetime.datetime.utcnow().isoformat()
#     with db_conn() as conn:
#         c = conn.cursor()
#         c.execute("INSERT INTO fhir_messages(created_at, raw) VALUES(?, ?)", (created_at, raw))
#         conn.commit()
#     family = fhir_patient["name"][0]["family"]
#     given  = fhir_patient["name"][0]["given"][0]
#     birth  = fhir_patient["birthDate"]
#     fid    = fhir_patient["id"]

#     with db_conn() as conn:
#         c = conn.cursor()
#         c.execute("""
#             INSERT INTO patients(fhir_id, family, given, birthDate, received_at)
#             VALUES (?, ?, ?, ?, ?)
#         """, (fid, family, given, birth, created_at))
#         conn.commit()

#     patients = fetch_last_patients(10)
#     asyncio.create_task(ws_manager.broadcast({"type": "patients", "data": patients}))

#     return {"ok": True, "fhir": fhir_patient, "sent_to_chief": True}

@app.delete("/api/patient/{pid}")
async def api_delete_patient(pid: int):
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM patients WHERE id = ?", (pid,))
        deleted = c.rowcount
        conn.commit()

    if deleted == 0:
        raise HTTPException(status_code=404, detail="patient not found")

    patients = fetch_last_patients(10)
    asyncio.create_task(ws_manager.broadcast({"type": "patients", "data": patients}))

    return {"ok": True, "deleted_id": pid}

@app.post("/fhir/Patient")
async def fhir_ingest(req: Request):
    if req.headers.get("content-type","").split(";")[0].strip().lower() not in {"application/fhir+json","application/json"}:
        return JSONResponse({"error":"Unsupported Content-Type"}, status_code=415)

    raw = await req.body()
    created_at = datetime.datetime.utcnow().isoformat()

    log_fhir("INBOUND on /fhir/Patient (raw)", raw)

    with db_conn() as conn:
        c = conn.cursor()
        c.execute("INSERT INTO fhir_messages(created_at, raw) VALUES(?, ?)", (created_at, raw.decode("utf-8","ignore")))
        conn.commit()

    try:
        obj = json.loads(raw.decode("utf-8"))
        if obj.get("resourceType") != "Patient":
            return JSONResponse({"error":"not a Patient resource"}, status_code=400)
        # log_fhir("INBOUND on /fhir/Patient (parsed)", obj)
        name = obj.get("name",[{}])[0]
        family = (name.get("family") or "").strip()
        given_arr = name.get("given") or []
        given = (given_arr[0] if given_arr else "").strip()
        birthDate = (obj.get("birthDate") or "").strip()
        fhir_id = (obj.get("id") or "").strip()
        if not (family and given and birthDate):
            return JSONResponse({"error":"missing required fields"}, status_code=400)
    except Exception as e:
        return JSONResponse({"error": f"bad json: {e}"}, status_code=400)

    with db_conn() as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO patients(fhir_id, family, given, birthDate, received_at)
            VALUES (?, ?, ?, ?, ?)
        """, (fhir_id, family, given, birthDate, created_at))
        conn.commit()

    patients = fetch_last_patients(10)
    asyncio.create_task(ws_manager.broadcast({"type":"patients","data":patients}))

    return JSONResponse({"status":"created","id":fhir_id}, status_code=201)

@app.get("/api/patients")
def api_patients():
    return fetch_last_patients(10)

@app.get("/api/fhir/last")
def api_fhir_last(n: int = 5):
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT created_at, raw FROM fhir_messages ORDER BY id DESC LIMIT ?", (n,))
        rows = c.fetchall()
    return [{"created_at": r[0], "raw": r[1]} for r in rows]

@app.websocket("/ws/chief")
async def chief_ws(ws: WebSocket):
    await ws_manager.connect(ws)
    try:
        await ws.send_json({"type":"patients","data":fetch_last_patients(10)})
        while True:
            try:
                await asyncio.wait_for(ws.receive_text(), timeout=30)
            except asyncio.TimeoutError:
                if ws.client_state == WebSocketState.CONNECTED:
                    await ws.send_json({"type":"ping"})
    except (WebSocketDisconnect, ConnectionResetError, OSError):
        pass
    finally:
        ws_manager.disconnect(ws)
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import sqlite3, datetime, uuid, asyncio
from typing import List
from typing import Optional

# uvicorn chief_server:app --ssl-keyfile certs/key.pem --ssl-certfile certs/cert.pem --port 8002

DB_PATH = "chief_patients.db"

app = FastAPI(title="Hospital Chief Server")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def db_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS patients (
                id INTEGER PRIMARY KEY,
                first_name TEXT NOT NULL,
                last_name  TEXT NOT NULL,
                dob        TEXT NOT NULL,
                registered_at TEXT NOT NULL
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS hl7_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                raw TEXT NOT NULL
            )
            """
        )
        conn.commit()

init_db()

class PatientData(BaseModel):
    patient_id: int
    first_name: str
    last_name: str
    dob: str
    hl7_raw: Optional[str] = None

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
        drop = []
        for ws in self.active:
            try:
                await ws.send_json(message)
            except Exception:
                drop.append(ws)
        for ws in drop:
            self.disconnect(ws)

ws_manager = WSManager()

def fetch_all_patients():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT id, first_name, last_name, dob FROM patients ORDER BY id DESC LIMIT 10")
        rows = c.fetchall()
    return [
        {"id": r[0], "first_name": r[1], "last_name": r[2], "dob": r[3]}
        for r in rows
    ]

@app.post("/api/register-patient")
async def register_patient(patient_data: PatientData):
    """Эндпоинт для приема данных от сервера регистрации"""
    
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            """
            INSERT OR REPLACE INTO patients(id, first_name, last_name, dob, registered_at) 
            VALUES (?,?,?,?,?)
            """,
            (
                patient_data.patient_id, 
                patient_data.first_name.strip(), 
                patient_data.last_name.strip(), 
                patient_data.dob,
                datetime.datetime.utcnow().isoformat()
            ),
        )
        conn.commit()
    
    # если HL7 прислали — сохраняем его, если нет — просто пропускаем
    if patient_data.hl7_raw:
        with db_conn() as conn:
            c = conn.cursor()
            c.execute(
                "INSERT INTO hl7_messages(created_at, raw) VALUES(?, ?)",
                (datetime.datetime.utcnow().isoformat(), patient_data.hl7_raw),
            )
            conn.commit()
    
    # Отправляем обновление всем подключенным UI главврача
    patients = fetch_all_patients()
    asyncio.create_task(ws_manager.broadcast({"type": "patients", "data": patients}))
    
    return {"status": "success", "patient_id": patient_data.patient_id}

@app.get("/api/patients")
async def get_patients():
    return fetch_all_patients()

@app.get("/api/health")
async def health_check():
    return {"status": "healthy", "service": "Chief Server"}

@app.websocket("/ws/chief")
async def chief_ws(ws: WebSocket):
    await ws_manager.connect(ws)
    try:
        # Отправляем текущий список пациентов при подключении
        await ws.send_json({"type": "patients", "data": fetch_all_patients()})
        while True:
            await ws.receive_text()  # Поддерживаем соединение
    except WebSocketDisconnect:
        ws_manager.disconnect(ws)
    except Exception:
        ws_manager.disconnect(ws)

@app.get("/")
async def root():
    return {"service": "Hospital Chief Server", "endpoints": ["/api/register-patient", "/ws/chief"]}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002, reload=True)
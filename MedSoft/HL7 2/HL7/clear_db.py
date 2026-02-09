import sqlite3
with sqlite3.connect("patients.db") as conn:
    c = conn.cursor()
    c.execute("DELETE FROM patients;")
    c.execute("DELETE FROM hl7_messages;")
    conn.commit()
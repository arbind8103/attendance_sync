from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import requests
import pymssql
from datetime import datetime, timedelta
import uvicorn
import secrets
import logging
import time
import json

# ===== Logging =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("attendance_sync.log"), logging.StreamHandler()]
)
logger = logging.getLogger("attendance_sync")

# ===== FASTAPI APP =====
app = FastAPI(title="Employee Transaction Sync API")

# ===== AUTH CONFIG =====
security = HTTPBasic()
API_USER = "admin"
API_PASS = "Noble@321#"
JWT_USERNAME = "admin"
JWT_PASSWORD = "Noble@321#"

# ===== CONFIG =====
API_TRANSACTIONS_URL = "http://196.216.49.238:8801/iclock/api/transactions/"
EMP_API_URL = "http://196.216.49.238:8801/personnel/api/employees/"
AREA_API_URL = "http://196.216.49.238:8801/personnel/api/areas/"

DB_CONFIG = {
    "server": "196.216.49.238",
    "port": 1433,
    "database": "akp_test",
    "username": "sa",
    "password": "Md5189md5189@321#"
}

PAGE_SIZE = 200
FETCH_TIMEOUT = 30
BATCH_SIZE = 500

# ===== AUTH DEPENDENCY =====
def authenticate(credentials: HTTPBasicCredentials = Depends(security)):
    if not (secrets.compare_digest(credentials.username, API_USER) and secrets.compare_digest(credentials.password, API_PASS)):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication credentials", headers={"WWW-Authenticate": "Basic"})
    return credentials.username

# ===== DB CONNECTION (retry) =====
def get_connection(retries=3, delay=1):
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            conn = pymssql.connect(
                server=DB_CONFIG["server"],
                user=DB_CONFIG["username"],
                password=DB_CONFIG["password"],
                database=DB_CONFIG["database"],
                port=DB_CONFIG["port"],
                timeout=30
            )
            return conn
        except Exception as e:
            last_exc = e
            logger.warning(f"DB connection attempt {attempt} failed: {e}")
            time.sleep(delay)
    logger.error("DB connection failed after retries")
    raise last_exc

# ===== TABLE CREATION =====
def init_tables():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='EmployeeTransactions' AND xtype='U')
    CREATE TABLE dbo.EmployeeTransactions (
        id INT IDENTITY PRIMARY KEY,
        emp_code VARCHAR(50),
        emp_name VARCHAR(100),
        location VARCHAR(100),
        punch_date DATE,
        punch_in DATETIME,
        punch_out DATETIME,
        work_duration FLOAT,
        created_at DATETIME DEFAULT GETDATE()
    )
    """)
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='EmployeeWorkAdjusted' AND xtype='U')
    CREATE TABLE dbo.EmployeeWorkAdjusted (
        id INT IDENTITY PRIMARY KEY,
        emp_code VARCHAR(50),
        emp_name VARCHAR(100),
        location VARCHAR(100),
        punch_date DATE,
        adj_in_time DATETIME,
        adj_out_time DATETIME,
        total_work_hrs FLOAT,
        attendance_status VARCHAR(10),
        fetch_status INT DEFAULT 0,
        created_at DATETIME DEFAULT GETDATE()
    )
    """)
    conn.commit()
    conn.close()
    logger.info("Tables initialized successfully.")

# ===== GET JWT TOKEN =====
def get_jwt_token():
    url = "http://196.216.49.238:8801/jwt-api-token-auth/"
    headers = {"Content-Type": "application/json"}
    data = {"username": JWT_USERNAME, "password": JWT_PASSWORD}
    try:
        resp = requests.post(url, headers=headers, data=json.dumps(data), timeout=FETCH_TIMEOUT)
        resp.raise_for_status()
        token = resp.json().get("token")
        logger.info("JWT token obtained successfully")
        return token
    except Exception as e:
        logger.error(f"JWT token fetch failed: {e}")
        return None

# ===== PAGINATED FETCH =====
def fetch_paginated_data(url, auth=None, headers=None, params=None):
    results = []
    page = 1
    while True:
        p = params.copy() if params else {}
        p.update({"page": page, "page_size": PAGE_SIZE})
        try:
            resp = requests.get(url, auth=auth, headers=headers, params=p, timeout=FETCH_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"Failed to fetch page {page} from {url}: {e}")
            break
        if isinstance(data, dict) and "data" in data:
            results.extend(data["data"])
            if not data.get("next"):
                break
        elif isinstance(data, list):
            results.extend(data)
            break
        else:
            break
        page += 1
    return results

# ===== LAST SYNC DATE =====
def get_last_sync_date(cursor):
    cursor.execute("SELECT MAX(punch_date) FROM dbo.EmployeeTransactions")
    row = cursor.fetchone()
    if row and row[0]:
        return row[0]
    return datetime.now() - timedelta(days=30)

# ===== ATTENDANCE STATUS =====
def get_attendance_status(work_hours):
    if work_hours >= 8:
        return "P"
    elif 4 <= work_hours < 8:
        return "HD"
    return "A"

# ===== FETCH DATA =====
def fetch_data():
    conn = get_connection()
    cursor = conn.cursor()
    try:
        start_date = get_last_sync_date(cursor)
        end_date = datetime.now()
        params = {"start_date": start_date.strftime("%Y-%m-%d"), "end_date": end_date.strftime("%Y-%m-%d")}

        trans_auth = (API_USER, API_PASS)
        tdata = fetch_paginated_data(API_TRANSACTIONS_URL, auth=trans_auth, params=params)
        logger.info(f"Fetched {len(tdata)} transactions")

        jwt_token = get_jwt_token()
        headers = {"Authorization": f"JWT {jwt_token}"} if jwt_token else None

        edata = fetch_paginated_data(EMP_API_URL, headers=headers if headers else None, auth=trans_auth)
        area_data = fetch_paginated_data(AREA_API_URL, headers=headers if headers else None, auth=trans_auth)

        area_map = {str(a.get("area_code") or a.get("id")): a.get("area_name") or a.get("name") or "Unknown" for a in area_data}
        employees = {}
        for e in edata:
            code = e.get("emp_code")
            if code:
                areas = e.get("area") or []
                location = "Unknown"
                if areas and isinstance(areas, list):
                    ac = areas[0].get("area_code") or areas[0].get("id")
                    if ac is not None:
                        location = area_map.get(str(ac), "Unknown")
                employees[str(code)] = {"first_name": e.get("first_name") or "Unknown", "location": location}
        return tdata, employees
    finally:
        conn.close()

# ===== PROCESS AND STORE =====
def process_and_store():
    tdata, employees = fetch_data()
    if not tdata:
        logger.info("No transactions to process.")
        return

    conn = get_connection()
    cursor = conn.cursor()

    insert_batch = []
    batch_count = 0

    for tx in tdata:
        emp_code = str(tx.get("emp_code"))
        punch_time_str = tx.get("punch_time")
        if not punch_time_str or not emp_code:
            continue
        emp = employees.get(emp_code, {})
        emp_name = emp.get("first_name", "Unknown")
        location = emp.get("location", "Unknown")

        try:
            punch_dt = datetime.fromisoformat(punch_time_str)
        except Exception:
            punch_dt = datetime.strptime(punch_time_str, "%Y-%m-%d %H:%M:%S")

        punch_date = punch_dt.date()

        cursor.execute("SELECT punch_in, punch_out FROM dbo.EmployeeTransactions WHERE emp_code=%s AND punch_date=%s", (emp_code, punch_date))
        row = cursor.fetchone()
        if not row:
            insert_batch.append((emp_code, emp_name, location, punch_date, punch_dt, punch_dt, 0.0))
        else:
            punch_in, punch_out = row
            changed = False
            if not punch_in or punch_dt < punch_in:
                punch_in = punch_dt
                changed = True
            if not punch_out or punch_dt > punch_out:
                punch_out = punch_dt
                changed = True
            if changed:
                duration = (punch_out - punch_in).total_seconds() / 3600.0
                cursor.execute("UPDATE dbo.EmployeeTransactions SET punch_in=%s, punch_out=%s, work_duration=%s WHERE emp_code=%s AND punch_date=%s",
                               (punch_in, punch_out, duration, emp_code, punch_date))

        if len(insert_batch) >= BATCH_SIZE:
            cursor.executemany("INSERT INTO dbo.EmployeeTransactions (emp_code, emp_name, location, punch_date, punch_in, punch_out, work_duration) VALUES (%s,%s,%s,%s,%s,%s,%s)", insert_batch)
            conn.commit()
            batch_count += len(insert_batch)
            insert_batch.clear()

    if insert_batch:
        cursor.executemany("INSERT INTO dbo.EmployeeTransactions (emp_code, emp_name, location, punch_date, punch_in, punch_out, work_duration) VALUES (%s,%s,%s,%s,%s,%s,%s)", insert_batch)
        conn.commit()
        batch_count += len(insert_batch)
        insert_batch.clear()

    # rebuild adjusted table
    cursor.execute("DELETE FROM dbo.EmployeeWorkAdjusted")
    cursor.execute("SELECT emp_code, emp_name, location, punch_date, punch_in, punch_out, work_duration FROM dbo.EmployeeTransactions")
    rows = cursor.fetchall()
    wa_batch = []
    for r in rows:
        emp_code, emp_name, location, punch_date, pin, pout, duration = r
        if not pin or not pout:
            continue
        if duration is None:
            duration = (pout - pin).total_seconds() / 3600.0
        adj_out = pin + timedelta(hours=8.5) if duration > 8.5 else pout
        total_hrs = min(duration, 8.5)
        status = get_attendance_status(total_hrs)
        wa_batch.append((emp_code, emp_name, location, punch_date, pin, adj_out, total_hrs, status, 0))
        if len(wa_batch) >= BATCH_SIZE:
            cursor.executemany("INSERT INTO dbo.EmployeeWorkAdjusted (emp_code, emp_name, location, punch_date, adj_in_time, adj_out_time, total_work_hrs, attendance_status, fetch_status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", wa_batch)
            conn.commit()
            wa_batch.clear()
    if wa_batch:
        cursor.executemany("INSERT INTO dbo.EmployeeWorkAdjusted (emp_code, emp_name, location, punch_date, adj_in_time, adj_out_time, total_work_hrs, attendance_status, fetch_status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", wa_batch)
        conn.commit()
        wa_batch.clear()

    conn.close()
    logger.info(f"Sync completed. {batch_count} transactions processed.")

# ===== API ENDPOINTS =====
@app.on_event("startup")
def startup_event():
    init_tables()

@app.get("/")
def home():
    return {"status": "Running", "message": "Employee Transaction API System Active"}

@app.get("/sync")
def sync_transactions(username: str = Depends(authenticate)):
    try:
        process_and_store()
        return {"status": "success", "message": "Transactions sync completed (check logs)."}
    except Exception as e:
        logger.exception(f"Sync endpoint error: {e}")
        return {"status": "error", "detail": str(e)}

@app.get("/fetch-adjusted")
def fetch_adjusted(username: str = Depends(authenticate)):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM dbo.EmployeeWorkAdjusted WHERE fetch_status=0")
        rows = cursor.fetchall()
        column_names = [c[0] for c in cursor.description]
        ids = [r[0] for r in rows]
        if ids:
            cursor.executemany("UPDATE dbo.EmployeeWorkAdjusted SET fetch_status=1 WHERE id=%s", [(i,) for i in ids])
            conn.commit()
        conn.close()
        return {"rows": [dict(zip(column_names, r)) for r in rows]}
    except Exception as e:
        logger.exception(f"fetch-adjusted error: {e}")
        return {"status": "error", "detail": str(e)}

# ===== RUN SERVER =====
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

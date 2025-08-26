# filename: test_sqlalchemy_sqlserver.py

from sqlalchemy import create_engine, text

# =========================
# CONFIGURATION
# =========================
SERVER = "localhost"  # e.g., "localhost" or "192.168.1.100"
DATABASE = "iifltradedb"
USERNAME = "testuser"
PASSWORD = "Password123"
DRIVER = "ODBC Driver 18 for SQL Server"  # must match `odbcinst -q -d` output

# =========================
# CONNECTION STRING
# =========================
connection_string = (
    f"mssql+pyodbc://{USERNAME}:{PASSWORD}@{SERVER}:1433/{DATABASE}"
    f"?driver={DRIVER.replace(' ', '+')}&Encrypt=no&TrustServerCertificate=yes"
)

# =========================
# CREATE ENGINE
# =========================
engine = create_engine(connection_string)

# =========================
# TEST CONNECTION
# =========================
try:

    with engine.connect() as conn:
        result = conn.execute(text("SELECT GETDATE() AS CurrentDateTime"))
        for row in result:
            print("Current SQL Server datetime:", row[0])

except Exception as e:
    print("Error connecting to SQL Server:", e)

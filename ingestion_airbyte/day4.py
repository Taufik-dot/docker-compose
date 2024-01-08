import psycopg2
import pandas
from sqlalchemy import create_engine

# Koneksi Postgres
pg_user = "postgres"
pg_password = "pass"
pg_host = "localhost"
pg_port = 5432
pg_database = "store"

#  koneksi Citus
citus_user = "postgres"
citus_password = "pass"
citus_host = "localhost"  
citus_port = 15432 
citus_database = "store"

# Membuat koneksi ke database PostgreSQL
pg_conn = psycopg2.connect(
    user=pg_user,
    password=pg_password,
    host=pg_host,
    port=pg_port,
    database=pg_database
)

# Membuat koneksi ke database Citus menggunakan SQLAlchemy
citus_engine = create_engine(f"postgresql+psycopg2://{citus_user}:{citus_password}@{citus_host}:{citus_port}/{citus_database}")

# Daftar tabel yang akan diingest
tables_to_ingest = ["brands", "orders", "order_detail", "products"]

# Melakukan ingest tabel-tabel
for table_name in tables_to_ingest:
    # Query data dari PostgreSQL
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute(f"SELECT * FROM {table_name}")
    rows = pg_cursor.fetchall()
    
    # Insert data ke Citus
    with citus_engine.connect() as citus_conn:
        for row in rows:
            citus_conn.execute(f"INSERT INTO {table_name} VALUES {row}")

# Menutup koneksi ke PostgreSQL
pg_conn.close()

# Menutup koneksi ke Citus
citus_engine.dispose()

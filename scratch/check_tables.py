import psycopg2
import pandas as pd

DB_CONFIG = {
    'host': 'localhost',
    'database': 'openalex_db',
    'user': 'postgres',
    'password': 'tu_contasena',
    'port': 5432
}

def check_tables():
    conn = psycopg2.connect(**DB_CONFIG)
    query = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'openalex' ORDER BY table_name;"
    df = pd.read_sql_query(query, conn)
    print(df)
    conn.close()

if __name__ == "__main__":
    check_tables()

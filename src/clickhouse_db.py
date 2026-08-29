"""
clickhouse_db.py - ClickHouse Client Connection for RevistasLATAM
Provides unified connection to ClickHouse database based on environment variables (.env).
"""
import os
from dotenv import load_dotenv

load_dotenv()

try:
    import clickhouse_connect
    HAS_CH_CONNECT = True
except ImportError:
    HAS_CH_CONNECT = False


class ClickHouseClient:
    def __init__(self, host=None, port=None, user=None, password=None, database=None):
        self.host = host or os.getenv('CH_HOST', 'localhost')
        self.port = int(port or os.getenv('CH_PORT', 8124))
        self.user = user or os.getenv('CH_USER', 'default')
        self.password = password or os.getenv('CH_PASSWORD', '')
        self.database = database or os.getenv('CH_DATABASE', 'default')
        self.client = None

    def get_client(self):
        if not HAS_CH_CONNECT:
            raise ImportError("clickhouse_connect no esta instalado. Instalar con: pip install clickhouse-connect")
        if not self.client:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                database=self.database,
                connect_timeout=15,
                send_receive_timeout=120
            )
        return self.client

    def is_connected(self):
        try:
            cl = self.get_client()
            res = cl.command("SELECT 1")
            return res == 1
        except Exception:
            return False

    def query_df(self, query, parameters=None):
        cl = self.get_client()
        return cl.query_df(query, parameters=parameters)

    def command(self, cmd, parameters=None):
        cl = self.get_client()
        return cl.command(cmd, parameters=parameters)

    def query(self, query, parameters=None):
        cl = self.get_client()
        return cl.query(query, parameters=parameters)

    def insert_df(self, table, df):
        cl = self.get_client()
        return cl.insert_df(table, df)

    def close(self):
        if self.client:
            try:
                self.client.close()
            except Exception:
                pass
            self.client = None


# Singleton instance
ch_client = ClickHouseClient()

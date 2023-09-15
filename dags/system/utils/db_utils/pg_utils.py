import asyncio
import psycopg2


def create_connection():
  conn = psycopg2.connect('postgresql://narasio:narasio@airflow-narasio-postgres-mine-1:5431/coindesk')
  return conn

def insert_query(connection, table, cols, values):
  print("INSERT INTO " + table + "(" + (', '.join(cols)) + ")" + " VALUES (" + ", ".join(["%s" for col in cols]) + ")")
  query = connection.cursor().execute("INSERT INTO " + table + "(" + (', '.join(cols)) + ")" + " VALUES (" + ", ".join(["%s" for col in cols]) + ")", tuple(values))
  return query

def execute_query(connection, query):
  connection.cursor().execute(query)
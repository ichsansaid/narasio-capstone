import asyncio
from datetime import datetime
import json
import os

import requests

from system.utils.db_utils import pg_utils

from re import sub

def cache_snake_case():
    snake_case_hash = {}
    def snake_case(s):
        if s in snake_case_hash:
            return snake_case_hash[s]
        return '_'.join(
            sub('([A-Z][a-z]+)', r' \1',
                sub('([A-Z]+)', r' \1',
                    s.replace('-', ' '))).split()).lower()
    return snake_case


def extract(data, result, cols):
    snake_case_funct = cache_snake_case()
    stack = [(None, data)]
    while len(stack) > 0:
        current_key, node = stack.pop()
        if type(node) == dict:
            for key in node:
                print(current_key, key)
                stack.append(("_".join([
                    *([snake_case_funct(current_key)] if current_key is not None else []), 
                    snake_case_funct(key)
                ]), node[key]))
        else:
            result[current_key] = node
    return result


def col_timestamp(data):
    data['time_updated_iso'] = datetime.strptime(data['time_updated_iso'], '%Y-%m-%dT%H:%M:%S%z')
    data['time_updated'] = datetime.strptime(data['time_updated'], '%b %d, %Y %H:%M:%S UTC')
    data['last_update'] = datetime.now()


def cleaning_data(include=None):
    if include is None:
        include = []
    hash_include = {}
    for inc in include:
        hash_include[inc] = True
    response = requests.get('https://api.coindesk.com/v1/bpi/currentprice.json')
    data = json.loads(response.text)
    result = {}
    extract(data, result, [])
    keys = list(result.keys())
    print(result, hash_include, keys)
    for col in keys:
        if not (col in hash_include):
            del result[col]
    return result


def usd_to_idr(usd: float):
    return usd * 15381.70


def bpi_idr_rate_float_etl(data):
    data['bpi_idr_rate_float'] = usd_to_idr(data['bpi_usd_rate_float'])

def mig_up(connection):
  connection.cursor().execute("""
    CREATE TABLE IF NOT EXISTS datamart_coindesk_api (
      id SERIAL PRIMARY KEY,
      disclaimer VARCHAR,
      chart_name VARCHAR,
      time_updated TIMESTAMP,
      time_updated_iso TIMESTAMP,
      bpi_usd_code VARCHAR,
      bpi_usd_description VARCHAR,
      bpi_usd_rate_float FLOAT,
      bpi_gbp_code VARCHAR,
      bpi_gbp_description VARCHAR,
      bpi_gbp_rate_float FLOAT,
      bpi_eur_code VARCHAR,
      bpi_eur_description VARCHAR,
      bpi_eur_rate_float FLOAT,
      bpi_idr_rate_float FLOAT,
      last_update TIMESTAMP
    );
  """)

def case_narasio():
    result = cleaning_data(include=[
        'disclaimer', 'chart_name', 'time_updated',
        'time_updated_iso', 
        'bpi_usd_code', 'bpi_usd_rate_float', 'bpi_usd_description', 
        'bpi_gdp_code', 'bpi_gdp_rate_float', 'bpi_gdp_description', 
        'bpi_gbp_code', 'bpi_gbp_rate_float', 'bpi_gbp_description', 
        'bpi_eur_code', 'bpi_eur_rate_float', 'bpi_eur_description'
    ])
    bpi_idr_rate_float_etl(result)
    col_timestamp(result)
    conn = pg_utils.create_connection()
    mig_up(conn)
    pg_utils.insert_query(conn, "datamart_coindesk_api", result.keys(), [str(value) for value in result.values()])
    conn.commit()
    return result



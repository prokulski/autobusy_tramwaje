import urllib.request
import urllib.error
import json
import time
import pandas as pd
import sqlite3

import logging

logging.basicConfig(level=logging.INFO, filename='grabber.log', format='%(asctime)s|%(levelname)s|%(filename)s|%(message)s')

# api key in separate file
from apikey import *


WAIT_TIME = 30
resource_id = "f2e5503e927d-4ad3-9500-4ab9e55deb59"

# tworzymy bazę danych
db_conn = sqlite3.connect("autobusy_i_tramwaje.sqlite", detect_types=sqlite3.PARSE_DECLTYPES)
c = db_conn.cursor()

# usuwamy tabelę jeśli istniała
c.execute('''CREATE TABLE IF NOT EXISTS buses
             (
                line TEXT,
                time DATETIME,
                long DOUBLE,
                lat DOUBLE,
                brigade TEXT
             )
             ''')

# usuwamy tabelę jeśli istniała
c.execute('''CREATE TABLE IF NOT EXISTS trams
             (
                line TEXT,
                time DATETIME,
                long DOUBLE,
                lat DOUBLE,
                brigade TEXT
             )
             ''')

def get_json_from_api(query_url):
    error = False

    try:
        j = urllib.request.urlopen(query_url)
    except urllib.error.HTTPError as e:
        # Return code error (e.g. 404, 501, ...)
        logging.warning('== API Error - HTTPError: {}'.format(e.code))
        error = True
    except urllib.error.URLError as e:
        # Not an HTTP-specific error (e.g. connection refused)
        logging.warning('== API Error - URLError: {}'.format(e.reason))
        error = True

    if not error:
        j = json.loads(j.read().decode())
        return True, j

    return False, None


def get_buses():
    query_url = f"https://api.um.warszawa.pl/api/action/busestrams_get/?resource_id={resource_id}&type=1&apikey={apikey}"
    success, df = get_json_from_api(query_url)
    if success:
        try:
          df = pd.DataFrame(df['result']).drop_duplicates()
          logging.info("Pobrane autobusy, " + str(len(df)) + " elementów.")
          return df
        except:
          logging.warning("Pobranie autobusów nieudane: " + str(df))
          return pd.DataFrame(columns=['Lines', 'Time', 'Lon', 'Lat', 'Brigade'])
    else:
        logging.error("Pobranie autobusów nieudane")
        return pd.DataFrame(columns=['Lines', 'Time', 'Lon', 'Lat', 'Brigade'])

def get_trams():
    query_url = f"https://api.um.warszawa.pl/api/action/busestrams_get/?resource_id={resource_id}&type=2&apikey={apikey}"
    success, df = get_json_from_api(query_url)
    if success:
        try:
          df = pd.DataFrame(df['result']).drop_duplicates()
          logging.info("Pobrane tramwaje, " + str(len(df)) + " elementów.")
          return df
        except:
          logging.warning("Pobranie tramwajów nieudane: " + str(df))
          return pd.DataFrame(columns=['Lines', 'Time', 'Lon', 'Lat', 'Brigade'])
    else:
        logging.error("Pobranie tramwajów nieudane.")
        return pd.DataFrame(columns=['Lines', 'Time', 'Lon', 'Lat', 'Brigade'])


def save_buses(db_conn):
    buses = get_buses()[['Lines', 'Time', 'Lon', 'Lat', 'Brigade']]
    buses.columns = ['line', 'time', 'long', 'lat', 'brigade']
    buses['time'] = buses['time'].apply(lambda x: pd.to_datetime(x))
    buses.to_sql("buses", db_conn, if_exists="append", index=False)

def save_trams(db_conn):
    trams = get_trams()[['Lines', 'Time', 'Lon', 'Lat', 'Brigade']]
    trams.columns = ['line', 'time', 'long', 'lat', 'brigade']
    trams['time'] = trams['time'].apply(lambda x: pd.to_datetime(x))
    trams.to_sql("trams", db_conn, if_exists="append", index=False)


if __name__ == "__main__":
    while True:
        save_trams(db_conn)
        save_buses(db_conn)
        logging.info("Czekam " + str(WAIT_TIME) + " sekund.")
        time.sleep(WAIT_TIME)


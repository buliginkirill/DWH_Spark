import os

import psycopg2
from psycopg2 import ProgrammingError
from psycopg2.extras import execute_values
from .log import Logger

import csv

from src.settings import PG_HOST, PG_PORT, PG_USER, PG_DB, PG_PASSWORD

class DataLoader:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            database=PG_DB,
            password=PG_PASSWORD
        )

    def read_data_mart(self):
        cur = self.conn.cursor()
        cur.execute('select * from dwh.craftsman_report_datamart')
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        return [cols, rows]

    def load_source_ddl(self):
        INP_DIR = './input/DWH-main/Скрипты создания таблиц источников'
        SCHEMAS = ['source1', 'source2', 'source3']
        for s in SCHEMAS:
            cur = self.conn.cursor()
            sql = f'DROP SCHEMA IF EXISTS {s} CASCADE;'
            cur.execute(sql)
            self.conn.commit()
        files = os.listdir(INP_DIR)

        for f in files:
            if not f.endswith('.sql'):
                continue
            Logger.log(f'Loading {f}')
            with open(os.path.join(INP_DIR, f), 'r') as file:
                sql = file.read()
                sql = sql.split(';\n')
                for s in sql:
                    cur = self.conn.cursor()
                    try:
                        cur.execute(s)
                    except ProgrammingError as e:
                        if not str(e) == "can't execute an empty query":
                            Logger.log('ERROR')
                            Logger.log(s)
                            Logger.log(str(e))
                    self.conn.commit()

    def load_marts_ddl(self):
        INP_DIR = './input/DWH-main/Скрипты по созданию таблиц DWH и витрины'
        SCHEMAS = ['dwh']
        for s in SCHEMAS:
            cur = self.conn.cursor()
            sql = f'DROP SCHEMA IF EXISTS {s} CASCADE;'
            cur.execute(sql)
            self.conn.commit()
        files = os.listdir(INP_DIR)

        for f in files:
            if not f.endswith('.sql'):
                continue
            Logger.log(f'Loading {f}')
            with open(os.path.join(INP_DIR, f), 'r') as file:
                sql = file.read()
                sql = sql.split(';\n')
                for s in sql:
                    cur = self.conn.cursor()
                    try:
                        cur.execute(s)
                    except ProgrammingError as e:
                        if not str(e) == "can't execute an empty query":
                            Logger.log('ERROR')
                            Logger.log(s)
                            Logger.log(str(e))
                    self.conn.commit()

    def get_table_names(self):
        sql = '''SELECT
    schemata.schema_name AS schema_name,
    tables.table_name AS table_name
FROM
    information_schema.schemata AS schemata
LEFT JOIN
    information_schema.tables AS tables
ON
    schemata.schema_name = tables.table_schema
WHERE
    schemata.schema_name like 'source%'
ORDER BY
    schemata.schema_name, tables.table_name;'''

        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()

    def load_data(self):
        INP_DIR = './input/DWH-main/Данные для источников'
        files = os.listdir(INP_DIR)
        files = [f for f in files if f.endswith('.csv')]
        tables = self.get_table_names()
        for sch, tbl in tables:
            cur = self.conn.cursor()
            cur.execute(f'DELETE FROM {sch}.{tbl}')
            self.conn.commit()

            file = [f for f in files if f.startswith('complete_'+tbl+'_2')]
            Logger.log(f'Uploading {sch}.{tbl} from {file}')
            with open(os.path.join(INP_DIR, file[0]), 'r') as file:
                data_reader = csv.reader(file, delimiter=',', quotechar='"')
                data = [r for r in data_reader]
                rows = data[1:]
                counter = 0
                for r in rows:
                    counter+=1
                    columns = data[0]
                    r = [None if v=='' else v for v in r]

                    zipped = list(zip(columns, r))
                    zipped = [(k, v) for k, v in zipped if v is not None]
                    columns = [k for k, v in zipped]
                    r = [v for k, v in zipped]

                    sql = f'INSERT INTO {sch}.{tbl} ({",".join(columns)}) OVERRIDING SYSTEM VALUE VALUES %s;'
                    cur = self.conn.cursor()
                    try:
                        # Logger.log(sql)
                        execute_values(cur, sql, [tuple(r)])
                    except ProgrammingError as e:
                            Logger.log('ERROR')
                            Logger.log(sql)
                            Logger.log(str(e))
                    self.conn.commit()
            Logger.log(f'\tUploaded {counter} rows to {sch}.{tbl}')

def run_load_data():
    dl = DataLoader()
    dl.load_source_ddl()
    dl.load_marts_ddl()
    dl.load_data()



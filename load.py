import psycopg2 as pg
from sqlalchemy import create_engine
from settings import get_config
from autologging import logged, traced
import pandas as pd
from pathlib import Path

CONFIG = get_config()

@traced
@logged
class NBALoad:
    pass

@traced
@logged
class PostgresDB:
    def __init__(self, config):
        self.__log.info("Connecting to database...")
        self.connection = self._get_connection(config['databases.postgres'])

        if (sql_path := Path(config['data.sql.usp_upsert_box_scores.sql'])).exists():
            self.exec = sql_path.read_text()

    def _create_engine(self):
        pass

    def _get_connection(self, conn_conf):
        try:
            connection = pg.connect(**conn_conf)
        except:
            self.__log.error("CONNECTION FAILED")

        return connection

    def _execute_many(self, df):
         # Path()

        tpls = [tuple(x) for x in df.values.tolist()]

        cols = ','.join(df.columns.values.tolist())

        print(cols)

        sql = f"INSERT INTO %s(%s) VALUES ({','.join(['%s' for i in df.columns])})"

        print(sql)
8

if __name__ == "__main__":
    df = pd.read_csv(".data/raw_data/box_scores/2010_box_scores.csv").drop(columns=['Unnamed: 0']).iloc[:10]
    pgres = PostgresDB(CONFIG)
    pgres._execute_many(df)
    print()

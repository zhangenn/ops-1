from constants import SQL_USER, SQL_PWD, SQL_DB, SQL_HOST
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine


engine = create_engine(URL(
    drivername='postgres+psycopg2',
    username=SQL_USER,
    password=SQL_PWD,
    database=SQL_DB,
    query={'host': SQL_HOST}))

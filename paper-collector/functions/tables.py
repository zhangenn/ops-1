from sqlalchemy import Column, Integer, String, DateTime, JSON, ForeignKey
from sqlalchemy import create_engine, MetaData, Table, update
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from constants import *
from sqlalchemy.engine.url import URL


engine = create_engine(URL(
    drivername='postgres+psycopg2',
    username=SQL_USER,
    password=SQL_PWD,
    database=SQL_DB,
    query={'host': SQL_HOST}))

Base = declarative_base()
Base.metadata.create_all(bind=engine)


class PaperTable(Base):
    __tablename__ = 'PaperTable'
    id = Column(String, primary_key=True)
    version = Column(Integer, nullable=False)
    author = Column(String, nullable=False)
    authors = relationship('AuthorTable',
                           backref='author_entry')
    title = Column(String, nullable=False)
    summary = Column(String)
    arxiv_comment = Column(String)
    tags = Column(JSON)
    published_datetime = Column(DateTime)
    updated_datetime = Column(DateTime)


class AuthorTable(Base):
    __tablename__ = 'AuthorTable'
    id = Column(String, primary_key=True)
    author = Column(String, nullable=False)
    paper_id = Column(String, ForeignKey('PaperTable.id'))

from sqlalchemy import create_engine, MetaData, Table, update, insert, func
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.engine.url import URL
from constants import *


def main():
    source_engine = create_engine(URL(
        drivername='postgres+psycopg2',
        username=SQL_USER,
        password=SQL_PWD,
        database=SQL_DB,
        query={'host': SQL_HOST}))

    source_metadata = MetaData()
    source_base = automap_base(metadata=source_metadata)
    source_base.prepare(source_engine, reflect=True)

    PaperTable = Table('PaperTable', source_metadata, autoload=True,
                       autoload_with=source_engine)
    AuthorTable = Table('AuthorTable', source_metadata, autoload=True,
                        autoload_with=source_engine)

    Source_session = sessionmaker(source_engine)
    source_sess = Source_session()
    query_paper = source_sess.query(PaperTable)
    query_author = source_sess.query(AuthorTable)

    dest_engine = create_engine(URL(
        drivername='postgres+psycopg2',
        username=SQL_USER,
        password=SQL_PWD,
        database=SQL_DB_DEST,
        query={'host': SQL_HOST}))

    dest_metadata = MetaData()
    dest_base = automap_base(metadata=dest_metadata)
    dest_base.prepare(dest_engine, reflect=True)

    new_paper_table = Table('papers', dest_metadata, autoload=True,
                            autoload_with=dest_engine)
    new_author_table = Table('authors', dest_metadata, autoload=True,
                             autoload_with=dest_engine)

    Dest_session = sessionmaker(dest_engine)
    dest_sess = Dest_session()
    for row in query_paper:
        dest_sess.execute(new_paper_table.insert(row))
        dest_sess.commit()
    print("Papers Completed")

    for row in query_author:
        dest_sess.execute(new_author_table.insert(row))
        dest_sess.commit()
    print("Authors Completed")


if __name__ == "__main__":
    main()

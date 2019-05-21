from sqlalchemy import create_engine, MetaData, Table, update
from sqlalchemy import Column, Integer, String, DateTime, JSON, ForeignKey
from sqlalchemy import inspect
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.engine.url import URL

import os
import arxiv
import pandas as pd
from datetime import datetime

keyword = '%28%22deep learning%22 OR %22neural network%22 \
            OR %22GPU%22 OR %22graphics processing unit%22 \
            OR %22reinforcement learning%22 OR %22perceptron%22%29'

category = '%28cat:cs.LG OR cat:stat.ML OR cat:cs.AI OR cat:cs.CV%29'

search_query = keyword + " AND " + category

FETCH_MAX = 100

SQL_USER = os.environ.get('SQL_USER')
SQL_PWD = os.environ.get('SQL_PWD')
SQL_HOST = os.environ.get('SQL_HOST')
SQL_DB = os.environ.get('SQL_DB')

assert SQL_USER, 'SQL_USER is required.'
assert SQL_PWD, 'SQL_PWD is required.'
assert SQL_HOST, 'SQL_HOST is required.'
assert SQL_DB, 'SQL_DB is required.'


def extract_category(tag_list):
    list_of_dict = []
    for i in range(len(tag_list)):
        temp_list = []
        for j in range(len(tag_list[i])):
            temp_list.append(tag_list[i][j]['term'])
        list_of_dict.append({'term': temp_list})
    return list_of_dict


def extract_column(df_file):
    pub_list = df_file['published'].values
    upd_list = df_file['updated'].values
    pub_dt = pd.DataFrame({'published_datetime': [
        datetime.strptime(item, "%Y-%m-%dT%H:%M:%SZ") for item in pub_list]})
    upd_dt = pd.DataFrame({'updated_datetime': [
        datetime.strptime(item, "%Y-%m-%dT%H:%M:%SZ") for item in upd_list]})

    tag_list = df_file['tags'].tolist()
    category_tags = pd.DataFrame({'category_tags': extract_category(tag_list)})

    df_file['unique_id'] = df_file['id'].str.extract(
        '(\d\d\d\d\.\d\d\d\d\d)', expand=True)
    df_file['version_number'] = df_file['id'].str.extract('(\d$)', expand=True)

    final_df = df_file[['unique_id', 'version_number', 'author',
                        'title', 'summary', 'arxiv_comment', 'authors']]

    final_df = pd.concat([final_df, category_tags, pub_dt, upd_dt], axis=1)

    final_df = final_df.drop_duplicates(subset='unique_id',
                                        keep='first', inplace=False)

    final_df = final_df.dropna(subset=['unique_id'], inplace=False)

    return final_df


def insert_new_articles_initiation(session, PaperTable,
                                   AuthorTable, id_string, df):
    paper_row = PaperTable(id=id_string,
                           version=int(df.iloc[0, 1]),
                           author=df.iloc[0, 2],
                           title=df.iloc[0, 3],
                           summary=df.iloc[0, 4],
                           arxiv_comment=df.iloc[0, 5],
                           tags=df.iloc[0, 7],
                           published_datetime=df.iloc[0, 8],
                           updated_datetime=df.iloc[0, 9])
    session.add(paper_row)
    session.commit()

    authors = df.iloc[0, 6]
    for i in range(len(authors)):
        id_str = id_string + "-" + str(i)
        author_row = AuthorTable(id=id_str,
                                 author=authors[i],
                                 author_entry=paper_row)
        session.add(author_row)
        session.commit()


def initiate_database(request):
    new_articles = arxiv.query(search_query, max_results=FETCH_MAX,
                               sort_by="lastUpdatedDate",
                               sort_order="descending")
    new_articles_df = pd.DataFrame.from_dict(new_articles)
    print("Successfully retrieved articles from arXiv.")
    ordered_new_articles = new_articles_df.reindex(
        columns=['title', 'author', 'authors', 'id', 'arxiv_comment',
                 'arxiv_primary_category', 'published', 'summary',
                 'tags', 'updated'])
    article_df = extract_column(ordered_new_articles)
    article_id_lst = article_df['unique_id'].values

    engine = create_engine(URL(
        drivername='postgres+psycopg2',
        username=SQL_USER,
        password=SQL_PWD,
        database=SQL_DB,
        query={'host': SQL_HOST}))

    Base = declarative_base()

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

    Base.metadata.create_all(bind=engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    count_insert = 0
    for id_string in article_id_lst:
        row_df = article_df[article_df['unique_id'] == id_string]
        insert_new_articles_initiation(session, PaperTable, AuthorTable,
                                       id_string, row_df)
        count_insert += 1
    
    session.close()
    return f'Completed: Inserted {count_insert} new articles.'

if __name__ == '__main__':
    r = initiate_database(None)
    print(r)

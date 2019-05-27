from sqlalchemy import create_engine, MetaData, Table, update, insert, func
from sqlalchemy import Column, Integer, String, DateTime, JSON, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
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

SQL_USER = os.environ.get('SQL_USER')
SQL_PWD = os.environ.get('SQL_PWD')
SQL_HOST = os.environ.get('SQL_HOST')
SQL_DB = os.environ.get('SQL_DB')

START_INDEX = int(os.environ.get('START', '0'))
FETCH_MAX = int(os.environ.get('MAX'))
fetch_additional = 200

assert SQL_USER, 'SQL_USER is required.'
assert SQL_PWD, 'SQL_PWD is required.'
assert SQL_HOST, 'SQL_HOST is required.'
assert SQL_DB, 'SQL_DB is required.'


def obtain_new_articles():
    new_articles = arxiv.query(search_query, max_results=FETCH_MAX,
                               start=START_INDEX,
                               sort_by="lastUpdatedDate",
                               sort_order="descending")
    new_articles_df = pd.DataFrame.from_dict(new_articles)
    ordered_new_articles = new_articles_df.reindex(
        columns=['title', 'author', 'authors', 'id', 'arxiv_comment',
                 'arxiv_primary_category', 'published', 'summary',
                 'tags', 'updated'])

    return ordered_new_articles


def extract_category(tag_list):
    list_of_dict = []
    for i in range(len(tag_list)):
        temp_list = []
        for j in range(len(tag_list[i])):
            temp_list.append(tag_list[i][j]['term'])
        list_of_dict.append({'term': temp_list})
    return list_of_dict


def extract_column(df_file):
    # Extracting information
    pub_list = df_file['published'].values
    upd_list = df_file['updated'].values
    pub_dt = pd.DataFrame({'published_datetime': [
        datetime.strptime(item, "%Y-%m-%dT%H:%M:%SZ") for item in pub_list]})
    upd_dt = pd.DataFrame({'updated_datetime': [
        datetime.strptime(item, "%Y-%m-%dT%H:%M:%SZ") for item in upd_list]})

    tag_list = df_file['tags'].tolist()
    category_tags = pd.DataFrame({'category_tags': extract_category(tag_list)})

    df_file['unique_id'] = df_file['id'].astype(str).str.extract(
        '(\d\d\d\d\.\d\d\d\d\d)', expand=True)
    df_file['version_number'] = df_file['id'].astype(str).str.extract('(\d$)', expand=True)

    final_df = df_file[['unique_id', 'version_number', 'author',
                        'title', 'summary', 'arxiv_comment', 'authors']]

    final_df = pd.concat([final_df, category_tags, pub_dt, upd_dt], axis=1)

    final_df = final_df.drop_duplicates(subset='unique_id',
                                        keep='first', inplace=False)

    final_df = final_df.dropna(subset=['unique_id'], inplace=False)

    return final_df


def check_existence(session, PaperTable, id_string):
    query = session.query(PaperTable).filter(
        PaperTable.c.id == id_string).all()

    return len(query) > 0


def update_existing_articles(session, PaperTable, id_string, df):
    upd = update(PaperTable).where(PaperTable.c.id == id_string).\
        values(version=int(df.iloc[0, 1]),
               summary=df.iloc[0, 4],
               arxiv_comment=df.iloc[0, 5],
               updated_datetime=df.iloc[0, 9])
    
    session.execute(upd)
    session.commit()


def insert_new_articles(session, PaperTable, AuthorTable, id_string, df):
    # Adding records into Paper Table
    paper_row = insert(PaperTable).values(id=id_string,
                                           version=int(df.iloc[0, 1]),
                                           author=df.iloc[0, 2],
                                           title=df.iloc[0, 3],
                                           summary=df.iloc[0, 4],
                                           arxiv_comment=df.iloc[0, 5],
                                           tags=df.iloc[0, 7],
                                           published_datetime=df.iloc[0, 8],
                                           updated_datetime=df.iloc[0, 9])

    session.execute(paper_row)
    session.commit()

    # Adding records into Author Table
    authors = df.iloc[0, 6]
    for i in range(len(authors)):
        id_str = id_string + "-" + str(i)
        author_row = insert(AuthorTable).values(id=id_str,
                                                author=authors[i],
                                                paper_id=id_string)

        session.execute(author_row)
        session.commit()


def update_database(data, context):
    engine = create_engine(URL(
                    drivername='postgres+psycopg2',
                    username=SQL_USER,
                    password=SQL_PWD,
                    database=SQL_DB,
                    query={'host': SQL_HOST}))

    # reflect the tables
    metadata = MetaData()
    Base = automap_base(metadata=metadata)
    Base.prepare(engine, reflect=True)

    # Mapped classes with names matching that of the table name
    PaperTable = Table('papers', metadata, autoload=True,
                        autoload_with=engine)
    AuthorTable = Table('authors', metadata, autoload=True,
                        autoload_with=engine)
    
    Session = sessionmaker(bind=engine)
    session = Session()

    latest_record = session.query(
        func.max(PaperTable.c.updated_datetime)).first()[0]
    article_df = extract_column(obtain_new_articles())
    min_new_fetch = article_df['updated_datetime'].min()
    
    # first-time inserting entries
    if latest_record == None:
        article_id_lst = article_df['unique_id'].values

        count_insert = 0
        for id_string in article_id_lst:
            row_df = article_df[article_df['unique_id'] == id_string]
            insert_new_articles(session, PaperTable, AuthorTable, id_string, row_df)
            count_insert += 1

        session.close()
        return f'Completed: Inserted {count_insert} new articles.'

    else:
        global FETCH_MAX
        while latest_record < min_new_fetch:
            FETCH_MAX += fetch_additional
            article_df = extract_column(obtain_new_articles())
            min_new_fetch = article_df['updated_datetime'].min()
            print(f"Fetch #: {FETCH_MAX}")

        article_id_lst = article_df['unique_id'].values

        count_update = 0
        count_insert = 0

        for id_string in article_id_lst:
            row_df = article_df[article_df['unique_id'] == id_string]

            if check_existence(session, PaperTable, id_string):
                exist_version = session.query(PaperTable.c.version).\
                    filter(PaperTable.c.id == id_string).one()
                if int(article_df.iloc[0, 1]) > exist_version[0]:
                    update_existing_articles(session, PaperTable, id_string, row_df)
                    count_update += 1

            else:
                insert_new_articles(session, PaperTable, AuthorTable, id_string, row_df)
                count_insert += 1

        session.close()
        return f"Completed: Inserted {count_insert} new articles, updated {count_update} existing articles."

if __name__ == '__main__':
    r = update_database(None, None)
    print(r)

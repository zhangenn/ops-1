
# Import modules
# import flash if message flashing is needed
from flask import Flask, request
from sqlalchemy import create_engine, MetaData, Table, update
from sqlalchemy import Column, Integer, String, DateTime, JSON, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.engine.url import URL

import os
import arxiv
import pandas as pd
from datetime import datetime

# Initial setup
app = Flask(__name__)
secret_key = os.urandom(50)
app.config['SECRET_KEY'] = secret_key
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False # reduce overhead

# Keywords include deep learning, neural network, GPU, graphics processing unit,
# reinforcement learning, OR perceptron
keyword = '%28%22deep learning%22 OR %22neural network%22 \
            OR %22GPU%22 OR %22graphics processing unit%22 \
            OR %22reinforcement learning%22 OR %22perceptron%22%29'

# Searches within machine learning (stats or cs), artificial intelligence or computer vision
category = '%28cat:cs.LG OR cat:stat.ML OR cat:cs.AI OR cat:cs.CV%29'

search_query = keyword + " AND " + category

# Max_results set to 1000
FETCH_MAX = 1000

def obtain_new_articles():
    new_articles = arxiv.query(search_query, max_results=FETCH_MAX,
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

    tag_list = df_file['tags'].tolist()  # tolist works better than values here
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

    # Execute for transient state, commit for persistent state
    session.execute(upd)
    session.commit()


def insert_new_articles(session, PaperTable, AuthorTable,
                        id_string, df):
    # Adding records into Paper Table
    paper_row = PaperTable.insert().values(id=id_string,
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
        author_row = AuthorTable.insert().values(id=id_str,
                                                 author=authors[i],
                                                 paper_id=id_string)

        session.execute(author_row)
        session.commit()


# Getting required args from HTTP line
@app.route('/')
def main():
    SQL_USER = request.args.get('SQL_USER')
    SQL_PWD = request.args.get('SQL_PWD')
    SQL_HOST = request.args.get('SQL_HOST')
    SQL_PORT = request.args.get('SQL_PORT')

    if not (SQL_USER and SQL_PWD and SQL_HOST and SQL_PORT):
        return "SQL_USER, SQL_PWD, SQL_HOST, and SQL_PORT are all required."

    db_url = {'drivername': 'postgres',
              'username': SQL_USER,
              'password': SQL_PWD,
              'host': SQL_HOST,
              'port': SQL_PORT}

    engine = create_engine(URL(**db_url))
    connection = engine.connect()
    print("Existing tables:", engine.table_names())

    # reflect the tables
    metadata = MetaData()
    Base = automap_base(metadata=metadata)
    Base.prepare(engine, reflect=True)

    # Mapped classes with names matching that of the table name
    PaperTable = Table('PaperTable', metadata, autoload=True,
                       autoload_with=engine)
    AuthorTable = Table('AuthorTable', metadata, autoload=True,
                        autoload_with=engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    article_df = extract_column(obtain_new_articles())
    article_id_lst = article_df['unique_id'].values

    count_update = 0
    count_insert = 0
    count_total = 0

    for id_string in article_id_lst:
        row_df = article_df[article_df['unique_id'] == id_string]

        count_total += 1
        if count_total % 200 == 0:
            print("Checked {} out of 1,000 retrieved results".format(count_total))

        if check_existence(session, PaperTable, id_string):
            exist_version = session.query(PaperTable.c.version).\
                filter(PaperTable.c.id == id_string).one()
            if int(article_df.iloc[0, 1]) > exist_version[0]:
                update_existing_articles(session,
                                         PaperTable, id_string, row_df)
                count_update += 1

        else:
            insert_new_articles(session, PaperTable, AuthorTable,
                                id_string, row_df)
            count_insert += 1

    return "Completed: Inserted {} new articles, updated {} existing articles.".format(
        count_insert, count_update)


if __name__ == '__main__':
    app.run()

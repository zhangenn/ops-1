
# Import modules
# import flash if message flashing is needed
from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy
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
db = SQLAlchemy(app)

# Keywords include deep learning, neural network, GPU, graphics processing unit,
# reinforcement learning, OR perceptron
keyword = '%28%22deep learning%22 OR %22neural network%22 \
            OR %22GPU%22 OR %22graphics processing unit%22 \
            OR %22reinforcement learning%22 OR %22perceptron%22%29'

# Searches within machine learning (stats or cs), artificial intelligence or computer vision
category = '%28cat:cs.LG OR cat:stat.ML OR cat:cs.AI OR cat:cs.CV%29'

search_query = keyword + " AND " + category

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


def get_articles():
    new_articles = arxiv.query(search_query, max_results=5000,
                               sort_by="lastUpdatedDate",
                               sort_order="descending")
    new_articles_df = pd.DataFrame.from_dict(new_articles)
    print("Successfully retrieved articles from arXiv.")
    ordered_new_articles = new_articles_df.reindex(
        columns=['title', 'author', 'authors', 'id', 'arxiv_comment',
                 'arxiv_primary_category', 'published', 'summary',
                 'tags', 'updated'])
    article_df = extract_column(ordered_new_articles)
    return article_df


def insert_articles(PaperTable, AuthorTable, id_string, df):
    # Adding records into Paper Table
    paper_row = PaperTable(id=id_string,
                           version=int(df.iloc[0, 1]),
                           author=df.iloc[0, 2],
                           title=df.iloc[0, 3],
                           summary=df.iloc[0, 4],
                           arxiv_comment=df.iloc[0, 5],
                           tags=df.iloc[0, 7],
                           published_datetime=df.iloc[0, 8],
                           updated_datetime=df.iloc[0, 9])
    db.session.add(paper_row)
    db.session.commit()

    # Adding records into Author Table
    authors = df.iloc[0, 6]
    for i in range(len(authors)):
        id_str = id_string + "-" + str(i)
        author_row = AuthorTable(id=id_str,
                                 author=authors[i],
                                 author_entry=paper_row)
        db.session.add(author_row)
        db.session.commit()


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

    app.config.update(
        SQLALCHEMY_DATABASE_URI = URL(**db_url))

    # Create tables
    class PaperTable(db.Model):
        __tablename__ = 'PaperTable'
        id = db.Column(db.String, primary_key=True)
        version = db.Column(db.Integer, nullable=False)
        author = db.Column(db.String, nullable=False)
        authors = db.relationship('AuthorTable',
                               backref='author_entry')
        title = db.Column(db.String, nullable=False)
        summary = db.Column(db.String)
        arxiv_comment = db.Column(db.String)
        tags = db.Column(db.JSON)
        published_datetime = db.Column(db.DateTime)
        updated_datetime = db.Column(db.DateTime)


    class AuthorTable(db.Model):
        __tablename__ = 'AuthorTable'
        id = db.Column(db.String, primary_key=True)
        author = db.Column(db.String, nullable=False)
        paper_id = db.Column(db.String, db.ForeignKey('PaperTable.id'))

    db.create_all()
    article_df = get_articles()
    article_id_lst = article_df['unique_id'].values

    count_insert = 0
    for id_string in article_id_lst:
        row_df = article_df[article_df['unique_id'] == id_string]
        insert_articles(PaperTable, AuthorTable, id_string, row_df)
        count_insert += 1

        #if count_insert % 1000 == 0:
        #    flash("Inserted {} new articles.".format(count_insert))

    return "Completed: Inserted {} new articles.".format(count_insert)


if __name__ == '__main__':
    app.run()

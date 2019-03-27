
# Import modules
from sqlalchemy import create_engine, MetaData, Table, update
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy import inspect
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.engine.url import URL

import arxiv
import pandas as pd

# Keywords include deep learning, neural network, GPU, graphics processing unit,
# reinforcement learning, OR perceptron
keyword = '%28%22deep learning%22 OR %22neural network%22 \
            OR %22GPU%22 OR %22graphics processing unit%22 \
            OR %22reinforcement learning%22 OR %22perceptron%22%29'

# Searches within machine learning (stats or cs), artificial intelligence or computer vision
category = '%28cat:cs.LG OR cat:stat.ML OR cat:cs.AI OR cat:cs.CV%29'

search_query = keyword + " AND " + category

# max_results set to 1000
def obtain_new_articles():
    new_articles = arxiv.query(search_query, max_results=1000, 
                               sort_by="lastUpdatedDate", 
                               sort_order="descending")
    new_articles_df = pd.DataFrame.from_dict(new_articles)
    ordered_new_articles = new_articles_df.reindex(
                            columns=['title', 'author', 'authors', 'id', 'arxiv_comment',
                                     'arxiv_primary_category', 'published', 'summary',
                                     'tags', 'updated'])
    
    return ordered_new_articles


def extract_column(df_file):
    # Extracting information
    df_file['published_date'] = df_file['published'].str.extract('(\d\d\d\d-\d\d-\d\d)', expand=True)
    df_file['published_time'] = df_file['published'].str.extract('(\d\d:\d\d:\d\d)', expand=True)
    df_file['updated_date'] = df_file['updated'].str.extract('(\d\d\d\d-\d\d-\d\d)', expand=True)
    df_file['updated_time'] = df_file['updated'].str.extract('(\d\d:\d\d:\d\d)', expand=True)
    df_file['unique_id'] = df_file['id'].str.extract('(\d\d\d\d\.\d\d\d\d\d)', expand=True)
    df_file['version_number'] = df_file['id'].str.extract('(\d$)', expand=True)

    final_df = df_file[['unique_id', 'version_number', 'author', 
                        'title', 'summary', 'arxiv_comment', 
                        'published_date', 'published_time', 
                        'updated_date', 'updated_time', 
                        'tags', 'authors']]

    final_df = final_df.drop_duplicates(subset='unique_id', 
                                        keep='first', inplace=False)
    
    final_df = final_df.dropna(subset=['unique_id'], inplace=False)
    
    return final_df


def check_existence(session, PaperTable, id_string):
    query = session.query(PaperTable).filter(PaperTable.c.id==id_string).all()
    
    if len(query) > 0:
        return True
    else:
        return False


def update_existing_articles(session, PaperTable, id_string, df):
    upd = update(PaperTable).where(PaperTable.c.id==id_string).\
                             values(version = int(df.iloc[0, 1]), \
                                    summary = df.iloc[0, 4], \
                                    arxiv_comment = df.iloc[0, 5], \
                                    updated_date = df.iloc[0, 8], \
                                    updated_time = df.iloc[0, 9])
    
    # Execute for transient state, commit for persistent state
    session.execute(upd)
    session.commit()


def insert_new_articles(session, PaperTable, AuthorTable, 
                        id_string, df):
    # Adding records into Paper Table
    paper_row = PaperTable.insert().values(id = id_string, 
                                           version = int(df.iloc[0, 1]), 
                                           author = df.iloc[0, 2], 
                                           title = df.iloc[0, 3], 
                                           summary = df.iloc[0, 4], 
                                           arxiv_comment = df.iloc[0, 5], 
                                           published_date = df.iloc[0, 6], 
                                           published_time = df.iloc[0, 7], 
                                           updated_date = df.iloc[0, 8], 
                                           updated_time = df.iloc[0, 9])
    
    session.execute(paper_row)
    session.commit()
    
    # Adding records into Author Table
    authors = df.iloc[0, 11]
    for i in range(len(authors)):
        id_str = id_string + "-" + str(i)
        author_row = AuthorTable.insert().values(id = id_str, 
                                                 author = authors[i], 
                                                 paper_id = id_string)
    
        session.execute(author_row)
        session.commit()

def insert_new_articles_initiation(session, PaperTable, 
                                   AuthorTable, id_string, df):
    # Adding records into Paper Table
    paper_row = PaperTable(id = id_string, 
                           version = int(df.iloc[0, 1]), 
                           author = df.iloc[0, 2], 
                           title = df.iloc[0, 3], 
                           summary = df.iloc[0, 4], 
                           arxiv_comment = df.iloc[0, 5], 
                           published_date = df.iloc[0, 6], 
                           published_time = df.iloc[0, 7], 
                           updated_date = df.iloc[0, 8], 
                           updated_time = df.iloc[0, 9])
    session.add(paper_row)
    session.commit()
    
    # Adding records into Author Table
    authors = df.iloc[0, 11]
    for i in range(len(authors)):
        id_str = id_string + "-" + str(i)
        author_row = AuthorTable(id = id_str, 
                                 author = authors[i], 
                                 author_entry = paper_row)
        session.add(author_row)
        session.commit()

# Call initiate_database() when creating the database for the first time
def initiate_database():
    new_articles = arxiv.query(search_query, max_results=9999, 
                               sort_by="lastUpdatedDate", 
                               sort_order="descending")
    new_articles_df = pd.DataFrame.from_dict(new_articles)
    ordered_new_articles = new_articles_df.reindex(
                            columns=['title', 'author', 'authors', 'id', 'arxiv_comment',
                                     'arxiv_primary_category', 'published', 'summary',
                                     'tags', 'updated'])
    article_df = extract_column(ordered_new_articles)
    article_id_lst = article_df['unique_id'].values

    db_url = {'drivername': 'postgres',
              'username': 'postgres',
              'password': 'postgres',
              'host': '127.0.0.1',
              'port': 5432}
    engine = create_engine(URL(**db_url))
    
    # Initiate base
    Base = declarative_base()
    
    # Create tables
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
        published_date = Column(String)
        published_time = Column(String)
        updated_date = Column(String)
        updated_time = Column(String)
        
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
        row_df = article_df[article_df['unique_id']==id_string]
        insert_new_articles_initiation(session, PaperTable, AuthorTable, 
                                       id_string, row_df)
        count_insert += 1
        if count_insert % 1000 == 0:
            print("Inserted {} new articles.".format(count_insert))
    
    print("Completed: Inserted {} new articles.".format(count_insert))
    session.close()


def main():
    # Local db url
    db_url = {'drivername': 'postgres',
              'username': 'postgres',
              'password': 'postgres',
              'host': '127.0.0.1',
              'port': 5432}
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
    
    # Inspect table columns
    #inspect_paper = inspect(PaperTable)
    #col_list_p = [column.name for column in inspect_paper.c]
    #print(col_list_p)
    
    #inspect_author = inspect(AuthorTable)
    #col_list_a = [column.name for column in inspect_author.c]
    #print(col_list_a)
    
    Session = sessionmaker(bind=engine)
    session = Session()
    
    article_df = extract_column(obtain_new_articles())
    # Below for testing purpose only
    #article_df = extract_column(pd.read_json("/Users/EmilyWang/Desktop/ops/paper-collector/DeepLearningArticles.json", 
    #                                         orient='index'))
    article_id_lst = article_df['unique_id'].values
    
    count_update = 0
    count_insert = 0
    count_total = 0

    for id_string in article_id_lst:
        row_df = article_df[article_df['unique_id']==id_string]
        
        count_total += 1
        if count_total % 200 == 0:
            print("Checked {} out of 1,000 retrieved results".format(count_total))
        
        if check_existence(session, PaperTable, id_string):
            exist_version = session.query(PaperTable.c.version).\
                                    filter(PaperTable.c.id==id_string).one()
            if int(article_df.iloc[0, 1]) > exist_version[0]:
                update_existing_articles(session, 
                                         PaperTable, id_string, row_df)
                count_update += 1
                
        else:
            insert_new_articles(session, PaperTable, AuthorTable, 
                                id_string, row_df)
            count_insert += 1
    
    print("Completed: Inserted {} new articles, updated {} existing articles.".format(count_insert, count_update))
    session.close()
    connection.close()


# Un-comment initiate_database() and comment main() when setting up the database for the first time

# Troubleshoot: If the database could not update 9999 records at once, change max_result 
# from 1000 to 9999 in obtain_new_articles() to do the check/update/insert process instead
#initiate_database()
main()

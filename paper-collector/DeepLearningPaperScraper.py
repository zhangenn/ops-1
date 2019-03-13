
import arxiv
import pandas as pd

KEYWORD = '%28%22deep learning%22 OR %22neural network%22 OR %22GPU%22 OR %22graphics processing unit%22 OR %22reinforcement learning%22 OR %22perceptron%22%29'

CATEGORY = '%28cat:cs.LG OR cat:stat.ML OR cat:cs.AI OR cat:cs.CV%29'

QUERY = KEYWORD + " AND " + CATEGORY

def firstTime():
    # Scraping articles for the first time
    articles = arxiv.query(QUERY, max_results=9999, sort_by="lastUpdatedDate", sort_order="descending")

    articlesDF = pd.DataFrame.from_dict(articles)
    orderedArticles = articlesDF.reindex(
        columns=['title', 'author', 'authors', 'id', 'arxiv_comment', 
                 'arxiv_primary_category', 'published', 'summary', 
                 'tags', 'updated'])

    orderedArticles.to_json("DeepLearningArticles.json", orient='index')

def updateArticles():
    # Updating records
    orderedArticles = pd.read_json("DeepLearningArticles.json", orient='index')
    temp_id = orderedArticles['id'].tolist()
    unique_id = [item[-12:-2] for item in temp_id]
    version_no = [item[-1] for item in temp_id]

    article_dict = {}
    for i in range(len(temp_id)):
        article_dict[unique_id[i]] = version_no[i]

    # Getting new articles, change max_results
    new_articles = arxiv.query(QUERY, max_results=100, sort_by="lastUpdatedDate", sort_order="descending")
    new_articlesDF = pd.DataFrame.from_dict(new_articles)
    ordered_new_artiDF = new_articlesDF.reindex(
        columns=['title', 'author', 'authors', 'id', 'arxiv_comment', 
                 'arxiv_primary_category', 'published', 'summary', 
                 'tags', 'updated'])

    temp_id_2 = ordered_new_artiDF['id'].tolist()
    counter = 0

    for item in temp_id_2:
        arti_key = item[-12:-2]

        # Adding newly published articles
        if arti_key not in article_dict:
            article_dict[arti_key] = item[-1]
            print("Updated the dictionary.")
            orderedArticles.append(ordered_new_artiDF[ordered_new_artiDF.loc[ordered_new_artiDF['id'] == item]], 
                                   ignore_index=True)
            print("Added one new article.")
            counter += 1

        # Updating old versions
        else:
            if int(item[-1]) > int(article_dict[arti_key]):
                article_index = orderedArticles.loc[orderedArticles['id'] == item].index[0]
                orderedArticles.drop(labels=article_index, axis=0, inplace=True)
                print("Deleted one old article.")
                article_dict[arti_key] = item[-1]
                print("Updated the dictionary.")
                orderedArticles.append(ordered_new_artiDF[ordered_new_artiDF.loc[ordered_new_artiDF['id'] == item]], 
                                       ignore_index=True)
                print("Added one new article.")
                counter += 1

    print("Update completed: {} updates made.".format(counter))
    orderedArticles.to_json("DeepLearningArticles.json", orient='index')

updateArticles()

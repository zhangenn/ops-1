
import json
import arxiv
import pandas as pd

OUTPUT_FILE = "DeepLearningArticles.json"

# Keywords include deep learning, neural network, GPU, graphics processing unit,
# reinforcement learning, OR perceptron
keyword = '%28%22deep learning%22 OR %22neural network%22 \
            OR %22GPU%22 OR %22graphics processing unit%22 \
            OR %22reinforcement learning%22 OR %22perceptron%22%29'

# Searches within machine learning (stats or cs), artificial intelligence or computer vision
category = '%28cat:cs.LG OR cat:stat.ML OR cat:cs.AI OR cat:cs.CV%29'

search_query = keyword + " AND " + category


def article_id(article_dataframe):
    temp_id = article_dataframe['id'].tolist()
    unique_id = [item[-12:] for item in temp_id]
    return unique_id


def initial_articles():
    # Scraping articles for the first time
    articles = arxiv.query(search_query, max_results=9999,
                           sort_by="lastUpdatedDate", sort_order="descending")

    articles_df = pd.DataFrame.from_dict(articles)
    ordered_articles = articles_df.reindex(
        columns=['title', 'author', 'authors', 'id', 'arxiv_comment',
                 'arxiv_primary_category', 'published', 'summary',
                 'tags', 'updated'])

    ordered_articles.to_json("DeepLearningArticles.json", orient='index')


def prettify_json(path):
    with open(path, 'r') as json_file:
        data = json.load(json_file)
    with open(path, 'w') as outfile:
        json.dump(data, outfile, indent=2)


def update_articles():
    # Updating records
    ordered_articles = pd.read_json(
        "DeepLearningArticles.json", orient='index')
    unique_id = article_id(ordered_articles)
    version_no = [item[-1] for item in unique_id]

    # Dictionary for existing article {unique id: version number}
    article_dict = {}
    for i in range(len(unique_id)):
        article_dict[unique_id[i][:-2]] = version_no[i]

    # Getting new articles, change max_results
    new_articles = arxiv.query(search_query, max_results=100,
                               sort_by="lastUpdatedDate", sort_order="descending")
    new_articles_df = pd.DataFrame.from_dict(new_articles)
    ordered_new_articles = new_articles_df.reindex(
        columns=['title', 'author', 'authors', 'id', 'arxiv_comment',
                 'arxiv_primary_category', 'published', 'summary',
                 'tags', 'updated'])

    unique_id_new = article_id(ordered_new_articles)
    prefix = 'http://arxiv.org/abs/'
    counter = 0

    for item in unique_id_new:
        article_key = item[:-2]

        # Adding newly published articles (new articles appended at the front)
        if article_key not in article_dict:
            article_dict[article_key] = item[-1]
            ordered_articles = pd.concat(
                [ordered_new_articles[
                    lambda ordered_new_articles: ordered_new_articles['id'] == prefix+item],
                    ordered_articles],
                axis=0, sort=False, ignore_index=True)
            print("Added a new paper.")
            counter += 1

        # Updating old versions
        else:
            if int(item[-1]) > int(article_dict[article_key]):
                old_article_id = prefix + item[:-1] + article_dict[article_key]
                article_index = ordered_articles.index.get_loc(
                    ordered_articles.index[ordered_articles['id'] == old_article_id][0])
                ordered_articles.drop(
                    labels=article_index, axis=0, inplace=True)
                article_dict[article_key] = item[-1]
                ordered_articles = pd.concat(
                    [ordered_new_articles[
                        lambda ordered_new_articles: ordered_new_articles['id'] == prefix+item],
                        ordered_articles],
                    axis=0, sort=False, ignore_index=True)
                print("Updated a newer version.")
                counter += 1

    ordered_articles.to_json(OUTPUT_FILE, orient='index')
    prettify_json(OUTPUT_FILE)
    print("Update completed: {} updates made.".format(counter))


update_articles()

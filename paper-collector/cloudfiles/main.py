import pandas as pd
import arxiv

def hello_world():
    article = arxiv.query('cat:cs.LG', max_results=3,
                               sort_by="lastUpdatedDate",
                               sort_order="descending")
    article_df = pd.DataFrame.from_dict(article)
    return f'df len: {len(article_df)}'


if __name__ == "__main__":
    r = hello_world()
    print(r)

import arxiv

QUERY = 'cat:cs.LG AND co:%28ICLR%29'

articles = arxiv.query(QUERY, max_results=9999)

print(len(articles))

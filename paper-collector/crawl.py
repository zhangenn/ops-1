import arxiv

QUERY = 'cat:cs.LG AND cat:cs.CV'

articles = arxiv.query(QUERY, max_results=9999)

print(len(articles))

import pymongo
from pymongo import MongoClient
from collections import Counter

# ---------- Fixed Params ------------
art_client = MongoClient('mongodb://10.237.26.25:27017/')
client = MongoClient('mongodb://10.237.26.154:27017/')
art_db = art_client['media-db']
my_db = client['media-db']
# -----------------------------------

print("Finding articles...")
# Enter the base set of keywords in the regex below, separated by |
x = art_db.articles.find({'$and':[{'text': {'$regex': 'e-governance|information and communication technology|e-govt|e-government|electronic governance|paperless office|communication technology|ict academy|ict sector|ict information|ict tool|e-district|m-governance ', '$options': 'i'}},{'categories':{'$exists':'True'}}]},
                     no_cursor_timeout=True)
                     
print(x)

articles = input('Enter name of collection to store resultant articles: ')
coll = my_db[articles]

print("Storing articles...")
art_map = {}

for art in x:
	url=art['articleUrl']
	lis =[]
	lis=art['categories']
	
	if url not in art_map and 'OPINION' in lis:
		art_map[url]=1
		coll.insert_one(art)

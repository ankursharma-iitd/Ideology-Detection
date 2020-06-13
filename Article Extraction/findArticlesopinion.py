import pymongo
from pymongo import MongoClient
from collections import Counter

# ---------- Fixed Params ------------
art_client = MongoClient('mongodb://act4dgem.cse.iitd.ac.in:27017/')
client = MongoClient('mongodb://act4dgem.cse.iitd.ac.in:27017/')
art_db = art_client['media-db']
my_db = client['eventwise_media-db']
# -----------------------------------

print("Finding articles...")

x = art_db.articles.find({},no_cursor_timeout=True)
                     
articles = input('Enter name of collection to store resultant articles: ')

print("Storing articles now...")
coll = my_db[articles]
art_map = {}
count = 0

for art in x:
	count+=1
	print('Doing for '+str(count)+' relevant article')
	url = art['articleUrl']
	try:
		cat = str(art['category'])
		print('category attribute found')
		if url not in art_map and cat=='OPINION':
			art_map[url] = 1
			coll.insert_one(art)
	except KeyError as err1:
		print('category attribute not found')
		
	try:
		lis = art['categories']
		print('categories attribute found')
		if url not in art_map and 'OPINION' in lis:
			art_map[url] = 1
			coll.insert_one(art)
	except KeyError as err2:
		print('categories attribute not found')
		

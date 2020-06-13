import pymongo
from pymongo import MongoClient
from collections import Counter

# ---------- Fixed Params ------------
art_client = MongoClient('mongodb://10.237.27.104:27017/')
client = MongoClient('mongodb://10.237.26.154:27017/')
art_db = art_client['media-db']
my_db = client['media-db']
# -----------------------------------

print("Finding articles...")
# Enter the base set of keywords in the regex below, separated by |
x = art_db.articles.find({'$and':[{'text': {'$regex': ' Digital India|digital india|Digital Swades|digital swades|India Digital|india digital|Digit India|digit india|Digital Desh|digital desh|make india|digital divide|digital payment|free wifi service|digital locker|digital transaction|wifi hotspot|budget cybersecurity|skill india|internet connectivity|smart city|digital business|bharatnet project|digital present|Bharat net|digitalised|digitalized ', '$options': 'i'}},{'categories':{'$exists':True}}]},
                     no_cursor_timeout=True)
                     
print(x)

articles_digitalIndia = input('Enter name of collection to store resultant articles: ')
coll = my_db[articles_digitalIndia]

print("Storing articles...")

art_map = {}

for art in x:
	url=art['articleUrl']
	
	if url not in art_map:
		art_map[url]=1
		coll.insert_one(art)
		

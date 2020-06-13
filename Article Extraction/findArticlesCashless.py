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
x = art_db.articles.find({'$and':[{'text': {'$regex': '  cashless | digital payment| mobikwik | Unified Payment interface | UPI | online transfer| SBI pay | ICICI pocket | Payzapp |freecharge |  e-wallet| mobile wallet| internet banking | net banking | mobile banking | PhonePe | physical-POS | M-POS | V-POS | digital transaction| pos machine| swipe machine| digital wallet| digital economy | card payment| BHIM | banking transaction| swiping machine| payment gateway ', '$options': 'i'}},{'categories':{'$exists':True}}]},
                     no_cursor_timeout=True)
                     
print(x)

articles_digitalIndia = input('Enter name of collection to store resultant articles: ')
coll = my_db[articles_digitalIndia]

print("Storing articles...")

art_map = {}

for art in x:
	url=art['articleUrl']
	lis =[]
	lis=art['categories']
	
	if url not in art_map and 'OPINION' in lis:
		art_map[url]=1
		coll.insert_one(art)
		

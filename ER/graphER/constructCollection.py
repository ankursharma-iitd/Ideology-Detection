import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId
from collections import Counter
import sys

client = MongoClient('mongodb://10.237.26.159:27017/')
db = client['eventwise_media-db']
x=db['all_media_entities_unresolved'].find({'_id':ObjectId(sys.argv[1])},no_cursor_timeout=True)
#x=db['all_media_entities_unresolved'].find({"$and":[{"stdName": {'$regex':'modi','$options':'i'}},{'graphid':{'$ne':None}}]},no_cursor_timeout=True)

print("Finding articles...")
# x=db.articles.find({'text':{'$regex':' Aadhar | UIDAI | aadhar | AADHAR| adharcard | adhar | Adhar | ADHAR '}},no_cursor_timeout=True).limit(50)
tech_articles = db['all_media_entities_unresolved_test2']
#adhar_articles = db['adhar_articles']
print("Storing articles...")
#tech_articles.insert(x)

#fout = open("tech_articles.txt", 'w')
for art in x:
    tech_articles.insert_one(art)
    #print(art['stdName']),
    #print(art['graphid'])
    #fout.write(art['text'])
    #fout.write("\n")
#fout.close())))

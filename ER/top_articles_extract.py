from pymongo import MongoClient

#client = MongoClient(config.mongoConfigs['host'], config.mongoConfigs['port'], j=True)
#db = client[config.mongoConfigs['db']] 

client = MongoClient('mongodb://10.237.26.159:27017/')
db = client['media-db']

if __name__=='__main__':
    collection = db['articles']
    new_coll = db['2L_articles']
    cursor=collection.find({}).limit(200000)
    print(cursor)
    new_coll.insert(cursor)

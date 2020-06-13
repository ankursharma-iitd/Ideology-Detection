from pymongo import MongoClient


if __name__=='__main__':
    import datetime
    today = datetime.date.today()
    print("Started on ")
    print today
    client = MongoClient('mongodb://10.237.26.159:27017/')
    db = client['media-db'] 
    collection = db['articles']
    cursor=collection.find({'$and':[{'extracted':{'$exists':False}}]})#.limit(10)
    print(cursor.count())
    for c in cursor:
        print(c)
        break

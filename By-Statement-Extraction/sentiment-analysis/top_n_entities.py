from pymongo import MongoClient
import sys
sys.path.insert(0,"../")
import config

def get_top_n_entities(collection, n, types=None):
    pipeline = [{"$project":{"stdName":1,"type":1,"aliases":1,"articleIds":1,"num":{"$size":"$articleIds"}}}]
    cursor = list(collection.aggregate(pipeline))
    top_n_entities = {}
    if types:
        entities = {type:[] for type in types}
        for ent in cursor:
            if(ent['type'] in types):
                entities[ent['type']].append(ent)
        
        for type in entities.keys():
            entities[type].sort(key=lambda x: x['num'], reverse=True)
            top_n_entities[type] = [{"name":obj['stdName'],"coverage":obj['num'],"aliases":obj['aliases'],"articleIds":obj['articleIds']} for obj in entities[type][:n]]
    else:
        cursor.sort(key=lambda x: x['num'], reverse=True)
        top_n_entities["all"] = [{"name":obj['stdName'],"coverage":obj['num'],"aliases":obj['aliases'],"articleIds":obj['articleIds']} for obj in cursor[:n]]
    return top_n_entities
    
def main():
  if __name__=='__main__':
    #number of top entities needed per type
    N = 30
    # types = ['Person', 'Company', 'Organization', 'Country', 'City', 'Continent', 'ProvinceOrState']
    types = ['Person']
    
    client = MongoClient(config.mongoConfigs['host'],config.mongoConfigs['port'])
    db = client[config.mongoConfigs['db']] 
    collection=db['farmers_opinion_resolved']

    entities = get_top_n_entities(collection, N, types)
    
    for type in entities.keys():
        print(type+":")
        for ent in entities[type]:
            print(ent['name']+ " - "+ str(ent['coverage']))
        print('')

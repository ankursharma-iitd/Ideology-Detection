from pymongo import MongoClient
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import pprint
from  bson import objectid
import re
from metaphone import doublemetaphone
from collections import defaultdict
from getHighTFIDFAssocs import *
from timeit import default_timer as timer
import editdistance
import jellyfish
import traceback
from bson.objectid import ObjectId

#es = Elasticsearch(timeout=30)
pp = pprint.PrettyPrinter(indent=4)

es = Elasticsearch('act4dgem.cse.iitd.ac.in', port=9200, timeout=None)

client = MongoClient('mongodb://act4dgem.cse.iitd.ac.in:27017/')
db = client['eventwise_media-db'] 
es_index='media-graph-final'
es_mapping='entities_resolved_esmapping'
# mongo_coll='all_media_entities_resolved_test2'
mongo_coll='all_media_entities_resolved_final4'

f2=open('person.txt','a')

def getAliasesObj(entity):
    aliases = ''
    if entity.get('aliases'):
        for t in entity['aliases']:
            t=re.sub('pvt|ltd|inc|corp|private|public|industry|industries|corporation|enterprise|company|limited','',t,flags=re.IGNORECASE)
            aliases += (' ' + t.lower())
    
    return {
             "match":{
                        "aliases": {
                          "query": aliases if entity.get('aliases') else ' ',
                          "fuzziness": "AUTO"
                          # "boost":2
                        }
                }
    } 
    
def mergeAssociatedEntities(assEntities1, assEntities2):
    asscEn=assEntities1[:]
    if assEntities1==[{}]:
        if assEntities2!=[{}]:
            asscEn=assEntities2
            return asscEn
        else:
            return [{}]
    elif assEntities2==[{}]:
        if assEntities1!=[{}]:
            asscEn=assEntities1
            return asscEn
        else:
            return [{}]
    else:
        for a2 in assEntities2:
            notFound = True
            for a1 in asscEn:
                if jellyfish.jaro_winkler(a2['text'].strip().lower(),a1['text'].strip().lower())>=0.8:
                    a1['count'] = int(a1['count']) + 1
                    notFound = False
                    break
            if (notFound):
                asscEn.append(a2)
        asscEn = sorted(asscEn, key=lambda x:int(x['count']), reverse=True)
        asscEn = asscEn[:min(20, len(asscEn))]
        return asscEn

def getTitleObj(entity):
    if entity.get('title'):
        titleArr = []
        titles = ''
        for t in entity['title']:
            if t['text'] and not t['text'] in titleArr:
                titleArr.append(t['text'])
                titles = titles + ' ' + t['text']
        
        titles = titles.replace(entity['stdName'], '')
    return  {
        "match": {
                    "title.text": {
                      "query":titles if entity.get('title') else ' ',
                      # "boost": 1.2,
                      "fuzziness": "AUTO"
                    }
                  }
    }
    
def getAssociatedEntities(entity):
    if entity.get('associatedEntities'):
        asscEntities = []
        asscEntText = ''

        for a in  entity['associatedEntities']:
            #print str(type(a))
            
            try:
                for i in a:
                    #print str(type(i)) + i
                    if i['text'] and (not i['text'] in asscEntities):
                        asscEntities.append(i['text'])
                        asscEntText += (' ' + i['text'])
            except:
                if a['text'] and (not a['text'] in asscEntities):
                    asscEntities.append(a['text'])
                    asscEntText += (' ' + a['text'].lower())
    
    return  {
          "match": {
            "associatedEntities.text": {
              "query":asscEntText  if entity.get('associatedEntities') else "abc",
              # "boost": 1.2,
              "fuzziness": "AUTO"
            }
          }
        }
                                 
def containsTitle(array, title):
    for t in array:
        if t['text'] == title['text']:
            return True 
    return False

def containsContext(array, context):
    for c in array:
        if c['text'] == context['text']:
            return True
    return False

def matchExact(name1, name2):
    name1 = re.sub('\W+',' ',name1.lower())
    name2 = re.sub('\W+',' ',name2.lower())
    name1=name1.strip()
    name2=name2.strip()
    
    if name1 == name2:
        return True
    
    splitNames1 = name1.split()
    splitNames2 = name2.split()
    
    if len(splitNames1) != len(splitNames2):
        return False
    
    for s1 in splitNames1:
        mf = False
        for s2 in splitNames2:
            if s1 == s2:
                mf = True
                break
        if not mf:
            return False
    return True

def match(name1, name2):
    
    name1 = re.sub('\W+',' ',name1.lower())
    name2 = re.sub('\W+',' ',name2.lower())
    #high and low jaro
    j1=' '.join(filter(lambda x:len(x)>1,name1.split()))
    j2=' '.join(filter(lambda x:len(x)>1,name2.split()))
    
    if not (j1.strip() and j2.strip()):
        return False
    
    score=jellyfish.jaro_winkler(j1, j2)
    # print("jaro score %s, %s   %f"%(j1,j2,score))
    if score>=0.9:
        return True
    if score<=0.5:
        return False
    
    splitNames1 = name1.split()
    splitNames2 = name2.split()
    arr1 = splitNames1[:]
    arr2 = splitNames2[:]
    
    # direct match
    for s1 in splitNames1:
        s1 = s1.strip()
        for s2 in splitNames2:
            s2 = s2.strip()
            
            if len(s1) == 1 and len(s2) == 1:
                if s1 == s2:
                    arr1.remove(s1)
                    arr2.remove(s2)
                    splitNames2.remove(s2)
                    break
            elif len(s1) > 1 and len(s2) > 1:
                nameSound = doublemetaphone(s1)
                #print s1,' : ',s2,' : ' ,str(jellyfish.jaro_distance(s1, s2))
                if s1 == s2 or (nameSound == doublemetaphone(s2)) or (s1[0] == s2[0] and ((len(s1) <= 6 and editdistance.eval(s1, s2) == 1) or (len(s1) > 6 and editdistance.eval(s1, s2) == 2))):
                    arr1.remove(s1)
                    arr2.remove(s2)   
                    splitNames2.remove(s2)
                    break                                          
                
    tempArr1 = arr1[:]
    tempArr2 = arr2[:]
    # print("arr1, arr2")
    # print(arr1,arr2)
    for s1 in arr1:
        s1 = s1.strip()
        
        for s2 in arr2:
            s2 = s2.strip()
            
            if len(s1) == 1 and len(s2) > 1 and s1[0] == s2[0]:
                tempArr1.remove(s1)
                tempArr2.remove(s2)
                arr2.remove(s2)
                break
            
            elif len(s1) > 1 and len(s2) == 1 and s1[0] == s2[0]:
                tempArr1.remove(s1)
                tempArr2.remove(s2)
                arr2.remove(s2)
                break
    # print("Processing k baad")
    # print(tempArr1, tempArr2)
    if len(tempArr1) or len(tempArr2):
        return False
    
    return True        

def namesMatched(nameArr1, nameArr2):
    # remove initials
    maxMatches = len(nameArr1)*len(nameArr2)
    totalMatches = 0
    for n1 in nameArr1:
        for n2 in nameArr2:
            if match(n1, n2):
                totalMatches += 1
    # print("Total matches: %d"%totalMatches)
    if totalMatches>2*maxMatches/3:
        return True
    else:
        return False

def namesExactlyMatched(nameArr1, nameArr2):
    for n1 in nameArr1:
        for n2 in nameArr2:
            if not matchExact(n1, n2):
                return False
    return True

def  orgMatched(nameArr1, nameArr2):
    # match exactly, abbr considering stop words, abbr without stop words, part of a name
    for n1 in nameArr1:
        #n1=re.sub('\W+',' ',n1)
        n1=re.sub('pvt|ltd|inc|corp|private|public|industry|industries|corporation|enterprise|company|limited','',n1,flags=re.IGNORECASE)
        n1L = n1.lower().replace("-"," ")
        
        for n2 in nameArr2:
            #n2 = re.sub('\W+',' ',n2)
            n2=re.sub('pvt|ltd|inc|corp|private|public|industry|industries|corporation|enterprise|company|limited','',n2,flags=re.IGNORECASE)
            n2L=n2.lower().replace("-"," ")
            
            
            if len(n1.split())>1 and len(n2.split())>1: # assuming that names of len 1 are abbr
                # print("jaro distance",jellyfish.jaro_winkler(n1L, n2L)) 
                # print(n1,n2)
                # print()
                if jellyfish.jaro_winkler(n1L, n2L)>=0.9:
                    return True
                    # continue
                elif (n1L in n2L) or (n2L in n1L):
                    continue

            if isAbbrOf(n1, n2):
                return True
                #continue
            else:
                return False
    
    return True

def matchesCompanyEntity(entity, matchedEntity):
    
    if entity.get('resolutions') and matchedEntity.get('resolutions'):
        if entity['resolutions'][0]['name'].strip() == matchedEntity['resolutions'][0]['name'].strip():
            return True
   
    return orgMatched(entity['aliases'], matchedEntity['aliases'])

def getPersonRequest(entity):
    return {
             "query":{
                    "bool":{
                        "filter":{
                            "bool":{ 
                                "must":[
                                        {"match":{
                                                    "type": {
                                                      "query": entity['type'],
                                                      "fuzziness": "AUTO"
                                                    }
                                            }
                                       }
                                     ]
                            }
                        },
                        "must": getAliasesObj(entity),
                        "should": getAssociatedEntities(entity)
                    }
                }
    }

def getCompanyRequest(entity):
    return {
             "query":{
                    "bool":{
                        "filter":{
                            "bool":{ 
                                "must":
                                        {"match":{
                                                    "type": {
                                                      "query": "Company Organization",
                                                      "fuzziness": "AUTO"
                                                    }
                                            }
                                       }
                                        
                            }
                        },
                        "must": getAliasesObj(entity)
                                  
                    }
                }
    }

def getChangedProperties(entity, matchedEntity):
    global id
    changedProperties = {}
    #print entity
    #print matchedEntity
    
    articleIds = defaultdict(bool)
    for artId in entity['articleIds']:
        articleIds[artId] = True
    for artId in matchedEntity['articleIds']:
        articleIds[artId] = True
    changedProperties['articleIds'] = articleIds.keys()
    changedProperties['media_ids']=[]
    
    if entity.get('media_ids'):
        changedProperties['media_ids'].extend(entity['media_ids'])
    if matchedEntity.get('media_ids'):
        changedProperties['media_ids'].extend(matchedEntity['media_ids'])

    if matchedEntity.get('graphid', None)!=None:
        changedProperties['stdName'] = matchedEntity['stdName']
        changedProperties['media_ids'].append(entity['_id'])

    elif entity.get('graphid', None)!=None:
        changedProperties['stdName'] = entity['stdName']
        changedProperties['media_ids'].append(matchedEntity['_id'])
    else:
        if len(matchedEntity['stdName'].strip()) > len(entity['stdName'].strip()):
            changedProperties['stdName'] = matchedEntity['stdName']
            changedProperties['media_ids'].append(matchedEntity['_id'])
        else:
            changedProperties['stdName'] =entity['stdName']
            changedProperties['media_ids'].append(matchedEntity['_id'])
    
    aliases = defaultdict(bool)
    for alias in entity['aliases']:
        aliases[alias] = True
    for alias in matchedEntity['aliases']:
        aliases[alias] = True
    changedProperties['aliases'] = aliases.keys()
    
    asscEn = mergeAssociatedEntities(entity['associatedEntities'], matchedEntity['associatedEntities'])       
    changedProperties['associatedEntities'] = asscEn
   
    if matchedEntity.get('title'):
        titles = matchedEntity['title'][:]
    else:
        titles = []
    
    if entity.get('title'):    
        for t in entity['title']:
            matchingTextFound = False
            
            for t1 in titles:
                if (t['text'] in t1['text']) or (t1['text'] in t['text']) or jellyfish.jaro_distance(t['text'].lower(), t1['text'].lower())>=0.8:
                    for articleId in t['articleIds']:
                        if articleId not in t1['articleIds']:
                            t1['articleIds'].append(articleId)
                    matchingTextFound = True
                    break
            if not matchingTextFound:
                titles.append(t)
            
    changedProperties['title'] = titles           
    
    global f2
    line=''
    
    if entity.get('graphid', None) !=None:
        graphid=entity['graphid']
        print "****************************************MEDIA GRAPH RESOLVED******************************"
        print entity['stdName'], matchedEntity['stdName']
        line=str(entity['graphid']) + ' ; ' + entity['stdName'].replace(';','')+ ' ; ' + str(matchedEntity['_id']) + ' ; ' + matchedEntity['stdName'].replace(';','')
        print(line)
        #print '\n'
        f2.writelines(line)
        f2.write('\n')

    elif matchedEntity.get('graphid', None)!=None:
        graphid=matchedEntity['graphid']
        print "****************************************MEDIA GRAPH RESOLVED******************************"
        print entity['stdName'], matchedEntity['stdName']
        line=str(matchedEntity['graphid']) + ' ; ' + matchedEntity['stdName'].replace(';','')+ ' ; ' + str(entity['_id']) + ' ; ' + entity['stdName'].replace(';','')
        print (line)
        #print '\n'
            
        f2.writelines(line)
        f2.write('\n')
    else:
        print "****************************************MEDIA GRAPH UNRESOLVED******************************"          
        graphid=None

    changedProperties['graphid']=graphid
    return changedProperties

def getChangedCompProp(entity, matchedEntity):
    global id
    changedProperties = {}
    
    articleIds = defaultdict(bool)
    for artId in entity['articleIds']:
        articleIds[artId] = True
    for artId in matchedEntity['articleIds']:
        articleIds[artId] = True
    changedProperties['articleIds'] = articleIds.keys()
    changedProperties['media_ids']=[]
    
    if entity.get('media_ids'):
        changedProperties['media_ids'].extend(entity['media_ids'])
    if matchedEntity.get('media_ids'):
        changedProperties['media_ids'].extend(matchedEntity['media_ids'])

    if matchedEntity.get('graphid', None)!=None:
        changedProperties['stdName'] = matchedEntity['stdName']
        changedProperties['media_ids'].append(entity['_id'])

    elif entity.get('graphid', None)!=None:
        changedProperties['stdName'] = entity['stdName']
        changedProperties['media_ids'].append(matchedEntity['_id'])
    else:
        if len(matchedEntity['stdName'].strip()) > len(entity['stdName'].strip()):
            changedProperties['stdName'] = matchedEntity['stdName']
            changedProperties['media_ids'].append(matchedEntity['_id'])
        else:
            changedProperties['stdName'] =entity['stdName']
            changedProperties['media_ids'].append(matchedEntity['_id'])
    
    aliases = defaultdict(bool)
    for alias in entity['aliases']:
        aliases[alias] = True
    for alias in matchedEntity['aliases']:
        aliases[alias] = True
    changedProperties['aliases'] = aliases.keys()

    if entity.get('resolutions') and entity['resolutions'] !=None:
        changedProperties['resolutions'] = entity['resolutions']
    elif matchedEntity.get('resolutions') and matchedEntity['resolutions'] !=None:
        changedProperties['resolutions'] = matchedEntity.get('resolutions')
    else:
        changedProperties['resolutions']=None

    #since media-db comp have no entities, we simply append the graph ones
    changedProperties['associatedEntities']=[]
    if entity.get('associatedEntities') and matchedEntity.get('associatedEntities'):
        changedProperties['associatedEntities'].extend(entity['associatedEntities'])  
        changedProperties['associatedEntities'].extend(matchedEntity['associatedEntities'])
    elif entity.get('associatedEntities'):
        changedProperties['associatedEntities'].extend(entity['associatedEntities'])
    elif matchedEntity.get('associatedEntities'):
        changedProperties['associatedEntities'].extend(matchedEntity['associatedEntities'])
    else:
        changedProperties['associatedEntities']=[]
    global f2
    line=''
    
    if entity.get('graphid',None) !=None:
        graphid=entity['graphid']
        print "****************************************MEDIADB RESOLVED******************************"
        print entity['stdName'], matchedEntity['stdName']
        line=str(entity['graphid']) + ' ; ' + entity['stdName'].replace(';','')+ ' ; ' + str(matchedEntity['_id']) + ' ; ' + matchedEntity['stdName'].replace(';','')
        print (line)
        f2.writelines(line)
        f2.write('\n')

    elif matchedEntity.get('graphid', None)!=None:
        graphid=matchedEntity['graphid']
        print "****************************************MEDIADB RESOLVED******************************"
        print entity['stdName'], matchedEntity['stdName']
        line=str(matchedEntity['graphid']) + ' ; ' + matchedEntity['stdName'].replace(';','')+ ' ; ' + str(entity['_id']) + ' ; ' + entity['stdName'].replace(';','')
        print (line)
        f2.writelines(line)
        f2.write('\n')

    else:
        print "****************************************MEDIA GRAPH UNRESOLVED******************************"          
        graphid=None

    changedProperties['graphid']=graphid
    #print changedProperties
    return changedProperties

def serializeObjectId(entity):
    if entity.get("_id"):
        entity["_id"] = str(entity["_id"])
    if entity.get("media_ids"):
        entity["media_ids"] = str(entity["media_ids"])
    
    if entity.get('articleIds'):
        ids = [str(id) for id in entity['articleIds']]
        entity['articleIds'] = ids
    
    if entity.get('title'):
        for t in entity['title']:
            ids = [str(id) for id in t['articleIds']]     
            t['articleIds'] = ids
            
    if entity.get('context'):
        for c in entity['context']:
            c['articleId'] = str(c['articleId'])
     
def getRelevantEntities(esQuery):
    global mongo_coll
    global es_index
    global es_mapping
    
    res = es.search(index=es_index, body=esQuery, doc_type=es_mapping)
    entityIds=[]
    scores=[]
    entities=[]

    for i in range(len(res['hits']['hits'])):
        en=res['hits']['hits'][i]
        entityIds.append(objectid.ObjectId(en['_id']))
        scores.append(en['_score'])

    if len(entityIds):   
        #print entityIds
        cursor=db[mongo_coll].find({'_id':{'$in':entityIds}})
        
        for relEn in cursor:
            entityIdIndex=entityIds.index(relEn['_id'])
            score=scores[entityIdIndex]
            relEn['score']=score
            entities.append(relEn)
            
        cursor.close()
    
    return entities

def insertEntity(entity):
    db[mongo_coll].insert_one(entity)
    insertedId = entity['_id']
    del entity['_id']
    serializeObjectId(entity)
    #print entity
    es.create(index=es_index, doc_type=es_mapping, body=entity, id=str(insertedId))

def updateDelEn(matchedEntities,changedProperties):
    db[mongo_coll].update_one({'_id':matchedEntities[0]['_id']}, {"$set":changedProperties})
    if len(matchedEntities)>1:
        db[mongo_coll].delete_many({'_id':{'$in':[i['_id'] for i in matchedEntities[1:]]}})
   
    serializeObjectId(changedProperties)
    actions=[
                   {
                       '_op_type': 'update',
                       '_index': es_index,
                       '_type': es_mapping,
                       '_id': str(matchedEntities[0]['_id']),
                       'doc': changedProperties
                   }
            ]

    if len(matchedEntities)>1:
        actions.extend([ {
                            '_op_type':'delete',
                            '_index': es_index,
                            '_type': es_mapping,
                            '_id': str(i['_id']),
                        } for i in matchedEntities[1:]
                    ])

    helpers.bulk(es, actions)



def match_title_assoc_entity(relEn,changedProperties,relEn_assoc,cp_assoc):
    print("match title assoc entity started")
    calculate_time_elapsed()
    # r=titleMatched_v(relEn['title'],changedProperties['title'])
    # if r:
        # return True
    # else:
    
    x = fuzzySubset(relEn_assoc,cp_assoc)
    print("Time taken in fuzzy subset: %.2fs\n"%(calculate_time_elapsed()))
    return x

def resolvePersonEntity(entity,ent_to_hightfidf):
    global es_index, es_mapping, mongo_coll

    if entity.get('graphid', None)==None:
        print ("media entity - inserting")
        insertEntity(entity)
        return
    
    if not entity.get('aliases'):
        entity['aliases']=[entity['stdName'].lower()]

    request = getPersonRequest(entity)
    #print "****REQUEST****"
    #print request
    relEntities=getRelevantEntities(request)
    index = 0
    changedProperties=entity
    matchedEntities=[]
    
    
    for relEn in relEntities:
        
        relEn_assoc={}
        cp_assoc={}
        try:
            relEn_assoc=ent_to_hightfidf[relEn['stdName']]
            cp_assoc=ent_to_hightfidf[changedProperties['stdName']]
        except:
            print("########################################################3")
            print('key error')
            print ('relEn[stdName]:',relEn['stdName'])
            print (relEn_assoc)
            print ('changedProperties[stdName]: ',changedProperties['stdName'])
            print (cp_assoc)
            print ("#######################################################")
        '''
        for dict1 in relEn['associatedEntities']:
            relEn_assoc.append(dict1['text'])
        for dict1 in changedProperties['associatedEntities']:
            cp_assoc.append(dict1['text'])
        '''
        relEn_assoc=set(relEn_assoc)
        cp_assoc=set(cp_assoc)
        
        try:
            if entity.get('graphid', None) != None and relEn.get('graphid', None) != None:
                #handle a case when there are two graph ids for one media id, in that case both of them must have the media id
                print("not done")
                continue
            # if entity.get('graphid', None)==None and relEn.get('graphid', None)==None:
                # continue
            
            if (namesMatched(relEn['aliases'], changedProperties['aliases'])) or namesExactlyMatched(relEn['aliases'], changedProperties['aliases']):
                if match_title_assoc_entity(relEn, changedProperties, relEn_assoc, cp_assoc):
                    index = 1
                    changedProperties = getChangedProperties(changedProperties, relEn)
                    matchedEntities.append(relEn)
                    # print("matched Ent length: ",len(matchedEntities))
        except:
            continue
    
    if index == 0:
        print ("no match")
        insertEntity(entity)
    else:
        print ("match found person")
        for en in matchedEntities:
            print(en['stdName'], changedProperties['stdName'])
        updateDelEn(matchedEntities,changedProperties)


def resolveCompanyEntity(entity):
    global es_index, es_mapping, mongo_coll
    
    if entity.get('graphid', None)==None:
        print ("media entity - inserting")
        insertEntity(entity)
        return
    
    if not entity.get('aliases'):
        entity['aliases']=[entity['stdName'].lower()]

    request = getCompanyRequest(entity)
    
    relEntities=getRelevantEntities(request)
    
    index = 0
    changedProperties=entity
    matchedEntities=[]
    
    
    
    isGraphEn = entity.get('graphid', None)!=None
    print("relEn len", len(relEntities))
    for relEn in relEntities:
        if (isGraphEn and relEn.get('graphid', None)!=None) or (not isGraphEn and relEn.get('graphid', None)==None):
            print("not done")
            continue
        if matchesCompanyEntity(changedProperties, relEn):
            index=1
            changedProperties = getChangedCompProp(changedProperties, relEn)
            matchedEntities.append(relEn)
    
    if index==0:
        print ("no match")
        for en in matchedEntities:
            print(en['stdName'], changedProperties['stdName'])
        insertEntity(entity)
    else:
        print ("match found")
        updateDelEn(matchedEntities, changedProperties)


def isAbbrOf(name1, name2):
    #name 1 and name2 are abbr
    n1=re.sub('\.','',name1).lower()
    n2=re.sub('\.','',name2).lower()
    if n1==n2:
        return True

    #name1 is abbr of name2
    n2 = re.sub('\.', ' ', name2)
    n2Arr = n2.split()
    n2WithStopWords = ''.join([x[0] for x in n2Arr]).lower()
    
    if n1 == n2WithStopWords:
        return True
    
    n2 = re.sub('of|the|and|at|for|a|an|under|with|from|into|to|in|on|by|about', ' ', name2)
    n2Arr = n2.split()
    n2WithoutStopWords = ''.join([x[0] for x in n2Arr]).lower()
    
    if n1 == n2WithoutStopWords:
        return True


    #name2 is abbr of name1
    n1 = re.sub('\.', '', name2).lower()
    
    n2 = re.sub('\.', ' ', name1)
    n2Arr = n2.split()
    n2WithStopWords = ''.join([x[0] for x in n2Arr]).lower()
    
    if n1 == n2WithStopWords:
        return True
    
    n2 = re.sub('of|the|and|at|for|a|an|under|with|from|into|to|in|on|by|about', ' ', name1)
    n2Arr = n2.split()
    n2WithoutStopWords = ''.join([x[0] for x in n2Arr]).lower()
    
    if n1 == n2WithoutStopWords:
        return True
    
    return False

'''
Function that takes a collection and returns entity to top 50%
highest tfidf associated entities' mapping.
'''
def getHighTFIDFDicts(cursor):
    
    count=0
    ent_to_assoc=defaultdict(list)
    assoc_to_ent=defaultdict(list)
    for item in cursor:
        count+=1
        entid=item['_id']
        #entname=item['stdName']+str(count) why appending str(count) here
        entname = item['stdName']
        if(count%100000==0):
            print("ent_to_assoc", count)
            print("Time taken: %.2fs\n"%(calculate_time_elapsed()))
        try:
            assoc_list=item['associatedEntities']
            #Create the associated entity->[(ent1,2), (ent2,5)] mapping
            #Also create the entity->[(assoc1,2),(assoc2,5)] mapping
            for diction in assoc_list:
                assoc=diction['text']
                cnt=diction['count']
                assoc_to_ent[assoc].append((entname,cnt))
                ent_to_assoc[entname].append((assoc,cnt))
        except:
            continue
    print("Entities to Assoc entities mapping done")
    ent_to_tfidf=createTFIDFDict(ent_to_assoc,assoc_to_ent)
    print ("len ent_to_tfidf: ",len(ent_to_tfidf))
    # print ent_to_tfidf
    ent_to_hightfidf=keepHighTFIDFAssocs(ent_to_tfidf)
    print ("len ent_to_hightfidf: ", len(ent_to_hightfidf))
    # print(ent_to_tfidf)
    # print ent_to_hightfidf
    return ent_to_hightfidf

    
if __name__ == '__main__':
    collection = db['all_media_entities_unresolved']
    # collection = db['all_media_entities_unresolved_test2']
    
    cursor = collection.find({}, no_cursor_timeout=True)
    # ent_to_hightfidf=getHighTFIDFDicts(cursor)
    ent_to_hightfidf={}
    
    # while(1):
    #cursor = collection.find({'$and':[{'type':{'$in':['Person', 'Company', 'Organization','Party','City','BusinessBody']}},{'resolved':True}]}, no_cursor_timeout=True)
    # cursor = collection.find({}, no_cursor_timeout=True)
    cursor = collection.find({"$and":[{"graphid":{"$exists":True}},{"$or":[{"type":"Company"},{"type":"Organization"}]}]}, no_cursor_timeout=True)
    # cursor = collection.find({"$and":[{"$or":[{"type":"Company"},{"type":"Organization"}]}]}, no_cursor_timeout=True)
    count = 1
    # import time
    for entity in cursor:
        """
        id=entity['_id']
        if entity['type'] == 'Person':
            resolvePersonEntity(entity, ent_to_hightfidf)
        elif(entity['type'] == 'Company' or entity['type'] == 'Organization'):
            resolveCompanyEntity(entity)
        else:
            # resolveOtherEntity(entity, ent_to_hightfidf)
            print("Invalid entity type")
            continue
        """
        #print(entity)
        resolveCompanyEntity(entity)
        print (count)
        count += 1
        # collection.update_one({'_id':id}, {'$set':{'resolved':True}})
        # time.sleep(1)
        
    cursor.close()
        #break
    client.close()     
            

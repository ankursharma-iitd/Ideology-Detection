from pymongo import MongoClient
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import pprint
from  bson import objectid
import re
from metaphone import doublemetaphone
import editdistance
import jellyfish
from getHighTFIDFAssocs import *
from timeit import default_timer as timer
#import config
import traceback

start = timer()

es = Elasticsearch('10.237.26.159', port=9200, timeout=30)
pp = pprint.PrettyPrinter(indent=4)

client = MongoClient('10.237.26.159', 27017, j=True)
database_name = 'media-db'
db = client[database_name]
es_index='media-db'
es_mapping='entities_resolved'



###################################################################################################################
import jellyfish as jf
from fuzzywuzzy import fuzz 
from fuzzywuzzy import process
from operator import itemgetter
import re

import phonetics

def calculate_time_elapsed():
    global start
    time_elapsed = timer()-start
    start = timer()
    return time_elapsed

def genMetaPhoneScore(str1,str2):	
    '''
        This function returns a score of Phonetic match of two strings. It uses doublemetaphone to generate phonetic strings.
    '''
    if(str1=="" or str2==""):
        return 0.0
    count=0
    try:
        str1=phonetics.dmetaphone(str1)[0]
        str2=phonetics.dmetaphone(str2)[0]
    except:
        return 0.0	
    for i in range(min(len(str1),len(str2))):
        if(str1[i]==str2[i]):
            count+=1

    return(float(count)/max(len(str1),len(str2)))

#addressing words = {Mr., Ms., Mrs., Dr., Prof., Shri., Smt., Sri.}

def validateParams(entity,matchingStrings):
    '''
        Checks the types of passed parameters.
    '''
    if(type(entity) is str and type(matchingStrings) is list):
        return True
    return False

def removeAddWords(stringVal):
    '''
        Return the string/list od strings by removing the Honorfics if any. Can also add more honorifics if needed in future.
    '''
    isString = False
    addWords = ['mr ','ms ','mrs ','dr ','prof ','shri ','smt ','sri ','sir ','adv ','retd ']
    if (type(stringVal) is not list) or (type(stringVal) is str):
        isString = True
        stringVal = [stringVal]
    modifiedStrings = []
    #Convert the strings to lower
    stringVal = map(str.lower,stringVal)
    #TODO : here instead of replacing only '.', replace all special chars with regex
    #regex = re.compile("[^0-9a-zA-Z\s]")
    #Replace '.' with ' '
    stringVal = map(lambda s: s.replace('.',' '), stringVal)
    #stringVal = map(lambda s: regex.sub(' ',s), stringVal)
    for s in stringVal:
        for add in addWords:
            s = s.replace(add,'')
        s = s.strip()
        modifiedStrings.append(s)
    if isString :
        return modifiedStrings[0]
    return modifiedStrings
        

def countInitials(stringList):
    '''
        Return count of intials of name(which is a list of words), E.g. : N D Modi --> returns 2 as count of initials
    '''
    count = 0
    for string in stringList:
        if(string!="" and len(string)<=2):
            count += 1
    return count

"""
def genScore(queryList,matchList):
    i=0
    j=0
    non_initial_match=0
    result_list=[]
    result_score=0.0
    common_words=0
    isMatchSingle=False
    while(i<len(queryList) and j < len(matchList)):
        if(queryList[i][0]==matchList[j][0]):
            common_words+=1
            if(len(queryList[i])<=2 or len(matchList[j])<=2):
                result_score+=float(1)/min(len(queryList),len(matchList))
            else:
                non_initial_match+=1
		t1=genMetaPhoneScore(str(queryList[i]),str(matchList[j]))
		result_list.append(t1)
		if(t1>=0.85):
		    extScoreTuple = process.extract(queryList[i],[matchList[j]])
            result_score+= float(extScoreTuple[0][1]*0.01)/min(len(queryList),len(matchList))
		else:
		    result_score+=0	
            i+=1
            j+=1

        elif(queryList[i][0] > matchList[j][0]):    #qL's first letter is greater than mL's first letter
            j+=1
        else:
            #print matchList[j]
            i+=1    

    if((non_initial_match==1 and result_list[0]<0.85) or (non_initial_match==0)):
        result_score=0.0

    if(common_words==1):
        isMatchSingle=True

    #print non_initial_match,result_list,result_score,isMatchSingle    
    #print common_words
    return result_score,isMatchSingle
"""


def calculateScore(queryList,matchList):
    """
        This function basically calculates the score of matching between given two subword list.
        Phonetics are handled and it uses fuzzywuzzy.process which internally uses Levenshtein
        distance to calculate score

        In case of partial match if process score is much morethan jaro score then we ignore it

        Here, input is a list of subwords.
    """ 
    calculate_time_elapsed()
    # print("Entering calculateScore")
    score = 0
    qLSize = len(queryList)
    mLSize = len(matchList)
    normDenom = qLSize if qLSize<mLSize else mLSize
    i = 0
    j = 0
    isMatchSingle = False
    if(mLSize==1):
        isMatchSingle = True
    initialMatchCount = 0
    lessThanThres = False
    if(qLSize<mLSize):
        noOfInitials = countInitials(queryList)
    else:
        noOfInitials = countInitials(matchList)
    while(i < qLSize and j < mLSize):
        if(queryList[i][0]==matchList[j][0]):   #  start letters of two words match
            if(len(queryList[i])<=2 or len(matchList[j])<=2):
                initialMatchCount += 1
                score += 1.0/normDenom
            else:
                phoneticScore = genMetaPhoneScore(str(queryList[i]),str(matchList[j]))
                if phoneticScore>0.85:
                    extScoreTuple = process.extract(queryList[i],[matchList[j]])
                    #here I'm trying out to use process.extract itself instead of jaro
                    processScore = float(extScoreTuple[0][1]*0.01)/normDenom
                    """
                    if there is only one full word in the list
                    """
                    '''
                    if(noOfInitials==(normDenom-1) and processScore<0.85):
                        lessThanThres = True
                    '''
                    """
                    here extract gives good partial matches which is misleading. so, in case of
                    partial match(len(bigstring) > 1.5(len(smallstring))) I use jaro to check
                    and eliminate it 
                    """
                    stringRatio = max(len(queryList[i]),len(matchList[j]))/min(len(queryList[i]),len(matchList[j]))
                    if(stringRatio>1.5 and jf.jaro_distance(unicode(queryList[i]),unicode(matchList[j])) < ((extScoreTuple[0][1]-5)*0.01)):
                        print('%s and %s are Partial Matches with %s and %s')%(queryList[i],matchList[j],str(jf.jaro_distance(unicode(queryList[i]),unicode(matchList[j]))),str(extScoreTuple[0][1]-5))
                    else:
                        score += processScore
            i+=1
            j+=1
        elif(queryList[i][0] > matchList[j][0]):    #qL's first letter is greater than mL's first letter
            j+=1
        else:
            i+=1
    if(initialMatchCount == normDenom):
        score = 0
    # print("Time taken in calculateScore: %.2fs\n"%(calculate_time_elapsed()))
    return score,isMatchSingle

#TODO handle the case for partial match
#It is a deprecated method. Use calculateScore instead of this.
def oldcalculateScore(queryList,matchList):
    """
        This function basically calculates the score of matching between given two subword list.
        I use extract function of fuzzywuzzy.process for phonetics as I couldn't find better phonetic scoring function than that

        Here I take input as list of subwords, instead of that I can also take two strings.
    """ 
    score = 0
    qLSize = len(queryList)
    mLSize = len(matchList)
    normDenom = qLSize if qLSize<mLSize else mLSize
    i = 0
    j = 0
    isMatchSingle = False
    if(mLSize==1):
        isMatchSingle = True
    initialMatchCount = 0
    lessThanThres = False
    if(qLSize<mLSize):
        noOfInitials = countInitials(queryList)
    else:
        noOfInitials = countInitials(matchList)
    while(i < qLSize and j < mLSize):
        if(queryList[i][0]==matchList[j][0]):   #  start letters of two words match
            if(len(queryList[i])<=2 or len(matchList[j])<=2):
                initialMatchCount += 1
                score += 1.0/normDenom
            else:
                #considering phonetics in this
                extScoreTuple = process.extract(queryList[i],[matchList[j]])
                #print extScoreTuple
                '''
                if(extScoreTuple[0][1] >= 85):
                    jaroScore = jf.jaro_distance(unicode(queryList[i]),unicode(matchList[j]))
                    score += jaroScore/normDenom
                '''
                #here I'm trying out to use process.extract itself instead of jaro
                processScore = float(extScoreTuple[0][1]*0.01)/normDenom
                if (noOfInitials==normDenom-1) and (processScore < 0.85):
                    lessThanThres = True
                """
                here extract gives good partial matches which is misleading. so, in case of
                partial match(len(bigstring) > 1.5(len(smallstring))) I use jaro to check
                and eliminate it 
                """
                
                stringRatio = max(len(queryList[i]),len(matchList[j]))/min(len(queryList[i]),len(matchList[j]))

                if(stringRatio>1.5 and jf.jaro_distance(unicode(queryList[i]),unicode(matchList[j])) < (extScoreTuple[0][1]-5)):
                    print('%s and %s are Partial Matches')%(queryList[i],matchList[j])
                else:
                    score += processScore
            i+=1
            j+=1
        elif(queryList[i][0] > matchList[j][0]):    #qL's first letter is greater than mL's first letter
            j+=1
        else:
            #print matchList[j]
            i+=1
    if(initialMatchCount == normDenom) or (lessThanThres):
        score = 0
    return score,isMatchSingle

def resolveEntities(entity,matchingStrings):
    """ 
        This function takes input as a queryword and listofwords and returns a list of matchingwords of queryword
        Here I remove the addressing words like prof. , sir. etc....
        It then matches the initials also then calculates the score of similarity between two strings.
    """
    calculate_time_elapsed()
    # print("Entering resolveEntities")
    ##validating the parameters
    validateResult = validateParams(entity,matchingStrings)
    if validateResult == False:
        print('Param1 should be a string & Param2 should be a List')
        exit(0)
    # print("Time taken in validateResult: %.2fs\n"%(calculate_time_elapsed()))
    ##en_Entity represents 'enhanced entity name'
    ##Removing addressing words like prof., dr. etc...
    en_Entity = removeAddWords(entity)
    en_MatchingStrings = removeAddWords(matchingStrings)

    ##splitting words on space and sorting the lists so formed
    en_EntitySubWords = en_Entity.split(' ')
    # removes empty strings from the list
    en_EntitySubWords = filter(None,en_EntitySubWords)
    en_EntityBack = " ".join(en_EntitySubWords)
    #print en_EntityBack
    en_EntitySubWordsSorted = sorted(en_EntitySubWords)
    entityInitials = "".join(item[0] for item in en_EntitySubWordsSorted)
    entityMatchingList = []
    en_MatchingStringsBack = []
    for matchString in en_MatchingStrings:
        matchSubWords = matchString.split(' ')
        matchSubWords = filter(None,matchSubWords)
        en_MatchingStringsBack.append(" ".join(matchSubWords))
        matchSubWordsSorted = sorted(matchSubWords)
        matchInitials = "".join(item[0] for item in matchSubWordsSorted)
        if (matchInitials not in entityInitials) and (entityInitials not in matchInitials):
            entityMatchingList.append((matchString.upper(),0,False))
            continue
        else:
            score,isMatchSingle = calculateScore(en_EntitySubWordsSorted,matchSubWordsSorted)
            entityMatchingList.append((matchString.upper(),score,isMatchSingle))
            #print entityInitials,matchInitials," ".join(matchSubWords),score
    #print process.extract(en_EntityBack,en_MatchingStringsBack)
    ## Returning the enitytMatchingList in sorted order of their scores
    #print entityMatchingList
    a=sorted(entityMatchingList,key=itemgetter(1),reverse=True)
    print ""
    print a
    print("Time taken in resolveEntities: %.2fs\n"%(calculate_time_elapsed()))
    for b in a:
        if b[1]<0.7:
              return False

    return True
    #return sorted(entityMatchingList,key=itemgetter(1),reverse=True)




#################################################################################################################

def getAliasesObj(entity):
    aliases = ''
    if entity.get('aliases'):
        for t in entity['aliases']:
            aliases += (' ' + t)
    
    return {
             "match":{
                        "aliases": {
                          "query": aliases,
                          "fuzziness": "AUTO"
                          # "boost":2
                        }
                }
    } 
    
def mergeAssociatedEntities(assEntities1, assEntities2):
    asscEn=assEntities1[:]
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
                      "query":titles if entity.get('title') else 'abc',
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
            if not a['text'] in asscEntities:
                asscEntities.append(a['text'])
                asscEntText += (' ' + a['text'])
            
    
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
    # print("Entering match function of namesMatched_v")
    calculate_time_elapsed()
    name1 = re.sub('\W+',' ',name1.lower())
    name2 = re.sub('\W+',' ',name2.lower())
    #high and low jaro
    j1=' '.join(filter(lambda x:len(x)>0,name1.split()))
    j2=' '.join(filter(lambda x:len(x)>0,name2.split()))
    
    if not (j1.strip() and j2.strip()):
        # print("Time taken in match function0: %.2fs\n"%(calculate_time_elapsed()))
        return False
    
    score=jellyfish.jaro_winkler(j1, j2)
    if score>=0.9:
        # print("Time taken in match function1: %.2fs\n"%(calculate_time_elapsed()))
        return True
    if score<=0.5:
        # print("Time taken in match function1: %.2fs\n"%(calculate_time_elapsed()))
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
    # print("Time taken in match function2: %.2fs\n"%(calculate_time_elapsed()))
    if len(tempArr1) and len(tempArr2):
        return False
    
    return True        

def namesMatched_m(nameArr1, nameArr2):
    # remove initials
    # print("Entering namesMatched_m")
    calculate_time_elapsed()
    l=[x.encode('UTF8') for x in nameArr2]
    for n1 in nameArr1:
         n2=n1
         n3 = n2.encode('utf8')
         
         if not resolveEntities(n3,l):
               # print("Time taken in namesMatched1: %.2fs\n"%(calculate_time_elapsed()))
               return False
    # print("Time taken in namesMatched2: %.2fs\n"%(calculate_time_elapsed()))
    return True

def namesMatched_v(nameArr1, nameArr2):
    # remove initials
    for n1 in nameArr1:
        for n2 in nameArr2:
            if not match(n1, n2):
                return False
    return True

def namesExactlyMatched(nameArr1, nameArr2):
    for n1 in nameArr1:
        for n2 in nameArr2:
            if not matchExact(n1, n2):
                return False
    return True

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
                    }
                }
    }

def getCountryRequest(entity):
    return {
             "query":{
                    "bool":{
                        "filter":{
                            "bool":{ 
                                "must":
                                        {"match":{
                                                    "type": {
                                                      "query": entity['type'],
                                                      "fuzziness": "AUTO"
                                                    }
                                            }
                                       },
                            }
                        },
                        "must": getAliasesObj(entity),
                        "should":
                                  {"match":{
                                                    "resolutions.name": {
                                                      "query": entity['resolutions'][0]['name'] if entity.get('resolutions') else "abc",
                                                      "fuzziness": "AUTO"
                                                    }
                                            }
                                }
                                  
                    }
                }
    }

def getCityStateRequest(entity):
    return {
             "query":{
                    "bool":{
                        "filter":{
                            "bool":{ 
                                "must":
                                        {"match":{
                                                    "type": {
                                                      "query": "City ProvinceOrState",
                                                      "fuzziness": "AUTO"
                                                    }
                                            }
                                       }
                                        ,
                            }
                        },
                        "must": getAliasesObj(entity),
                        "should":
                                  {"match":{
                                                    "resolutions.name": {
                                                      "query": entity['resolutions'][0]['name'] if entity.get('resolutions') else "abc",
                                                      "fuzziness": "AUTO"
                                                    }
                                            }
                                }
                                  
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
                                        ,
                            }
                        },
                        "must": getAliasesObj(entity),
                        "should":
                                  {"match":{
                                                    "resolutions.name": {
                                                      "query": entity['resolutions'][0]['name'] if entity.get('resolutions') else "abc",
                                                      "fuzziness": "AUTO"
                                                    }
                                            }
                                }
                                  
                    }
                }
    }

def getChangedProperties(entity, matchedEntity):
    changedProperties = {}
    articleIds = defaultdict(bool)
    for artId in entity['articleIds']:
        articleIds[artId] = True
    for artId in matchedEntity['articleIds']:
        articleIds[artId] = True
    changedProperties['articleIds'] = articleIds.keys()
    
    if len(matchedEntity['stdName'].strip()) < len(entity['stdName'].strip()):
        changedProperties['stdName'] = entity['stdName']
    else:
        changedProperties['stdName'] = matchedEntity['stdName']
   
    aliases = matchedEntity['aliases'][:]
    for a in entity['aliases']:
        if a not in aliases:
            aliases.append(a)
    changedProperties['aliases'] = aliases
    
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
         
    return changedProperties
     
def getChangedCountryProperties(entity, matchedEntity):
    changedProperties = {}
    
    articleIds = defaultdict(bool)
    for artId in entity['articleIds']:
        articleIds[artId] = True
    for artId in matchedEntity['articleIds']:
        articleIds[artId] = True
    changedProperties['articleIds'] = articleIds.keys()
    
    if len(matchedEntity['stdName'].strip()) < len(entity['stdName'].strip()):
        changedProperties['stdName'] = entity['stdName']
    else:
        changedProperties['stdName'] = matchedEntity['stdName']
        
    aliases = matchedEntity['aliases'][:]
    for a in entity['aliases']:
        if a not in aliases:
            aliases.append(a)
    changedProperties['aliases'] = aliases
    
    if entity.get('resolutions'):
        changedProperties['resolutions'] = entity['resolutions'][:]
    else:
        changedProperties['resolutions'] = matchedEntity['resolutions'][:] if matchedEntity.get('resolutions') else None
        
    return changedProperties
            
def serializeObjectId(entity):
    if entity.get("_id"):
        entity["_id"] = str(entity["_id"])
    
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

def countriesMatch(entity, matchedEntity):
    if entity.get('resolutions') and matchedEntity.get('resolutions'):
        if entity['resolutions'][0]['name'] == matchedEntity['resolutions'][0]['name']:
            return True
        return False
    
    name1 = re.sub('\W+',' ',entity['stdName'].lower()).strip()
    name2 = re.sub('\W+',' ',matchedEntity['stdName'].lower()).strip()
    
    #country names differ only in case of typos, so high threshold
    if jellyfish.jaro_winkler(name1,name2)>=0.9 or (name1 in name2) or (name2 in name1):
        return True

    return False

def citiesMatch(entity, matchedEntity):
    if entity.get('resolutions') and matchedEntity.get('resolutions'):
        res1=entity['resolutions'][0]
        res2=matchedEntity['resolutions'][0]
        if res1.get('containedbycountry')!=res2.get('containedbycountry'):
            return False
     
    name1 = re.sub('\W+',' ',entity['stdName'].lower()).strip()
    name2 = re.sub('\W+',' ',matchedEntity['stdName'].lower()).strip()
    name1 = re.sub('northern|southern|eastern|western|north|south|east|west','',name1)
    name2 = re.sub('northern|southern|eastern|western|north|south|east|west','',name2)
    
    if jellyfish.jaro_winkler(name1, name2)>=0.9 or (name1 in name2) or (name2 in name1):
        return True
    return False
        
def getRelevantEntities(esQuery, mongo_coll):
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

def insertEntity(entity,mongo_coll):
    #print entity
    #db[mongo_coll].replace_one({'_id': entity['_id']},{"id":entity['_id']},upsert=True)
    #db.collection.update_one({"_id":entity['_id']}, {"$set": {"id":entity['_id']}}\
    #, upsert=True)
    db[mongo_coll].insert_one(entity)
    insertedId = entity['_id']
    #print "ID="+str(insertedId)
    del entity['_id']
    serializeObjectId(entity)
    try:
        es.create(index=es_index, doc_type=es_mapping, \
        body=entity, id=str(insertedId))
    except Exception as e:
        print "ID already exists: ", e

def updateDelEn(matchedEntities,changedProperties,mongo_coll):
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
#######################################################################


def match_title(t1,t2):
    title1 = re.sub('\W+', ' ', t1.lower())
    title2 = re.sub('\W+', ' ', t2.lower())
    score = jellyfish.jaro_winkler(unicode(title1), unicode(title2))
    print
    'title1: ', title1, '<>title2: ', title2, ' <> score: ', score
    if score > .88:
        return True
    else:
        return False
    # if (len(title1) <= 10 or len(title2) <= 10) and score < .5:
    #     print
    #     'returning false ', len(title1), ' ', len(title2)
    #     return False
    # return None


def titleMatched_v(nameArr1, nameArr2):
    # remove initials
    for n1 in nameArr1:
        for n2 in nameArr2:
            if not match_title(n1['text'], n2['text']):
                return False
    return True


def match_title_assoc_entity(relEn,changedProperties,relEn_assoc,cp_assoc):
    print("match title assoc entity started")
    calculate_time_elapsed()
    r=titleMatched_v(relEn['title'],changedProperties['title'])
    if r:
        return True
    else:
        x = fuzzySubset(relEn_assoc,cp_assoc)
        print("Time taken in fuzzy subset: %.2fs\n"%(calculate_time_elapsed()))
        return x



def resolvePersonEntity(entity,ent_to_hightfidf, mongo_coll):
    global es_index, es_mapping
    
    request = getPersonRequest(entity)
    print("Getting relevent entities and es query")
    relEntities=getRelevantEntities(request, mongo_coll)
    print("Got relevent entities and es query")
    index = 0
    changedProperties=entity
    matchedEntities=[]
    
    for relEn in relEntities:
        relEn_assoc={}
        cp_assoc={}
        #print relEn['associatedEntities'][:]['text']
        try:
            #print('relEn[stdName]:',relEn['stdName'])
            relEn_assoc=ent_to_hightfidf[relEn['stdName']]
            cp_assoc=ent_to_hightfidf[changedProperties['stdName']]
        except:
            print "########################################################3"
            print('key error')
            print 'relEn[stdName]:',relEn['stdName']
            print relEn_assoc
            print 'changedProperties[stdName]: ',changedProperties['stdName']
            print cp_assoc
            print "#######################################################"
        '''
        for dict1 in relEn['associatedEntities']:
            relEn_assoc.append(dict1['text'])
        for dict1 in changedProperties['associatedEntities']:
            cp_assoc.append(dict1['text'])
        '''
        relEn_assoc=set(relEn_assoc)
        cp_assoc=set(cp_assoc)
        #print cp_assoc
        #exit(0)
        #print "rel entity score:",relEn['score'],"  rel en aliases:",relEn['aliases']
        try:
            if ((namesMatched_m(relEn['aliases'], changedProperties['aliases']) and namesMatched_v(relEn['aliases'], changedProperties['aliases']))) or (namesExactlyMatched(relEn['aliases'], changedProperties['aliases'])):# and match_title_assoc_entity(relEn,changedProperties,relEn_assoc,cp_assoc):# or fuzzySubset(relEn_assoc,cp_assoc):
                if match_title_assoc_entity(relEn, changedProperties, relEn_assoc, cp_assoc):
                    index = 1
                    changedProperties = getChangedProperties(changedProperties, relEn)
                    matchedEntities.append(relEn)
        except:
            continue
    
    if index == 0:
        print "no match"
        insertEntity(entity, mongo_coll)
    else:
        print "match found"
        updateDelEn(matchedEntities,changedProperties,mongo_coll)
        
def resolveCountryEntity(entity, mongo_coll):
    global es_index, es_mapping

    request = getCountryRequest(entity)
    relEntities=getRelevantEntities(request, mongo_coll)
    
    index = 0
    changedProperties=entity
    matchedEntities=[]
    
    for relEn in relEntities:
        if countriesMatch(changedProperties, relEn):
            index = 1
            changedProperties = getChangedCountryProperties(changedProperties, relEn)
            matchedEntities.append(relEn)
    if index == 0:
        insertEntity(entity, mongo_coll)
    else:
        updateDelEn(matchedEntities,changedProperties,mongo_coll)
        
def resolveCityStateEntity(entity, mongo_coll):
    global es_index, es_mapping
    
    request = getCityStateRequest(entity)
    relEntities=getRelevantEntities(request, mongo_coll)
    
    index = 0
    changedProperties=entity
    matchedEntities=[]
    
    for relEn in relEntities:
        if citiesMatch(changedProperties, relEn):
            index = 1
            changedProperties = getChangedCountryProperties(changedProperties, relEn)
            matchedEntities.append(relEn)
 
    if index == 0:
        insertEntity(entity,mongo_coll)
    else:
        updateDelEn(matchedEntities, changedProperties,mongo_coll)
        
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
    

def  orgMatched(nameArr1, nameArr2):
    # match exactly, abbr considering stop words, abbr without stop words, part of a name
    for n1 in nameArr1:
        #n1=re.sub('\W+',' ',n1)
        n1=re.sub('pvt|ltd|inc|corp|private|public','',n1,flags=re.IGNORECASE)
        n1L = n1.lower()
        
        for n2 in nameArr2:
            #n2 = re.sub('\W+',' ',n2)
            n2=re.sub('pvt|ltd|inc|corp|private|public','',n2,flags=re.IGNORECASE)
            n2L=n2.lower()
            
            
            if len(n1.split())>1 and len(n2.split())>1: # assuming that names of len 1 are abbr
                if jellyfish.jaro_winkler(n1L, n2L)>=0.9:
                    continue
                elif (n1L in n2L) or (n2L in n1L):
                    continue

            if isAbbrOf(n1, n2):
                continue
            else:
                return False
    
    return True

def matchesCompanyEntity(entity, matchedEntity):
    
    if entity.get('resolutions') and matchedEntity.get('resolutions'):
        if entity['resolutions'][0]['name'].strip() == matchedEntity['resolutions'][0]['name'].strip():
            return True
            
    return orgMatched(entity['aliases'], matchedEntity['aliases'])

def getChangedCompProp(entity, matchedEntity):             
    changedProperties = {}
    
    changedProperties['aliases'] = entity['aliases'][:]
    for alias in matchedEntity['aliases']:
        if alias not in changedProperties['aliases']:
            changedProperties['aliases'].append(alias)
    
    # changedProperties['articleIds'] = entity['articleIds'][:]
    
    changedProperties['articleIds'] = defaultdict(bool)
    for artId in entity['articleIds']:
        changedProperties['articleIds'][artId] = True
    for artId in matchedEntity['articleIds']:
        # if artId not in changedProperties['articleIds']:
        changedProperties['articleIds'][artId] = True
    changedProperties['articleIds'] = changedProperties['articleIds'].keys()
    # print("%s relEN articlesIds(%d,%d) took in %.2fs"%(matchedEntity['stdName'],len(entity['articleIds']),len(matchedEntity['articleIds']),calculate_time_elapsed()))
                
    stdName1 = entity['stdName'].strip()
    stdName2 = matchedEntity['stdName'].strip()
    changedProperties['stdName'] = stdName1 if len(stdName1) > len(stdName2) else stdName2
        
    if entity.get('resolutions'):
        changedProperties['resolutions'] = entity['resolutions']
    else:
        changedProperties['resolutions'] = matchedEntity.get('resolutions')
        
    return changedProperties


def resolveCompanyEntity(entity, mongo_coll):
    global es_index, es_mapping
    
    request = getCompanyRequest(entity)
    relEntities=getRelevantEntities(request, mongo_coll)
    
    index = 0
    changedProperties=entity
    matchedEntities=[]
    
    for relEn in relEntities:
        if matchesCompanyEntity(changedProperties, relEn):
            index=1
            changedProperties=getChangedCompProp(changedProperties, relEn)
            matchedEntities.append(relEn)
    
    if index==0:
        insertEntity(entity,mongo_coll)
    else:
        updateDelEn(matchedEntities, changedProperties,mongo_coll)

'''
Function that takes a collection and returns entity to top 50%
highest tfidf associated entities' mapping.
'''    
def getHighTFIDFDicts(cursors):
    
    count=0
    ent_to_assoc=defaultdict(list)
    assoc_to_ent=defaultdict(list)
    for cursor in cursors:
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
                    #if assoc in assoc_to_ent.keys():
                    assoc_to_ent[assoc].append((entname,cnt))
                    #else:
                    #    assoc_to_ent[assoc]=[(entname,cnt)]

                    #if entname in ent_to_assoc.keys():
                    ent_to_assoc[entname].append((assoc,cnt))
                    #else:
                    #    ent_to_assoc[entname]=[(assoc,cnt)]
            except:
                continue
    print("Entities to Assoc entities mapping done")
    ent_to_tfidf=createTFIDFDict(ent_to_assoc,assoc_to_ent)
    print "len ent_to_tfidf: ",len(ent_to_tfidf)
    # print ent_to_tfidf
    ent_to_hightfidf=keepHighTFIDFAssocs(ent_to_tfidf)
    print "len ent_to_hightfidf: ", len(ent_to_hightfidf)
    # print ent_to_hightfidf
    return ent_to_hightfidf

def resolve(collection, ent_to_hightfidf, mongo_coll, cursor):
    # cursor = collection.find({"resolved":False}, no_cursor_timeout=True)#.limit(entities_limit)
    count = 0
    totalCount = 0
    for entity in cursor:
        try:
            count += 1
            #if(entity['resolved']==True):
            #    continue
            id=entity['_id']
            if entity['type'] == 'Person':
                print (entity['stdName'])
                resolvePersonEntity(entity,ent_to_hightfidf, mongo_coll)
            elif entity['type']in ['Country', 'Continent']:
                resolveCountryEntity(entity, mongo_coll)
            elif entity['type'] in ['City', 'ProvinceOrState']:
                resolveCityStateEntity(entity, mongo_coll)
            elif entity['type'] in ['Company', 'Organization']:
                resolveCompanyEntity(entity, mongo_coll)
            print (count)
            collection.update_one({'_id':id}, {'$set':{'resolved':True}})
            totalCount+=1
            print("Time taken: %.2fs\n"%(calculate_time_elapsed()))
        except Exception:
            traceback.print_exc()
            continue

    print("Total entities: %d"%count)
    print("Total entities resolved: %d"%totalCount)
    cursor.close()


def level0():
    #collection = db['46L_articles__unresolved_entities']
    # coll_name ='all_media_entities_unresolved_test'
    coll_name = 'tech-all-refined_unresolved_entities'
    collection = db[coll_name]
    num_chunks = 1
    coll_size = db.command("collstats", coll_name)['count']
    chunk_size = coll_size//num_chunks + 1
    
    for chunk in range(0,1):
        
        #mongo_coll='chunk_%d_resolved_entities'%(chunk+1)
        # mongo_coll='all_media_entities_resolved_test'
        mongo_coll = 'tech-all-refined_resolved'
        
        cursor = collection.find({}, no_cursor_timeout=True).skip(chunk*chunk_size).limit(chunk_size)
        ent_to_hightfidf=getHighTFIDFDicts([cursor])
        cursor.close()
        print("Time taken: %.2fs\n"%(calculate_time_elapsed()))
        cursor = collection.find({}, no_cursor_timeout=True).skip(chunk*chunk_size).limit(chunk_size)
        resolve(collection, ent_to_hightfidf, mongo_coll, cursor)
        
    client.close()

def levelPlus1():
    level = 4
    for chunk in range(1,16,2**level):
        name = '_'.join([str(i) for i in range(chunk,chunk+2**level)])
        name1 = '_'.join([str(i) for i in range(chunk,chunk+2**(level-1))])
        name2 = '_'.join([str(i) for i in range(chunk+2**(level-1),chunk+2**level)])
        # unresolved_coll='chunk_%s_unresolved_entities'%(name)
        unresolved_coll=db['chunk_%s_resolved_entities'%(name1)]
        unresolved_col2=db['chunk_%s_resolved_entities'%(name2)]
        resolved_col='chunk_%s_resolved_entities'%(name)
        print('chunk_%s_resolved_entities'%(name1))
        print('chunk_%s_resolved_entities'%(name2))
        print(resolved_col)
         
        cursor1 = unresolved_coll.find({}, no_cursor_timeout=True)
        cursor2 = unresolved_col2.find({}, no_cursor_timeout=True)
        ent_to_hightfidf=getHighTFIDFDicts([cursor1,cursor2])
        cursor1.close()
        cursor2.close()
        print("Time taken: %.2fs\n"%(calculate_time_elapsed()))
        cursor = unresolved_coll.find({}, no_cursor_timeout=True)
        resolve(unresolved_coll, ent_to_hightfidf, resolved_col, cursor)
        cursor = unresolved_col2.find({}, no_cursor_timeout=True)
        resolve(unresolved_col2, ent_to_hightfidf, resolved_col, cursor)

def liveER():
    
    unresolved_coll=db['entities_unresolved_overall']
    resolved_col='entities_resolved_overall' 

    cursor1 = unresolved_coll.find({"resolved":False}, no_cursor_timeout=True)
    cursor2 = db[resolved_col].find({},no_cursor_timeout=True)
    ent_to_hightfidf=getHighTFIDFDicts([cursor1, cursor2])
    cursor1.close()
    cursor2.close()
    print("Time taken: %.2fs\n"%(calculate_time_elapsed()))
    cursor = unresolved_coll.find({"resolved":False}, no_cursor_timeout=True)
    resolve(unresolved_coll, ent_to_hightfidf, resolved_col, cursor)

        

if __name__ == '__main__':
    #Overall ER
    #levelPlus1()
    level0()
    
    #ER for new articles
    #liveER()


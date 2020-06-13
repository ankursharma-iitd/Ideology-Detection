#Script to test ER using associated entities.
from pymongo import MongoClient
import pprint
from  bson import objectid
import re
from collections import defaultdict
import math
from fuzzy_subset import fuzzySubset,sim
import operator
import pickle
from timeit import default_timer as timer
#import config

'''
Function to sort the tuples of form (name,tfidf) on tfidf.
'''

start = timer()

def calculate_time_elapsed():
    global start
    time_elapsed = timer()-start
    start = timer()
    return time_elapsed

def sortTuples(list_of_tuples):
    d=dict()
    for tup in list_of_tuples:
        d[tup[0]]=float(tup[1])
    sorted_d = sorted(d.items(),key=operator.itemgetter(1))
    return sorted_d
def inverseDocumentFrequency(l,N):
    #Number of entities the assoc occurs with
    num_ent=len(l)
    idf=math.log(N/float(num_ent))
    return idf	

def createTFIDFDict(ent_to_assoc,assoc_to_ent):
    ent_to_tfidf=defaultdict(list)
    count=0
    ent_to_assoc_len = len(ent_to_assoc)
    for ent in ent_to_assoc.keys():
        #W: number of associated entities for ent (equivalent to #words in page p)
        W=len(ent_to_assoc[ent])
        count+=1
        #if(count%100000==0):
        print("tfidf", count)
        #   print("Time taken to create tfidf dict for 1 lac entities %.2fs\n"%(calculate_time_elapsed()))
        for tup in ent_to_assoc[ent]:
            assoc=tup[0]
            #Frequency of assoc cooccuring with ent
            freq=tup[1]
            #Term frequency of assoc for that ent
            tf=freq/float(W)
            #IDF for assoc across all entities
            #The second entry in the function is the number of entities
            idf=inverseDocumentFrequency(assoc_to_ent[assoc], ent_to_assoc_len)
            #Relevance score for this (ent,assoc) pair
            relevance=tf*idf
            #Add the (assoc,tfidf) to that particular entity
            #if ent in ent_to_tfidf.keys():
            ent_to_tfidf[ent].append((assoc,relevance))
            #else:
            #   ent_to_tfidf[ent]=[(assoc,relevance)]

    #pickle.dump(ent_to_tfidf, open("ent_to_tfidf.pkl", 'w'))
    print("ent to tfidf dict created")

    return ent_to_tfidf

'''
Function to keep top 50% of values (tfidf) for each entity in the
entity->(assoc,tfidf) dictionary.
'''
def keepHighTFIDFAssocs(diction):
    ent_to_hightfidf=dict()
    count=0
    for ent in diction.keys():
        count+=1
        if(count%10000==0):
            print("sort", count)
            print("Time taken for sorting of 10k entities: %.2fs\n"%(calculate_time_elapsed()))
        sorted_l=sortTuples(diction[ent])
        #Store just the names (first element) from each tuple in the list
        #of tuples.
        s_l=[]
        for tup in sorted_l:
            s_l.append(tup[0])
        if(len(s_l)%2==0):
            l=len(s_l)//2
        else:
            l=len(s_l)//2+1
        

        new_l=s_l[0:l]
        #ent->(names with high tfidf) mapping (tfidf scores themselves are
        #not stored)
        ent_to_hightfidf[ent]=new_l
    return ent_to_hightfidf

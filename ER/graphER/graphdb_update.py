import pymongo
from pymongo import MongoClient
from collections import Counter
import ast


client = MongoClient('mongodb://10.237.26.159:27017/')
#client = MongoClient('mongodb://localhost:27017/')
db = client['eventwise_media-db']

db1 = client['eventwise_media-db']

def updateComp(newcollection):
        x=db.graph_comp.find({})
        posts = db1[newcollection]
        
        doc={'_id':None,'graphid':None,'associatedEntities':[{'count':0,'text':'#'}],'stdName':None,'aliases':'','type':'Company','articleIds':[],'title':[]}
        
        for i in x:
                d={'_id':None,'graphid':None,'associatedEntities':[{'count':0,'text':'#'}],'stdName':None,'type':'Company','articleIds':[]}
                #print(d)
                d['_id']=i['_id']
                d['graphid']=i['uuid']
                d['stdName']=i['name']  
        
                dep=i['extra'].encode('ascii','ignore').split(',')
        
                if len(dep)>0:
                        d['associatedEntities']=[]
                for p in dep:
                        p=p.replace('null','').replace('[','').replace(']','').replace('"','')
                        p_={'count':1,'text':p}
                        if not(p_ in d['associatedEntities']):
                                d['associatedEntities'].append(p_)
        
                posts.insert_one(d)
        

def updateBsp(newcollection):
        x=db.graph_bsp.find({})
        posts = db1[newcollection]
        
        doc={'_id':None,'graphid':None,'associatedEntities':[{'count':0,'text':'#'}],'stdName':None,'aliases':'','type':'Person','articleIds':[],'title':[]}
        err=[]
        
        for i in x:
                d={'_id':None,'graphid':None,'associatedEntities':[{'count':0,'text':'#'}],'stdName':None,'aliases':'','type':'Person','articleIds':[],'title':[]}
                #print(d)
                d['_id']=i['_id']
                d['graphid']=i['uuid']
                d['stdName']=i['name']
                als=i['aliases'].encode('ascii','ignore').split(',')
                als_=[]
                
                for p in als:
                        p=p.replace('null','').replace('[','').replace(']','').replace('"','')
                        als_.append(p)
                d['aliases']=als_
                #use the string formatting
                dep=i['extra'].encode('ascii','ignore').split(',')
        
                if len(dep)>0:
                        d['associatedEntities']=[]
                for p in dep:
                        p=p.replace('null','').replace('[','').replace(']','').replace('"','')
                        p_={'count':1,'text':p}
                        if not(p_ in d['associatedEntities']):
                                d['associatedEntities'].append(p_)
        
                posts.insert_one(d)
        
def updateMin(newcollection):
        x=db.graph_min.find({})
        posts = db1[newcollection]
        
        for i in x:
                d={'_id':None,'graphid':None,'associatedEntities':[{'count':0,'text':'#'}],'stdName':None,'type':'Organization','articleIds':[],'aliases':''}
                #print(d)
                d['_id']=i['_id']
                d['graphid']=i['uuid']
                d['stdName']=i['name']
                
                als=i['aliases'].encode('ascii','ignore').split(',')
                als_=[]
                for p in als:
                        p=p.replace('null','').replace('[','').replace(']','').replace('"','')
                        als_.append(p)
                d['aliases']=als_
                
                dep=i['extra'].encode('ascii','ignore').split(',')
        
                if len(dep)>0:
                        d['associatedEntities']=[]
                for p in dep:
                        p=p.replace('null','').replace('[','').replace(']','').replace('"','')
                        p_={'count':1,'text':p}
                        if not(p_ in d['associatedEntities']):
                                d['associatedEntities'].append(p_)
        
                posts.insert_one(d)
        

def updatePol(newcollection):
        x=db.graph_pol.find({})
        posts = db1[newcollection]

        doc={'_id':None,'graphid':None,'associatedEntities':[{'count':0,'text':'#'}],'stdName':None,'aliases':'','type':'Person','articleIds':[]}
        err=[]
        for i in x:
                d=doc
                d['_id']=i['_id']
                d['graphid']=i['uuid']
                d['stdName']=i['name']
                try:
                        als=ast.literal_eval(i['aliases'])
                except:
                        err.append(i)
                d['aliases']=als
                ent=ast.literal_eval(i['party'])
                a_ent={}
                for e in ent:
                        if e!='Independent':
                                a_ent.update({'count':1,'text':e})
                d['associatedEntities']=[a_ent]
                posts.insert_one(d)
        

def updateIAS(newcollection):
        x=db.graph_ias.find({})
        posts = db1[newcollection]
        
        doc={'_id':None,'graphid':None,'associatedEntities':[{'count':0,'text':'#'}],'stdName':None,'aliases':'','type':'Person','articleIds':[],'title':[]}
        
        err=[]
        for i in x:
                d={'_id':None,'graphid':None,'associatedEntities':[{'count':0,'text':'#'}],'stdName':None,'aliases':'','type':'Person','articleIds':[],'title':[]}
                #print(d)
                d['_id']=i['_id']
                d['graphid']=i['uuid']
                d['stdName']=i['name']
                als=i['aliases'].encode('ascii','ignore').split(',')
                als_=[]
                
                for p in als:
                        p=p.replace('null','').replace('[','').replace(']','').replace('"','')
                        als_.append(p)
                d['aliases']=als_
                #use the string formatting
                dep=i['dept'].encode('ascii','ignore').split(',')
        
                if len(dep)>0:
                        d['associatedEntities']=[]
                for p in dep:
                        p=p.replace('null','').replace('[','').replace(']','').replace('"','')
                        p_={'count':1,'text':p}
                        if not(p_ in d['associatedEntities']):
                                d['associatedEntities'].append(p_)
                title=[]
                title=i['extra'].encode('ascii','ignore').split(',')
        
                for p in title:
                        p=p.replace('null','').replace('[','').replace(']','').replace('"','')
                        p_={'text':p,'articleIds':[]}
                        if not(p_ in d['title']):
                                d['title'].append({'text':p,'articleIds':[]})
                        
                posts.insert_one(d)
        
newcollection='all_media_entities_unresolved'

#updateIAS(newcollection)
updatePol(newcollection)
#updateMin(newcollection)
#updateComp(newcollection)
#updateBsp(newcollection)

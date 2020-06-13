from stanfordcorenlp import StanfordCoreNLP
from datetime import datetime
import json
import re, string, os, itertools
import nltk
from nltk.corpus import stopwords
from collections import defaultdict
import os
import numpy as np
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import subprocess
import shlex
from pymongo import MongoClient
import pathlib
from ExtractSentences import ExtractSentences
from text_parser import StanfordNLP

fixed_keywords = ['says', 'said', 'asks', 'asked', 'told', 'announced', 'announce', 'claimed', 'claim']

def get_all_entities(collection, types, num_entities=-1):
    '''
    Method to parse all the entities from the collection of a particular 'type'
    '''
    pipeline = [{"$project":{"stdName":1,"type":1,"aliases":1,"articleIds":1,"num":{"$size":"$articleIds"}}}]
    cursor = list(collection.aggregate(pipeline))
    top_n_entities = {}
    entities = {type:[] for type in types}
    for ent in cursor:
        if(ent['type'] in types):
            entities[ent['type']].append(ent)

    for type in entities.keys():
        entities[type].sort(key=lambda x: x['num'], reverse=True)
        if num_entities == -1:
            num_entities = len(entities[type])
            print('All the {} entities are under consideration.'.format(num_entities))
        else:
            print('Num of top-entities under consideration: {}'.format(num_entities))
        top_n_entities[type] = [{"name":obj['stdName'],"coverage":obj['num'],"aliases":obj['aliases'],"articleIds":obj['articleIds']} for obj in entities[type][:num_entities]]
    return top_n_entities

def findSentiment(sentiString):
    '''
    return Sentiment by Vader
    :param sentiString:
    :return:
    '''
    analyzer = SentimentIntensityAnalyzer()
    sentiment = analyzer.polarity_scores(sentiString)
    a_sent = (sentiment["compound"])
    return a_sent

'''
entitySpecificSentimentAnalysis:
    takes two argument
    1. Input File : set of sentences
    2. Keywords associated with target

    and output two list
    1. Articles on target
    2. Articles by target
    3. Articles not about target
'''


def preprocesstext(doc_set):
    text = doc_set.lower()
    text = text.replace('\r', '')
    text = text.replace('\r\n', '')
    text = text.replace('\n', '')
    text = text.replace('"', '')
    text = text.replace('%', ' ')
    return text


def entitySpecificCoverageAnalysis(doc_set, entity_keywords, entity_name, e_aliases):
    '''
    Finds the sentences that are about or by the entity
    :param doc_set: set of sentences
    :param entity_keywords: keywords as to which entity to identify in the sentence.
    :return: onTarget_sentences, byTarget_sentences, removed_sentences, onTargetTopic, byTargetTopic
    '''
    sNLP = StanfordNLP()
    onTargetArticles = []
    byTargetArticles = []
    removedArticles = []
    short_entity_name = ''.join(entity_name.split()).lower()
    entity_keywords.append(short_entity_name)
    for i in range(len(doc_set)):
        print('Document: {}'.format(i))
        text = preprocesstext(doc_set[i])
        for alis in e_aliases:
            text = text.replace(' ' + alis.lower() + ' ', ' ' + short_entity_name + ' ')
            text = text.replace(' ' + alis.lower() + '. ', ' ' + short_entity_name + ' . ')
            text = text.replace(' ' + alis.lower() + ', ', ' ' + short_entity_name + ' , ')
        try:
            pos_text = sNLP.pos(text)
        except json.decoder.JSONDecodeError:
            print('JSON_Decode_Error: ', text)
            continue
        parse_text = sNLP.dependency_parse(text)
        state1 = False
        state2 = False
        for pt in parse_text:
            if ((pt[0] == 'nsubj') or (pt[0] == 'nmod') or (pt[0] == 'amod') or (pt[0] == 'dobj')) and (
                        (pos_text[pt[1] - 1][0] in entity_keywords) or (pos_text[pt[2] - 1][0] in entity_keywords)):
                if ((pt[0] == 'nsubj') and (
                                pos_text[pt[1] - 1][0] in fixed_keywords or pos_text[pt[2] - 1][0] in fixed_keywords)):
                    state2 = True
                else:
                    state1 = True
        if state1:
            onTargetArticles.append(text)
        if state2:
            byTargetArticles.append(text)
        else:
            removedArticles.append(text)
    return (onTargetArticles, byTargetArticles, removedArticles)

def get_names_aliases_articles(entities):
    e_names = []
    e_aliases = []
    e_articleIds = []
    indices = []
    for type in entities.keys():
        for entity in entities[type]:
            e_names.append(entity['name'])
            e_aliases.append(entity['aliases'])
            e_articleIds.append(entity["articleIds"])
    return (e_names, e_aliases, e_articleIds)

def findPowerEliteIndex(entity_name, e_names, e_aliases):
    '''
    FInd all the entity resolution which may contain the given entity.
    :param entity_name: Given entity
    :param e_names: list of all entity names
    :param e_aliases: list of all entity aliases
    :return: set of indices
    '''
    print('Search ', entity_name, ' : ', len(e_names))
    indices = []
    for i in range(len(e_names)):
        name = e_names[i].replace('.', '')
        alias = ','.join(e_aliases[i])
        if entity_name.lower() in name.lower() or entity_name.lower() in alias.lower():
            indices.append(i)
    return indices
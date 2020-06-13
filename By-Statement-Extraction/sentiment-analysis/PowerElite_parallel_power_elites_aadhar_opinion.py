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
from ExtractSentences import ExtractSentences
import top_n_entities
from pymongo import MongoClient
import sys
import pathlib

sys.path.insert(0, "../")
import config

######### MODE  ##############
mode = int(input('Select Mode: 1 - IE_Power_elite  0 - Top100 \n'))
findPowerElite = False
if mode==1:
    findPowerElite = True
findEntityWithIndex = True

#########  INPUT    #########
N = 40000  # top N entities
resolved_entity_table = 'farmers_opinion_resolved'
article_table = 'farmers_opinion'
# num_topics = 20  # No. of topics identified using LDA
# url_topic_mapping_file = './Input/article_data_20.csv'

# ........................... #
power_elite_file = './Input/power_elites.txt'
power_elite_alias_file = './Input/power_elite_alias_file.txt'
res_folder_main = './RESULTS_PowerElite_farmers_opinion/'
res_folder = res_folder_main + '1'
os.system('mkdir -p ' + res_folder.lstrip('./'))


alias_folder = res_folder_main + 'aliases'
all_entity_names_fname = './Input/' + resolved_entity_table + '_all_entity_names.txt'
do_exist_all_entity_names = pathlib.Path(all_entity_names_fname).is_file()
os.system('mkdir -p ' + alias_folder.lstrip('./'))

# Command line arguments
start_ind = int(sys.argv[1])
end_ind = int(sys.argv[2])

###### create folders if not exist ######
directory1 = os.path.dirname(res_folder_main)
directory2 = os.path.dirname(alias_folder)
directory3 = os.path.dirname(res_folder)

if not os.path.exists(directory1):
	os.makedirs(directory1)
	
if not os.path.exists(directory2):
	os.makedirs(directory2)
	
if not os.path.exists(directory3):
	os.makedirs(directory3)

#########################################

client = MongoClient(config.mongoConfigs['host'], config.mongoConfigs['port'])
db = client[config.mongoConfigs['db']]
collection = db[resolved_entity_table]  # collection having resolved entities
art_collection = db[article_table]  # collection having articles

entity_types = config.entity_types
short_sources_list = config.short_sources_list
sources_list = config.sources_list
fixed_keywords = ['says', 'said', 'asks', 'asked', 'told', 'announced', 'announce', 'claimed', 'claim']

extractor = ExtractSentences()  # object for extracting sentences from text


class StanfordNLP:
    def __init__(self, host='http://localhost', port=9000):
        self.nlp = StanfordCoreNLP(host, port=port,
                                   timeout=30000)
        self.props = {
            'annotators': 'tokenize,ssplit,pos,lemma,ner,parse,depparse,dcoref,relation',
            'pipelineLanguage': 'en',
            'outputFormat': 'json'
        }

    def word_tokenize(self, sentence):
        return self.nlp.word_tokenize(sentence)

    def pos(self, sentence):
        return self.nlp.pos_tag(sentence)

    def ner(self, sentence):
        return self.nlp.ner(sentence)

    def parse(self, sentence):
        return self.nlp.parse(sentence)

    def dependency_parse(self, sentence):
        return self.nlp.dependency_parse(sentence)

    def annotate(self, sentence):
        return json.loads(self.nlp.annotate(sentence, properties=self.props))

    @staticmethod
    def tokens_to_dict(_tokens):
        tokens = defaultdict(dict)
        for token in _tokens:
            tokens[int(token['index'])] = {
                'word': token['word'],
                'lemma': token['lemma'],
                'pos': token['pos'],
                'ner': token['ner']
            }
        return tokens


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


def printRemovedToFile(entity, source, sentences):
    '''
    Write the removed sentences to a file
    :param entity: Entity Name
    :param source:  News source Short URL to be used in file naming
    :param sentences: set of removed sentences
    :return: None
    '''
    fname = './' + res_folder + '/removed_' + '_'.join(entity.split()) + '_' + source + '.txt'
    outfile = open(fname, 'w')
    for s in sentences:
        outfile.write(s + '\n')
    outfile.close()


def printToFile(about_entity, by_entity, entity, source):
    '''
    :param about_entity: set of sentences that are about the entity
    :param by_entity:  set of sentences that are statements made by the entity
    :param entity: Entity name
    :param source: News source Short URL to be used in file naming
    :return: None
    '''
    about_sent = 0
    by_sent = 0
    if len(about_entity):
        fname = './' + res_folder + '/about_' + '_'.join(entity.split()) + '_' + source + '.txt'
        outfile = open(fname, 'w')
        for l in range(len(about_entity)):
            line = about_entity[l]
            line_sent = findSentiment(line)
            outfile.write(';;' + str(line_sent) + ';;' + line + '\n')
            about_sent = about_sent + line_sent
        outfile.close()
    if len(by_entity):
        fname = './' + res_folder + '/by_' + '_'.join(entity.split()) + '_' + source + '.txt'
        outfile = open(fname, 'w')
        for l in range(len(by_entity)):
            line = by_entity[l]
            line_sent = findSentiment(line)
            outfile.write(';;' + str(line_sent) + ';;' + line + '\n')
            by_sent = by_sent + line_sent
        outfile.close()
    return (about_sent, by_sent)


def isEntityInText(entity_name, text):
    '''
    finds the match of entity in the given text
    :param entity_name:
    :param text:
    :return:
    '''
    count = 0
    total = 0
    for word in entity_name.split():
        total = total + 1
        if word.lower() in text.lower():
            count = count + 1
    try:
        if count / total >= 0.5:
            return True
        else:
            return False
    except Exception:
        return False


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


def findPowerEliteInfo():
    # Read all the Entities in decreasing order of coverage (mentions)
    entities = top_n_entities.get_top_n_entities(collection, N, entity_types)

    e_names = []
    e_aliases = []
    e_articleIds = []
    indices = []

    file_all_entity_names = None
    if not do_exist_all_entity_names:
        file_all_entity_names = open(all_entity_names_fname, 'w')
    for type in entities.keys():
        for entity in entities[type]:
            e_names.append(entity['name'])
            e_aliases.append(entity['aliases'])
            e_articleIds.append(entity["articleIds"])
            if not do_exist_all_entity_names:
                file_all_entity_names.write(str(entity['name']) + '\t' + str(entity['coverage']) + '\n')

    if not do_exist_all_entity_names:
        file_all_entity_names.close()
        print('Created file with all entity names')
    print("Read all top entities...")

    # power_elites = [line.strip() for line in open(power_elite_file, 'r').readlines()]
    power_elitesdata = np.genfromtxt(power_elite_file, delimiter='\t', dtype=str)
    power_elites = power_elitesdata[:, 0]
    print("Read all power elite names...")

    # main output file
    fname = res_folder_main + 'entity_coverage_results_' + str(start_ind) + 'to' + str(end_ind) + '.txt'
    entity_coverage_file = open(fname, 'w')
    entity_total_coverage_file = open(fname.rstrip('.txt') +'total.txt','w')
    entity_coverage_file.write('Entity\t')
    entity_total_coverage_file.write('Entity\t')
    for source in sources_list:
        entity_coverage_file.write(source + '\t\t\t\t')
    entity_coverage_file.write('Total_about \t Total_by \n')
    entity_total_coverage_file.write('Total_about \t Total_by \n')
    fwrite_temp = open(power_elite_alias_file, 'r')
    about_entity_coverages = {s: defaultdict(int) for s in sources_list}
    by_entity_coverages = {s: defaultdict(int) for s in sources_list}
    aliases = [list(map(str.lower, map(str.strip, line.split(",")))) for line in fwrite_temp]
    print("Read all aliases of all power elites...")

    fname = res_folder_main + 'entity_index_' + str(start_ind) + 'to' + str(end_ind) + '.txt'
    entity_index_file = open(fname, 'w')

    print("start index: ", start_ind, "  end index:", end_ind)
    for i in range(start_ind, end_ind + 1):
        entity_name = power_elites[i]
        is_power_elite = power_elitesdata[i, 1]
        pe_group = power_elitesdata[i, 2]
        entity_ind = findPowerEliteIndex(entity_name, e_names, e_aliases)
        entity_ind_str = ','.join(str(ei) for ei in entity_ind)
        # To keep track of all the top 100 entities
        indices.extend(entity_ind)

        if len(entity_ind) == 0 or len(aliases[i]) == 0:
            entity_index_file.write(entity_name + '\t-1\t' + entity_ind_str + '\n')
            entity_coverage_file.write(
                entity_name + '\t' + is_power_elite + '\t' + pe_group + '0\t0\t0\t0\t' * 7 + '\n')
            print(entity_name + ' SKIPPPING ...')
            continue
        print(entity_name + ' STARTING ...')
        print(entity_name + '\t Indices: ' + entity_ind_str)
        entity_coverage_file.write(entity_name + '\t' + is_power_elite + '\t' + pe_group + '\t')
        entity_total_coverage_file.write(entity_name + '\t' + is_power_elite + '\t' + pe_group + '\t')
        entity_index_file.write(entity_name + '\t' + str(entity_ind[0]) + '\t' + entity_ind_str + '\n')

        articles = {s: [] for s in sources_list}
        print(entity_name + ' : READ ARTICLE IDS ...')

        print(entity_ind)
        new_entity_alias = []
        for l in range(len(entity_ind)):
            new_entity_alias.extend(e_aliases[entity_ind[l]])
            for article_id in e_articleIds[entity_ind[l]]:
                article = art_collection.find({"_id": article_id})[0]
                # url = article["articleUrl"]
                text = article["text"]
                source = article["sourceName"]
                if source in sources_list:
                    articles[source].append(text)

        print(entity_name + ' : Compute for each SOURCE ...')
        about_cov = 0
        by_cov = 0

        for j in range(len(sources_list)):
            source = sources_list[j]
            print(source, ' , #articles : ', len(articles[source]))
            sentences = []

            print('Extract sentences...')
            for text in list(set(articles[source])):
                ext_sentences = extractor.split_into_sentences(text)
                sentences.extend(ext_sentences)

            print('Find entity specific sentences...')
            onTargetArticles, byTargetArticles, removedArticles = entitySpecificCoverageAnalysis \
                (sentences, aliases[i], entity_name, new_entity_alias)

            print('Print about and by entity sentences to file...')
            about_sent, by_sent = printToFile(onTargetArticles, byTargetArticles,
                                              entity_name, short_sources_list[j])
            printRemovedToFile(entity_name, short_sources_list[j], removedArticles)

            about_entity_coverages[source][entity_name] += len(onTargetArticles)
            about_cov = about_cov + len(onTargetArticles)
            by_entity_coverages[source][entity_name] += len(byTargetArticles)
            by_cov = by_cov + len(byTargetArticles)

            entity_coverage_file.write(str(about_entity_coverages[source][entity_name]) + '\t' + str(about_sent) + '\t')
            entity_coverage_file.write(str(by_entity_coverages[source][entity_name]) + '\t' + str(by_sent) + '\t')

            print(entity_name, source, "done...")
            print('About: ', len(onTargetArticles), ' By: ', len(byTargetArticles), 'Removed: ', len(removedArticles))
            print('About: ', about_sent, ' By: ', by_sent, '\n')
        entity_coverage_file.write(str(about_cov) + '\t' + str(by_cov) + '\n')
        entity_total_coverage_file.write(str(about_cov) + '\t' + str(by_cov) + '\n')
        print('\n total about: ' + str(about_cov) + '\t total by: ' + str(by_cov))
        print(entity_name, "done...")

    indices.sort()
    indices = np.array(indices)
    count_array = [i for i in range(100)]
    count_array = np.array(count_array)
    indices_covered = indices[np.where(indices < 100)[0]]
    result_list = np.setdiff1d(count_array, indices_covered)  # list(set(count_array) - set(indices_covered))
    print(indices_covered)
    file_handle = open('input_to_top100_farmers_opinion', 'w')
    for ele in result_list:
        file_handle.write(str(ele) + '\n')
    file_handle.close()
    # print('Indices covered: \n', )
    entity_index_file.close()
    entity_coverage_file.close()
    entity_total_coverage_file.close()


def findEntityWithGivenIndex(userlist_file):
    data = np.genfromtxt(userlist_file, delimiter='\t', dtype=str)
    if data.size < 2:
        print('Given input file is empty or is invalid. Check again')
        return
    elif data.size == 2:
        indices = [data.astype(np.int)]
        if start_ind != end_ind:
            print('Given start and end index is invalid as given file has only 1 entity. \n Check again')
            return
    else:
        indices = data.astype(np.int)

    print('Read all the Entites')
    entities = top_n_entities.get_top_n_entities(collection, N, entity_types)

    e_names = []
    e_aliases = []
    e_articleIds = []

    file_all_entity_names = None
    if not do_exist_all_entity_names:
        file_all_entity_names = open(all_entity_names_fname, 'w')
    for type in entities.keys():
        for entity in entities[type]:
            e_names.append(entity['name'])
            e_aliases.append(entity['aliases'])
            e_articleIds.append(entity["articleIds"])
            if not do_exist_all_entity_names:
                file_all_entity_names.write(str(entity['name']) + '\t' + str(entity['coverage']) + '\n')

    if not do_exist_all_entity_names:
        file_all_entity_names.close()
        print('Created file with all entity names')
    print("Read all top entities...")

    print("Now write the aliases...")
    alias_file = alias_folder + '/elite_alias_file_' + str(start_ind) + 'to' + str(end_ind) + '.txt'
    fwrite_alias = open(alias_file, 'w')

    for ind in range(start_ind, end_ind + 1):
        fwrite_alias.write(str(','.join(e_aliases[indices[ind]])) + '\n')
    fwrite_alias.close()

    print("Aliases written to " + alias_file + " Please modify the aliases, leave blank line in case of no alias.")
    print("Press Ctrl+X when done...")
    os.system('nano ' + alias_file)
    print('Edited file now. Press enter')
    input()

    # main output file
    fname = res_folder_main + 'entity_coverage_results_' + str(start_ind) + 'to' + str(end_ind) + '.txt'
    entity_coverage_file = open(fname, 'w')
    entity_coverage_file.write('Entity\t')
    for source in sources_list:
        entity_coverage_file.write(source + '\t\t\t\t')
    entity_coverage_file.write('Total_about \t Total_by \n')

    fwrite_temp = open(alias_file, 'r')
    about_entity_coverages = {s: defaultdict(int) for s in sources_list}
    by_entity_coverages = {s: defaultdict(int) for s in sources_list}
    aliases = [list(map(str.lower, map(str.strip, line.split(",")))) for line in fwrite_temp]
    print(aliases)
    print("Read all aliases of all entities...")

    alias_ind = 0

    for ii in range(start_ind, end_ind + 1):
        i = indices[ii]
        entity_name = e_names[i]
        is_power_elite = str(0)
        pe_group = ''
        print(entity_name, ii, i, len(e_articleIds[i]))
        print(entity_name + ' STARTING ...')
        entity_coverage_file.write(entity_name + '\t' + is_power_elite + '\t' + pe_group + '\t')

        articles = {s: [] for s in sources_list}
        new_entity_alias = e_aliases[i]
        print(entity_name + ' : READ ARTICLE IDS ...')

        for article_id in e_articleIds[i]:
            article = art_collection.find({"_id": article_id})[0]
            text = article["text"]
            source = article["sourceName"]
            if(source in sources_list or source in short_sources_list):
            	articles[source].append(text)

        print(entity_name + ' : Compute for each SOURCE ...')
        about_cov = 0
        by_cov = 0

        for j in range(len(sources_list)):
            source = sources_list[j]
            print(source, ' , #articles : ', len(articles[source]))
            sentences = []

            print('Extract sentences...')
            for text in list(set(articles[source])):
                ext_sentences = extractor.split_into_sentences(text)
                sentences.extend(ext_sentences)

            print('Find entity specific sentences...')
            onTargetArticles, byTargetArticles, removedArticles = entitySpecificCoverageAnalysis(
                sentences, aliases[alias_ind], entity_name, new_entity_alias)

            print('Print about and by entity sentences to file...')
            about_sent, by_sent = printToFile(onTargetArticles, byTargetArticles,
                                              entity_name, short_sources_list[j])
            printRemovedToFile(entity_name, short_sources_list[j], removedArticles)

            about_entity_coverages[source][entity_name] += len(onTargetArticles)
            about_cov = about_cov + len(onTargetArticles)
            by_entity_coverages[source][entity_name] += len(byTargetArticles)
            by_cov = by_cov + len(byTargetArticles)

            entity_coverage_file.write(str(about_entity_coverages[source][entity_name]) + '\t' + str(about_sent) + '\t')
            entity_coverage_file.write(str(by_entity_coverages[source][entity_name]) + '\t' + str(by_sent) + '\t')

            print(entity_name, source, "done...")
            print('About: ', len(onTargetArticles), ' By: ', len(byTargetArticles), 'Removed: ', len(removedArticles))
            print('About: ', about_sent, ' By: ', by_sent, '\n')
        entity_coverage_file.write(str(about_cov) + '\t' + str(by_cov) + '\n')
        print(entity_name, "done...")
        alias_ind = alias_ind + 1
    entity_coverage_file.close()


if __name__ == '__main__':
    if findPowerElite:
        findPowerEliteInfo()
    elif findEntityWithIndex:
        '''
            ifile format:
                <name> \t <index>
            This file needs to be created manually.
        '''
        ifile = input('Enter filename (current directory) which contains list of Entites and their indices: \n')
        findEntityWithGivenIndex(ifile)

'''
    {
        "name":obj['stdName'],
        "coverage":obj['num'],
        "aliases":obj['aliases'],
        "articleIds":obj['articleIds']
    }
'''
'''
text = "note ban a foolish decision, paytm means pay to modi, says rahul gandhi economictimes"
pos_text = sNLP.pos(text);
parse_text = sNLP.dependency_parse(text)
for i in range(1,len(parse_text)):
  print(parse_text[i][0] + ' ' + pos_text[parse_text[i][1]-1][0] + ' ' + pos_text[parse_text[i][2]-1][0] )
'''

import rake
import json, nltk, math, os
import numpy as np
from ExtractSentences import ExtractSentences
from pymongo import MongoClient
from collections import Counter, defaultdict
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize, sent_tokenize
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from stanfordcorenlp import StanfordCoreNLP
nltk.download('punkt')
nltk.download('wordnet')

###################################################################################################
# INPUTS
# 'Enter the collection name here:  '
coll_name = 'demon_opinion'

# 'Enter all aliases sepeated by comma of the topic : ' >> These are search keywords
# aliases = ['Aadhar Card', 'UIDAI', 'aadhar', 'aadhar card', 'aadhaar', 'aadhaar card', 'AADHAR CARD', 'adhar card', 'adharcard', 'adhar', 'ADHAR']
# aliases = ['Goods and Services Tax', 'Goods & Services Tax', 'the GST']
aliases = ['demonitis ',' demonitiz ',' denomination note ',' cash withdrawal ',' swipe machine ',' unaccounted money ',' withdrawal limit ',' pos machine ',' fake currency ',' digital payment ',' digital transaction ',' cash transaction ',' cashless economy ',' black money ',' cash crunch ',' currency switch ',' long queue ',' demonetised note ',' cashless transaction ',' note ban ',' currency switch']
#aliases = ['e-governance','information and communication technologies','ICT', 'e-govt', 'e-government', 'electronic governance', 'paperless office']

# 'Enter the fixword that u want aliases to be replaced with '
fix_keyword = 'demon'

###################################################################################################
# OUTPUT
res_folder = './OUTPUT'
os.system('mkdir -p ' + res_folder.lstrip('./'))

ofile1 = open(res_folder + '/' + str(fix_keyword) + '_removed_articles',
              'w')  # it will have (url,text) of articles that are not related to our topic
ofile2 = open(res_folder + '/' + str(fix_keyword) + '_keywords.txt',
              'w')  # it will contain top words measure by rake's relevance of each article
ofile3 = open(res_folder + '/' + str(fix_keyword) + '_top100_words', 'w')  # it will contain top 100 frequent words in all articles

ofile4 = open(res_folder + '/' + str(fix_keyword) + 'Lemmatised_articles.txt', 'w')


###################################################################################################

class StanfordNLP:
    def __init__(self, host='http://localhost', port=9000):
        self.nlp = StanfordCoreNLP(host, port=port,
                                   timeout=30000)  # , quiet=False, logging_level=logging.DEBUG)
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


def isArticleRelatedToTopic(article, aliases, keyword):
    # step0: split into sentences
    extractor = ExtractSentences()
    sent_text = np.array(extractor.split_into_sentences(article))

    # step1 lower the text
    text = article.lower()
    keyword = keyword.lower()

    # step2 replace aliases from keyword
    for a in aliases:
        text = text.replace(str(' ') + a.lower() + str(' '), str(' ') + keyword + str(' '))

    # accept article that have keyword's frequency greater than freq_threshold
    freq_threshold = 2
    key_freq = 0
    for word in nltk.word_tokenize(text):
        if word == keyword:
            key_freq = key_freq + 1

    if key_freq > freq_threshold:
        print('\tKeyword frequency ', key_freq)
        return True

    # accept the articles where keyword is in top line_threshold lines
    occ_threshold = 0.5
    sent_text = np.atleast_1d(sent_text)
    top_sent = sent_text[:int(math.ceil(occ_threshold * len(sent_text)))]
    for ts in top_sent:
        ts = ts.lower()
        ts = ts.replace('.', ' ').replace(',', ' ').replace('-',' ')
        for a in aliases:
            ts = ts.replace(str(' ') + a.lower() + str(' '), str(' ') + keyword + str(' '))
        if keyword in nltk.word_tokenize(ts):
            print('\t top 50% lines')
            return True

    # accept if keyword is present in any of selected relations
    sNLP = StanfordNLP()
    try:
        pos_text = sNLP.pos(text)
        parse_text = sNLP.dependency_parse(text)
        selected_relation = ['amod', 'nmod', 'dobj', 'iobj', 'nsubj', 'nsubjpass']

        for i in range(1, len(parse_text)):
            rel = parse_text[i][0]
            word1 = pos_text[parse_text[i][1] - 1][0]
            word2 = pos_text[parse_text[i][2] - 1][0]
            if (word1 == keyword or word2 == keyword) and (rel in selected_relation):
                print('\t passed NLP')
                return True
    except json.decoder.JSONDecodeError as e:
        print(e)
        print(text)
    # reject
    return False


# ---------- Fixed Params ------------
client = MongoClient('mongodb://act4dgem.cse.iitd.ac.in:27017/')
my_db = client['eventwise_media-db']
rake_object = rake.Rake("./SmartStoplist.txt", 3, 5, 1)  # min no. of chars=3, max no. of words=5, min frequency=1
wordnet_lemmatizer = WordNetLemmatizer()
# -----------------------------------
coll = my_db[coll_name]

rem_coll_name = coll_name + '_removed'
rem_coll = my_db[rem_coll_name]

print('Fetch the related article text..')
cursor = coll.find({})
arr = []
removed_art_urls = []

removed_count = 0
selected_count = 0
count = 0
for art in cursor:
    text = art['text']
    count = count + 1
    print(count, ": total_count")
    if not isArticleRelatedToTopic(text, aliases, fix_keyword):
        url = art['articleUrl']
        ofile1.write(str(url) + '\t\t' + str(text) + '\n')
        removed_count = removed_count + 1
        removed_art_urls.append(url)
        print(removed_count, " : removed_count")
        # coll.remove({'articleUrl':url})
        continue
    # print(text)
    sentences = sent_tokenize(text)
    new_text = ''
    for sent in sentences:
        line = ''
        for word in word_tokenize(sent):
            tword = wordnet_lemmatizer.lemmatize(word)
            if len(tword) > 2:
                line = line + ' ' + tword
        new_text = new_text + line + ' . '
    arr.append(new_text)
    ofile4.write(text + '\n')
    selected_count = selected_count + 1
    print(selected_count, ": selected_count")

ofile4.close()
ofile1.close()
print('Total articles: ', selected_count)
print('#Removed articles: ', removed_count)

removed_art_urls = list(set(removed_art_urls))
print('Moving removed articles to different db, # : ',len(removed_art_urls))
count  = 0
for url in removed_art_urls:
    docs = coll.find_one({'articleUrl': url.strip()})
    count = count + 1
    if docs:
        rem_coll.insert_one(docs)
        coll.delete_many({'articleUrl': url})
        print(count, ' : Deleted')
    else:
        print(count)

'''
	x = coll.find({'articleUrl': { '$in' : removed_art_urls}})
	rem_coll.insert(x)
	coll.find_one_and_delete({'articleUrl': { '$in' : removed_art_urls}})
'''

print('\nFinding the keywords..')
keywords = {}

for ind, text in enumerate(arr):
    list_keyword = rake_object.run(text)
    json_keywords = [{'text': k[0], 'relevance': k[1]} for k in list_keyword]
    relevance = 0.0
    new_keywords = []
    if (len(json_keywords) >= 1):
        relevance = 20.0 / 100 * json_keywords[0]['relevance']
    for item in json_keywords:
        if (item['relevance'] >= relevance):
            new_keywords.append(str(item['text']) + ',' + str(item['relevance']))
            if len(item['text'].split()) <= 2:
                if item['text'] not in keywords:
                    keywords[item['text']] = 0.0
                keywords[item['text']] = keywords[item['text']] + float(item['relevance'])
    ofile2.write('\t'.join(new_keywords) + '\n')
ofile2.close()

print('Finding the top 100 keywords..')
sorted_keys = sorted(keywords, key=keywords.get, reverse=True)
for k in sorted_keys[:100]:
    ofile3.write(str(k) + '\n')
ofile3.close()


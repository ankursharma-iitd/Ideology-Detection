import numpy as np
import os
import sys
import subprocess
import shlex
import scipy.stats as stats
import matplotlib.pyplot as plt
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

'''
take two input in console
    Folder name which has files having text by entity
'''

tag = 'fp'
folder = './bytop20_' + tag + '_opinion'
persons_list = []
person_names = os.listdir(folder)
for name in person_names:
    x = name.split('_')[1:][:-1]
    pname = ' '.join(x)
    persons_list.append(pname)

persons_list = list(set(persons_list))
print(len(persons_list))
short_sources_list = ["Hindu", "TOI", "HT", "IE", "DecH", "Telegraph", "NIE"]


def findSentiment(tweet_text):
    analyzer = SentimentIntensityAnalyzer()
    sentiment = analyzer.polarity_scores(tweet_text)
    a_compound_sent = (sentiment["compound"])
    a_neg_sent = (sentiment["neg"])
    a_pos_sent = (sentiment["pos"])
    return (a_compound_sent, a_pos_sent, a_neg_sent)


ofile = open('Entity_wise_polarity_' + tag + '_opinion.csv', 'w')
ofile.write('Entity_name;aggr_sent;total_pos;total_neg;polarity\n')
delimiter = ';'

for person in persons_list:
    person_temp = person
    person = person.replace(' ', '_')
    total_sent = 0
    total_num = 0
    total_pos = 0
    total_neg = 0
    polarity = 0
    for source in short_sources_list:
        file_to_extract = folder + '/' + 'by_' + person + '_' + source + '.txt'
        print(file_to_extract)
        if not os.path.isfile(file_to_extract): 
            print('file not found')
            continue
        data = np.genfromtxt(file_to_extract, dtype='str', delimiter=';;')
        if data[0].size ==1:
            by_sentence_list = data[2]
        else:
            by_sentence_list = data[:,2]
        #total_pos = 0
        #total_neg = 0
        for sentence in by_sentence_list:
            # sentence = (bsl.split(';;'))[2]
            compound, pos, neg = findSentiment(sentence)
            if compound >= 0.05 or compound <= -0.05:
                total_sent = total_sent + compound
                total_num = total_num + 1
                total_pos = total_pos + pos
                total_neg = total_neg + neg
        #total_pos = total_pos + total_pos
        #total_neg = total_neg + total_neg
        if total_sent > 0 :
            if  total_neg:
                polarity = total_pos/total_neg
            else:
                polarity = total_pos
        elif total_sent < 0:
            if  total_pos:
                polarity = total_neg/total_pos
            else:
                polarity = total_neg
    ofile.write(format(person_temp) + delimiter + format(total_sent) + delimiter+ format(total_pos)+delimiter+ format(total_neg)+delimiter + format(polarity) + '\n')
    ofile.flush()
ofile.close()

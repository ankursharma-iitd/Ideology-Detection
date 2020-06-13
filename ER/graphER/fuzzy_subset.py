from pyjarowinkler import distance
from fuzzywuzzy import fuzz 
from fuzzywuzzy import process


def sortAbbr(string):
    s_l=string.split(' ')
    sorted_name_l=sorted(s_l)
    initials_l=[]
    for item in s_l:
        initials_l.append(item[0])
    sorted_initials_l=sorted(initials_l)
    return sorted_name_l,sorted_initials_l
    
def sim(item1,item2):
    #Extract sorted initials from the names
    sorted_name_item1_l,sorted_initials_item1_l=sortAbbr(item1)
    sorted_name_item2_l,sorted_initials_item2_l=sortAbbr(item2)
    sorted_initials_item1_l=set(sorted_initials_item1_l)
    sorted_initials_item2_l=set(sorted_initials_item2_l)
    d=0
    #If the two sets are subsets
    if sorted_initials_item1_l.issubset(sorted_initials_item2_l) or\
    sorted_initials_item2_l.issubset(sorted_initials_item1_l):
        sn1=' '.join(sorted_name_item1_l)
        sn2=' '.join(sorted_name_item2_l)
        d=distance.get_jaro_distance(sn1,sn2)
    return d

def fuzzySubset(set1,set2):
    #Store the larger set in s1 and smaller in s2
    if len(set1)>=len(set2):
        s1=set1
        s2=set2
    else:
        s1=set2
        s2=set1
    #Subset checking: if for even one element of smaller set,
    #no element in larger set matches, s2 is not a subset.
    large_str = ' '.join(sorted(s1))

    maxMatches = len(s2)
    totalMatches = 0
    
    for item1 in s2:
        # flag=0
        # for item2 in s1:
        if fuzz.partial_ratio(large_str, item1) < 70:
            totalMatches += 1
    #print("tot,max  ",totalMatches,maxMatches)
    if(totalMatches >= maxMatches/2):
        return True
    else:
        return False



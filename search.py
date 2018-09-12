import os
import sys
import re
from math import *
from stemming.porter import stem
import threading
import time
reload(sys)

stemmer={}
stopwords_filename="stopwords.txt"
def create_stopwords(filename):
    dic={}
    with open(filename,'r')as f:
        for line in f:
            words=line.split()
            dic[ words[0]]=True
    return dic

def title_extraction(filename):
    title={}
    with open(filename,'r') as title_file:
        for row in title_file:
            row=row.split(': ')
            if(len(row)<3):
                continue
            title[row[0]]=[row[1], row[2]]
    return title
def load_secondary_index(filename):
    secondary_index=[]
    fle=open(filename,'r')
    for row in fle:
        secondary_index.append(row)
    fle.close()
    return secondary_index

def remove_stopword(line):
    global stopwords
    result=[]
    for word in line:
        if(word not in stopwords):
            result.append(word)
    return result

def query_processing(query):

    queryb= []
    queryc= []
    queryt= []
    queryi= []
    query=query.split(' ')
    last='b'
    for word in query:
        if(word=='i:'):
            last='i'
        elif(word=='t:'):
            last='t'
        elif(word=='b:'):
            last='b'
        elif(word=='c:'):
            last='c'
        else:
            if(last=='i'):
                queryi.append(word)
            elif(last=='b'):
                queryb.append(word)
            elif(last=='c'):
                queryc.append(word)
            elif(last=='t'):
                queryt.append(word)

    if(queryt):
        queryt=remove_stopword(queryt)
    if(queryc):
        queryc=remove_stopword(queryc)
    if(queryb):
        queryb=remove_stopword(queryb)
    if(queryi):
        queryi=remove_stopword(queryi)

    return [queryt,queryi,queryc,queryb]

def findi(query,secondary_index,number_of_files):
    global title
    rank=dict()
    answer=[]
    global indexing_file
    i=0
    for row in secondary_index:
        if(i>number_of_files):
            break
        if(query<=row):

            fle=open(indexing_file+str(i))
            for word in fle:
                words= re.split('[=:-]',word)
                if(words[0]==query):
                    j=1
                    idf=log10(number_of_files/(len(words)/2 + 1.0) )
                    while(j+1<len(words)):

                        if(query[-1]=='p'):
                            file_length=None
                            if(len(title[words[j]][1].split(':'))==2):
                                file_length=int(title[words[j]][1].split(':')[1][:-1])
                                rank[int(words[j])]=log10(float(words[j+1])/file_length+1)*idf
                            else:
                                rank[int(words[j])]=log10(float(words[j+1])/100+1)*idf

                        else:
                            rank[int(words[j])]=log10(float(words[j+1])/100+1)*idf
                        j+=2
                elif(words[0]>query):
                    break


            return rank

        i+=1

def query_search(queries,secondary_index,number_of_files):
    ranking={}
    queries[3]= re.sub(r"[-.]",'',str(queries[3]))
    queries[2]= re.sub(r"[-.]",'',str(queries[2]))
    queries[1]= re.sub(r"[-.]",'',str(queries[1]))
    queries[0]= re.sub(r"[-.]",'',str(queries[0]))
    queryb= re.split('[^a-zA-Z0-9]',str(queries[3]))
    queryc= re.split('[^a-zA-Z0-9]',str(queries[2]))
    queryi= re.split('[^a-zA-Z0-9]',str(queries[1]))
    queryt= re.split('[^a-zA-Z0-9]',str(queries[0]))

    if(queryb):
        for word in queryb:
            if(word==''):
                continue
            temp=word.lower()
            word=stem(word)+";p"
            rank=None
            #print word
            rank=findi(word,secondary_index,number_of_files)
            for row in rank:
                if(row in ranking):
                    ranking[int(row)]+=rank[int(row)]
                else:
                    ranking[int(row)]=rank[int(row)]


            leng={}
            word=stem(temp)+";t"
            rank={}
            rank=findi(word,secondary_index,number_of_files)

            for row in rank:
                if(row in ranking):
                    ranking[int(row)]+=log10(30)*rank[int(row)]
                else:
                    ranking[int(row)]=log10(30)*rank[int(row)]
            word=stem(temp)+";i"
            rank={}
            rank=findi(word,secondary_index,number_of_files)
            for row in rank:
                if(row in ranking):
                    ranking[int(row)]+=rank[int(row)]
                else:
                    ranking[int(row)]=rank[int(row)]

    if(queryc):
        for word in queryc:
            if(word==''):
                continue
            word=stem(word.lower())+";t"
            leng=findi(word,secondary_index,number_of_files)
            for row in leng:
                if(row in ranking):
                    ranking[row]+=leng[row]
                else:
                    ranking[row]=leng[row]

    if(queryt):
        for word in queryt:
            if(word==''):
                continue
            word=stem(word.lower())+";t"
            leng=findi(word,secondary_index,number_of_files)
            for row in leng:
                if(row in ranking):
                    ranking[row]+=leng[row]
                else:
                    ranking[row]=leng[row]


    if(queryi):
        for word in queryi:
            if(word==''):
                continue
            word=stem(word.lower())+";i"
            #print word
            leng=findi(word,secondary_index,number_of_files)
            for row in leng:
                if(row in ranking):
                    ranking[row]+=leng[row]
                else:
                    ranking[row]=leng[row]


    return ranking


stopwords_filename= "stopwords.txt"
title_name="title_file.txt"
secondary_index_file_name="secondary_indexing.txt"
stopwords=create_stopwords(stopwords_filename)
title=title_extraction(title_name)
secondary_index=load_secondary_index(secondary_index_file_name)
number_of_files=len(secondary_index)
input_query_file=sys.argv[2]
indexing_file=sys.argv[1]
fle=open(input_query_file,'r')
for query in fle:
    print "Results for "+query[:-1]+ " is:"
    query=query_processing(query)

    result=query_search(query,secondary_index,number_of_files)
    i=0
    if(result==None or result==[] or result=={}):
        print("No relevant result found. Try again by changing it\n")
        continue
    #print result
    for key,value in sorted(result.iteritems(),reverse= True):
        if(i>=10):
            break
        i+=1
        print str(key) + ": "+ title[str(key)][0]
    print("\n")

fle.close()

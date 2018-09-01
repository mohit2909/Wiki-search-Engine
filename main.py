import xml.etree.ElementTree as ET
import os
import sys
import re

from stemming.porter import stem
import threading


reload(sys)
if(len(sys.argv)!=3):
    print("Insufficient number of arguments")
    exit()
stemmer={}
file_name=sys.argv[1]
output_file=sys.argv[2]
length={}
class compute_parallel(threading.Thread):
    def __init__(self,text,stopwords,id):
        threading.Thread.__init__(self)
        self.text=text
        self.stopwords=stopwords
        self.id=id
        #self._return = None

    def stemming(self,low,res):
        res[0]=stem(low)

    def run(self):
            id=str(self.id)
            global word_dict
            global categ_dict
            global stemmer
            stopwords=self.stopwords
            text= re.split("[^a-zA-Z]+", self.text)
            category_text= re.findall('Category:[a-zA-Z ]+',self.text)
            dictionary={}

            for row in text:
                row= re.sub(r'^https?:\/\/.*[\r\n]*', '', row, flags=re.MULTILINE)
                low= row.lower()
                if(low not in stopwords):
                    stems=None
                    if(low in stemmer):
                        stems=stemmer[low]
                    else:
                        stems=stem(low)
                        stemmer[low]=stems

                    if(stems in dictionary):
                        dictionary[stems]+=dictionary[stems]+1
                        #print(stems)
                        #print(dictionary[stems])
                    else:
                        dictionary[stems]=1

            length[str(id)]=len(text)
            text= re.split("[^0-9]+", self.text)
            for row in text:
                low= row.lower()
                if(low not in stopwords):
                    stems=None
                    if(low in stemmer):
                        stems=stemmer[low]
                    else:
                        stems=stem(low)
                        stemmer[low]=stems

                    if(stems in dictionary):
                        dictionary[stems]+=dictionary[stems]+1

                    else:
                        dictionary[stems]=1

            if(length[str(id)]):
                length[str(id)]+=len(text)
            cat={}
            for word in category_text:
                words= word.split(":")
                low= words[1].lower()
                '''if(low not in stopwords):

                    stems= stem(low)
                    if(stem in cat):
                        cat[stems]+=1
                    else:
                        cat[stems]=1
                '''
                word=re.split("[: ]",word)

                for sub_word in word:
                    if(sub_word != "Category"):
                        low= sub_word.lower()

                        if(low not in stopwords):
                            stems=None
                            if(low in stemmer):
                                stems=stemmer[low]
                            else:
                                stems=stem(low)
                                stemmer[low]=stems
                            if(stems in cat):
                                cat[stems]+=1
                            else:
                                cat[stems]=1

            for row in dictionary:
                if(row==None or row==''):
                    continue
                if(row+ ';p' not in word_dict):
                    word_dict[row+ ';p']= str(id) + '-' + str(dictionary[row]) + ':'
                else:
                    word_dict[row+';p']= word_dict[row+';p'] + str(id) + '-' + str(dictionary[row]) + ':'
            for row in cat:
                if(row==None or row==''):
                    continue
                if( row+';c' not in word_dict):
                    word_dict[row+';c']= id+ '-' + str(cat[row]) + ':'
                else:
                    word_dict[row+';c']= word_dict[row+ ';c']+ id+ '-' + str(cat[row]) + ':'

            cat={}
            dictionary={}


categ_dict={}
word_dict={}
thread=[]
allowed_docs=100
count_docs=0;
#file_name="./wiki.xml"
stopwords_filename="stopwords.txt"
def strip_tag_name(t):
    t = elem.tag
    idx = k = t.rfind("}")
    if idx != -1:
        t = t[idx + 1:]
    return t


def create_stopwords(filename):
    dic={}
    with open(filename,'r')as f:
        for line in f:
            words=line.split()
            dic[ words[0]]=True
    return dic

def sorted_file(fl1,fl2):
    fle2=open("Chunk"+str(fl2),"r")
    fle1=open("Chunk"+str(fl1),"r")
    fle=open("temp","w")
    line1=fle1.readline()
    line2=fle2.readline()
    j=0
    while line1 and line2:
        #print(j)
        word1=line1.split(":")
        word2=line2.split(":")
        if(word1[0]==word2[0]):
            tp=line1[:-1]
            #print(line1[:-1],end=" ")
            leng= len(word2)

            '''i=0

            for row in word1:
                if(i==leng-1):
                    break
                tp=tp+row+":"
                i+=1
            '''
            i=0
            for row in word2:
                if(i==0 or i ==leng-1):
                    i+=1
                    continue
                tp=tp+ str(word2[i])+":"

                i+=1
            tp=tp+"\n"
            fle.write(tp)
            line1=fle1.readline()
            line2=fle2.readline()
        elif(word1[0]<word2[0]):
            fle.write(line1)
            #print(line1)
            line1= fle1.readline()

        elif(word1[0]> word2[0]):
            fle.write(line2)
            line2= fle2.readline()
        j+=1

    while(line1):
        fle.write(line1)
        line1= fle1.readline()

    while(line2):
        fle.write(line2)
        line2= fle2.readline()

    fle1.close()
    fle2.close()
    fle.close()
    os.remove("Chunk"+str(fl1))
    os.remove("Chunk"+str(fl2))
    os.rename("temp", "Chunk"+ str(fl1) )
    return fl1

def merge(l,r):
    mid=l+ (r-l)/2;
    if(l==r):
        return l
    #print(l,mid,r)
    fl1=merge(l,mid);
    fl2=merge(mid+1,r);
    fl=sorted_file(fl1,fl2)
    return fl
title_dict={}
title_st={}
chunk=0
start="start"
end="end"
map_title={}
revision=False
stopwords= create_stopwords(stopwords_filename)
i=1
j=0
for event, elem in ET.iterparse(file_name, events=(start, end)):
    tname = strip_tag_name(elem.tag);

    if(event == start):
        if(tname =="page"):
            dictionary= {}
            category_dict={}
            revision= False
            ns=0
        elif(tname == "revision"):
            revision= True
        elif(tname == "redirect"):
            redirect_title= str(elem.attrib)
            if(redirect_title is None):
                pass
            else:
                titles=redirect_title[11: -2]
                #print(redirect_title)
                if(titles in title_dict):
                    title_dict[titles]+=1
                else:
                    title_dict[titles]=1
            text= re.split("[^a-zA-Z]+", str(elem.attrib))
            temp={}
            for row in text:
                low=row.lower()
                if(low not in stopwords):
                    stems=None
                    if(low in stemmer):
                        stems=stemmer[low]
                    else:
                        stems=stem(low)
                        stemmer[low]=stems
                    if(stems in temp):
                        temp[stems]+=1
                    else:
                        temp[stems]=1

            for row in temp:
                if(row==None or row==''):
                    continue
                if(row+ ';t' not in word_dict):
                    word_dict[row + ';t']=  str(id) + '-' + str(temp[row]) + ':'
                else:
                    word_dict[row + ';t']= word_dict[row+';t'] + str(id) + '-' + str(temp[row]) + ':'

            temp={}


    else:
        if(tname == "title"):
            title= elem.text
        elif(tname == "id" and not revision ):
            id= elem.text
            map_title[title]=id
        elif(tname == "text"):
            text= elem.text
            if(text is not None):
                thread.append(compute_parallel(text,stopwords,id))
                thread[-1].run()
                j+=1;

            print(i)
            i+= 1
            count_docs+=1
            elem.clear()

        elif(tname=="ns"):
            ns=elem.text

        elif(tname=="page"):
            if(count_docs==allowed_docs):
                count_docs=0
                chunk+=1
                fle=open("Chunk"+str(chunk),"w+")
                for key in sorted(word_dict):
                    fle.write(key.decode("utf-8").encode("utf-8")+":" +word_dict[key].encode("utf-8") +'\n')
                fle.close()
                word_dict={}
                stemmer={}

        if(i>=500):
            break

chunk+=1
fle=open("Chunk"+str(chunk),"w+")
for key in sorted(word_dict):
    fle.write(key.decode("utf-8").encode("utf-8")+":" +word_dict[key].encode("utf-8") +'\n')
fle.close()
#merge()
#print(title_dict)
#output_file="output_file.txt"
num=merge(1,chunk)
os.rename("Chunk"+str(num),output_file)
'''
fle=open(output_file,"w+")

for i in range(1,chunk+1):
    fl=open("Chunk"+str(i),"r")
    for row in fl:
        #print(row)
        fle.write(row)
    fl.close()
fle.close()
'''
fle=open("title_file.txt","w+")
for row in map_title:
    if(row in title_dict):
        fle.write(str(map_title[row]).encode("utf-8")+ ": " + str(row.encode("utf-8")) + ": " +str(title_dict[row])+": ")
        if(length[map_title[row]):
            fle.write(str(length[map_title[row]])+"\n")
        else:
            fle.write("0\n")
    else:
        fle.write(str(map_title[row]).encode("utf-8")+ ": " + str(row.encode("utf-8")) + ": 0 :")
        if(length[map_title[row]):
            fle.write(str(length[map_title[row]])+"\n")
        else:
            fle.write("0\n")

fle.close()

word_dict={}
categ_dict={}

stopwords=[]
map_title={}
title_dict={}
stemmer={}
title_st={}

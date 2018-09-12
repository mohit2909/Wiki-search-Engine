import xml.etree.ElementTree as ET
import os
import sys
import re
import Stemmer
from stemming.porter import stem
import threading
from nltk.stem.porter import *

reload(sys)


if(len(sys.argv)!=3):
    print("Insufficient number of arguments")
    exit()
def is_ascii(s):
    return all(ord(c) < 128 for c in s)
stemmer={}
file_name=sys.argv[1]
output_file=sys.argv[2]
length={}
class compute_parallel():
    def __init__(self,text,stopwords,id):
        self.text=text
        self.stopwords=stopwords
        self.id=id
        #self._return = None


    def run(self):
            id=str(self.id)
            global word_dict
            global categ_dict
            stopwords=self.stopwords
            #self.text=re.sub(r'^https?:\/\/.*[\r\n]*', '', self.text, flags=re.MULTILINE)
            text= re.sub(r"[-.]",'',self.text)
            text= re.split("[^A-Za-z]", self.text)
            info_text=re.findall(r'{{Infobox+([\w\W]*)[$}]',self.text)
            info_text=re.sub(r'[-.]', '', str(info_text), flags=re.MULTILINE)
            info_text= re.split("[^0-9A-Za-z]", str(info_text))

            #category_text= re.findall('Category:[a-zA-Z ]+',self.text)
            dictionary={}

            for row in text:
                row=row.split(' ')
                for word in row:
                    low= word.lower()
                    if(low not in stopwords):
                        stems=None
                        if(low in stemmer):
                            stems=stemmer[low]
                        else:
                            stems=stem(low)
                            stemmer[low]=stems

                        if(stems in dictionary):
                            dictionary[stems]+=1

                        else:
                            dictionary[stems]=1

            length[str(id)]=len(text)
            text= re.split("[^0-9]+", self.text)
            for row in text:
                low= row
                if(low not in stopwords and len(row)<4):
                    stems=low
                    if(stems in dictionary):
                        dictionary[stems]+=dictionary[stems]+1

                    else:
                        dictionary[stems]=1

            info_dictionary={}

            for word in info_text:
                low=word.lower()
                if(low=="n"):
                    continue
                if(low not in stopwords):
                    stems=None
                    if(low in stemmer):
                        stems=stemmer[low]
                    else:
                        stems=stem(low)
                        stemmer[low]=stems

                    if(stems in info_dictionary):
                        info_dictionary[stems]+=1

                    else:
                        info_dictionary[stems]=1

            if(length[str(id)]):
                length[str(id)]+=len(text)

            for row in info_dictionary:
                if(row==None or row==''):
                    continue

                if(row+ ';i' not in word_dict):
                    word_dict[row+';i']= str(id) + '-' + str(info_dictionary[row]) + ':'
                else:
                    word_dict[row+';i']= word_dict[row+';i'] + str(id) + '-' + str(info_dictionary[row]) + ':'


            for row in dictionary:
                if(row==None or row==''):
                    continue
                if(row+ ';p' not in word_dict):
                    word_dict[row+ ';p']= str(id) + '-' + str(dictionary[row]) + ':'
                else:
                    word_dict[row+';p']= word_dict[row+';p'] + str(id) + '-' + str(dictionary[row]) + ':'

            info_dictionary={}
            cat={}
            dictionary={}

secondary=[]
categ_dict={}
word_dict={}

allowed_docs=10000
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




def merge():
    global secondary
    global offset
    global chunk
    global numofdoc
    global output_file
    last=None
    offtxt= open("offset.txt",'w+')
    files = [open("Chunk"+str(i)) for i in range(chunk)]
    com_read=[0 for i in range(chunk)]
    lines = [j.readline() for j in files]
    k=0
    out=0
    outtxt= open(output_file+ str(out),'w+')
    while 1:
        words=[]
        postings=[]
        for i in range(len(lines)):
            if com_read[i]==0:
                #print(lines[i])
                tmp=lines[i].split('=')
                tmp1=tmp[0]
                tmp2=tmp[1]
                words.append(tmp1)
                postings.append(tmp2.rstrip())

            else:
                words.append('~')
                postings.append('~')

        mini=min(words)
        ind=[]
        for i in range(len(words)):
            if words[i]==mini and com_read[i]==0:
                ind.append(i)

        to_write=mini+'='
        to_write1=''
        for i in ind:
            to_write+=postings[i]
            line= files[i].readline()
            if not line:
                lines[i]="~"
                com_read[i]=1
            else:
                lines[i]=line

        offtet=mini+' '+str(offset)
        last=mini
        offset+=(len(to_write)+1)
        offtxt.write(offtet+'\n')
        outtxt.write(to_write+'\n');
        flag=0
        if(k%100000==0 and k!=0):
            #   print(mini)
            secondary.append(mini)
            outtxt.close()
            out+=1
            outtxt= open(output_file+ str(out),'w+')



        for i in range(chunk):
            if com_read[i]==0:
                flag=1
        if flag==0:
            break
        k+=1

    secondary.append(last)
    outtxt.close()
    offtxt.close()


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
                if(titles in title_dict):
                    title_dict[titles]+=1
                else:
                    title_dict[titles]=1
            '''
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
            '''


    else:
        if(tname == "title"):
            title= elem.text
            text=elem.text
            ''''
            if(str(text).decode("utf-8")+ ';t' not in word_dict):
                word_dict[str(text).decode("utf-8") + ';t']=  str(id) + '-' + str(1) + ':'
            else:
                word_dict[str(text).decode("utf-8") + ';t']= word_dict[str(text).decode("utf-8")+';t'] + str(id) + '-' + str(1) + ':'
            '''
            text= re.split("[^a-zA-Z]+", text)
            temp={}
            for row in text:
                if(not is_ascii(row)):
                    continue
                low=row.encode('ascii','ignore').lower()
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
        elif(tname == "id" and not revision ):
            id= elem.text
            map_title[title]=id
        elif(tname == "text"):
            text= elem.text
            if(text is not None):
                thread=compute_parallel(text,stopwords,id)
                thread.run()
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

                fle=open("Chunk"+str(chunk),"w+")
                for key in sorted(word_dict):
                    fle.write(key.encode("utf-8")+"=" +word_dict[key] +'\n')
                fle.close()
                chunk+=1
                word_dict={}
                stemmer={}



offset=0
fle=open("Chunk"+str(chunk),"w+")
for key in sorted(word_dict):
    fle.write(key.encode("utf-8"))
    fle.write("="+word_dict[key] +"\n")
fle.close()
chunk+=1

merge()
for i in range(chunk):
    os.remove("Chunk"+str(i))
fle=open("title_file.txt","w+")
for row in map_title:
    if(row in title_dict):
        fle.write(str(map_title[row]).encode("utf-8")+ ": " + str(row.encode("utf-8")) + ": " +str(title_dict[row])+": ")
        if(length[map_title[row]]):
            fle.write(str(length[map_title[row]])+"\n")
        else:
            fle.write("0\n")
    else:
        fle.write(str(map_title[row]).encode("utf-8")+ ": " + str(row.encode("utf-8")) + ": 0 :")
        if(length[map_title[row]]):
            fle.write(str(length[map_title[row]])+"\n")
        else:
            fle.write("0\n")

fle.close()
fle=open("secondary_indexing.txt",'w')
for word in secondary:
    fle.write(word+"\n")

fle.close()
word_dict={}
categ_dict={}

stopwords=[]
map_title={}
title_dict={}
stemmer={}
title_st={}

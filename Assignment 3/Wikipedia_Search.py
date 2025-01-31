import pyspark
from pyspark import SparkContext, SparkConf
from operator import add
import numpy as np

sc=pyspark.SparkContext()

# Read in all the xml wikipedia files from the directory and create an RDD
dataRDD = sc.wholeTextFiles("hdfs://elephant/user/u0941400/small_pages/*.xml").cache()#


'''Parsing function which parses the individual xml page and then extracts link, title and text content. Returns a tuple (title, (list of links, list of text)) for each page'''
import re
def xml_parsing(fileName):
    
    text_content = fileName.encode('utf-8')

    # Regular expressions to find all the links and text from XML files and storing them in two lists
    TITLE_RE = re.compile(r'<title>\s*(.+)\s*<\/title>')
    LINK_RE = re.compile(r'\[\[([^\]]+)\]')
    TEXT_RE = re.compile(r'<text.+>\s*(.+)\s*<\/text>')

    #print 'Links:' , LINK_RE.findall(text_content)
    #print 'Text: ', TEXT_RE.findall(text_content)
    title = TITLE_RE.findall(text_content)
    #Creating a tuple of links, text where each of them is a list
    tup = (LINK_RE.findall(text_content), TEXT_RE.findall(text_content))

    return " ".join(title), tup


# Parsing each page by passing it to the parsing function

contents = dataRDD.values().map(xml_parsing)

# FlatMap to get the list of all the links/page titles across all the n-pages
linkRDD = contents.flatMap(lambda x: [x[0]])


''' Writing the pagtitles to a dictionary with key being file name and value being (title, index) where index is generated by enumerator.'''

resultRDD= linkRDD.zip(dataRDD.map(lambda x : x[0]))
#resultRDD.map(lambda x: (x[0][1],x[0][0],x[1]))
linkList = sorted(resultRDD.collect())
linked={}
for i, j in enumerate(linkList):
    linked[j[1]]= (i, j[0])
# print 'Link is:', link    
import json
with open('PageTitles.json', 'w') as fp:
    json.dump(linked, fp)
    
linkListMain = sorted(linkRDD.collect())
link ={}
for i, j in enumerate(linkListMain):
	link[j]=i


#Constructing  the probability transition matrix

import numpy as np
import operator

nVector = [(0.15/len(link))]*len(link)
def documentTermVector(i, fileLinks):
    vecList = [0.0]*len(link)
    m = 0
    #print len(fileLinks)
    for each in fileLinks:
        if "|" in each:
            x = each.split("|")[0]
            try:
                if(link[x]):
                    m += 1
            except KeyError:
                continue
        else:
            try:
                #print each
                if(link[each]+1):
                    #print m
                    m += 1
            except KeyError:
                continue
    print m
    for each in fileLinks:
        if "|" in each:
            x = each.split("|")[0]
            try:
                vecList[link[x]] += 0.85/m
            except KeyError:
                continue
        else:
            try:
                vecList[link[each]] += 0.85/m
            except KeyError:
                continue
    tempList = map(operator.add, vecList,nVector)
    finallist = [[link[i], s, r] for s, r in enumerate(tempList)]
    return finallist


tdmMatrix = contents.flatMap(lambda x: documentTermVector(x[0], x[1][0]))
colMat = tdmMatrix.map(lambda x: (x[1], [x[2]])).reduceByKey(lambda p,q:p+q)

# Constructing the random surfer or v initially with equi-probabilty

randomSurfer = sc.parallelize([(1.0/len(link))]*len(link)).map(lambda x: (0, [x])).reduceByKey(lambda p,q:p+q)


# Matrix-vector multiplication to compute v'
cartRDD = colMat.cartesian(randomSurfer)                .map(lambda x: np.dot(x[0][1], x[1][1]))

#Matrix-vector multiplication to compute the next v'
cart2RDD = colMat.cartesian(cartRDD.map(lambda x: (0, [x])).reduceByKey(lambda p,q:p+q))                .map(lambda x: np.dot(x[0][1], x[1][1]))


#Computing the pageRank in a recursive function, with convergence threshold being 0.000001

pageRankDict = {}
def pageRank(vprime, vnewprime, c):
    c+=1
    norm = []
    norm = vprime.map(lambda x: (0, [x]))                 .reduceByKey(lambda p,q:p+q)                 .cartesian(vnewprime.map(lambda x: (0, [x])).reduceByKey(lambda p,q:p+q))                 .map(lambda x: np.linalg.norm((np.array(x[0][1]) - np.array(x[1][1])), ord = 2)).collect()
    n = float(norm[0])
    print "Norm is: ", c, n, type(n)
    if n <= 0.001:
        pageRankList = vnewprime.collect()
        for i, j in enumerate(pageRankList):
            pageRankDict[i]=j
        with open('PageRankResults.json', 'w' ) as fp:
            json.dump(pageRankDict, fp)
        return pageRankList
    else:
        vNew = colMat.cartesian(vnewprime.map(lambda x: (0, [x]))                     .reduceByKey(lambda p,q:p+q))                     .map(lambda x: np.dot(x[0][1], x[1][1]))
        pageRank(vnewprime, vNew, c)


#Calling the pageRank function

pg = pageRank(cartRDD, cart2RDD, 0)


#term frequency-inverse document frequency

from pyspark.mllib.feature import HashingTF


#Function top parse text and create a list of words for each file (used in search)
def text_parsing(fileName):
    text_content = fileName.encode('utf-8')

    # Regular expressions to find all the links and text from XML files and storing them in two lists
    TEXT_RE = re.compile(r'<text.+>([\s\S]*)<\/text>')
    
    liste = TEXT_RE.findall(text_content)
    str1 = re.split('[^a-zA-Z.]', liste[0].lower())
    str2 = filter (None, str1)
    return str2

splitRDD = dataRDD.values().map(text_parsing)


#Building tf-idf

hashingTF = HashingTF()
tf = hashingTF.transform(splitRDD)
from pyspark.mllib.feature import IDF

# ...from tf create IDF
tf.cache()
idf = IDF().fit(tf)
tfidf = idf.transform(tf)


zipped = splitRDD.zip(tfidf)
fRDD = splitRDD.flatMap(lambda x: x).distinct()
#print fRDD.count()


wordRDD = fRDD.map(lambda x: (x, hashingTF.indexOf(x)))
listW = wordRDD.collect()


# Writing the tf-idf values to dictionary

import json
dictW = dict(listW)

with open("wordHashD.json", 'w') as f:
    json.dump(dictW, f)




finalZip = dataRDD.map(lambda x: x[0]).zip(zipped)
# finalZip.take(2)


# Search function

def searchWords(words):
    words = words.lower()
    resultList = []
    for word in words.split(" "):
        resultList.append(finalZip.map(lambda x: (x[0], x[1][0], x[1][1]))                     .filter(lambda x: word in x[1])                     .map(lambda x: (x[2][dictW[word]], x[0]))                     .map(lambda x : (x[1], pageRankDict[linked[x[1]][0]], x[0], linked[x[1]][1]))                     .sortBy(lambda x: x[1], False)                     .sortBy(lambda x: x[2], False).take(5))
    return resultList



result = searchWords("Belgium")

with open("searchResults.txt",'w') as f:
	for i in range(0, len(result)):
		f.write("%s" %result[i])
		f.write("\n")
	
	







import os
import sys
import pyspark
from pyspark import SparkConf, SparkContext
from operator import add
import re
def removePunctuation(text):
    """Removes punctuation, changes to lower case, and strips leading and trailing spaces.

    Note:
        Only spaces, letters, and numbers should be retained.  Other characters should should be
        eliminated (e.g. it's becomes its).  Leading and trailing spaces should be removed after
        punctuation is removed.

    Args:
        text (str): A string.

    Returns:
        str: The cleaned up string.
    """
    text = re.sub(r'[^\w\s]','',text)
    return text.lower().strip()


# In[75]:
sc=pyspark.SparkContext()

sherlockRDD = (sc.textFile("Sherlock.txt", 8).map(removePunctuation))
sherlockRDD = sherlockRDD                 .glom()                 .map(lambda x: " ".join(x))                 .flatMap(lambda x: x.split(".")
                )


# In[94]:

#filter out the empty elements.  Remove all entries where the word is `''`.
sherlockWordsRDD = sherlockRDD.map(lambda s: s.split(" "))
sherlockBigramRDD = sherlockWordsRDD.flatMap(lambda s: [(s[i],s[i+1]) for i in range(0,len(s)-1)])
sherlockFilteredBigramRDD = sherlockBigramRDD.filter(lambda s: len(s[0]) > 4 and len(s[1]) > 4)
print sherlockFilteredBigramRDD.take(5)


# In[25]:

def wordCount(wordListRDD):
    """Creates a pair RDD with word counts from an RDD of words.

    """
    return wordListRDD.map(lambda x: (x,1)).reduceByKey(lambda x, y: x + y)

top10WordsAndCounts =  wordCount(sherlockFilteredBigramRDD).takeOrdered(10, lambda x: -x[1])


# In[96]:

print '\n'.join(map(lambda (w, c): '{0}: {1}'.format(w, c), top10WordsAndCounts))

# Spark example to print top 5 and bottom 5 users by their average tweet length  and user who tweeted most using Spark
# PGT April 2016   
# To run, do: spark-submit --master yarn-client SQL.py hdfs://hadoop2-0-0/data/twitter/*

from __future__ import print_function
import sys, json
from pyspark import SparkContext
from pyspark.sql import SQLContext,Row

# Given a full tweet object, return the text of the tweet
def getInfo(line):
  try:
    js = json.loads(line)
    text = js['text'].encode('ascii', 'ignore')
    user = js['user']['screen_name']
    return [(user,text)]
  except Exception as a:
    return []
  
if __name__ == "__main__":
  if len(sys.argv) < 2:
    print("enter a filename")
    sys.exit(1)
 
   
  sc = SparkContext(appName="tweetSearch")
  sqlContext = SQLContext(sc)
  
  tweets = sc.textFile(sys.argv[1])
  
  texts = tweets.flatMap(getInfo)
  
  df = sqlContext.createDataFrame(texts.map(lambda (u,t): Row(text = t, length = len(t), user = u)))
  df.registerTempTable("tweets")
  
  #print("DataFrame size %d" % df.count())
  
  matches0 = sqlContext.sql("SELECT user, avg(length) as length FROM tweets group by user order by length")
  matches1 = sqlContext.sql("SELECT user, avg(length) as length FROM tweets group by user order by length desc")
  matches2 = sqlContext.sql("SELECT user, count(*) as count FROM tweets group by user order by count desc")
  
  #print("Matches size %d" % matches.count())
  results = matches0.take(5)
  print("Bottom 5 users by tweet length")
  for r in results:
    print(r)
  print("Top 5 users by tweet length")
  results = matches1.take(5)
  for r in results:
    print(r)
  print("User who tweeted most")
  results = matches2.take(1)
  for r in results:
    print(r)
  
  sc.stop()

Solution:
=========

What twitter user tweeted the most?  What is the top 5 longest tweeters over each’s average tweet length?  Bottom 5?

Hadoop Approach:
================

To solve this problem we need following fields from data :
userid, userScreenName, text

Map Logic: from input twitter data we take required fields and send to reducer.
Note: we need to encode text (i.e tweet message) to UTF-8 since python 2.x default is ASCII. To get correct count of tweet length it is necessary for us to use this.

Reduce Logic: from the mapper input we calculate average tweet from total sum of indvidual tweet lengths and count of tweets. 
The output here will be userid, text_avg_length, screenName and count

To get top 5 longest tweeters we can run bash sort command on the reducer output and pipe top 5 records.
for example:
hadoop fs -cat /use/ramidity/ramidity111/* | sort -n -k3 -r | head -n5
sort -n -k2 -r says "sort numerically by column #3 i.e count, in descending order". head -n5 pulls the top five. 


Spark Approach:
================

Required fields from input data: hour of the day of tweet, tweet text message & userName

•	Import print_function, SparkContext, SQLContext. <br />
•	A  Mapper function is created which parses each input line using json library and extracts the Screen Name from user data and Twitter text. <br />
		def getInfo(line): <br />
		js = json.loads(line) <br />
		text = js['text'].encode('ascii', 'ignore') <br />
		user = js['user']['screen_name'] <br />
		return [(user,text)] <br />
		
•   Dataset is then loaded into a RDD using the sc.textFile(sys.argv[1]) when the file name is sent from the command line parameters. <br />
•	Using “flatMap” the RDD is then transformed by using the function getInfo which was initially written to parse each input line. <br />
•	Using “sqlContext” and “createDataFrame”, a map is implemented to create a new transformed RDD as shown below <br />
		 df = sqlContext.createDataFrame(texts.map(lambda (u,t): Row(text = t, length = len(t), user = u))) <br />
•	A temporary table is created using the function registerTempTable(“Name_Of_Table”). <br />
•	3 SQL queries are written to find out the user who tweeted the most and the top/bottom users from the temporary table created in the above step. <br />
	    sqlContext.sql("SELECT user, avg(length) as length FROM tweets group by user order by length") <br />
		sqlContext.sql("SELECT user, avg(length) as length FROM tweets group by user order by length desc") <br />
		sqlContext.sql("SELECT user, count(*) as count FROM tweets group by user order by count desc") <br />

command to execute : spark-submit --master yarn-client SQL.py hdfs://hadoop2-0-0/data/twitter/* <br />
    
Output:
========

a) User who tweeted most?
username / count => marilyn9743 / 3419 

b) Top 5 users with highest avg tweet length

username   ||   avg length

Huntersweat	416 <br />
RoyalEliteKiva	350 <br />
blackxhole	320 <br />
KelleeMichele	272 <br />
pizzadellarry 	253 <br />

c) Bottom 5 user with highest avg tweet length

username   ||   avg length

Laila_Lafrai	0 <br />
Fun_Size20	0 <br />
Trevorsturgill5	0 <br />
2013Afi9	0 <br />
lm_Lil_Wanie 	0 <br />


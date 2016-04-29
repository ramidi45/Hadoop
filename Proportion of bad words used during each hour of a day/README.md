Solution:
=========
Detect the proportion of bad words in a tweet.  Plot bad word proportion by hour for all 24 hours. </br>



Approach:
==========

Required fields from input data: hour of the day of tweet, tweet text message</br>

•	Import print_function, SparkContext, SQLContext.</br>
•	A  Mapper function is created which parses each input line using json library and extracts the Twitter text and then counts number of bad words in it.</br>
	We store all bad words in a dictionary and check whether each input word from tweet text is in bad word list. If present increment badword count. </br>
		def getInfo(line):</br>
		js = json.loads(line)</br>
		#badwords list from google</br>
		text = js['text'].encode('ascii', 'ignore')</br>
		badCount=0</br>
		totalCount =0</br>
		for word in text.split():</br>
		  totalCount +=1 </br>
		  if (badwords.get(word)!=None):</br>
			badCount +=1              </br>
		hour = int(js['created_at'].split()[3].split(':')[0])</br>
		return [(badCount,totalCount,hour)]</br>
		
•   Dataset is then loaded into a RDD using the sc.textFile(sys.argv[1]) when the file name is sent from the command line parameters.</br>
•	Using “flatMap” the RDD is then transformed by using the function getInfo which was initially written to parse each input line.</br>
•	Using “sqlContext” and “createDataFrame”, a map is implemented to create a new transformed RDD as shown below</br>
		   df = sqlContext.createDataFrame(texts.map(lambda (b,t,h): Row(badCount = b, totalCount = t, hour = h)))</br>
•	A temporary table is created using the function registerTempTable(“Name_Of_Table”).</br>
•	SQL query is written to reduce the result by doing sum of bad count and total count of words grouped by hour.</br>
	    sqlContext.sql("SELECT sum(badCount) as badCount, sum(totalCount) as totalCount, sum(badCount)/sum(totalCount) as result, hour FROM tweets group by hour")</br>

command to execute : spark-submit --master yarn-client SQL.py hdfs://hadoop2-0-0/data/twitter/*</br>
    
Output:
========

</br>
Row(badCount=25803, totalCount=4178557, result=0.0061750982456383868, hour=0)   </br>
Row(badCount=29719, totalCount=4573253, result=0.0064984377641035826, hour=1)</br>
Row(badCount=32590, totalCount=4779333, result=0.006818943145413806, hour=2)</br>
Row(badCount=31844, totalCount=4437175, result=0.0071766382890014477, hour=3)</br>
Row(badCount=27874, totalCount=3685930, result=0.0075622705802877426, hour=4)</br>
Row(badCount=21960, totalCount=2884068, result=0.0076142448791082593, hour=5)</br>
Row(badCount=16117, totalCount=2200365, result=0.0073246938576099875, hour=6)</br>
Row(badCount=10951, totalCount=1772439, result=0.006178491897323406, hour=7)</br>
Row(badCount=7770, totalCount=1504550, result=0.0051643348509521122, hour=8)</br>
Row(badCount=5805, totalCount=1418125, result=0.004093433230498017, hour=9)</br>
Row(badCount=5824, totalCount=1575736, result=0.0036960506074621637, hour=10)</br>
Row(badCount=7317, totalCount=1883033, result=0.0038857524005155514, hour=11)</br>
Row(badCount=9378, totalCount=2267961, result=0.0041349917392759401, hour=12)</br>
Row(badCount=12183, totalCount=2713778, result=0.0044893134221001127, hour=13)</br>
Row(badCount=15682, totalCount=3183793, result=0.0049255714803066661, hour=14)</br>
Row(badCount=19224, totalCount=3609724, result=0.0053256149223597149, hour=15)</br>
Row(badCount=20813, totalCount=3841682, result=0.005417679027051172, hour=16)</br>
Row(badCount=21076, totalCount=3974914, result=0.0053022530801924272, hour=17)</br>
Row(badCount=21763, totalCount=3981886, result=0.0054655005191007475, hour=18)</br>
Row(badCount=22138, totalCount=4067237, result=0.0054430071323603715, hour=19)</br>
Row(badCount=22668, totalCount=4105322, result=0.0055216131645702826, hour=20)</br>
Row(badCount=22417, totalCount=4143736, result=0.0054098523651120627, hour=21)</br>
Row(badCount=21723, totalCount=4002695, result=0.0054270934957572339, hour=22)</br>
Row(badCount=23276, totalCount=4032049, result=0.0057727473054022902, hour=23)</br>


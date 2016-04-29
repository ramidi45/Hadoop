Solution:
==========
What twitter user tweeted the most?  What is the top 5 longest tweeters over each’s average tweet length?  Bottom 5?

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

Output: 
[root@tarun Mar15-streaming]# hadoop fs -cat /user/ramidity/ramidity111/* | sort -n -k2 -r | head -n5

976206314    416    1    Huntersweat
1656339680    350    1    RoyalEliteKiva
338547198    320    1    blackxhole
852055597    272    1    KelleeMichele
452806252    253    1    pizzadellarry 

Now to get the user who tweeted most we send the output of above mapReduce program to one more mapReduce where we find out which userScreenName has maximum count.
Map Logic:
for each input we take count and check against local maxima and if input count is more than local maxima then we update local maxima.
Finally we write local maxima count and its corresponding key i.e(screenName)

Reducer Logic:
Same mapper logic is implemented to further reduce the output to one single maxima to get user who tweeted most. 
        count   user screenName
Output: 3419    marilyn9743
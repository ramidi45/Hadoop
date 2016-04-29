Hadoop MapReduce P3
====================
Q) For each day of the week, what was the most mentioned hashtag?  Hour of the day?

Sol: To solve the above problem we need hashtag and day and hour from the twitter data.

We will make use of two mapreduce programs to solve this problem.

First mapReduce program is used to find count of each hashtag day wise.

Mapper Logic:
send day+hashtag as key and day as value to reducer

Why we use day+hashtag ? 
Since we need count of the hashtag for day wise we use day+hashtag combination.

Reducer Logic:
Reducer is used to find count of day+hashtag combination.

Second mapReduce program is used to find the most popular hashtag day wise.

Mapper Logic:
for each input we take count and check against local maxima and if input count is more than local maxima then we update local maxima.
Since the data is sorted, we check for day change condition and when there is day change we write local maxima count and its corresponding key i.e(day+hashtag)

Reducer Logic:
Same mapper logic is implemented to further reduce the output to one single maxima for each day of the week.

Now to find for each hour of day same logic is used and only change is key will be hour+hashtag combination.

Output:

Fri    dog
Mon    dog
Sat    dog
Sun    dog
Thu    dog
Tue    dog
Wed    dog

00    dog
01    dog
02    winning
03    winning
04    winning
05    dog
06    dog
07    dog
08    dog
09    dog
10    dog
11    dog
12    dog
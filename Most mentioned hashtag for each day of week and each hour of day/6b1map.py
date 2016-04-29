#!/usr/bin/env python

import sys
import string
import json


for line in sys.stdin:
    data = json.loads(line)
    day= data['created_at'].split()[3].split(':')[0] #hour of the day
    hashtags=data['entities']['hashtags'] #hashtag array
    for i in range(len(hashtags)):
        hashtag=hashtags[i]['text'].encode('ascii','ignore').decode('ascii')
        if(hashtag!=''): 
          print '%s\t%s' % (day+hashtags[i]['text'].encode('ascii','ignore').decode('ascii'), day)

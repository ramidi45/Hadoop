#!/usr/bin/env python

import sys
import string
import json


for line in sys.stdin:
    data = json.loads(line)
    day= data['created_at'].split()[0] #day of the week
    hashtags=data['entities']['hashtags'] #hashtag arrays
    for i in range(len(hashtags)):
        hashtag=hashtags[i]['text'].encode('ascii','ignore').decode('ascii')
        if(hashtag!=''): 
          print '%s\t%s' % (day+hashtags[i]['text'].encode('ascii','ignore').decode('ascii'), day)

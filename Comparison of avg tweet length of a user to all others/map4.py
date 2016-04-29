#!/usr/bin/env python

import sys
import string
import json 

for line in sys.stdin:
    data = json.loads(line)
    if data['user']['screen_name']== "PrezOno":
      print '%s\t%s' % ("PrezOno",len(data['text']))
    else:
      print '%s\t%s' % ("Other",len(data['text'])) 

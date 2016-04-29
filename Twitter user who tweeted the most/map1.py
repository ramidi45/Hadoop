#!/usr/bin/env python

import sys
import string
import json 

for line in sys.stdin:
    data = json.loads(line)
    value = data['text']
    value= value.encode('utf8') if hasattr(value,'enccode') else value
    print '%s\t%s\t%s' % (data['user']['id'],len(value),data['user']['screen_name'])

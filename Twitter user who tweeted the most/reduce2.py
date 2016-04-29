#!/usr/bin/env python

import sys
import string
max=0
screenName= ''

for line in sys.stdin:
    data = line.strip('\n').split('\t')
    if int(data[0])> max:
      max= int(data[0])
      screenName = data[1]   
print '%s\t%s' % (max,screenName)

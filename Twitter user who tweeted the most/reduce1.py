#!/usr/bin/env python

import sys
import string

count = 0
sum = 0
old_key = None
name = None

for line in sys.stdin:
  (key,val) = line.strip().split('\t',1)
  if old_key != key:
    if old_key:
      print '%s\t%s\t%s\t%s' % ( old_key,sum/count,count,name)
      count = 0
      sum =0
  old_key = key
  name =val.split('\t',1)[1]
  try:
    sum = sum + int(val.split('\t',1)[0])
    count += 1
  except:
    continue
print '%s\t%s\t%s\t%s' % (old_key,sum/count,count,name)

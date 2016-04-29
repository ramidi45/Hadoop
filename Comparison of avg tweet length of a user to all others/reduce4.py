#!/usr/bin/env python

import sys
import string

count = 0
sum = 0
old_key = None

for line in sys.stdin:
  (key,val) = line.strip().split('\t')
  if old_key != key:
    if old_key:
      print '%s\t%s' % ( old_key,sum/count)
      count = 0
      sum = 0
  old_key = key
  try:
    sum = sum + int(val)
    count += 1
  except:
    continue
print '%s\t%s' % (old_key,sum/count)

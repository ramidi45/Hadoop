#!/usr/bin/env python

import sys
import string

max=0
old_key = '&&&&'
hashtag = ''

for line in sys.stdin:
  (key,val) = line.strip().split('\t')
  if old_key[:2] != key[:2] and old_key!='&&&&':
      print '%s\t%s' % ( hashtag,max)
      max = 0
  old_key = key
  try:
    if int(val)>max:
      max= int(val)
      hashtag=key
  except: 
    continue
print '%s\t%s' % (hashtag,max)

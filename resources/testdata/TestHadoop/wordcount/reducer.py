#!/usr/bin/python

import sys

wordcount = {}
for line in sys.stdin.readlines():
    line = line.strip()
    values = line.split()
    word = values[0]
    count = int(values[1])
    if not word in wordcount:
        wordcount[word] = 0
    wordcount[word] += count
    pass

for word, count in wordcount.iteritems():
    print "%s %s" % (word, count)


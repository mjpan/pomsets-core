#!/usr/bin/python

import sys

wordcount = {}
for line in sys.stdin.readlines():
    line = line.strip()
    for word in line.split(' '):
        if not len(word):
            continue
        wordcount[word] = wordcount.get(word, 0) + 1

for word, count in wordcount.iteritems():
    print "%s %s" % (word, count)



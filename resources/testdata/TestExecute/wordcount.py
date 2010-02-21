#!/usr/bin/python

from __future__ import with_statement

import sys

inputPath = sys.argv[1]
outputPath = sys.argv[2]

wordcount = {}
with open(inputPath, 'r') as f:
    for line in f.readlines():
        line = line.strip()
        for word in line.split(' '):
            if not word in wordcount:
                wordcount[word] = 0
            wordcount[word] += 1
    pass

with open(outputPath, 'w') as f:
    for word, count in wordcount.iteritems():
        print >> f,  "%s %s" % (word, count)

    pass


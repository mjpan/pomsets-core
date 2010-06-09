#!/usr/bin/python

from __future__ import with_statement

import sys

index = 1

inputPath = ''
while index < len(sys.argv):
    inputPath = sys.argv[index]
    index = index + 1
    if len(inputPath) > 0:
        break
    pass
print "processing input path %s" % inputPath
if len(inputPath) == 0:
    raise Exception('need to specify an input path')

outputPath = ''
while index < len(sys.argv):
    outputPath = sys.argv[index]
    index = index + 1
    if len(outputPath) > 0:
        break
    pass
print "processing output path %s" % outputPath
if len(outputPath) == 0:
    raise Exception('need to specify an output path')

wordcount = {}
with open(inputPath, 'r') as f:
    for line in f.readlines():
        line = line.strip()
        for word in line.split(' '):
            word = word.strip()
            if not len(word) > 0:
                continue
            if not word in wordcount:
                wordcount[word] = 0
            wordcount[word] += 1
    pass

with open(outputPath, 'w') as f:
    for word, count in wordcount.iteritems():
        print >> f,  "%s %s" % (word, count)

    pass


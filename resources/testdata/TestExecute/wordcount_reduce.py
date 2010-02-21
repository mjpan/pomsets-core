#!/usr/bin/python

from __future__ import with_statement

import sys

inputPaths = []
outputPath = None
args = [x for x in sys.argv]
flags = ['-input', '-output']
while len(args) is not 0:
    arg = args.pop(0)
    if arg == '-input':
        while len(args) is not 0:
            if args[0] in flags:
                break
            inputPaths.append(args.pop(0))
            pass
        pass
    if arg == '-output':
        outputPath = args.pop(0)
        pass
    pass


wordcount = {}
for inputPath in inputPaths:
    with open(inputPath, 'r') as f:
        for line in f.readlines():
            line = line.strip()
            values = line.split()
            word = values[0]
            count = int(values[1])
            if not word in wordcount:
                wordcount[word] = 0
            wordcount[word] += count
        pass

with open(outputPath, 'w') as f:
    for word, count in wordcount.iteritems():
        print >> f,  "%s %s" % (word, count)

    pass


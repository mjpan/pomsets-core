from __future__ import with_statement

def loadListValuesFromFiles(filePaths):

    values = []

    for filePath in filePaths:
        with open(filePath) as f:
            for line in f.readlines():
                line = line.strip()
                if len(line):
                    values.append(line)
                pass
            pass
        pass

    return values

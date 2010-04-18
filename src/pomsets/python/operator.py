from __future__ import with_statement

def loadListValuesFromFiles(filePaths):

    if type(filePaths) is str:
        filePaths = [filePaths]

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


def stringReplace(fullString, originalSubstring, newSubstring, *args):
    return fullString.replace(originalSubstring, newSubstring, *args)

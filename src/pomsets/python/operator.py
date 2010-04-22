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


def stringReplace(strings, originalSubstring, newSubstring, *args):
    if type(strings) is str:
        strings = [strings]
    values = []
    for fullString in strings:
        values.append(
            fullString.replace(originalSubstring, newSubstring, *args))
        pass
    return values

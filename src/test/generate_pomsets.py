import os
import sys

import util
util.setPythonPath()

import pomsets.library as LibraryModule

import test.definition as DefinitionModule


def main(argv=None):

    util.configLogging()
    
    if argv is None:
        argv = []
        
    if len(argv) < 2:
        raise ValueError('need to specify directory to output the definitions')
    outputDir = argv[1]
    
    baseDefinition = DefinitionModule.createPomsetContainingParameterSweep()
    LibraryModule.pickleDefinition(
        os.path.join(outputDir, 'mr_wordcount.pomset'), baseDefinition)
    DefinitionModule.bindParameterSweepDefinitionParameters(baseDefinition)
    LibraryModule.pickleDefinition(
        os.path.join(outputDir, 'mr_wordcount_staging.pomset'), baseDefinition)

    
    baseDefinition = DefinitionModule.createPomsetContainingLoopDefinition()
    LibraryModule.pickleDefinition(
        os.path.join(outputDir, 'loop_wordcount.pomset'), baseDefinition)
    DefinitionModule.bindLoopDefinitionParameters(baseDefinition)
    LibraryModule.pickleDefinition(
        os.path.join(outputDir, 'loop_wordcount_staging.pomset'), baseDefinition)

    
    return

if __name__=="__main__":
    main(sys.argv)


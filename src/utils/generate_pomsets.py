import os
import sys

sys.path.insert(0, os.getenv('POMSETS_HOME'))

import pomsets.library as LibraryModule

import utils
import utils.definition as DefinitionModule


def main(argv=None):

    utils.configLogging()
    
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


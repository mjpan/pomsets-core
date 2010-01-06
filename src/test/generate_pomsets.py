import os
import sys

import util
util.setPythonPath()

import test.definition as DefinitionModule


def main(argv=None):
    
    if argv is None:
        argv = []
        
    if len(argv) < 2:
        raise ValueError('need to specify directory to output the definitions')
    outputDir = argv[1]
    
    baseDefinition = DefinitionModule.createPomsetContainingParameterSweep()
    DefinitionModule.pickleDefinition(
        os.path.join(outputDir, 'mr_wordcount.pomset'), baseDefinition)
    DefinitionModule.bindParameterSweepDefinitionParameters(baseDefinition)
    DefinitionModule.pickleDefinition(
        os.path.join(outputDir, 'mr_wordcount_staging.pomset'), baseDefinition)

    
    baseDefinition = DefinitionModule.createPomsetContainingLoopDefinition()
    DefinitionModule.pickleDefinition(
        os.path.join(outputDir, 'loop_wordcount.pomset'), baseDefinition)
    DefinitionModule.bindLoopDefinitionParameters(baseDefinition)
    DefinitionModule.pickleDefinition(
        os.path.join(outputDir, 'loop_wordcount_staging.pomset'), baseDefinition)

    
    return

if __name__=="__main__":
    main(sys.argv)


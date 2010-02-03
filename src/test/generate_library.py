import os
import sys
import uuid

import util
util.setPythonPath()

import pomsets.definition as DefinitionModule
import pomsets.library as LibraryModule
import pomsets.parameter as ParameterModule

import test.definition as DefinitionTestModule


ID_WORDCOUNT = 'word count::8613fe86-e7fc-4487-b4d2-0989706f8825'
ID_WORDCOUNT_REDUCE = 'word count reducer::08979d5f-6c0d-43b7-9206-dfe69eae6c26'


def generateBootstrapper():
    defToLoadDef = DefinitionModule.AtomicDefinition()
    defToLoadDef.commandBuilderType('python eval')
    defToLoadDef.id(LibraryModule.ID_LOADLIBRARYDEFINITION)
    defToLoadDef.name('load library definition')
    # need a command builder to call the loadPomset function
    # need a python eval environment to execute the output of commandbuilder
    parameter = ParameterModule.DataParameter(
        id='pomset url', definition=defToLoadDef, optional=False, active=True,
        portDirection=ParameterModule.PORT_DIRECTION_INPUT)
    ParameterModule.setAttributes(parameter, {})
    defToLoadDef.addParameter(parameter)
    defToLoadDef.isLibraryDefinition(True)
    defToLoadDef.functionToExecute(DefinitionModule.executeTaskInEnvironment)
    return defToLoadDef



def generateDefaultLoader(outputDir):

    definitionsToLoad = []

    defToLoadDef = generateBootstrapper()
    DefinitionTestModule.pickleDefinition(
        os.path.join(outputDir, 'loadLibraryDefinition.pomset'), defToLoadDef)
    
    definitionsToLoad.append(defToLoadDef)
    
    wcDefinition = DefinitionTestModule.createWordCountDefinition()
    wcDefinition.id(ID_WORDCOUNT)
    wcDefinitionPath = 'wordcount.pomset'
    wcDefinition.url(wcDefinitionPath)
    DefinitionTestModule.pickleDefinition(
        os.path.join(outputDir, wcDefinitionPath), wcDefinition)
    definitionsToLoad.append(wcDefinition)
    
    wcrDefinition = DefinitionTestModule.createWordCountReduceDefinition()
    wcrDefinition.id(ID_WORDCOUNT_REDUCE)
    wcrDefinitionPath = 'wordcount_reduce.pomset'
    wcrDefinition.url(wcrDefinitionPath)
    DefinitionTestModule.pickleDefinition(
        os.path.join(outputDir, wcrDefinitionPath), wcrDefinition)
    definitionsToLoad.append(wcrDefinition)
    
    library = LibraryModule.Library()
    map(library.addDefinition, definitionsToLoad)

    defToLoadDefs = library.generateBootstrapLoaderPomset()
    DefinitionTestModule.pickleDefinition(
        os.path.join(outputDir, 'loadLibraryDefinitions.pomset'), defToLoadDefs)
    
    return

def generateLoaderWithFailure1(outputDir):
    
    definitionsToLoad = []

    defToLoadDef = generateBootstrapper()
    DefinitionTestModule.pickleDefinition(
        os.path.join(outputDir, 'loadLibraryDefinition.pomset'), defToLoadDef)
    
    definitionsToLoad.append(defToLoadDef)
    
    wcDefinition = DefinitionTestModule.createWordCountDefinition()
    wcDefinition.id(ID_WORDCOUNT)
    wcDefinitionPath = 'wordcount.pomset'
    wcDefinition.url(wcDefinitionPath)
    # we purposely do not pickle it
    # to ensure that the loading fi
    #DefinitionTestModule.pickleDefinition(
    #    os.path.join(outputDir, wcDefinitionPath), wcDefinition)
    definitionsToLoad.append(wcDefinition)
    
    library = LibraryModule.Library()
    map(library.addDefinition, definitionsToLoad)

    defToLoadDefs = library.generateBootstrapLoaderPomset()
    DefinitionTestModule.pickleDefinition(
        os.path.join(outputDir, 'loadLibraryDefinitions.pomset'), defToLoadDefs)
    
    return



def generateLoaderWithFailure2(outputDir):

    definitionsToLoad = []

    defToLoadDef = generateBootstrapper()
    DefinitionTestModule.pickleDefinition(
        os.path.join(outputDir, 'loadLibraryDefinition.pomset'), defToLoadDef)
    
    definitionsToLoad.append(defToLoadDef)
    
    wcDefinition = DefinitionTestModule.createWordCountDefinition()
    wcDefinition.id(ID_WORDCOUNT)
    wcDefinitionPath = 'wordcount.pomset'
    wcDefinition.url(wcDefinitionPath)
    # we purposely do not pickle it
    # to ensure that the loading fi
    #DefinitionTestModule.pickleDefinition(
    #    os.path.join(outputDir, wcDefinitionPath), wcDefinition)
    definitionsToLoad.append(wcDefinition)
    
    wcrDefinition = DefinitionTestModule.createWordCountReduceDefinition()
    wcrDefinition.id(ID_WORDCOUNT_REDUCE)
    wcrDefinitionPath = 'wordcount_reduce.pomset'
    wcrDefinition.url(wcrDefinitionPath)
    DefinitionTestModule.pickleDefinition(
        os.path.join(outputDir, wcrDefinitionPath), wcrDefinition)
    definitionsToLoad.append(wcrDefinition)
    
    library = LibraryModule.Library()
    map(library.addDefinition, definitionsToLoad)

    defToLoadDefs = library.generateBootstrapLoaderPomset()
    DefinitionTestModule.pickleDefinition(
        os.path.join(outputDir, 'loadLibraryDefinitions.pomset'), defToLoadDefs)
    
    return



def main(argv):

    util.configLogging()
    
    if argv is None:
        argv = []

    shouldGenerateDefaultLoader = True
    shouldGenerateLoaderWithFailure1 = False
    shouldGenerateLoaderWithFailure2 = False
    minArgLen = 2

    for arg in argv:
        if arg.startswith('-default='):
            minArgLen = minArgLen + 1
            shouldGenerateDefaultLoader = arg[9:] in ['True', 'true', '1']
        if arg.startswith('-failure1='):
            minArgLen = minArgLen + 1
            shouldGenerateLoaderWithFailure1 = arg[10:] in ['True', 'true', '1']
        if arg.startswith('-failure2='):
            minArgLen = minArgLen + 1
            shouldGenerateLoaderWithFailure2 = arg[10:] in ['True', 'true', '1']
        pass


    if len(argv) < minArgLen:
        raise ValueError('need to specify directory to output the definitions')
        
    outputDir = argv[-1]

    if shouldGenerateDefaultLoader:
        generateDefaultLoader(outputDir)
        
    if shouldGenerateLoaderWithFailure1:
        generateLoaderWithFailure1(outputDir)

    if shouldGenerateLoaderWithFailure2:
        generateLoaderWithFailure2(outputDir)
    return

if __name__=="__main__":
    main(sys.argv)


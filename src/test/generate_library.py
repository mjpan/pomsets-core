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

def main(argv=None):

    util.configLogging()
    
    if argv is None:
        argv = []

    if len(argv) < 2:
        raise ValueError('need to specify directory to output the definitions')
        
    outputDir = argv[1]
    
    definitionsToLoad = []

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
    
    """
    # now create a load library definitions pomset
    # that will load the two wordcount pomsets
    defToLoadDefs = DefinitionModule.getNewNestDefinition()
    for definitionToLoad in definitionsToLoad:
        loadNode = defToLoadDefs.createNode(id=uuid.uuid4())
        loadNode.definitionToReference(defToLoadDef)
        loadNode.name('load %s' % definitionToLoad.url())
        loadNode.setParameterBinding('pomset url', definitionToLoad.url())
        pass
    defToLoadDefs.id(LibraryModule.ID_BOOTSTRAPLOADER)
    defToLoadDefs.name('bootstrap pomsets loader')

    """
    library = LibraryModule.Library()
    map(library.addDefinition, definitionsToLoad)

    defToLoadDefs = library.generateBootstrapLoaderPomset()
    DefinitionTestModule.pickleDefinition(
        os.path.join(outputDir, 'loadLibraryDefinitions.pomset'), defToLoadDefs)
    
    return

if __name__=="__main__":
    main(sys.argv)


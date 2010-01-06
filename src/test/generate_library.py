import os
import sys
import uuid

import util
util.setPythonPath()

import pomsets.definition as DefinitionModule
import pomsets.parameter as ParameterModule

import test.definition as DefinitionTestModule


ID_WORDCOUNT = 'word count::8613fe86-e7fc-4487-b4d2-0989706f8825'
ID_WORDCOUNT_REDUCE = 'word count reducer::08979d5f-6c0d-43b7-9206-dfe69eae6c26'
ID_LOADLIBRARYDEFINITION = 'load library definition::bb028375-bbd5-43ec-b6c3-4955c062063f'
ID_BOOTSTRAPLOADER = 'library bootstrap loader::751fe366-1448-4db3-9db4-944075de7a5b'

def main(argv=None):
    
    if argv is None:
        argv = []

    if len(argv) < 1:
        raise ValueError('need to specify directory to output the definitions')
        
    outputDir = argv[1]
    
    defToLoadDef = DefinitionModule.AtomicDefinition()
    defToLoadDef.commandBuilderType('python eval')
    defToLoadDef.id(ID_LOADLIBRARYDEFINITION)
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
    
    definitionsToLoad = []
    
    wcDefinition = DefinitionTestModule.createWordCountDefinition()
    wcDefinition.id(ID_WORDCOUNT)
    wcDefinitionPath = 'wordcount.pomset'
    wcDefinition.url(wcDefinitionPath)
    DefinitionTestModule.pickleDefinition(
        wcDefinitionPath, wcDefinition)
    # definitionsToLoad.append('wordcount.pomset')
    definitionsToLoad.append(wcDefinition)
    
    wcrDefinition = DefinitionTestModule.createWordCountReduceDefinition()
    wcrDefinition.id(ID_WORDCOUNT_REDUCE)
    wcrDefinitionPath = 'wordcount_reduce.pomset'
    wcrDefinition.url(wcrDefinitionPath)
    DefinitionTestModule.pickleDefinition(
        wcrDefinitionPath, wcrDefinition)
    # definitionsToLoad.append('wordcount_reduce.pomset')
    definitionsToLoad.append(wcrDefinition)
    
    # now create a load library definitions pomset
    # that will load the two wordcount pomsets
    defToLoadDefs = DefinitionModule.getNewNestDefinition()
    for definitionToLoad in definitionsToLoad:
        loadNode = defToLoadDefs.createNode(id=uuid.uuid4())
        loadNode.definitionToReference(defToLoadDef)
        loadNode.name('load %s' % definitionToLoad.url())
        loadNode.setParameterBinding('pomset url', definitionToLoad.url())
        pass
    defToLoadDefs.id(ID_BOOTSTRAPLOADER)
    DefinitionTestModule.pickleDefinition(
        os.path.join(outputDir, 'loadLibraryDefinitions.pomset'), defToLoadDefs)
    
    return

if __name__=="__main__":
    main(sys.argv)


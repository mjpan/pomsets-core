import os
import sys
import uuid

import optparse as OptparseModule

sys.path.insert(0, os.getenv('POMSETS_HOME'))

import pomsets.builder as BuilderModule
import pomsets.context as ContextModule
import pomsets.definition as DefinitionModule
import pomsets.library as LibraryModule
import pomsets.parameter as ParameterModule

import pomsets.test_utils as DefinitionTestModule




def generateBootstrapper():

    """
    defToLoadDef = DefinitionModule.AtomicDefinition()
    defToLoadDef.commandBuilderType('library bootstrap loader')
    defToLoadDef.executeEnvironmentType('library bootstrap loader')
    defToLoadDef.id(LibraryModule.ID_LOADLIBRARYDEFINITION)
    defToLoadDef.name('load library definition')
    """

    builder = BuilderModule.Builder()
    definitionContext = builder.createNewAtomicPomset(
        name='load library definition', 
        executableObject=None,
        commandBuilderType='library bootstrap loader',
        executeEnvironmentType='library bootstrap loader')
    definition = definitionContext.pomset()
    definition.id(LibraryModule.ID_LOADLIBRARYDEFINITION)

    """
    # need a command builder to call the loadPomset function
    # need a python eval environment to execute the output of commandbuilder
    parameter = ParameterModule.DataParameter(
        id='pomset url', optional=False, active=True,
        portDirection=ParameterModule.PORT_DIRECTION_INPUT)
    ParameterModule.setAttributes(parameter, {})
    defToLoadDef.addParameter(parameter)
    """
    builder.addPomsetParameter(
        definition, 'pomset url', 
        { 'direction':ParameterModule.PORT_DIRECTION_INPUT }
        )


    definition.isLibraryDefinition(True)
    # definition.functionToExecute(DefinitionModule.executeTaskInEnvironment)
    return definition



def generateDefaultLoader(outputDir):

    definitionsToLoad = []

    defToLoadDef = generateBootstrapper()
    ContextModule.pickleDefinition(
        os.path.join(outputDir, 'loadLibraryDefinition.pomset'), defToLoadDef)
    context = ContextModule.wrapPomsetInContext(defToLoadDef)
    definitionsToLoad.append(context)
    
    wcDefinition = DefinitionTestModule.createWordCountDefinition()
    wcDefinitionPath = wcDefinition.id() + '.pomset'
    wcDefinition.url(wcDefinitionPath)
    ContextModule.pickleDefinition(
        os.path.join(outputDir, wcDefinitionPath), wcDefinition)
    context = ContextModule.wrapPomsetInContext(wcDefinition)
    definitionsToLoad.append(context)
    
    wcrDefinition = DefinitionTestModule.createWordCountReduceDefinition()
    wcrDefinitionPath = wcrDefinition.id() + '.pomset'
    wcrDefinition.url(wcrDefinitionPath)
    ContextModule.pickleDefinition(
        os.path.join(outputDir, wcrDefinitionPath), wcrDefinition)
    context = ContextModule.wrapPomsetInContext(wcrDefinition)
    definitionsToLoad.append(context)


    loadValuesDefinition = \
        DefinitionTestModule.createLoadListValuesFromFilesDefinition()
    loadValuesDefinitionPath = loadValuesDefinition.id() + '.pomset'
    loadValuesDefinition.url(loadValuesDefinitionPath)
    ContextModule.pickleDefinition(
        os.path.join(outputDir, loadValuesDefinitionPath), loadValuesDefinition)
    context = ContextModule.wrapPomsetInContext(loadValuesDefinition)
    definitionsToLoad.append(context)

    library = LibraryModule.Library()
    map(library.addPomsetContext, definitionsToLoad)

    defToLoadDefs = library.generateBootstrapLoaderPomset()
    ContextModule.pickleDefinition(
        os.path.join(outputDir, 'loadLibraryDefinitions.pomset'), defToLoadDefs)
    
    return


def generateLoaderWithFailure1(outputDir):
    
    definitionsToLoad = []

    defToLoadDef = generateBootstrapper()
    ContextModule.pickleDefinition(
        os.path.join(outputDir, 'loadLibraryDefinition.pomset'), defToLoadDef)
    context = ContextModule.wrapPomsetInContext(defToLoadDef)
    definitionsToLoad.append(context)
    
    wcDefinition = DefinitionTestModule.createWordCountDefinition()
    wcDefinitionPath = wcDefinition.id() + '.pomset'
    wcDefinition.url(wcDefinitionPath)
    # we purposely do not pickle it
    # to ensure that the loading fails
    context = ContextModule.wrapPomsetInContext(wcDefinition)
    definitionsToLoad.append(context)
    
    library = LibraryModule.Library()
    map(library.addPomsetContext, definitionsToLoad)

    defToLoadDefs = library.generateBootstrapLoaderPomset()
    ContextModule.pickleDefinition(
        os.path.join(outputDir, 'loadLibraryDefinitions.pomset'), defToLoadDefs)
    
    return



def generateLoaderWithFailure2(outputDir):

    definitionsToLoad = []

    defToLoadDef = generateBootstrapper()
    ContextModule.pickleDefinition(
        os.path.join(outputDir, 'loadLibraryDefinition.pomset'), defToLoadDef)
    context = ContextModule.wrapPomsetInContext(defToLoadDef)
    definitionsToLoad.append(context)
    
    wcDefinition = DefinitionTestModule.createWordCountDefinition()
    wcDefinitionPath = wcDefinition.id() + '.pomset'
    wcDefinition.url(wcDefinitionPath)
    # we purposely do not pickle it
    # to ensure that the loading fails
    context = ContextModule.wrapPomsetInContext(wcDefinition)
    definitionsToLoad.append(context)
    
    wcrDefinition = DefinitionTestModule.createWordCountReduceDefinition()
    wcrDefinitionPath = wcrDefinition.id() + '.pomset'
    wcrDefinition.url(wcrDefinitionPath)
    ContextModule.pickleDefinition(
        os.path.join(outputDir, wcrDefinitionPath), wcrDefinition)
    context = ContextModule.wrapPomsetInContext(wcrDefinition)
    definitionsToLoad.append(context)
    
    library = LibraryModule.Library()
    map(library.addPomsetContext, definitionsToLoad)

    defToLoadDefs = library.generateBootstrapLoaderPomset()
    ContextModule.pickleDefinition(
        os.path.join(outputDir, 'loadLibraryDefinitions.pomset'), defToLoadDefs)
    
    return



def main(args):

    parser = OptparseModule.OptionParser()
    parser.add_option("--default", dest="default",
                      help="if should generate default loader")
    parser.add_option("--failure1", dest="failure1",
                      help="if should generate loader with failure test case 1")
    parser.add_option("--failure2", dest="failure2",
                      help="if should generate loader with failure test case 2")
    parser.add_option("-o", "--output", dest="outputDir",
                      help="output dir")

    (options, args) = parser.parse_args(args)

    shouldGenerateDefaultLoader = options.default is not None
    shouldGenerateLoaderWithFailure1 = options.failure1 is not None
    shouldGenerateLoaderWithFailure2 = options.failure2 is not None

    outputDir = options.outputDir
    if outputDir is None:
        raise ValueError('need to specify directory to output the definitions')
        
    if shouldGenerateDefaultLoader:
        generateDefaultLoader(outputDir)
        
    if shouldGenerateLoaderWithFailure1:
        generateLoaderWithFailure1(outputDir)

    if shouldGenerateLoaderWithFailure2:
        generateLoaderWithFailure2(outputDir)
    return

if __name__=="__main__":
    main(sys.argv)


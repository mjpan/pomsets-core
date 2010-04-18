from __future__ import with_statement

import os
import sys
import threadpool
import unittest
import logging

import pomsets.builder as BuilderModule
import pomsets.command as TaskCommandModule
import pomsets.context as ContextModule
import pomsets.definition as DefinitionModule
import pomsets.library as DefinitionLibraryModule
import pomsets.parameter as ParameterModule
import pomsets.python as PythonModule
import pomsets.task as TaskModule

ID_WORDCOUNT = 'word count_8613fe86-e7fc-4487-b4d2-0989706f8825'
ID_WORDCOUNT_REDUCE = 'word count reducer_08979d5f-6c0d-43b7-9206-dfe69eae6c26'


def pickleAndReloadDefinition(path, definition):

    # try pickling the definition
    # and the reloading it
    filesToDelete = []
    try:

        pomsetContext = ContextModule.wrapPomsetInContext(definition)
        ContextModule.savePomsetAs(pomsetContext, path)

        filesToDelete.append(path)

        pomsetContext = ContextModule.loadPomset(path)
        definition = pomsetContext.reference()
    except Exception, e:
        logging.error("errored with msg >> %s" % e)
        raise
    finally:
        for fileToDelete in filesToDelete:
            if os.path.exists(fileToDelete):
                os.unlink(fileToDelete)
                pass
        pass

    return definition


def createLoadListValuesFromFilesDefinition():
    builder = BuilderModule.Builder()

    path = ['pomsets.python.operator',
            'loadListValuesFromFiles']
    executableObject = builder.createExecutableObject(
        path,
        executableClass=PythonModule.Function)

    pythonEvalContext = builder.createNewAtomicPomset(
        name='load list values from files',
        executableObject = executableObject,
        commandBuilderType = 'python eval',
        executeEnvironmentType = 'python eval')
    
    definition= pythonEvalContext.pomset()
    builder.addPomsetParameter(
        definition, 'eval result',
        {'direction':ParameterModule.PORT_DIRECTION_OUTPUT,
         'commandline':False})
    builder.addPomsetParameter(
        definition, 'file to read',
        {'direction':ParameterModule.PORT_DIRECTION_INPUT})

    definition.id(DefinitionLibraryModule.ID_LOADLISTVALUESFROMFILES)
    definition.isLibraryDefinition(True)

    return definition


def createMakeDirsDefinition():

    builder = BuilderModule.Builder()
    path = ('os', 'makedirs')
    executableObject = builder.createExecutableObject(
        path,
        executableClass=PythonModule.Function)

    pythonEvalContext = builder.createNewAtomicPomset(
        name='os.makedirs',
        executableObject = executableObject,
        commandBuilderType = 'python eval',
        executeEnvironmentType = 'python eval')
    
    definition= pythonEvalContext.pomset()
    builder.addPomsetParameter(
        definition, 'eval result',
        {'direction':ParameterModule.PORT_DIRECTION_OUTPUT,
         'commandline':False})
    builder.addPomsetParameter(
        definition, 'path',
        {'direction':ParameterModule.PORT_DIRECTION_OUTPUT,
         'optional':False})
    builder.addPomsetParameter(
        definition, 'mode',
        {'direction':ParameterModule.PORT_DIRECTION_INPUT,
         'optional':True,
         'default value':0777})

    builder.addParameterOrdering(definition, 'path', 'mode')

    definition.id(DefinitionLibraryModule.ID_OSMAKEDIRS)
    definition.isLibraryDefinition(True)

    return definition


def createStringReplaceDefinition():

    builder = BuilderModule.Builder()
    path = ('pomsets.python.operator', 'stringReplace')
    executableObject = builder.createExecutableObject(
        path,
        executableClass=PythonModule.Function)

    pythonEvalContext = builder.createNewAtomicPomset(
        name='string replace',
        executableObject = executableObject,
        commandBuilderType = 'python eval',
        executeEnvironmentType = 'python eval')
    
    definition= pythonEvalContext.pomset()
    builder.addPomsetParameter(
        definition, 'eval result',
        {'direction':ParameterModule.PORT_DIRECTION_OUTPUT,
         'commandline':False})
    builder.addPomsetParameter(
        definition, 'string to modify',
        {'direction':ParameterModule.PORT_DIRECTION_INPUT,
         'optional':False})
    builder.addPomsetParameter(
        definition, 'original substring',
        {'direction':ParameterModule.PORT_DIRECTION_INPUT,
         'optional':False})
    builder.addPomsetParameter(
        definition, 'new substring',
        {'direction':ParameterModule.PORT_DIRECTION_INPUT,
         'optional':False})
    builder.addPomsetParameter(
        definition, 'count',
        {'direction':ParameterModule.PORT_DIRECTION_INPUT,
         'optional':True,
         'default value':1})

    builder.addParameterOrdering(definition, 'string to modify', 'original substring')
    builder.addParameterOrdering(definition, 'original substring', 'new substring')
    builder.addParameterOrdering(definition, 'new substring', 'count')

    definition.id(DefinitionLibraryModule.ID_STRINGREPLACE)
    definition.isLibraryDefinition(True)

    return definition



def createRangeDefinition():

    builder = BuilderModule.Builder()
    path = (None, 'range')
    executableObject = builder.createExecutableObject(
        path,
        executableClass=PythonModule.Function)

    pythonEvalContext = builder.createNewAtomicPomset(
        name='range',
        executableObject = executableObject,
        commandBuilderType = 'python eval',
        executeEnvironmentType = 'python eval')
    
    definition= pythonEvalContext.pomset()
    builder.addPomsetParameter(
        definition, 'eval result',
        {'direction':ParameterModule.PORT_DIRECTION_OUTPUT,
         'commandline':False})
    builder.addPomsetParameter(
        definition, 'start',
        {'direction':ParameterModule.PORT_DIRECTION_INPUT,
         'optional':True,
         'default value':0})
    builder.addPomsetParameter(
        definition, 'end',
        {'direction':ParameterModule.PORT_DIRECTION_INPUT})
    builder.addPomsetParameter(
        definition, 'step',
        {'direction':ParameterModule.PORT_DIRECTION_INPUT,
         'optional':True})

    builder.addParameterOrdering(definition, 'start', 'end')
    builder.addParameterOrdering(definition, 'end', 'step')

    definition.id(DefinitionLibraryModule.ID_RANGENUMBERS)
    definition.isLibraryDefinition(True)

    return definition



def createWordCountDefinition(dir=None):
    
    parameterOrdering = DefinitionModule.createParameterOrderingTable()
    row = parameterOrdering.addRow()
    row.setColumn('source', 'input file')
    row.setColumn('target', 'output file')

    if dir is None:
        dir = os.getcwd().split(os.path.sep) + ['resources', 'testdata', 'TestExecute']
    command = dir + ['wordcount.py']
    executable = TaskCommandModule.Executable()
    executable.stageable(True)
    executable.path(command)
    executable.staticArgs([])
    
    definition = DefinitionModule.createShellProcessDefinition(
        inputParameters = {
            'input file':{
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE:True,
                ParameterModule.PORT_ATTRIBUTE_ISINPUTFILE:True,
            },
            'output file':{
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE:True,
                ParameterModule.PORT_ATTRIBUTE_ISSIDEEFFECT:True,
            }
        },
        parameterOrderings = parameterOrdering,
        executable = executable
    )
    definition.name('wordcount mapper')
    definition.id(ID_WORDCOUNT)
    definition.isLibraryDefinition(True)
    
    return definition


DEFINITION_WORDCOUNT = createWordCountDefinition()


def createWordCountReduceDefinition(dir=None):
    
    parameterOrdering = DefinitionModule.createParameterOrderingTable()
    row = parameterOrdering.addRow()
    row.setColumn('source', 'input files')
    row.setColumn('target', 'output file')
    
    if dir is None:
        dir =  os.getcwd().split(os.path.sep) + ['resources', 'testdata',
                                                 'TestExecute']
    command = dir+['wordcount_reduce.py']
    executable = TaskCommandModule.Executable()
    executable.stageable(True)
    executable.path(command)
    executable.staticArgs([])
    
    definition = DefinitionModule.createShellProcessDefinition(
        inputParameters = {
            'input files':{
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE:True,
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE_OPTIONS:{
                    ParameterModule.COMMANDLINE_PREFIX_FLAG:['-input']
                    },
                ParameterModule.PORT_ATTRIBUTE_ISINPUTFILE:True,
            },
            'output file':{
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE:True,
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE_OPTIONS:{
                    ParameterModule.COMMANDLINE_PREFIX_FLAG:['-output']
                },
                ParameterModule.PORT_ATTRIBUTE_ISSIDEEFFECT:True
            }
        },
        parameterOrderings = parameterOrdering,
        executable = executable
    )
    
    definition.name('wordcount reducer')
    definition.id(ID_WORDCOUNT_REDUCE)
    definition.isLibraryDefinition(True)
    
    return definition

DEFINITION_WORDCOUNT_REDUCE = createWordCountReduceDefinition()





def createNestDefinition():
    compositeDefinition = DefinitionModule.getNewNestDefinition()
    return compositeDefinition

def createBranchDefinition():
    raise NotImplementedError


def createPomsetContainingBranchDefinition():
    compositeDefinition = DefinitionModule.getNewNestDefinition()

    branchDefinition = DefinitionModule.BranchDefinition()
    
    # the loop definition needs to add one (or more nodes)
    wcNode = branchDefinition.createNode(id='wordcount')
    wcNode.definitionToReference(DEFINITION_WORDCOUNT)
    wcNode.name('wordcount')
    
    reduceNode = branchDefinition.createNode(id='reduce')
    reduceNode.definitionToReference(DEFINITION_WORDCOUNT_REDUCE)
    reduceNode.name('reduce')
    
    branchNode = compositeDefinition.createNode(id='branch')
    branchNode.definitionToReference(branchDefinition)

    return compositeDefinition


def bindBranchDefinitionParameters(definition):
    nodes = [x for x in definition.nodes() if x.id() == 'branch']
    branchNode = nodes[0]

    branchNode.setParameterBinding(
        DefinitionModule.LoopDefinition.PARAMETER_CONDITION_STATE, 
        0)

    branchNode.setParameterBinding(
        DefinitionModule.LoopDefinition.PARAMETER_CONDITION_FUNCTION, 
        "lambda x: x")

    branchNode.setParameterBinding(
        DefinitionModule.LoopDefinition.PARAMETER_CONDITION_MAP, 
        "[(0, 'wordcount'), (1, 'reduce')]")
    
    return


def createPomsetContainingLoopDefinition():
    compositeDefinition = DefinitionModule.getNewNestDefinition()

    loopDefinition = DefinitionModule.LoopDefinition()
    
    # the loop definition needs to add one (or more nodes)
    wcNode = loopDefinition.createNode(id='wordcount')
    wcNode.definitionToReference(DEFINITION_WORDCOUNT)
    wcNode.name('wordcount')

    loopNode = compositeDefinition.createNode(id='loop')
    loopNode.definitionToReference(loopDefinition)
    
    
    return compositeDefinition


def bindLoopDefinitionParameters(definition):
    nodes = [x for x in definition.nodes() if x.id() == 'loop']
    loopNode = nodes[0]

    # TODO:
    # set the parameter bindings on loopNode
    loopNode.setParameterBinding(
        DefinitionModule.LoopDefinition.PARAMETER_INITIAL_STATE, 
        0)
    loopNode.setParameterBinding(
        DefinitionModule.LoopDefinition.PARAMETER_CONTINUE_CONDITION,
        "lambda x: x < 5")
    loopNode.setParameterBinding(
        DefinitionModule.LoopDefinition.PARAMETER_STATE_TRANSITION,
        "lambda x: x+1"
    )
    loopNode.setParameterBinding(
        DefinitionModule.LoopDefinition.PARAMETER_STATE_CONFIGURATION,
        # set the value of input file
        # set the value of output file
        [
            # for some reason
            # exec is missing
            # we we need to do this using multiple strings
            "childTask.setParameterBinding('input file', ['/tmp/loop' + str(parentTask.getParameterBinding(DefinitionModule.LoopDefinition.PARAMETER_STATE))])",
            "childTask.setParameterBinding('output file', ['/tmp/loop' +  str(parentTask.getParameterBinding(DefinitionModule.LoopDefinition.PARAMETER_STATE)+1)])"
         ]
    )
    return




def createPomsetContainingParameterSweep():

    compositeDefinition = DefinitionModule.getNewNestDefinition()

    # setup the reference definition for parameter sweep
    mapperNode = compositeDefinition.createNode(id='mapper')
    mapperNode.definitionToReference(DEFINITION_WORDCOUNT)
    mapperNode.isParameterSweep('input file', True)
    mapperNode.isParameterSweep('output file', True)
    mapperNode.addParameterSweepGroup(['input file', 'output file'])
    mapperNode.isCritical(True)
    mapperNode.name('mapper')

    reducerNode = compositeDefinition.createNode(id='reducer')
    reducerNode.definitionToReference(DEFINITION_WORDCOUNT_REDUCE)
    reducerNode.isCritical(True)
    reducerNode.name('reducer')

    builder = BuilderModule.Builder()
    builder.connect(
        compositeDefinition,
        mapperNode, 'output file',
        reducerNode, 'input files')

    compositeDefinition.name('basic map-reduce')
    
    return compositeDefinition


def bindParameterSweepDefinitionParameters(definition):
    
    # now we add additional info
    # - file staging flags
    # - parameter values
    nodes = [x for x in definition.nodes() if x.id() == 'mapper']
    mapperNode = nodes[0]

    nodes = [x for x in definition.nodes() if x.id() == 'reducer']
    reducerNode = nodes[0]

    (dataNode, parameterToEdit) = \
     mapperNode.getParameterToEdit('input file')
    dataNode.setParameterBinding(parameterToEdit.id(),
                                 ['/tmp/pomsets/text1', '/tmp/pomsets/text2'])

    (dataNode, parameterToEdit) = \
     mapperNode.getParameterToEdit('output file')

    valuesToBind = ['/tmp/pomsets/count1', '/tmp/pomsets/count2']
    dataNode.setParameterBinding(parameterToEdit.id(),
                                 valuesToBind)


    (dataNode, parameterToEdit) = \
     reducerNode.getParameterToEdit('output file')
    dataNode.setParameterBinding(parameterToEdit.id(), ['/tmp/pomsets/count_all'])
    
    # write out to a different location
    mapperNode.parameterStagingRequired('input file', True)
    mapperNode.parameterStagingRequired('output file', True)
    reducerNode.parameterStagingRequired('input files', True)
    reducerNode.parameterStagingRequired('output file', True)
    
    return definition
    



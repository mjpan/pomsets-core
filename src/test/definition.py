from __future__ import with_statement

import os
import sys
import threadpool
import unittest
import logging
import pickle

APP_ROOT = os.getenv('APP_ROOT')
POMSET_ROOT = "%s/pomsets" % APP_ROOT

import pomsets.command as TaskCommandModule
import pomsets.definition as DefinitionModule
import pomsets.library as DefinitionLibraryModule
import pomsets.parameter as ParameterModule
import pomsets.task as TaskModule


def pickleAndReloadDefinition(path, definition):

    # try pickling the definition
    # and the reloading it
    filesToDelete = []
    try:
        pickleCreated = False
        with open(path, 'w') as f:
            pickle.dump(definition, f)
            pickleCreated = True
            pass

        if not pickleCreated:
            raise Exception('failed on creating pickle')

        filesToDelete.append(path)

        DefinitionLibraryModule.loadDefinitionFromFullFilePath(path)
    except Exception, e:
        logging.error("errored with msg >> %s" % e)
        pass
    finally:
        for fileToDelete in filesToDelete:
            if os.path.exists(fileToDelete):
                os.unlink(fileToDelete)
                pass
        pass

    return definition


def createWordCountDefinition():
    
    parameterOrdering = DefinitionModule.createParameterOrderingTable()
    row = parameterOrdering.addRow()
    row.setColumn('source', 'input file')
    row.setColumn('target', 'output file')

    
    # command = ['%s/resources/testdata/TestExecute/wordcount.py' % POMSET_ROOT]
    command = POMSET_ROOT.split(os.path.sep) + ['resources', 'testdata',
               'TestExecute', 'wordcount.py']
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
    
    return definition


DEFINITION_WORDCOUNT = createWordCountDefinition()


def createWordCountReduceDefinition():
    
    parameterOrdering = DefinitionModule.createParameterOrderingTable()
    row = parameterOrdering.addRow()
    row.setColumn('source', 'input files')
    row.setColumn('target', 'output file')
    
    command = POMSET_ROOT.split(os.path.sep) + ['resources', 'testdata',
               'TestExecute', 'wordcount_reduce.py']
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
    
    return definition

DEFINITION_WORDCOUNT_REDUCE = createWordCountReduceDefinition()




def pickleDefinition(path, definition):

    try:
        pickleCreated = False
        with open(path, 'w') as f:
            pickle.dump(definition, f)
            pickleCreated = True
            pass

        if not pickleCreated:
            raise Exception('failed on creating pickle')

    except Exception, e:
        logging.error("errored with msg >> %s" % e)
        pass

    return

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
    mapperNode.name('mapper')

    reducerNode = compositeDefinition.createNode(id='reducer')
    reducerNode.definitionToReference(DEFINITION_WORDCOUNT_REDUCE)
    reducerNode.name('reducer')

    blackboardParameter = \
        ParameterModule.BlackboardParameter('intermediate file', compositeDefinition)
    compositeDefinition.addParameter(blackboardParameter)
    compositeDefinition.connectParameters(
        compositeDefinition, 'intermediate file',
        mapperNode, 'output file'
    )
    compositeDefinition.connectParameters(
        compositeDefinition, 'intermediate file',
        reducerNode, 'input files',
    )

    compositeDefinition.connectParameters(
        mapperNode, 'temporal output',
        reducerNode, 'temporal input',
    )
    
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
    dataNode.setParameterBinding(parameterToEdit,
                                 ['/tmp/text1', '/tmp/text2'])

    (dataNode, parameterToEdit) = \
     mapperNode.getParameterToEdit('output file')
    dataNode.setParameterBinding(parameterToEdit,
                                 ['/tmp/count1', '/tmp/count2'])
    (dataNode, parameterToEdit) = \
     reducerNode.getParameterToEdit('output file')
    dataNode.setParameterBinding(parameterToEdit, ['/tmp/count_all'])
    
    # write out to a different location
    mapperNode.parameterStagingRequired('input file', True)
    mapperNode.parameterStagingRequired('output file', True)
    reducerNode.parameterStagingRequired('input files', True)
    reducerNode.parameterStagingRequired('output file', True)
    
    return definition
    



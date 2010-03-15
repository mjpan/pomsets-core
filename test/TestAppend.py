from __future__ import with_statement

import os
import sys
import threadpool
import unittest
import logging
import shutil

import utils
utils.setPythonPath()
POMSET_ROOT = utils.getPomsetRoot()

import currypy
import pypatterns.command as CommandPatternModule

import cloudpool
import cloudpool.shell as ShellModule

import pomsets.automaton as AutomatonModule
import pypatterns.filter as FilterModule

import pomsets.command as TaskCommandModule
import pomsets.definition as DefinitionModule
import pomsets.library as DefinitionLibraryModule
import pomsets.parameter as ParameterModule
import pomsets.task as TaskModule

import test.TestExecute as BaseModule



def appendTerm(task, *args, **kargs):
    inputBindings = task.parameterBindings()
    newSentence = inputBindings['input sentence'] + inputBindings['terms to append']
    task.setParameterBinding(
        'output sentence', newSentence)
    return True


# TODO: move this to linguistics execute module
def createLinguisticDefinition():

    definition = DefinitionModule.AtomicDefinition()
    definition.functionToExecute(appendTerm)
    
    parameter = ParameterModule.DataParameter(
        id='input sentence', 
        portDirection=ParameterModule.PORT_DIRECTION_INPUT)
    definition.addParameter(parameter)
    
    parameter = ParameterModule.DataParameter(
        id='terms to append', 
        portDirection=ParameterModule.PORT_DIRECTION_INPUT)
    definition.addParameter(parameter)
    
    parameter = ParameterModule.DataParameter(
        id='output sentence', 
        portDirection=ParameterModule.PORT_DIRECTION_OUTPUT)
    definition.addParameter(parameter)
    
    return definition

DEFINITION_LINGUISTIC = createLinguisticDefinition()


class TestCase1(BaseModule.BaseTestClass):
    """
    execute of atomic linguistic function
    """

    def createDefinition(self):

        definition = DEFINITION_LINGUISTIC
        return definition

    def createTask(self, definition):
        task = TaskModule.AtomicTask()
        task.definition(definition)
        task.setParameterBinding('input sentence', ['hello'])
        task.setParameterBinding('terms to append', ['world'])

        return task

    def createCommandBuilderMap(self):
        commandBuilder = TaskCommandModule.CommandBuilder()
        commandBuilderMap = {
            'shell process':commandBuilder,
            'print task':commandBuilder
        }
        return commandBuilderMap


    def assertPostExecute(self):
        BaseModule.BaseTestClass.assertPostExecute(self)

        request = self.request
        task = request.kwds['task']
        assert task.parameterBindings().get('output sentence') == ['hello', 'world']
        return

    def getPicklePath(self):
        return os.path.sep + os.path.join('tmp', 'TestExecute.TestCase1.testExecute2')

    # END class TestCase1
    pass

class TestCase2(BaseModule.BaseTestClass):
    """
    execute of linguistic composite function
    """

    def createDefinition(self):
        atomicDefinition = DEFINITION_LINGUISTIC

        compositeDefinition = DefinitionModule.getNewNestDefinition()
        
        parameterInfo = [
            ('input sentence', ParameterModule.DataParameter, 
             ParameterModule.PORT_DIRECTION_INPUT),
            ('output sentence', ParameterModule.DataParameter, 
             ParameterModule.PORT_DIRECTION_OUTPUT),
            ('synsem data', ParameterModule.BlackboardParameter, None)]
        
        for parameterName, parameterClass, portDirection in parameterInfo:
            parameter = parameterClass(
                id=parameterName, 
                portDirection=portDirection)
            compositeDefinition.addParameter(parameter)
            pass
        
        node = compositeDefinition.createNode(id='node')
        node.definitionToReference(atomicDefinition)
        node.setParameterBinding('terms to append', ['world'])
        
        compositeDefinition.connectNodes(
            compositeDefinition, 'input sentence',
            compositeDefinition, 'synsem data'
        )
        compositeDefinition.connectNodes(
            compositeDefinition, 'synsem data',
            compositeDefinition, 'output sentence'
        )
        compositeDefinition.connectNodes(
            compositeDefinition, 'synsem data',
            node, 'input sentence'
        )
        compositeDefinition.connectNodes(
            node, 'output sentence',
            compositeDefinition, 'synsem data'
        )
        return compositeDefinition

    def createTask(self, definition):
        compositeTask = TaskModule.CompositeTask()
        compositeTask.definition(definition)
        taskGenerator = TaskModule.NestTaskGenerator()
        compositeTask.taskGenerator(taskGenerator)
        compositeTask.setParameterBinding('input sentence', ['hello'])
        return compositeTask

    def createCommandBuilderMap(self):
        commandBuilder = TaskCommandModule.CommandBuilder()
        commandBuilderMap = {
            'shell process':commandBuilder,
            'print task':commandBuilder
        }        
        return commandBuilderMap

    def assertPostExecute(self):
        BaseModule.BaseTestClass.assertPostExecute(self)

        request = self.request
        task = request.kwds['task']
        assert task.parameterBindings().get('output sentence') == ['hello', 'world']
        return 

    def getPicklePath(self):
        return os.path.sep + os.path.join('tmp', 'TestExecute.TestCase2.testExecute2')

    # END class TestCase2
    pass

class TestCase3(BaseModule.BaseTestClass):
    """
    execute of linguistic composite function
    """

    def createDefinition(self):
        atomicDefinition = DEFINITION_LINGUISTIC

        compositeDefinition = DefinitionModule.getNewNestDefinition()
        
        parameterInfo = [
            ('input sentence', ParameterModule.DataParameter, 
             ParameterModule.PORT_DIRECTION_INPUT),
            ('output sentence', ParameterModule.DataParameter, 
             ParameterModule.PORT_DIRECTION_OUTPUT),
            ('synsem data', ParameterModule.BlackboardParameter, None)]
        for parameterName, parameterClass, portDirection in parameterInfo:
            parameter = parameterClass(
                id=parameterName, 
                portDirection=portDirection)
            compositeDefinition.addParameter(parameter)
            pass
        
        expectedSentence = ['a', 'b', 'c', 'd']
        nodes = []
        for index in range(len(expectedSentence)):
            node = compositeDefinition.createNode(id='node%s' % index)
            node.definitionToReference(atomicDefinition)
            compositeDefinition.connectNodes(
                compositeDefinition, 'synsem data',
                node, 'input sentence'
            )
            compositeDefinition.connectNodes(
                node, 'output sentence',
                compositeDefinition, 'synsem data'
            )
            node.setParameterBinding('terms to append', [expectedSentence[index]])
            nodes.append(node)
            pass
        compositeDefinition.connectNodes(
            compositeDefinition, 'input sentence',
            compositeDefinition, 'synsem data'
        )
        compositeDefinition.connectNodes(
            compositeDefinition, 'synsem data',
            compositeDefinition, 'output sentence'
        )

        # add temporal connections
        for sourceNode, targetNode in zip(nodes[:-1], nodes[1:]):
            compositeDefinition.connectNodes(
                sourceNode, 'temporal output',
                targetNode, 'temporal input'
            )
        return compositeDefinition


    def createTask(self, definition):
        compositeTask = TaskModule.CompositeTask()
        compositeTask.definition(definition)
        taskGenerator = TaskModule.NestTaskGenerator()
        compositeTask.taskGenerator(taskGenerator)
        compositeTask.setParameterBinding('input sentence', [])
        return compositeTask

    def createCommandBuilderMap(self):
        commandBuilder = TaskCommandModule.CommandBuilder()
        commandBuilderMap = {
            'shell process':commandBuilder,
            'print task':commandBuilder
        }        
        return commandBuilderMap


    def assertPostExecute(self):
        BaseModule.BaseTestClass.assertPostExecute(self)

        request = self.request
        task = request.kwds['task']
        assert task.parameterBindings().get('output sentence') == ['a', 'b', 'c', 'd']
        
        return 

    def getPicklePath(self):
        return os.path.sep + os.path.join('tmp', 'TestExecute.TestCase3.testExecute2')

    # END class TestCase3
    pass

class TestCase4(BaseModule.BaseTestClass):
    """
    execute of linguistic composite function
    """

    def createDefinition(self):
        atomicDefinition = DEFINITION_LINGUISTIC

        compositeDefinition = DefinitionModule.getNewNestDefinition()
        
        parameterInfo = [
            ('input sentence', ParameterModule.DataParameter, 
             ParameterModule.PORT_DIRECTION_INPUT),
            ('output sentence', ParameterModule.DataParameter, 
             ParameterModule.PORT_DIRECTION_OUTPUT),
            ('synsem data', ParameterModule.BlackboardParameter, None)]
        for parameterName, parameterClass, portDirection in parameterInfo:
            parameter = parameterClass(
                id=parameterName, 
                portDirection=portDirection)
            compositeDefinition.addParameter(parameter)
            pass

        expectedSentence = ['a', 'b', 'c', 'd', 'e', 'f']
        nodes = []
        for index in range(len(expectedSentence)):
            node = compositeDefinition.createNode(id='node%s' % index)
            node.definitionToReference(atomicDefinition)
            compositeDefinition.connectNodes(
                compositeDefinition, 'synsem data',
                node, 'input sentence'
            )
            compositeDefinition.connectNodes(
                node, 'output sentence',
                compositeDefinition, 'synsem data'
            )
            node.setParameterBinding('terms to append', [expectedSentence[index]])
            nodes.append(node)
            pass
        compositeDefinition.connectNodes(
            compositeDefinition, 'input sentence',
            compositeDefinition, 'synsem data'
        )
        compositeDefinition.connectNodes(
            compositeDefinition, 'synsem data',
            compositeDefinition, 'output sentence'
        )

        # add temporal connections
        for sourceIndex, targetIndex in [(0, 1), (1,2), 
                                         (0, 3), (3, 4),
                                         (2, 5), (4, 5)]:
            sourceNode = nodes[sourceIndex]
            targetNode = nodes[targetIndex]
            compositeDefinition.connectNodes(
                sourceNode, 'temporal output',
                targetNode, 'temporal input'
            )
        return compositeDefinition


    def createTask(self, definition):
        compositeTask = TaskModule.CompositeTask()
        compositeTask.definition(definition)
        taskGenerator = TaskModule.NestTaskGenerator()
        compositeTask.taskGenerator(taskGenerator)
        compositeTask.setParameterBinding('input sentence', [])
        return compositeTask

    def createCommandBuilderMap(self):
        commandBuilder = TaskCommandModule.CommandBuilder()
        commandBuilderMap = {
            'shell process':commandBuilder,
            'print task':commandBuilder
        }
        return commandBuilderMap


    def assertPostExecute(self):
        BaseModule.BaseTestClass.assertPostExecute(self)

        request = self.request
        task = request.kwds['task']

        output = task.parameterBindings().get('output sentence')
        for sourceTerm, targetTerm in [
            ('a', 'b'), ('a', 'd'),
            ('b', 'c'), ('d', 'e'),
            ('c', 'f'), ('e', 'f')
            ]:
            assert output.index(sourceTerm) < output.index(targetTerm)
        
        return 

    def getPicklePath(self):
        return os.path.sep + os.path.join('tmp', 'TestExecute.TestCase4.testExecute2')

    # END class TestCase4
    pass


def main():
    
    utils.configLogging()

    suite = unittest.TestSuite()

    suite.addTest(unittest.makeSuite(TestCase1, 'test'))
    suite.addTest(unittest.makeSuite(TestCase2, 'test'))
    suite.addTest(unittest.makeSuite(TestCase3, 'test'))
    suite.addTest(unittest.makeSuite(TestCase4, 'test'))
    
    runner = unittest.TextTestRunner()
    runner.run(suite)
    return

if __name__=="__main__":
    main()


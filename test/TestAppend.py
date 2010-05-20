from __future__ import with_statement

import os
import sys
import threadpool
import unittest
import logging
import shutil

import utils
utils.setPythonPath()

import currypy

import cloudpool
import cloudpool.shell as ShellModule

import pypatterns.filter as FilterModule

import pomsets.builder as BuilderModule
import pomsets.command as TaskCommandModule
import pomsets.definition as DefinitionModule
import pomsets.library as DefinitionLibraryModule
import pomsets.parameter as ParameterModule
import pomsets.python as PythonModule

import test.TestExecute as BaseModule



def appendTerm(task, *args, **kargs):
    inputBindings = task.parameterBindings()
    newSentence = inputBindings['input sentence'] + inputBindings['terms to append']

    task.setParameterBinding(
        'output sentence', newSentence)
    return True


# TODO: move this to linguistics execute module
def createLinguisticDefinition():

    builder = BuilderModule.Builder()

    path = ['test.TestAppend', 'appendTerm']
    executableObject = builder.createExecutableObject(
        path,
        executableClass=PythonModule.Function)

    context = builder.createNewAtomicPomset(
        name='append',
        executableObject=executableObject,
        commandBuilderType='linguistic',
        executeEnvironmentType='linguistic')
    definition = context.pomset()
    definition.functionToExecute(appendTerm)

    builder.addPomsetParameter(
        definition,
        'input sentence',
        {'direction':ParameterModule.PORT_DIRECTION_INPUT})
    
    builder.addPomsetParameter(
        definition,
        'terms to append',
        {'direction':ParameterModule.PORT_DIRECTION_INPUT})
        
    builder.addPomsetParameter(
        definition,
        'output sentence',
        {'direction':ParameterModule.PORT_DIRECTION_OUTPUT})
    
    return definition


DEFINITION_LINGUISTIC = createLinguisticDefinition()



def createCommandBuilderMap():
    commandBuilder = TaskCommandModule.CommandBuilder()
    commandBuilderMap = {
        'shell process':commandBuilder,
        'print task':commandBuilder,
        'linguistic':commandBuilder
    }
    return commandBuilderMap


def createExecuteEnvironmentMap():
    return {
        'shell process':ShellModule.LocalShell(),
        'linguistic':ShellModule.LocalShell(),
        }




class TestCase1(unittest.TestCase, BaseModule.BaseTestClass):
    """
    execute of atomic linguistic function
    """

    def setUp(self):
        BaseModule.BaseTestClass.setUp(self)
        return
 

    def createCommandBuilderMap(self):
        return createCommandBuilderMap()


    def createExecuteEnvironmentMap(self):
        return createExecuteEnvironmentMap()


    def createDefinition(self):

        definition = DEFINITION_LINGUISTIC
        return definition


    def bindTaskParameterValues(self, task):
        task.setParameterBinding('input sentence', ['hello'])
        task.setParameterBinding('terms to append', ['world'])
        return



    def assertPostExecute(self):
        BaseModule.BaseTestClass.assertPostExecute(self)

        request = self.request
        task = request.kwds['task']
        assert task.parameterBindings().get('output sentence') == ['hello', 'world']
        return

    # END class TestCase1
    pass


class TestCase2(unittest.TestCase, BaseModule.BaseTestClass):
    """
    execute of linguistic composite function
    """

    def setUp(self):
        BaseModule.BaseTestClass.setUp(self)
        return
 

    def createCommandBuilderMap(self):
        return createCommandBuilderMap()


    def createExecuteEnvironmentMap(self):
        return createExecuteEnvironmentMap()


    def createDefinition(self):

        atomicDefinition = DEFINITION_LINGUISTIC

        context = self.builder.createNewNestPomset(name='nest')
        compositeDefinition = context.pomset()

        node = self.builder.createNewNode(
            compositeDefinition, name='node',
            definitionToReference=atomicDefinition)
        self.builder.bindParameterValue(
            node, 'terms to append', ['world'])

        self.builder.exposeNodeParameter(
            compositeDefinition, 'input sentence',
            node, 'input sentence',
            shouldCreate=True)
        self.builder.exposeNodeParameter(
            compositeDefinition, 'output sentence',
            node, 'output sentence',
            shouldCreate=True)

        return compositeDefinition


    def bindTaskParameterValues(self, task):
        task.setParameterBinding('input sentence', ['hello'])
        return


    def assertPostExecute(self):
        BaseModule.BaseTestClass.assertPostExecute(self)

        request = self.request
        task = request.kwds['task']

        childTask = task.getChildTasks()[0]
        expected = ['hello', 'world']
        actual = childTask.parameterBindings().get('output sentence')
        self.assertEquals(expected, actual)

        expected = ['hello', 'world']
        actual = task.parameterBindings().get('output sentence')
        self.assertEquals(expected, actual)

        return 

    # END class TestCase2
    pass


class TestCase3(unittest.TestCase, BaseModule.BaseTestClass):
    """
    execute of linguistic composite function
    """

    def setUp(self):
        BaseModule.BaseTestClass.setUp(self)
        return
 

    def createCommandBuilderMap(self):
        return createCommandBuilderMap()


    def createExecuteEnvironmentMap(self):
        return createExecuteEnvironmentMap()


    def createDefinition(self):
        atomicDefinition = DEFINITION_LINGUISTIC

        context = self.builder.createNewNestPomset(name='nest')
        compositeDefinition = context.pomset()

        
        parameterInfo = [
            ('input sentence', ParameterModule.DataParameter, 
             ParameterModule.PORT_DIRECTION_INPUT),
            ('output sentence', ParameterModule.DataParameter, 
             ParameterModule.PORT_DIRECTION_OUTPUT),
            ('synsem data', ParameterModule.BlackboardParameter, None)]
        for parameterName, parameterClass, portDirection in parameterInfo:
            self.builder.addPomsetParameter(
                compositeDefinition,
                parameterName,
                {'direction':portDirection},
                parameterClass=parameterClass)
            pass

        expectedSentence = ['a', 'b', 'c', 'd']
        nodes = []
        for index in range(len(expectedSentence)):
            node = self.builder.createNewNode(
                compositeDefinition, name='node%s' % index,
                definitionToReference=atomicDefinition)

            compositeDefinition._connectParameters(
                compositeDefinition, 'synsem data',
                node, 'input sentence'
            )
            compositeDefinition._connectParameters(
                node, 'output sentence',
                compositeDefinition, 'synsem data'
            )

            self.builder.bindParameterValue(
                node, 'terms to append', [expectedSentence[index]])

            nodes.append(node)
            pass

        compositeDefinition._connectParameters(
            compositeDefinition, 'input sentence',
            compositeDefinition, 'synsem data'
        )
        compositeDefinition._connectParameters(
            compositeDefinition, 'synsem data',
            compositeDefinition, 'output sentence'
        )

        # add temporal connections
        for sourceNode, targetNode in zip(nodes[:-1], nodes[1:]):
            self.builder.connect(
                compositeDefinition,
                sourceNode, 'temporal output',
                targetNode, 'temporal input'
            )

        return compositeDefinition


    def bindTaskParameterValues(self, task):
        task.setParameterBinding('input sentence', [])
        return


    def assertPostExecute(self):
        BaseModule.BaseTestClass.assertPostExecute(self)

        request = self.request
        task = request.kwds['task']

        actual = task.parameterBindings().get('output sentence') 
        expected = ['a', 'b', 'c', 'd']
        self.assertEquals(expected, actual)
        
        return 

    # END class TestCase3
    pass


class TestCase4(unittest.TestCase, BaseModule.BaseTestClass):
    """
    execute of linguistic composite function
    """
    def setUp(self):
        BaseModule.BaseTestClass.setUp(self)
        return
 

    def createCommandBuilderMap(self):
        return createCommandBuilderMap()


    def createExecuteEnvironmentMap(self):
        return createExecuteEnvironmentMap()


    def createDefinition(self):
        atomicDefinition = DEFINITION_LINGUISTIC

        context = self.builder.createNewNestPomset(name='nest')
        compositeDefinition = context.pomset()
        
        parameterInfo = [
            ('input sentence', ParameterModule.DataParameter, 
             ParameterModule.PORT_DIRECTION_INPUT),
            ('output sentence', ParameterModule.DataParameter, 
             ParameterModule.PORT_DIRECTION_OUTPUT),
            ('synsem data', ParameterModule.BlackboardParameter, None)]
        for parameterName, parameterClass, portDirection in parameterInfo:
            self.builder.addPomsetParameter(
                compositeDefinition,
                parameterName,
                {'direction':portDirection},
                parameterClass=parameterClass)
            pass

        expectedSentence = ['a', 'b', 'c', 'd', 'e', 'f']
        nodes = []
        for index in range(len(expectedSentence)):

            node = self.builder.createNewNode(
                compositeDefinition, name='node%s' % index,
                definitionToReference=atomicDefinition)

            compositeDefinition._connectParameters(
                compositeDefinition, 'synsem data',
                node, 'input sentence'
            )
            compositeDefinition._connectParameters(
                node, 'output sentence',
                compositeDefinition, 'synsem data'
            )

            self.builder.bindParameterValue(
                node, 'terms to append', [expectedSentence[index]])
            nodes.append(node)
            pass

        compositeDefinition._connectParameters(
            compositeDefinition, 'input sentence',
            compositeDefinition, 'synsem data'
        )
        compositeDefinition._connectParameters(
            compositeDefinition, 'synsem data',
            compositeDefinition, 'output sentence'
        )

        # add temporal connections
        for sourceIndex, targetIndex in zip(range(5), range(1,6)):

            sourceNode = nodes[sourceIndex]
            targetNode = nodes[targetIndex]
            self.builder.connect(
                compositeDefinition,
                sourceNode, 'temporal output',
                targetNode, 'temporal input'
            )

        return compositeDefinition


    def bindTaskParameterValues(self, task):
        task.setParameterBinding('input sentence', [])
        return


    def assertPostExecute(self):
        BaseModule.BaseTestClass.assertPostExecute(self)

        request = self.request
        task = request.kwds['task']

        actual = task.parameterBindings().get('output sentence') 
        expected = ['a', 'b', 'c', 'd', 'e', 'f']
        self.assertEquals(expected, actual)

        
        return 

    # END class TestCase4
    pass



if __name__=="__main__":
    main()


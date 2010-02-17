from __future__ import with_statement

import os
import sys
import threadpool
import unittest
import logging
import uuid

import pypatterns.filter as FilterModule
import pypatterns.relational as RelationalModule

import pomsets.builder as BuilderModule
import pomsets.command as TaskCommandModule
import pomsets.context as ContextModule
import pomsets.definition as DefinitionModule
import pomsets.parameter as ParameterModule

import pomsets.test_utils as TestDefinitionModule

class TestBuilder(unittest.TestCase):

    def setUp(self):
        self.builder = BuilderModule.Builder()
        return

    def testCreateExecutableObject(self):
        path = ['', 'bin', 'echo']
        executable = self.builder.createExecutableObject(path)

        self.assertTrue(isinstance(executable, TaskCommandModule.Executable))
        self.assertFalse(executable.stageable())
        self.assertEquals(path, executable.path())
        self.assertEquals([], executable.staticArgs())
        return


    def testCreateNewAtomicPomset(self):

        path = ['', 'bin', 'echo']
        executableObject = self.builder.createExecutableObject(path)

        pomsetContext = self.builder.createNewAtomicPomset(
            executableObject=executableObject)
        self.assertTrue(isinstance(pomsetContext, ContextModule.Context))

        pomset = pomsetContext.pomset()
        self.assertTrue(isinstance(pomset, DefinitionModule.AtomicDefinition))

        self.assertTrue(pomset.functionToExecute() is DefinitionModule.executeTaskInEnvironment)

        executable = pomset.executable()
        self.assertTrue(executable is executableObject)


        self.assertEquals('shell process', pomset.commandBuilderType())

        return


    def testAddParameterToAtomicPomset1(self):
        """
        test for basic input value (non-file) parameter
        using all default values for the attribute
        """

        path = ['', 'bin', 'echo']
        executableObject = self.builder.createExecutableObject(path)

        pomsetContext = self.builder.createNewAtomicPomset(
            executableObject=executableObject)
        pomset = pomsetContext.pomset()

        # test for regular input value
        attributes = {
            'direction':ParameterModule.PORT_DIRECTION_INPUT,
            }
        parameterName = 'input value'
        parameter = self.builder.addPomsetParameter(
            pomset, parameterName, attributes)

        self.assertTrue(pomset.hasParameter(parameterName))
        self.assertTrue(pomset.getParameter(parameterName) is parameter)

        self.assertEquals(False, parameter.optional())
        self.assertEquals(True, parameter.active())
        self.assertEquals(
            True,
            parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_COMMANDLINE))
        self.assertEquals(
            False,
            parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_ISINPUTFILE))
        self.assertEquals(
            False,
            parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_ISSIDEEFFECT))
        self.assertEquals(
            False,
            parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_ISLIST))
        self.assertEquals(
            False,
            parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_ISENUM))

        commandlineOptions = parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_COMMANDLINE_OPTIONS)
        self.assertEquals(
            [], commandlineOptions[ParameterModule.COMMANDLINE_PREFIX_FLAG])
        self.assertEquals(
            False, commandlineOptions[ParameterModule.COMMANDLINE_PREFIX_FLAG_DISTRIBUTE])
        self.assertEquals(
            {}, commandlineOptions[ParameterModule.COMMANDLINE_ENUM_MAP])


        return


    def testAddParameterToAtomicPomset2(self):
        """
        test for default input file parameter
        """
        path = ['', 'bin', 'echo']
        executableObject = self.builder.createExecutableObject(path)

        pomsetContext = self.builder.createNewAtomicPomset(
            executableObject=executableObject)
        pomset = pomsetContext.pomset()

        attributes = {
            'direction':ParameterModule.PORT_DIRECTION_INPUT,
            'file':True
            }
        parameterName = 'input file'
        parameter = self.builder.addPomsetParameter(
            pomset, parameterName, attributes)

        self.assertTrue(pomset.hasParameter(parameterName))
        self.assertTrue(pomset.getParameter(parameterName) is parameter)

        self.assertEquals(False, parameter.optional())
        self.assertEquals(True, parameter.active())
        self.assertEquals(
            True,
            parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_COMMANDLINE))
        self.assertEquals(
            True,
            parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_ISINPUTFILE))
        self.assertEquals(
            False,
            parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_ISSIDEEFFECT))
        self.assertEquals(
            False,
            parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_ISLIST))
        self.assertEquals(
            False,
            parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_ISENUM))

        commandlineOptions = parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_COMMANDLINE_OPTIONS)
        self.assertEquals(
            [], commandlineOptions[ParameterModule.COMMANDLINE_PREFIX_FLAG])
        self.assertEquals(
            False, commandlineOptions[ParameterModule.COMMANDLINE_PREFIX_FLAG_DISTRIBUTE])
        self.assertEquals(
            {}, commandlineOptions[ParameterModule.COMMANDLINE_ENUM_MAP])

        return


    def testAddParameterToAtomicPomset3(self):
        """
        test for default output file parameter
        """

        path = ['', 'bin', 'echo']
        executableObject = self.builder.createExecutableObject(path)

        pomsetContext = self.builder.createNewAtomicPomset(
            executableObject=executableObject)
        pomset = pomsetContext.pomset()

        attributes = {
            'direction':ParameterModule.PORT_DIRECTION_OUTPUT,
            'file':True
            }
        parameterName = 'output file'
        parameter = self.builder.addPomsetParameter(
            pomset, parameterName, attributes)

        self.assertTrue(pomset.hasParameter(parameterName))
        self.assertTrue(pomset.getParameter(parameterName) is parameter)

        self.assertEquals(False, parameter.optional())
        self.assertEquals(True, parameter.active())
        self.assertEquals(
            True,
            parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_COMMANDLINE))
        self.assertEquals(
            False,
            parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_ISINPUTFILE))
        self.assertEquals(
            True,
            parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_ISSIDEEFFECT))
        self.assertEquals(
            False,
            parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_ISLIST))
        self.assertEquals(
            False,
            parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_ISENUM))

        commandlineOptions = parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_COMMANDLINE_OPTIONS)
        self.assertEquals(
            [], commandlineOptions[ParameterModule.COMMANDLINE_PREFIX_FLAG])
        self.assertEquals(
            False, commandlineOptions[ParameterModule.COMMANDLINE_PREFIX_FLAG_DISTRIBUTE])
        self.assertEquals(
            {}, commandlineOptions[ParameterModule.COMMANDLINE_ENUM_MAP])

        return


    def testAddParameterOrdering(self):
        """
        This verifies that we can create a parameter ordering
        """

        path = ['', 'bin', 'echo']
        executableObject = self.builder.createExecutableObject(path)

        pomsetContext = self.builder.createNewAtomicPomset(
            executableObject=executableObject)
        pomset = pomsetContext.pomset()

        attributes = {
            'direction':ParameterModule.PORT_DIRECTION_INPUT,
            'file':True
            }
        sourceParameterName = 'input file'
        parameter = self.builder.addPomsetParameter(
            pomset, sourceParameterName, attributes)

        attributes = {
            'direction':ParameterModule.PORT_DIRECTION_OUTPUT,
            'file':True
            }
        targetParameterName = 'output file'
        parameter = self.builder.addPomsetParameter(
            pomset, targetParameterName, attributes)


        parameterOrderings = pomset.parameterOrderingTable()

        self.assertTrue(parameterOrderings.rowCount() is 0)
        self.builder.addParameterOrdering(
            pomset, sourceParameterName, targetParameterName)
        
        self.assertTrue(parameterOrderings.rowCount() is 1)
        rowValue = parameterOrderings.rows()[0].values()
        
        self.assertEquals(sourceParameterName, rowValue['source'])
        self.assertEquals(targetParameterName, rowValue['target'])

        return


    def testCreateNewNestPomset(self):

        pomsetContext = self.builder.createNewNestPomset()
        self.assertTrue(isinstance(pomsetContext, ContextModule.Context))

        pomset = pomsetContext.pomset()
        self.assertTrue(isinstance(pomset, DefinitionModule.NestDefinition))

        return

    def testCreateNewNode(self):
        pomsetContext = self.builder.createNewNestPomset()
        pomset = pomsetContext.pomset()

        mapDefinition = TestDefinitionModule.DEFINITION_WORDCOUNT

        node = self.builder.createNewNode(
            pomset, definitionToReference=mapDefinition)
        self.assertTrue(isinstance(node, DefinitionModule.ReferenceDefinition))

        self.assertTrue(node.definitionToReference(),
                        mapDefinition)

        return


    def testCanConnect(self):
        pomsetContext = self.builder.createNewNestPomset()
        pomset = pomsetContext.pomset()
        
        mapDefinition = TestDefinitionModule.DEFINITION_WORDCOUNT
        reduceDefinition = TestDefinitionModule.DEFINITION_WORDCOUNT_REDUCE

        node1 = self.builder.createNewNode(
            pomset, definitionToReference=mapDefinition)
        node2 = self.builder.createNewNode(
            pomset, definitionToReference=reduceDefinition)

        # ensure that cannon connect a parameter to itself
        self.assertFalse(
            self.builder.canConnect(
                pomset,
                node1, 'input file',
                node1, 'input file'))

        # ensure false if the paraemter does not exist
        self.assertFalse(
            self.builder.canConnect(
                pomset,
                node1, 'input',
                node2, 'output file'))
        self.assertFalse(
            self.builder.canConnect(
                pomset,
                node1, 'input file',
                node2, 'output'))

        # ensure cannot connect parameters if they are of the same direction
        self.assertFalse(
            self.builder.canConnect(
                pomset,
                node1, 'input file',
                node2, 'input files'))

        # ensure that we cannot connect if not the same time
        self.assertFalse(
            self.builder.canConnect(
                pomset,
                node1, 'temporal input',
                node2, 'input files'))
        self.assertFalse(
            self.builder.canConnect(
                pomset,
                node1, 'input file',
                node2, 'temporal input'))
        self.assertFalse(
            self.builder.canConnect(
                pomset,
                node1, 'temporal input',
                node2, 'output file'))
        self.assertFalse(
            self.builder.canConnect(
                pomset,
                node1, 'output file',
                node2, 'temporal input'))

        # ensure that the target parameter needs to be of direction input
        self.assertFalse(
            self.builder.canConnect(
                pomset,
                node1, 'output file',
                node2, 'output file'))
        
        # ensure that source parameter needs to be of direction output
        self.assertFalse(
            self.builder.canConnect(
                pomset,
                node1, 'input file',
                node2, 'input files'))
        
        # now ensure we can connect the following
        self.assertTrue(
            self.builder.canConnect(
                pomset,
                node1, 'output file',
                node2, 'input files'))
        self.assertTrue(
            self.builder.canConnect(
                pomset,
                node1, 'temporal output',
                node2, 'temporal input'))

        return


    def testConnectDataParameters(self):

        pomsetContext = self.builder.createNewNestPomset()
        pomset = pomsetContext.pomset()
        
        mapDefinition = TestDefinitionModule.DEFINITION_WORDCOUNT
        reduceDefinition = TestDefinitionModule.DEFINITION_WORDCOUNT_REDUCE

        sourceNode = self.builder.createNewNode(
            pomset, definitionToReference=mapDefinition)
        targetNode = self.builder.createNewNode(
            pomset, definitionToReference=reduceDefinition)

        sourceParameterId = 'output file'
        targetParameterId = 'input files'
        bbParameterName = '%s.%s-%s.%s' % (sourceNode.name(),
                                           sourceParameterId,
                                           targetNode.name(),
                                           targetParameterId)
        self.assertFalse(pomset.hasParameter(bbParameterName))

        # connect the data parameters
        path = self.builder.connect(
            pomset,
            sourceNode, sourceParameterId,
            targetNode, targetParameterId)

        # assert the parameter connection path
        self.assertEquals(len(path), 7)
        filter = pomset.constructParameterConnectionFilter(
            sourceNode, sourceParameterId,
            targetNode, targetParameterId)
        rows = [x for x in 
            pomset.parameterConnectionPathTable().retrieve(
                filter, ['path', 'additional parameters'])]
        self.assertEquals(len(rows), 1)
        row = rows[0]
        self.assertEquals(tuple([path[2],path[4]]),
                          row[0])
        self.assertEquals(tuple([path[3]]), row[1])

        # assert two additional entries in parameter connections
        connections = RelationalModule.Table.reduceRetrieve(
            pomset.parameterConnectionsTable(),
            filter, ['parameter connection'])
        self.assertEquals(0, len(connections))

        # verify that each of the individual connections
        # are in the table
        filter = pomset.constructParameterConnectionFilter(
            sourceNode, sourceParameterId,
            pomset, bbParameterName)
        connections = RelationalModule.Table.reduceRetrieve(
            pomset.parameterConnectionsTable(),
            filter, ['parameter connection'])
        self.assertEquals(1, len(connections))
        filter = pomset.constructParameterConnectionFilter(
            pomset, bbParameterName,
            targetNode, targetParameterId)
        connections = RelationalModule.Table.reduceRetrieve(
            pomset.parameterConnectionsTable(),
            filter, ['parameter connection'])
        self.assertEquals(1, len(connections))
        
        # assert an additional internal blackboard parameter
        self.assertTrue(pomset.hasParameter(bbParameterName))



        self.builder.disconnect(pomset,
                                sourceNode, sourceParameterId,
                                targetNode, targetParameterId)

        filter = pomset.constructParameterConnectionFilter(
            sourceNode, sourceParameterId,
            targetNode, targetParameterId)
        rows = [x for x in 
            pomset.parameterConnectionPathTable().retrieve(
                filter, ['path', 'additional parameters'])]
        self.assertEquals(len(rows), 0)

        # verify that each of the individual connections
        # are in the table
        filter = pomset.constructParameterConnectionFilter(
            sourceNode, sourceParameterId,
            pomset, bbParameterName)
        connections = RelationalModule.Table.reduceRetrieve(
            pomset.parameterConnectionsTable(),
            filter, ['parameter connection'])
        self.assertEquals(0, len(connections))
        filter = pomset.constructParameterConnectionFilter(
            pomset, bbParameterName,
            targetNode, targetParameterId)
        connections = RelationalModule.Table.reduceRetrieve(
            pomset.parameterConnectionsTable(),
            filter, ['parameter connection'])
        self.assertEquals(0, len(connections))


        self.assertFalse(pomset.hasParameter(bbParameterName))
        return


    def testConnectTemporalParameters(self):

        pomsetContext = self.builder.createNewNestPomset()
        pomset = pomsetContext.pomset()
        
        mapDefinition = TestDefinitionModule.DEFINITION_WORDCOUNT
        reduceDefinition = TestDefinitionModule.DEFINITION_WORDCOUNT_REDUCE

        sourceNode = self.builder.createNewNode(
            pomset, definitionToReference=mapDefinition)
        targetNode = self.builder.createNewNode(
            pomset, definitionToReference=reduceDefinition)

        sourceParameterId = 'temporal output'
        targetParameterId = 'temporal input'
        parameters = pomset.getParametersByFilter(FilterModule.TRUE_FILTER)
        numParameters = len(parameters)
        path = self.builder.connect(
            pomset,
            sourceNode, sourceParameterId,
            targetNode, targetParameterId)

        # assert parameter connection path
        filter = pomset.constructParameterConnectionFilter(
            sourceNode, sourceParameterId,
            targetNode, targetParameterId)
        rows = [x for x in 
            pomset.parameterConnectionPathTable().retrieve(
                filter, ['path', 'additional parameters'])]
        self.assertEquals(len(rows), 1)
        row = rows[0]
        self.assertEquals(1, len(row[0]))
        self.assertTrue(path[2] is row[0][0])
        self.assertEquals(0, len(row[1]))

        # assert no additional parameters
        parameters = pomset.getParametersByFilter(FilterModule.TRUE_FILTER)
        self.assertEquals(numParameters, len(parameters))
        # assert the parameter connection
        filter = pomset.constructParameterConnectionFilter(
            sourceNode, sourceParameterId,
            targetNode, targetParameterId)
        connections = RelationalModule.Table.reduceRetrieve(
            pomset.parameterConnectionsTable(),
            filter, ['parameter connection'])
        self.assertEquals(1, len(connections))


        self.builder.disconnect(
            pomset,
            sourceNode, sourceParameterId,
            targetNode, targetParameterId)
        parameters = pomset.getParametersByFilter(FilterModule.TRUE_FILTER)
        self.assertEquals(numParameters, len(parameters))
        filter = pomset.constructParameterConnectionFilter(
            sourceNode, sourceParameterId,
            targetNode, targetParameterId)
        connections = RelationalModule.Table.reduceRetrieve(
            pomset.parameterConnectionsTable(),
            filter, ['parameter connection'])
        self.assertEquals(0, len(connections))
        

        return
            

    # END class TestBuilder
    pass

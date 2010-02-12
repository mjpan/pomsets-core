from __future__ import with_statement

import os
import sys
import threadpool
import unittest
import logging
import uuid

import pomsets.builder as BuilderModule
import pomsets.context as ContextModule
import pomsets.definition as DefinitionModule
import pomsets.parameter as ParameterModule

import utils.definition as TestDefinitionModule

class TestBuilder(unittest.TestCase):

    def setUp(self):
        self.builder = BuilderModule.Builder()
        return

    def testCreateNewPomset(self):

        pomsetContext = self.builder.createNewPomset()
        self.assertTrue(isinstance(pomsetContext, ContextModule.Context))

        pomset = pomsetContext.pomset()
        self.assertTrue(isinstance(pomset, DefinitionModule.Definition))

        return

    def testCreateNewNode(self):
        pomsetContext = self.builder.createNewPomset()
        pomset = pomsetContext.pomset()

        mapDefinition = TestDefinitionModule.DEFINITION_WORDCOUNT

        node = self.builder.createNewNode(
            pomset, definitionToReference=mapDefinition)
        self.assertTrue(isinstance(node, DefinitionModule.ReferenceDefinition))

        self.assertTrue(node.definitionToReference(),
                        mapDefinition)

        return


    def testCanConnect(self):
        pomsetContext = self.builder.createNewPomset()
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
                node1, 'input file',
                node1, 'input file'))

        # ensure false if the paraemter does not exist
        self.assertFalse(
            self.builder.canConnect(
                node1, 'input',
                node2, 'output file'))
        self.assertFalse(
            self.builder.canConnect(
                node1, 'input file',
                node2, 'output'))

        # ensure cannot connect parameters if they are of the same direction
        self.assertFalse(
            self.builder.canConnect(
                node1, 'input file',
                node2, 'input files'))

        # ensure that we cannot connect if not the same time
        self.assertFalse(
            self.builder.canConnect(
                node1, 'temporal input',
                node2, 'input files'))
        self.assertFalse(
            self.builder.canConnect(
                node1, 'input file',
                node2, 'temporal input'))
        self.assertFalse(
            self.builder.canConnect(
                node1, 'temporal input',
                node2, 'output file'))
        self.assertFalse(
            self.builder.canConnect(
                node1, 'output file',
                node2, 'temporal input'))

        # ensure that the target parameter needs to be of direction input
        self.assertFalse(
            self.builder.canConnect(
                node1, 'output file',
                node2, 'output file'))
        
        # ensure that source parameter needs to be of direction output
        self.assertFalse(
            self.builder.canConnect(
                node1, 'input file',
                node2, 'input files'))
        
        # now ensure we can connect the following
        self.assertTrue(
            self.builder.canConnect(
                node1, 'output file',
                node2, 'input files'))
        self.assertTrue(
            self.builder.canConnect(
                node1, 'temporal output',
                node2, 'temporal input'))

        return


    def testConnect(self):

        pomsetContext = self.builder.createNewPomset()
        pomset = pomsetContext.pomset()
        
        mapDefinition = TestDefinitionModule.DEFINITION_WORDCOUNT
        reduceDefinition = TestDefinitionModule.DEFINITION_WORDCOUNT_REDUCE

        node1 = self.builder.createNewNode(
            pomset, definitionToReference=mapDefinition)
        node2 = self.builder.createNewNode(
            pomset, definitionToReference=reduceDefinition)

        # ensure that cannon connect a parameter to itself
        self.builder.connect(
            pomset,
            node1, 'output file',
            node2, 'input files')
        
        self.builder.connect(
            pomset,
            node1, 'temporal output',
            node2, 'temporal input')
        return
            

    # END class TestBuilder
    pass

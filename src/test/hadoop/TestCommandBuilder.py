from __future__ import with_statement

import os
import sys
import threadpool
import unittest
import logging
import pickle


# TODO:
# figure out how to use the code in test.util
# to configure the environment correctly
APP_ROOT = os.getenv('APP_ROOT')
sys.path.insert(0, '%s/currypy/src' % APP_ROOT)
sys.path.insert(0, '%s/pypatterns/src' % APP_ROOT)
sys.path.insert(0, '%s/cloudpool/src' % APP_ROOT)

POMSET_ROOT = "%s/pomsets/src" % APP_ROOT
sys.path.insert(0, POMSET_ROOT)

import currypy


import pomsets.automaton as AutomatonModule
import pypatterns.filter as FilterModule

import pomsets.command as TaskCommandModule
import pomsets.definition as DefinitionModule
import pomsets.parameter as ParameterModule
import pomsets.task as TaskModule

import pomsets.hadoop as HadoopModule

import test.hadoop.definition as TestDefinitionModule
import test.util as UtilsModule


class TestHadoop(unittest.TestCase):

    def setUp(self):
        return
    
    def tearDown(self):
        return
    
    
    def testBasic(self):

        
        definition = TestDefinitionModule.createHadoopWordcountDefinition()
        
        task = TaskModule.AtomicTask()
        task.definition(definition)
        
        task.setParameterBinding('input file', ['hadoopInput'])
        task.setParameterBinding('output file', ['hadoopOutput'])
        
        #commandBuilder = HadoopModule.JarCommandBuilder(
        #    buildCommandFunction=TaskCommandModule.buildCommandFunction_commandlineArgsOnly
        #)
        commandBuilder = TaskCommandModule.CommandBuilder(
            TaskCommandModule.buildCommandFunction_commandlineArgs
        )
        
        
        command = commandBuilder.buildCommand(task)
        self.assertEquals(
            command,
            ['%s/bin/hadoop' % os.getenv('HADOOP_HOME'),
             'jar', 
             os.getenv('HADOOP_JAR_EXAMPLES'),
             'wordcount',
             'hadoopInput', 'hadoopOutput']
        )
        
        
        return
    
    def testStreaming(self):
        definition = TestDefinitionModule.createHadoopStreamingDefinition()
        
        task = TaskModule.AtomicTask()
        task.definition(definition)
        
        task.setParameterBinding('input file', ['hadoopInput'])
        task.setParameterBinding('output file', ['hadoopOutput'])
        task.setParameterBinding('mapper', ['myMapper'])
        task.setParameterBinding('reducer', ['myReducer'])
        
        #commandBuilder = HadoopModule.JarCommandBuilder(
        #    buildCommandFunction=TaskCommandModule.buildCommandFunction_commandlineArgsOnly
        #)
        commandBuilder = TaskCommandModule.CommandBuilder(
            TaskCommandModule.buildCommandFunction_commandlineArgs
        )

        
        command = commandBuilder.buildCommand(task)
        self.assertEquals(
            ['%s/bin/hadoop' % os.getenv('HADOOP_HOME'),
             'jar', 
             os.getenv('HADOOP_JAR_STREAMING'),
             '-input', 'hadoopInput', 
             '-output', 'hadoopOutput',
             '-mapper', 'myMapper',
             '-reducer', 'myReducer'],
            command,

        )

        return
    
    def testPipes(self):
        # bin/hadoop pipes -input inputPath -output outputPath -program path/to/pipes/program/executable
  
        
        definition = TestDefinitionModule.createHadoopPipesDefinition()
        
        task = TaskModule.AtomicTask()
        task.definition(definition)
        
        task.setParameterBinding('input file', ['hadoopInput'])
        task.setParameterBinding('output file', ['hadoopOutput'])
        task.setParameterBinding('program', ['pipesProgram'])
        
        commandBuilder = TaskCommandModule.CommandBuilder(
            TaskCommandModule.buildCommandFunction_commandlineArgs
        )
        #commandBuilder = HadoopModule.PipesCommandBuilder(
        #    buildCommandFunction=TaskCommandModule.buildCommandFunction_commandlineArgsOnly
        #)
        
        command = commandBuilder.buildCommand(task)
        self.assertEquals(
            ['%s/bin/hadoop' % os.getenv('HADOOP_HOME'),
             'pipes',
             '-program', 'pipesProgram',
             '-input', 'hadoopInput', 
             '-output', 'hadoopOutput'],
            command,
        )

        return
    
    
    # END class TestHadoopCommandBuilder
    pass



def main():
    UtilsModule.configLogging()

    suite = unittest.TestSuite()
    
    suite.addTest(unittest.makeSuite(TestHadoop, 'test'))
    
    runner = unittest.TextTestRunner()
    runner.run(suite)
    return

if __name__=="__main__":
    main()


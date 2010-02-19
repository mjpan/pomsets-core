from __future__ import with_statement

import os
import sys
import threadpool
import unittest
import logging

import currypy


import pomsets.automaton as AutomatonModule
import pypatterns.filter as FilterModule

import pomsets.command as TaskCommandModule
import pomsets.definition as DefinitionModule
import pomsets.parameter as ParameterModule
import pomsets.task as TaskModule

import pomsets.hadoop as HadoopModule

import test.hadoop.definition as TestDefinitionModule


if not os.getenv('HADOOP_HOME'):
    os.environ['HADOOP_HOME'] = os.path.sep.join(['', 'hadoop'])



class TestBuildCommand(unittest.TestCase):


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
        
        commandBuilder = TaskCommandModule.CommandBuilder(
            TaskCommandModule.buildCommandFunction_commandlineArgs
        )
        
        command = commandBuilder.buildCommand(task)
        self.assertEquals(
            command,
            [os.path.join(TestDefinitionModule.getHomeLocation(),
                          'bin', 'hadoop'),
             'jar', 
             TestDefinitionModule.getExamplesJar(),
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
        
        commandBuilder = TaskCommandModule.CommandBuilder(
            TaskCommandModule.buildCommandFunction_commandlineArgs
        )

        
        command = commandBuilder.buildCommand(task)
        self.assertEquals(
            [os.path.join(TestDefinitionModule.getHomeLocation(),
                          'bin', 'hadoop'),
             'jar', 
             TestDefinitionModule.getStreamingJar(),
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
    # UtilsModule.configLogging()

    suite = unittest.TestSuite()
    
    suite.addTest(unittest.makeSuite(TestHadoop, 'test'))
    
    runner = unittest.TextTestRunner()
    runner.run(suite)
    return

if __name__=="__main__":
    main()


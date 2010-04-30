from __future__ import with_statement

import os
import sys
import threadpool
import unittest
import logging
import shutil

# TODO:
# figure out how to use the code in test.util
# to configure the environment correctly
#APP_ROOT = os.getenv('APP_ROOT')

#POMSET_ROOT = "%s/pomsets/src" % APP_ROOT
#sys.path.insert(0, POMSET_ROOT)

if not os.getenv('HADOOP_HOME'):
    os.environ['HADOOP_HOME'] = os.path.sep.join(['', 'hadoop'])


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

import pomsets.hadoop as HadoopModule

import definition as GeneratePomsetsModule
import test.TestExecute as BaseModule
# import test.util as UtilsModule


#def getHadoopExecutable():
#    return os.path.join(os.getenv('HADOOP_HOME'), 'bin', 'hadoop')


if not os.getenv('HADOOP_HOME'):
    os.environ['HADOOP_HOME'] = os.path.sep.join(['', 'hadoop'])

class TestHadoopBase(BaseModule.BaseTestClass):
    """
    Verifies that we can execute Hadoop nodes
    """
    
    def setUp(self):

        self.assertTrue(os.path.exists(HadoopModule.getExecutablePath()))

        BaseModule.BaseTestClass.setUp(self)

        # TODO:
        # will need to place test data into HDFS
        
        self.removeOutputFiles()
        return
    
    def tearDown(self):
        self.removeOutputFiles()
        BaseModule.BaseTestClass.tearDown(self)
        return
    
    def fileExists(self, file):
        f = os.system('%s fs -stat %s' % (HadoopModule.getExecutablePath(), file))
        return f is 0
    
    

    def createCommandBuilderMap(self):
        defaultCommandBuilder = TaskCommandModule.CommandBuilder(
            TaskCommandModule.buildCommandFunction_commandlineArgs
        )
        
        #hadoopJarCommandBuilder =  HadoopModule.JarCommandBuilder(
        #    buildCommandFunction=TaskCommandModule.buildCommandFunction_commandlineArgsOnly
        #)

        #hadoopPipesCommandBuilder = HadoopModule.PipesCommandBuilder(
        #    buildCommandFunction=TaskCommandModule.buildCommandFunction_commandlineArgsOnly
        #)

        commandBuilderMap = {
            'shell process':defaultCommandBuilder,
            'print task':defaultCommandBuilder,
            #'hadoop jar':hadoopJarCommandBuilder,
            #'hadoop pipes':hadoopPipesCommandBuilder
        }
        return commandBuilderMap
    
    

    def removeOutputFiles(self):
        return
    
    # END class TestHadoopBase
    pass


class TestHadoop1(TestHadoopBase, unittest.TestCase):
    """
    Verifies that we can execute Hadoop nodes
    """
    
    DIR_OUTPUT = 'TestHadoop1-output'

    def setUp(self):
        TestHadoopBase.setUp(self)
        return

    def tearDown(self):
        TestHadoopBase.tearDown(self)
        return


    def createTask(self, definition):
        task = TaskModule.AtomicTask()
        task.definition(definition)
        task.setParameterBinding('input file', ['input'])
        task.setParameterBinding('output file', [TestHadoop1.DIR_OUTPUT])
        return task
    
    
    # setup hadoop
    # create the pomset containing hadoop nodes
    # execute the pomset
    def createDefinition(self):
        definition = GeneratePomsetsModule.createHadoopWordcountDefinition()
        return definition
    
    #def getPicklePath(self):
    #    return os.path.sep + \
    #           os.path.join('tmp', 'TestExecute.TestHadoop1.testExecute2')
    
    def assertPostExecute(self):
        BaseModule.BaseTestClass.assertPostExecute(self)
        
        # now verify that the output file exists
        self.assertTrue(self.fileExists(TestHadoop1.DIR_OUTPUT))
        self.assertTrue(
            self.fileExists(
                os.path.join(TestHadoop1.DIR_OUTPUT, 'part-r-00000')))
        
        # TODO:
        # copy the resulting output
        # and verify the results
        
        return
    
    def removeOutputFiles(self):
        # ensure that the outdir does not exist
        if self.fileExists(TestHadoop1.DIR_OUTPUT):
            os.system('%s fs -rmr %s' % (HadoopModule.getExecutablePath(), TestHadoop1.DIR_OUTPUT))
        return
    
    # END class TestHadoop1
    pass


class TestHadoopStreaming1(TestHadoopBase, unittest.TestCase):
    """
    Verifies that we can execute Hadoop streaming nodes
    """

    DIR_OUTPUT = 'TestHadoopStreaming1-output'
    
    def setUp(self):
        TestHadoopBase.setUp(self)
        return

    def tearDown(self):
        TestHadoopBase.tearDown(self)
        return

    def createTask(self, definition):
        task = TaskModule.AtomicTask()
        task.definition(definition)
        task.setParameterBinding('input file', ['input'])
        task.setParameterBinding('output file', [TestHadoopStreaming1.DIR_OUTPUT])
        task.setParameterBinding(
            'reducer', 
            [os.path.join('resources', 'testdata', 'TestHadoop', 'wordcount', 'reducer.py')])
        task.setParameterBinding(
            'mapper', 
            [os.path.join('resources', 'testdata', 'TestHadoop', 'wordcount', 'mapper.py')])
        
        return task
    
    
    # setup hadoop
    # create the pomset containing hadoop nodes
    # execute the pomset
    def createDefinition(self):
        definition = GeneratePomsetsModule.createHadoopStreamingDefinition()
        return definition
    
    #def getPicklePath(self):
    #    return os.path.sep + \
    #           os.path.join('tmp', 'TestExecute.TestHadoopStreaming1.testExecute2')
    
    def assertPostExecute(self):
        BaseModule.BaseTestClass.assertPostExecute(self)
        
        # now verify that the output file exists
        self.assertTrue(self.fileExists(TestHadoopStreaming1.DIR_OUTPUT))
        self.assertTrue(
            self.fileExists(
                os.path.join(TestHadoopStreaming1.DIR_OUTPUT, 'part-00000')))
        
        # TODO:
        # copy the resulting output
        # and verify the results
        
        return
    
    def removeOutputFiles(self):
        # ensure that the outdir does not exist
        if self.fileExists(TestHadoopStreaming1.DIR_OUTPUT):
            os.system('%s fs -rmr %s' % (HadoopModule.getExecutablePath(), TestHadoopStreaming1.DIR_OUTPUT))
        return
    
    # END class TestHadoopStreaming1
    pass


def main():
    
    # UtilsModule.configLogging()

    suite = unittest.TestSuite()

    #suite.addTest(unittest.makeSuite(TestHadoop1, 'test'))
    suite.addTest(unittest.makeSuite(TestHadoopStreaming1, 'test'))
    #suite.addTest(unittest.makeSuite(TestHadoopPipes1, 'test'))
    
    runner = unittest.TextTestRunner()
    runner.run(suite)
    return

if __name__=="__main__":
    main()


from __future__ import with_statement

import os
import sys
import threadpool
import unittest
import logging
import pickle
import shutil

import util
util.setPythonPath()
POMSET_ROOT = util.getPomsetRoot()

"""
APP_ROOT = os.getenv('APP_ROOT')
sys.path.insert(0, '%s/pypatterns/src' % APP_ROOT)
sys.path.insert(0, '%s/currypy/src' % APP_ROOT)
sys.path.insert(0, '%s/cloudpool/src' % APP_ROOT)

POMSET_ROOT = "%s/pomsets" % APP_ROOT
sys.path.insert(0, '%s/src' % POMSET_ROOT)
"""

import currypy
import pypatterns.command as CommandPatternModule

import cloudpool.shell as ShellModule

import pomsets.automaton as AutomatonModule
import pypatterns.filter as FilterModule

import pomsets.command as TaskCommandModule
import pomsets.definition as DefinitionModule
import pomsets.library as DefinitionLibraryModule
import pomsets.parameter as ParameterModule
import pomsets.task as TaskModule

import test.definition as TestDefinitionModule
import test.generate_library as GenerateLibraryModule


def runBootstrapLoader(automaton, library):
    
    definition = library.definitions()[GenerateLibraryModule.ID_BOOTSTRAPLOADER]
    
    task = TaskModule.CompositeTask()
    task.definition(definition)
    taskGenerator = TaskModule.NestTaskGenerator()
    task.taskGenerator(taskGenerator)
    
    successCallback = automaton.getPostExecuteCallbackFor(task)
    errorCallback = automaton.getErrorCallbackFor(task)
    executeTaskFunction = automaton.getExecuteTaskFunction(task)
    
    commandBuilder = DefinitionLibraryModule.CommandBuilder()
    commandBuilderMap = {
        'library bootstrap loader':commandBuilder,
        'python eval':commandBuilder
        }
    executeEnvironment = DefinitionLibraryModule.LibraryLoader(library)
    requestContext = {
        'task':task,
        'command builder map':commandBuilderMap,
        'execute environment':executeEnvironment
    }
    
    request = threadpool.WorkRequest(
        executeTaskFunction,
        args = [],
        kwds = requestContext,
        callback = successCallback,
        exc_callback = errorCallback
    )
    threadPool = automaton.getThreadPoolUsingRequest(request)
    request.kwds['thread pool'] = threadPool
    
    task.workRequest(request)

    task.automaton(automaton)

    threadPool.putRequest(request)
    threadPool.wait()
    
    assert not request.exception
    
    return

    
class TestBase(unittest.TestCase):

    def setUp(self):
        return

    def tearDown(self):
        return

    def initializeLibrary(self):
        library = DefinitionLibraryModule.Library()
        library.bootstrapLoaderDefinitionsDir(
            os.path.join(POMSET_ROOT, 'resources', 'testdata', 'TestLibrary', 'library'))
        library.loadBootstrapLoaderDefinitions()
        return library
    
    
    
    def testLoadBootstrapLoader(self):
        library = self.initializeLibrary()
        
        loadedDefinitions = library.definitions()
        
        self.assertEqual(2, len(loadedDefinitions))
        
        self.assertTrue(GenerateLibraryModule.ID_BOOTSTRAPLOADER in loadedDefinitions)
        self.assertTrue(GenerateLibraryModule.ID_LOADLIBRARYDEFINITION in loadedDefinitions)
        
        self.assertTrue(all(x.isLibraryDefinition() 
                            for x in loadedDefinitions.values()))
        
        return
    
    
    
    # END class TestBase
    pass


class TestBootstrapLoader(unittest.TestCase):

    def setUp(self):
        automaton = AutomatonModule.Automaton()
        automaton.setThreadPool(None, threadpool.ThreadPool(1))
        automaton.commandManager(CommandPatternModule.CommandManager())
        self.automaton = automaton
        return

    
    def tearDown(self):
        return

    
    def initializeLibrary(self):
        library = DefinitionLibraryModule.Library()
        library.bootstrapLoaderDefinitionsDir(
            os.path.join(POMSET_ROOT, 'resources', 'testdata', 'TestLibrary', 'library'))
        library.loadBootstrapLoaderDefinitions()
        return library
    


    
    def test1(self):
            
        
        library = self.initializeLibrary()
        
        definitions = library.definitions()
        self.assertTrue(len(definitions) is 2)
        self.assertTrue(GenerateLibraryModule.ID_BOOTSTRAPLOADER in definitions)
        self.assertTrue(GenerateLibraryModule.ID_LOADLIBRARYDEFINITION in definitions)
        self.assertFalse(GenerateLibraryModule.ID_WORDCOUNT_REDUCE in definitions)
        self.assertFalse(GenerateLibraryModule.ID_WORDCOUNT in definitions)
        
        runBootstrapLoader(self.automaton, library)
        
        definitions = library.definitions()
        self.assertTrue(len(definitions) is 4)
        self.assertTrue(GenerateLibraryModule.ID_BOOTSTRAPLOADER in definitions)
        self.assertTrue(GenerateLibraryModule.ID_LOADLIBRARYDEFINITION in definitions)
        self.assertTrue(GenerateLibraryModule.ID_WORDCOUNT_REDUCE in definitions)
        self.assertTrue(GenerateLibraryModule.ID_WORDCOUNT in definitions)
        
        return
    
    
    def testPickleAndLoad(self):
        """
        This verifies a pomset saved out and reloaded
        still references the same definition
        
        - create a pomset that references the two library definitions
        - save out the pomset
        - load the pomset again
        - assert the references are identical, using Python's "is"
        """

        # load the library definitions
        library = self.initializeLibrary()

        runBootstrapLoader(self.automaton, library)
        
        loadedDefinitions = library.definitions()
        
        # create the pomset, add a node
        # and have that node reference a library definition
        compositeDefinition = DefinitionModule.getNewNestDefinition()
        mapperNode = compositeDefinition.createNode(id='mapper')
        mapperNode.definitionToReference(loadedDefinitions[GenerateLibraryModule.ID_WORDCOUNT])

        # pickle the pomset
        # unpickle the pomset
        definition = TestDefinitionModule.pickleAndReloadDefinition(
            '/tmp/foo.pomset',
            compositeDefinition
        )
        
        assert definition.nodes()[0].definitionToReference() is loadedDefinitions[GenerateLibraryModule.ID_WORDCOUNT]
        
        return

    
    # END class TestBoostrapLoader
    pass

    
class TestLoadAcrossSessions(unittest.TestCase):

    def setUp(self):
        automaton = AutomatonModule.Automaton()
        automaton.setThreadPool(None, threadpool.ThreadPool(1))
        automaton.commandManager(CommandPatternModule.CommandManager())
        self.automaton = automaton
        return
    
    def tearDown(self):
        return
    
    def initializeLibrary(self):
        library = DefinitionLibraryModule.Library()
        library.bootstrapLoaderDefinitionsDir(
            os.path.join(POMSET_ROOT, 'resources', 'testdata', 'TestLibrary', 'library'))
        library.loadBootstrapLoaderDefinitions()
        
        return library
    
    
    def testLoad1(self):
        """
        This verifies that a pomset saved out still references
        the same library definition, even across sessions
        when the library definitions have been reloaded as well
        
        - create a pomset that references the two library definitions
        - save out the pomset
        - reset the location of the library, to an empty directory
        - load the pomset again
        - assert the references are identical, using Python's "is"        
        """
    
        library = self.initializeLibrary()
        
        runBootstrapLoader(self.automaton, library)
        
        loadedDefinitions = library.definitions()
        
        definition = DefinitionLibraryModule.loadDefinitionFromFullFilePath(
            os.path.join(POMSET_ROOT, 'resources', 'testdata', 'TestLibrary', 'foo.pomset'))

        
        # at this point, the definitions are different
        assert definition.nodes()[0].definitionToReference() is not loadedDefinitions[GenerateLibraryModule.ID_WORDCOUNT]
        
        # update the references
        library.updateWithLibraryDefinitions(definition)
        
        # now, the definitions should be the same
        assert definition.nodes()[0].definitionToReference() is loadedDefinitions[GenerateLibraryModule.ID_WORDCOUNT]
        
        return
    
    
    # END class TestLoadAcrossSessions
    pass



def main():
    
    util.configLogging()

    suite = unittest.TestSuite()
    
    suite.addTest(unittest.makeSuite(TestBase, 'test'))
    suite.addTest(unittest.makeSuite(TestBootstrapLoader, 'test'))
    suite.addTest(unittest.makeSuite(TestLoadAcrossSessions, 'test'))
    
    runner = unittest.TextTestRunner()
    runner.run(suite)
    return

if __name__=="__main__":
    main()


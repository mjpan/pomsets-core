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


import currypy
import pypatterns.command as CommandPatternModule
import pypatterns.filter as FilterModule
import pypatterns.relational as RelationalModule

import cloudpool.shell as ShellModule

import pomsets.automaton as AutomatonModule

import pomsets.command as TaskCommandModule
import pomsets.definition as DefinitionModule
import pomsets.library as DefinitionLibraryModule
import pomsets.parameter as ParameterModule
import pomsets.task as TaskModule

import test.definition as TestDefinitionModule
import test.generate_library as GenerateLibraryModule




def runBootstrapLoader(automaton, library):

    definition = library.getBootstrapLoader()
    
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
    
    return request

    
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
        
        loadedDefinitionTable = library.definitionTable()
        
        self.assertEqual(2, loadedDefinitionTable.rowCount())
        
        for definitionId, expectedValue in [
            (DefinitionLibraryModule.ID_BOOTSTRAPLOADER, True),
            (DefinitionLibraryModule.ID_LOADLIBRARYDEFINITION, True),
            (GenerateLibraryModule.ID_WORDCOUNT_REDUCE, False),
            (GenerateLibraryModule.ID_WORDCOUNT, False)]:
            
            filter = RelationalModule.ColumnValueFilter(
                'definition',
                FilterModule.IdFilter(definitionId))
            self.assertEquals(library.hasDefinition(filter), expectedValue)
            pass
            
        
        
        filter = FilterModule.TRUE_FILTER
        allDefinitions = RelationalModule.Table.reduceRetrieve(
            loadedDefinitionTable,
            filter, ['definition'], [])
        self.assertTrue(all(x.isLibraryDefinition() for x in allDefinitions))
        
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

        loadedDefinitionTable = library.definitionTable()
        
        self.assertEqual(2, loadedDefinitionTable.rowCount())

        for definitionId, expectedValue in [
            (DefinitionLibraryModule.ID_BOOTSTRAPLOADER, True),
            (DefinitionLibraryModule.ID_LOADLIBRARYDEFINITION, True),
            (GenerateLibraryModule.ID_WORDCOUNT_REDUCE, False),
            (GenerateLibraryModule.ID_WORDCOUNT, False)]:
            
            filter = RelationalModule.ColumnValueFilter(
                'definition',
                FilterModule.IdFilter(definitionId))
            self.assertEquals(library.hasDefinition(filter), expectedValue)
        
        
        
        
        
        request = runBootstrapLoader(self.automaton, library)
        assert not request.exception
    

        self.assertEqual(4, loadedDefinitionTable.rowCount())
        for definitionId, expectedValue in [
            (DefinitionLibraryModule.ID_BOOTSTRAPLOADER, True),
            (DefinitionLibraryModule.ID_LOADLIBRARYDEFINITION, True),
            (GenerateLibraryModule.ID_WORDCOUNT_REDUCE, True),
            (GenerateLibraryModule.ID_WORDCOUNT, True)]:
            
            filter = RelationalModule.ColumnValueFilter(
                'definition',
                FilterModule.IdFilter(definitionId))
            self.assertEquals(library.hasDefinition(filter), expectedValue)

            
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

        request = runBootstrapLoader(self.automaton, library)
        assert not request.exception

        # create the pomset, add a node
        # and have that node reference a library definition
        filter = RelationalModule.ColumnValueFilter(
            'definition',
            FilterModule.IdFilter(GenerateLibraryModule.ID_WORDCOUNT))
        definitionToReference = library.getDefinition(filter)
        compositeDefinition = DefinitionModule.getNewNestDefinition()
        mapperNode = compositeDefinition.createNode(id='mapper')
        mapperNode.definitionToReference(definitionToReference)

        # pickle the pomset
        # unpickle the pomset
        definition = TestDefinitionModule.pickleAndReloadDefinition(
            os.path.join('tmp', 'foo.pomset'),
            compositeDefinition
        )
        
        definitionToReference = library.getDefinition(filter)
        assert definition.nodes()[0].definitionToReference() is definitionToReference
        
        return

    
    # END class TestBoostrapLoader
    pass


class TestRecoverFromLoadFailure(unittest.TestCase):

    def setUp(self):
        # generate and/or specify the location
        # of a bootstrap pomsets
        # that will fail
        return

    def tearDown(self):
        return

    def testLoad1(self):
        """
        this should use the library in
        resources/testdata/TestLibrary/libraryFailure1 
        which specifies only a single pomset to load
        That pomset is unloadable
        """
        # run the bootstrap loader
        # ensure that the there's no error
        # ensure that only the non-failed ones
        # are in the library

        return

    def testLoad2(self):
        """
        this should use the library in
        resources/testdata/TestLibrary/libraryFailure1 
        which specifies only two pomsets to load
        of which one is unloadable
        """
        # run the bootstrap loader
        # ensure that the there's no error
        # ensure that only the non-failed ones
        # are in the library

        return

    # END class TestRecoverFromLoadFailure
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
        
        request = runBootstrapLoader(self.automaton, library)
        assert not request.exception
            
        # TODO:
        # implement something that will re-generate
        # the pomset for the test
        definition = DefinitionLibraryModule.loadDefinitionFromFullFilePath(
            os.path.join(POMSET_ROOT, 'resources', 'testdata', 'TestLibrary', 'foo.pomset'))

        
        # at this point, the definitions are different
        filter = RelationalModule.ColumnValueFilter(
            'definition',
            FilterModule.IdFilter(GenerateLibraryModule.ID_WORDCOUNT))
        libraryDefinition = library.getDefinition(filter)

        assert definition.nodes()[0].definitionToReference() is not libraryDefinition
        
        # update the references
        library.updateWithLibraryDefinitions(definition)
        
        # now, the definitions should be the same
        assert definition.nodes()[0].definitionToReference() is libraryDefinition
        
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


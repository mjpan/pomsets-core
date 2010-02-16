from __future__ import with_statement

import os
import sys
import threadpool
import unittest
import logging
import shutil


import currypy

import pypatterns.command as CommandPatternModule
import pypatterns.filter as FilterModule
import pypatterns.relational as RelationalModule

import cloudpool as CloudModule
import cloudpool.shell as ShellModule

import pomsets.automaton as AutomatonModule
import pomsets.command as TaskCommandModule
import pomsets.context as ContextModule
import pomsets.definition as DefinitionModule
import pomsets.library as DefinitionLibraryModule
import pomsets.parameter as ParameterModule
import pomsets.task as TaskModule

import pomsets.test_utils as TestDefinitionModule


def runBootstrapLoader(automaton, library, isCritical=False):

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

    automaton.enqueueRequest(request, shouldWait=True)
    
    return request

    
class BaseTestClass(unittest.TestCase):



    def initializeLibrary(self):
        library = DefinitionLibraryModule.Library()
        library.shouldMarkAsLibraryDefinition(True)
        libraryDir = self.getLibraryDir()
        library.bootstrapLoaderDefinitionsDir(libraryDir)

        library.loadBootstrapLoaderDefinitions()
        return library
    
    # END BaseTestClass
    pass

    
class TestBootstrapLoader(BaseTestClass):

    def getLibraryDir(self):
        return os.path.join(os.getcwd(), 'resources', 'testdata', 'TestLibrary', 'library')
    
    def test1(self):
        library = self.initializeLibrary()
        
        loadedDefinitionTable = library.definitionTable()
        
        self.assertEqual(2, loadedDefinitionTable.rowCount())
        
        for definitionId, expectedValue in [
            (DefinitionLibraryModule.ID_BOOTSTRAPLOADER, True),
            (DefinitionLibraryModule.ID_LOADLIBRARYDEFINITION, True),
            (TestDefinitionModule.ID_WORDCOUNT_REDUCE, False),
            (TestDefinitionModule.ID_WORDCOUNT, False)]:
            
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
    
    
    
    # END class TestBootstrapLoader
    pass


class TestBootstrapLoaderPomset(BaseTestClass):

    def getLibraryDir(self):
        return os.path.join(os.getcwd(), 'resources', 'testdata', 'TestLibrary', 'library')

    def setUp(self):
        automaton = AutomatonModule.Automaton()
        automaton.setThreadPool(None, CloudModule.Pool(1))
        automaton.commandManager(CommandPatternModule.CommandManager())
        self.automaton = automaton
        return

    
    
    def test1(self):
            
        
        library = self.initializeLibrary()

        loadedDefinitionTable = library.definitionTable()
        
        self.assertEqual(2, loadedDefinitionTable.rowCount())

        for definitionId, expectedValue in [
            (DefinitionLibraryModule.ID_BOOTSTRAPLOADER, True),
            (DefinitionLibraryModule.ID_LOADLIBRARYDEFINITION, True),
            (TestDefinitionModule.ID_WORDCOUNT_REDUCE, False),
            (TestDefinitionModule.ID_WORDCOUNT, False)]:
            
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
            (TestDefinitionModule.ID_WORDCOUNT_REDUCE, True),
            (TestDefinitionModule.ID_WORDCOUNT, True)]:
            
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
            FilterModule.IdFilter(TestDefinitionModule.ID_WORDCOUNT))
        definitionToReference = library.getDefinition(filter)
        compositeDefinition = DefinitionModule.getNewNestDefinition()
        mapperNode = compositeDefinition.createNode(id='mapper')
        mapperNode.definitionToReference(definitionToReference)

        # pickle the pomset
        # unpickle the pomset
        definition = TestDefinitionModule.pickleAndReloadDefinition(
            os.sep.join(['', 'tmp', 'foo.pomset']),
            compositeDefinition
        )
        library.updateWithLibraryDefinitions(definition)
        
        definitionToReference = library.getDefinition(filter)
        assert definition.nodes()[0].definitionToReference() is definitionToReference
        
        return

    
    # END class TestBoostrapLoader
    pass


class TestRecoverFromLoadFailure1(BaseTestClass):

    def getLibraryDir(self):
        return os.path.join(os.getcwd(), 'resources', 'testdata', 'TestLibrary', 'libraryFailure1')


    def setUp(self):
        automaton = AutomatonModule.Automaton()
        automaton.setThreadPool(None, CloudModule.Pool(1))
        automaton.commandManager(CommandPatternModule.CommandManager())
        self.automaton = automaton
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

        library = self.initializeLibrary()

        loadedDefinitionTable = library.definitionTable()
        
        self.assertEqual(2, loadedDefinitionTable.rowCount())

        request = runBootstrapLoader(self.automaton, library, isCritical=False)

        self.assertEqual(2, loadedDefinitionTable.rowCount())

        return


    # END class TestRecoverFromLoadFailure
    pass


class TestRecoverFromLoadFailure2(BaseTestClass):

    def getLibraryDir(self):
        return os.path.join(os.getcwd(), 'resources', 'testdata', 'TestLibrary', 'libraryFailure2')


    def setUp(self):
        automaton = AutomatonModule.Automaton()
        automaton.setThreadPool(None, CloudModule.Pool(1))
        automaton.commandManager(CommandPatternModule.CommandManager())
        self.automaton = automaton
        return


    def tearDown(self):
        return


    def testLoad1(self):
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
        library = self.initializeLibrary()

        loadedDefinitionTable = library.definitionTable()
        
        self.assertEqual(2, loadedDefinitionTable.rowCount())

        request = runBootstrapLoader(self.automaton, library, isCritical=False)

        self.assertEqual(3, loadedDefinitionTable.rowCount())

        parentTask = request.kwds['task']
        childTasks = parentTask.getChildTasks()
        requestExceptions = [x.workRequest().exception for x in childTasks]
        
        # these 3 assertions will ensure that 
        # there's only one success and one failure
        self.assertEqual(2, len(requestExceptions))
        self.assertTrue(any(requestExceptions))
        self.assertTrue(not all(requestExceptions))

        return

    # END class TestRecoverFromLoadFailure
    pass
    

class TestLoadAcrossSessions(BaseTestClass):

    def getLibraryDir(self):
        return os.path.join(os.getcwd(), 'resources', 'testdata', 'TestLibrary', 'library')

    def setUp(self):
        automaton = AutomatonModule.Automaton()
        automaton.setThreadPool(None, CloudModule.Pool(1))
        automaton.commandManager(CommandPatternModule.CommandManager())
        self.automaton = automaton
        return
    
    
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
        pomsetContext = ContextModule.loadPomset(
            path=os.path.join(os.getcwd(), 'resources', 'testdata', 'TestLibrary', 'foo.pomset'))
        definition = pomsetContext.pomset()

        
        # at this point, the definitions are different
        filter = RelationalModule.ColumnValueFilter(
            'definition',
            FilterModule.IdFilter(TestDefinitionModule.ID_WORDCOUNT))
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
    
    suite.addTest(unittest.makeSuite(TestBootstrapLoader, 'test'))
    suite.addTest(unittest.makeSuite(TestBootstrapLoaderPomset, 'test'))
    # suite.addTest(unittest.makeSuite(TestLoadAcrossSessions, 'test'))
    suite.addTest(unittest.makeSuite(TestRecoverFromLoadFailure1, 'test'))
    suite.addTest(unittest.makeSuite(TestRecoverFromLoadFailure2, 'test'))

    runner = unittest.TextTestRunner()
    runner.run(suite)
    return

if __name__=="__main__":
    main()


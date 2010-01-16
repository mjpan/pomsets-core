from __future__ import with_statement

import os
import pickle

import cloudpool.environment as EnvironmentModule
import cloudpool.task as TaskModule

import pypatterns.filter as FilterModule
import pypatterns.relational as RelationalModule

import pomsets.definition as DefinitionModule
import pomsets.resource as ResourceModule


ID_LOADLIBRARYDEFINITION = 'load library definition::bb028375-bbd5-43ec-b6c3-4955c062063f'
ID_BOOTSTRAPLOADER = 'library bootstrap loader::751fe366-1448-4db3-9db4-944075de7a5b'


def loadDefinitionFromFullFilePath(path):
    definition = None
    
    with open(path, 'r') as f:
        definition = pickle.load(f)
        pass

    if definition is None:
        raise Exception('failed on loading pickle')

    return definition


class CommandBuilder(TaskModule.CommandBuilder):

    def buildCommand(self, task):
        workRequest = task.workRequest()
        command = 'library.loadDefinitionFromRelativePath("%s")' % task.getParameterBinding('pomset url')
        
        return command
    
    # END class CommandBuilder
    pass


class LibraryLoader(EnvironmentModule.Environment, ResourceModule.Struct):
    
    DEFAULT_COMMANDBUILDER_TYPE = 'library bootstrap loader'

    ATTRIBUTES = ['library']
    
    def __init__(self, library):
        ResourceModule.Struct.__init__(self)
        self.library(library)
        return
    
    def execute(self, task, *args, **kargs):
        request = task.workRequest()
        
        commandBuilder = self.getCommandBuilder(task)

        command = commandBuilder.buildCommand(task)

        library = self.library()
        evalResult = eval(command)

        request.kwds['eval result'] = evalResult
        
        return 0
    
    # END class LibraryLoader
    pass


class Path(ResourceModule.Struct):
    
    ATTRIBUTES = ['rawPath']
    
    def __init__(self, rawPath):
        ResourceModule.Struct.__init__(self)
        
        self.rawPath(rawPath)
        pass
    
    # END class Path
    pass

class Library(ResourceModule.Struct):

    BOOTSTRAP_LOADER_FILES = [
        'loadLibraryDefinition.pomset',
        'loadLibraryDefinitions.pomset'
    ]
    
    ATTRIBUTES = [
        'hasLoadedDefinitions', 
        'definitionTable',
        'bootstrapLoaderDefinitionsDir',
        'bootstrapLoaderDefinitions'
    ]
    
    def __init__(self):
        ResourceModule.Struct.__init__(self)
        self.hasLoadedDefinitions(False)
        self.bootstrapLoaderDefinitions({})
        
        # self.definitions({})
        table = RelationalModule.createTable(
            'definitions', 
            ['definition', 'id', 'name'])
        self.definitionTable(table)

        return
    
    def updateWithLibraryDefinitions(self, definition, recursive=True):
        # if the definition contains ReferenceDefinitions
        # and those ReferenceDefinitions indicate that they reference
        # library definitions, 
        if not isinstance(definition, DefinitionModule.CompositeDefinition):
            return
        
        # libraryDefinitions = self.definitions()
        
        for referenceDefinition in definition.nodes():
            if not referenceDefinition.referencesLibraryDefinition():
                continue
            
            referencedDefinition = referenceDefinition.definitionToReference()
            libraryDefinitionId = referencedDefinition.id()
            
            filter = RelationalModule.ColumnValueFilter(
                'definition',
                FilterModule.IdFilter(libraryDefinitionId))
            
            matchingDefinitions = RelationalModule.Table.reduceRetrieve(
                self.definitionTable(), filter, ['definition'], [])
            # if libraryDefinitionId in libraryDefinitions:
            #    referenceDefinition.definitionToReference(
            #        libraryDefinitions[libraryDefinitionId])
            if len(matchingDefinitions) is not 0:
                referencedDefinition.definitionToReference(
                    matchingDefinitions[0])
            elif recursive and referencedDefinition is not definition:
                # recursively traverse and update with library definitions
                # but do so only if the reference is not recursive
                self.updateWithLibraryDefinitions(referencedDefinition)
                pass
    
            pass
        
        return

    def loadDefinitionFromRelativePath(self, relativePath):
        fullPath = os.path.join(self.bootstrapLoaderDefinitionsDir(), relativePath)
        return self.loadDefinitionFromFullFilePath(fullPath)
    
    def loadDefinitionFromFullFilePath(self, fullPath):
        definition = loadDefinitionFromFullFilePath(fullPath)
        definition.isLibraryDefinition(True)
        # self.definitions()[definition.id()] = definition
        self.addDefinition(definition)
        self.updateWithLibraryDefinitions(self, definition)
        return definition
    
    
    def loadBootstrapLoaderDefinitions(self):
        
        dirPath = self.bootstrapLoaderDefinitionsDir()
        for bootstrapLoaderFile in Library.BOOTSTRAP_LOADER_FILES:
            fullPath = os.path.join(dirPath, bootstrapLoaderFile)
            definition = self.loadDefinitionFromFullFilePath(fullPath)
            self.bootstrapLoaderDefinitions()[bootstrapLoaderFile] = definition
            pass
        return

    def addDefinition(self, definition):
        row = self.definitionTable().addRow()
        row.setColumn('id', definition.id())
        row.setColumn('name', definition.name())
        row.setColumn('definition', definition)
        return
    
    def hasDefinition(self, filter):
        matchingDefinitions = RelationalModule.Table.reduceRetrieve(
            self.definitionTable(), filter, ['definition'], [])
        return len(matchingDefinitions) is not 0
    
    def getDefinition(self, filter):
        matchingDefinitions = RelationalModule.Table.reduceRetrieve(
            self.definitionTable(), filter, ['definition'], [])
        return matchingDefinitions[0]
    
    """
    def loadDefinitions(self):
        if self.hasLoadedDefinitions():
            raise ValueError('has already loaded definitions')
        
        self.loadBootstrapLoaderDefinitions()
        
        definitions = reduce(
            list.__add__,
            map(self.loadDefinitionsFromPathObject, self.paths()))

        # set the isLibrary flag 
        # for all the definitions that have b
        [x.isLibraryDefinition(True) for x in definitions]
        
        self.definitions().update(dict([(x.id(), x) for x in definitions]))
        
        self.hasLoadedDefinitions(True)
        return
    """

    
    def buildManifestPomset(self):

        
        
        return
    
    def getBootstrapLoader(self):
        filter = RelationalModule.ColumnValueFilter(
            'definition',
            FilterModule.IdFilter(ID_BOOTSTRAPLOADER))
        definition = self.getDefinition(filter)
        return definition
    
    # END class Library
    pass


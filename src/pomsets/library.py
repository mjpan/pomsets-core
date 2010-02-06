from __future__ import with_statement

import logging
import os
import pickle
import uuid

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


def pickleDefinition(path, definition):

    try:
        pickleCreated = False
        with open(path, 'w') as f:
            pickle.dump(definition, f)
            pickleCreated = True
            pass

        if not pickleCreated:
            raise Exception('failed on creating pickle')

    except Exception, e:
        logging.error("errored with msg >> %s" % e)
        pass

    return



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
        (ID_LOADLIBRARYDEFINITION, 'loadLibraryDefinition.pomset'),
        (ID_BOOTSTRAPLOADER, 'loadLibraryDefinitions.pomset')
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
            ['definition', 'id'])
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
        self.addDefinition(definition)
        self.updateWithLibraryDefinitions(self, definition)
        return definition
    
    
    def loadBootstrapLoaderDefinitions(self):
        """
        This loads the two bootstrapper pomsets
        * the atomic one that loads a single pomset
        * the composite one that specifies a bunch of pomsets to be loaded
        """
        
        dirPath = self.bootstrapLoaderDefinitionsDir()
        if not os.path.exists(dirPath):
            raise NotImplementedError('need to handle when bootstrap loader definitions dir does not exist')
        
        for definitionId, bootstrapLoaderFile in Library.BOOTSTRAP_LOADER_FILES:
            fullPath = os.path.join(dirPath, bootstrapLoaderFile)

            if not os.path.exists(fullPath):
                raise NotImplementedError('need to handle when bootstrap loader file %s does not exist')
            
            definition = self.loadDefinitionFromFullFilePath(fullPath)
            pass
        return


    def addDefinition(self, definition):
        definitionId = definition.id()
        if definitionId in [ID_LOADLIBRARYDEFINITION,
                            ID_BOOTSTRAPLOADER]:
            logging.debug("adding definition %s to bootstrap id %s" %
                          (definition, definitionId))

            self.bootstrapLoaderDefinitions()[definitionId] = definition
            pass

        logging.debug("adding definition %s with id %s to library" % 
                      (definition, definitionId))

        row = self.definitionTable().addRow()
        row.setColumn('id', definitionId)
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

    
    def getBootstrapLoader(self):
        filter = RelationalModule.ColumnValueFilter(
            'definition',
            FilterModule.IdFilter(ID_BOOTSTRAPLOADER))
        definition = self.getDefinition(filter)
        return definition


    def generateBootstrapLoaderPomset(self):
        """
        generates the composite pomset used to load
        all the library pomsets
        """
        # now create a load library definitions pomset
        # that will load the two wordcount pomsets
        defToLoadDef = self.bootstrapLoaderDefinitions()[ID_LOADLIBRARYDEFINITION]

        defToLoadDefs = DefinitionModule.getNewNestDefinition()

        # we need to filter out the bootstrap pomset loader
        # because it should not be loaded again
        bootstrapLoaderFilter = RelationalModule.ColumnValueFilter(
            'definition',
            FilterModule.IdFilter(ID_LOADLIBRARYDEFINITION)
        )
        notBootstrapLoaderFilter = FilterModule.constructNotFilter()
        notBootstrapLoaderFilter.addFilter(bootstrapLoaderFilter)
        
        definitions = RelationalModule.Table.reduceRetrieve(
            self.definitionTable(), 
            notBootstrapLoaderFilter,
            ['definition'], [])

        for definitionToLoad in definitions:
            loadNode = defToLoadDefs.createNode(id=uuid.uuid4())
            loadNode.definitionToReference(defToLoadDef)
            loadNode.isCritical(False)
            loadNode.name('load %s' % definitionToLoad.url())
            loadNode.setParameterBinding('pomset url', definitionToLoad.url())
            pass
        defToLoadDefs.id(ID_BOOTSTRAPLOADER)
        defToLoadDefs.name('bootstrap pomsets loader')

        return defToLoadDefs

    
    def saveBootstrapLoaderPomset(self):
        outputPath = os.path.join(
            self.bootstrapLoaderDefinitionsDir(),
            'loadLibraryDefinitions.pomset')

        pomset = self.generateBootstrapLoaderPomset()
        
        pickleDefinition(outputPath, pomset)

        return

    
    # END class Library
    pass


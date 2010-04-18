from __future__ import with_statement

import logging
import os
import pickle
import uuid

import cloudpool.environment as EnvironmentModule
import cloudpool.task as TaskModule

import pypatterns.filter as FilterModule
import pypatterns.relational as RelationalModule

import pomsets.context as ContextModule
import pomsets.definition as DefinitionModule
import pomsets.resource as ResourceModule


ID_LOADLIBRARYDEFINITION = 'load library definition::bb028375-bbd5-43ec-b6c3-4955c062063f'
ID_BOOTSTRAPLOADER = 'library bootstrap loader::751fe366-1448-4db3-9db4-944075de7a5b'

ID_LOADLISTVALUESFROMFILES = 'load list values from files_fc6175d6-c18b-4403-9b79-bd2b0b0012ff'
ID_RANGENUMBERS = 'range numbers_79fb1a45-c9b9-4708-9493-81a64d00d3e7'
ID_OSMAKEDIRS = 'os.makedirs_8036683f-33ce-4607-a2c8-b6b6fdeb97dc'
ID_STRINGREPLACE = 'string replace_5b42c647-3a07-435b-86fa-98dbf035d161'


def getBootstrapLoaderPomsetsFilter():
    # we need to filter out the bootstrap pomset loaders
    # because it should not be loaded again
    bootstrapLoaderFilter = FilterModule.constructOrFilter()
    bootstrapLoaderFilter.addFilter(
        RelationalModule.ColumnValueFilter(
            'definition',
            FilterModule.IdFilter(ID_LOADLIBRARYDEFINITION)
        )
    )
    bootstrapLoaderFilter.addFilter(
        RelationalModule.ColumnValueFilter(
            'definition',
            FilterModule.IdFilter(ID_BOOTSTRAPLOADER)
        )
    )
    return bootstrapLoaderFilter



class CommandBuilder(TaskModule.CommandBuilder):

    def buildCommand(self, task):
        workRequest = task.workRequest()
        command = 'library.loadFromRelativePath("%s")' % task.getParameterBinding('pomset url')
        
        return command
    
    # END class CommandBuilder
    pass


class LibraryLoader(EnvironmentModule.Environment, ResourceModule.Struct):
    """
    NOTE: 
    This is very similar to, and can probably subclass from,
    cloudpool.environment.PythonEval.  We just have not 
    spent the time to figure out how to do so yet
    """

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

        logging.debug("attempt to run %s" % command)
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
        'bootstrapLoaderDefinitions',
        'shouldMarkAsLibraryDefinition'
    ]
    
    def __init__(self):
        ResourceModule.Struct.__init__(self)
        self.hasLoadedDefinitions(False)
        self.bootstrapLoaderDefinitions({})
        
        table = RelationalModule.createTable(
            'definitions', 
            ['definition', 'id', 'context'])
        self.definitionTable(table)

        return
    
    def updateWithLibraryDefinitions(self, definition, recursive=True):
        # if the definition contains ReferenceDefinitions
        # and those ReferenceDefinitions indicate that they reference
        # library definitions, 
        if not isinstance(definition, DefinitionModule.CompositeDefinition):
            return
        
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

            if len(matchingDefinitions) is not 0:
                referenceDefinition.definitionToReference(
                    matchingDefinitions[0])
            elif recursive and referencedDefinition is not definition:
                # recursively traverse and update with library definitions
                # but do so only if the reference is not recursive
                self.updateWithLibraryDefinitions(referencedDefinition)
                pass
    
            pass
        
        return


    def loadFromRelativePath(self, relativePath):
        fullPath = os.path.join(
            self.bootstrapLoaderDefinitionsDir(), relativePath)

        return self.loadFromFullFilePath(fullPath)
    

    def loadFromFullFilePath(self, fullPath):
        context = ContextModule.loadPomset(path=fullPath)

        self.addPomsetContext(context)

        return context
    
    
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
            
            pomsetContext = self.loadFromFullFilePath(fullPath)
            pass
        return


    def addPomsetContext(self, context):

        definition = context.pomset()
        definitionId = definition.id()
        if definitionId in [ID_LOADLIBRARYDEFINITION,
                            ID_BOOTSTRAPLOADER]:
            logging.debug("adding definition %s to bootstrap id %s" %
                          (definition, definitionId))

            self.bootstrapLoaderDefinitions()[definitionId] = definition
            pass

        logging.debug("adding definition %s with id %s to library" % 
                      (definition, definitionId))

        if self.shouldMarkAsLibraryDefinition():
            definition.isLibraryDefinition(True)
        self.updateWithLibraryDefinitions(self, definition)

        row = self.definitionTable().addRow()
        row.setColumn('id', definitionId)
        row.setColumn('definition', definition)
        row.setColumn('context', context)
        return


    def hasDefinition(self, filter):
        matchingDefinitions = RelationalModule.Table.reduceRetrieve(
            self.definitionTable(), filter, ['definition'], [])
        return len(matchingDefinitions) is not 0

    
    def removeDefinition(self, filter):
        self.definitionTable().removeRows(filter)
        return

    
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

        notBootstrapLoaderFilter = FilterModule.constructNotFilter()
        notBootstrapLoaderFilter.addFilter(getBootstrapLoaderPomsetsFilter())
        
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

    
    def saveBootstrapLoaderPomset(self, outputPath=None):
        
        # default to the library's specified dir
        if outputPath is None:
            outputPath = os.path.join(
                self.bootstrapLoaderDefinitionsDir(),
                'loadLibraryDefinitions.pomset')

        pomset = self.generateBootstrapLoaderPomset()
        pomsetContext = ContextModule.wrapPomsetInContext(pomset)

        ContextModule.savePomsetAs(pomsetContext, outputPath)
        return

    
    # END class Library
    pass


import copy
import logging
import uuid

import pypatterns.relational as RelationalModule
import pypatterns.filter as FilterModule

import pomsets.command as TaskCommandModule
import pomsets.context as ContextModule
import pomsets.definition as DefinitionModule
import pomsets.parameter as ParameterModule

class Builder(object):


    def bindParameterValue(self, node, parameterId, value):
        (nodeToEdit, parameterToEdit) = \
            node.getParameterToEdit(parameterId)
        nodeToEdit.setParameterBinding(parameterToEdit.id(), value)
        return

    def removePomsetParameter(self, pomset, parameterName):

        if parameterName in [DefinitionModule.Definition.SYMBOL_INPUT_TEMPORAL,
                             DefinitionModule.Definition.SYMBOL_OUTPUT_TEMPORAL]:
            raise ValueError('cannot remove parameters defined by the system')

        parameter = pomset.getParameter(parameterName)
        pomset.removeParameter(parameter)
        return
        
    def addPomsetParameter(self, pomset, parameterName, attributes):

        # the direction has to be specified
        # almost everything else has a 
        direction = attributes['direction']

        isOptional = attributes.get('optional', False)
        isActive = attributes.get('active', True)
        isCommandline = attributes.get('commandline', True)
        isFile = attributes.get('file', False)
        isList = attributes.get('list', False)
        isEnum = attributes.get('enum', False)

        if isCommandline:
            prefixFlag = attributes.get('prefix flag', [])
            distributePrefixFlag = attributes.get(
                'distribute prefix flag', False)
            
            enumMap = attributes.get('enum map', {})

            commandlineOptions = {
                ParameterModule.COMMANDLINE_PREFIX_FLAG:prefixFlag,
                ParameterModule.COMMANDLINE_PREFIX_FLAG_DISTRIBUTE:distributePrefixFlag,
                ParameterModule.COMMANDLINE_ENUM_MAP:enumMap
                }
            pass

        isInputFile = (direction == ParameterModule.PORT_DIRECTION_INPUT and 
                       isFile)

        isSideEffect = (direction == ParameterModule.PORT_DIRECTION_OUTPUT and
                        isFile)
        if isSideEffect:
            # we have to do this because the output is the file
            # but the name of the file is actually the input
            direction = ParameterModule.PORT_DIRECTION_INPUT

        parameterAttributes = {
            ParameterModule.PORT_ATTRIBUTE_COMMANDLINE:isCommandline,
            ParameterModule.PORT_ATTRIBUTE_ISINPUTFILE:isInputFile,
            ParameterModule.PORT_ATTRIBUTE_ISSIDEEFFECT:isSideEffect,
            ParameterModule.PORT_ATTRIBUTE_ISLIST:isList,
            ParameterModule.PORT_ATTRIBUTE_COMMANDLINE_OPTIONS:commandlineOptions
            }

        parameter = ParameterModule.DataParameter(
            id=parameterName, 
            optional=isOptional, active=isActive,
            portDirection=direction)
        parameter.name(parameterName)

        ParameterModule.setAttributes(parameter, parameterAttributes)

        pomset.addParameter(parameter)

        return parameter


    def addParameterOrdering(self, pomset, sourceParameterName, targetParameterName):
        parameterOrderings = pomset.parameterOrderingTable()
        row = parameterOrderings.addRow()
        row.setColumn('source', sourceParameterName)
        row.setColumn('target', targetParameterName)
        return


    def createExecutableObject(self, path, staticArgs=None):
        executableObject = TaskCommandModule.Executable()
        executableObject.stageable(False)
        executableObject.path(path)
        if staticArgs is None:
            staticArgs = []
        executableObject.staticArgs(staticArgs)
        return executableObject


    def createNewAtomicPomset(self, name=None, 
                              executableObject=None,
                              *args, **kwds):

        newAtomicPomset = DefinitionModule.AtomicDefinition(*args, **kwds)
        if name is None:
            name = 'pomset %s' % uuid.uuid4().hex[:3]
        newAtomicPomset.name(name)

        newAtomicPomset.functionToExecute(
            DefinitionModule.executeTaskInEnvironment)

        newAtomicPomset.executable(executableObject)

        # create the parameter orderings
        parameterOrderings = DefinitionModule.createParameterOrderingTable()
        newAtomicPomset.parameterOrderingTable(parameterOrderings)

        newAtomicPomset.commandBuilderType('shell process')

        newPomsetContext = ContextModule.Context()
        newPomsetContext.pomset(newAtomicPomset)
        
        return newPomsetContext




    def createNewNestPomset(self, name=None):
        """
        """
        #TODO: this should construct a command to create the new pomset
        #      and execute the command (or send an event to execute the command)
        #      within the command framework, the command should also
        #      create an event to update the GUI

        newPomset = DefinitionModule.getNewNestDefinition()

        if name is None:
            name = 'pomset %s' % uuid.uuid4().hex[:3]
        newPomset.name(name)
        
        newPomsetContext = ContextModule.Context()
        newPomsetContext.pomset(newPomset)
        
        return newPomsetContext


    def copyNode(self, pomset, node, name=None, 
                 shouldCopyEdges=False,
                 shouldCopyParameterBindings=False):

        # create a node that references the same definition
        # copy edges if necessary
        # copy parameter bindings if necessary
        nodeCopy = copy.copy(node)
        pomset.addNode(nodeCopy)

        if shouldCopyEdges:
            raise NotImplementedError('need to implement shouldCopyEdges')

        if shouldCopyParameterBindings:
            raise NotImplementedError('need to implement shouldCopyParameterBindings')

        return nodeCopy


    def createNewNode(self, pomset, name=None,
                      definitionToReference=None):

        #TODO: this should construct a command to create the new node
        #      and execute the command (or send an event to execute the command)
        #      within the command framework, the command should also
        #      create an event to update the GUI

        if name is None:
            name = 'job %s' % len(pomset.nodes())

        id = uuid.uuid4().hex
        node = pomset.createNode(id=id)
        node.name(name)

        if definitionToReference is None:
            raise ValueError("need to specify a definition to reference")

        node.definitionToReference(definitionToReference)

        # TODO:
        # see if it's possible to not have to run this line
        node.executable = definitionToReference.executable

        return node


    def removeNode(self, pomset, node, maintainTransitivity=False):
        if maintainTransitivity:
            raise NotImplementedError

        # first remove all the incoming and outgoing connections
        filter = FilterModule.constructOrFilter()
        filter.addFilter(
            RelationalModule.ColumnValueFilter(
                'source node',
                FilterModule.IdentityFilter(node)
                )
            )
        filter.addFilter(
            RelationalModule.ColumnValueFilter(
                'target node',
                FilterModule.IdentityFilter(node)
                )
            )
        
        columns = ['source node', 'source parameter',
                   'target node', 'target parameter']
        rowValueList = [x for x in 
                pomset.parameterConnectionPathTable().retrieve(
                filter=filter, columns=columns)]

        for rowValues in rowValueList:
            self.disconnect(pomset, *rowValues)
            pass

        pomset.removeNode(node)

        return


    def canConnect(self, 
                   pomset,
                   sourceNode, sourceParameterId,
                   targetNode, targetParameterId):
        """
        This is a validation function to determine whether the
        ports provided can be connected to each other
        """
        return pomset.canConnect(sourceNode, sourceParameterId,
                                 targetNode, targetParameterId)


    def connect(self, 
                pomset,
                sourceNode, sourceParameterId,
                targetNode, targetParameterId):
        """
        This assumes that the caller has already
        verified that canConnect() returns True
        """
        return pomset.connectNodes(sourceNode, sourceParameterId,
                                   targetNode, targetParameterId)



    def disconnect(self, pomset,
                   sourceNode, sourceParameterId,
                   targetNode, targetParameterId):
        """
        looks for the connection path
        then removes the individual atomic connections
        """
        return pomset.disconnect(sourceNode, sourceParameterId,
                                 targetNode, targetParameterId)


    # END class Builder
    pass


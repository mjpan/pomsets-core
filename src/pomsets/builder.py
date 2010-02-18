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
                              staticArgs=None, *args, **kwds):

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

        # cannot connect to itself
        if sourceNode == targetNode and sourceParameterId==targetParameterId:
            logging.debug("cannot connect parameter to itself")
            return False

        sourceParameter = None
        targetParameter = None
        try:
            sourceParameter = sourceNode.getParameter(sourceParameterId)
            targetParameter = targetNode.getParameter(targetParameterId)
        except Exception, e:
            # if the parameter does not exist
            # then there's no way to connect
            logging.debug('cannot connect non-existent parameters')
            return False
        
        # inputs cannot connect to each other
        # outputs also cannot connect to each other
        #if sourceParameter.portDirection() == targetParameter.portDirection():
        #    print 'cannot connect parameters of the same direction'
        #    logging.debug('cannot connect parameters of the same direction')
        #    return False

        if not sourceParameter.portType() == targetParameter.portType():
            logging.debug("cannot connect ports of different types")
            return False

        # the target parameter cannot be an input
        # nor an output file
        if not targetParameter.portDirection() == ParameterModule.PORT_DIRECTION_INPUT or \
                targetParameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_ISSIDEEFFECT):
            logging.debug("parameter %s is not an input" % targetParameterId)
            return False
        
        # the source parameter cannot be an output (file or not)
        if not sourceParameter.portDirection() == ParameterModule.PORT_DIRECTION_OUTPUT and \
           not sourceParameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_ISSIDEEFFECT):
            logging.debug("parameter %s is not an output" % sourceParameterId)
            return False


        # cannot connect if a path already exists
        filter = pomset.constructParameterConnectionFilter(
            sourceNode, sourceParameterId,
            targetNode, targetParameterId)
        paths = RelationalModule.Table.reduceRetrieve(
            pomset.parameterConnectionPathTable(),
            filter, ['path'], [])
        if len(paths) is not 0:
            return False

        # cannot connect if data and already connected to something else
        if targetParameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_ISINPUTFILE):
            filter = FilterModule.constructAndFilter()
            filter.addFilter(
                RelationalModule.ColumnValueFilter(
                    'target node',
                    FilterModule.IdentityFilter(targetNode)
                    )
                )
            filter.addFilter(
                RelationalModule.ColumnValueFilter(
                    'target parameter',
                    FilterModule.EquivalenceFilter(targetParameterId)
                    )
                )
            paths = RelationalModule.Table.reduceRetrieve(
                pomset.parameterConnectionPathTable(),
                filter, ['path'], [])
            if len(paths) is not 0:
                return False
            pass

        return True



    def connect(self, 
                pomset,
                sourceNode, sourceParameterId,
                targetNode, targetParameterId):
        """
        This assumes that the caller has already
        verified that canConnect() returns True
        """
        sourceParameter = sourceNode.getParameter(sourceParameterId)
        targetParameter = targetNode.getParameter(targetParameterId)


        portType = sourceParameter.portType()
        if portType == ParameterModule.PORT_TYPE_TEMPORAL:
            connection = pomset.connectParameters(
                sourceNode, sourceParameterId,
                targetNode, targetParameterId
            )
            pomset.addParameterConnectionPath(
                sourceNode, sourceParameterId,
                targetNode, targetParameterId,
                tuple([connection])
                )

            path = [
                sourceNode,
                sourceParameterId,
                connection,
                targetParameterId,
                targetNode
            ]
            
        else:
    
            # create a blackboard parameter
            bbParameterId = '%s.%s-%s.%s' % (sourceNode.name(),
                                             sourceParameterId,
                                             targetNode.name(),
                                             targetParameterId)
            bbParameter = ParameterModule.BlackboardParameter(
                bbParameterId)
            pomset.addParameter(bbParameter)
    
            # create a parameter connection (source->blackboard)
            sourceParameterConnection = pomset.connectParameters(
                sourceNode, sourceParameterId,
                pomset, bbParameterId
            )
    
            # create a parameter connection (blackboard->target)
            targetParameterConnection = pomset.connectParameters(
                pomset, bbParameterId,
                targetNode, targetParameterId
            )

            pomset.addParameterConnectionPath(
                sourceNode, sourceParameterId,
                targetNode, targetParameterId,
                tuple([sourceParameterConnection, targetParameterConnection]),
                tuple([bbParameterId])
                )

            path = [
                sourceNode,
                sourceParameterId,
                sourceParameterConnection,
                bbParameter,
                targetParameterConnection,
                targetParameterId,
                targetNode
            ]

        return path




    def disconnect(self, pomset,
                   sourceNode, sourceParameterId,
                   targetNode, targetParameterId):
        """
        looks for the connection path
        then removes the individual atomic connections
        """

        filter = pomset.constructParameterConnectionFilter(
            sourceNode, sourceParameterId,
            targetNode, targetParameterId)

        paths = RelationalModule.Table.reduceRetrieve(
            pomset.parameterConnectionPathTable(),
            filter, ['path'], [])

        for connections, additionalParameterIds in pomset.parameterConnectionPathTable().retrieve(filter=filter, columns=['path', 'additional parameters']):

            map(pomset.removeParameterConnection, list(connections))
            parameters = [pomset.getParameter(x) 
                          for x in additionalParameterIds]
            map(pomset.removeParameter, parameters)
            pass
        pomset.parameterConnectionPathTable().removeRows(filter)

        # now to see if there are any raw parameter connection
        connections = RelationalModule.Table.reduceRetrieve(
            pomset.parameterConnectionsTable(),
            filter, ['parameter connection'], [])
        map(pomset.removeParameterConnection, connections)
        
        return


    # END class Builder
    pass


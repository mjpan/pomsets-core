import copy
import logging
import subprocess
import time

import pypatterns.filter as FilterModule
import pomsets.graph as GraphModule
import pypatterns.relational as RelationalModule
import pomsets.resource as ResourceModule

import pomsets.parameter as ParameterModule


def doTask(task, *args, **kwds):
    kwds['task'] = task
    kwds['thread pool'] = task.threadPool()
    return task.do()


class GuiOptionsHolder(ResourceModule.Struct):

    ATTRIBUTES = ['guiOptions']

    # END class GuiOptionsHolder
    pass

class ParameterBindingsHolder(ResourceModule.Struct):

    ATTRIBUTES = ['parameterBindings']

    def __init__(self):
        ResourceModule.Struct.__init__(self)
        self.parameterBindings({})
        pass

    def setParameterBinding(self, key, value):
        logging.debug(
            'setting key "%s" to values "%s" to %s\'s parameter bindings' % 
            (key, value, self))       

        self.parameterBindings()[key] = value
    
    def getParameterBinding(self, key):
        if not self.hasParameterBinding(key):
            raise KeyError('%s not in %s\'s parameter bindings' % (key, self))
        return self.parameterBindings()[key]
        
    def hasParameterBinding(self, key):
        return key in self.parameterBindings()

    # END class ParameterBindingsHolder
    pass


class Definition(ResourceModule.Struct):
    
    ATTRIBUTES = GuiOptionsHolder.ATTRIBUTES + [
        'id',
        'parametersTable',
        'isLibraryDefinition',
        'url',
        'description',
        'name',
        'parameterOrderingTable',
    ]

    SYMBOL_INPUT = 'input'
    SYMBOL_OUTPUT = 'output'
    SYMBOL_INPUT_TEMPORAL = 'temporal input'
    SYMBOL_OUTPUT_TEMPORAL = 'temporal output'
    
    def __init__(self, id=None, name=None):
        ResourceModule.Struct.__init__(self)

        if id is None:
            id = ResourceModule.Resource.ID_GENERATOR()
        self.id(id)
        
        if name is None:
            name = ''
        self.name(name)
        
        self.initializeParameters()
        self.isLibraryDefinition(False)
        return
    
    
    def addParameter(self, parameter, id=None):

        row = self.parametersTable().addRow()

        # TODO:
        # why do we allow of a different id
        # than the one of the parameter?
        if id is None:
            id = parameter.id()
        row.setColumn('id', id)

        row.setColumn('parameter', parameter)
        row.setColumn('port direction', parameter.portDirection())
        row.setColumn('port type', parameter.portType())
        
        return parameter
 
    
    def removeParameter(self, parameter):
        filter = RelationalModule.ColumnValueFilter(
            'parameter',
            FilterModule.IdentityFilter(parameter)
        )
        self.parametersTable().removeRows(filter)
        return

    
    def initializeParameters(self):
        
        table = RelationalModule.createTable(
            'parameters', 
            ['id', 'parameter', 'port direction', 'port type']
        )
        self.parametersTable(table)
        
        inputParameter = ParameterModule.InputTemporalParameter(
            Definition.SYMBOL_INPUT_TEMPORAL)
        
        outputParameter = ParameterModule.OutputTemporalParameter(
            Definition.SYMBOL_OUTPUT_TEMPORAL)
        
        self.addParameter(inputParameter)
        self.addParameter(outputParameter)
        return

    def getParametersByFilter(self, filter):
        theFilter = RelationalModule.ColumnValueFilter(
            'parameter',
            filter)
        
        parameters = RelationalModule.Table.reduceRetrieve(
            self.parametersTable(),
            theFilter,
            ['parameter'],
            []
        )
        
        return parameters


    def getParameterIdFilter(self, id):
        filter = RelationalModule.ColumnValueFilter(
            'id',
            FilterModule.EquivalenceFilter(id)
        )
        return filter

    def getParametersHavingId(self, id):
        filter = self.getParameterIdFilter(id)

        parameters = RelationalModule.Table.reduceRetrieve(
            self.parametersTable(),
            filter,
            ['parameter']
        )
        return parameters
    
    
    def hasParameter(self, id):
        parameters = self.getParametersHavingId(id)
        return len(parameters) is not 0
    
    def getParameter(self, id):
        parameters = self.getParametersHavingId(id)
        
        if len(parameters) is 0:
            raise NotImplementedError(
                '%s has no parameter with id %s' % (self.name(), id))
        elif len(parameters) is not 1:
            raise NotImplementedError(
                '%s has more than one parameter with id %s' % self.name(), id)
        return parameters[0]


    def renameParameter(self, originalName, newName):
        filter = self.getParameterIdFilter(originalName)
        for row in self.parametersTable().retrieveForModification(filter):
            row.setColumn('id', newName)
            parameter = row.getColumn('parameter')
            parameter.id(newName)
            parameter.name(newName)
            pass
        return

    
    def isAtomic(self):
        return False
    
    pass


class CompositeDefinition(GraphModule.Graph, Definition, 
                          ParameterBindingsHolder):
    
    ATTRIBUTES = GraphModule.Graph.ATTRIBUTES + \
               Definition.ATTRIBUTES + \
               ParameterBindingsHolder.ATTRIBUTES + \
               ['definitions', 'parameterConnectionsTable',
                'parameterConnectionPathTable']

    
    def __init__(self):
        GraphModule.Graph.__init__(self)
        Definition.__init__(self)

        ParameterBindingsHolder.__init__(self)

        # definitions are 
        # the "alphabet" of the pomset
        self.definitions(set([]))
        
        self.initializeParameterConnections()
        self.initializeParameterConnectionPaths()
        return
    
    def __eq__(self, other):
        differences = [x for x in self.getDifferencesWith(other)]
        return len(differences) is not 0
    
    def getDifferencesWith(self, other):
        if not other:
            yield 'other is None'
            raise StopIteration
        if not self.__class__ == other.__class__:
            yield 'other is of class %s' % other.__class__
            raise StopIteration

        # parameters
        selfParameters = dict(
            [(x.id(), x) 
             for x in self.getParametersByFilter(FilterModule.TRUE_FILTER)]
        )
        otherParameters = dict(
            [(x.id(), x) 
             for x in other.getParametersByFilter(FilterModule.TRUE_FILTER)]
        )
        diffParameters = \
            set(selfParameters.keys()).symmetric_difference(otherParameters.keys())
        if len(diffParameters) is not 0:
            yield 'different number of parameters'
        else:
            for parameterId, selfParameter in selfParameters.iteritems():
                otherParameter = otherParameters[parameterId]
                if not selfParameter == otherParameter:
                    yield 'parameter %s not the same' % parameterId
                    
        
        # nodes
        selfNodes = dict(
            [(x.id(), x) for x in self.nodes()]
        )
        otherNodes = dict(
            [(x.id(), x) for x in other.nodes()]
        )
        diffNodes = \
            set(selfNodes.keys()).symmetric_difference(otherNodes.keys())
        if len(diffNodes) is not 0:
            yield 'different number of nodes'
        else:
            for nodeId, selfNode in selfNodes.iteritems():
                otherNode = otherNodes[nodeId]
                if not selfNode == otherNode:
                    yield 'node %s not the same' % nodeId
                    

        # parameter connections
        columns = ['source node', 'source parameter', 
                   'target node', 'target parameter']
        for row in self.parameterConnectionsTable().retrieve(columns=columns):
            # TODO:
            # should compare parameter connections
            
            pass
            
        # parameter bindings
        if not self.parameterBindings() == other.parameterBindings():
            yield 'parameter bindings'
            
        raise StopIteration
    
    def __copy__(self):
        
        result = self.__class__()
        
        memo = {self:result}

        # copy the name
        result.name(self.name())

        # copy over the parameters
        notTemporalFilter = FilterModule.constructNotFilter()
        notTemporalFilter.addFilter(
            FilterModule.EquivalenceFilter(ParameterModule.PORT_TYPE_TEMPORAL)
        )
        parameterFilter = FilterModule.ObjectKeyMatchesFilter(
            keyFunction = lambda x: x.portType(),
            filter = notTemporalFilter
        )
        map(result.addParameter, self.getParametersByFilter(parameterFilter))
            
        # copy over the reference definitions
        for node in self.nodes():
            newNode = copy.copy(node)
            result.addNode(newNode)
            memo[node] = newNode
            
        # copy over the parameter connections
        columns = ['source node', 'source parameter', 
                   'target node', 'target parameter']
        for row in self.parameterConnectionsTable().retrieve(columns=columns):
            sourceNode = row[0]
            targetNode = row[2]
            sourceParameter = row[1]
            targetParameter = row[3]
            copySourceNode = memo[sourceNode]
            copyTargetNode = memo[targetNode]
            connection = result.connectParameters(
                copySourceNode, sourceParameter,
                copyTargetNode, targetParameter
                )
            result.addParameterConnectionPath(
                copySourceNode, sourceParameter,
                copyTargetNode, targetParameter,
                tuple([connection])
                )

            pass
            
        # copy over the parameter bindings
        result.parameterBindings(self.parameterBindings())
        
        return result
    
    def getClassForCreateNode(self):
        return ReferenceDefinition
    
    def getClassForCreateEdge(self):
        return ParameterModule.ParameterConnection


    def initializeParameterConnectionPaths(self):
        table = RelationalModule.createTable(
            'parameter connection path',
            ['source node', 'source parameter', 
             'target node', 'target parameter',
             'path', 'additional parameters']
        )
        self.parameterConnectionPathTable(table)
        return

    def initializeParameterConnections(self):
        table = RelationalModule.createTable(
            'parameter connections',
            ['source node', 'source parameter', 
             'target node', 'target parameter',
             'parameter connection']
        )
        self.parameterConnectionsTable(table)
        return
    

    def addDefinition(self, definition):
        self.definitions().add(definition)
        return

    def removeDefinition(self, definition):
        # first need to verify that there is no node
        # which references the definition
        # if there is, throw an exception
        referencedDefinitions = [x.definitionToReference() for x in self.nodes()]
        if definition in referencedDefinitions:
            raise ValueError('cannot remove referenced definition')
        if definition in self.definitions():
            self.definitions().remove(definition)
        return

    def removeNode(self, node):

        # we need to remove the edges here
        # instead of defaulting to grpah
        # because what's presented as an data edge to the user
        # is actually two parameter connections
        # though temporal connections are just a single connection

        # remove just the node, 
        # since we've removed the edges
        GraphModule.Graph.removeNode(self, node, shouldRemoveEdges=False)

        return
    
    def addNewNode(self, definitionToReference, 
                   predecessors=None, successors=None):

        if predecessors is None:
            predecessors = []
        if successors is None:
            successors = []

        # add definitionToReference of definitions that
        # this definition references
        self.addDefinition(definitionToReference)
        
        id = ResourceModule.Resource.ID_GENERATOR()
        node = self.createNode(id=id)

        node.definitionToReference(definitionToReference)

        # add temporal connections
        for predecessor in predecessors:
            connection = self.connectParameters(
                predecessor, 'temporal output',
                node, 'temporal input'
            )
            self.addParameterConnectionPath(
                predecessor, 'temporal output',
                node, 'temporal input',
                tuple([connection])
                )
            pass
        for successor in successors:
            connection = self.connectParameters(
                node, 'temporal output',
                successor, 'temporal input'
            )
            self.addParameterConnectionPath(
                node, 'temporal output',
                successor, 'temporal input',
                tuple([connection])
                )
            pass

        return node
    
    
    def connectParameters(self, 
                          sourceNode, sourceParameter, 
                          targetNode, targetParameter):
        
        edge = self.createEdge([sourceNode, targetNode])
        # this sets the references for parameter connections
        edge.setReferences(sourceNode, sourceParameter,
                           targetNode, targetParameter)

        row = self.parameterConnectionsTable().addRow()
        row.setColumn('source node', sourceNode)
        row.setColumn('source parameter', sourceParameter)
        row.setColumn('target node', targetNode)
        row.setColumn('target parameter', targetParameter)
        row.setColumn('parameter connection', edge)
        
        return edge


    def addParameterConnectionPath(self, 
                                   sourceNode, sourceParameter,
                                   targetNode, targetParameter, path,
                                   additionalParameters=None):

        if additionalParameters is None:
            additionalParameters = tuple([])

        row = self.parameterConnectionPathTable().addRow()

        row.setColumn('source node', sourceNode)
        row.setColumn('source parameter', sourceParameter)
        row.setColumn('target node', targetNode)
        row.setColumn('target parameter', targetParameter)
        row.setColumn('path', path)
        row.setColumn('additional parameters', additionalParameters)

        return



    def constructParameterConnectionFilter(self, 
                                           sourceNode, sourceParameterId,
                                           targetNode, targetParameterId):
        filter = FilterModule.constructAndFilter()
        filter.addFilter(
            RelationalModule.ColumnValueFilter(
                'source node',
                FilterModule.IdentityFilter(sourceNode)
                )
            )
        filter.addFilter(
            RelationalModule.ColumnValueFilter(
                'source parameter',
                FilterModule.EquivalenceFilter(sourceParameterId)
                )
            )
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
        return filter


    def removeParameterConnection(self, connection):
        filter = self.constructParameterConnectionFilter(
            connection.sourceNode(),
            connection.sourceParameter(),
            connection.targetNode(),
            connection.targetParameter())
        self.parameterConnectionsTable().removeRows(filter)
        self.removeEdge(connection)
        return



    def getIdForParameterReference(self, node, parameterId):
        if node is self:
            return parameterId
        return '%s.%s' % (node.id(), parameterId)
    
    
    def _getNotSelfParameterConnectionFilter(self):
    
        theSelfFilter = FilterModule.constructOrFilter()
        theSelfFilter.addFilter(
            FilterModule.ObjectKeyMatchesFilter(
                filter = FilterModule.IdentityFilter(self),
                keyFunction = lambda x: x.sourceNode()
            )
        )
        theSelfFilter.addFilter(
            FilterModule.ObjectKeyMatchesFilter(
                filter = FilterModule.IdentityFilter(self),
                keyFunction = lambda x: x.targetNode()
            )
        )
        theNotSelfFilter = FilterModule.constructNotFilter()
        theNotSelfFilter.addFilter(theSelfFilter)
        return theNotSelfFilter
        
    
    def getMinimalNodes(self):
        theNotSelfFilter = self._getNotSelfParameterConnectionFilter()
        edges = [x for x in theNotSelfFilter.wrap(self.edges())]
        targetNodes = set([x.targetNode() for x in edges])
        allNodes = set(self.nodes())
        
        return allNodes.difference(targetNodes)

    def getMaximalNodes(self):
        theNotSelfFilter = self._getNotSelfParameterConnectionFilter()
        edges = [x for x in theNotSelfFilter.wrap(self.edges())]
        sourceNodes = set([x.sourceNode() for x in edges])
        allNodes = set(self.nodes())
        
        return allNodes.difference(sourceNodes)


    
    def functionToExecute(self):
        return doTask
    
    # END class CompositeDefinition
    pass
    

class NestDefinition(CompositeDefinition):
    
    ATTRIBUTES = CompositeDefinition.ATTRIBUTES


    pass


class LoopDefinition(CompositeDefinition):

    ATTRIBUTES = CompositeDefinition.ATTRIBUTES
    
    PARAMETER_INITIAL_STATE = 'loop initial state'
    PARAMETER_STATE = 'loop state'
    PARAMETER_CONTINUE_CONDITION = 'loop continue condition'
    PARAMETER_STATE_TRANSITION = 'loop state transition'
    PARAMETER_STATE_CONFIGURATION = 'loop state configuration'

    def __init__(self):
        CompositeDefinition.__init__(self)
        
        parameter = ParameterModule.DataParameter(
            id=LoopDefinition.PARAMETER_INITIAL_STATE, 
            portDirection=ParameterModule.PORT_DIRECTION_INPUT)
        self.addParameter(parameter)
        
        parameter = ParameterModule.DataParameter(
            id=LoopDefinition.PARAMETER_CONTINUE_CONDITION, 
            portDirection=ParameterModule.PORT_DIRECTION_INPUT)
        self.addParameter(parameter)
        
        parameter = ParameterModule.DataParameter(
            id=LoopDefinition.PARAMETER_STATE_TRANSITION, 
            portDirection=ParameterModule.PORT_DIRECTION_INPUT)
        self.addParameter(parameter)

        parameter = ParameterModule.DataParameter(
            id=LoopDefinition.PARAMETER_STATE_CONFIGURATION, 
            portDirection=ParameterModule.PORT_DIRECTION_INPUT)
        self.addParameter(parameter)
        
        parameter = ParameterModule.BlackboardParameter(
            LoopDefinition.PARAMETER_STATE)
        self.addParameter(parameter)
        
        connection = self.connectParameters(
            self, LoopDefinition.PARAMETER_INITIAL_STATE,
            self, LoopDefinition.PARAMETER_STATE
        )
        self.addParameterConnectionPath(
            self, LoopDefinition.PARAMETER_INITIAL_STATE,
            self, LoopDefinition.PARAMETER_STATE,
            tuple([connection])
            )
        
        return
    
    
    # END class LoopDefinition
    pass

class BranchDefinition(CompositeDefinition):
    
    ATTRIBUTES = CompositeDefinition.ATTRIBUTES
    
    PARAMETER_CONDITION_STATE = 'branch condition state'
    PARAMETER_CONDITION_FUNCTION = 'branch condition function'
    PARAMETER_CONDITION_MAP = 'branch condition map'
    
    def __init__(self):
        CompositeDefinition.__init__(self)
        
        parameter = ParameterModule.DataParameter(
            id=BranchDefinition.PARAMETER_CONDITION_STATE, 
            portDirection=ParameterModule.PORT_DIRECTION_INPUT)
        self.addParameter(parameter)

        parameter = ParameterModule.DataParameter(
            id=BranchDefinition.PARAMETER_CONDITION_FUNCTION, 
            portDirection=ParameterModule.PORT_DIRECTION_INPUT)
        self.addParameter(parameter)

        parameter = ParameterModule.DataParameter(
            id=BranchDefinition.PARAMETER_CONDITION_MAP, 
            portDirection=ParameterModule.PORT_DIRECTION_INPUT)
        self.addParameter(parameter)

        
        return
    
    # END class BranchDefinition
    pass

class AtomicDefinition(Definition):

    ATTRIBUTES = Definition.ATTRIBUTES + [
        'executable',
        'functionToExecute',
        'commandBuilderType'
    ]
    
    def __init__(self, **kwds):
        Definition.__init__(self, **kwds)
        pass
    
    def hasParameterBinding(self, key):
        return False
    
    def isAtomic(self):
        return True
    
    # END class AtomicDefinition
    pass



def executeTaskInEnvironment(task, *args, **kwds):
    environment = task.workRequest().kwds['execute environment']
    return environment.execute(task, *args, **kwds)


def createParameterOrderingTable():
    table = RelationalModule.createTable('parameter ordering', 
                                         ['source', 'target'])
    return table
    

def createPythonEvalDefinition():
    definition = AtomicDefinition()
    definition.commandBuilderType('python eval')
    
    parameter = ParameterModule.DataParameter(
        id='command', optional=False, active=True,
        portDirection=ParameterModule.PORT_DIRECTION_INPUT)
    ParameterModule.setAttributes(parameter, {})
    definition.addParameter(parameter)
    
    parameter = ParameterModule.DataParameter(
        id='eval result', optional=False, active=True,
        portDirection=ParameterModule.PORT_DIRECTION_OUTPUT)
    ParameterModule.setAttributes(parameter, {})
    definition.addParameter(parameter)
    
    return definition


def createShellProcessDefinition(inputParameters=None,
                                 parameterOrderings=None,
                                 executable=None,
                                 definitionClass=None,
                                 *args, **kwds):
    """
    TODO:
    move this into a utility file
    that makes use of both the definition and command modules
    
    @param inputParameters- a dict of input parameter id to boolean 
           indicating whether they are commandline args
    @param parameterOrderings- a table that specifies the ordering of parameters
           will be used by the comparator for parameter sorting
    """
    
    # this is to handle the case
    # where we are just executing the "command" input
    # as a single command, and not trying to combine commandlines together
    if inputParameters is None:
        inputParameters = {
            'command':{ParameterModule.PORT_ATTRIBUTE_COMMANDLINE:False}
        }
        
    if definitionClass is None:
        definitionClass = AtomicDefinition
    definition = definitionClass(*args, **kwds)
    definition.functionToExecute(executeTaskInEnvironment)

    for inputParameterName, parameterAttributes in inputParameters.iteritems():
        
        isOptional = False
        if parameterAttributes.get(ParameterModule.PORT_ATTRIBUTE_COMMANDLINE, False):
            isOptional = parameterAttributes.get(ParameterModule.PORT_ATTRIBUTE_ISOPTIONAL, False)
            pass

        # whether something is active is actually dynamically set
        # and should be specified on the reference definition
        isActive = True

        parameter = ParameterModule.DataParameter(
            id=inputParameterName, 
            optional=isOptional, active=isActive,
            portDirection=ParameterModule.PORT_DIRECTION_INPUT)
        
        ParameterModule.setAttributes(parameter, parameterAttributes)
        
        definition.addParameter(parameter)
        pass

    if parameterOrderings is None:
        parameterOrderings = createParameterOrderingTable()
    # TODO:
    # need to compute the full transitive reflexive closure of precedences
    definition.parameterOrderingTable(parameterOrderings)

    # default the executable if necessary
    if executable is None:
        import pomsets.command as ExecuteCommandModule
        executable = ExecuteCommandModule.Executable()
        executable.stageable(False)
        executable.path([])
        executable.staticArgs([])
        
    # set the executable
    definition.executable(executable)
    
    definition.commandBuilderType('shell process')
    
    return definition


DEFINITION_SHELLPROCESS = createShellProcessDefinition(
    inputParameters={'command':{ParameterModule.PORT_ATTRIBUTE_COMMANDLINE:False}}
    )
DEFINITION_SSHPROCESS = createShellProcessDefinition(
    inputParameters={'command':{ParameterModule.PORT_ATTRIBUTE_COMMANDLINE:False},
                     'connection':{ParameterModule.PORT_ATTRIBUTE_COMMANDLINE:False}}
    )


    

class ReferenceDefinition(GraphModule.Node, ParameterBindingsHolder):
    
    ATTRIBUTES = GraphModule.Node.ATTRIBUTES + \
               ParameterBindingsHolder.ATTRIBUTES + \
               GuiOptionsHolder.ATTRIBUTES + [
        'parameterSweeps',
        'parameterSweepGroups',
        'parameterStagingMap',
        'comment',
        'isCritical'
    ]
    
    def __init__(self, id=None, graph=None):
        GraphModule.Node.__init__(self, id=id, graph=graph)

        ParameterBindingsHolder.__init__(self)

        self.parameterSweeps(set([]))
        
        table = RelationalModule.createTable('parameter sweep groups',
                                             ['id', 'group'])
        self.parameterSweepGroups(table)

        # this allows us to persist GUI options
        # e.g. node position
        self.guiOptions({})
        
        self.parameterStagingMap({})

        # default to True
        self.isCritical(True)

        pass

    """
    def __eq__(self, other):
        differences = [x for x in self.getDifferencesWith(other)]
        return len(differences) is 0
    
    def getDifferencesWith(self, other):
        if not other:
            yield 'other is None'
            raise StopIteration
        if not self.__class__ == other.__class__:
            yield 'other is of class %s' % other.__class__
            raise StopIteration

        if not self.definitionToReference() == other.definitionToReference():
            yield 'referenced definition'
        
        if not self.parameterBindings() == other.parameterBindings():
            yield 'parameter bindings'
        
        if not self.parameterSweeps() == other.parameterSweeps():
            yield 'parameter sweeps'

        if not self.parameterSweepGroups() == other.parameterSweepGroups():
            yield 'parameter sweep groups'

        if not self.parameterStagingMap() == other.parameterStagingMap():
            yield 'parameter staging map'
        
        return
    """
    def __eq__(self, other):
        return self is other


    def __copy__(self):
        result = ReferenceDefinition(id=self.id())
        result.name(self.name())
        
        result.definitionToReference(self.resource())
        
        result.parameterBindings(self.parameterBindings())
        result.parameterSweeps(self.parameterSweeps())
        result.parameterSweepGroups(self.parameterSweepGroups())
        result.guiOptions(copy.copy(self.guiOptions()))
        result.parameterStagingMap(self.parameterStagingMap())
        
        return result
    
    
    def isAtomic(self):
        if isinstance(self.definitionToReference(), CompositeDefinition):
            return False
        if self.hasParameterSweep():
            return False
        return True
            
    def executable(self):
        return self.definitionToReference().executable()
    
    
    def functionToExecute(self, value=None):
        if value is not None:
            # we do not allow the function to be set
            raise NotImplementedError
        return self._functionToExecute

    def parameterOrderingTable(self):
        return self.definitionToReference().parameterOrderingTable()
    
    def getParameter(self, id):
        return self.definitionToReference().getParameter(id)
    
    
    def getParametersByFilter(self, filter):
        """
        # we've commented this out
        # because we don't want to just return the unbound parameters
        # don't remove this, as this code should go elsewhere
        
        theFilter = FilterModule.constructAndFilter()
        
        theBoundParameterFilter = FilterModule.constructOrFilter()
        for key in self.parameterBindings().keys():
            theBoundParameterFilter.addFilter(
                FilterModule.IdFilter(key)
            )
            pass
        theNotBoundParameterFilter = FilterModule.constructNotFilter()
        theNotBoundParameterFilter.addFilter(theBoundParameterFilter)
        
        theFilter.addFilter(filter)
        theFilter.addFilter(theNotBoundParameterFilter)
        
        return self.definitionToReference().getParametersByFilter(theFilter)
        """
        return self.definitionToReference().getParametersByFilter(filter)
    
    
    def getParameterToEdit(self, parameterName):

        parameter = self.getParameter(parameterName)

        logging.debug('parameter is of type %s' % parameter.portType())

        if parameter.portType() == ParameterModule.PORT_TYPE_BLACKBOARD:
            return (self, parameter)
        if parameter.portType() == ParameterModule.PORT_TYPE_TEMPORAL:
            return (self, parameter)

        logging.debug('parameter is of direction %s' % 
                      parameter.portDirection())
        if parameter.portDirection() == ParameterModule.PORT_DIRECTION_OUTPUT:
            return (self, parameter)
        if parameter.portDirection() == ParameterModule.PORT_DIRECTION_INTERNAL:
            return (self, parameter)

        # at this point, we should only be left with input data parameters
        # so, if there is an incoming parameter connection
        # and the source of that parameter connection is a blackboard parameter
        # then that is the parameter to edit
        """
        parameterConnectionFilter = FilterModule.constructAndFilter()
        parameterConnectionFilter.addFilter(
            RelationalModule.ColumnValueFilter(
                'source node',
                FilterModule.IdentityFilter(self.graph())
                )
            )
        parameterConnectionFilter.addFilter(
            RelationalModule.ColumnValueFilter(
                'target node',
                FilterModule.IdentityFilter(self)
                )
            )
        parameterConnectionFilter.addFilter(
            RelationalModule.ColumnValueFilter(
                'target parameter',
                FilterModule.EquivalenceFilter(parameterName)
                )
            )

        rows = [x for x in 
                self.graph().parameterConnectionsTable().retrieve(
                parameterConnectionFilter, 
                ['source node', 'source parameter']
                )]
        parameters = []
        for sourceNode, sourceParameterName in rows:
            sourceParameter = sourceNode.getParameter(sourceParameterName)
            if not sourceParameter.portType() ==  ParameterModule.PORT_TYPE_BLACKBOARD:
                continue
            parameters.append((sourceNode, sourceParameter))
        """
        filter = FilterModule.constructAndFilter()
        filter.addFilter(
            RelationalModule.ColumnValueFilter(
                'target node',
                FilterModule.IdentityFilter(self)
                )
            )
        filter.addFilter(
            RelationalModule.ColumnValueFilter(
                'target parameter',
                FilterModule.EquivalenceFilter(parameterName)
                )
            )
        
        parameters = RelationalModule.Table.reduceRetrieve(
            self.graph().parameterConnectionPathTable(),
            filter,
            ['additional parameters'], [])


        if len(parameters) is 0:
            logging.debug('no incoming blackboard parameters found')
            return (self, parameter)

        if len(parameters) is not 1:
            raise ValueError('expected only one incoming connection from a blackboard parameter, got %s' % len(parameters))

        parameterId = parameters[0][0]
        parameter = self.graph().getParameter(parameterId)

        logging.debug('returning %s as the parameter to edit for (%s,%s)' %
                      (parameter, self, parameterName))

        # parameters is a list of tuples of (node, parameter)
        return (self.graph(), parameter)

    
    def referencesLibraryDefinition(self):
        definition = self.definitionToReference()
        if definition is None:
            return False
        return definition.isLibraryDefinition()
    
    
    def definitionToReference(self, definition=None):
        if definition is not None:
            self._functionToExecute = definition.functionToExecute()
            pass
        return self.resource(definition)

    
    def incomingEdgeFilter(self):
        filter = FilterModule.constructAndFilter()
        filter.addFilter(self.graph()._getNotSelfParameterConnectionFilter())
        filter.addFilter(GraphModule.Node.incomingEdgeFilter(self))
        return filter

    def outgoingEdgeFilter(self):
        filter = FilterModule.constructAndFilter()
        filter.addFilter(self.graph()._getNotSelfParameterConnectionFilter())
        filter.addFilter(GraphModule.Node.outgoingEdgeFilter(self))
        return filter
    
    def parameterConnectionsTable(self):
        return self.definitionToReference().parameterConnectionsTable()
    
    def getMinimalNodes(self):
        return self.definitionToReference().getMinimalNodes()
    
    def getMaximalNodes(self):
        return self.definitionToReference().getMaximalNodes()
    
    def getIdForParameterReference(self, node, parameterId):
        return self.definitionToReference().getIdForParameterReference(node, parameterId)

    def hasParameterSweep(self):
        return len(self.parameterSweeps()) is not 0

    def isParameterSweep(self, parameterId, value=None):
        
        if value is not None:
            if value:
                self.parameterSweeps().add(parameterId)
            else:
                self.parameterSweeps().discard(parameterId)
        
        return parameterId in self.parameterSweeps()
    
    
    def addParameterSweepGroup(self, groupMembers):
        unknownSweeps = [x for x in groupMembers if x not in self.parameterSweeps()]
        if len(unknownSweeps) is not 0:
            raise NotImplementedError('cannot not group for unknown sweeps %s' % unknownSweeps)

        # TODO:
        # verify that the sweeps are not already in another group
        # in which case, we have to add the ability to add to a group
        
        groupMembersKey = tuple(groupMembers)
        for groupMember in groupMembers:
            row = self.parameterSweepGroups().addRow()
            row.setColumn('id', groupMember)
            row.setColumn('group', groupMembersKey)
            pass
        
        return

    def getGroupForParameterSweep(self, parameterId):
        filter = RelationalModule.ColumnValueFilter(
            'id',
            FilterModule.EquivalenceFilter(parameterId)
        )
        groups = RelationalModule.Table.reduceRetrieve(
            self.parameterSweepGroups(),
            filter,
            ['group'],
            []
        )
        # by default, a parameter sweep is in its own group
        if len(groups) is 0:
            return (parameterId)
        elif len(groups) is not 1:
            raise NotImplementedError(
                'have not implemented case where parameter sweep is in 2 different groups')
        return groups[0]
    
    
    def isReadyToExecute(self, parentTask):
        allPredecessors = set(self.predecessors())
        
        # need to filter for the predecessors that have completed
        predecessorFilter = FilterModule.constructOrFilter()
        for predecessor in allPredecessors:
            predecessorFilter.addFilter(
                RelationalModule.ColumnValueFilter(
                    'definition',
                    FilterModule.IdentityFilter(predecessor)
                )
            )
            pass
        
        completedPredecessorFilter = FilterModule.constructAndFilter()
        completedPredecessorFilter.addFilter(predecessorFilter)
        completedPredecessorFilter.addFilter(
            RelationalModule.ColumnValueFilter(
                'status',
                FilterModule.EquivalenceFilter('completed')
            )
        )
        completedPredecessors = RelationalModule.Table.reduceRetrieve(
            parentTask.tasksTable(),
            completedPredecessorFilter,
            ['definition'],
            []
        )
        
        # if every one of those predecessors
        # can be found in tokens, and vice versa
        return len(set(completedPredecessors).symmetric_difference(allPredecessors)) is 0
    
    
    def parameterStagingRequired(self, parameter, value=None):

        if value is not None:
            self.parameterStagingMap()[parameter] = value
            
        # default to False
        return self.parameterStagingMap().get(parameter, False)
    
    
    # END class ReferenceDefinition
    pass




def getNewNestDefinition():
    return NestDefinition()


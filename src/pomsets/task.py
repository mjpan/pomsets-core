import copy
import logging
import os
import threadpool

import cloudpool.task as TaskModule

import pypatterns.command as CommandModule

import pypatterns.filter as FilterModule
import pypatterns.relational as RelationalModule
import pypatterns.relational.commands as RelationalCommandModule

import pomsets.graph as GraphModule
import pomsets.resource as ResourceModule

import pomsets.definition as DefinitionModule
import pomsets.error as ErrorModule
import pomsets.parameter as ParameterModule




class Task(DefinitionModule.ParameterBindingsHolder, TaskModule.Task):

    ATTRIBUTES = DefinitionModule.ParameterBindingsHolder.ATTRIBUTES + [
        'definition', 
        'threadPool', # we may not need this, as the automaton holds the threadpool
        'parentTask',
        'workRequest',
        'automaton',
    ]
    
    def __init__(self):

        DefinitionModule.ParameterBindingsHolder.__init__(self)
        TaskModule.Task.__init__(self)
        
        pass

    
    def hasParentTask(self):
        return self.parentTask() is not None
    
    def notifyParentOfCompletion(self):
        if self.hasParentTask():
            self.parentTask().childTaskHasCompleted(self)
        return

    def notifyParentOfError(self, errorInfo):
        if self.hasParentTask():
            self.parentTask().childTaskHasErrored(self, errorInfo)
        return

    def getParameterBinding(self, key):
        if not self.hasParameterBinding(key):
            if not self.definition().hasParameterBinding(key):
                raise KeyError('%s not in %s\'s parameter bindings' % (key, self))
            return self.definition().getParameterBinding(key)
        return DefinitionModule.ParameterBindingsHolder.getParameterBinding(self, key)


    def pullParameterBindingsFromDefinition(self):

        if hasattr(self.definition(), 'parameterBindings'):

            """
            for key, value in self.definition().parameterBindings().iteritems():
                if key in self.parameterBindings():
                    continue
                

                self.setParameterBinding(key, value)
                pass
            """
            for parameter in self.definition().getParametersByFilter(FilterModule.TRUE_FILTER):
                parameterId = parameter.id()
                if parameterId in self.parameterBindings():
                    continue

                if self.definition().hasParameterBinding(parameterId):
                    self.setParameterBinding(
                        parameterId,
                        self.definition().getParameterBinding(parameterId))
                elif parameter.defaultValue() is not None:
                    self.setParameterBinding(
                        parameterId,
                        parameter.defaultValue())
                    pass
                pass

        return

    
    def pullDataForParameters(self):
        """
        these functions take the perspective of the child as the active agent

        data is pulled by the child from the blackboard parameters
        and pushed by the child back to blackboard parameters
        """
        if self.hasParentTask():
            self.parentTask().pullDataForParametersOfChild(self)
        return

    
    def pushDataForParameters(self):
        """
        these functions take the perspective of the child as the active agent

        data is pulled by the child from the blackboard parameters
        and pushed by the child back to blackboard parameters
        """
        
        if self.hasParentTask():
            self.parentTask().pushDataForParametersOfChild(self)
        return

    
    def validateParameters(self):

        theFilter = FilterModule.constructAndFilter()
        theFilter.addFilter(
            FilterModule.ObjectKeyMatchesFilter(
                filter=FilterModule.EquivalenceFilter(ParameterModule.PORT_TYPE_DATA),
                keyFunction = lambda x: x.portType()
            )
        )
        theFilter.addFilter(
            FilterModule.ObjectKeyMatchesFilter(
                filter=FilterModule.EquivalenceFilter(ParameterModule.PORT_DIRECTION_INPUT),
                keyFunction = lambda x: x.portDirection()
            )
        )
        theFilter.addFilter(
            FilterModule.ObjectKeyMatchesFilter(
                filter=FilterModule.IdentityFilter(False),
                keyFunction = lambda x: x.optional()
            )
        )


        boundParameterIds = self.parameterBindings().keys()

        parameters = [x for x in 
                      self.definition().getParametersByFilter(theFilter)]
        
        unboundParameterIds = [x.id() for x in parameters
                               if x.id() not in boundParameterIds]
        if len(unboundParameterIds):
            raise NotImplementedError(
                'validation for execution of task %s failed >> cannot process unbound parameters %s' % (self.definition().id(), unboundParameterIds))

    # END class Task
    pass


class CompositeTask(Task):
    """
    Composite tasks require a task generator 
    to generate the tasks that "implements" it
    """
    
    ATTRIBUTES = Task.ATTRIBUTES + [
        'taskGenerator',
        'tasksTable',
        'allChildTasksHaveCompleted',
        'hasGeneratedTasks',
        'isParameterSweepTasksHolder',
        'executionIsPaused',
        'shouldLimitChildrenConcurrency',
        'childrenConcurrencyLimit'
    ]
    
    def __init__(self):
        Task.__init__(self)
        
        self.allChildTasksHaveCompleted(False)
        self.initializeTasksTable()
        self.isParameterSweepTasksHolder(False)

        # default to not limiting children concurrency
        # but once the flag is switched
        # default to only one
        self.shouldLimitChildrenConcurrency(False)
        self.childrenConcurrencyLimit(1)

        return

    def initializeTasksTable(self):
        table = RelationalModule.createTable(
            'tasks', 
            ['definition', 'task', 'request', 'status', 'tokens'],
        )
        self.tasksTable(table)
        return

    
    def _getFilterForRequest(self, request):
        filter = RelationalModule.ColumnValueFilter(
            'request',
            FilterModule.IdentityFilter(request)
        )
        return filter

    
    def _getFilterForDefinition(self, definition):
        filter = RelationalModule.ColumnValueFilter(
            'definition',
            FilterModule.IdentityFilter(definition)
        )
        return filter

    
    def _getFilterForTask(self, task):
        filter = RelationalModule.ColumnValueFilter(
            'task',
            FilterModule.IdentityFilter(task)
        )
        return filter
 

    def getErroredChildTasks(task, recursive=True):

        filter = RelationalModule.ColumnValueFilter(
            'task',
            FilterModule.ObjectKeyMatchesFilter(
                keyFunction = lambda x: x.workRequest().exception,
                filter = FilterModule.IdentityFilter(True)
                )
            )
        for childTask in task.getChildTasks(filter=filter):
            if recursive and isinstance(childTask, CompositeTask):
                for x in childTask.getErroredChildTasks():
                    yield x
                continue
            yield childTask
        raise StopIteration

    
    def getChildTasks(self, filter=None):
        if filter is None:
            filter = FilterModule.TRUE_FILTER
        tasks = RelationalModule.Table.reduceRetrieve(
            self.tasksTable(),
            filter,
            ['task'],
            []
        )
        return tasks
            
    
    def _getChildTasksForDefinition(self, childDefinition):
        filter = self._getFilterForDefinition(childDefinition)
        return self.getChildTasks(filter=filter)
    
    def hasChildTaskForDefinition(self, childDefinition):
        tasks = self._getChildTasksForDefinition(childDefinition)
        return len(tasks) is not 0
    
    
    def getChildTaskForDefinition(self, childDefinition):
        tasks = self._getChildTasksForDefinition(childDefinition)
        if len(tasks) is not 1:
            raise NotImplemented(
                'not implemented for %s tasks for child definition' % len(tasks)
            )
        return tasks[0]
    
    
    def do(self):

        if self.parentTask() is not None:
            self.parentTask().childTaskIsRunning(self)

        # create tasks for each of the minimal nodes in the graph
        # add those tasks to the queue
        # each of the requests being added to the queue
        # should have a callback 

        self.pullDataForParameters()
        
        # see if there are any blackboard parameters to be handled
        self.pullDataForBlackboardParameters()
        
        self.pullParameterBindingsFromDefinition()
      
        self.validateParameters()

        self.preInitializeChildTasks()
        
        self.initializeChildTasks()

        self.startNextTasks()
        
        return True

    
    def initializeChildTasks(self):
        self.taskGenerator().generateReadyTasks(self)
        return
    
    
    def preInitializeChildTasks(self):
        pass

    
    def startNextTasks(self):
        theFilter = RelationalModule.ColumnValueFilter(
            'status',
            FilterModule.EquivalenceFilter('initialized')
        )
        initializedDefinitions = RelationalModule.Table.reduceRetrieve(
            self.tasksTable(),
            theFilter,
            ['definition'],
            []
        )

        map(self.enqueueTaskIfReady, initializedDefinitions)

        return
    
    

    def isReadyToExecute(self, definitionForChildTask):

        if self.isParameterSweepTasksHolder():
            parentTask = self.parentTask()
            return parentTask.isReadyToExecute(definitionForChildTask)

        allPredecessors = set(definitionForChildTask.predecessors())
        
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
            self.tasksTable(),
            completedPredecessorFilter,
            ['definition'],
            []
        )
        
        # if every one of those predecessors
        # can be found in tokens, and vice versa
        readyToExecute = len(set(completedPredecessors).symmetric_difference(allPredecessors)) is 0

    
        return readyToExecute



    def enqueueTaskIfReady(self, definition):

        if not self.isReadyToExecute(definition):
            logging.debug("definition %s is not ready to execute" % definition.name())
            return

        logging.debug("checking if status of task for definition %s is \"initialized\"" % definition.name())

        # retrieve all the tasks that have been initialized
        filter = FilterModule.constructAndFilter()
        filter.addFilter(self._getFilterForDefinition(definition))
        filter.addFilter(
            RelationalModule.ColumnValueFilter(
                'status',
                FilterModule.EquivalenceFilter('initialized')
            )
        )

        # typically we expect only a single row
        # however, for parameter sweep tasks
        # we will get multiple rows that match
        rows = [x for x in self.tasksTable().retrieveForModification(filter)]
        
        # the processing of each row
        # can probably be transformed into a command
        # that creates the task, creates the request
        # enqueues the request
        # and with command builders that update the table
        requests = []
        command = CommandModule.CompositeCommand()
        for row in rows:
            definition = row.getColumn('definition')

            task, request = \
                self.instantiateTaskAndWorkRequestFromDefinition(definition)

            command.addCommand(
                RelationalCommandModule.SetColumnValueCommand(
                    row, 'task', task
                )
            )

            command.addCommand(
                RelationalCommandModule.SetColumnValueCommand(
                    row, 'request', request
                )
            )

            command.addCommand(
                RelationalCommandModule.SetColumnValueCommand(
                    row, 'status', 'queued'
                )
            )

            requests.append(request)
            pass


        # execute the command
        self.automaton().executeCommand(command)

        logging.debug("status of %s tasks for definition %s should now be \"queued\"" % (len(rows), definition.name()))


        # TODO:
        # should instead at this time determine 
        # the actual threadpool to place the request
        
        # enqueue the requests
        map(self.automaton().enqueueRequest, requests, 
            # threadpool.wait is not re-entrant
            # so we don't want it to wait
            [False]*len(requests))
        
        return

    
    def instantiateTaskAndWorkRequestFromDefinition(self, definition):
        task = self.instantiateTaskForChildDefinition(definition)
        request = self.instantiateWorkRequestForTask(task)
        return (task, request)

    def instantiateWorkRequestForTask(self, task):
        """
        create the request from the task
        """
        
        # create a copy of one's own global values
        # update the value for task
        # and provide that to the task
        kwds = copy.copy(self.workRequest().kwds)
        kwds['task'] = task

        automaton = self.automaton()
        callback = automaton.getPostExecuteCallbackFor(task)
        exc_callback = automaton.getErrorCallbackFor(task)
        
        executeTaskFunction = automaton.getExecuteTaskFunction(task)
        
        request = threadpool.WorkRequest(
            executeTaskFunction,
            args = [],
            kwds = kwds,
            callback = callback,
            exc_callback = exc_callback
        )
        task.workRequest(request)
        return request


    def getClassToCreateCompositeTask(self):
        return CompositeTask
    
    def getClassToCreateAtomicTask(self):
        return AtomicTask


    def createChildTaskForDefinition(self, definition):
        return self.taskGenerator().createTaskForDefinition(self, definition)
    
    def instantiateTaskForChildDefinition(self, definition):
        task = self.createChildTaskForDefinition(definition)
        
        return task
        
    
    def pushDataForBlackboardParameters(self):
        self.taskGenerator().pushDataForBlackboardParameters(self)
        return
        

    def getFilterForOwnInternalParameterConnections(self):

        definition = self.definition()
        if isinstance(definition, DefinitionModule.ReferenceDefinition):
            definition = definition.definitionToReference()
        
        theParameterConnectionFilter = FilterModule.constructAndFilter()
        theParameterConnectionFilter.addFilter(
            RelationalModule.ColumnValueFilter(
                'source node',
                FilterModule.IdentityFilter(definition)
            )
        )
        theParameterConnectionFilter.addFilter(
            RelationalModule.ColumnValueFilter(
                'target node',
                FilterModule.IdentityFilter(definition)
            )
        )
        
        #
        # TODO: also move the blackboard filtering part here
        # 
        
        return theParameterConnectionFilter

    
    def pullDataForBlackboardParameters(self):
        self.taskGenerator().pullDataForBlackboardParameters(self)
        return
        
        
    
    def pullDataForParametersOfChild(self, childTask):
        
        self.taskGenerator().pullDataForParametersOfChild(self, childTask)
        return
    
    
    def pushDataForParametersOfChild(self, childTask):
        
        self.taskGenerator().pushDataForParametersOfChild(self, childTask)
        return
        

    def hasInitializedChildTask(self, definition):
        filter = self._getFilterForDefinition(definition)
        for x in self.tasksTable().retrieve(filter, []):
            return True
        return False
    

    def initializeForChildDefinition(self, definition):
        command = CommandModule.CompositeCommand()

        addRowCommand = RelationalCommandModule.AddRowCommand(self.tasksTable())
        command.addCommand(addRowCommand)

        command.addCommandBuilder(
            addRowCommand, 
            SetColumnValueCommandBuilder(
                self.automaton(), addRowCommand, 'definition', definition)
            )
        command.addCommandBuilder(
            addRowCommand, 
            SetColumnValueCommandBuilder(
                self.automaton(), addRowCommand, 'status', 'initialized')
            )

        ret = self.automaton().executeCommand(command)
        
        return


    def getTaskInformation(self, task):
        filter = self._getFilterForTask(task)
        
        rows = [x for x in self.tasksTable().retrieveForModification(filter)]
        if len(rows) is not 1:
            raise NotImplementedError('expected rows for modification to be 1, got %s' % len(rows))
        row = rows[0]
        return row
    

    def childTaskIsRunning(self, task):
        row = self.getTaskInformation(task)
        
        command = RelationalCommandModule.SetColumnValueCommand(
            row, 'status', 'running'
        )
        
        self.automaton().executeCommand(command)
        return

    def childTaskHasCompleted(self, task):
        row = self.getTaskInformation(task)
        
        command = RelationalCommandModule.SetColumnValueCommand(
            row, 'status', 'completed'
        )
        
        self.automaton().executeCommand(command)

        # TODO: add this as a critical section

        # need to check if all have completed
        # if so, then mark as completed
        # otherwise generate more tasks
        if self.taskGenerator().canGenerateMoreTasks(self, completedChildTask=task):

            self.taskGenerator().generateReadyTasks(self, completedChildTask=task)
            self.startNextTasks()
        elif self.taskGenerator().allGeneratedTasksHaveExecuted(self):
            self.postProcessForAllChildTasksHaveCompleted()
            pass
        
        return

    
    def postProcessForAllChildTasksHaveCompleted(self):

        self.pushDataForBlackboardParameters()
        self.pushDataForParameters()
        self.allChildTasksHaveCompleted(True)
        self.notifyParentOfCompletion()
        return
    
    
    def childTaskHasErrored(self, task, errorInfo):

        row = self.getTaskInformation(task)
        
        command = RelationalCommandModule.SetColumnValueCommand(
            row, 'status', 'errored'
        )
        
        self.automaton().executeCommand(command)
        
        request = self.workRequest()
        request.exception = True

        error = NotImplementedError('child task %s has errored')
        errorInfo = (type(error), error, errorInfo)
        request.kwds['error info'] = errorInfo
        request.exc_callback(request, errorInfo)

        return

    # END class CompositeTask
    pass


class DynamicUpdateCompositeTask(CompositeTask):

    
    def preInitializeChildTasks(self):
        if self.shouldUpdateDefinitionForNextInput():
            self.updateDefinitionForNextInput()
        return CompositeTask.preInitializeChildTasks(self)
    
    
    def postProcessForAllChildTasksHaveCompleted(self):
        
        if self.shouldUpdateDefinitionForNextInput():
            nodes = self.updateDefinitionForNextInput()
            self.definitionsForNextTasks = nodes
            self.taskGenerator().generateReadyTasks(self)
            del self.definitionsForNextTasks
            
            self.startNextTasks()
        else:
            CompositeTask.postProcessForAllChildTasksHaveCompleted(self)
        return

    # END class DynamicUpdateCompositeTask
    pass




class AtomicTask(Task):

    ATTRIBUTES = Task.ATTRIBUTES + []
    
    def __init__(self):
        Task.__init__(self)
        
        pass
    
    
    def do(self):

        if self.parentTask() is not None:
            self.parentTask().childTaskIsRunning(self)

        self.configureExecuteEnvironment()
        
        self.pullDataForParameters()
        
        self.pullParameterBindingsFromDefinition()
        
        self.validateParameters()
                
        functionToExecute = self.definition().functionToExecute()
        result = functionToExecute(self)

        self.pushDataForParameters()

        return result
    
    
    def configureExecuteEnvironment(self):
        """
        This enables the task to be assigned to the execute environment
        that is assigned to the task
        """
        request = self.workRequest()

        definition = self.definition()
        if isinstance(definition, DefinitionModule.ReferenceDefinition):
            definition = definition.definitionToReference()

        executeEnvironmentType = definition.executeEnvironmentType()
        workerThreadConfiguresEnvironment = request.kwds.get(
            'worker thread configures execute environment', False)

        if executeEnvironmentType == 'shell process' and \
                workerThreadConfiguresEnvironment:

            # the thread contains the shell
            # to the particular host
            
            workerThread = request.kwds.get('worker thread', None)
            if workerThread is None:
                raise KeyError('need worker thread to configure environment')

            request.kwds['execute environment'] = \
                workerThread.executeEnvironment()
        else:

            executeEnvironmentMap = request.kwds['execute environment map']

            key = definition.executeEnvironmentType()
            if key not in executeEnvironmentMap:
                raise KeyError('no execute environment for type %s' % key)
            request.kwds['execute environment'] = executeEnvironmentMap[key]

        return


    def getCommandBuilderType(self):
        """
        this is called by Task.getCommandBuilder()
        """

        definition = self.definition()
        if isinstance(definition, DefinitionModule.ReferenceDefinition):
            definition = definition.definitionToReference()
        return definition.commandBuilderType()
    
    
    def stageExecutable(self):
        
        executable = self.definition().executable()

        # if executable needs to be staged
        if not executable.stageable():
            return
        
        shell = self.getExecuteEnvironment()

        fs = shell.getFS()

        localPath = executable.path()
        remotePath = shell.getStagedPath(localPath)

        try:
            f = fs.file(remotePath)
            f.close()
        except IOError, e:
            # if the executable does not exist at the remote location
            if not executable.stageable():
                raise ErrorModule.ExecutionError(
                    'unstageable executable does not exist at host')
            shell.stageFile(file=localPath, task=self)
            # shell.setFilePermissions(remotePath, 755)
            import stat
            shell.setFilePermissions(
                remotePath,
                stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
            
        return
    
    def stageInputFiles(self):

        # create a composite filter
        filter = FilterModule.constructAndFilter()
        
        # one of the subfilters is that it is an input data parameter
        filter.addFilter(
            FilterModule.ObjectKeyMatchesFilter(
                keyFunction = lambda x: x.getAttribute(ParameterModule.PORT_ATTRIBUTE_ISINPUTFILE),
                filter = FilterModule.IdentityFilter(True)
            )
        )
        
        # the other is that the parameter requires staging
        filter.addFilter(
            FilterModule.ObjectKeyMatchesFilter(
                keyFunction = lambda x: self.definition().parameterStagingRequired(x.id()),
                filter = FilterModule.IdentityFilter(True)
            )
        )
        
        shell = self.getExecuteEnvironment()

        for parameter in self.definition().getParametersByFilter(filter):
            values = self.getParameterBinding(parameter.id())
            for value in values:
                logging.debug("staging parameter %s file %s" % (parameter.id(),
                                                                value))
                shell.stageFile(file=value, task=self, parameter=parameter)
            pass
            
        return

    def stageOutputFiles(self):
        # create a composite filter
        filter = FilterModule.constructAndFilter()
        
        # one of the subfilters is that it is an input data parameter
        filter.addFilter(
            FilterModule.ObjectKeyMatchesFilter(
                keyFunction = lambda x: x.getAttribute(ParameterModule.PORT_ATTRIBUTE_ISSIDEEFFECT),
                filter = FilterModule.IdentityFilter(True)
            )
        )
        
        # the other is that the parameter requires staging
        filter.addFilter(
            FilterModule.ObjectKeyMatchesFilter(
                keyFunction = lambda x: self.definition().parameterStagingRequired(x.id()),
                filter = FilterModule.IdentityFilter(True)
            )
        )
        
        shell = self.getExecuteEnvironment()

        for parameter in self.definition().getParametersByFilter(filter):
            values = self.getParameterBinding(parameter.id())
            for value in values:
                shell.destageFile(file=value, task=self, parameter=parameter)
            pass
        
        return
    
    
    # END class AtomicTask
    pass



class TaskGenerator(ResourceModule.Struct):
    
    ATTRIBUTES = []
    
    def __init__(self):
        ResourceModule.Struct.__init__(self)
        return
    
    def pullDataForBlackboardParameters(self, parentTask):
        """
        copies the data from the input parameter of the parent task
        to the blackboard parameter
        """

        # create a filter where the source nodes are self,
        # the target node is self, 
        # and the target parameter is of type blackboard
        theParameterConnectionFilter = parentTask.getFilterForOwnInternalParameterConnections()


        for parameterConnection in RelationalModule.Table.reduceRetrieve(
            parentTask.definition().parameterConnectionsTable(),
            theParameterConnectionFilter,
            ['parameter connection']):

            targetParameterId = parameterConnection.targetParameter()

            targetParameter = parentTask.definition().getParameter(targetParameterId)
            if not targetParameter.portType() == ParameterModule.PORT_TYPE_BLACKBOARD:
                continue
            sourceParameterId = parameterConnection.sourceParameter()


            parentTask.setParameterBinding(
                targetParameterId,
                parentTask.getParameterBinding(sourceParameterId)
            )
            pass
        
        return

    
    def pushDataForBlackboardParameters(self, parentTask):
        """
        once all the child tasks have completed
        this copies the data from the blackboard parameters
        to the appropriate output parameters
        """
        
        definition = parentTask.definition()
        if isinstance(definition, DefinitionModule.ReferenceDefinition):
            definition = definition.definitionToReference()
            
        # create a filter where the source nodes are parentTask,
        # the target node is parentTask,
        # and the target parameter is of type blackboard
        theParameterConnectionFilter = FilterModule.constructAndFilter()
        theParameterConnectionFilter.addFilter(
            RelationalModule.ColumnValueFilter(
                'source node',
                FilterModule.IdentityFilter(definition)
            )
        )
        theParameterConnectionFilter.addFilter(
            RelationalModule.ColumnValueFilter(
                'target node',
                FilterModule.IdentityFilter(definition.graph())
            )
        )

        for parameterConnection in RelationalModule.Table.reduceRetrieve(
            definition.parameterConnectionsTable(),
            theParameterConnectionFilter,
            ['parameter connection']):
            
            sourceId = parameterConnection.sourceParameter()
            sourceParameter = parentTask.definition().getParameter(sourceId)
            if not sourceParameter.portType() == ParameterModule.PORT_TYPE_BLACKBOARD:
                continue
            targetId = parameterConnection.targetParameter()
            parentTask.setParameterBinding(
                targetId,
                parentTask.getParameterBinding(sourceId)
            )
            pass
        
        return

    
    def pullDataForParametersOfChild(self, parentTask, childTask):
        
        """
        This takes the bindings of the blackboard parameters
        and moves them to the parameters of the child
        so that the child will have access to them
        """
        
        definition = childTask.definition()

        # bind the data using the parameter connections
        parameterFilter = FilterModule.constructAndFilter()

        parameterFilter.addFilter(
            FilterModule.ObjectKeyMatchesFilter(
                filter=FilterModule.EquivalenceFilter(ParameterModule.PORT_TYPE_DATA),
                keyFunction = lambda x: x.portType()
            )
        )
        parameterFilter.addFilter(
            FilterModule.ObjectKeyMatchesFilter(
                filter=FilterModule.EquivalenceFilter(ParameterModule.PORT_DIRECTION_INPUT),
                keyFunction = lambda x: x.portDirection()
            )
        )
        
        referencedParameters = [
            x for x in definition.getParametersByFilter(parameterFilter)
        ]

        
        # propagate the parameter bindings into the input parameters
        for referencedParameter in referencedParameters:

            connectionFilter = FilterModule.constructAndFilter()
            connectionFilter.addFilter(
                RelationalModule.ColumnValueFilter(
                    'target parameter',
                    FilterModule.EquivalenceFilter(referencedParameter.id())
                )
            )
            connectionFilter.addFilter(
                RelationalModule.ColumnValueFilter(
                    'target node',
                    FilterModule.IdentityFilter(definition)
                )
            )
            
            incomingConnections = RelationalModule.Table.reduceRetrieve(
                parentTask.definition().parameterConnectionsTable(),
                connectionFilter,
                ['parameter connection'],
                []
            )

            if len(incomingConnections) is 0:
                # there's no incoming connection to this parameter
                # check if self has the binding
                if not parentTask.hasParameterBinding(referencedParameter.id()):
                    logging.debug('missing parameter binding for %s' % referencedParameter.id())
                    pass
                continue
            elif len(incomingConnections) is not 1:
                # it's possible that we don't need to handle it here
                logging.debug(
                    'got more than one incoming connection for parameter %s' %
                    referencedParameter.id())
                continue
            
            incomingConnection = incomingConnections[0]
            parameterId = parentTask.definition().getIdForParameterReference(
                incomingConnection.sourceNode(),
                incomingConnection.sourceParameter()
            )

            if not parentTask.hasParameterBinding(parameterId):
                continue
            
            childTask.setParameterBinding(
                referencedParameter.id(),
                parentTask.getParameterBinding(parameterId)
            )
            
            pass
        
        return
    
    def pushDataForParametersOfChild(self, parentTask, childTask):
        """
        This takes the bindings on the parameters of the child
        and moves them to the blackboard parameters
        so that the next child will have access to them
        """
        
        parentTaskDefinition = parentTask.definition()
        if isinstance(parentTaskDefinition, DefinitionModule.ReferenceDefinition):
            parentTaskDefinition = parentTaskDefinition.definitionToReference()
            

        # parameter connection filter
        filter = FilterModule.constructAndFilter()
        filter.addFilter(
            RelationalModule.ColumnValueFilter(
                'source node',
                FilterModule.IdentityFilter(childTask.definition())
            )
        )
        filter.addFilter(
            RelationalModule.ColumnValueFilter(
                'target node',
                FilterModule.IdentityFilter(parentTaskDefinition)
            )
        )

        parameterConnections = RelationalModule.Table.reduceRetrieve(
            parentTaskDefinition.parameterConnectionsTable(),
            filter,
            ['parameter connection'], [])

        for sourceNode, sourceParameterId, targetNode, targetParameterId, parameterConnection in parentTaskDefinition.parameterConnectionsTable().retrieve(
            filter, 
            ['source node', 'source parameter', 'target node', 'target parameter', 'parameter connection']):

            sourceParameter = sourceNode.getParameter(sourceParameterId)
            sourceParameterType = sourceParameter.portType()
            if not sourceParameterType == ParameterModule.PORT_TYPE_DATA:
                continue
            sourceParameterDirection = sourceParameter.portDirection()
            if not (sourceParameterDirection == ParameterModule.PORT_DIRECTION_OUTPUT or 
                    (sourceParameterDirection == ParameterModule.PORT_DIRECTION_INPUT and sourceParameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_ISSIDEEFFECT))):

                continue

            parentTask.setParameterBinding(
                targetParameterId,
                childTask.getParameterBinding(sourceParameterId))

        return
    
    
    # END class TaskGenerator
    pass


class NestTaskGenerator(TaskGenerator):
    """
    This generates tasks for composite tasks wich are nests
    ie contain (recursively) other tasks
    """
    
    ATTRIBUTES = TaskGenerator.ATTRIBUTES + []
    
    @staticmethod
    def createTaskForDefinition(parentTask, definition):
        return createTaskForDefinition(parentTask, definition)
    
    
    def canGenerateMoreTasks(self, parentTask, completedChildTask=None):
        if not hasattr(parentTask, 'hasGeneratedMinimalTasks') or \
           not parentTask.hasGeneratedMinimalTasks:
            return True
        
        # if hasattr(parentTask, 'completedChildTask'):
        if completedChildTask is not None:
            completedDefinition = completedChildTask.definition()
            if len([x for x in completedDefinition.successors()]) is not 0:
                return True
            
        if hasattr(parentTask, 'definitionsForNextTasks'):
            return True

        return False

    
    def generateReadyTasks(self, parentTask, completedChildTask=None):

        if parentTask.executionIsPaused():
            return

        definitionsForNextTasks = []
        if not hasattr(parentTask, 'hasGeneratedMinimalTasks') or \
           not parentTask.hasGeneratedMinimalTasks:
            definitionsForNextTasks.extend(parentTask.definition().getMinimalNodes())
            parentTask.hasGeneratedMinimalTasks=True
        elif completedChildTask is not None:
            completedDefinition = completedChildTask.definition()
            definitionsForNextTasks.extend([x for x in completedDefinition.successors()])
            pass
            
        if hasattr(parentTask, 'definitionsForNextTasks'):
            definitionsForNextTasks.extend(parentTask.definitionsForNextTasks)

        if len(definitionsForNextTasks) is 0:
            # if there are no more ready tasks
            # and all generated tasks have executed
            # (this includes the case where the nest is empty
            #  i.e. no tasks were generated)
            # then just call the post process
            if self.allGeneratedTasksHaveExecuted(parentTask):
                parentTask.postProcessForAllChildTasksHaveCompleted()
                pass
            else:
                raise NotImplementedError('not implemented for when there are no more tasks ready to execute, but when the nest is not empty')
            pass
        else:
            for definitionForNextTask in definitionsForNextTasks:
                # the task may have already have been initialized
                # if it is expecting multiple tokens
                # and this is not the first one
                if not parentTask.hasInitializedChildTask(definitionForNextTask):
                    parentTask.initializeForChildDefinition(definitionForNextTask)
                    pass
                pass
        
        return

    

    @staticmethod
    def getClassToCreateTask(parentTask, definition):
        if not definition.isAtomic():
            return parentTask.getClassToCreateCompositeTask()
        elif isinstance(definition, DefinitionModule.AtomicDefinition):
            return parentTask.getClassToCreateAtomicTask()
        
        definitionToReference = definition.definitionToReference()
        return NestTaskGenerator.getClassToCreateTask(parentTask, definitionToReference)



    
    def allGeneratedTasksHaveExecuted(self, parentTask):
        maximalDefinitions = parentTask.definition().getMaximalNodes()
        definitionFilter = FilterModule.constructOrFilter()
        for maximalDefinition in maximalDefinitions:
            definitionFilter.addFilter(
                RelationalModule.ColumnValueFilter(
                    'definition',
                    FilterModule.IdentityFilter(maximalDefinition)
                )
            )
            pass
        theFilter = FilterModule.constructAndFilter()
        theFilter.addFilter(definitionFilter)
        theFilter.addFilter(
            RelationalModule.ColumnValueFilter(
                'status',
                FilterModule.EquivalenceFilter('completed')
            )
        )
        completedMaximalDefinitions = RelationalModule.Table.reduceRetrieve(
            parentTask.tasksTable(),
            theFilter,
            ['definition'],
            []
        )
        
        # if the set of maximal definitions is the same 
        # as the set of completed maximal definitions
        # then all the tasks have completed
        if len(set(maximalDefinitions).symmetric_difference(completedMaximalDefinitions)) is 0:
            return True
        
        return False
    
    
    # END class NestTaskGenerator
    pass 


class LoopTaskGenerator(TaskGenerator):
    """
    This generates tasks for a sequential loop
    """

    ATTRIBUTES = TaskGenerator.ATTRIBUTES + [
        'loopIndex',
        'taskTable',
        'hasTransitionedState'
    ]
    
    @staticmethod
    def createTaskForDefinition(parentTask, definition):
        return createTaskForDefinition(parentTask, definition)

    
    def __init__(self):
        TaskGenerator.__init__(self)
        table = RelationalModule.createTable(
            'tasks', 
            ['task', 'definition', 'index']
        )
        self.taskTable(table)
        self.loopIndex(0)
        return
    
    def canGenerateMoreTasks(self, parentTask, completedChildTask=None):
        
        # increment the state
        # if the previous task just completed
        # if hasattr(parentTask, 'completedChildTask') and \
        if completedChildTask is not None and \
           not self.hasTransitionedState():
            # here we have to process the state transition
            stateTransitionFunction = eval(
                parentTask.getParameterBinding(
                    DefinitionModule.LoopDefinition.PARAMETER_STATE_TRANSITION
                )
            )
            state = parentTask.getParameterBinding(
                DefinitionModule.LoopDefinition.PARAMETER_STATE
            )
            newState = stateTransitionFunction(state)
            parentTask.setParameterBinding(
                DefinitionModule.LoopDefinition.PARAMETER_STATE,
                newState
            )
            self.hasTransitionedState(True)
            pass
        
        
        shouldContinueFunction = eval(
            parentTask.getParameterBinding(
                DefinitionModule.LoopDefinition.PARAMETER_CONTINUE_CONDITION
            )
        )
        
        state = parentTask.getParameterBinding(
            DefinitionModule.LoopDefinition.PARAMETER_STATE)
        
        shouldContinue = shouldContinueFunction(state)
        
        return shouldContinue

    
    def generateReadyTasks(self, parentTask, completedChildTask=None):
        
        if parentTask.executionIsPaused():
            return

        # the initial state does not satisfy the continue condition
        if not self.canGenerateMoreTasks(parentTask):
            return
        
        parentDefinition = parentTask.definition()
        if isinstance(parentDefinition, DefinitionModule.ReferenceDefinition):
            parentDefinition = parentDefinition.definitionToReference()
        
        definitionsForTask = parentDefinition.nodes()
        if len(definitionsForTask) is not 1:
            raise ValueError('not implemented to loop over %s definitions')
        definitionForTask = definitionsForTask[0]
        
        currentIndex = self.loopIndex()
        
        parentTask.initializeForChildDefinition(definitionForTask)
        row = self.taskTable().addRow()
        row.setColumn('definition', definitionForTask)
        row.setColumn('index', currentIndex)
        row.setColumn('task', None)
        
        self.loopIndex(currentIndex + 1)
        
        # set to False because we just created a new task
        self.hasTransitionedState(False)
        
        return

    
    def allGeneratedTasksHaveExecuted(self, parentTask):
        """
        NOTE: this was copied over from ParameterSweepTaskGenerator
        """
        taskFilter = RelationalModule.ColumnValueFilter(
            'task',
            FilterModule.IdentityFilter(None)
        )
        rows = RelationalModule.Table.reduceRetrieve(
            self.taskTable(),
            taskFilter,
            ['task'],
            []
        )
        if len(rows) is not 0:
            return False
        
        # now ensure that all the tasks have actually completed
        theNotCompletedFilter = FilterModule.constructNotFilter()
        theNotCompletedFilter.addFilter(FilterModule.EquivalenceFilter('completed'))
        
        taskFilter = RelationalModule.ColumnValueFilter(
            'status',
            theNotCompletedFilter
        )
        notCompletedTasks = RelationalModule.Table.reduceRetrieve(
            parentTask.tasksTable(),
            taskFilter,
            ['task'],
            []
        )
        if len(notCompletedTasks) is not 0:
            return False
        return True

    def pullDataForParametersOfChild(self, parentTask, childTask):

        # first call the superclass
        TaskGenerator.pullDataForParametersOfChild(self, parentTask, childTask)
        
        # now process the state configuration
        # we use map/eval because for some reason
        # exec is missing
        configurationCode = parentTask.getParameterBinding(
            DefinitionModule.LoopDefinition.PARAMETER_STATE_CONFIGURATION
        )
        map(eval, configurationCode)
        
        return
    
    # END class LoopTaskGenerator
    pass


class BranchTaskGenerator(TaskGenerator):
    """
    This generates a task.  
    which one it generates depends up on the condition
    """
    
    ATTRIBUTES = TaskGenerator.ATTRIBUTES
    
    @staticmethod
    def createTaskForDefinition(parentTask, definition):
        return createTaskForDefinition(parentTask, definition)

    
    def canGenerateMoreTasks(self, parentTask, completedChildTask=None):
        """
        a branch is only executed once
        """
        if not parentTask.hasGeneratedTasks():
            return True
        return False

    def generateReadyTasks(self, parentTask, completedChildTask=None):
        
        if parentTask.executionIsPaused():
            return

        if not self.canGenerateMoreTasks(
            parentTask, completedChildTask=completedChildTask):
            return
        
        conditionFunction = eval(parentTask.getParameterBinding(
            DefinitionModule.BranchDefinition.PARAMETER_CONDITION_FUNCTION))
        conditionState = parentTask.getParameterBinding(
            DefinitionModule.BranchDefinition.PARAMETER_CONDITION_STATE)
        conditionMap = eval(parentTask.getParameterBinding(
            DefinitionModule.BranchDefinition.PARAMETER_CONDITION_MAP))
        
        condition = conditionFunction(conditionState)

        parentDefinition = parentTask.definition()
        if isinstance(parentDefinition, DefinitionModule.ReferenceDefinition):
            parentDefinition = parentDefinition.definitionToReference()
            
        for conditionKey, definitionId in conditionMap:
            if not conditionKey == condition:
                continue
            
            definitionsForTask = [x for x in parentDefinition.nodes()
                                  if x.id() == definitionId]
            if len(definitionsForTask) is not 1:
                raise NotImplementedError(
                    'not implemented for %s nodes with id %s' % 
                    (len(definitionsForTask), definitionId))
            definitionForTask = definitionsForTask[0]
            if not parentTask.hasInitializedChildTask(definitionForTask):
                parentTask.initializeForChildDefinition(definitionForTask)
            break

        parentTask.hasGeneratedTasks(True)
        
        return
    
    
    def allGeneratedTasksHaveExecuted(self, parentTask):
        
        # now ensure that all the tasks have actually completed
        theNotCompletedFilter = FilterModule.constructNotFilter()
        theNotCompletedFilter.addFilter(FilterModule.EquivalenceFilter('completed'))
        
        taskFilter = RelationalModule.ColumnValueFilter(
            'status',
            theNotCompletedFilter
        )
        notCompletedTasks = RelationalModule.Table.reduceRetrieve(
            parentTask.tasksTable(),
            taskFilter,
            ['task'],
            []
        )
        if len(notCompletedTasks) is not 0:
            return False
        return True
    
    
    # END class BranchTaskGenerator
    pass


class ParameterSweepTaskGenerator(TaskGenerator):
    """
    This generates tasks which are parameter sweeps
    """

    ATTRIBUTES = TaskGenerator.ATTRIBUTES  + ['taskTable']
    
    
    @staticmethod
    def createTaskForDefinition(parentTask, definition):
        """
        this needs to be different than the one for nest task generator
        because this one will not attempt to generate
        additional parameter sweeps for the same task
        thereby resulting in an infinite loop
        """
        
        taskClass = ParameterSweepTaskGenerator.getClassToCreateTask(parentTask, definition)
        task = taskClass()

        task.parentTask(parentTask)
        task.definition(definition)
        task.automaton(parentTask.automaton())
        
        if isinstance(task, CompositeTask):
            taskGenerator = None
            if isinstance(definition, DefinitionModule.CompositeDefinition):
                taskGenerator = NestTaskGenerator()
                pass
            else:
                raise NotImplementedError('not implemented for this type of definition')
            task.taskGenerator(taskGenerator)
            pass

        return task
    
    
    def __init__(self):
        TaskGenerator.__init__(self)
        table = RelationalModule.createTable(
            'tasks', 
            ['task', 'definition', 'index', 'parameter bindings']
        )
        self.taskTable(table)
        return    
    
    def canGenerateMoreTasks(self, parentTask, completedChildTask=None):
        if not parentTask.hasGeneratedTasks():
            return True
        return False
    
    
    def generateReadyTasks(self, parentTask, completedChildTask=None):

        if parentTask.executionIsPaused():
            return

        definitionsForNextTasks = []
        
        if not parentTask.hasGeneratedTasks():
            definitionForTask = parentTask.definition()
            
            # here we need to create new tasks 
            # that are not Parameter Sweep Tasks
            # but use the same definition
            # we will need to bind the correct values to the parameters

            # TODO: handle chunking
            # first, we need to determine how many tasks to create
            numTasksToCreate = 1
            processedGroups = set([])
            parameterSweepIdMap = {}
            for parameterSweepId in definitionForTask.parameterSweeps():
                
                numParameters = len(parentTask.getParameterBinding(parameterSweepId))
                parameterSweepIdMap[parameterSweepId] = numParameters
                
                group = definitionForTask.getGroupForParameterSweep(parameterSweepId)
                if group in processedGroups:
                    continue
                processedGroups.add(group)
                numTasksToCreate *= numParameters
                pass

            # This is a bit complicated
            # so we need to keep track of which index we are in the each ps group
            # so that we can retrieve the value from the same index in each ps group
            # but we also need to keep track of where we are in the total tasks
            # so that we can increment indices in the ps group accordingly
            # so that when one group rolls over, another gets incremented
            parameterSweepGroupIndices = [0 for x in processedGroups]
            indexToGroupMap = dict(enumerate(processedGroups))
            groupToIndexMap = dict([(y, x) for x, y in enumerate(processedGroups)])

            for index in range(numTasksToCreate):
                parentTask.initializeForChildDefinition(definitionForTask)
                row = self.taskTable().addRow()
                row.setColumn('definition', definitionForTask)
                row.setColumn('index', index)
                row.setColumn('task', None)
                parameterBindings = copy.copy(parentTask.parameterBindings())
                for parameterSweepId, modulus in parameterSweepIdMap.iteritems():
                    group = definitionForTask.getGroupForParameterSweep(parameterSweepId)
                    parameterBindings[parameterSweepId] = \
                        [parameterBindings[parameterSweepId][parameterSweepGroupIndices[groupToIndexMap[group]]]]
                    pass
                row.setColumn('parameter bindings', parameterBindings)

                psGroupIndex = len(parameterSweepGroupIndices) - 1
                while True:
                    parameterSweepGroupIndices[psGroupIndex] = (parameterSweepGroupIndices[psGroupIndex] + 1) % parameterSweepIdMap[indexToGroupMap[psGroupIndex][0]]
                    if not parameterSweepGroupIndices[psGroupIndex] == 0:
                        break
                    psGroupIndex = psGroupIndex - 1
                    if psGroupIndex < 0:
                        break
                    pass

                pass
            
            parentTask.hasGeneratedTasks(True)
            parentTask.isParameterSweepTasksHolder(True)
            pass

        return

            
    def pullDataForParametersOfChild(self, parentTask, childTask):
        """
        This takes the bindings of the blackboard parameters
        and moves them to the parameters of the child
        so that the child will have access to them
        """
        
        taskGenerator = parentTask.taskGenerator()
        
        taskFilter = FilterModule.constructAndFilter()
        taskFilter.addFilter(
            RelationalModule.ColumnValueFilter(
                'definition',
                FilterModule.IdentityFilter(childTask.definition())
            )
        )
        taskFilter.addFilter(
            RelationalModule.ColumnValueFilter(
                'task',
                FilterModule.IdentityFilter(None)
            )
        )

        # here we set the parameter bindings for the particular
        # child task. 
        try:
            row = taskGenerator.taskTable().retrieveForModification(taskFilter).next()
            row.setColumn('task', childTask)
            parameterBindings = row.getColumn('parameter bindings')
            for key, value in parameterBindings.iteritems():
                childTask.setParameterBinding(key, value)
                pass
            pass
        except StopIteration:
            raise NotImplementedError(
                'have not implemented if all parameter bindings have been matches')
        return

    
    def pushDataForParametersOfChild(self, parentTask, childTask):
        """
        Parameter sweep tasks do not use this
        because the "child tasks" are actually created from
        the same definition as the parent,
        so there's no need to pass data around
        """
        return

    def pullDataForBlackboardParameters(self, parentTask):
        """
        Parameter sweep tasks do not use this
        because the "child tasks" are actually created from
        the same definition as the parent,
        so there's no need to pass data around
        """
        return
    
    def pushDataForBlackboardParameters(self, parentTask):
        """
        Parameter sweep tasks do not use this
        because the "child tasks" are actually created from
        the same definition as the parent,
        so there's no need to pass data around
        """
        return

    
    @staticmethod
    def getClassToCreateTask(parentTask, definition):
        if isinstance(definition, DefinitionModule.CompositeDefinition):
            return parentTask.getClassToCreateCompositeTask()
        elif isinstance(definition, DefinitionModule.AtomicDefinition):
            return parentTask.getClassToCreateAtomicTask()
        
        definitionToReference = definition.definitionToReference()
        return ParameterSweepTaskGenerator.getClassToCreateTask(parentTask, definitionToReference)



    def allGeneratedTasksHaveExecuted(self, parentTask):
        
        # first check that we have instantiated tasks for all the parameter sets
        taskFilter = RelationalModule.ColumnValueFilter(
            'task',
            FilterModule.IdentityFilter(None)
        )
        rows = RelationalModule.Table.reduceRetrieve(
            self.taskTable(),
            taskFilter,
            ['task'],
            []
        )
        if len(rows) is not 0:
            return False
        
        # now ensure that all the tasks have actually completed
        theNotCompletedFilter = FilterModule.constructNotFilter()
        theNotCompletedFilter.addFilter(FilterModule.EquivalenceFilter('completed'))
        
        taskFilter = RelationalModule.ColumnValueFilter(
            'status',
            theNotCompletedFilter
        )
        notCompletedTasks = RelationalModule.Table.reduceRetrieve(
            parentTask.tasksTable(),
            taskFilter,
            ['task'],
            []
        )
        if len(notCompletedTasks) is not 0:
            return False
        return True
    
    # END class ParameterSweepTask
    pass



class SetColumnValueCommandBuilder(
    RelationalCommandModule.SetColumnValueCommandBuilder):
    
    """
    This command builder is the command builder for the command pattern
    (and not the command builder that constructs the commandline command)
    """
    
    def __init__(self, automaton, *args, **kwds):
        RelationalCommandModule.SetColumnValueCommandBuilder.__init__(
            self, *args, **kwds)
        self._automaton = automaton
        pass

    
    def addCommandsPostExecute(self, commands):
        
        RelationalCommandModule.SetColumnValueCommandBuilder.addCommandsPostExecute(
            self, commands)
            
        return
    
    # END class SetColumnValueCommandBuilder
    pass
    

def getTaskGeneratorForDefinition(definition):
    taskGenerator = None
    
    if isinstance(definition, DefinitionModule.CompositeDefinition):
        # this is the root definition
        taskGenerator = NestTaskGenerator()
        pass
    elif isinstance(definition, DefinitionModule.ReferenceDefinition):
        if definition.hasParameterSweep():
            taskGenerator = ParameterSweepTaskGenerator()
        elif isinstance(definition.definitionToReference(),
                        DefinitionModule.LoopDefinition):
            taskGenerator = LoopTaskGenerator()
        elif isinstance(definition.definitionToReference(),
                        DefinitionModule.BranchDefinition):
            taskGenerator = BranchTaskGenerator()
        elif isinstance(definition.definitionToReference(), 
                        DefinitionModule.CompositeDefinition):
            taskGenerator = NestTaskGenerator()
        pass
    else:
        raise NotImplementedError(
            'not implemented for this type of definition')
    return taskGenerator


def createTaskForDefinition(parentTask, definition):
    taskClass = NestTaskGenerator.getClassToCreateTask(parentTask, definition)
    task = taskClass()
    
    task.parentTask(parentTask)
    task.definition(definition)
    task.automaton(parentTask.automaton())

    if isinstance(task, CompositeTask):
        taskGenerator = getTaskGeneratorForDefinition(definition)
        
        task.taskGenerator(taskGenerator)
        pass

    return task

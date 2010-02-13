import logging
import sys

import resource as ResourceModule

import pomsets.error as ErrorModule

import threadpool as ThreadpoolModule

class Automaton(ResourceModule.Struct):
    """
    Automaton keeps track of execution related data
    """

    ATTRIBUTES = [
        'executeEnvironment',
        'commandManager',
        'contextManager'
    ]
    
    @staticmethod
    def doTask(*args, **kwds):
    
        task = kwds['task']
        
        # TODO:
        # is it possible to set a task's threadpool else
        # eg when the request is enqueued?
        # TODO:
        # remove the need to get the threadpool from kwds
        # should be able to get it directly from the automaton
        task.threadPool(kwds['thread pool'])
    
        import traceback
        try:
            returnValue = task.do()
        except Exception, e:
            exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
            #import StringIO
            #strio = StringIO.StringIO()
            # traceback.print_stack(file=strio)
            # kwds['exception stack trace'] = strio.getvalue()
            taskWorkRequest = task.workRequest()
            taskWorkRequest.kwds['exception stack trace'] = \
                repr(traceback.format_exception(exceptionType, exceptionValue,
                                                exceptionTraceback))


            raise
        
        return returnValue
    
    @staticmethod
    def compositeTaskCompleteCallback(request, result):
        """
        this is called after the nodes have been initialized
        the parent task is notified once all nodes have completed
        """
        logging.debug(
            'task %s has initialized all minimal nodes' % request.kwds['task'])
        
        return
    
    @staticmethod
    def atomicTaskCompleteCallback(request, result):
    
        task = request.kwds['task']
        
        task.notifyParentOfCompletion()
        
        return
    
    
    @staticmethod
    def compositeTaskCriticalErrorCallback(request, errorInfo):
        
        logging.error("%s errored >> %s" % (request, errorInfo))
        
        task = request.kwds['task']
        task.notifyParentOfError(errorInfo)
        
        return


    @staticmethod
    def atomicTaskCriticalErrorCallback(request, errorInfo):
        
        logging.error("%s errored >> %s" % (request, errorInfo))
        
        request.exception = True
        task = request.kwds['task']

        error = NotImplementedError('task %s has errored')
        errorInfo = (type(error), error, errorInfo)
        request.kwds['error info'] = errorInfo

        task.notifyParentOfError(errorInfo)
        
        return


    @staticmethod
    def taskNonCriticalErrorCallback(request, errorInfo):
        
        logging.warn("%s errored >> %s" % (request, errorInfo))
        
        request.exception = True
        error = NotImplementedError('child task %s has errored')
        errorInfo = (type(error), error, errorInfo)
        request.kwds['error info'] = errorInfo

        task = request.kwds['task']
        task.notifyParentOfCompletion()
        
        return


    def __init__(self):
        
        ResourceModule.Struct.__init__(self)
        
        pass



    def raiseError(self, task, error):
        
        # find the root module
        # stop that module
        # and stop all its children recursively
        root = self.getRootTask(task)
        root.stop()

        raise error


    
    def getRootTask(self, task):
        root = task
        while True:
            if root.hasParentTask():
                root= root.parentTask()
            else:
                break

        return root
    
    
    def getThreadPoolUsingRequest(self, request):
        return self._threadpool
    
    def setThreadPool(self, poolId, threadpool):
        self._threadpool = threadpool
        return
    
    
    def enqueueRequest(self, request, shouldWait=True):

        # set the execute environment of the request
        if self.executeEnvironment() is not None:
            request.kwds['execute environment'] = self.executeEnvironment()
        
        threadpool = self.getThreadPoolUsingRequest(request)

        threadpool.putRequest(request)
        
        if shouldWait:
            threadpool.wait()

        if request.exception:
            raise ErrorModule.ExecutionError(
                "request errored")
        
        return


    def executeCommand(self, command):
        self.commandManager().do(command)
        return


    def getPostExecuteCallbackFor(self, task):
        import pomsets.task as TaskModule
        if isinstance(task, TaskModule.CompositeTask):
            return Automaton.compositeTaskCompleteCallback
        return Automaton.atomicTaskCompleteCallback
    
    def getErrorCallbackFor(self, task):

        definition = task.definition()

        # if the definition does not specify a value for isCritical
        # or if it specifies it and the value is True
        if not hasattr(definition, 'isCritical') or \
                definition.isCritical():

            import pomsets.task as TaskModule
            if isinstance(task, TaskModule.CompositeTask):
                return Automaton.compositeTaskCriticalErrorCallback
            return Automaton.atomicTaskCriticalErrorCallback

        return Automaton.taskNonCriticalErrorCallback
    

    def getExecuteTaskFunction(self, task):
        return Automaton.doTask


    def generateRequest(self, task=None, requestKwds=None):

        if task is None:
            raise NotImplementedError('task cannot be None')

        if requestKwds is None:
            raise NotImplementedError('requestKwds cannot be None')

        # TODO:
        # this code currently duplicated in 3 places, 
        # should consolidate into a single function
        # * in CompositeTask, when it instantiates a new work request
        # * in TestExecute
        # * here
        callback = self.getPostExecuteCallbackFor(task)
        exc_callback = self.getErrorCallbackFor(task)
        executeTaskFunction = self.getExecuteTaskFunction(task)

        requestKwds['task'] = task
        request = ThreadpoolModule.WorkRequest(
            executeTaskFunction,
            args = [],
            kwds = requestKwds,
            callback = callback,
            exc_callback = exc_callback
        )

        return request


    def executePomset(self, task=None, pomset=None, requestKwds=None):

        # create a new task
        # we assume that the pomset is a composite definition
        import pomsets.task as TaskModule
        if task is None:
            task = TaskModule.CompositeTask()

        task.initializeTasksTable()
        task.definition(pomset)

        taskGenerator = TaskModule.NestTaskGenerator()
        task.taskGenerator(taskGenerator)

        task.automaton(self)
        
        # generate the request
        request = self.generateRequest(task=task,
                                       requestKwds=requestKwds)

        # check if any threads have been started
        try:
            threadpool = self.getThreadPoolUsingRequest(request)
            """
            should be moved to the cloud automaton's getThreadPoolUsingRequest
            if threadpool.isEmpty():
                raise ExecuteErrorModule.ExecutionError(
                    'need to start thread before execution')
            """


            # now set the thread pool in the request
            request.kwds['thread pool'] = threadpool

            task.workRequest(request)

            self.enqueueRequest(request)
        except ValueError, e:
            logging.error(e)
            raise ExecuteErrorModule.ExecutionError(e)

        return task

    
    #end class Automaton
    pass

import logging
import sys

import resource as ResourceModule


class Automaton(ResourceModule.Struct):

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
            kwds['exception stack trace'] = \
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
    def taskErrorCallback(request, errorInfo):
        
        logging.error("%s errored >> %s" % (request, errorInfo))
        
        task = request.kwds['task']
        task.notifyParentOfError(errorInfo)
        
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
            print "errored >> %s" % request.exception
        
        return

    def executeCommand(self, command):
        self.commandManager().do(command)
        return

    def postEvent(self, event):
        # do nothing
        return

    def getPostExecuteCallbackFor(self, task):
        import pomsets.task as TaskModule
        if isinstance(task, TaskModule.CompositeTask):
            return Automaton.compositeTaskCompleteCallback
        return Automaton.atomicTaskCompleteCallback
    
    def getErrorCallbackFor(self, task):
        return Automaton.taskErrorCallback
    

    def getExecuteTaskFunction(self, task):
        return Automaton.doTask
    
    #end class Automaton
    pass

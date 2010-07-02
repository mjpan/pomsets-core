import pomsets.python as PythonModule


import cloud


class CommandBuilder(PythonModule.CommandBuilder):

    def buildEvalString(self, functionName, arguments):

        arguments = [functionName] + arguments
        functionName = 'cloud.call'

        return PythonModule.CommandBuilder.buildEvalString(self, functionName, arguments)


    # END class CommandBuilder
    pass



class PythonEval(PythonModule.PythonEval):

    def evalResult(self, task, command):
        # NOTE:
        # the import needs to be in the same function
        # as the eval() call
        moduleName = task.definition().executable().module()
        module = None
        if moduleName is not None:
            module = PythonEval.importModule(moduleName)
        import cloud

        evalResult = eval(command)
        return evalResult


    def storeEvalResult(self, task, evalResult):

        # because picloud returns a ticket number
        # we need to store off that ticket number
        # and then get the results of that ticket number
        # and that's the actual evalResult

        ticketId = evalResult
        evalResult = cloud.result(ticketId)

        request = task.workRequest()
        request.kwds['picloud ticket id'] = ticketId

        PythonModule.PythonEval.storeEvalResult(self, task, evalResult)

        return


    
    # END class PythonEval
    pass

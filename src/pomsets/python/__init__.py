import imp
import logging
import os

import cloudpool.environment as EnvironmentModule
import cloudpool.task as TaskModule

import pypatterns.filter as FilterModule

import pomsets.command as CommandModule
import pomsets.parameter as ParameterModule

class Function(CommandModule.Executable):

    ATTRIBUTES = CommandModule.Executable.ATTRIBUTES + []

    def __init__(self):
        CommandModule.Executable.__init__(self)
        self.path(['',''])
        return

    def name(self):
        return self.path()[1]

    def module(self):
        return self.path()[0]

    # END class Function
    pass


class CommandBuilder(TaskModule.CommandBuilder):

    def getFunctionName(self, task):
        # get the function name
        functionName = task.definition().executable().name()

        moduleName = task.definition().executable().module()
        if moduleName is not None:
            functionName = '.'.join(['module', functionName])

        return functionName

    def getArguments(self, task):

        parameterBindings = task.parameterBindings()

        parameterFilter = task.definition().getFilterForCommandlineArguments()
    
        parameters = [
            x for x in
            task.definition().getParametersByFilter(parameterFilter)]

        parameters = ParameterModule.sortParameters(
            parameters, 
            task.definition().parameterOrderingTable())

        commandArgList = []
        for parameter in parameters:
            key = parameter.id()

            value = task.getParameterBinding(key)
            if isinstance(value, str):
                # TODO
                # this should attempt to escape certain chars?
                value = '"%s"' % value
            else:
                value = str(value)

            # need to determine if should pass as keyword
            # and if so, need to retrieve the keyword
            if parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_KEYWORD):
                keyword = parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_KEYWORDTOPASS)
                value = '='.join([keyword, value])

            commandArgList.append(value)
            pass

        return commandArgList


    def buildEvalString(self, functionName, arguments):
        commandList = []
        commandList.append(functionName)
        commandList.append('(')
        commandList.append(', '.join(arguments))
        commandList.append(')')

        command = ''.join(commandList)
        return command


    def buildCommand(self, task):
        workRequest = task.workRequest()

        
        # get the function name
        functionName = self.getFunctionName(task)
        arguments = self.getArguments(task)

        command = self.buildEvalString(functionName, arguments)
        return command
    
    # END class CommandBuilder
    pass


class PythonEval(EnvironmentModule.Environment):

    DEFAULT_COMMANDBUILDER_TYPE = 'python eval'


    @staticmethod
    def importModule(name):

        modulePath = name.split('.')
        module = __import__(modulePath[0])

        if len(modulePath) > 1:
            module = __import__(name, fromlist=modulePath[:1])

        return module


    def storeEvalResult(self, task, evalResult):
        request = task.workRequest()
        request.kwds['eval result'] = evalResult
        task.setParameterBinding('eval result', evalResult)
        return


    def evalResult(self, task, command):
        # NOTE:
        # the import needs to be in the same function
        # as the eval() call
        moduleName = task.definition().executable().module()
        module = None
        if moduleName is not None:
            module = PythonEval.importModule(moduleName)

        evalResult = eval(command)
        return evalResult


    def execute(self, task, *args, **kargs):

        request = task.workRequest()
        
        commandBuilder = self.getCommandBuilder(task)

        command = commandBuilder.buildCommand(task)

        request.kwds['executed command'] = [command]
        logging.debug('%s executing command "%s"' % (self.__class__, command))

        evalResult = self.evalResult(task, command)

        self.storeEvalResult(task, evalResult)

        return 0


    # TODO:
    # implement the following
    # the question is, if this is a python eval
    # and it's taking too long,
    # is there a way to kill it?
    # def kill(self, task, *args, **kwds):
    #     raise NotImplementedError

    
    # END class PythonEval
    pass

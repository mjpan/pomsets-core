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

    def buildCommand(self, task):
        workRequest = task.workRequest()

        
        # get the function name
        functionName = task.definition().executable().name()
        

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

            # TODO:
            # need to determine if should pass as keyword
            # and if so, need to retrieve the keyword
            if parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_KEYWORD):
                keyword = parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_KEYWORDTOPASS)
                value = '='.join([keyword, value])

            commandArgList.append(value)
            pass

        commandList = []
        commandList.append(functionName)
        commandList.append('(')
        commandList.append(', '.join(commandArgList))
        commandList.append(')')

        command = ''.join(commandList)
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

    
    def execute(self, task, *args, **kargs):

        request = task.workRequest()
        
        commandBuilder = self.getCommandBuilder(task)

        command = commandBuilder.buildCommand(task)


        # evalResult = eval(command)
        moduleName = task.definition().executable().module()
        if moduleName is not None:
            module = PythonEval.importModule(moduleName)
            command = '.'.join(['module', command])

        request.kwds['executed command'] = [command]
        logging.debug('%s executing command "%s"' % (self.__class__, command))

        evalResult = eval(command)

        request.kwds['eval result'] = evalResult
        task.setParameterBinding('eval result', evalResult)

        return 0
    
    # END class PythonEval
    pass

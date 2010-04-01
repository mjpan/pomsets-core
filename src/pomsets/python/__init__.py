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
    
        # TODO:
        # should change the naming so that they're not just
        # "command line arguments"
        # more like "explicitly passed arguments"
        parameterFilter = FilterModule.ObjectKeyMatchesFilter(
            filter = FilterModule.IdentityFilter(True),
            keyFunction = lambda x: x.getAttribute(ParameterModule.PORT_ATTRIBUTE_COMMANDLINE)
            )
    
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

        name = name.replace('.', os.path.sep)

        fp, pathname, description = imp.find_module(name)

        try:
            return imp.load_module(name, fp, pathname, description)
        finally:
            # Since we may exit via an exception, close fp explicitly.
            if fp:
                fp.close()
        return

    
    def execute(self, task, *args, **kargs):
        request = task.workRequest()
        
        commandBuilder = self.getCommandBuilder(task)

        command = commandBuilder.buildCommand(task)

        request.kwds['executed command'] = command
        
        logging.debug('%s executing command "%s"' % (self.__class__, command))

        # evalResult = eval(command)
        moduleName = task.definition().executable().module()
        module = PythonEval.importModule(moduleName)
        command = '.'.join(['module', command])
        evalResult = eval(command)

        request.kwds['eval result'] = evalResult
        task.setParameterBinding('eval result', evalResult)
        
        return 0
    
    # END class PythonEval
    pass

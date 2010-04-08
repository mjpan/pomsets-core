import os

import currypy

import cloudpool.task as TaskModule
import cloudpool.environment as ExecuteEnvironmentModule

import pypatterns.filter as FilterModule
import pypatterns.relational as RelationalModule
import pomsets.resource as ResourceModule

import pomsets.parameter as ParameterModule




def buildCommandFunction_default(task):
    """
    This just looks for the key "command" in kwds
    and sets that as the command
    """
    
    kwds = task.parameterBindings()
    command = kwds['command']
    return command


def buildCommandFunction_commandlineArgs(task):

    command = [os.path.sep.join(task.definition().executable().path())] + \
            task.definition().executable().staticArgs() + \
            buildCommandFunction_commandlineArgsOnly(task)
    
    execPath = task.definition().executable().path()

    return command

    
def buildCommandFunction_commandlineArgsOnly(task):
    
    parameterBindings = task.parameterBindings()
    
    commandlineParameterFilter = \
        task.definition().getFilterForCommandlineArguments()

    commandLineParameters = [
        x for x in
        task.definition().getParametersByFilter(commandlineParameterFilter)]

    commandLineParameters = ParameterModule.sortParameters(
        commandLineParameters, 
        task.definition().parameterOrderingTable())
    
    command = []
    for parameter in commandLineParameters:

        command.extend(buildParameterArgument(parameter, parameterBindings))
        
        pass
            
    return command


def buildParameterArgument(parameter, parameterBindings):
    argument = []
    
    # TODO:
    # need to be able to parameterize how the parameter is output
    # currently just outputs the prefix flag
    # and the value placed in parameterBindings for parameter
    
    commandlineOptions = parameter.getAttribute(
        ParameterModule.PORT_ATTRIBUTE_COMMANDLINE_OPTIONS,
        defaultValue = {}
    )
    
    prefixFlag = commandlineOptions.get(
        ParameterModule.COMMANDLINE_PREFIX_FLAG, []
    )
    argumentValue = parameterBindings[parameter.id()]
    
    argumentValueIsList = parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_ISLIST)
    shouldDistributePrefixFlag = commandlineOptions.get(
        ParameterModule.COMMANDLINE_PREFIX_FLAG_DISTRIBUTE, False)
    
    if argumentValueIsList and shouldDistributePrefixFlag:
        # TODO:
        # should call some function recursively
        # so that it can handle the value appropriately
        
        map(argument.extend, [prefixFlag+[x] for x in argumentValue])
    else:
        argumentValueIsEnum = parameter.getAttribute(
            ParameterModule.PORT_ATTRIBUTE_ISENUM)
        if argumentValueIsEnum:
            enumMap = commandlineOptions.get(
                ParameterModule.COMMANDLINE_ENUM_MAP, {}
            )
            # TODO:
            # verify that there's only one item in argumentValue?
            argumentValue = enumMap.get(argumentValue[0], [])
            pass
        
        argument.extend(prefixFlag)
        argument.extend(argumentValue)
        pass
    
    return argument


class CommandBuilder(TaskModule.CommandBuilder):


    def __init__(self, 
                 buildCommandFunction=None):
        
        TaskModule.CommandBuilder.__init__(self)
        
        if buildCommandFunction is None:
            buildCommandFunction = buildCommandFunction_default

        self.buildCommand = buildCommandFunction

        return

    # END class CommandBuilder
    pass


class DelegatingCommandBuilder(CommandBuilder):
    
    def __init__(self, buildCommandFunction=None):

        self._delegate = \
            CommandBuilder(buildCommandFunction=buildCommandFunction)
        
        return
    
    def buildCommand(self, task):
        command = self._delegate.buildCommand(task)
        
        command = self.wrapDelegateCommand(task, command)
        return command
    
    # END class DelegatingCommandBuilder
    pass



class Executable(ResourceModule.Struct):

    # TODO:
    # in the future, whether something is stageable
    # will be determined by whether its dependencies
    # are all stageable
    ATTRIBUTES = [
        'stageable', 'path', 'staticArgs'
    ]
    
    def __init__(self):
        ResourceModule.Struct.__init__(self)
        return
    
    # END class Executable
    pass



class PrintTaskCommand(ExecuteEnvironmentModule.Environment, ResourceModule.Struct):
    
    DEFAULT_COMMANDBUILDER_TYPE = 'print task'
    
    ATTRIBUTES = [
        'outputStream',
        'prefix',
        'postfix'
        ]

    def __init__(self):
        ResourceModule.Struct.__init__(self)
        return
    
    def execute(self, task, *args, **kargs):
        
        commandBuilder = self.getCommandBuilder(task)
        
        command = commandBuilder.buildCommand(task)

        if self.outputStream():
            if self.prefix():
                self.outputStream().write(self.prefix())

            self.outputStream().write(task.definition().name() + '\n')
            self.outputStream().write(' '.join(command))

            if self.postfix():
                self.outputStream().write(self.postfix())
        
        return 0

    
    # END class PrintTaskCommand
    pass

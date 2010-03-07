import os

import currypy

import cloudpool.task as TaskModule
import cloudpool.environment as ExecuteEnvironmentModule

import pypatterns.filter as FilterModule
import pypatterns.relational as RelationalModule
import pomsets.resource as ResourceModule

import pomsets.parameter as ParameterModule


def sortParameters(parameters, orderings):
    # TODO:
    # this is not the most efficient implementation
    # i.e. it creates too many intermediate data structures
    # will have to optimize
    
    # map according to id
    idMap = dict([(x.id(), x) for x in parameters])
    
    # first find all the parameters w/o precedings
    successors = {}
    relations = {}
    for row in orderings.rows():
        predecessor = row.getColumn('source')
        successor = row.getColumn('target')
        relations[predecessor] = \
            relations.get(predecessor, []) + [successor]
        successors[successor] = []
        pass

    toProcess = list(set(idMap.keys()).difference(set(successors.keys())))
    sorted = []
    while len(toProcess):
        current = toProcess.pop(0)
        if current in sorted:
            continue
        toProcess.extend(relations.get(current, []))
        sorted.append(current)
        pass
    
    return [idMap[x] for x in sorted]


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
    
    commandlineParameterFilter = FilterModule.ObjectKeyMatchesFilter(
        filter = FilterModule.IdentityFilter(True),
        keyFunction = lambda x: x.getAttribute(ParameterModule.PORT_ATTRIBUTE_COMMANDLINE)
    )
    
    commandLineParameters = [
        x for x in
        task.definition().getParametersByFilter(commandlineParameterFilter)]

    commandLineParameters = sortParameters(
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



class PrintTaskCommand(ExecuteEnvironmentModule.Environment):
    
    DEFAULT_COMMANDBUILDER_TYPE = 'print task'
    
    def __init__(self):
        return
    
    def outputStream(self, value=None):
        if value is not None:
            self._outputStream = value
        if not hasattr(self, '_outputStream'):
            self._outputStream = None
        return self._outputStream

    def prefix(self, value=None):
        if value is not None:
            self._prefix = value
        if not hasattr(self, '_prefix'):
            self._prefix = None
        return self._prefix

    def postfix(self, value=None):
        if value is not None:
            self._postfix = value
        if not hasattr(self, '_postfix'):
            self._postfix = None
        return self._postfix

    def execute(self, task, *args, **kargs):
        
        commandBuilder = self.getCommandBuilder(task)
        
        command = commandBuilder.buildCommand(task)

        if self.prefix():
            self.outputStream().write(self.prefix())

        self.outputStream().write(task.definition().name() + '\n')
        if self.outputStream():
            self.outputStream().write(' '.join(command))
        if self.postfix():
            self.outputStream().write(self.postfix())
        
        return 0
    
    # END class PrintTaskCommand
    pass

#!/usr/bin/python

import logging
import sys

import cloudpool as CloudModule
import cloudpool.shell as ShellModule

import pypatterns.command as CommandPatternModule

import pomsets.automaton as AutomatonModule
import pomsets.command as TaskCommandModule	
import pomsets.context as ContextModule
import pomsets.error as ErrorModule
import pomsets.task as TaskModule


def configLogging():
    """
    this will be called by all the main functions 
    to use the default setup for logging
    """
    # define a basic logger to write to file
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename='/tmp/execute_pomset.log',
                        filemode='w')

    # end def configureLogging
    pass




def generateRequestKwds():
    kwds = {
        'execute environment map':createExecuteEnvironmentMap(),
        'command builder map':createCommandBuilderMap()
        }
    return kwds

def createCommandBuilderMap():
    commandBuilder = TaskCommandModule.CommandBuilder(
        TaskCommandModule.buildCommandFunction_commandlineArgs
    )
    commandBuilderMap = {
        'shell process':commandBuilder,
    }
    return commandBuilderMap
    

def createExecuteEnvironment():
    return {
        'shell process':ShellModule.LocalShell()
        }
    



def main(args):

    # TODO:
    # integrate opts parsing

    if len(args) < 2:
        raise NotImplementedError('need to specify pomset to execute')

    # TODO:
    # opts needed:
    # config file: static configuration of listed args below
    # threadpool: local, remote, cloud
    # number of threads: easy for local/remote.  how about cloud?

    pomsetPath = args[1]
    pomsetContext = ContextModule.loadPomset(pomsetPath)
    pomset = pomsetContext.reference()

    automaton = AutomatonModule.Automaton()
    automaton.setThreadPool(None, CloudModule.Pool(1))
    automaton.commandManager(CommandPatternModule.CommandManager())
    
    requestKwds = generateRequestKwds()

    compositeTask = TaskModule.CompositeTask()
    try:
        compositeTask = automaton.executePomset(
            task = compositeTask,
            pomset=pomset, requestKwds=requestKwds)
    except ErrorModule.ExecutionError:

        # here we can output various info
        # including execution errors
        workRequest = compositeTask.workRequest()
            
        erroredTasks = [x for x in compositeTask.getErroredChildTasks()]

        print "%s errored tasks" % len(erroredTasks)
        for erroredTask in erroredTasks:
            taskInfo = erroredTask.workRequest().kwds
            print "task: %s" % erroredTask.definition().name()
            if taskInfo.get('executed command', None):
                # this is only available only if the command was actually executed
                print "\tcommand: %s" % taskInfo.get('executed command')
            if taskInfo.get('exception type', None):
                # this should always be available
                print '\t' + ' '.join([taskInfo.get('exception type', None),
                                       taskInfo.get('exception value', None)])

            pass
        pass
    
    return

if __name__=="__main__":

    configLogging()
    main(sys.argv)

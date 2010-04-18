#!/usr/bin/python

import logging
import sys

import StringIO

import cloudpool as CloudModule
import cloudpool.shell as ShellModule

import pypatterns.command as CommandPatternModule

import pomsets.automaton as AutomatonModule
import pomsets.command as TaskCommandModule	
import pomsets.context as ContextModule
import pomsets.error as ErrorModule
import pomsets.python as PythonModule
import pomsets.task as TaskModule


import os
sys.path.insert(0, os.getenv('POMSETS_HOME'))

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
    envMap = createExecuteEnvironmentMap()

    kwds = {
        'execute environment map':envMap,
        'command builder map':createCommandBuilderMap()
        }
    return kwds


def createCommandBuilderMap():

    commandBuilder = TaskCommandModule.CommandBuilder(
        TaskCommandModule.buildCommandFunction_commandlineArgs
    )
    pythonEval = PythonModule.CommandBuilder()
    commandBuilderMap = {
        'print task':commandBuilder,
        'shell process':commandBuilder,
        'python eval':pythonEval
    }
    return commandBuilderMap
    

def createExecuteEnvironmentMap():
    env = TaskCommandModule.PrintTaskCommand()
    env.prefix('# ')
    env.postfix('\n\n')
    s = StringIO.StringIO()
    env.outputStream(s)

    executeEnvironmentMap = {
        'shell process':env,
        'python eval':PythonModule.PythonEval()
        }
    return executeEnvironmentMap



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
            pomset=pomset, requestKwds=requestKwds,
            shouldWait=True)

        request = compositeTask.workRequest()
        if request.exception:
            raise ErrorModule.ExecutionError('erroed on execution')

    except ErrorModule.ExecutionError, e:

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

    s = requestKwds['execute environment map']['shell process'].outputStream()
    #for line in s.readlines():
    #    print line
    print s.getvalue()
    return

if __name__=="__main__":

    configLogging()
    main(sys.argv)

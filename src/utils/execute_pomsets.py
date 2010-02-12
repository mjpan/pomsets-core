import sys
sys.path.insert(0, '/Users/mjpan/pomsets/pomsets.20100105/pomsets/src')

import cloudpool.shell as ShellModule

import pypatterns.command as CommandPatternModule

import pomsets.automaton as AutomatonModule
import pomsets.command as TaskCommandModule	
import pomsets.context as ContextModule

import threadpool

def generateRequestKwds():
    kwds = {
        'execute environment':createExecuteEnvironment(),
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
    return ShellModule.LocalShell()
    

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
    pomset = pomsetContext.pomset()

    automaton = AutomatonModule.Automaton()
    automaton.setThreadPool(None, threadpool.ThreadPool(1))
    automaton.commandManager(CommandPatternModule.CommandManager())
    
    requestKwds = generateRequestKwds()

    compositeTask = automaton.executePomset(
        pomset=pomset, requestKwds=requestKwds)

    # here we can output various info
    # including execution errors
    workRequest = compositeTask.workRequest()

    
    return

if __name__=="__main__":
    main(sys.argv)

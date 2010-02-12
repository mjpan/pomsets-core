import sys
sys.path.insert(0, '/Users/mjpan/pomsets/pomsets.20100105/pomsets/src')

import pomsets.automaton as AutomatonModule
import pomsets.context as ContextModule

import threadpool

def generateRequestKwds():
    kwds = {
        'executeEnvironment':None,
        'commandBuilderMap':None
        }
    return kwds


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
    
    requestKwds = generateRequestKwds()

    compositeTask = automaton.executePomset(
        pomset=None, requestKwds=requestKwds)

    # here we can output various info
    # including execution errors
    workRequest = compositeTask.workRequest()

    
    return

if __name__=="__main__":
    main(sys.argv)

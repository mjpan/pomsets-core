import logging
import os


import pomsets.builder as BuilderModule
import pomsets.command as CommandModule
import pomsets.definition as DefinitionModule


def getHomeLocation():
    return os.getenv('HADOOP_HOME') 

def getExecutablePath():
    return os.path.join(getHomeLocation(), 'bin', 'hadoop')

def getStreamingJar():
    return os.path.join(getHomeLocation(), 'streaming.jar')

class JarExecutable(CommandModule.Executable):

    # TODO:
    # in the future, whether something is stageable
    # will be determined by whether its dependencies
    # are all stageable
    ATTRIBUTES = CommandModule.Executable.ATTRIBUTES + [
        'jarFile',
        'jarClass'
    ]
    
    def __init__(self):
        CommandModule.Executable.__init__(self)
        self.jarClass([])
        self.jarFile([])
        return
    
    def staticArgs(self, value=None):
        return ['jar'] + self.jarFile() + self.jarClass()
    
    # END class Executable
    pass


class PipesExecutable(CommandModule.Executable):
    
    # TODO:
    # in the future, whether something is stageable
    # will be determined by whether its dependencies
    # are all stageable
    ATTRIBUTES = CommandModule.Executable.ATTRIBUTES + [
        'pipesFile',
    ]
    
    def __init__(self):
        CommandModule.Executable.__init__(self)
        self.pipesFile([])
        return
    
    def staticArgs(self, value=None):
        return ['pipes', '-program'] + self.pipesFile()
    
    # END class PipesExecutable
    pass



class Builder(BuilderModule.Builder):


    def createExecutableObject(self, jarFile, jarClass):
        executable = JarExecutable()
        executable.stageable(False)
        executable.path([getExecutablePath()])
        executable.jarFile([jarFile])
        executable.jarClass([jarClass])

        return executable


    def createStreamingExecutableObject(self):

        executable = JarExecutable()
        executable.stageable(False)
        executable.path([getExecutablePath()])
        executable.jarFile([getStreamingJar()])
        executable.jarClass([])

        return executable


    def createPipesExecutableObject(self, path, staticArgs=None):
        command = [path]
        executable = PipesExecutable()
        executable.stageable(False)
        executable.path([getExecutablePath()])
        executable.pipesFile(command)
        return executable


    # END class Builder
    pass

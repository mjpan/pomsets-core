import logging
import os


import pomsets.builder as BuilderModule
import pomsets.command as CommandModule
import pomsets.definition as DefinitionModule


def getHomeLocation():
    return os.getenv('HADOOP_HOME') 

def getExecutablePath():
    return os.path.join(getHomeLocation(), 'bin', 'hadoop')


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


    def createExecutableObject(self):
        executableObject = JarExecutable()
        return executableObject


    def createNewAtomicPomset(self, *args, **kwds):

        if kwds.get('executableObject', None) is None:
            kwds['executableObject'] = self.createExecutableObject()
            pass

        # this should call BuilderModule.Builder.createNewAtomicPomset
        # but then replace the executable object

        raise NotImplemented("%s.createNewAtomicPomset" % self.__class__)


    def createNewPipesPomset(self):

        # this should call BuilderModule.Builder.createNewAtomicPomset
        # but then replace the executable object

        raise NotImplemented("%s.createNewPipesPomset" % self.__class__)

    def createNewStreamingPomset(self):

        # this should call BuilderModule.Builder.createNewAtomicPomset
        # but then replace the executable object

        raise NotImplemented("%s.createNewStreamingPomset" % self.__class__)



    # END class Builder
    pass

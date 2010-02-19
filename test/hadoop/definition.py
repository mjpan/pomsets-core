import os

import pomsets.command as TaskCommandModule
import pomsets.definition as DefinitionModule
import pomsets.parameter as ParameterModule

import pomsets.hadoop as HadoopModule

def getHomeLocation():
    return os.getenv('HADOOP_HOME', 
                     os.path.sep.join(['', 'hadoop']))

def getExamplesJar():
    return os.getenv(
        'HADOOP_JAR_EXAMPLES',
        os.path.join(getHomeLocation(), 'hadoop-0.20.1-examples.jar'))

def getStreamingJar():
    return os.getenv(
        'HADOOP_JAR_STREAMING',
        os.path.join(getHomeLocation(), 
                     os.path.sep.join(['contrib', 'streaming', 'streaming.jar'])))


def createHadoopWordcountDefinition():
    parameterOrdering = DefinitionModule.createParameterOrderingTable()
    row = parameterOrdering.addRow()
    row.setColumn('source', 'input file')
    row.setColumn('target', 'output file')

    # TODO:
    # need to be able to customize this for each host
    executable = HadoopModule.JarExecutable()
    executable.stageable(False)
    executable.path([HadoopModule.getExecutablePath()])
    executable.jarFile([getExamplesJar()])
    executable.jarClass(['wordcount'])
    
    definition = DefinitionModule.createShellProcessDefinition(
        inputParameters = {
            'input file':{
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE:True,
                ParameterModule.PORT_ATTRIBUTE_ISINPUTFILE:True,
            },
            'output file':{
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE:True,
                ParameterModule.PORT_ATTRIBUTE_ISSIDEEFFECT:True,
            }
        },
        parameterOrderings = parameterOrdering,
        executable = executable
    )

    return definition


def createHadoopStreamingDefinition():
    parameterOrdering = DefinitionModule.createParameterOrderingTable()
    row = parameterOrdering.addRow()
    row.setColumn('source', 'input file')
    row.setColumn('target', 'output file')
    row = parameterOrdering.addRow()
    row.setColumn('source', 'output file')
    row.setColumn('target', 'mapper')
    row = parameterOrdering.addRow()
    row.setColumn('source', 'mapper')
    row.setColumn('target', 'reducer')

    
    # TODO:
    # need to be able to customize this for each host
    executable = HadoopModule.JarExecutable()
    executable.stageable(False)
    executable.path([HadoopModule.getExecutablePath()])
    executable.jarFile([getStreamingJar()])
    executable.jarClass([])
    
    
    definition = DefinitionModule.createShellProcessDefinition(
        inputParameters = {
            'input file':{
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE:True,
                ParameterModule.PORT_ATTRIBUTE_ISINPUTFILE:True,
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE_OPTIONS:{
                    ParameterModule.COMMANDLINE_PREFIX_FLAG:['-input']
                    },
            },
            'output file':{
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE:True,
                ParameterModule.PORT_ATTRIBUTE_ISSIDEEFFECT:True,
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE_OPTIONS:{
                    ParameterModule.COMMANDLINE_PREFIX_FLAG:['-output']
                    },
            },
            'mapper':{
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE:True,
                ParameterModule.PORT_ATTRIBUTE_ISINPUTFILE:True,
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE_OPTIONS:{
                    ParameterModule.COMMANDLINE_PREFIX_FLAG:['-mapper']
                    },
            },
            'reducer':{
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE:True,
                ParameterModule.PORT_ATTRIBUTE_ISINPUTFILE:True,
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE_OPTIONS:{
                    ParameterModule.COMMANDLINE_PREFIX_FLAG:['-reducer']
                    },
            },
        },
        parameterOrderings = parameterOrdering,
        executable = executable
    )

    return definition



def createHadoopPipesDefinition():

    parameterOrdering = DefinitionModule.createParameterOrderingTable()
    row = parameterOrdering.addRow()
    row.setColumn('source', 'input file')
    row.setColumn('target', 'output file')
    
    # TODO:
    # need to be able to customize this for each host
    command = ['pipesProgram']
    executable = HadoopModule.PipesExecutable()
    executable.stageable(False)
    executable.path([HadoopModule.getExecutablePath()])
    executable.pipesFile(command)
    
    
    definition = DefinitionModule.createShellProcessDefinition(
        inputParameters = {
            'input file':{
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE:True,
                ParameterModule.PORT_ATTRIBUTE_ISINPUTFILE:True,
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE_OPTIONS:{
                    ParameterModule.COMMANDLINE_PREFIX_FLAG:['-input']
                    },
            },
            'output file':{
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE:True,
                ParameterModule.PORT_ATTRIBUTE_ISSIDEEFFECT:True,
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE_OPTIONS:{
                    ParameterModule.COMMANDLINE_PREFIX_FLAG:['-output']
                    },
            },
        },
        parameterOrderings = parameterOrdering,
        executable = executable
    )

    return definition


import os
import unittest

import pomsets.context as ContextModule
import pomsets.definition as DefinitionModule
import pomsets.hadoop as HadoopModule
import pomsets.parameter as ParameterModule


if not os.getenv('HADOOP_HOME'):
    os.environ['HADOOP_HOME'] = os.path.sep.join(['', 'hadoop'])


class TestBuilder(unittest.TestCase):


    def setUp(self):
        self.builder = HadoopModule.Builder()
        return

    def testCreateDefaultExecutable(self):

        jarFile = 'myJarFile'
        jarClass = 'myJarClass'
        executableObject = self.builder.createExecutableObject(jarFile, jarClass)

        self.assertTrue(isinstance(executableObject, HadoopModule.JarExecutable))
        self.assertFalse(executableObject.stageable())
        self.assertEquals([HadoopModule.getExecutablePath()],
                          executableObject.path())
        self.assertEquals([jarFile],
                          executableObject.jarFile())
        self.assertEquals([jarClass],
                          executableObject.jarClass())

        return


    def testCreateStreamingExecutable(self):

        executableObject = self.builder.createStreamingExecutableObject()

        self.assertTrue(isinstance(executableObject, HadoopModule.JarExecutable))
        self.assertFalse(executableObject.stageable())
        self.assertEquals([HadoopModule.getExecutablePath()],
                          executableObject.path())
        self.assertEquals([HadoopModule.getStreamingJar()],
                          executableObject.jarFile())
        self.assertEquals([],
                          executableObject.jarClass())

        return


    def testCreatePipesExecutable(self):

        path = os.path.sep.join(['', 'path', 'to', 'pipes'])
        executableObject = self.builder.createPipesExecutableObject(path)
        
        self.assertTrue(isinstance(executableObject, HadoopModule.PipesExecutable))
        self.assertFalse(executableObject.stageable())
        self.assertEquals([HadoopModule.getExecutablePath()],
                          executableObject.path())
        self.assertEquals([path],
                          executableObject.pipesFile())

        return


    def testCreateDefinition(self):

        jarFile = 'myJarFile'
        jarClass = 'myJarClass'
        executableObject = self.builder.createExecutableObject(jarFile, jarClass)

        pomsetContext = self.builder.createNewAtomicPomset(
            executableObject=executableObject)
        self.assertTrue(isinstance(pomsetContext, ContextModule.Context))

        pomset = pomsetContext.pomset()
        self.assertTrue(isinstance(pomset, DefinitionModule.AtomicDefinition))

        self.assertTrue(pomset.functionToExecute() is DefinitionModule.executeTaskInEnvironment)

        executable = pomset.executable()
        self.assertTrue(executable is executableObject)

        self.assertEquals('shell process', pomset.commandBuilderType())

        return


    def testAddParameterToAtomicPomset1(self):
        """
        test for basic input value (non-file) parameter
        using all default values for the attribute
        """

        jarFile = 'myJarFile'
        jarClass = 'myJarClass'
        executableObject = self.builder.createExecutableObject(jarFile, jarClass)

        pomsetContext = self.builder.createNewAtomicPomset(
            executableObject=executableObject)
        pomset = pomsetContext.pomset()

        # test for regular input value
        attributes = {
            'direction':ParameterModule.PORT_DIRECTION_INPUT,
            }
        parameterName = 'input value'
        parameter = self.builder.addPomsetParameter(
            pomset, parameterName, attributes)

        self.assertTrue(pomset.hasParameter(parameterName))
        self.assertTrue(pomset.getParameter(parameterName) is parameter)

        self.assertEquals(False, parameter.optional())
        self.assertEquals(True, parameter.active())
        self.assertEquals(
            True,
            parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_COMMANDLINE))
        self.assertEquals(
            False,
            parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_ISINPUTFILE))
        self.assertEquals(
            False,
            parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_ISSIDEEFFECT))
        self.assertEquals(
            False,
            parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_ISLIST))
        self.assertEquals(
            False,
            parameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_ISENUM))

        commandlineOptions = parameter.getAttribute(
            ParameterModule.PORT_ATTRIBUTE_COMMANDLINE_OPTIONS)
        self.assertEquals(
            [], commandlineOptions[ParameterModule.COMMANDLINE_PREFIX_FLAG])
        self.assertEquals(
            False, commandlineOptions[ParameterModule.COMMANDLINE_PREFIX_FLAG_DISTRIBUTE])
        self.assertEquals(
            {}, commandlineOptions[ParameterModule.COMMANDLINE_ENUM_MAP])


        return


    # END class TestBuilder
    pass


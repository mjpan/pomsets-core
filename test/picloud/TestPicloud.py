import os
import unittest

import test.TestExecute as BaseModule
import test.TestOperator as TestPythonModule

import pomsets.parameter as ParameterModule
import pomsets.picloud as PomsetsPicloudModule
import pomsets.task as TaskModule
import pomsets.test_utils as TestUtilsModule



import cloud as PicloudModule



# set picloud into simulator mode
PicloudModule.start_simulator()

def createCommandBuilderMap(instance):
    commandBuilderMap = BaseModule.BaseTestClass.createCommandBuilderMap(instance)
    commandBuilderMap['python eval'] = PomsetsPicloudModule.CommandBuilder()
    return commandBuilderMap


def createExecuteEnvironmentMap():
    return {
        'python eval':PomsetsPicloudModule.PythonEval()
        }



class TestPythonCommandBuilder(TestPythonModule.TestPythonCommandBuilder):


    def setUp(self):
        # do all the setups for the superclass
        TestPythonModule.TestPythonCommandBuilder.setUp(self)

        # substitute the command builder for picloud
        self.commandBuilder = PomsetsPicloudModule.CommandBuilder()
        return



    def assertBuiltCommand(self, expected):
        task = TaskModule.createTaskForDefinition(
            self.parentTask, self.definition)

        command = self.commandBuilder.buildCommand(task)

        self.assertEquals(expected,
                          command)

        return


    def testBuildCommand1(self):
        """
        this ensures that building succeeds
        when there are no arguments
        """

        self.assertBuiltCommand('cloud.call(%s)' % self.functionName)
        return


    def testBuildCommandArgOnly1(self):
        # this ensures that building succeeds
        # when there is only a single non-keyword arg

        parameterId = 'arg1'

        # add parameters to definition
        self.builder.addPomsetParameter(
            self.definitionToReference, parameterId,
            {'direction':ParameterModule.PORT_DIRECTION_INPUT})

        arg1 = 'arg1'
        self.builder.bindParameterValue(self.definition,
                                        parameterId,
                                        arg1)


        self.assertBuiltCommand('cloud.call(%s, "%s")' % (self.functionName, arg1))
        return


    def testBuildCommandArgOnly2(self):
        # this ensures that building succeeds
        # when there are only non-keyword args

        parameter1Id = 'arg1'
        parameter2Id = 'arg2'

        # add parameters to definition
        self.builder.addPomsetParameter(
            self.definitionToReference, parameter1Id,
            {'direction':ParameterModule.PORT_DIRECTION_INPUT})
        self.builder.addPomsetParameter(
            self.definitionToReference, parameter2Id,
            {'direction':ParameterModule.PORT_DIRECTION_INPUT})

        # order parameters
        self.builder.addParameterOrdering(
            self.definitionToReference, 
            parameter1Id, parameter2Id)


        # bind values on task
        arg1 = 'arg1'
        arg2 = 5

        self.builder.bindParameterValue(self.definition,
                                        parameter1Id,
                                        arg1)
        self.builder.bindParameterValue(self.definition,
                                        parameter2Id,
                                        arg2)


        self.assertBuiltCommand('cloud.call(%s, "%s", %s)' % 
                                (self.functionName, arg1, arg2))
        return


    def testBuildCommandKeywordOnly1(self):
        # this ensures that building succeeds
        # when there are only a single keyword arg

        parameterId = 'arg1'
        arg1Key = 'arg1key'
 
        # add parameters to definition
        self.builder.addPomsetParameter(
            self.definitionToReference, parameterId,
            {'direction':ParameterModule.PORT_DIRECTION_INPUT,
             'is keyword':True,
             'keyword to pass':arg1Key})

        arg1 = 'arg1value'
        self.builder.bindParameterValue(self.definition,
                                        parameterId,
                                        arg1)


        self.assertBuiltCommand('cloud.call(%s, %s="%s")' % 
                                (self.functionName, arg1Key, arg1))
        return


    def testBuildCommandKeywordOnly2(self):
        # this ensures that building succeeds
        # when there are only keyword args

        parameter1Id = 'arg1'
        parameter2Id = 'arg2'
        parameter3Id = 'arg3'

        arg1Key = 'arg1key'
        arg2Key = 'arg2key'
        arg3Key = 'arg3key'
 
        # add parameters to definition
        self.builder.addPomsetParameter(
            self.definitionToReference, parameter1Id,
            {'direction':ParameterModule.PORT_DIRECTION_INPUT,
             'is keyword':True,
             'keyword to pass':arg1Key})
        self.builder.addPomsetParameter(
            self.definitionToReference, parameter2Id,
            {'direction':ParameterModule.PORT_DIRECTION_INPUT,
             'is keyword':True,
             'keyword to pass':arg2Key})
        self.builder.addPomsetParameter(
            self.definitionToReference, parameter3Id,
            {'direction':ParameterModule.PORT_DIRECTION_INPUT,
             'is keyword':True,
             'keyword to pass':arg3Key})

        # order parameters
        self.builder.addParameterOrdering(
            self.definitionToReference, 
            parameter2Id, parameter1Id)
        self.builder.addParameterOrdering(
            self.definitionToReference, 
            parameter1Id, parameter3Id)

        arg1 = 'arg1value'
        arg2 = 8
        arg3 = None

        self.builder.bindParameterValue(self.definition,
                                        parameter1Id,
                                        arg1)
        self.builder.bindParameterValue(self.definition,
                                        parameter2Id,
                                        arg2)
        self.builder.bindParameterValue(self.definition,
                                        parameter3Id,
                                        arg3)


        self.assertBuiltCommand('cloud.call(%s, %s=%s, %s="%s", %s=%s)' % 
                                (self.functionName, 
                                 arg2Key, arg2,
                                 arg1Key, arg1,
                                 arg3Key, arg3))
        return


    def testBuildCommand6(self):
        # this ensures that building succeeds
        # when there are both keyword and non-keyword args

        parameter1Id = 'arg1'
        parameter2Id = 'arg2'
        parameter3Id = 'arg3'

        arg1Key = 'arg1key'
        arg3Key = 'arg3key'
 
        # add parameters to definition
        self.builder.addPomsetParameter(
            self.definitionToReference, parameter1Id,
            {'direction':ParameterModule.PORT_DIRECTION_INPUT,
             'is keyword':True,
             'keyword to pass':arg1Key})
        self.builder.addPomsetParameter(
            self.definitionToReference, parameter2Id,
            {'direction':ParameterModule.PORT_DIRECTION_INPUT})
        self.builder.addPomsetParameter(
            self.definitionToReference, parameter3Id,
            {'direction':ParameterModule.PORT_DIRECTION_INPUT,
             'is keyword':True,
             'keyword to pass':arg3Key})

        # order parameters
        self.builder.addParameterOrdering(
            self.definitionToReference, 
            parameter2Id, parameter1Id)
        self.builder.addParameterOrdering(
            self.definitionToReference, 
            parameter1Id, parameter3Id)

        arg1 = 'arg1value'
        arg2 = 8
        arg3 = None

        self.builder.bindParameterValue(self.definition,
                                        parameter1Id,
                                        arg1)
        self.builder.bindParameterValue(self.definition,
                                        parameter2Id,
                                        arg2)
        self.builder.bindParameterValue(self.definition,
                                        parameter3Id,
                                        arg3)


        self.assertBuiltCommand('cloud.call(%s, %s, %s="%s", %s=%s)' % 
                                (self.functionName, 
                                 arg2,
                                 arg1Key, arg1,
                                 arg3Key, arg3))

        return


    # END class 
    pass




class TestLoadListValues1(TestPythonModule.TestLoadListValues1):

    def getPythonEvalDefinition(self):
        pythonEvalDefinition = \
            TestUtilsModule.createLoadListValuesFromFilesDefinition()
        pythonEvalDefinition
        return pythonEvalDefinition


    def createCommandBuilderMap(self):
        return createCommandBuilderMap(self)

    def createExecuteEnvironmentMap(self):
        return createExecuteEnvironmentMap()

    # END class TestLoadListValues1
    pass


class TestLoadListValues2(TestPythonModule.TestLoadListValues2):

    def createCommandBuilderMap(self):
        return createCommandBuilderMap(self)

    def createExecuteEnvironmentMap(self):
        return createExecuteEnvironmentMap()

    # END class TestLoadListValues2
    pass


class TestRange1(TestPythonModule.TestRange1):

    def createCommandBuilderMap(self):
        return createCommandBuilderMap(self)

    def createExecuteEnvironmentMap(self):
        return createExecuteEnvironmentMap()

    # END class TestRange1
    pass


class TestRange2(TestPythonModule.TestRange2):

    def createCommandBuilderMap(self):
        return createCommandBuilderMap(self)

    def createExecuteEnvironmentMap(self):
        return createExecuteEnvironmentMap()

    # END class TestRange2
    pass


class TestRange3(TestPythonModule.TestRange3):
    """
    This tests the default value
    """
    def createCommandBuilderMap(self):
        return createCommandBuilderMap(self)

    def createExecuteEnvironmentMap(self):
        return createExecuteEnvironmentMap()

    # END class TestRange3
    pass



class TestRange4(TestPythonModule.TestRange4):

    def createCommandBuilderMap(self):
        return createCommandBuilderMap(self)

    def createExecuteEnvironmentMap(self):
        return createExecuteEnvironmentMap()

    # END class TestRange4
    pass



class TestMakeDirs1(TestPythonModule.TestMakeDirs1):

    def createCommandBuilderMap(self):
        return createCommandBuilderMap(self)

    def createExecuteEnvironmentMap(self):
        return createExecuteEnvironmentMap()

    # END class TestMakeDirs1
    pass


class TestMakeDirs2(TestPythonModule.TestMakeDirs2):

    def createCommandBuilderMap(self):
        return createCommandBuilderMap(self)

    def createExecuteEnvironmentMap(self):
        return createExecuteEnvironmentMap()

    # END TestMakeDirs2
    pass



class TestStringReplace1(TestPythonModule.TestStringReplace1):

    def createCommandBuilderMap(self):
        return createCommandBuilderMap(self)

    def createExecuteEnvironmentMap(self):
        return createExecuteEnvironmentMap()

    # END class TestStringReplace1
    pass

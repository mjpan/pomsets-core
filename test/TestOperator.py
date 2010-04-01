import os
import unittest


import TestExecute as BaseModule

import pomsets.builder as BuilderModule
import pomsets.python as PythonModule
import pomsets.task as TaskModule
import pomsets.parameter as ParameterModule

class TestPythonFunction(unittest.TestCase):

    def setUp(self):
        self.builder = BuilderModule.Builder()
        return

    def tearDown(self):
        return
    
    def testGetFunctionName(self):

        functionName = 'testFunction'


        executable = self.builder.createExecutableObject(
            ['', functionName],
            executableClass=PythonModule.Function)

        self.assertEquals(functionName, executable.name())

        return

    # END class TestPythonFunction
    pass


class TestPythonCommandBuilder(unittest.TestCase):


    def setUp(self):
        self.builder = BuilderModule.Builder()

        self.functionName = 'testFunction'
        self.executableObject = self.builder.createExecutableObject(
            ['', self.functionName],
            executableClass=PythonModule.Function)

        parentContext = self.builder.createNewNestPomset(name='root')
        parentDefinition = parentContext.pomset()
        
        pythonEvalContext = self.builder.createNewAtomicPomset(
            name='testPython',
            executableObject = self.executableObject,
            commandBuilderType = 'python eval')
        pythonEvalDefinition = pythonEvalContext.pomset()
        self.definitionToReference = pythonEvalDefinition

        self.definition = self.builder.createNewNode(
            parentDefinition, name='node', 
            definitionToReference=pythonEvalDefinition)

        self.commandBuilder = PythonModule.CommandBuilder()

        parentTask = TaskModule.CompositeTask()
        parentTask.definition(parentDefinition)
        taskGenerator = TaskModule.NestTaskGenerator()
        parentTask.taskGenerator(taskGenerator)
        self.parentTask = parentTask

        return


    def tearDown(self):
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

        self.assertBuiltCommand('%s()' % self.functionName)
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


        self.assertBuiltCommand('%s("%s")' % (self.functionName, arg1))
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


        self.assertBuiltCommand('%s("%s", %s)' % 
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


        self.assertBuiltCommand('%s(%s="%s")' % 
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


        self.assertBuiltCommand('%s(%s=%s, %s="%s", %s=%s)' % 
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


        self.assertBuiltCommand('%s(%s, %s="%s", %s=%s)' % 
                                (self.functionName, 
                                 arg2,
                                 arg1Key, arg1,
                                 arg3Key, arg3))

        return


    # END class 
    pass



class TestLoadListValues(BaseModule.BaseTestClass, unittest.TestCase):


    BASE_DIR = os.path.join(os.getcwd(), 'resources', 'testdata', 'TestOperator')
    INPUT_FILE = 'listValues1'


    def setUp(self):
        BaseModule.BaseTestClass.setUp(self)
        self.builder = BuilderModule.Builder()
        return


    def createDefinition(self):
        path = ['pomsets.python.operator',
                'loadListValuesFromFiles']
        executableObject = self.builder.createExecutableObject(
            path,
            executableClass=PythonModule.Function)

        pythonEvalContext = self.builder.createNewAtomicPomset(
            name='testPython',
            executableObject = executableObject,
            commandBuilderType = 'python eval')
        pythonEvalDefinition = pythonEvalContext.pomset()
        self.builder.addPomsetParameter(
            pythonEvalDefinition, 'eval result',
            {'direction':ParameterModule.PORT_DIRECTION_OUTPUT,
             'commandline':False})
        self.builder.addPomsetParameter(
            pythonEvalDefinition, 'file to read',
            {'direction':ParameterModule.PORT_DIRECTION_INPUT})


        parentContext = self.builder.createNewNestPomset(name='root')
        parentDefinition = parentContext.pomset()
        

        self.definition = self.builder.createNewNode(
            parentDefinition, name='node', 
            definitionToReference=pythonEvalDefinition)


        self.builder.bindParameterValue(
            self.definition,
            'file to read',
            [os.path.sep.join([TestLoadListValues.BASE_DIR, 
                               TestLoadListValues.INPUT_FILE])]
            )

        return parentDefinition


    def createCommandBuilderMap(self):
        commandBuilderMap = BaseModule.BaseTestClass.createCommandBuilderMap(self)
        commandBuilderMap['python eval'] = PythonModule.CommandBuilder()
        return commandBuilderMap


    def createExecuteEnvironment(self):
        return PythonModule.PythonEval()


    def createTask(self, definition):

        parentTask = TaskModule.CompositeTask()
        parentTask.definition(definition)
        taskGenerator = TaskModule.NestTaskGenerator()
        parentTask.taskGenerator(taskGenerator)

        # TODO:
        # bind the parameters
        #task = TaskModule.createTaskForDefinition(
        #    parentTask, self.definition)
        # return task

        self.parentTask = parentTask

        return parentTask


    def getPicklePath(self):
        return os.path.sep + \
            os.path.join('tmp', 'TestOperator.TestLoadListValues.testExecute2')


    def assertPostExecute(self):
        BaseModule.BaseTestClass.assertPostExecute(self)

        childTasks = self.parentTask.getChildTasks()
        childTask = childTasks[0]

        values = childTask.getParameterBinding('eval result')

        self.assertEquals(['/tmp/foo', '/user/home/me/bar.txt', './baz.ext'],
                          values)
        
        return 


    # END class TestOperator
    pass


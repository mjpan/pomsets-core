import unittest

import pypatterns.command as CommandPatternModule

import pomsets.automaton as AutomatonModule
import pomsets.builder as BuilderModule
import pomsets.task as TaskModule
import pomsets.test_utils as TestUtilsModule

class TestParameterSweepTaskGenerator(unittest.TestCase):

    def setUp(self):
        
        # create the automaton
        self.automaton = AutomatonModule.Automaton()
        self.automaton.commandManager(CommandPatternModule.CommandManager())


        # create the generator
        self.generator = TaskModule.ParameterSweepTaskGenerator()

        # create the pomset
        self.pomset = TestUtilsModule.createPomsetContainingParameterSweep()
        TestUtilsModule.bindParameterSweepDefinitionParameters(self.pomset)

        return

    def tearDown(self):
        return

    def testGenerateTasks1(self):

        parentDefinition = [x for x in self.pomset.nodes()
                            if x.id() == 'mapper'][0]
        

        # create the parent task
        parentTask = TaskModule.CompositeTask()
        parentTask.definition(parentDefinition)
        parentTask.automaton(self.automaton)

        parentTask.pullParameterBindingsFromDefinition()

        self.generator.generateReadyTasks(parentTask)

        self.assertEquals(2,
                          self.generator.taskTable().rowCount())

        expectedValueMap = {
            0:{
                'input file':['/tmp/pomsets/text1'],
                'output file':['/tmp/pomsets/count1']
                },
            1:{
                'input file':['/tmp/pomsets/text2'],
                'output file':['/tmp/pomsets/count2']
                },
            }


        # now assert the generated tasks
        for row in self.generator.taskTable().rows():
            index = row.getColumn('index')
            expectedValues = expectedValueMap[index]
            parameterBindings = row.getColumn('parameter bindings')
            for key, expectedBinding in parameterBindings.iteritems():
                self.assertEquals(expectedBinding,
                                  parameterBindings[key])

        

        return


    def testGenerateTasks2(self):

        parentDefinition = [x for x in self.pomset.nodes()
                            if x.id() == 'mapper'][0]
        
        parentDefinition.removeParameterSweepGroup(
            ['input file', 'output file'])

        # create the parent task
        parentTask = TaskModule.CompositeTask()
        parentTask.definition(parentDefinition)
        parentTask.automaton(self.automaton)

        parentTask.pullParameterBindingsFromDefinition()

        self.generator.generateReadyTasks(parentTask)

        self.assertEquals(4,
                          self.generator.taskTable().rowCount())

        expectedValueMap = {
            0:{
                'input file':['/tmp/pomsets/text1'],
                'output file':['/tmp/pomsets/count1']
                },
            1:{
                'input file':['/tmp/pomsets/text2'],
                'output file':['/tmp/pomsets/count2']
                },
            2:{
                'input file':['/tmp/pomsets/text1'],
                'output file':['/tmp/pomsets/count2']
                },
            3:{
                'input file':['/tmp/pomsets/text2'],
                'output file':['/tmp/pomsets/count1']
                },
            }


        # now assert the generated tasks
        for row in self.generator.taskTable().rows():
            index = row.getColumn('index')
            expectedValues = expectedValueMap[index]
            parameterBindings = row.getColumn('parameter bindings')
            for key, expectedBinding in parameterBindings.iteritems():
                self.assertEquals(expectedBinding,
                                  parameterBindings[key])

        

        return





    # END class TestTaskGenerator
    pass

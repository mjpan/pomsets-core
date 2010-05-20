from __future__ import with_statement

import os
import sys
import unittest

import StringIO

import currypy

import pypatterns.filter as FilterModule

import pomsets.definition as DefinitionModule
import pomsets.parameter as ParameterModule
import pomsets.task as TaskModule

import TestExecute as BaseModule


def createExecuteEnvironment():
    io = StringIO.StringIO()
    import pomsets.command as ExecuteEnvironmentModule
    env = ExecuteEnvironmentModule.PrintTaskCommand()
    env.postfix('\n')
    env.outputStream(io)
    return env


class TestCase1(BaseModule.TestCase1):
    """
    execute of atomic function
    """

    
    def createExecuteEnvironmentMap(self):
        self.env = createExecuteEnvironment()
        return {
            'shell process':self.env
            }

    def assertPostExecute(self):
        expected = """echo
/bin/echo foo
"""
        actual = self.env.outputStream().getvalue()
        self.assertEquals(expected, actual)
        return
    
    
    # END TestCase1
    pass


class TestCase2(BaseModule.TestCase2):
    """
    execute of atomic function
    """

    def createExecuteEnvironmentMap(self):
        self.env = createExecuteEnvironment()
        return {
            'shell process':self.env
            }

    def assertPostExecute(self):
        expected = """echo
/bin/echo "echoed testExecuteAtomicFunction2"
"""
        actual = self.env.outputStream().getvalue()
        self.assertEquals(expected, actual)
        return
    
    # END class TestCase2
    pass




class TestCase4(BaseModule.TestCase4):
    """
    execute of composite function
    """

    def createExecuteEnvironmentMap(self):
        self.env = createExecuteEnvironment()
        return {
            'shell process':self.env
            }

    def assertPostExecute(self):
        expected = """echo
/bin/echo foo
"""
        actual = self.env.outputStream().getvalue()
        self.assertEquals(expected, actual)
        return

    # END class TestCase4
    pass



class TestCase8(BaseModule.TestCase8):
    """
    execute of composite function
    """

    def createExecuteEnvironmentMap(self):
        self.env = createExecuteEnvironment()
        return {
            'shell process':self.env
            }


    def assertPostExecute(self):
        expected = """echo 0
/bin/echo foo
echo 1
/bin/echo foo
echo 2
/bin/echo foo
"""
        actual = self.env.outputStream().getvalue()
        self.assertEquals(expected, actual)
        return
    
    # END class TestCase8
    pass


class TestCase9(BaseModule.TestCase9):
    """
    execute of composite function
    """

    def createExecuteEnvironmentMap(self):
        self.env = createExecuteEnvironment()
        return {
            'shell process':self.env
            }

    def assertPostExecute(self):
        expected = """echo 0
/bin/echo foo
echo 1
/bin/echo foo
echo 2
/bin/echo foo
"""
        actual = self.env.outputStream().getvalue()
        self.assertEquals(expected, actual)
        return
    
    # END class TestCase9
    pass


class TestCase10(BaseModule.TestCase10):
    """
    execution fails due to incomplete parameter binding 
    """

    def createExecuteEnvironmentMap(self):
        self.env = createExecuteEnvironment()
        return {
            'shell process':self.env
            }

    # END class TestCase
    pass


class TestCase12(BaseModule.TestCase12):
    """
    execute of composite function
    """

    def createExecuteEnvironmentMap(self):
        self.env = createExecuteEnvironment()
        return {
            'shell process':self.env
            }


    def assertPostExecute(self):
        expected = """echo
/bin/echo foo
"""
        actual = self.env.outputStream().getvalue()
        self.assertEquals(expected, actual)
        return
    
    # END class TestCase12
    pass


class TestCase13(BaseModule.TestCase13):
    """
    execute of composite function
    """

    def createExecuteEnvironmentMap(self):
        self.env = createExecuteEnvironment()
        return {
            'shell process':self.env
            }


    def assertPostExecute(self):
        expected = """echo
/bin/echo foo
"""
        actual = self.env.outputStream().getvalue()
        self.assertEquals(expected, actual)
        return
    
    # END class TestCase13
    pass


class TestNest1(BaseModule.TestNest1):
    """
    execute of nest in composite function
    """

    def createExecuteEnvironmentMap(self):
        self.env = createExecuteEnvironment()
        return {
            'shell process':self.env
            }


    def assertPostExecute(self):
        expected = """atomic node 1
/bin/echo foo 1
atomic node 2
/bin/echo foo 2
"""
        actual = self.env.outputStream().getvalue()
        self.assertEquals(expected, actual)
        return
    
    # END class TestNest1
    pass


class TestNest2(BaseModule.TestNest2):
    """
    execute of nest in composite function
    """

    def createExecuteEnvironmentMap(self):
        self.env = createExecuteEnvironment()
        return {
            'shell process':self.env
            }


    def assertPostExecute(self):
        expected = """atomic node 1
/bin/echo foo 1
atomic node 3
/bin/echo foo 3
atomic node 2
/bin/echo foo 2
"""
        actual = self.env.outputStream().getvalue()
        self.assertEquals(expected, actual)
        return
    
    # END class TestNest2
    pass


class TestNest3(BaseModule.TestNest3):
    """
    execute of nest in composite function
    """

    def createExecuteEnvironmentMap(self):
        self.env = createExecuteEnvironment()
        return {
            'shell process':self.env
            }


    def assertPostExecute(self):
        expecteds = [
"""atomic node 1
/bin/echo foo 1
atomic node 3
/bin/echo foo 3
atomic node 2
/bin/echo foo 2
""",
"""atomic node 3
/bin/echo foo 3
atomic node 1
/bin/echo foo 1
atomic node 2
/bin/echo foo 2
"""
]
        actual = self.env.outputStream().getvalue()
        self.assertTrue(actual in expecteds)
        return
    
    # END class TestNest3
    pass


class TestNest4(BaseModule.TestNest4):
    """
    execute of nest in composite function
    """

    def createExecuteEnvironmentMap(self):
        self.env = createExecuteEnvironment()
        return {
            'shell process':self.env
            }


    def assertPostExecute(self):
        expecteds = [
"""atomic node 1
/bin/echo foo 1
atomic node 3
/bin/echo foo 3
atomic node 2
/bin/echo foo 2
""",
"""atomic node 1
/bin/echo foo 1
atomic node 2
/bin/echo foo 2
atomic node 3
/bin/echo foo 3
"""
]
        actual = self.env.outputStream().getvalue()
        self.assertTrue(actual in expecteds)
        return
    
    # END class TestNest4
    pass


class TestNest5(BaseModule.TestNest5):
    """
    execute of nest in composite function
    """

    def createExecuteEnvironmentMap(self):
        self.env = createExecuteEnvironment()
        return {
            'shell process':self.env
            }


    def assertPostExecute(self):
        expected = """atomic node 1
/bin/echo foo 1
atomic node 3
/bin/echo foo 3
atomic node 2
/bin/echo foo 2
"""
        actual = self.env.outputStream().getvalue()
        self.assertEquals(expected, actual)
        return
    
    # END class TestNest5
    pass


class TestNest6(BaseModule.TestNest6):
    """
    execute of nest in composite function
    """

    def createExecuteEnvironmentMap(self):
        self.env = createExecuteEnvironment()
        return {
            'shell process':self.env
            }


    def assertPostExecute(self):
        expected = """atomic node 1
/bin/echo foo 1
atomic node 2
/bin/echo foo 2
atomic node 3
/bin/echo foo 3
"""
        actual = self.env.outputStream().getvalue()
        self.assertEquals(expected, actual)
        return
    
    # END class TestNest6
    pass


class TestNest7(BaseModule.TestNest7):
    """
    execute of nest in composite function
    """

    def createExecuteEnvironmentMap(self):
        self.env = createExecuteEnvironment()
        return {
            'shell process':self.env
            }


    def assertPostExecute(self):
        expecteds = [
"""atomic node 1
/bin/echo foo 1
atomic node 2
/bin/echo foo 2
atomic node 3
/bin/echo foo 3
""",
"""atomic node 2
/bin/echo foo 2
atomic node 1
/bin/echo foo 1
atomic node 3
/bin/echo foo 3
"""
]
        actual = self.env.outputStream().getvalue()
        self.assertTrue(actual in expecteds)
        return
    
    # END class TestNest7
    pass


class TestNest8(BaseModule.TestNest8):
    """
    execute of nest in composite function
    """

    def createExecuteEnvironmentMap(self):
        self.env = createExecuteEnvironment()
        return {
            'shell process':self.env
            }


    def assertPostExecute(self):
        expecteds = [
"""atomic node 3
/bin/echo foo 3
atomic node 1
/bin/echo foo 1
atomic node 2
/bin/echo foo 2
""",
"""atomic node 3
/bin/echo foo 3
atomic node 2
/bin/echo foo 2
atomic node 1
/bin/echo foo 1
"""
]
        actual = self.env.outputStream().getvalue()
        self.assertTrue(actual in expecteds)
        return
    
    # END class TestNest8
    pass



class TestParameterSweep1(BaseModule.TestParameterSweep1):

    def createExecuteEnvironmentMap(self):
        self.env = createExecuteEnvironment()
        return {
            'shell process':self.env
            }

    def assertPostExecute(self):
        expected = """echo
/bin/echo "echoed TestParameterSweep1 : 1"
echo
/bin/echo "echoed TestParameterSweep1 : 2"
"""
        actual = self.env.outputStream().getvalue()
        self.assertEquals(expected, actual)
        return
    
    
    # END class TestParameterSweep1
    pass


class TestParameterSweep2(BaseModule.TestParameterSweep2):

    def createExecuteEnvironmentMap(self):
        self.env = createExecuteEnvironment()
        return {
            'shell process':self.env
            }

    def assertPostExecute(self):
        expected = """mapper\n%s/resources/testdata/TestExecute/wordcount.py %s/resources/testdata/TestExecute/text1 /tmp/count1\nmapper\n%s/resources/testdata/TestExecute/wordcount.py %s/resources/testdata/TestExecute/text2 /tmp/count2
""" % tuple([os.getcwd()]*4)

        actual = self.env.outputStream().getvalue()
        self.assertEquals(expected, actual)
        return

    
    # END class TestParameterSweep2
    pass


class TestParameterSweep3(BaseModule.TestParameterSweep3):

    def createExecuteEnvironmentMap(self):
        self.env = createExecuteEnvironment()
        return {
            'shell process':self.env
            }

    def assertPostExecute(self):
        expected = """reducer\n%s/resources/testdata/TestExecute/wordcount_reduce.py -input %s/resources/testdata/TestExecute/count1 %s/resources/testdata/TestExecute/count2 -output /tmp/count_reduce
""" %  tuple([os.getcwd()]*3)

        actual = self.env.outputStream().getvalue()
        self.assertEquals(expected, actual)
        return
    
    # END class TestParameterSweep3
    pass


class TestParameterSweep4(BaseModule.TestParameterSweep4):
    """
    tests combining a mapper with a reducer
    """

    def createExecuteEnvironmentMap(self):
        self.env = createExecuteEnvironment()
        return {
            'shell process':self.env
            }

    def assertPostExecute(self):
        expected = """mapper\n%s/resources/testdata/TestExecute/wordcount.py %s/resources/testdata/TestExecute/text1 /tmp/count1\nmapper\n%s/resources/testdata/TestExecute/wordcount.py %s/resources/testdata/TestExecute/text2 /tmp/count2\nreducer\n%s/resources/testdata/TestExecute/wordcount_reduce.py -input /tmp/count1 /tmp/count2 -output /tmp/count_reduce
"""  % tuple([os.getcwd()]*5)

        actual = self.env.outputStream().getvalue()
        self.assertEquals(expected, actual)
        return
    
    # END class TestParameterSweep4
    pass




class TestParameterSweep5(BaseModule.TestParameterSweep4):
    """
    This is the same as TestParameterSweep4 
    with the exception that the mapper's parameter sweep parameters
    are not in the same parameter sweep group
    """


    def createDefinition(self):
    
        compositeDefinition = DefinitionModule.getNewNestDefinition()

        # setup the reference definition for parameter sweep
        mapperNode = compositeDefinition.createNode(id='mapper')
        mapperNode.definitionToReference(BaseModule.DEFINITION_WORDCOUNT)
        mapperNode.isParameterSweep('input file', True)
        mapperNode.isParameterSweep('output file', True)
        mapperNode.name('mapper')

        reducerNode = compositeDefinition.createNode(id='reducer')
        reducerNode.definitionToReference(BaseModule.DEFINITION_WORDCOUNT_REDUCE)
        reducerNode.name('reducer')

        inputParameter = \
            ParameterModule.DataParameter(
                id='input file', 
                portDirection=ParameterModule.PORT_DIRECTION_INPUT)
        inputParameter.setAttribute(
            ParameterModule.PORT_ATTRIBUTE_PARAMETERSWEEP, True)
        compositeDefinition.addParameter(inputParameter)
        compositeDefinition._connectParameters(
            compositeDefinition, 'input file',
            mapperNode, 'input file'
        )
        
        outputParameter = \
            ParameterModule.DataParameter(
                id='output file', 
                portDirection=ParameterModule.PORT_DIRECTION_INPUT)
        compositeDefinition.addParameter(outputParameter)
        compositeDefinition._connectParameters(
            compositeDefinition, 'output file',
            reducerNode, 'output file'
        )
        
        blackboardParameter = \
            ParameterModule.BlackboardParameter('intermediate file')
        compositeDefinition.addParameter(blackboardParameter)
        compositeDefinition._connectParameters(
            compositeDefinition, 'intermediate file',
            mapperNode, 'output file'
        )

        edge = compositeDefinition.connectNodes(
            mapperNode, 'output file',
            reducerNode, 'input files',
        )
        self.intermediateParameterId = edge.path()[3]

        return compositeDefinition


    def createExecuteEnvironmentMap(self):
        self.env = createExecuteEnvironment()
        return {
            'shell process':self.env
            }

    def assertPostExecute(self):
        expected = """mapper\n%s/resources/testdata/TestExecute/wordcount.py %s/resources/testdata/TestExecute/text1 /tmp/count1\nmapper\n%s/resources/testdata/TestExecute/wordcount.py %s/resources/testdata/TestExecute/text2 /tmp/count1\nmapper\n%s/resources/testdata/TestExecute/wordcount.py %s/resources/testdata/TestExecute/text1 /tmp/count2\nmapper\n%s/resources/testdata/TestExecute/wordcount.py %s/resources/testdata/TestExecute/text2 /tmp/count2\nreducer\n%s/resources/testdata/TestExecute/wordcount_reduce.py -input /tmp/count1 /tmp/count2 -output /tmp/count_reduce
"""  % tuple([os.getcwd()]*9)

        actual = self.env.outputStream().getvalue()
        self.assertEquals(expected, actual)
        return

    
    # END class TestParameterSweep5
    pass




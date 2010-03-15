from __future__ import with_statement

import os
import sys
import unittest

import StringIO

import currypy

import pypatterns.filter as FilterModule

import TestExecute as BaseModule

class TestCase1(BaseModule.TestCase1):
    """
    execute of atomic function
    """

    def createExecuteEnvironment(self):
        io = StringIO.StringIO()
        import pomsets.command as ExecuteEnvironmentModule
        self.env = ExecuteEnvironmentModule.PrintTaskCommand()
        self.env.postfix('\n')
        self.env.outputStream(io)
        return self.env

    def assertPostExecute(self):
        expected = """echo
/bin/echo foo
"""
        actual = self.env.outputStream().getvalue()
        assert expected == actual, \
               'expected "%s", got "%s"' % (expected, actual)
        return
    
    
    # END TestCase1
    pass


class TestCase2(BaseModule.TestCase2):
    """
    execute of atomic function
    """

    def createExecuteEnvironment(self):
        io = StringIO.StringIO()
        import pomsets.command as ExecuteEnvironmentModule
        self.env = ExecuteEnvironmentModule.PrintTaskCommand()
        self.env.postfix('\n')
        self.env.outputStream(io)
        return self.env
    
    def assertPostExecute(self):
        expected = """echo
/bin/echo "echoed testExecuteAtomicFunction2"
"""
        actual = self.env.outputStream().getvalue()
        assert expected == actual, \
               'expected "%s", got "%s"' % (expected, actual)
        return
    
    # END class TestCase2
    pass




class TestCase4(BaseModule.TestCase4):
    """
    execute of composite function
    """

    def createExecuteEnvironment(self):
        io = StringIO.StringIO()
        import pomsets.command as ExecuteEnvironmentModule
        self.env = ExecuteEnvironmentModule.PrintTaskCommand()
        self.env.postfix('\n')
        self.env.outputStream(io)
        return self.env

    def assertPostExecute(self):
        expected = """echo
/bin/echo foo
"""
        actual = self.env.outputStream().getvalue()
        assert expected == actual, \
               'expected "%s", got "%s"' % (expected, actual)
        return

    # END class TestCase4
    pass



class TestCase8(BaseModule.TestCase8):
    """
    execute of composite function
    """

    def createExecuteEnvironment(self):
        io = StringIO.StringIO()
        import pomsets.command as ExecuteEnvironmentModule
        self.env = ExecuteEnvironmentModule.PrintTaskCommand()
        self.env.postfix('\n')
        self.env.outputStream(io)
        return self.env

    def assertPostExecute(self):
        expected = """echo 0
/bin/echo foo
echo 1
/bin/echo foo
echo 2
/bin/echo foo
"""
        actual = self.env.outputStream().getvalue()
        assert expected == actual, \
               'expected "%s", got "%s"' % (expected, actual)
        return
    
    # END class TestCase8
    pass


class TestCase9(BaseModule.TestCase9):
    """
    execute of composite function
    """

    def createExecuteEnvironment(self):
        io = StringIO.StringIO()
        import pomsets.command as ExecuteEnvironmentModule
        self.env = ExecuteEnvironmentModule.PrintTaskCommand()
        self.env.postfix('\n')
        self.env.outputStream(io)
        return self.env
    
    def assertPostExecute(self):
        expected = """echo 0
/bin/echo foo
echo 1
/bin/echo foo
echo 2
/bin/echo foo
"""
        actual = self.env.outputStream().getvalue()
        assert expected == actual, \
               'expected "%s", got "%s"' % (expected, actual)
        return
    
    # END class TestCase9
    pass


class TestCase10(BaseModule.TestCase10):
    """
    execution fails due to incomplete parameter binding 
    """

    def createExecuteEnvironment(self):
        io = StringIO.StringIO()
        import pomsets.command as ExecuteEnvironmentModule
        self.env = ExecuteEnvironmentModule.PrintTaskCommand()
        self.env.postfix('\n')
        self.env.outputStream(io)
        return self.env

    
    # END class TestCase
    pass



class TestParameterSweep1(BaseModule.TestParameterSweep1):

    def createExecuteEnvironment(self):
        io = StringIO.StringIO()
        import pomsets.command as ExecuteEnvironmentModule
        self.env = ExecuteEnvironmentModule.PrintTaskCommand()
        self.env.postfix('\n')
        self.env.outputStream(io)
        return self.env

    def assertPostExecute(self):
        expected = """echo
/bin/echo "echoed testExecuteParameterSweep1 : 1"
echo
/bin/echo "echoed testExecuteParameterSweep1 : 2"
"""
        actual = self.env.outputStream().getvalue()
        assert expected == actual, \
               'expected "%s", got "%s"' % (expected, actual)
        return
    
    
    # END class TestParameterSweep1
    pass


class TestParameterSweep2(BaseModule.TestParameterSweep2):

    def createExecuteEnvironment(self):
        io = StringIO.StringIO()
        import pomsets.command as ExecuteEnvironmentModule
        self.env = ExecuteEnvironmentModule.PrintTaskCommand()
        self.env.postfix('\n')
        self.env.outputStream(io)
        return self.env

    def assertPostExecute(self):
        expected = """mapper\n%s/resources/testdata/TestExecute/wordcount.py %s/resources/testdata/TestExecute/text1 /tmp/count1\nmapper\n%s/resources/testdata/TestExecute/wordcount.py %s/resources/testdata/TestExecute/text2 /tmp/count2
""" % tuple([os.getcwd()]*4)

        actual = self.env.outputStream().getvalue()
        self.assertEquals(expected, actual)
        return

    
    # END class TestParameterSweep2
    pass


class TestParameterSweep3(BaseModule.TestParameterSweep3):

    def createExecuteEnvironment(self):
        io = StringIO.StringIO()
        import pomsets.command as ExecuteEnvironmentModule
        self.env = ExecuteEnvironmentModule.PrintTaskCommand()
        self.env.postfix('\n')
        self.env.outputStream(io)
        return self.env

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

    def createExecuteEnvironment(self):
        io = StringIO.StringIO()
        import pomsets.command as ExecuteEnvironmentModule
        self.env = ExecuteEnvironmentModule.PrintTaskCommand()
        self.env.postfix('\n')
        self.env.outputStream(io)
        return self.env
    
    def assertPostExecute(self):
        expected = """mapper\n%s/resources/testdata/TestExecute/wordcount.py %s/resources/testdata/TestExecute/text1 /tmp/count1\nmapper\n%s/resources/testdata/TestExecute/wordcount.py %s/resources/testdata/TestExecute/text2 /tmp/count2\nreducer\n%s/resources/testdata/TestExecute/wordcount_reduce.py -input /tmp/count1 /tmp/count2 -output /tmp/count_reduce
"""  % tuple([os.getcwd()]*5)

        actual = self.env.outputStream().getvalue()
        self.assertEquals(expected, actual)
        return
    
    # END class TestParameterSweep4
    pass




def main():
    sys.path.insert(0, '../src')

    import utils
    utils.configLogging()

    suite = unittest.TestSuite()

    suite.addTest(unittest.makeSuite(TestCase1, 'test'))
    suite.addTest(unittest.makeSuite(TestCase2, 'test'))
    suite.addTest(unittest.makeSuite(TestCase4, 'test'))
    suite.addTest(unittest.makeSuite(TestCase8, 'test'))
    suite.addTest(unittest.makeSuite(TestCase9, 'test'))
    suite.addTest(unittest.makeSuite(TestCase10, 'test'))
    suite.addTest(unittest.makeSuite(TestParameterSweep1, 'test'))
    suite.addTest(unittest.makeSuite(TestParameterSweep2, 'test'))
    suite.addTest(unittest.makeSuite(TestParameterSweep3, 'test'))
    suite.addTest(unittest.makeSuite(TestParameterSweep4, 'test'))

    runner = unittest.TextTestRunner()
    runner.run(suite)
    return

if __name__=="__main__":
    main()


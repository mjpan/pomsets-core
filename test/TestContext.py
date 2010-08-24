import unittest

import pomsets.context as ContextModule

class TestCase(unittest.TestCase):

    def testOverlayObjects(self):
        # overlayObjects(self, baseValues, objectPaths):

        context = ContextModule.Context()

        raise NotImplementedError

    def testOverlayExecuteEnvironments1(self):
        """
        test for legacy, 
        when the executeEnvironments attribute was not yet defined
        """
        context = ContextModule.Context()
        context._executeEnvironments = None

        baseValues = {}
        context.overlayExecuteEnvironments(baseValues)
        self.assertEquals({}, baseValues)

        return


    def testOverlayExecuteEnvironments2(self):
        """
        ensure that the appropriate error is thrown on invalid value
        """
        context = ContextModule.Context()
        context.addExecuteEnvironment('foo', 'bar')
        
        baseValues = {}
        self.assertRaises(NotImplementedError,
                          context.overlayExecuteEnvironments,
                          baseValues)
        return


    def testOverlayExecuteEnvironments3(self):
        context = ContextModule.Context()
        context.addExecuteEnvironment('foo', 'pomsets.python.PythonEval')
        
        baseValues = {}
        context.overlayExecuteEnvironments(baseValues)

        import pomsets.python
        self.assertTrue(isinstance(baseValues['foo'], pomsets.python.PythonEval))
        return


    def testOverlayCommandBuilders1(self):
        """
        test for legacy
        when the commandBuilders attribute was not yet defined
        """
        context = ContextModule.Context()
        context._executeEnvironments = None

        baseValues = {}
        context.overlayCommandBuilders(baseValues)
        self.assertEquals({}, baseValues)

        return


    def testOverlayCommandBuilders2(self):
        context = ContextModule.Context()
        context.addCommandBuilder('foo', 'bar')
        
        baseValues = {}
        self.assertRaises(NotImplementedError,
                          context.overlayCommandBuilders,
                          baseValues)
        return

    def testOverlayCommandBuilders3(self):
        context = ContextModule.Context()
        context.addExecuteEnvironment('foo', 'pomsets.python.CommandBuilder')
        
        baseValues = {}
        context.overlayExecuteEnvironments(baseValues)

        import pomsets.python
        self.assertTrue(isinstance(baseValues['foo'],
                                   pomsets.python.CommandBuilder))
        return


    # END class TestCase
    pass

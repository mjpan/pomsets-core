from __future__ import with_statement

import os
import sys
import threadpool
import unittest
import logging
import uuid

import currypy

import pomsets.automaton as AutomatonModule
import pypatterns.filter as FilterModule

import pomsets.command as TaskCommandModule
import pomsets.definition as DefinitionModule
import pomsets.parameter as ParameterModule
import pomsets.task as TaskModule


class TestParameterOrder(unittest.TestCase):
    
    def testOrder1(self):
        """
        two parameters with no specified ordering returns 0
        """
        raise NotImplementedError
    
    def testOrder2(self):
        """
        two parameters with ordering specified returns either 1 or -1
        """
        raise NotImplementedError
    
    def testOrder3(self):
        """
        two parameters that both succeed another parameter 
        but have no relations to each other returns 0
        """
        raise NotImplementedError

    def testOrder4(self):
        """
        two parameters that both precede another parameter 
        but have no relations to each other returns 0
        """
        raise NotImplementedError

    def testOrder5(self):
        """
        two parameters that relate to each other transitively
        but have no direct relations to each other returns either 1 or -1
        """
        raise NotImplementedError
    
    # END class TestParameterOrder
    pass


class TestParameterArgument(unittest.TestCase):

    def assertParameterArgument(self, parameter, parameterBindings, expected):
        argument = TaskCommandModule.buildParameterArgument(
            parameter, parameterBindings)
        assert expected == argument, \
               'expected argument to be %s, got %s' % (expected, argument)
        return
    
        
    def testBasic(self):
        """
        tests that the parameter's value gets output
        """

        # create the parameter
        parameterName = 'test'
        parameter = ParameterModule.DataParameter(
            id=parameterName, 
            portDirection=ParameterModule.PORT_DIRECTION_INPUT)
        parameterAttributes = {
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE:True,
                ParameterModule.PORT_ATTRIBUTE_ISINPUTFILE:True,
                }
        ParameterModule.setAttributes(parameter, parameterAttributes)

        # create the parameter bindings
        parameterValue = ['foo']
        parameterBindings = {
            parameterName:parameterValue
        }
                
        # verify the parameter argument
        self.assertParameterArgument(parameter,
                                     parameterBindings,
                                     parameterValue)
        return
    
    def testBasicFlag(self):
        """
        tests that a flag gets output 
        """
        
        # create the parameter
        parameterName = 'test'
        parameter = ParameterModule.DataParameter(
            id=parameterName, 
            portDirection=ParameterModule.PORT_DIRECTION_INPUT)
        prefixFlag = ['-input']
        parameterAttributes = {
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE:True,
                ParameterModule.PORT_ATTRIBUTE_ISINPUTFILE:True,
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE_OPTIONS:{
                    ParameterModule.COMMANDLINE_PREFIX_FLAG:prefixFlag
                    },        
                }
        ParameterModule.setAttributes(parameter, parameterAttributes)

        # create the parameter bindings
        parameterValue = ['foo']
        parameterBindings = {
            parameterName:parameterValue
        }
                
        # verify the parameter argument
        self.assertParameterArgument(parameter,
                                     parameterBindings,
                                     prefixFlag + parameterValue)
        return
    
    
    
    def testBooleanFlagImplicitTrue(self):
        """
        tests for boolean value, implicitly specified by the flag,
        e.g. "-foo" is True and "" is False
        """
        # create the parameter
        parameterName = 'test'
        parameter = ParameterModule.DataParameter(
            id=parameterName, 
            portDirection=ParameterModule.PORT_DIRECTION_INPUT)

        trueValue = ['-foo']
        falseValue = []
        parameterAttributes = {
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE:True,
                ParameterModule.PORT_ATTRIBUTE_ISINPUTFILE:True,
                ParameterModule.PORT_ATTRIBUTE_ISENUM:True,
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE_OPTIONS:{
                    ParameterModule.COMMANDLINE_ENUM_MAP:{
                        True:trueValue,
                        False:falseValue
                        }
                    },
                }
        ParameterModule.setAttributes(parameter, parameterAttributes)

        # create the parameter bindings
        parameterValue = [True]
        parameterBindings = {
            parameterName:parameterValue
        }
                
        # verify the parameter argument
        self.assertParameterArgument(parameter,
                                     parameterBindings,
                                     trueValue)

        return

    
    def testBooleanFlagImplicitTrue(self):
        """
        tests for boolean value, implicitly specified by the flag,
        e.g. "-foo" is True and "" is False
        """
        # create the parameter
        parameterId = uuid.uuid4().hex
        parameterName = 'test'
        parameter = ParameterModule.DataParameter(
            id=parameterId, name=parameterName, 
            portDirection=ParameterModule.PORT_DIRECTION_INPUT)

        trueValue = ['-foo']
        falseValue = []
        parameterAttributes = {
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE:True,
                ParameterModule.PORT_ATTRIBUTE_ISINPUTFILE:True,
                ParameterModule.PORT_ATTRIBUTE_ISENUM:True,
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE_OPTIONS:{
                    ParameterModule.COMMANDLINE_ENUM_MAP:{
                        True:trueValue,
                        False:falseValue
                        }
                    },
                }
        ParameterModule.setAttributes(parameter, parameterAttributes)

        # create the parameter bindings
        parameterValue = [False]
        parameterBindings = {
            parameterId:parameterValue
        }
                
        # verify the parameter argument
        self.assertParameterArgument(parameter,
                                     parameterBindings,
                                     falseValue)

        return
    
    
    
    def testBooleanFlagExplicitTrue(self):
        """
        tests for boolean value, explicity specified by the value
        and not by the flag, e.g.
        "-foo True" and "-foo False"
        """
        # create the parameter
        parameterName = 'test'
        parameter = ParameterModule.DataParameter(
            id=parameterName, 
            portDirection=ParameterModule.PORT_DIRECTION_INPUT)

        prefixFlag = ['-foo']
        trueValue = ['1']
        falseValue = ['0']
        parameterAttributes = {
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE:True,
                ParameterModule.PORT_ATTRIBUTE_ISINPUTFILE:True,
                ParameterModule.PORT_ATTRIBUTE_ISENUM:True,
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE_OPTIONS:{
                    ParameterModule.COMMANDLINE_PREFIX_FLAG:prefixFlag,
                    ParameterModule.COMMANDLINE_ENUM_MAP:{
                        True:trueValue,
                        False:falseValue
                        }
                    },
                }
        ParameterModule.setAttributes(parameter, parameterAttributes)

        # create the parameter bindings
        parameterValue = [True]
        parameterBindings = {
            parameterName:parameterValue
        }
                
        # verify the parameter argument
        self.assertParameterArgument(parameter,
                                     parameterBindings,
                                     prefixFlag + trueValue)
        
        return

    
    def testBooleanFlagExplicitFalse(self):
        """
        tests for boolean value, explicity specified by the value
        and not by the flag, e.g.
        "-foo True" and "-foo False"
        """
        # create the parameter
        parameterName = 'test'
        parameter = ParameterModule.DataParameter(
            id=parameterName, 
            portDirection=ParameterModule.PORT_DIRECTION_INPUT)

        prefixFlag = ['-foo']
        trueValue = ['1']
        falseValue = ['0']
        parameterAttributes = {
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE:True,
                ParameterModule.PORT_ATTRIBUTE_ISINPUTFILE:True,
                ParameterModule.PORT_ATTRIBUTE_ISENUM:True,
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE_OPTIONS:{
                    ParameterModule.COMMANDLINE_PREFIX_FLAG:prefixFlag,
                    ParameterModule.COMMANDLINE_ENUM_MAP:{
                        True:trueValue,
                        False:falseValue
                        }
                    },
                }
        ParameterModule.setAttributes(parameter, parameterAttributes)

        # create the parameter bindings
        parameterValue = [False]
        parameterBindings = {
            parameterName:parameterValue
        }
                
        # verify the parameter argument
        self.assertParameterArgument(parameter,
                                     parameterBindings,
                                     prefixFlag + falseValue)
        
        return
    
    
    def testBasicListValue(self):
        """
        tests to ensure that a list of values is output
        """
        
        # create the parameter
        parameterName = 'test'
        parameter = ParameterModule.DataParameter(
            id=parameterName, 
            portDirection=ParameterModule.PORT_DIRECTION_INPUT)
        parameterAttributes = {
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE:True,
                ParameterModule.PORT_ATTRIBUTE_ISINPUTFILE:True,
                }
        ParameterModule.setAttributes(parameter, parameterAttributes)

        # create the parameter bindings
        parameterValue = ['foo', 'bar']
        parameterBindings = {
            parameterName:parameterValue
        }
                
        # verify the parameter argument
        self.assertParameterArgument(parameter,
                                     parameterBindings,
                                     parameterValue)
        return

    
    def testListValueWithFlag(self):
        """
        tests to ensure that a list of values is output
        along with its prefix flag (non-distributed)
        """
        # create the parameter
        parameterName = 'test'
        parameter = ParameterModule.DataParameter(
            id=parameterName, 
            portDirection=ParameterModule.PORT_DIRECTION_INPUT)
        prefixFlag = ['-input']
        parameterAttributes = {
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE:True,
                ParameterModule.PORT_ATTRIBUTE_ISINPUTFILE:True,
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE_OPTIONS:{
                    ParameterModule.COMMANDLINE_PREFIX_FLAG:prefixFlag
                    },        
                }
        ParameterModule.setAttributes(parameter, parameterAttributes)

        # create the parameter bindings
        parameterValue = ['foo', 'bar']
        parameterBindings = {
            parameterName:parameterValue
        }
                
        # verify the parameter argument
        self.assertParameterArgument(parameter,
                                     parameterBindings,
                                     prefixFlag + parameterValue)
        return


    def testListValueWithFlagDistributed(self):
        """
        tests to ensure that a list of values is output
        along with its prefix flag (distributed)
        """
        # create the parameter
        parameterName = 'test'
        parameter = ParameterModule.DataParameter(
            id=parameterName, 
            portDirection=ParameterModule.PORT_DIRECTION_INPUT)
        prefixFlag = ['-input']
        parameterAttributes = {
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE:True,
                ParameterModule.PORT_ATTRIBUTE_ISINPUTFILE:True,
                ParameterModule.PORT_ATTRIBUTE_ISLIST:True,
                ParameterModule.PORT_ATTRIBUTE_COMMANDLINE_OPTIONS:{
                    ParameterModule.COMMANDLINE_PREFIX_FLAG:prefixFlag,
                    ParameterModule.COMMANDLINE_PREFIX_FLAG_DISTRIBUTE:True
                    },        
                }
        ParameterModule.setAttributes(parameter, parameterAttributes)

        # create the parameter bindings
        parameterValue = ['foo', 'bar']
        parameterBindings = {
            parameterName:parameterValue
        }
                
        # verify the parameter argument
        self.assertParameterArgument(
            parameter,
            parameterBindings,
            reduce(lambda x, y: x+prefixFlag+[y],
                   parameterValue, [])
        )

        return
    
    
    # END class TestParameterArgument
    pass




def main():
    utils.configLogging()

    suite = unittest.TestSuite()
    
    suite.addTest(unittest.makeSuite(TestParameterArgument, 'test'))
    
    runner = unittest.TextTestRunner()
    runner.run(suite)
    return

if __name__=="__main__":
    main()


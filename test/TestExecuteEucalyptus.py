from __future__ import with_statement

import os
import sys
import threadpool
import unittest
import logging

import paramiko
import simplejson as ConfigModule

#import util
#util.setPythonPath()

import cloudpool.shell as ShellModule

import currypy

import pypatterns.filter as FilterModule

import pomsets.command as CommandModule
import pomsets.definition as DefinitionModule
import pomsets.parameter as ParameterModule
import pomsets.task as TaskModule


import TestExecute as BaseModule

from euca2ools import Euca2ool, InstanceValidationError, Util


def loadCredential():

    configFilePath = os.path.join(
        'resources', 'testdata', 'TestExecuteEucalyptus', 'config')

    with open(configFilePath) as f:

	config = ConfigModule.load(f)
	for credential in config['cloud controller credentials']:
	    if not credential['service name'] == 'Eucalyptus':
		continue
	    return credential
	
	raise NotImplementedError(
	    'no credentials found for Eucalyptus in config file %s' % configFilePath
	    )

    raise NotImplementedError(
        'could not read credentials from config file %s' % configFilePath)
    
    
def getShell():
    
    assert os.getenv('EC2_ACCESS_KEY') is not None, 'EC2_ACCESS_KEY must be set'

    shell = ShellModule.SecureShell()
    # set the hostname, user, and keyfile
    
    credential = loadCredential()

    serviceAPI = credential['service API']
    values = credential['values']
    
    userKeyPair = values['user key pair']
    keyfile = values['identity file']
    user = 'root'

    # now to determine the host to run the test
    euca = Euca2ool()
    euca_conn = euca.make_connection()
    reservations = euca_conn.get_all_instances([])
    
    unfilteredInstances = reduce(lambda x, y: x+y.instances, reservations, [])
    instances = filter(
        # filter and return only the instances
        # whose user key matches
        lambda x: x.key_name == userKeyPair,
        # reduce all the instances of all the reservations
        # into a single list
        unfilteredInstances
    )
    if len(instances) is 0:
	raise NotImplementedError('cannot test execution on Eucalytpus as there are no instances matching credentials')
    hostname = instances[0].public_dns_name
    
    
    shell.hostname(hostname)
    shell.user(user)
    shell.keyfile(keyfile)

    return shell


class TestConnection(unittest.TestCase):
    """
    """

    def testConnect(self):

        shell = getShell()
        
        shell.establishConnection()

        shell.disconnect()

        return



    # END TestConnection
    pass


class TestCase1(BaseModule.TestCase1):
    """
    execute of atomic function
    """

    def setUp(self):
        BaseModule.TestCase1.setUp(self)

        # TODO:
        # use boto to start up an aws VM

        self.shell = getShell()
        self.shell.establishConnection()
        return

    def tearDown(self):
        BaseModule.TestCase1.tearDown(self)
        self.shell.disconnect()
        return

    def createExecuteEnvironment(self):
        return self.shell

    # END TestCase1
    pass


class TestCase2(BaseModule.TestCase2):
    """
    execute of atomic function
    """

    def setUp(self):
        BaseModule.TestCase2.setUp(self)

        # TODO:
        # use boto to start up an aws VM

        self.shell = getShell()
        self.shell.establishConnection()
        return

    def tearDown(self):
        BaseModule.TestCase2.tearDown(self)
        self.shell.disconnect()
        return

    def createExecuteEnvironment(self):
        return self.shell
    
    # END class TestCase2
    pass




class TestCase4(BaseModule.TestCase4):
    """
    execute of composite function
    """

    def setUp(self):
        BaseModule.TestCase4.setUp(self)

        # TODO:
        # use boto to start up an aws VM

        self.shell = getShell()
        self.shell.establishConnection()
        return

    def tearDown(self):
        BaseModule.TestCase4.tearDown(self)
        self.shell.disconnect()
        return

    def createExecuteEnvironment(self):
        return self.shell


    # END class TestCase4
    pass



class TestCase8(BaseModule.TestCase8):
    """
    execute of composite function
    """

    def setUp(self):
        BaseModule.TestCase8.setUp(self)

        # TODO:
        # use boto to start up an aws VM

        self.shell = getShell()
        self.shell.establishConnection()
        return

    def tearDown(self):
        BaseModule.TestCase8.tearDown(self)
        self.shell.disconnect()
        return

    def createExecuteEnvironment(self):
        return self.shell

    # END class TestCase8
    pass


class TestCase9(BaseModule.TestCase9):
    """
    execute of composite function
    """

    def setUp(self):
        BaseModule.TestCase9.setUp(self)

        # TODO:
        # use boto to start up an aws VM

        self.shell = getShell()
        self.shell.establishConnection()
        return

    def tearDown(self):
        BaseModule.TestCase9.tearDown(self)
        self.shell.disconnect()
        return

    def createExecuteEnvironment(self):
        return self.shell
    
    # END class TestCase9
    pass


class TestCase10(BaseModule.TestCase10):
    """
    execution fails due to incomplete parameter binding 
    """

    def setUp(self):
        BaseModule.TestCase10.setUp(self)

        # TODO:
        # use boto to start up an aws VM

        self.shell = getShell()
        self.shell.establishConnection()
        return

    def tearDown(self):
        BaseModule.TestCase10.tearDown(self)
        self.shell.disconnect()
        return

    def createExecuteEnvironment(self):
        return self.shell

    # END class TestCase
    pass



class TestParameterSweep1(BaseModule.TestParameterSweep1):

    def setUp(self):
        BaseModule.TestParameterSweep1.setUp(self)

        # TODO:
        # use boto to start up an aws VM

        self.shell = getShell()
        self.shell.establishConnection()
        return

    def tearDown(self):
        BaseModule.TestParameterSweep1.tearDown(self)
        self.shell.disconnect()
        return

    def createExecuteEnvironment(self):
        return self.shell

    # END class TestParameterSweep1
    pass


class TestParameterSweep2(BaseModule.TestParameterSweep2):

    def assertPreExecute(self):
	return
    
    def removeFile(self, file):
        try:
            self.fs.remove(file)
        except IOError:
            pass
        return

    def fileExists(self, file):
        try:
            self.fs.open(file)
        except IOError:
            return False
        return True
    
    
    def setUp(self):

        # TODO:
        # use boto to start up an aws VM

        self.shell = getShell()
        self.shell.establishConnection()
        
        self.fs = self.shell.getFS()
        
        BaseModule.TestParameterSweep2.setUp(self)
        
        return

    def tearDown(self):
        BaseModule.TestParameterSweep2.tearDown(self)
        self.shell.disconnect()
        return

    def createExecuteEnvironment(self):
        return self.shell

    def createDefinition(self):
	definition = BaseModule.TestParameterSweep2.createDefinition(self)
	mapperNode = definition.nodes()[0]
	mapperNode.parameterStagingRequired('input file', True)
	mapperNode.parameterStagingRequired('output file', True)
	return definition
    
    # END class TestParameterSweep2
    pass


class TestParameterSweep3(BaseModule.TestParameterSweep3):

    def assertPreExecute(self):
	return
    
    def removeFile(self, file):
        try:
            self.fs.remove(file)
        except IOError:
            pass
        return
    
    def fileExists(self, file):
        try:
            self.fs.open(file)
        except IOError:
            return False
        return True
    
    
    def setUp(self):

        # TODO:
        # use boto to start up an aws VM

        self.shell = getShell()
        self.shell.establishConnection()
        
        self.fs = self.shell.getFS()
        
        BaseModule.TestParameterSweep3.setUp(self)
        return

    def tearDown(self):
        BaseModule.TestParameterSweep3.tearDown(self)
        self.shell.disconnect()
        return

    def createExecuteEnvironment(self):
        return self.shell

    def createDefinition(self):
	definition = BaseModule.TestParameterSweep3.createDefinition(self)
	mapperNode = definition.nodes()[0]
	mapperNode.parameterStagingRequired('input files', True)
	mapperNode.parameterStagingRequired('output file', True)
	return definition
    
    # END class TestParameterSweep3
    pass


class TestParameterSweep4(BaseModule.TestParameterSweep4):
    """
    tests combining a mapper with a reducer
    """

    def removeFile(self, file):
        try:
            self.fs.remove(file)
        except IOError:
            pass
        return
    
    def fileExists(self, file):
        try:
            self.fs.open(file)
        except IOError:
            return False
        return True
    
    def setUp(self):

        # TODO:
        # use boto to start up an aws VM

        self.shell = getShell()
        self.shell.establishConnection()
        
        self.fs = self.shell.getFS()

        BaseModule.TestParameterSweep4.setUp(self)
        
        return

    def tearDown(self):
        BaseModule.TestParameterSweep4.tearDown(self)
        self.shell.disconnect()
        return

    def createExecuteEnvironment(self):
        return self.shell
    
    # END class TestParameterSweep4
    pass




def main():
    util.configLogging()

    suite = unittest.TestSuite()

    #suite.addTest(unittest.makeSuite(TestConnection, 'test'))
    #suite.addTest(unittest.makeSuite(TestCase2, 'test'))
    #suite.addTest(unittest.makeSuite(TestParameterSweep1, 'test'))
    suite.addTest(unittest.makeSuite(TestParameterSweep2, 'test'))
    suite.addTest(unittest.makeSuite(TestParameterSweep3, 'test'))
    suite.addTest(unittest.makeSuite(TestParameterSweep4, 'test'))

    runner = unittest.TextTestRunner()
    runner.run(suite)
    return

if __name__=="__main__":
    main()


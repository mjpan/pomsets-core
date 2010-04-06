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

import currypy

import cloudpool.shell as ShellModule

import pypatterns.filter as FilterModule

import pomsets.command as CommandModule
import pomsets.definition as DefinitionModule
import pomsets.parameter as ParameterModule
import pomsets.task as TaskModule


import TestExecute as BaseModule


class Credentials(object):
    def loadCredentials(self):

        configFilePath = os.path.join(
            os.getcwd(), 'resources', 'testdata', 'TestExecuteRemote', 'config')

        with open(configFilePath) as f:

            config = ConfigModule.load(f)

            remoteExecuteCredentials = config['remote execute credentials']
            assert len(remoteExecuteCredentials), 'expected to find remote execute credentials'

            return remoteExecuteCredentials

        raise NotImplemented(
            'could not read credentials from config file %s' % configFilePath)


    def getShell(self):

        shell = ShellModule.SecureShell()
        # set the hostname, user, and keyfile

        credentials = self.loadCredentials()

        credential = credentials[0]

        hostname = credential['hostname']
        user = credential['user']
        keyfile = credential['keyfile']

        shell.hostname(hostname)
        shell.user(user)
        shell.keyfile(keyfile)

        return shell

    # ENC class Credentials
    pass

class TestConnection(unittest.TestCase):
    """
    """

    def getCredentialClass(self):
        return Credentials

    def testConnect(self):

        cClass = self.getCredentialClass()
        c = cClass()
        shell = c.getShell()

        shell.establishConnection()

        shell.disconnect()

        return



    # END TestConnection
    pass


class TestCase1(BaseModule.TestCase1):
    """
    execute of atomic function
    """

    def getCredentialClass(self):
        return Credentials

    def setUp(self):
        BaseModule.TestCase1.setUp(self)

        # TODO:
        # use boto to start up an aws VM

        cClass = self.getCredentialClass()
        c = cClass()

        self.shell = c.getShell()
        self.shell.establishConnection()
        return

    def tearDown(self):
        BaseModule.TestCase1.tearDown(self)
        self.shell.disconnect()
        return

    def createExecuteEnvironmentMap(self):
        return {
            'shell process':self.shell
            }

    # END TestCase1
    pass


class TestCase2(BaseModule.TestCase2):
    """
    execute of atomic function
    """

    def getCredentialClass(self):
        return Credentials

    def setUp(self):
        BaseModule.TestCase2.setUp(self)

        cClass = self.getCredentialClass()
        c = cClass()

        self.shell = c.getShell()
        self.shell.establishConnection()
        return

    def tearDown(self):
        BaseModule.TestCase2.tearDown(self)
        self.shell.disconnect()
        return

    def createExecuteEnvironmentMap(self):
        return {
            'shell process':self.shell
            }

    # END class TestCase2
    pass




class TestCase4(BaseModule.TestCase4):
    """
    execute of composite function
    """

    def getCredentialClass(self):
        return Credentials

    def setUp(self):
        BaseModule.TestCase4.setUp(self)

        cClass = self.getCredentialClass()
        c = cClass()

        self.shell = c.getShell()
        self.shell.establishConnection()
        return

    def tearDown(self):
        BaseModule.TestCase4.tearDown(self)
        self.shell.disconnect()
        return

    def createExecuteEnvironmentMap(self):
        return {
            'shell process':self.shell
            }


    # END class TestCase4
    pass



class TestCase8(BaseModule.TestCase8):
    """
    execute of composite function
    """

    def getCredentialClass(self):
        return Credentials

    def setUp(self):
        BaseModule.TestCase8.setUp(self)

        cClass = self.getCredentialClass()
        c = cClass()

        self.shell = c.getShell()
        self.shell.establishConnection()
        return

    def tearDown(self):
        BaseModule.TestCase8.tearDown(self)
        self.shell.disconnect()
        return

    def createExecuteEnvironmentMap(self):
        return {
            'shell process':self.shell
            }

    # END class TestCase8
    pass


class TestCase9(BaseModule.TestCase9):
    """
    execute of composite function
    """

    def getCredentialClass(self):
        return Credentials

    def setUp(self):
        BaseModule.TestCase9.setUp(self)

        cClass = self.getCredentialClass()
        c = cClass()

        self.shell = c.getShell()
        self.shell.establishConnection()
        return

    def tearDown(self):
        BaseModule.TestCase9.tearDown(self)
        self.shell.disconnect()
        return

    def createExecuteEnvironmentMap(self):
        return {
            'shell process':self.shell
            }

    # END class TestCase9
    pass


class TestCase10(BaseModule.TestCase10):
    """
    execution fails due to incomplete parameter binding 
    """

    def getCredentialClass(self):
        return Credentials

    def setUp(self):
        BaseModule.TestCase10.setUp(self)

        cClass = self.getCredentialClass()
        c = cClass()

        self.shell = c.getShell()
        self.shell.establishConnection()
        return

    def tearDown(self):
        BaseModule.TestCase10.tearDown(self)
        self.shell.disconnect()
        return

    def createExecuteEnvironmentMap(self):
        return {
            'shell process':self.shell
            }

    # END class TestCase
    pass



class TestParameterSweep1(BaseModule.TestParameterSweep1):

    def getCredentialClass(self):
        return Credentials

    def setUp(self):
        BaseModule.TestParameterSweep1.setUp(self)

        cClass = self.getCredentialClass()
        c = cClass()

        self.shell = c.getShell()
        self.shell.establishConnection()
        return

    def tearDown(self):
        BaseModule.TestParameterSweep1.tearDown(self)
        self.shell.disconnect()
        return

    def createExecuteEnvironmentMap(self):
        return {
            'shell process':self.shell
            }

    # END class TestParameterSweep1
    pass


class TestParameterSweep2(BaseModule.TestParameterSweep2):

    BASE_DIR = os.path.sep + os.path.join('tmp', 'TestExecuteRemote', 'TestParameterSweep2')

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


    def getCredentialClass(self):
        return Credentials

    def setUp(self):

        cClass = self.getCredentialClass()
        c = cClass()

        self.shell = c.getShell()
        self.shell.establishConnection()

        self.fs = self.shell.getFS()	

        BaseModule.TestParameterSweep2.setUp(self)

        self.stageFiles()
        return


    def stageFiles(self):
        pathParts = TestParameterSweep2.BASE_DIR.split(os.path.sep)[2:]
        # assumes /tmp is already created
        currentPath = os.path.sep + 'tmp'
        for part in pathParts:
            currentPath = os.path.join(currentPath, part)
            if self.fileExists(currentPath):
                continue
            self.fs.mkdir(currentPath)

        inputFilesRemote = [
            os.path.join(x, y)
            for x,y in zip(
                [TestParameterSweep2.BASE_DIR]*len(BaseModule.TestParameterSweep2.INPUT_FILES), 
                BaseModule.TestParameterSweep2.INPUT_FILES)
        ]
        for localFile, remoteFile in zip(self.inputFiles, inputFilesRemote):
            self.fs.put(localFile, remoteFile)
        self.inputFiles = inputFilesRemote

        return

    def removeFiles(self):

        # this only removed the input files
        BaseModule.TestParameterSweep2.removeFiles(self)

        for inputFile in self.inputFiles:
            self.removeFile(inputFile)
            pass

        # path to not remove
        # assumes /tmp was already created
        pathToKeep = os.path.sep + 'tmp'
        currentPath = TestParameterSweep2.BASE_DIR
        while len(currentPath) and not currentPath == pathToKeep:
            self.fs.rmdir(currentPath)
            currentPath = currentPath[:currentPath.rfind(os.path.sep)]

        return

    def tearDown(self):
        BaseModule.TestParameterSweep2.tearDown(self)
        self.shell.disconnect()
        return

    def createExecuteEnvironmentMap(self):
        return {
            'shell process':self.shell
            }

    # END class TestParameterSweep2
    pass


class TestParameterSweep3(BaseModule.TestParameterSweep3):

    BASE_DIR = os.path.sep + os.path.join('tmp', 'TestExecuteRemote', 'TestParameterSweep3')

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


    def getCredentialClass(self):
        return Credentials

    def setUp(self):

        cClass = self.getCredentialClass()
        c = cClass()

        self.shell = c.getShell()
        self.shell.establishConnection()

        self.fs = self.shell.getFS()

        BaseModule.TestParameterSweep3.setUp(self)

        self.stageFiles()
        return

    def stageFiles(self):
        pathParts = TestParameterSweep3.BASE_DIR.split(os.path.sep)[2:]
        # assumes /tmp is already created
        currentPath = os.path.sep + 'tmp'
        for part in pathParts:
            currentPath = os.path.join(currentPath, part)
            self.fs.mkdir(currentPath)

        inputFilesRemote = [
            os.path.join(x, y) 
            for x,y in zip(
                [TestParameterSweep3.BASE_DIR]*len(BaseModule.TestParameterSweep3.INPUT_FILES), 
                BaseModule.TestParameterSweep3.INPUT_FILES)
        ]

        for localFile, remoteFile in zip(self.inputFiles, inputFilesRemote):
            self.fs.put(localFile, remoteFile)
        self.inputFiles = inputFilesRemote


        return

    def removeFiles(self):

        # this only removed the output files
        BaseModule.TestParameterSweep3.removeFiles(self)

        for inputFile in self.inputFiles:
            self.removeFile(inputFile)
            pass

        # path to not remove
        # assumes /tmp was already created
        pathToKeep = os.path.sep + 'tmp'
        currentPath = TestParameterSweep3.BASE_DIR
        while len(currentPath) and not currentPath == pathToKeep:
            self.fs.rmdir(currentPath)
            currentPath = currentPath[:currentPath.rfind(os.path.sep)]

        return

    def tearDown(self):
        BaseModule.TestParameterSweep3.tearDown(self)
        self.shell.disconnect()
        return

    def createExecuteEnvironmentMap(self):
        return {
            'shell process':self.shell
            }

    # END class TestParameterSweep3
    pass


class TestParameterSweep4(BaseModule.TestParameterSweep4):
    """
    tests combining a mapper with a reducer
    """
    BASE_DIR = os.path.sep + os.path.join('tmp', 'TestExecuteRemote', 'TestParameterSweep4')

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

    def getCredentialClass(self):
        return Credentials

    def setUp(self):

        cClass = self.getCredentialClass()
        c = cClass()

        self.shell = c.getShell()
        self.shell.establishConnection()

        self.fs = self.shell.getFS()

        BaseModule.TestParameterSweep4.setUp(self)
        self.stageFiles()
        return


    def stageFiles(self):
        pathParts = TestParameterSweep4.BASE_DIR.split(os.path.sep)[2:]
        # assumes /tmp is already created
        currentPath = os.path.sep + 'tmp'
        for part in pathParts:
            currentPath = os.path.join(currentPath, part)
            self.fs.mkdir(currentPath)

        inputFilesRemote = [
            os.path.join(x, y)
            for x,y in zip(
                [TestParameterSweep4.BASE_DIR]*len(BaseModule.TestParameterSweep4.INPUT_FILES), 
                BaseModule.TestParameterSweep4.INPUT_FILES)
        ]
        self.intermediateFiles = [
            os.path.join(x, y)
            for x,y in zip([TestParameterSweep4.TEST_DIR]*len(TestParameterSweep4.INTERMEDIATE_FILES), 
                           TestParameterSweep4.INTERMEDIATE_FILES)
        ]


        for localFile, remoteFile in zip(self.inputFiles, inputFilesRemote):
            self.fs.put(localFile, remoteFile)
        self.inputFiles = inputFilesRemote


        return

    def removeFiles(self):

        # this only removed the output files
        BaseModule.TestParameterSweep4.removeFiles(self)

        for inputFile in self.inputFiles:
            self.removeFile(inputFile)
            pass

        # path to not remove
        # assumes /tmp was already created
        pathToKeep = os.path.sep + 'tmp'
        currentPath = TestParameterSweep4.BASE_DIR
        while len(currentPath) and not currentPath == pathToKeep:
            self.fs.rmdir(currentPath)
            currentPath = currentPath[:currentPath.rfind(os.path.sep)]

        return

    def tearDown(self):
        BaseModule.TestParameterSweep4.tearDown(self)
        self.shell.disconnect()
        return



    def createExecuteEnvironmentMap(self):
        return {
            'shell process':self.shell
            }

    # END class TestParameterSweep4
    pass




def main():
    util.configLogging()

    suite = unittest.TestSuite()


    suite.addTest(unittest.makeSuite(TestConnection, 'test'))
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


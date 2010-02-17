from __future__ import with_statement

import os
import sys
import threadpool
import unittest
import logging

import paramiko
import simplejson as ConfigModule

import cloudpool.shell as ShellModule

import currypy

import pypatterns.filter as FilterModule

import pomsets.command as CommandModule
import pomsets.definition as DefinitionModule
import pomsets.parameter as ParameterModule
import pomsets.task as TaskModule


import TestExecuteRemote as BaseModule

from euca2ools import Euca2ool, InstanceValidationError, Util

BASE_DIR = os.getcwd()

class Credentials(object):
    def loadCredential(self):

        configFilePath = os.path.join(
            BASE_DIR, 'resources', 'testdata', 'TestExecuteEucalyptus', 'config')

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


    def getShell(self):

        shell = ShellModule.SecureShell()
        # set the hostname, user, and keyfile

        credential = self.loadCredential()

        serviceAPI = credential['service API']
        values = credential['values']

        userKeyPair = values['user key pair']
        keyfile = values['identity file']
        accessKey = values['access key']
        secretKey = values['secret key']
        url = values['url']
        user = 'root'

        # now to determine the host to run the test
        euca = Euca2ool()

        # set the values for euca2ools
        euca.ec2_url = url
        euca.ec2_user_access_key = accessKey
        euca.ec2_user_secret_key = secretKey

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

    # END class Credentials
    pass



class TestConnection(BaseModule.TestConnection):
    """
    subclasses from TestConnection
    the only difference is the credentials class
    which uses the EC2 credentials to determine a host
    """
    def getCredentialClass(self):
        return Credentials

    # END TestConnection
    pass


class TestCase1(BaseModule.TestCase1):
    """
    subclasses from TestCase1
    the only difference is the credentials class
    which uses the EC2 credentials to determine a host

    execute of atomic function
    """
    def getCredentialClass(self):
        return Credentials

    # END TestCase1
    pass


class TestCase2(BaseModule.TestCase2):
    """
    subclasses from the same in BaseModule
    the only difference is the credentials class
    which uses the EC2 credentials to determine a host

    execute of atomic function
    """
    def getCredentialClass(self):
        return Credentials

    # END class TestCase2
    pass




class TestCase4(BaseModule.TestCase4):
    """
    subclasses from the same in BaseModule
    the only difference is the credentials class
    which uses the EC2 credentials to determine a host

    execute of composite function
    """
    def getCredentialClass(self):
        return Credentials

    # END class TestCase4
    pass



class TestCase8(BaseModule.TestCase8):
    """
    subclasses from the same in BaseModule
    the only difference is the credentials class
    which uses the EC2 credentials to determine a host

    execute of composite function
    """
    def getCredentialClass(self):
        return Credentials

    # END class TestCase8
    pass


class TestCase9(BaseModule.TestCase9):
    """
    subclasses from the same in BaseModule
    the only difference is the credentials class
    which uses the EC2 credentials to determine a host

    execute of composite function
    """
    def getCredentialClass(self):
        return Credentials

    # END class TestCase9
    pass


class TestCase10(BaseModule.TestCase10):
    """
    subclasses from the same in BaseModule
    the only difference is the credentials class
    which uses the EC2 credentials to determine a host

    execution fails due to incomplete parameter binding 
    """
    def getCredentialClass(self):
        return Credentials

    # END class TestCase
    pass



class TestParameterSweep1(BaseModule.TestParameterSweep1):
    """
    subclasses from the same in BaseModule
    the only difference is the credentials class
    which uses the EC2 credentials to determine a host

    """
    def getCredentialClass(self):
        return Credentials

    # END class TestParameterSweep1
    pass


class TestParameterSweep2(BaseModule.TestParameterSweep2):
    """
    subclasses from the same in BaseModule
    the only difference is the credentials class
    which uses the EC2 credentials to determine a host

    """
    def getCredentialClass(self):
        return Credentials

    # END class TestParameterSweep2
    pass


class TestParameterSweep3(BaseModule.TestParameterSweep3):
    """
    subclasses from the same in BaseModule
    the only difference is the credentials class
    which uses the EC2 credentials to determine a host

    """
    def getCredentialClass(self):
        return Credentials

    # END class TestParameterSweep3
    pass


class TestParameterSweep4(BaseModule.TestParameterSweep4):
    """
    subclasses from the same in BaseModule
    the only difference is the credentials class
    which uses the EC2 credentials to determine a host

    tests combining a mapper with a reducer
    """
    def getCredentialClass(self):
        return Credentials

    # END class TestParameterSweep4
    pass




def main():
    utils.configLogging()

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


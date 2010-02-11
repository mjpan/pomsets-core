"""
import unittest

import TestExecuteHadoop


def additional_tests():
    suite = unittest.TestSuite()

    suite.addTest(unittest.makeSuite(TestExecuteHadoop.TestHadoop1, 'test'))
    suite.addTest(unittest.makeSuite(TestExecuteHadoop.TestHadoopStreaming1, 'test'))
    return suite

"""

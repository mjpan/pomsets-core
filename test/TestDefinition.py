import os
import unittest

import pomsets.builder as BuilderModule
import pomsets.context as ContextModule
import pomsets.definition as DefinitionModule



def createTestPomset1(builder):

    compositeContext = builder.createNewNestPomset(name='composite')
    compositeDefinition = compositeContext.pomset()

    atomicContext = builder.createNewAtomicPomset(name='atomic')
    atomicDefinition = atomicContext.pomset()

    builder.addPomsetParameter(
        atomicDefinition, 'input file', {'direction':'input'})
    builder.addPomsetParameter(
        atomicDefinition, 'output file', {'direction':'input'})

    # setup the reference definition for parameter sweep
    mapperNode = compositeDefinition.createNode(id='node')
    mapperNode.definitionToReference(atomicDefinition)
    mapperNode.isCritical(True)
    mapperNode.name('mapper')

    compositeDefinition.name('basic map-reduce')
    
    return compositeDefinition



class TestDefinition1(unittest.TestCase):

    def setUp(self):

        self.builder = BuilderModule.Builder()

        pomset = createTestPomset1(self.builder)
        self.pomset = pomset

        return


    def tearDown(self):
        return


    def testGetParameter1(self):
        pomset = self.pomset
        self.assertRaises(NotImplementedError,
                          pomset.getParameter, 'foo')
        return


    def testGetParameter2(self):
        pomset = self.pomset

        self.builder.addPomsetParameter(
            pomset, 'foo', 
            {'direction':'input'})
        self.builder.addPomsetParameter(
            pomset, 'foo', 
            {'direction':'input'})

        self.assertRaises(NotImplementedError,
                          pomset.getParameter, 'foo')

        return


    def testGetParameter3(self):
        pomset = self.pomset

        self.builder.addPomsetParameter(
            pomset, 'foo', 
            {'direction':'input'})


        return


    def testParameterIsInOwnParameterSweepGroup(self):
        
        pomset = self.pomset

        nodes = [x for x in pomset.nodes() if x.id() == 'node']
        node = nodes[0]

        for parameterId in ['input file', 'output file']:
            self.assertTrue(
                node.parameterIsInOwnParameterSweepGroup(parameterId))
            node.isParameterSweep(parameterId, value=True)

        node.addParameterSweepGroup(['input file', 'output file'])
        for parameterId in ['input file', 'output file']:
            self.assertFalse(
                node.parameterIsInOwnParameterSweepGroup(parameterId))

        return


    def testGetParameterSweepGroup(self):

        pomset = self.pomset

        nodes = [x for x in pomset.nodes() if x.id() == 'node']
        node = nodes[0]

        for parameterId in ['input file', 'output file']:
            self.assertTrue(
                node.parameterIsInOwnParameterSweepGroup(parameterId))
            #self.assertEquals(node.getGroupForParameterSweep(parameterId),
            #                  tuple([parameterId]))

        for parameterId in ['input file', 'output file']:
            node.isParameterSweep(parameterId, value=True)

        node.addParameterSweepGroup(['input file', 'output file'])

        self.assertEquals(node.getGroupForParameterSweep('input file'),
                          node.getGroupForParameterSweep('output file'))

        return

    def testRemoveFromParameterSweepGroup(self):

        pomset = self.pomset

        nodes = [x for x in pomset.nodes() if x.id() == 'node']
        node = nodes[0]

        for parameterId in ['input file', 'output file']:
            node.isParameterSweep(parameterId, value=True)

        node.addParameterSweepGroup(['input file', 'output file'])

        for parameterId in ['input file', 'output file']:
            self.assertFalse(
                node.parameterIsInOwnParameterSweepGroup(parameterId))

        group = node.getGroupForParameterSweep('input file')
        node.removeParameterSweepGroup(group)

        for parameterId in ['input file', 'output file']:
            self.assertTrue(
                node.parameterIsInOwnParameterSweepGroup(parameterId))

        self.assertEquals(0,
                          node.parameterSweepGroups().rowCount())

        return

    def testAddToParameterSweepGroup(self):

        pomset = self.pomset

        nodes = [x for x in pomset.nodes() if x.id() == 'node']
        node = nodes[0]

        self.assertRaises(
            NotImplementedError,
            node.addParameterSweepGroup,
            ['input file', 'output file'])

        for parameterId in ['input file', 'output file']:
            self.assertTrue(
                node.parameterIsInOwnParameterSweepGroup(parameterId))

        for parameterId in ['input file', 'output file']:
            node.isParameterSweep(parameterId, value=True)

        node.addParameterSweepGroup(['input file', 'output file'])

        self.assertEquals(2,
                          node.parameterSweepGroups().rowCount())
        
        for parameterId in ['input file', 'output file']:
            self.assertFalse(
                node.parameterIsInOwnParameterSweepGroup(parameterId))
        self.assertEquals(node.getGroupForParameterSweep('input file'),
                          node.getGroupForParameterSweep('output file'))


        return


    # END class TestDefinition1
    pass


class TestDefinition2(unittest.TestCase):

    TEST_DATA_PATH = os.path.sep.join(['resources', 'testdata', 'TestDefinition', 'render.pomset'])

    def setUp(self):

        # load pomset from path
        pomsetContext = ContextModule.loadPomset(path=TestDefinition2.TEST_DATA_PATH)
        self.pomset = pomsetContext.pomset()
        return


    def assertPredecessors(self, node, expectedPredecessorNames):

        predecessors = [x for x in node.predecessors()]

        self.assertEquals(len(expectedPredecessorNames),
                          len(predecessors))

        actualPredecessorNames = [x.name() for x in predecessors]

        for expectedName in expectedPredecessorNames:
            self.assertTrue(expectedName in actualPredecessorNames)
        return


    def assertSuccessors(self, node, expectedSuccessorNames):
        successors = [x for x in node.successors()]

        self.assertEquals(len(expectedSuccessorNames),
                          len(successors))

        actualSuccessorNames = [x.name() for x in successors]

        for expectedName in expectedSuccessorNames:
            self.assertTrue(expectedName in actualSuccessorNames)
        return


    def testOrdering(self):

        nodes = [x for x in self.pomset.nodes() 
                 if x.name() == 'generate tiling info']
        self.assertTrue(len(nodes) is 1)
        node = nodes[0]
        self.assertPredecessors(node, [])
        self.assertSuccessors(node, ['generate tile list'])

        nodes = [x for x in self.pomset.nodes() 
                 if x.name() == 'generate tile list']
        self.assertTrue(len(nodes) is 1)
        node = nodes[0]
        self.assertPredecessors(node, ['generate tiling info'])
        self.assertSuccessors(node, ['read tile image list',
                                     'read tile index list'])

        nodes = [x for x in self.pomset.nodes() 
                 if x.name() == 'read tile index list']
        self.assertTrue(len(nodes) is 1)
        node = nodes[0]
        self.assertPredecessors(node, ['generate tile list'])
        self.assertSuccessors(node, ['render tile'])

        nodes = [x for x in self.pomset.nodes() 
                 if x.name() == 'read tile image list']
        self.assertTrue(len(nodes) is 1)
        node = nodes[0]
        self.assertPredecessors(node, ['generate tile list'])
        self.assertSuccessors(node, ['render tile'])

        nodes = [x for x in self.pomset.nodes() 
                 if x.name() == 'render tile']
        self.assertTrue(len(nodes) is 1)
        node = nodes[0]
        self.assertPredecessors(node, ['read tile index list',
                                       'read tile image list'])
        self.assertSuccessors(node, ['comp tiles'])


        nodes = [x for x in self.pomset.nodes() 
                 if x.name() == 'comp tiles']
        self.assertTrue(len(nodes) is 1)
        node = nodes[0]
        self.assertPredecessors(node, ['render tile'])
        self.assertSuccessors(node, [])

        return


    # END class TestDefinition2
    pass

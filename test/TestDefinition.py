import unittest

import pomsets.builder as BuilderModule
import pomsets.definition as DefinitionModule



def createTestPomset():

    builder = BuilderModule.Builder()

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



class TestDefinition(unittest.TestCase):

    def setUp(self):

        pomset = createTestPomset()
        self.pomset = pomset

        return


    def tearDown(self):
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


    # END class TestDefinition
    pass

import os
import sys
import unittest
import logging


import pomsets.error as ErrorModule
import pomsets.graph as GraphModule

class TestCase(unittest.TestCase):

    def createGroupedGraph1(self):
        nodeMap = {}
        
        id =0
        aGraph = GraphModule.Graph(id=id)
        nodeMap[id] = aGraph

        for id in [1,2,3,9,10]:
            node = aGraph.createNode(id=id)
            nodeMap[id] = node
            pass

        id=11
        aGraph = aGraph.createGraph(id=id)
        nodeMap[id] = aGraph

        for id in [4,5,6,7,8]:
            node = aGraph.createNode(id=id)
            nodeMap[id] = node
            pass

        edgesInfo = [
            (0, 1, 11),
            (0, 2, 11),
            (0, 3, 11),
            (0, 11, 9),
            (0, 11, 10),
            (11, 4, 5),
            (11, 4, 6),
            (11, 5, 8),
            (11, 6, 8),
            ]

        for graphId, source, target in edgesInfo:
            nodes = [nodeMap[source], nodeMap[target]]
            nodeMap[graphId].createEdge(nodes)
            pass

        return nodeMap[0], nodeMap

    def createUngroupedGraph1(self):
        def idGenerator():
            id = -1
            while 1:
                id = id + 1
                yield id
                pass
            pass

        idFunction = idGenerator().next

        nodeMap = {}
        createFunction = GraphModule.Graph
        for idx in range(1,12):
            id = idFunction()
            if idx is 2:
                createFunction = nodeMap[0].createNode

            node = createFunction(id=id)
            nodeMap[id] = node
            pass

        edgesInfo = [
            (0,1,4),
            (0,1,7),
            (0,2,4),
            (0,3,4),
            (0,4,5),
            (0,4,6),
            (0,5,8),
            (0,6,8),
            (0,8,9),
            (0,8,10)
            ]
        for graphId, source, target in edgesInfo:
            nodes = [nodeMap[source], nodeMap[target]]
            nodeMap[graphId].createEdge(nodes)
            pass

        return nodeMap[0], nodeMap

    def createUngroupedGraph2(self):
        def idGenerator():
            id = -1
            while 1:
                id = id + 1
                yield id
                pass
            pass

        idFunction = idGenerator().next

        nodeMap = {}
        createFunction = GraphModule.Graph
        for idx in range(1,12):
            id = idFunction()
            if idx is 2:
                createFunction = nodeMap[0].createNode

            node = createFunction(id=id)
            nodeMap[id] = node
            pass

        edgesInfo = [
            (0,1,4),
            (0,1,7),
            (0,2,4),
            (0,2,7),
            (0,3,4),
            (0,3,7),
            (0,4,5),
            (0,4,6),
            (0,5,8),
            (0,6,8),
            (0,7,9),
            (0,7,10),
            (0,8,9),
            (0,8,10),
            ]
        for graphId, source, target in edgesInfo:
            nodes = [nodeMap[source], nodeMap[target]]
            nodeMap[graphId].createEdge(nodes)
            pass

        return nodeMap[0], nodeMap


    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testDifferentGraph(self):
        expectedGraph, expectedNodeMap = self.createUngroupedGraph2()
        badGraph, badNodeMap = self.createUngroupedGraph1()
        try:
            GraphModule.assertGraphAutomorphism(expectedGraph, badGraph,
                                                lambda x: x.nodes()[0])
            raise ErrorModule.InvalidValueError("expected to fail")
        except AssertionError:
            pass
        return
    
    
    # TODO:
    # will need to fix this test
    def _testUngroupNode(self):
        expectedGraph, expectedNodeMap = self.createUngroupedGraph2()
        actualGraph, actualNodeMap = self.createGroupedGraph1()
        badGraph, badNodeMap = self.createUngroupedGraph1()

        actualGraph.ungroupNode(actualNodeMap[11])

        logging.debug("testUngroupNode expectedNodes >> %s" % expectedGraph.nodes())
        logging.debug("testUngroupNode actualNodes >> %s" % actualGraph.nodes())

        logging.debug("testUngroupNode expectedEdges >> %s" % expectedGraph.edges())
        logging.debug("testUngroupNode actualNodes >> %s" % actualGraph.edges())

        GraphModule.assertGraphAutomorphism(expectedGraph, actualGraph,
                                            lambda x: x.nodes()[0])

        pass

    
    # TODO:
    # will need to fix this test
    def _testGroupNodes(self):
        
        actualGraph, actualNodeMap = self.createUngroupedGraph1()
        expectedGraph, expectedNodeMap = self.createGroupedGraph1()

        nodesToGroup = []
        for id in [4,5,6,7,8]:
            node = actualNodeMap[id]
            nodesToGroup.append(node)
            logging.debug("appended node %s for id %s" % (node.id(), id))
        group = actualGraph.groupNodes(nodesToGroup)
        group.id(11)

        GraphModule.assertGraphAutomorphism(expectedGraph, actualGraph,
                                            lambda x: x.nodes()[-1])
        pass

    
    def testEdgeFilters(self):
        theGraph = GraphModule.Graph(id='graph')
        theNode1 = theGraph.createNode(id='node1')
        theNode2 = theGraph.createNode(id='node2')
        theEdge = theGraph.createEdge([theNode1, theNode2])

        assert theNode1.outgoingEdgeFilter().matches(theEdge)
        assert not theNode1.incomingEdgeFilter().matches(theEdge)
        
        theEdges = theNode1.getEdges()
        assert theEdge in theEdges
        
        assert theNode2.incomingEdgeFilter().matches(theEdge)
        assert not theNode2.outgoingEdgeFilter().matches(theEdge)
        
        theEdges = theNode2.getEdges()
        assert theEdge in theEdges
        
        return
    
    # END class TestCase


def main():
    util.configLogging()
    suite = unittest.makeSuite(TestCase,'test')
    runner = unittest.TextTestRunner()
    runner.run(suite)
    return

if __name__=="__main__":
    main()


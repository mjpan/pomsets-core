import copy
import currypy
import logging

import pomsets.resource as ResourceModule
import pypatterns.filter as FilterModule
import pomsets.error as ErrorModule

"""
assumptions: directed acyclic graph
"""

class GraphObject (ResourceModule.ResourceReference, ResourceModule.Struct):

    ATTRIBUTES = [
        'graph'
        ]

    def __init__(self, id=None, graph=None):
        ResourceModule.ResourceReference.__init__(self, id=id)
        ResourceModule.Struct.__init__(self)
        self.graph(graph)
        pass

    def __ne__(self, other):
        """
        implements != for all the subclasses
        """
        result = self.__eq__(other)
        if result is NotImplemented:
            return result
        return not result        

    def __deepcopy__(self, memo):
        if self in memo:
            return memo[self]
        
        copyId = self.id()
        graph = self.graph()
        copyGraph = memo.get(graph, graph)
        result = self.__class__()
        result.id(copyId)
        result.graph(copyGraph)
        copyResource = copy.deepcopy(self.resource(), memo)
        result.resource(copyResource)
        
        memo[self] = result
        return result
    
    
    # END class GraphObject
    pass

class Node (GraphObject):

    ATTRIBUTES = GraphObject.ATTRIBUTES + []

    
    @staticmethod
    def staticIsContainer(object):
        return False
    
    def __init__(self, id=None, graph=None):
        GraphObject.__init__(self, id=id, graph=graph)
        pass

    
    def __repr__(self):
        return "%s" % self.id()

    
    def __eq__(self, other):
        for difference in self.getDifferencesWith(other):
            return False
        return True

    def getDifferencesWith(self, other):

        if not isinstance(other, self.__class__):
            yield "self is of class %s, other is of class %s" % (self.__class__, other.__class__)

        #if not self.getKeyForEquivalenceTesting() == other.getKeyForEquivalenceTesting():
        #    yield "self has equivalence key %s, other has %s" % (self.getKeyForEquivalenceTesting(),
        #                                                         other.getKeyForEquivalenceTesting())
            
        selfIncomingEdges = self.getEdges(self.incomingEdgeFilter())
        otherIncomingEdges = other.getEdges(other.incomingEdgeFilter())
        if not len(selfIncomingEdges) == len(otherIncomingEdges):
            yield 'self has %s incoming edges, other has %s' % (len(selfIncomingEdges),
                                                                len(otherIncomingEdges))
        else:
            for selfEdge, otherEdge in zip(sorted(selfIncomingEdges, Edge.nameComparator),
                                           sorted(otherIncomingEdges, Edge.nameComparator)):
                selfEdgeName = selfEdge.name()
                otherEdgeName = otherEdge.name()
                if not selfEdgeName == otherEdgeName:
                    yield 'self has incoming edge %s, other has %s' % (selfEdgeName, otherEdgeName)
                pass
        
        selfOutgoingEdges = self.getEdges(self.outgoingEdgeFilter())
        otherOutgoingEdges = other.getEdges(other.outgoingEdgeFilter())

        if not len(selfOutgoingEdges) == len(otherOutgoingEdges):
            yield 'self has %s outgoing edges, other has %s' % (len(selfOutgoingEdges),
                                                                len(otherOutgoingEdges))
        else:
            for selfEdge, otherEdge in zip(sorted(selfOutgoingEdges, Edge.nameComparator),
                                           sorted(otherOutgoingEdges, Edge.nameComparator)):
                selfEdgeName = selfEdge.name()
                otherEdgeName = otherEdge.name()
                if not selfEdgeName == otherEdgeName:
                    yield 'self has outgoing edge %s, other has %s' % (selfEdgeName, otherEdgeName)
                pass
                                                                 
        raise StopIteration

    def isContainer(self):
        return self.__class__.staticIsContainer(self)

    def getKeyForEquivalenceTesting(self):
        return self.id()

    def getEdgeFilter(self):
        theFilter = FilterModule.constructOrFilter()
        theFilter.addFilter(self.incomingEdgeFilter())
        theFilter.addFilter(self.outgoingEdgeFilter())
        return theFilter
    
    
    def numEdges(self, filter=None):
        edges = [x for x in self.getEdges(filter)]
        return len(edges)

    def getEdges(self, aFilter=None):
        
        if aFilter is None:
            aFilter = self.getEdgeFilter()

        return [x for x in aFilter.wrap(self.graph().edges())]

    
    def incomingEdgeFilter(self):
        """
        this can be different than the incoming edge filter
        because there may be other edges (eg from parent to child)
        """
        return FilterModule.ObjectKeyMatchesFilter(
            filter=FilterModule.IdentityFilter(self),
            keyFunction=lambda x: x.convertEntityToNode(x.entities()[1]))
            
        

    def outgoingEdgeFilter(self):
        """
        this can be different than the outgoing edge filter
        because there may be other edges (eg from child to parent)
        """
        return FilterModule.ObjectKeyMatchesFilter(
            filter=FilterModule.IdentityFilter(self),
            keyFunction=lambda x: x.convertEntityToNode(x.entities()[0]))
        

    def predecessors(self):
        theFilter = self.incomingEdgeFilter()
        for edge in self.getEdges(theFilter):
            source = edge.source()
            sourceNode = edge.convertEntityToNode(source)
            yield sourceNode

    def successors(self):
        theFilter = self.outgoingEdgeFilter()
        for edge in self.getEdges(theFilter):
            sink = edge.sink()
            sinkNode = edge.convertEntityToNode(sink)
            yield sinkNode

    def clone(self):
        return Node(id=self.id(), graph=self.graph())

    
    # END class Node
    pass

class Graph (Node):

    ATTRIBUTES = Node.ATTRIBUTES + [
        'nodes',
        'edges'
        ]

    GROUP_EDGES_INCOMING = 1
    GROUP_EDGES_OUTGOING = 2
    GROUP_EDGES_INTERNAL = 3

    FUNCTION_ZERO = currypy.Curry(FilterModule.FUNCTION_IDENTITY,
                                      object=0)
    
    FILTER_NODE_MINIMAL = FilterModule.ObjectFilter(
        valueFunction = FUNCTION_ZERO,
        objectFunction = lambda x: x.numEdges(x.incomingEdgeFilter())
    )

    FILTER_NODE_MAXIMAL = FilterModule.ObjectFilter(
        valueFunction = FUNCTION_ZERO,
        objectFunction = lambda x: x.numEdges(x.outgoingEdgeFilter())
        )

    @staticmethod
    def staticIsContainer(anObject):
        return isinstance(anObject, Graph)

    @staticmethod
    def extractContainer(anObject):
        return anObject


    @staticmethod
    def staticHeadNodeFunction(anObject):
        return anObject.nodes()[0]
    
    def __init__(self, id=None, graph=None):
        Node.__init__(self, id=id, graph=graph)
        
        self.initializeNodes()
        self.initializeEdges()
        return

    
    def __deepcopy__(self, memo):
        if self in memo:
            return memo[self]
        
        result = GraphObject.__deepcopy__(self, memo)
        memo[self] = result
        
        copyNodes = copy.deepcopy(self.nodes(), memo)
        result.nodes(copyNodes)
        
        copyEdges = copy.deepcopy(self.edges(), memo)
        result.edges(copyEdges)
        
        return result
    
    
    def initializeNodes(self):
        # initialize the nodes 
        self.nodes([])
        return
    
    def initializeEdges(self):
        # initialize the edges
        self.edges([])
        return


    def getDifferencesWith(self, other, headNodeFunction=None):
        
        if not type(self) is type(other):
            yield 'difference classes'
            raise StopIteration
        
        if headNodeFunction is None:
            headNodeFunction = self.__class__.staticHeadNodeFunction
            
        # if self.__class__.isContainer(self) and not other.__class__.isContainer(other):
        if self.isContainer() and not other.isContainer():
            yield 'self is container but other is not'
            raise StopIteration
        
        # if not self.__class__.isContainer(self) and other.__class__.isContainer(other):
        if not self.isContainer() and other.isContainer():
            yield 'self is not container but other is'
            raise StopIteration

        if self.isRoot() and not other.isRoot():
            yield 'self is root but other is not'
            raise StopIteration
        
        if not self.isRoot() and other.isRoot():
            yield 'self is not root but other is'
            raise StopIteration

        if not self.isRoot():
            for difference in Node.getDifferencesWith(self, other):
                yield difference
            pass
        
        # check the graph internals
        try:
            assertGraphAutomorphism(self, other,
                                    headNodeFunction,
                                    self.__class__.isContainer,
                                    self.__class__.extractContainer)
        except AssertionError, e:
            yield str(e)
        pass
    
    def __ne__(self, other):
        result = self.__eq__(other)
        if result is NotImplemented:
            return result
        return not result        

    def _containsAll(self, nodes=None, edges=None):
        return set(self.nodes()).issuperset(set(nodes))

    def isRoot(self):
        return (self.graph() is None)

    def copyObjects(self, nodes=None, edges=None, nodeMap=None):
        """
        """
        # initialize the node map if it was not passed in as an argument
        # if it was passed in as an argument, the we use it
        # because the info may be needed outside of this function
        if nodeMap is None:
            nodeMap = {}

        # if the nodes being copied
        if nodes is not None:
            for node in set(nodes):
                self.copyNode(node, nodeMap)
                pass
            pass

        """
        need to process the edges first
        """
        if edges is not None:
            for edge in set(edges):
                self.copyEdge(edge, nodeMap)
                pass
            pass
        # END def copyObjects
        pass

    def getClassForCreateGraph(self):
        return self.__class__

    def createGraph(self, id=None, graphClass=None):
        if graphClass is None:
            graphClass = self.getClassForCreateGraph()
        graph = graphClass(graph=self, id=id)
        self.addNode(graph)
        return graph

    def getClassForCreateNode(self):
        return Node

    def createNode(self, id=None, nodeClass=None):
        if nodeClass is None:
            nodeClass = self.getClassForCreateNode()
        node = nodeClass(graph=self, id=id)
        self.addNode(node)
        return node

    def addNode(self, node):
        node.graph(self)
        self.nodes().append(node)
        pass

    def removeNode(self, node, shouldRemoveEdges=True):
        if not node.graph() is self:
            raise ErrorModule.InvalidValueError(
                "cannot remove node when node is not in this graph")

        if shouldRemoveEdges:
            # remove all edges that going to this node as well
            # need to cache because the underlying c++ iterator
            # gets screwed up if we attempt to remove
            edgesToRemove = [edge for edge in node.getEdges()]
            for edge in edgesToRemove:
                logging.debug("remove node %s remove edge %s" % (node, edge))
                self.removeEdge(edge)

        node.graph(None)
        self.nodes().remove(node)
        return
    
    def copyNode(self, node, nodeMap):
        copiedNode = node.clone()
        self.addNode(copiedNode)
        logging.debug("cloned node %s" % node.id())
        nodeMap[node] = [copiedNode]
        return copiedNode

    def initializeEdgesMapForGrouping(self):
        edgesMap = {
            Graph.GROUP_EDGES_INCOMING:[],
            Graph.GROUP_EDGES_OUTGOING:[],
            Graph.GROUP_EDGES_INTERNAL:[]
            }
        return edgesMap

    def preprocessForModifyExternalsOfNewGroup(self, graph):
        return graph

    def groupNodes(self, nodes):
        # need to ensures are all in this graph
        if not self._containsAll(nodes):
            logging.debug("self.nodes() >> %s" % self.nodes())
            logging.debug("nodes >> %s" % nodes)

            raise ErrorModule.NodeNotExistError(
                "cannot group nodes because they are not in thie graph")
            
        # need to ensure that the nodes form a convex subgraph
        # code here

        # create a new graph and add to self
        graph = self.createGraph()

        # keep track of the 3 types of edges in this situation
        edgesMap = self.initializeEdgesMapForGrouping()

        # ----------------------
        # BEGIN modifying internals of new group
        # ----------------------

        for node in nodes:
            for edge in node.getEdges():
                edgeType = 0
                sourceNode = edge.convertEntityToNode(edge.source())
                sinkNode = edge.convertEntityToNode(edge.sink())
                if sourceNode in nodes:
                    edgeType = edgeType + Graph.GROUP_EDGES_OUTGOING
                if sinkNode in nodes:
                    edgeType = edgeType + Graph.GROUP_EDGES_INCOMING

                edgesMap[edgeType].append(edge)
                pass
            pass

        # for each node and internal edges, copy to graph
        graph.copyObjects(nodes=nodes, edges=edgesMap[Graph.GROUP_EDGES_INTERNAL])
        for node in nodes:
            logging.debug("groupNodes removing node %s" % node)
            self.removeNode(node)
            pass

        # ----------------------
        # BEGIN modifying externals of new group
        # ----------------------
        
        # need to do this because function module
        # makes a distinction between function and functionassignment
        graph = self.preprocessForModifyExternalsOfNewGroup(graph)

        replaceMap = {}
        for node in nodes:
            replaceMap[node] = [graph]
            pass

        # for each external edge, copy and remove original
        # some subclasses (eg function)  automatically create certain edges
        # when nodes are created in a graph
        # so we do not want to copy those edges, which would result
        # in duplicate edges.  instead, we query each class
        # to provide a filter for such edges
        edgesInfo = [
            (Graph.GROUP_EDGES_INCOMING, 
             lambda x: x.convertEntityToNode(x.source()),
             self.getFilterForCopyingIncomingEdgesForGrouping()),
            (Graph.GROUP_EDGES_OUTGOING, 
             lambda x: x.convertEntityToNode(x.sink()),
             self.getFilterForCopyingOutgoingEdgesForGrouping())
            ]
        for edgeType, objectFunction, edgeFilter in edgesInfo:
            # processedNodes keeps track of possibility of an external
            # node having multipe edges into the new group, e.g
            # group(4,5) with edges (2,4) and (2,5)
            processedNodes = set([])
            logging.debug("copying edges for type %s" % edgeType)
            for edge in edgeFilter.wrap(edgesMap[edgeType]):
                externalNode = objectFunction(edge)
                if externalNode in processedNodes:
                    continue
                copiedEdges = self.copyEdge(edge, replaceMap)
                processedNodes.add(externalNode)
                # no need to remove edges here 
                # because they were already removed by copyObjects
                pass
            pass

        # END def groupNodes
        return graph

    def getFilterForCopyingIncomingEdgesForGrouping(self):
        return FilterModule.TRUE_FILTER

    def getFilterForCopyingOutgoingEdgesForGrouping(self):
        return FilterModule.TRUE_FILTER

    def ungroupNode(self, aGraph):
        if not isinstance(aGraph, Graph):
            raise ErrorModule.InvalidValueError("cannot ungroup atomic nodes")

        # get the graph's minimal vertices
        minimalNodes = [node for node in Graph.FILTER_NODE_MINIMAL.wrap(aGraph.nodes())]
        # get the graph's maximal vertices
        maximalNodes = [node for node in Graph.FILTER_NODE_MAXIMAL.wrap(aGraph.nodes())]
        
        nodeMap = {}

        # move all objects (nodes and edges) from graph
        self.copyObjects(nodes=aGraph.nodes(), edges=aGraph.edges(), nodeMap=nodeMap)

        theList = []
        for node in maximalNodes:
            theList = theList + nodeMap[node]
        maximalNodes = theList

        theList = []
        for node in minimalNodes:
            theList = theList + nodeMap[node]
        minimalNodes = theList

        # a list of tuples, each of which is
        # 1. the nodes to replace graph in the copied edges
        # 2. the filter for retrieving the edges to be copied
        copyInfo = [
            (minimalNodes, aGraph.incomingEdgeFilter()),
            (maximalNodes, aGraph.outgoingEdgeFilter())
            ]

        for replaceNodes, edgeFilter in copyInfo:
            replaceMap = {
                aGraph:replaceNodes
                }
            #replaceMap = {}
            #replaceMap.update(nodeMap)
            #replaceMap[graph] = replaceNodes
            for edge in aGraph.getEdges(edgeFilter):
                copiedEdges = self.copyEdge(edge, replaceMap)
                # no need to remove the edge here
                # because we'll be remove the edges
                # when we remove the node
                pass
            pass
        
        self.removeNode(aGraph)
        
        # END def ungroupNodes
        pass

    def getReplaceNodes(self, node, replaceMap):
        nodes = set([node])
        if replaceMap.has_key(node):
            nodes = set(replaceMap[node])
            pass
        return nodes

    def copyEdgeSource(self, source, replaceNodes):
        for replaceNode in replaceNodes:
            yield replaceNode
            pass
        pass

    def copyEdgeSink(self, sink, replaceNodes):
        for replaceNode in replaceNodes:
            yield replaceNode
            pass
        pass

    def copyEdge(self, edge, replaceMap):
        """
        need to update so that this works for edges with more than 2 endpoints
        """
        source = edge.source()
        sourceNode = edge.convertEntityToNode(source)
        sourceNodes = self.getReplaceNodes(sourceNode, replaceMap)

        sink = edge.sink()
        sinkNode = edge.convertEntityToNode(sink)
        sinkNodes = self.getReplaceNodes(sinkNode, replaceMap)

        logging.debug("copying edge %s to edges(%s,%s)" %
                      (edge, sourceNodes, sinkNodes))
        edges = []
        for newSource in self.copyEdgeSource(source, sourceNodes):
            for newSink in self.copyEdgeSink(sink, sinkNodes):
                # should call edge.clone() instead?
                copiedEdge = self.createEdge([newSource, newSink])
                logging.debug("copied to new edge %s" % copiedEdge)
                edges.append(copiedEdge)
                pass
            pass

        return edges

    def getClassForCreateEdge(self):
        return BinaryEdge

    def validateNodesForCreateEdge(self, nodes):
        pass

    def createEdge(self, nodes):
        self.validateNodesForCreateEdge(nodes)
        edgeClass = self.getClassForCreateEdge()
        edge = edgeClass(self, nodes)
        self.addEdge(edge)
        return edge

    def addEdge(self, edge):
        edge.graph(self)
        self.edges().append(edge)
        pass

    def removeEdge(self, edge):
        if not edge.graph() is self:
            raise ErrorModule.InvalidValueError(
                "cannot remove edge when edge is not in this graph")
        edge.graph(None)
        self.edges().remove(edge)
        pass

    # END class Graph
    pass

class Edge (GraphObject):

    ATTRIBUTES = GraphObject.ATTRIBUTES + [
        'entities'
        ]

    
    @staticmethod 
    def nameComparator(relation1, relation2):
        return cmp(relation1.name(), relation2.name())
        
    
    def __init__(self, graph=None, entities=None, id=None):
        """
        its possible to create an edge with no parent graph
        (just need to pass in "None" as graph)
        but we force the API to explicit about such situations
        """
        GraphObject.__init__(self, id=id, graph=graph)
        self.entities(entities)

        # END def __init__
        pass

    def __eq__(self, other):
        if isinstance(other, Edge):
            return self.entities() == other.entities()
        return NotImplemented

    def __deepcopy__(self, memo):
        
        if self in memo:
            return memo[self]
        
        result = GraphObject.__deepcopy__(self, memo)
        
        entities = self.entities()
        copyEntities = map(memo.get, entities, entities)
        result.entities(copyEntities)
        
        result.name(self.name())
        
        memo[self] = result
        return result
    
    def convertEntityToNode(self, x):
        return x

    # END class Edge

class BinaryEdge (Edge):

    ATTRIBUTES = Edge.ATTRIBUTES + []

    def __init__(self, graph=None, nodes=None, id=None):
        Edge.__init__(self, graph=graph, entities=nodes, id=None)
        return

    def __repr__(self):
        return "%s-%s" % (self.source(), self.sink())

    def source(self, value=None):
        entities = self.entities()
        if value is not None:
            entities[0] = value
        return entities[0]

    def sink(self, value=None):
        entities = self.entities()
        if value is not None:
            entities[1] = value
        return entities[1]

    """
    will also need to implement sourcePort and sinkPort
    """

    # END class Edge
    pass

def assertGraphAutomorphism(expected, actual,
                            headNodeFunction,
                            isContainer=Graph.isContainer,
                            extractContainer=Graph.extractContainer):
    differences = [
        x for x in determineGraphAutomorphismDifferences(
            expected, actual, headNodeFunction, isContainer=isContainer, extractContainer=extractContainer)
    ]

    assert len(differences) is 0
    return True

    
def determineGraphAutomorphismDifferences(expected, actual,
                                          headNodeFunction,
                                          isContainer=Graph.isContainer,
                                          extractContainer=Graph.extractContainer):

    if len(expected.nodes()) is not len(actual.nodes()):
        yield 'self has %s nodes, other has %s' % (len(expected.nodes()),
                                                   len(actual.nodes()))
        
    if len(expected.nodes()) is 0 or len(actual.nodes()) is 0:
        raise StopIteration
    
    expectedHead = headNodeFunction(expected)
    actualHead = headNodeFunction(actual)
    
    nodesToIgnore = {}
    nodesToProcess = {expectedHead:actualHead}
    while len(nodesToProcess):

        expectedNode, actualNode = nodesToProcess.popitem()
        if expectedNode in nodesToIgnore:
            continue
        
        nodesToIgnore[expectedNode] = actualNode
        
        if not expectedNode == actualNode:
            yield 'expected node %s is not the same as actual node %s' % (
                expectedNode, actualNode)

        for expectedEdge, actualEdge in \
                zip(sorted(expectedNode.getEdges(expectedNode.incomingEdgeFilter()),
                           Edge.nameComparator),
                    sorted(actualNode.getEdges(actualNode.incomingEdgeFilter()),
                           Edge.nameComparator)):
            expectedPredecessor = expectedEdge.entities()[0]
            actualPredecessor = actualEdge.entities()[0]
            if expectedPredecessor in nodesToProcess:
                if not nodesToProcess[expectedPredecessor] is actualPredecessor:
                    yield 'error on matching edges %s and %s >> expected node %s maps to actual nodes %s and %s' % (
                        expectedEdge, actualEdge, expectedPredecessor, actualPredecessor, nodesToProcess[expectedPredecessor])
                    raise StopIteration
                continue
            nodesToProcess[expectedPredecessor] = actualPredecessor
            pass
        
        for expectedEdge, actualEdge in \
                zip(sorted(expectedNode.getEdges(expectedNode.outgoingEdgeFilter()),
                           Edge.nameComparator),
                    sorted(actualNode.getEdges(actualNode.outgoingEdgeFilter()),
                           Edge.nameComparator)):
            expectedSuccessor = expectedEdge.entities()[1]
            actualSuccessor = actualEdge.entities()[1]
            if expectedSuccessor in nodesToProcess:
                if not nodesToProcess[expectedSuccessor] is actualSuccessor:
                    yield 'error on matching edges %s and %s >> expected node %s maps to actual nodes %s and %s' % (
                        expectedEdge, actualEdge, expectedSuccessor, actualSuccessor, nodesToProcess[expectedSuccessor])
                    raise StopIteration
                continue
            nodesToProcess[expectedSuccessor] = actualSuccessor
            pass
        
        pass
    
    raise StopIteration



class GraphStructureFilter(FilterModule.CompositeFilter):

    def __init__(self):

        FilterModule.CompositeFilter.__init__(
            self,
            FilterModule.AndAccumulator())

        self._initializeGraphTypeFilter()

        self._rawFilters = []
        
        return

    def _initializeGraphTypeFilter(self):
        self._filters.append(
            FilterModule.ObjectFilter(
                valueFunction = FilterModule.FUNCTION_TRUE,
                objectFunction = lambda x: isinstance(x, Graph)
            )
        )

        self._filters[-1].name('isinstance(self, Graph)')
        
        return

    
    def addFilter(self, filter):
        """
        expects filters that match nodes
        """
        theWrappedFilter = GraphNodeWrapperFilter(filter)

        FilterModule.CompositeFilter.addFilter(self, theWrappedFilter)
        self._rawFilters.append(filter)
        return


    # END class GraphStructureFilter
    pass

class GraphNodeWrapperFilter(FilterModule.ObjectFilter):

    @staticmethod
    def match(object, filterHolder=None):
        if filterHolder is None:
            raise NotImplementedError
        
        return any([filterHolder._filter.matches(y) for y in object.nodes()])
    
    
    def __init__(self, filter):
        FilterModule.ObjectFilter.__init__(
            self,
            valueFunction = FilterModule.FUNCTION_TRUE,
            objectFunction = currypy.Curry(GraphNodeWrapperFilter.match,
                                               filterHolder=self)
        )
        self._filter = filter
        return
    
    # END class GraphNodeWrapperFilter
    pass


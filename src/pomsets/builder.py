import logging
import uuid

import pomsets.context as ContextModule
import pomsets.definition as DefinitionModule
import pomsets.parameter as ParameterModule

class Builder(object):


    def createNewPomset(self):
        """
        """
        #TODO: this should construct a command to create the new pomset
        #      and execute the command (or send an event to execute the command)
        #      within the command framework, the command should also
        #      create an event to update the GUI

        newPomset = DefinitionModule.getNewNestDefinition()
        newPomset.name('pomset %s' % uuid.uuid4().hex[:3])
        
        newPomsetContext = ContextModule.Context()
        newPomsetContext.pomset(newPomset)
        
        return newPomsetContext


    def createNewNode(self, pomset, name=None,
                      definitionToReference=None):
        """
        """

        #TODO: this should construct a command to create the new node
        #      and execute the command (or send an event to execute the command)
        #      within the command framework, the command should also
        #      create an event to update the GUI

        if name is None:
            name = 'job %s' % len(pomset.nodes())

        id = uuid.uuid4().hex
        node = pomset.createNode(id=id)
        node.name(name)

        if definitionToReference is None:
            raise ValueError("need to specify a definition to reference")

        node.definitionToReference(definitionToReference)

        # TODO:
        # see if it's possible to not have to run this line
        node.executable = definitionToReference.executable


        return node



    def canConnect(self, 
                   sourceNode, sourceParameterId,
                   targetNode, targetParameterId):
        """
        This is a validation function to determine whether the
        ports provided can be connected to each other
        """

        # cannot connect to itself
        if sourceNode == targetNode and sourceParameterId==targetParameterId:
            print "cannot connect parameter to itself"
            logging.debug("cannot connect parameter to itself")
            return False

        sourceParameter = None
        targetParameter = None
        try:
            sourceParameter = sourceNode.getParameter(sourceParameterId)
            targetParameter = targetNode.getParameter(targetParameterId)
        except Exception, e:
            # if the parameter does not exist
            # then there's no way to connect
            print 'cannot connect non-existent parameters'
            logging.debug('cannot connect non-existent parameters')
            return False
        
        # inputs cannot connect to each other
        # outputs also cannot connect to each other
        #if sourceParameter.portDirection() == targetParameter.portDirection():
        #    print 'cannot connect parameters of the same direction'
        #    logging.debug('cannot connect parameters of the same direction')
        #    return False

        if not sourceParameter.portType() == targetParameter.portType():
            logging.debug("cannot connect ports of different types")
            print "cannot connect ports of different types"
            return False

        if not targetParameter.portDirection() == ParameterModule.PORT_DIRECTION_INPUT:
            logging.debug("parameter %s is not an input" % targetParameterId)
            print "parameter %s is not an input" % targetParameterId
            return False
        
        if not sourceParameter.portDirection() == ParameterModule.PORT_DIRECTION_OUTPUT and \
           not sourceParameter.getAttribute(ParameterModule.PORT_ATTRIBUTE_ISSIDEEFFECT):
            logging.debug("parameter %s is not an output" % sourceParameterId)
            print "parameter %s is not an output" % sourceParameterId
            return False

        return True



    def connect(self, 
                pomset,
                sourceNode, sourceParameterId,
                targetNode, targetParameterId):
        """
        This assumes that the caller has already
        verified that canConnect() returns True
        """
        sourceParameter = sourceNode.getParameter(sourceParameterId)
        targetParameter = targetNode.getParameter(targetParameterId)


        portType = sourceParameter.portType()
        if portType == ParameterModule.PORT_TYPE_TEMPORAL:
            connection = pomset.connectParameters(
                sourceNode, sourceParameterId,
                targetNode, targetParameterId
            )
            path = [
                sourceNode,
                sourceParameterId,
                connection,
                targetParameterId,
                targetNode
            ]
            
        else:
    
            # create a blackboard parameter
            bbParameterId = '%s-%s' % (sourceParameterId,
                                         targetParameterId)
            bbParameter = ParameterModule.BlackboardParameter(
                bbParameterId, pomset)
            pomset.addParameter(bbParameter)
    
            # create a parameter connection (source->blackboard)
            sourceParameterConnection = pomset.connectParameters(
                sourceNode, sourceParameterId,
                pomset, bbParameterId
            )
    
            # create a parameter connection (blackboard->target)
            targetParameterConnection = pomset.connectParameters(
                pomset, bbParameterId,
                targetNode, targetParameterId
            )
    
            path = [
                sourceNode,
                sourceParameterId,
                sourceParameterConnection,
                bbParameter,
                targetParameterConnection,
                targetParameterId,
                targetNode
            ]

        return path

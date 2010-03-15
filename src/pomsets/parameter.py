
import pomsets.graph as GraphModule
import pomsets.resource as ResourceModule

PORT_TYPE_DATA = 'data'
PORT_TYPE_BLACKBOARD = 'blackboard'
PORT_TYPE_TEMPORAL = 'temporal'
PORT_DIRECTION_INPUT = 'input'
PORT_DIRECTION_OUTPUT = 'output'
PORT_DIRECTION_INTERNAL = 'internal'

PORT_ATTRIBUTE_COMMANDLINE = 'isCommandline'
PORT_ATTRIBUTE_COMMANDLINE_OPTIONS = 'commandlineOptions'
PORT_ATTRIBUTE_PARAMETERSWEEP = 'isParameterSweep'
PORT_ATTRIBUTE_ISSIDEEFFECT = 'isSideEffect'
PORT_ATTRIBUTE_ISINPUTFILE = 'isInputFile'
PORT_ATTRIBUTE_ISLIST = 'isList'
PORT_ATTRIBUTE_ISENUM = 'isEnum'
PORT_ATTRIBUTE_ISOPTIONAL = 'isOptional'
PORT_ATTRIBUTE_DESCRIPTION = 'description'

# these are actually dynamic parameter attributes
# PORT_ATTRIBUTE_STAGINGREQUIRED = 'stagingRequired'
# PORT_ATTRIBUTE_PSGROUP = 'parameter sweep group'

COMMANDLINE_ENUM_MAP = 'enum map'
# what the flag to the commandline argument should be
COMMANDLINE_PREFIX_FLAG = 'prefix flag'
# if the parameter has a list of values,
# True if the flag should be prepended for each value,
# False (default) if the flag should be prepended just once
COMMANDLINE_PREFIX_FLAG_DISTRIBUTE = 'distribute prefix flag'
# TODO:
# need an attribute as to whether the value should be quoted
# need an attribute as to whether the quote should be distributed
# need an attribute to specify what the start/end quote chars should be



def setAttributes(parameter, attributes):
    for key, value in attributes.iteritems():
        parameter.setAttribute(key, value)
    return


class Parameter(ResourceModule.Struct):

    ATTRIBUTES = [
        'id', 'name', 'definition', 'attributes', 'description'
    ]

    def __init__(self, id=None, name=None,
                 possibleValues=None, defaultValue=None):

        ResourceModule.Struct.__init__(self)

        if id is None:
            id = ResourceModule.Resource.ID_GENERATOR()
        self.id(id)
        
        if name is None:
            name = ''
        self.name(name)

        self.possibleValues(possibleValues)

        if defaultValue is not None:
            self.defaultValue(defaultValue)

        self.attributes({})

        pass


    def __str__(self):
        return self.id()

    def __repr__(self):
        return self.id()

    def __eq__(self, other):
        if type(self) == type(other):
            return self.id() == other.id() and \
                   self.function().id() == other.function().id()
        return NotImplemented

    def getAttribute(self, attributeName, defaultValue=False):
        return self.attributes().get(attributeName, defaultValue)

    def setAttribute(self, attributeName, value):
        self.attributes()[attributeName] = value
        return


    def possibleValues(self, possibleValues=None):
        """
	will need to implement to deal with the situation where
	the new set of possible values does not allow for 
	an already set value
	"""
        if possibleValues is not None:
            self.__possibleValues = possibleValues
            pass
        try:
            self.__possibleValues
        except AttributeError:
            #if possible values undefined, default to unrestricted
            self.__possibleValues = UnrestrictedPossibleValues()
            pass
        return self.__possibleValues


    def defaultValue(self, defaultValue=None):
        if defaultValue is not None:
            #validate that the default value is permitted
            possibleValues = self.possibleValues()
            try:
                possibleValues.validateValue(defaultValue)
            except:
                raise InvalidValueError("default value not permitted by specified set of possible values")
            self.__defaultValue = defaultValue
            pass
        
        try:
            self.__defaultValue
        except AttributeError:
            #if no default defined, default to none
            return None
        return self.__defaultValue

    pass



class ExposedParameter(Parameter):
    """
    the externally exposed parameters of a function are defined here
    """

    ATTRIBUTES = Parameter.ATTRIBUTES + [
        'portDirection',
        'portType',
        'optional',
        'active'
    ]

    def __init__(self, optional=True, active=False, 
                 portDirection=None, portType=None, **kwds):
        Parameter.__init__(self, **kwds)
        self.optional(optional)
        self.active(active)
        self.portDirection(portDirection)
        self.portType(portType)
        return

    def __eq__(self, other):
        differences = [x for x in self.getDifferencesWith(other)]
        return len(differences) is not 0
    
    def getDifferencesWith(self, other):
        if not other:
            yield 'other is None'
            raise StopIteration
        if not self.__class__ == other.__class__:
            yield 'other is of class %s' % other.__class__
            raise StopIteration
        if not self.id() == other.id():
            yield 'id'
        if not self.optional() == other.optional():
            yield 'optional'
        if not self.active() == other.active():
            yield 'active'
        if not self.portDirection() == other.portDirection():
            yield 'portDirection'
        if not self.portType() == other.portType():
            yield 'portType'
        raise StopIteration
    
    
    # END class ExposedParameter
    pass


class BlackboardParameter(ExposedParameter):

    ATTRIBUTES = ExposedParameter.ATTRIBUTES + []

    def __init__(self, id=None, **kwds):
        ExposedParameter.__init__(self, id=id, 
                                  portDirection=PORT_DIRECTION_INTERNAL,
                                  portType=PORT_TYPE_BLACKBOARD)
        return

    # END class BlackboardParameter
    pass


class PortParameter(ExposedParameter):

    ATTRIBUTES = ExposedParameter.ATTRIBUTES + []

    def __init__(self, **kwds):
        ExposedParameter.__init__(self, **kwds)
        return

    # END class PortParameter
    pass

class DataParameter(PortParameter):

    ATTRIBUTES = PortParameter.ATTRIBUTES + []

    def __init__(self, **kwds):
        PortParameter.__init__(self, 
                               portType=PORT_TYPE_DATA,
                               **kwds)

        return

    # END class DataParameter
    pass



class InputTemporalParameter(PortParameter):

    ATTRIBUTES = PortParameter.ATTRIBUTES + []

    def __init__(self, id=None):
        PortParameter.__init__(self, id=id, 
                               portDirection=PORT_DIRECTION_INPUT,
                               portType=PORT_TYPE_TEMPORAL,
                               optional=True,
                               active=True)

        return

    # END class InputTemporalParameter
    pass




class OutputTemporalParameter(PortParameter):

    ATTRIBUTES = PortParameter.ATTRIBUTES + []

    def __init__(self, id=None):
        PortParameter.__init__(self, id=id, 
                               portType=PORT_TYPE_TEMPORAL,
                               portDirection=PORT_DIRECTION_OUTPUT,
                               optional=True,
                               active=True)
        # END def __init__
        pass

    # END class OutputTemporalParameter
    pass



class PossibleValues(ResourceModule.Resource):
    def __init__(self):
        pass

    def __iter__(self):
        raise NotImplementedError

    def validateValue(self, value):
        raise NotImplementedError

    pass

class RangePossibleValues(PossibleValues):
    def __init__(self):
        pass

    def __iter__(self):
        raise NotImplementedError

    def validateValue(self, value):
        """ x >= min && x <= max && x-min/step = n
	"""
        if value < min:
            raise InvalidValueError
        if value > max:
            raise InvalidValueError
        val = (value-min)/step
        if val != int(val):
            raise InvalidValueError
        return

    def min(self, min=0):
        raise NotImplementedError

    def max(self, max=-1):
        raise NotImplementedError

    def step(self, step=1):
        raise NotImplementedError

    pass

class UnrestrictedPossibleValues(PossibleValues):
    """ a single value, unrestricted in its contents """
    def __init__(self):
        pass

    def __iter__(self):
        raise NotImplementedError

    def validateValue(self, value):
        return

    pass

class EnumerationPossibleValues(PossibleValues):
    def __init__(self, values=None):
        PossibleValues.__init__(self, values)
        pass

    def validateValue(self, value):
        for enumValue in self:
            if value == enumValue:
                return
            pass
        raise InvalidValueError

    def __iter__(self):
        raise NotImplementedError

    def addValue(self):
        raise NotImplementedError

    def removeValue(self):
        raise NotImplementedError

    pass

class DefaultValue(ResourceModule.ResourceReference):
    def __init__(self, possibleValues=None):
        self.__possibleValues = possibleValues

    def value(self, value=None):
        if value is not None:
            #if it is not valid
            #raise InvalidValueError

            #set value
            pass
        return self.value

    pass



class ParameterConnectionPath(GraphModule.Edge):
    
    ATTRIBUTES = GraphModule.Edge.ATTRIBUTES + [
        'sourceNode',
        'targetNode',
        'sourceParameter',
        'targetParameter',
        'path'
    ]

    def __init__(self, graph=None, nodes=None):
        GraphModule.Edge.__init__(self, graph=graph, entities=nodes)
        self.path([])
        return
    

    # END class ParameterConnectionPath
    pass



class ParameterConnection(ResourceModule.Struct):

    ATTRIBUTES = [
        'sourceNode',
        'targetNode',
        'sourceParameter',
        'targetParameter'
    ]

    #def __init__(self, definition=None, nodes=None):
    #    GraphModule.Edge.__init__(self, graph=definition, entities=nodes)
    #    return

    def setReferences(self,
                      sourceNode, sourceParameter,
                      targetNode, targetParameter):

        """
	This should only be called in conjunction with
	the constructor
	"""

        self.sourceNode(sourceNode)
        self.targetNode(targetNode)
        self.sourceParameter(sourceParameter)
        self.targetParameter(targetParameter)
        return


    def source(self):
        return (self.sourceNode(), self.sourceParameter())

    def target(self):
        return (self.targetNode(), self.targetParameter())

    def sink(self):
        return self.target()

    def entities(self, value=None):
        return [self.source(), self.target()]

    def convertEntityToNode(self, entity):
        return entity[0]

    pass


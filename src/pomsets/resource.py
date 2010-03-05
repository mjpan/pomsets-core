import time
import logging
import currypy

def generateId():
    """
    this should be a static function to generate a unique id
    """
    id = -1
    while True:
        id = id + 1
        yield "%s" % id



class Property(object):

    def __init__(self):
        self._value = None
        return

    def __call__(self, value=None):
        """
        this is a generic function that allows 
        """
        if value is not None:
            self._value = value
        return self._value

    # END class Property
    pass


class Struct(object):

    @staticmethod
    def attribute(object, value=None, attributeName=None):
        """
        this is a generic function that allows 
        """
        # if value is being passed in, this is being used as a setter
        if value is not None:
            setattr(object, attributeName, value)
        # if object does not have the attribute
        # need to initialize to None
        if not hasattr(object, attributeName):
            setattr(object, attributeName, None)
        # finally, return the value of the attribute
        return getattr(object, attributeName)


    def __init__(self):
        for ATTRIBUTE in self.__class__.ATTRIBUTES:
            #setattr(self, ATTRIBUTE,
            #        currypy.Curry(Struct.attribute, self,
            #            attributeName='_'+ATTRIBUTE)
            #        )
            if hasattr(self, ATTRIBUTE):
                continue
            setattr(self, ATTRIBUTE, Property())
        return

    """
    def __getattr__(self, name):
        if name in self.__class__.ATTRIBUTES:
            #return currypy.Curry(self.attribute, attributeName="_%s"%name)
            return currypy.Curry(
                Struct.attribute, self, 
                attributeName="_%s" % name)
        try:
            return self.__dict__[name]
        except KeyError, e:	    # key is not found locally
            raise AttributeError(e)

    def __setattr__(self, name, value):
        if name in self.__class__.ATTRIBUTES:
            return Struct.attribute(
                self, attributeName="_%s" % name, value=value)
        self.__dict__[name] = value
    """

    def resetAttribute(self, attributeName):
        setattr(self, attributeName, None)
        return


    # END class Struct
    pass


class Instantiable(Struct):

    ATTRIBUTES = [
        'instantiationClass',
        'instantiations'
    ]


    def __init__(self, instantiationClass, instantiations=None):

        self.instantiationClass(instantiationClass)

        if instantiations is None:
            instantiations = []
        self.instantiations(instantiations)

        return

    def addInstantiation(self, instantiation):
        self.instantiations().append(instantiation)
        pass

    def removeInstantiation(self, instantiation):
        self.instantiations().remove(instantiation)
        pass

    # END class Instantiable
    pass

class Resource(object):
    LOCALE_DEFAULT = "en"

    PREVIOUS_ID = None

    NAME_ANONYMOUS = "_anonymous"



    ID_GENERATOR = generateId().next

    def __init__(self, id=None, name=None, description="", 
                 locale=LOCALE_DEFAULT):
        self.__localizedNameMap = {}
        self.__localizedDescriptionMap = {}

        self.id(id)
        self.name(name=name, locale=locale)
        self.description(locale, description)
        pass

    def id(self, id=None):
        if id is not None:
            # self.__id = id
            setattr(self, "__id", id)
            pass

        if not hasattr(self, "__id"):
            setattr(self, "__id", Resource.ID_GENERATOR())

        return getattr(self, "__id")


    def name(self, name=None, locale=LOCALE_DEFAULT):
        if name is None and not self.__localizedNameMap.has_key(locale):
            name = Resource.NAME_ANONYMOUS
        if name is not None:
            self.__localizedNameMap[locale] = name
            pass
        return self.__localizedNameMap[locale]

    def description(self, description=None, locale=LOCALE_DEFAULT):
        if description is not None:
            self.__localizedDescriptionMap[locale] = description
        if not locale in self.__localizedDescriptionMap:
            self.__localizedDescriptionMap[locale] = u''
        return self.__localizedDescriptionMap[locale]

    # END class Resource
    pass

class ResourceReference(Resource):
    def __init__(self, resource=None, id=None, name=None):
        self.resource(resource)

        Resource.__init__(self, id=id, name=name)

        """
	if resource is None:
	    raise InvalidValueError("cannot initializae a null reference")
        """
        pass

    def resource(self, resource=None):
        if resource is not None:
            setattr(self, "__resource", resource)

        return getattr(self, "__resource", None)

    def resourceID(self):
        return self.resource().id()

# cannot put this in yet because Module 
# inherits from ResourceReference as well
#    def __getattr__(self, name):
#	"""
#	this overrides the default __getattr__
#	so that it will look into self.resource() 
#	if self does not have the attribute 
#	"""
#	if name in self.__dict__:
#	    return self.__dict__[name]
#	print "attempting to get from resource of %s" %self
#	return getattr(self.resource(), name)


    pass



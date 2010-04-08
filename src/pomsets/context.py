from __future__ import with_statement

import pickle

import pomsets.resource as ResourceModule



def pickleDefinition(path, definition):

    pickleCreated = False
    with open(path, 'w') as f:
        pickle.dump(definition, f)
        pickleCreated = True
        pass

    if not pickleCreated:
        raise IOError('failed on creating pickle')

    return



def savePomset(context):
    savePomsetAs(context, context.url())
    return


def savePomsetAs(context, path, extension='pomset'):

    if not path.endswith(extension):
        path = '.'.join([path, extension])

    try:
        # pomset = context.pomset()
        reference = context.reference()
        pickleDefinition(path, reference)

        # saved, so no longer modified
        context.isModified(False)

        # TODO:
        # this should be a full URL
        context.url(path)
    except IOError, e:
        logging.error(e)
        raise

    return


# TODO:
# the value passed in should be a full URL
def loadPomset(path=None):

    reference = None
    pomset = None

    loadedObject = None
    with open(path, 'r') as f:
        loadedObject = pickle.load(f)
        pass

    if loadedObject is None:
        # TODO: should send a failed command event
        raise IOError("failed to load pomset from %s" % path)

    pomsetContext = wrapPomsetInContext(loadedObject)

    # TODO:
    # the value passed in should be a full URL
    pomsetContext.url(path)

    return pomsetContext



def wrapPomsetInContext(pomset):

    import pomsets.definition as DefinitionModule

    reference = None
    if isinstance(pomset, DefinitionModule.ReferenceDefinition):
        reference = pomset
        pomset = reference.definitionToReference()
    if reference is None:
        reference = DefinitionModule.ReferenceDefinition()
        reference.definitionToReference(pomset)

    pomsetContext = Context()
    pomsetContext.pomset(pomset)
    pomsetContext.reference(reference)
    pomsetContext.isModified(False)

    return pomsetContext


class Context(ResourceModule.Struct):

    ATTRIBUTES = [
        'isModified',
        'pomset',
        'reference',
        'url'
        ]

    def __init__(self):
        ResourceModule.Struct.__init__(self)
        return
    
    def getContextPathFor(self, pomset):

        # TODO:
        # remove the hardcoding
        # and actually handle pomset nests

        return [self.pomset()]

    def getParameterToEdit(self, definition, parameterName):
        nodeToEdit, parameterToEdit = \
            definition.getParameterToEdit(parameterName)
        if nodeToEdit is self.pomset():
            nodeToEdit = self.reference()

        return (nodeToEdit, parameterToEdit)

    # END class Context
    pass

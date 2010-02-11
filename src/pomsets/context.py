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


def savePomsetAs(context, path, extension='.pomset'):

    pomset = context.pomset()

    if not path.endswith(extension):
        path = '.'.join([path, extension])

    try:
        pickleDefinition(path, pomset)

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

    pomset = None
    with open(path, 'r') as f:
        pomset = pickle.load(f)
        pass

    if pomset is None:
        # TODO: should send a failed command event
        raise IOError("failed to load pomset from %s" % path)

    pomsetContext = Context()
    pomsetContext.pomset(pomset)

    pomsetContext.isModified(False)

    # TODO:
    # the value passed in should be a full URL
    pomsetContext.url(path)

    return pomsetContext


class Context(ResourceModule.Struct):

    ATTRIBUTES = [
        'isModified',
        'pomset',
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
    
    # END class Context
    pass

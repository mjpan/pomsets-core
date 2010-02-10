import logging
import os
import sys


def configLogging():
    """
    this will be called by all the main functions 
    to use the default setup for logging
    """
    # define a basic logger to write to file
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename='/tmp/pomsets.log',
                        filemode='w')

    # define a handler to write to stderr
    # console = logging.StreamHandler()
    # set the level of this to verbosity of severity 'warning'
    # console.setLevel(logging.WARNING)
    # set a format which is simpler for console use
    # formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    # tell the handler to use this format
    # console.setFormatter(formatter)
    # add the handler to the root logger
    # logging.getLogger('').addHandler(console)

    # end def configureLogging
    pass


def setPythonPath():
    for key in ['POMSETS_HOME']:
        value = os.getenv(key)
        if value is None:
            continue
        sys.path.insert(0, value)
        pass

    return


def getPomsetRoot():
    # configure POMSET_ROOT
    POMSET_ROOT = os.getenv('POMSET_ROOT')
    if POMSET_ROOT is None:
        APP_ROOT = os.getenv('APP_ROOT')
        if APP_ROOT is not None:
            POMSET_ROOT = os.sep.join([os.getenv('APP_ROOT'), 'pomsets'])
        else:
            POMSET_ROOT = ''
    return POMSET_ROOT

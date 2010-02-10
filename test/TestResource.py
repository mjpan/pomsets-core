import logging
import os
import sys
import unittest


#APP_ROOT = os.getenv('APP_ROOT')
#sys.path.insert(0, '%s/currypy/src' % APP_ROOT)
#sys.path.insert(0, '%s/pomsets/src' % APP_ROOT)


import pomsets.resource as ResourceModule

# import utils

class TestStruct(unittest.TestCase):
    
    def testGetattr(self):
        mockStruct = MockStruct()

        # verify for when the attribute is defined in one's own class
        assert mockStruct.StructAttribute() is None
        theValue = 'abc'
        mockStruct.__dict__['_StructAttribute'] = theValue
        assert theValue == mockStruct.StructAttribute()
        try:
            mockStruct.NotStructAttribute()
            assert False
        except AttributeError, e:
            pass

        # verify for when the attribute is defined in one's superclass
        assert mockStruct.SuperclassAttribute() is None
        theValue = 'def'
        mockStruct.__dict__['_SuperclassAttribute'] = theValue
        assert theValue == mockStruct.SuperclassAttribute()

        pass
    
    
    def testSetattr(self):
        
        mockStruct = MockStruct()
        
        theValue = 'abc'
        mockStruct.StructAttribute(theValue)
        assert mockStruct.__dict__['_StructAttribute'] == theValue
        assert mockStruct.StructAttribute() == theValue
        try:
            mockStruct.NotStructAttribute(theValue)
            assert False
        except AttributeError, e:
            pass

        # assert the setattr of an attribute value explicity defined
        # in the superclass also works
        theValue = 'def'
        mockStruct.SuperclassAttribute(theValue)
        assert mockStruct.__dict__['_SuperclassAttribute'] == theValue
        assert mockStruct.SuperclassAttribute() == theValue
        
        pass
    
    pass



class SuperclassStruct(ResourceModule.Struct):
    ATTRIBUTES = ['SuperclassAttribute']

    def __init__(self):
        pass
    pass


class MockStruct(SuperclassStruct):
    
    ATTRIBUTES = SuperclassStruct.ATTRIBUTES + ['StructAttribute']
    
    def __init__(self):
        pass
    
    pass

def main():
    util.configLogging()
    suite = unittest.makeSuite(TestStruct,'test')
    runner = unittest.TextTestRunner()
    runner.run(suite)
    return

if __name__=="__main__":
    main()

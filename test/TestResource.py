from __future__ import with_statement

import logging
import os
import pickle
import sys
import tempfile
import unittest

import pomsets.resource as ResourceModule

class TestStruct(unittest.TestCase):
    
    def testGetattr(self):
        mockStruct = MockStruct()

        # verify for when the attribute is defined in one's own class
        assert mockStruct.StructAttribute() is None
        theValue = 'abc'
        # mockStruct.__dict__['_StructAttribute'] = theValue
        mockStruct.StructAttribute(theValue)
        assert isinstance(mockStruct.StructAttribute, ResourceModule.Property)
        assert theValue == mockStruct.StructAttribute._value
        assert theValue == mockStruct.StructAttribute()
        try:
            mockStruct.NotStructAttribute()
            assert False
        except AttributeError, e:
            pass

        # verify for when the attribute is defined in one's superclass
        assert mockStruct.SuperclassAttribute() is None
        theValue = 'def'
        # mockStruct.__dict__['_SuperclassAttribute'] = theValue
        mockStruct.SuperclassAttribute(theValue)
        assert isinstance(mockStruct.SuperclassAttribute, ResourceModule.Property)
        assert theValue == mockStruct.SuperclassAttribute._value
        assert theValue == mockStruct.SuperclassAttribute()

        pass
    
    
    def testSetattr(self):
        
        mockStruct = MockStruct()
        
        theValue = 'abc'
        mockStruct.StructAttribute(theValue)
        # assert mockStruct.__dict__['_StructAttribute'] == theValue
        assert isinstance(mockStruct.StructAttribute, ResourceModule.Property)
        assert mockStruct.StructAttribute._value == theValue
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
        # assert mockStruct.__dict__['_SuperclassAttribute'] == theValue
        assert isinstance(mockStruct.SuperclassAttribute, ResourceModule.Property)
        assert mockStruct.SuperclassAttribute._value == theValue
        assert mockStruct.SuperclassAttribute() == theValue
        
        pass
    
class TestPickle(unittest.TestCase):

    def setUp(self):
        self._tempfile = tempfile.NamedTemporaryFile()
        return

    def tearDown(self):
        filename = self._tempfile.name
        self._tempfile.close()
        if os.path.exists(filename):
            os.unlink(filename)
        return

    def testPickle(self):
        obj = MockStruct()
        obj.SuperclassAttribute(1)
        obj.StructAttribute('foo')
        pickle.dump(obj, self._tempfile)

        pass


    # END class TestStruct
    pass



class SuperclassStruct(ResourceModule.Struct):
    ATTRIBUTES = ['SuperclassAttribute']

    def __init__(self):
        ResourceModule.Struct.__init__(self)
        pass
    pass


class MockStruct(SuperclassStruct):
    
    ATTRIBUTES = SuperclassStruct.ATTRIBUTES + ['StructAttribute']
    
    def __init__(self):
        SuperclassStruct.__init__(self)
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

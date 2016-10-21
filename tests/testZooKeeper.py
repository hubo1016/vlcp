'''
Created on 2016/9/20

:author: hubo
'''
import unittest
from vlcp.utils.zookeeper import *

class Test(unittest.TestCase):
    def testUString(self):
        self.assertEqual(ustring.tobytes(b'abcdefg'), b'\x00\x00\x00\x07abcdefg')
        self.assertEqual(ustring.create(b'\x00\x00\x00\x07abcdefg'), b'abcdefg')
        self.assertEqual(ustring.parse(b'\x00'), None)
        self.assertEqual(ustring.parse(b'\x00\x00\x00\x00'), (b'', 4))
        self.assertEqual(ustring.parse(b'\xff\xff\xff\xff'), (None, 4))
        self.assertEqual(ustring.parse(b'\x00\x00\x00\x07abc'), None)
        self.assertEqual(ustring.parse(b'\x00\x00\x00\x07abcdefghij'), (b'abcdefg', 11))
    
    def testVector(self):
        vector_int = vector(int32)
        self.assertEqual(vector_int.tobytes([1,2,3]), b'\x00\x00\x00\x03\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00\x03')
        self.assertEqual(vector_int.create(b'\x00\x00\x00\x03\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00\x03'), [1,2,3])
        self.assertEqual(vector_int.parse(b'\x00\x00\x00\x03\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00\x03abc'), ([1,2,3], 16))
        self.assertEqual(vector_int.parse(b'\x00\x00\x00\x03\x00\x00\x00\x01\x00'), None)
        self.assertEqual(vector_int.parse(b'\x00\x00\x00\x00'), ([],4))
        self.assertEqual(vector_int.parse(b'\xff\xff\xff\xff'), (None, 4))
        vector_ustring = vector(ustring)
        self.assertEqual(vector_ustring.tobytes([b'abc',b'def']), b'\x00\x00\x00\x02\x00\x00\x00\x03abc\x00\x00\x00\x03def')
        self.assertEqual(vector_ustring.parse(b'\x00\x00\x00\x02\x00\x00\x00\x03abc\x00\x00\x00\x03defab'), ([b'abc',b'def'], 18))

    def testConnectResponse(self):
        data = '\x00\x00\x00\x1a\x00\x00\x00\x00\x00\x00u0\x00\x00\x00\x00\x01#Eg\x00\x00\x00\x06defghi'
        r, l = ConnectResponse.parse(data)
        self.assertEqual(l, len(data))
        r.zookeeper_type = CONNECT_PACKET
        r._autosubclass()
        self.assertEqual(r.sessionId, 0x1234567)
        self.assertEqual(r.passwd, 'defghi')
        self.assertFalse(hasattr(r, 'readOnly'))
        data = '\x00\x00\x00\x1b\x00\x00\x00\x00\x00\x00u0\x00\x00\x00\x00\x01#Eg\x00\x00\x00\x06defghi\x01'
        r, l = ConnectResponse.parse(data)
        self.assertEqual(l, len(data))
        r.zookeeper_type = CONNECT_PACKET
        r._autosubclass()
        self.assertEqual(r.sessionId, 0x1234567)
        self.assertEqual(r.passwd, 'defghi')
        self.assertTrue(hasattr(r, 'readOnly'))
        self.assertEqual(r.readOnly, True)
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
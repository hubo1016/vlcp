'''
Created on 2016/10/24

:author: hubo
'''
import unittest
from vlcp.server import Server
from vlcp.event import RoutineContainer
from vlcp.event import Stream
from vlcp.utils.encoders import unicode_decoder, unicode_encoder, gzip_encoder, gzip_decoder, deflate_encoder,\
        deflate_decoder, str_decoder, str_encoder
import tempfile
from vlcp.event.stream import MemoryStream, FileWriter, FileStream
from contextlib import closing
import os

class Test(unittest.TestCase):


    def setUp(self):
        self.server = Server()
        self.rc = RoutineContainer(self.server.scheduler)

    def tearDown(self):
        pass
    
    def testStream_1(self):
        s = Stream()
        retvalue = []
        rc = self.rc
        def write_routine():
            for m in s.write(b'abcde', rc):
                yield m
            for m in s.write(b'defgh', rc):
                yield m
            for m in s.write(b'ijklm', rc, True):
                yield m
        def read_routine():
            for m in s.read(rc):
                yield m
            retvalue.append(rc.data)
        rc.subroutine(write_routine())
        rc.subroutine(read_routine())
        self.server.serve()
        self.assertEqual(retvalue, [b'abcdedefghijklm'])

    def testStream_2(self):
        s = Stream(True)
        retvalue = []
        rc = self.rc
        def write_routine():
            for m in s.write(u'abcde', rc):
                yield m
            for m in s.write(u'defgh', rc):
                yield m
            for m in s.write(u'ijklm', rc, True):
                yield m
        def read_routine():
            for m in s.read(rc):
                yield m
            retvalue.append(rc.data)
        rc.subroutine(write_routine())
        rc.subroutine(read_routine())
        self.server.serve()
        self.assertEqual(retvalue, [u'abcdedefghijklm'])

    def testStream_3(self):
        s = Stream()
        retvalue = []
        rc = self.rc
        def write_routine():
            for m in s.write(b'abcde', rc):
                yield m
            for m in s.write(b'de\nf\rgh\r\n', rc, buffering=False):
                yield m
            for m in s.readline(rc):
                yield m
            retvalue.append(rc.data)
            for m in s.readline(rc):
                yield m
            retvalue.append(rc.data)
            for m in s.write(b'ijklm', rc, True):
                yield m
            for m in s.readline(rc):
                yield m
            retvalue.append(rc.data)
        rc.subroutine(write_routine())
        self.server.serve()
        self.assertEqual(retvalue, [b'abcdede\n', b'f\rgh\r\n', b'ijklm'])

    def testStream_4(self):
        s = Stream()
        retvalue = []
        rc = self.rc
        def write_routine():
            for m in s.write(b'abcde', rc):
                yield m
            for m in s.write(b'de\nf\rgh\r\n', rc, buffering=False):
                yield m
            for m in s.readline(rc, 4):
                yield m
            retvalue.append(rc.data)
            for m in s.readline(rc, 3):
                yield m
            retvalue.append(rc.data)
            for m in s.readline(rc, 4):
                yield m
            retvalue.append(rc.data)
            for m in s.readline(rc, 4):
                yield m
            retvalue.append(rc.data)
            for m in s.readline(rc, 4):
                yield m
            retvalue.append(rc.data)
            for m in s.write(b'ijklm', rc, True):
                yield m
            for m in s.readline(rc, 4):
                yield m
            retvalue.append(rc.data)
            for m in s.readline(rc, 4):
                yield m
            retvalue.append(rc.data)
        rc.subroutine(write_routine())
        self.server.serve()
        self.assertEqual(retvalue, [b'abcd',b'ede', b'\n', b'f\rgh', b'\r\n', b'ijkl', b'm'])
    
    def testStream_5(self):
        s = Stream()
        retvalue = []
        rc = self.rc
        def write_routine():
            for m in s.write(b'abcde', rc):
                yield m
            for m in s.write(b'de\nf\rgh\r\n', rc, buffering=False):
                yield m
            for m in s.readline(rc, 4):
                yield m
            retvalue.append(rc.data)
            for m in s.readline(rc, 3):
                yield m
            retvalue.append(rc.data)
            for m in s.read(rc, 4):
                yield m
            retvalue.append(rc.data)
            for m in s.write(b'ijklm', rc, True):
                yield m
            for m in s.read(rc, 4):
                yield m
            retvalue.append(rc.data)
            for m in s.readline(rc, 4):
                yield m
            retvalue.append(rc.data)
            try:
                for m in s.readline(rc, 4):
                    yield m
                retvalue.append(rc.data)
            except EOFError:
                retvalue.append(None)
        rc.subroutine(write_routine())
        self.server.serve()
        self.assertEqual(retvalue, [b'abcd',b'ede', b'\nf\rg', b'h\r\ni', b'jklm', None])
    
    def testStream_6(self):
        s = Stream()
        retvalue = []
        rc = self.rc
        def write_routine():
            def read_next():
                data = s.readonce()
                while not data:
                    for m in s.prepareRead(rc):
                        yield m
                    data = s.readonce()
                rc.retvalue = data
            for m in s.write(b'abcde', rc, buffering=False):
                yield m
            for m in s.write(b'de\nf\rgh\r\n', rc, buffering=False):
                yield m
            for m in read_next():
                yield m
            retvalue.append(rc.retvalue)
            for m in read_next():
                yield m
            retvalue.append(rc.retvalue)
            for m in s.write(b'ijklm', rc, True):
                yield m            
            for m in read_next():
                yield m
            retvalue.append(rc.retvalue)
            try:
                for m in read_next():
                    yield m
                retvalue.append(rc.retvalue)
            except EOFError:
                retvalue.append(None)
        rc.subroutine(write_routine())
        self.server.serve()
        self.assertEqual(retvalue, [b'abcde', b'de\nf\rgh\r\n', b'ijklm', None])

    def testStream_7(self):
        s = Stream()
        retvalue = []
        rc = self.rc
        def write_routine():
            def read_next(size = None):
                data = s.readonce(size)
                while not data:
                    for m in s.prepareRead(rc):
                        yield m
                    data = s.readonce(size)
                rc.retvalue = data
            for m in s.write(b'abcde', rc, buffering=False):
                yield m
            for m in s.write(b'de\nf\rgh\r\n', rc, buffering=False):
                yield m
            for m in read_next(4):
                yield m
            retvalue.append(rc.retvalue)
            for m in read_next(4):
                yield m
            retvalue.append(rc.retvalue)
            for m in read_next(4):
                yield m
            retvalue.append(rc.retvalue)
            for m in read_next(4):
                yield m
            retvalue.append(rc.retvalue)
            for m in read_next(4):
                yield m
            retvalue.append(rc.retvalue)
            for m in s.write(b'ijklm', rc, True):
                yield m            
            for m in read_next(4):
                yield m
            retvalue.append(rc.retvalue)
            for m in read_next(4):
                yield m
            retvalue.append(rc.retvalue)
            try:
                for m in read_next(4):
                    yield m
                retvalue.append(rc.retvalue)
            except EOFError:
                retvalue.append(None)
        rc.subroutine(write_routine())
        self.server.serve()
        self.assertEqual(retvalue, [b'abcd', b'e',b'de\nf', b'\rgh\r', b'\n', b'ijkl', b'm', None])

    def testStream_8(self):
        s = Stream()
        s2 = Stream()
        retvalue = []
        rc = self.rc
        def write_routine():
            rc.subroutine(s2.copyTo(s, rc, False))
            def read_next(size = None):
                data = s.readonce(size)
                while not data:
                    for m in s.prepareRead(rc):
                        yield m
                    data = s.readonce(size)
                rc.retvalue = data
            for m in s2.write(b'abcde', rc, buffering=False):
                yield m
            for m in s2.write(b'de\nf\rgh\r\n', rc, buffering=False):
                yield m
            for m in read_next(4):
                yield m
            retvalue.append(rc.retvalue)
            for m in read_next(4):
                yield m
            retvalue.append(rc.retvalue)
            for m in read_next(4):
                yield m
            retvalue.append(rc.retvalue)
            for m in read_next(4):
                yield m
            retvalue.append(rc.retvalue)
            for m in read_next(4):
                yield m
            retvalue.append(rc.retvalue)
            for m in s2.write(b'ijklm', rc, True):
                yield m            
            for m in read_next(4):
                yield m
            retvalue.append(rc.retvalue)
            for m in read_next(4):
                yield m
            retvalue.append(rc.retvalue)
            try:
                for m in read_next(4):
                    yield m
                retvalue.append(rc.retvalue)
            except EOFError:
                retvalue.append(None)
        rc.subroutine(write_routine())
        self.server.serve()
        self.assertEqual(retvalue, [b'abcd', b'e',b'de\nf', b'\rgh\r', b'\n', b'ijkl', b'm', None])

    def testStream_9(self):
        s = Stream()
        s2 = Stream()
        retvalue = []
        rc = self.rc
        def write_routine():
            for m in s.write(b'abcde', rc):
                yield m
            for m in s.write(b'defgh', rc):
                yield m
            for m in s.write(b'ijklm', rc, True):
                yield m
        def read_routine():
            for m in s2.read(rc):
                yield m
            retvalue.append(rc.data)
        rc.subroutine(write_routine())
        rc.subroutine(read_routine())
        rc.subroutine(s.copyTo(s2, rc))
        self.server.serve()
        self.assertEqual(retvalue, [b'abcdedefghijklm'])

    def testStream_10(self):
        s = Stream(isunicode=True, encoders=[unicode_decoder('utf-8')])
        retvalue = []
        rc = self.rc
        def write_routine():
            for m in s.write(b'abcde', rc):
                yield m
            for m in s.write(b'defgh', rc):
                yield m
            for m in s.write(b'ijklm', rc, True):
                yield m
        def read_routine():
            for m in s.read(rc):
                yield m
            retvalue.append(rc.data)
        rc.subroutine(write_routine())
        rc.subroutine(read_routine())
        self.server.serve()
        self.assertEqual(retvalue, [u'abcdedefghijklm'])

    def testStream_11(self):
        s = Stream(encoders=[unicode_encoder('utf-8')])
        retvalue = []
        rc = self.rc
        def write_routine():
            for m in s.write(u'abcde', rc):
                yield m
            for m in s.write(u'defgh', rc):
                yield m
            for m in s.write(u'ijklm', rc, True):
                yield m
        def read_routine():
            for m in s.read(rc):
                yield m
            retvalue.append(rc.data)
        rc.subroutine(write_routine())
        rc.subroutine(read_routine())
        self.server.serve()
        self.assertEqual(retvalue, [b'abcdedefghijklm'])

    def testStream_12(self):
        s = Stream(isunicode=(str is not bytes), encoders=[str_decoder('utf-8')])
        retvalue = []
        rc = self.rc
        def write_routine():
            for m in s.write(b'abcde', rc):
                yield m
            for m in s.write(b'defgh', rc):
                yield m
            for m in s.write(b'ijklm', rc, True):
                yield m
        def read_routine():
            for m in s.read(rc):
                yield m
            retvalue.append(rc.data)
        rc.subroutine(write_routine())
        rc.subroutine(read_routine())
        self.server.serve()
        self.assertEqual(retvalue, ['abcdedefghijklm'])

    def testStream_13(self):
        s = Stream(encoders=[str_encoder('utf-8')])
        retvalue = []
        rc = self.rc
        def write_routine():
            for m in s.write('abcde', rc):
                yield m
            for m in s.write('defgh', rc):
                yield m
            for m in s.write('ijklm', rc, True):
                yield m
        def read_routine():
            for m in s.read(rc):
                yield m
            retvalue.append(rc.data)
        rc.subroutine(write_routine())
        rc.subroutine(read_routine())
        self.server.serve()
        self.assertEqual(retvalue, [b'abcdedefghijklm'])

    def testStream_14(self):
        s = Stream(encoders=[str_encoder('utf-8'), deflate_encoder()])
        s2 = Stream((str is not bytes), encoders=[deflate_decoder(), str_decoder('utf-8')])
        retvalue = []
        rc = self.rc
        def write_routine():
            for m in s.write('abcde', rc):
                yield m
            for m in s.write('defgh', rc):
                yield m
            for m in s.write('ijklm', rc, True):
                yield m
        def read_routine():
            for m in s2.read(rc):
                yield m
            retvalue.append(rc.data)
        rc.subroutine(write_routine())
        rc.subroutine(read_routine())
        rc.subroutine(s.copyTo(s2, rc))
        self.server.serve()
        self.assertEqual(retvalue, ['abcdedefghijklm'])
        
    def testStream_15(self):
        s = Stream(encoders=[str_encoder('utf-8'), gzip_encoder()])
        s2 = Stream((str is not bytes), encoders=[gzip_decoder(), str_decoder('utf-8')])
        retvalue = []
        rc = self.rc
        def write_routine():
            for m in s.write('abcde', rc):
                yield m
            for m in s.write('defgh', rc):
                yield m
            for m in s.write('ijklm', rc, True):
                yield m
        def read_routine():
            for m in s2.read(rc):
                yield m
            retvalue.append(rc.data)
        rc.subroutine(write_routine())
        rc.subroutine(read_routine())
        rc.subroutine(s.copyTo(s2, rc))
        self.server.serve()
        self.assertEqual(retvalue, ['abcdedefghijklm'])
        
    def testStream_16(self):
        s = Stream(splitsize=4)
        s2 = Stream(writebufferlimit=0)
        retvalue = []
        rc = self.rc
        def write_routine():
            rc.subroutine(s2.copyTo(s, rc, False))
            def read_next(size = None):
                data = s.readonce(size)
                while not data:
                    for m in s.prepareRead(rc):
                        yield m
                    data = s.readonce(size)
                rc.retvalue = data
            for m in s2.write(b'abcde', rc):
                yield m
            for m in s2.write(b'de\nf\rgh\r\n', rc):
                yield m
            for m in read_next():
                yield m
            retvalue.append(rc.retvalue)
            for m in read_next():
                yield m
            retvalue.append(rc.retvalue)
            for m in read_next():
                yield m
            retvalue.append(rc.retvalue)
            for m in read_next():
                yield m
            retvalue.append(rc.retvalue)
            for m in read_next():
                yield m
            retvalue.append(rc.retvalue)
            for m in s2.write(b'ijklm', rc, True):
                yield m            
            for m in read_next():
                yield m
            retvalue.append(rc.retvalue)
            for m in read_next():
                yield m
            retvalue.append(rc.retvalue)
            try:
                for m in read_next():
                    yield m
                retvalue.append(rc.retvalue)
            except EOFError:
                retvalue.append(None)
        rc.subroutine(write_routine())
        self.server.serve()
        self.assertEqual(retvalue, [b'abcd', b'e',b'de\nf', b'\rgh\r', b'\n', b'ijkl', b'm', None])

    def testStream_17(self):
        s = MemoryStream(b'abcdefg\nhijklmn\nopqrst\n')
        retvalue = []
        rc = self.rc
        def read_routine():
            for m in s.readline(rc):
                yield m
            retvalue.append(rc.data)
            retvalue.append(s.readonce())
        rc.subroutine(read_routine())
        self.server.serve()
        self.assertEqual(retvalue, [b'abcdefg\n', b'hijklmn\nopqrst\n'])

    def testStream_18(self):
        fio, tmp = tempfile.mkstemp()
        try:
            os.close(fio)
            s = FileWriter(open(tmp, 'wb'))
            retvalue = []
            rc = self.rc
            def write_routine():
                for m in s.write(b'abcde\n', rc):
                    yield m
                for m in s.write(b'defgh', rc):
                    yield m
                for m in s.write(b'ijklm', rc, True):
                    yield m
            rc.subroutine(write_routine())
            self.server.serve()
            s = FileStream(open(tmp, 'rb'))
            def read_routine():
                with closing(s):
                    for m in s.readline(rc):
                        yield m
                    retvalue.append(rc.data)
                    for m in s.read(rc):
                        yield m
                    retvalue.append(rc.data)
            rc.subroutine(read_routine())
            self.server.serve()
            self.assertEqual(retvalue, [b'abcde\n', b'defghijklm'])
        finally:
            os.remove(tmp)



if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
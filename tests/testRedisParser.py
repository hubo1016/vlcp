'''
Created on 2016/1/7

:author: hubo
'''
from __future__ import print_function
import unittest
#from vlcp.protocol.redis import RedisParser
from timeit import timeit
from hiredis import Reader as RedisParser

class Test(unittest.TestCase):
    def testParser(self):
        p = RedisParser()
        p.feed(b'+OK\r\n-ERR test\r\n:18\r\n$5\r\nabcde\r\n*3\r\n+OK\r\n-ERR test2\r\n$6\r\na\r\nb\r\n\r\n')
        self.assertEqual(p.gets(), b'OK')
        e = p.gets()
        self.assertTrue(isinstance(e, Exception) and str(e) == 'ERR test')
        self.assertEqual(p.gets(), 18)
        self.assertEqual(p.gets(), b'abcde')
        a = p.gets()
        self.assertTrue(isinstance(a, list))
        self.assertEqual(len(a), 3)
        self.assertEqual(a[0], b'OK')
        self.assertTrue(isinstance(a[1], Exception) and str(a[1]) == 'ERR test2')
        self.assertEqual(a[2], b'a\r\nb\r\n')
        self.assertEqual(p.gets(), False)
    def testException(self):
        p = RedisParser()
        p.feed(b':abc\r\n')
        try:
            p.gets()
            self.assertTrue(False)
        except:
            pass
    def testContinue(self):
        p = RedisParser()
        p.feed(b'$5\r\nabcde\r')
        self.assertEqual(p.gets(), False)
        p.feed(b'\n:5\r')
        self.assertEqual(p.gets(), b'abcde')
        self.assertEqual(p.gets(), False)
        p.feed(b'\n*2\r\n+O')
        self.assertEqual(p.gets(), 5)
        self.assertEqual(p.gets(), False)
        p.feed(b'K\r\n')
        self.assertEqual(p.gets(), False)
        p.feed(b'*2\r\n:3\r\n$4\r\nab\r\n')
        self.assertEqual(p.gets(), False)
        p.feed(b'\r\n')
        self.assertEqual(p.gets(), [b'OK', [3, b'ab\r\n']])
        self.assertEqual(p.gets(), False)
    def testNone(self):
        p = RedisParser()
        p.feed(b'$-1\r\n')
        self.assertEqual(p.gets(), None)
        self.assertEqual(p.gets(), False)
        p.feed(b'$0\r\n')
        self.assertEqual(p.gets(), False)
        p.feed(b'\r\n')
        self.assertEqual(p.gets(), b'')
        self.assertEqual(p.gets(), False)
        p.feed(b'+\r\n')
        self.assertEqual(p.gets(), b'')
        self.assertEqual(p.gets(), False)
        p.feed(b'*0\r\n')
        self.assertEqual(p.gets(), [])
        self.assertEqual(p.gets(), False)
    def testNested(self):
        p = RedisParser()
        p.feed(b'*5\r\n*2\r\n+OK\r\n:3\r\n-ERR test\r\n*2\r\n$20\r\n*2\r\n+OK\r\n$5\r\nabcde\r\n\r\n:-999999\r\n*1\r\n*1\r\n*1\r\n*1\r\n+OK\r\n$-1\r\n')
        a = p.gets()
        self.assertTrue(isinstance(a, list))
        self.assertEqual(len(a), 5)
        self.assertEqual(a[0], [b'OK', 3])
        self.assertTrue(isinstance(a[1], Exception) and str(a[1]) == 'ERR test')
        self.assertEqual(a[2], [b'*2\r\n+OK\r\n$5\r\nabcde\r\n', -999999])
        self.assertEqual(a[3], [[[[b'OK']]]])
        self.assertEqual(a[4], None)
        self.assertEqual(p.gets(), False)
    def testNested2(self):
        data = b'*5\r\n*2\r\n+OK\r\n:3\r\n-ERR test\r\n*2\r\n$20\r\n*2\r\n+OK\r\n$5\r\nabcde\r\n\r\n:-999999\r\n*1\r\n*1\r\n*1\r\n*1\r\n+OK\r\n$-1\r\n'
        for i in range(1, len(data)):
            p = RedisParser()
            p.feed(data[:i])
            self.assertEqual(p.gets(), False)
            p.feed(data[i:])
            a = p.gets()
            self.assertTrue(isinstance(a, list))
            self.assertEqual(len(a), 5)
            self.assertEqual(a[0], [b'OK', 3])
            self.assertTrue(isinstance(a[1], Exception) and str(a[1]) == 'ERR test')
            self.assertEqual(a[2], [b'*2\r\n+OK\r\n$5\r\nabcde\r\n', -999999])
            self.assertEqual(a[3], [[[[b'OK']]]])
            self.assertEqual(a[4], None)
            self.assertEqual(p.gets(), False)
    def testMaxInt(self):
        p = RedisParser()
        p.feed(b':9223372036854775807\r\n:-9223372036854775808\r\n')
        self.assertEqual(p.gets(), 9223372036854775807)
        self.assertEqual(p.gets(), -9223372036854775808)
        self.assertEqual(p.gets(), False)
    def testTime(self):
        p = RedisParser()
        def test():
            p.feed(b'*5\r\n*2\r\n+OK\r\n:3\r\n-ERR test\r\n*2\r\n$20\r\n*2\r\n+OK\r\n$5\r\nabcde\r\n\r\n:-999999\r\n*1\r\n*1\r\n*1\r\n*1\r\n+OK\r\n$-1\r\n')
            p.gets()
        print(timeit(test, number=10000))

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
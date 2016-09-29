'''
Created on 2016/09/28

:author: hubo
'''
from __future__ import print_function
import unittest
from vlcp.server.server import Server
from vlcp.event.runnable import RoutineContainer
from vlcp.event.future import Future, RoutineFuture
from vlcp.config.config import manager


class Test(unittest.TestCase):
    def setUp(self):
        self.server = Server()
        self.rc = RoutineContainer(self.server.scheduler)
    def tearDown(self):
        pass
    
    def test_future1(self):
        rc = self.rc
        future = Future(self.server.scheduler)
        # Test send before wait
        obj = [0]
        def routine_sender():
            future.set_result('abc')
            if False:
                yield
        def routine_receiver():
            for m in rc.waitWithTimeout(0.1):
                yield m
            for m in future.wait(rc):
                yield m
            obj[0] = rc.retvalue
        rc.subroutine(routine_sender())
        rc.subroutine(routine_receiver())
        self.server.serve()
        self.assertEqual(obj[0], 'abc')

    def test_future2(self):
        rc = self.rc
        future = Future(self.server.scheduler)
        # Test send after wait
        obj = [0]
        def routine_sender():
            for m in rc.waitWithTimeout(0.1):
                yield m
            future.set_result('abc')
        def routine_receiver():
            for m in future.wait(rc):
                yield m
            obj[0] = rc.retvalue
        rc.subroutine(routine_sender())
        rc.subroutine(routine_receiver())
        self.server.serve()
        self.assertEqual(obj[0], 'abc')

    def test_future_nowait1(self):
        rc = self.rc
        future = Future(self.server.scheduler)
        # Test send before wait
        obj = [None, None]
        def routine_sender():
            future.set_result('abc')
            if False:
                yield
        def routine_receiver():
            for m in rc.waitWithTimeout(0.1):
                yield m
            obj[0] = future.done()
            obj[1] = future.result()
        rc.subroutine(routine_sender())
        rc.subroutine(routine_receiver())
        self.server.serve()
        self.assertEqual(obj, [True, 'abc'])

    def test_future_nowait2(self):
        rc = self.rc
        future = Future(self.server.scheduler)
        # Test send before wait
        obj = [None, None]
        def routine_sender():
            for m in rc.waitWithTimeout(0.1):
                yield m
            future.set_result('abc')
        def routine_receiver():
            obj[0] = future.done()
            obj[1] = future.result()
            if False:
                yield
        rc.subroutine(routine_sender())
        rc.subroutine(routine_receiver())
        self.server.serve()
        self.assertEqual(obj, [False, None])

    def test_future_exception1(self):
        rc = self.rc
        future = Future(self.server.scheduler)
        # Test send before wait
        obj = [0]
        def routine_sender():
            future.set_exception(ValueError('abc'))
            if False:
                yield
        def routine_receiver():
            for m in rc.waitWithTimeout(0.1):
                yield m
            try:
                for m in future.wait(rc):
                    yield m
            except ValueError as exc:
                obj[0] = str(exc)
            else:
                obj[0] = None
        rc.subroutine(routine_sender())
        rc.subroutine(routine_receiver())
        self.server.serve()
        self.assertEqual(obj[0], 'abc')

    def test_future_exception2(self):
        rc = self.rc
        future = Future(self.server.scheduler)
        # Test send before wait
        obj = [0]
        def routine_sender():
            for m in rc.waitWithTimeout(0.1):
                yield m
            future.set_exception(ValueError('abc'))
        def routine_receiver():
            try:
                for m in future.wait(rc):
                    yield m
            except ValueError as exc:
                obj[0] = str(exc)
            else:
                obj[0] = None
        rc.subroutine(routine_sender())
        rc.subroutine(routine_receiver())
        self.server.serve()
        self.assertEqual(obj[0], 'abc')

    def test_future_nowait_exception1(self):
        rc = self.rc
        future = Future(self.server.scheduler)
        # Test send before wait
        obj = [None, None]
        def routine_sender():
            future.set_exception(ValueError('abc'))
            if False:
                yield
        def routine_receiver():
            for m in rc.waitWithTimeout(0.1):
                yield m
            try:
                obj[0] = future.done()
                obj[1] = future.result()
            except ValueError as exc:
                obj[1] = str(exc)
        rc.subroutine(routine_sender())
        rc.subroutine(routine_receiver())
        self.server.serve()
        self.assertEqual(obj, [True, 'abc'])

    def test_future_nowait_exception2(self):
        rc = self.rc
        future = Future(self.server.scheduler)
        # Test send before wait
        obj = [None, None]
        def routine_sender():
            for m in rc.waitWithTimeout(0.1):
                yield m
            future.set_exception(ValueError('abc'))
        def routine_receiver():
            try:
                obj[0] = future.done()
                obj[1] = future.result()
            except ValueError as exc:
                obj[1] = str(exc)
            if False:
                yield
        rc.subroutine(routine_sender())
        rc.subroutine(routine_receiver())
        self.server.serve()
        self.assertEqual(obj, [False, None])
    
    def test_ensure_result(self):
        rc = self.rc
        future = Future(self.server.scheduler)
        # Test send before wait
        obj = [0]
        def routine_sender():
            with future.ensure_result():
                future.set_result('abc')
            if False:
                yield
        def routine_receiver():
            for m in rc.waitWithTimeout(0.1):
                yield m
            for m in future.wait(rc):
                yield m
            obj[0] = rc.retvalue
        rc.subroutine(routine_sender())
        rc.subroutine(routine_receiver())
        self.server.serve()
        self.assertEqual(obj[0], 'abc')

    def test_ensure_result2(self):
        rc = self.rc
        future = Future(self.server.scheduler)
        # Test send before wait
        obj = [0]
        obj2 = [0]
        def routine_sender():
            try:
                with future.ensure_result():
                    raise ValueError('abc')
            except ValueError:
                obj2[0] = True
            else:
                obj2[0] = False
            if False:
                yield
        def routine_receiver():
            for m in rc.waitWithTimeout(0.1):
                yield m
            try:
                for m in future.wait(rc):
                    yield m
            except ValueError as exc:
                obj[0] = str(exc)
            else:
                obj[0] = None
        rc.subroutine(routine_sender())
        rc.subroutine(routine_receiver())
        self.server.serve()
        self.assertEqual(obj[0], 'abc')
        self.assertEqual(obj2[0], True)

    def test_ensure_result3(self):
        rc = self.rc
        future = Future(self.server.scheduler)
        # Test send before wait
        obj = [0]
        obj2 = [0]
        def routine_sender():
            try:
                with future.ensure_result(True):
                    raise ValueError('abc')
            except ValueError:
                obj2[0] = True
            else:
                obj2[0] = False
            if False:
                yield
        def routine_receiver():
            for m in rc.waitWithTimeout(0.1):
                yield m
            try:
                for m in future.wait(rc):
                    yield m
            except ValueError as exc:
                obj[0] = str(exc)
            else:
                obj[0] = None
        rc.subroutine(routine_sender())
        rc.subroutine(routine_receiver())
        self.server.serve()
        self.assertEqual(obj[0], 'abc')
        self.assertEqual(obj2[0], False)

    def test_ensure_result4(self):
        rc = self.rc
        future = Future(self.server.scheduler)
        # Test send before wait
        obj = [0]
        obj2 = [0]
        def routine_sender():
            try:
                with future.ensure_result(False, 'abc'):
                    pass
            except ValueError:
                obj2[0] = True
            else:
                obj2[0] = False
            if False:
                yield
        def routine_receiver():
            for m in rc.waitWithTimeout(0.1):
                yield m
            for m in future.wait(rc):
                yield m
            obj[0] = rc.retvalue
        rc.subroutine(routine_sender())
        rc.subroutine(routine_receiver())
        self.server.serve()
        self.assertEqual(obj[0], 'abc')
        self.assertEqual(obj2[0], False)

    def test_routine1(self):
        rc = self.rc
        # Test send before wait
        obj = [0]
        def routine_sender():
            rc.retvalue = 'abc'
            if False:
                yield
        future = RoutineFuture(routine_sender(), rc)
        def routine_receiver():
            for m in rc.waitWithTimeout(0.1):
                yield m
            for m in future.wait(rc):
                yield m
            obj[0] = rc.retvalue
        rc.subroutine(routine_receiver())
        self.server.serve()
        self.assertEqual(obj[0], 'abc')

    def test_routine2(self):
        rc = self.rc
        # Test send before wait
        obj = [0]
        def routine_sender():
            for m in rc.waitWithTimeout(0.1):
                yield m
            rc.retvalue = 'abc'
        future = RoutineFuture(routine_sender(), rc)
        def routine_receiver():
            for m in future.wait(rc):
                yield m
            obj[0] = rc.retvalue
        rc.subroutine(routine_receiver())
        self.server.serve()
        self.assertEqual(obj[0], 'abc')

    def test_routine_exception1(self):
        rc = self.rc
        # Test send before wait
        obj = [0]
        def routine_sender():
            raise ValueError('abc')
            if False:
                yield
        future = RoutineFuture(routine_sender(), rc)
        def routine_receiver():
            for m in rc.waitWithTimeout(0.1):
                yield m
            try:
                for m in future.wait(rc):
                    yield m
            except ValueError as exc:
                obj[0] = str(exc)
            else:
                obj[0] = None
        rc.subroutine(routine_receiver())
        self.server.serve()
        self.assertEqual(obj[0], 'abc')

    def test_routine_exception2(self):
        rc = self.rc
        # Test send before wait
        obj = [0]
        def routine_sender():
            for m in rc.waitWithTimeout(0.1):
                yield m
            raise ValueError('abc')
        future = RoutineFuture(routine_sender(), rc)
        def routine_receiver():
            try:
                for m in future.wait(rc):
                    yield m
            except ValueError as exc:
                obj[0] = str(exc)
            else:
                obj[0] = None
        rc.subroutine(routine_receiver())
        self.server.serve()
        self.assertEqual(obj[0], 'abc')


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
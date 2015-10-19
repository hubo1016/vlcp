'''
Created on 2015/10/14

@author: hubo
'''
import unittest
from server import Server
from server.module import callAPI, ModuleLoadStateChanged
import logging
import os.path
from event.runnable import RoutineContainer
from config import manager
from utils.pycache import removeCache

try:
    reload
except:
    from utils.pycache import reload

module1 = b'''
from server.module import Module, api
from config import defaultconfig
from event import Event, withIndices
from event.runnable import RoutineContainer

@withIndices()
class ModuleTestEvent(Event):
    pass

@defaultconfig
class TestModule1(Module):
    class MyHandler(RoutineContainer):
        def method2(self, a, b):
            self.retvalue = a + b
            if False:
                yield
        def method3(self, a, b):
            for m in self.waitForSend(ModuleTestEvent(a = a, b = b)):
                yield m
            self.retvalue = None
    def __init__(self, server):
        Module.__init__(self, server)
        self.handlerRoutine = self.MyHandler(self.scheduler)
        self.createAPI((api(self.method1),
                api(self.handlerRoutine.method2,self.handlerRoutine),
                api(self.handlerRoutine.method3,self.handlerRoutine),
                api(self.method4)
                ))
    def method1(self):
        return 'version1'
    def method4(self):
        raise ValueError('test')
'''

module2 = b'''
from server.module import Module, api, depend
from config import defaultconfig
from event import Event, withIndices
from event.runnable import RoutineContainer
from . import testmodule1

@withIndices()
class ModuleTestEvent2(Event):
    pass

@defaultconfig
@depend(testmodule1.TestModule1)
class TestModule2(Module):
    class MyHandler(RoutineContainer):
        def main(self):
            matcher = testmodule1.ModuleTestEvent.createMatcher()
            while True:
                yield (matcher,)
                self.subroutine(self.waitForSend(ModuleTestEvent2(result=self.event.a + self.event.b, version = 'version1')), False)
    def __init__(self, server):
        Module.__init__(self, server)
        self.routines.append(self.MyHandler(self.scheduler))
'''

module1v2 = b'''
from server.module import Module, api
from config import defaultconfig
from event import Event, withIndices
from event.runnable import RoutineContainer

@withIndices()
class ModuleTestEvent(Event):
    pass

@defaultconfig
class TestModule1(Module):
    class MyHandler(RoutineContainer):
        def method2(self, a, b):
            self.retvalue = a + b
            if False:
                yield
        def method3(self, a, b):
            for m in self.waitForSend(ModuleTestEvent(a = a, b = b)):
                yield m
            self.retvalue = None
    def __init__(self, server):
        Module.__init__(self, server)
        self.handlerRoutine = self.MyHandler(self.scheduler)
        self.createAPI((api(self.method1),
                api(self.handlerRoutine.method2,self.handlerRoutine),
                api(self.handlerRoutine.method3,self.handlerRoutine),
                api(self.method4)
                ))
    def method1(self):
        return 'version2'
    def method4(self):
        raise ValueError('test')
'''

module2v2 = b'''
from server.module import Module, api, depend
from config import defaultconfig
from event import Event, withIndices
from event.runnable import RoutineContainer
from . import testmodule1

@withIndices()
class ModuleTestEvent2(Event):
    pass

@defaultconfig
@depend(testmodule1.TestModule1)
class TestModule2(Module):
    class MyHandler(RoutineContainer):
        def main(self):
            matcher = testmodule1.ModuleTestEvent.createMatcher()
            while True:
                yield (matcher,)
                self.subroutine(self.waitForSend(ModuleTestEvent2(result=self.event.a + self.event.b, version = 'version2')), False)
    def __init__(self, server):
        Module.__init__(self, server)
        self.routines.append(self.MyHandler(self.scheduler))
'''


module1v3 = b'''
from server.module import Module, api
from config import defaultconfig
from event import Event, withIndices
from event.runnable import RoutineContainer

@withIndices()
class ModuleTestEvent(Event):
    pass

@defaultconfig
class TestModule1(Module):
    class MyHandler(RoutineContainer):
        def method2(self, a, b):
            self.retvalue = a + b
            if False:
                yield
        def method3(self, a, b):
            for m in self.waitForSend(ModuleTestEvent(a = a, b = b)):
                yield m
            self.retvalue = None
    def __init__(self, server):
        Module.__init__(self, server)
        self.handlerRoutine = self.MyHandler(self.scheduler)
        self.createAPI((api(self.method1),
                api(self.handlerRoutine.method2,self.handlerRoutine),
                api(self.handlerRoutine.method3,self.handlerRoutine),
                api(self.method4)
                ))
    def method1(self):
        return 'version3'
    def method4(self):
        raise ValueError('test')
'''

module2v3 = b'''
from server.module import Module, api, depend
from config import defaultconfig
from event import Event, withIndices
from event.runnable import RoutineContainer
from . import testmodule1

@withIndices()
class ModuleTestEvent2(Event):
    pass

@defaultconfig
@depend(testmodule1.TestModule1)
class TestModule2(Module):
    class MyHandler(RoutineContainer):
        def main(self):
            matcher = testmodule1.ModuleTestEvent.createMatcher()
            while True:
                yield (matcher,)
                self.subroutine(self.waitForSend(ModuleTestEvent2(result=self.event.a + self.event.b, version = 'version3')), False)
    def __init__(self, server):
        Module.__init__(self, server)
        self.routines.append(self.MyHandler(self.scheduler))
'''



class Test(unittest.TestCase):
    def testModuleLoad(self):
        logging.basicConfig()
        manager['server.startup'] = ('tests.gensrc.testmodule1.TestModule1', 'tests.gensrc.testmodule2.TestModule2')
        s = Server()
        import tests.gensrc
        basedir = tests.gensrc.__path__[0]
        with open(os.path.join(basedir, 'testmodule1.py'), 'wb') as f:
            f.write(module1)
        with open(os.path.join(basedir, 'testmodule2.py'), 'wb') as f:
            f.write(module2)
        # Run unittest discover may already load the module, reload it
        import tests.gensrc.testmodule1
        import tests.gensrc.testmodule2
        removeCache(tests.gensrc.testmodule1)
        removeCache(tests.gensrc.testmodule2)
        reload(tests.gensrc.testmodule1)
        reload(tests.gensrc.testmodule2)
        # Sometimes the timestamp is not working, make sure python re-compile the source file
        r = RoutineContainer(s.scheduler)
        apiResults = []
        def testproc():
            yield (ModuleLoadStateChanged.createMatcher(),)
            for m in callAPI(r, "testmodule1", "method1", {}):
                yield m
            apiResults.append(r.retvalue)
            for m in callAPI(r, "testmodule1", "method2", {'a' : 1, 'b' : 2}):
                yield m
            apiResults.append(r.retvalue)
            try:
                for m in callAPI(r, "testmodule1", "method4", {}):
                    yield m
                apiResults.append(None)
            except ValueError as exc:
                apiResults.append(exc.args[0])
            from .gensrc.testmodule2 import ModuleTestEvent2
            matcher = ModuleTestEvent2.createMatcher()
            self.event = False
            def proc2():            
                for m in callAPI(r, "testmodule1", "method3", {'a' : 1, 'b' : 2}):
                    yield m
            def callback(event, matcher):
                self.event = event
                if False:
                    yield
            for m in r.withCallback(proc2(), callback, matcher):
                yield m
            if not self.event:
                for m in r.waitWithTimeout(0.1, matcher):
                    yield m
                if not r.timeout:
                    self.event = r.event
            if self.event:
                apiResults.append((self.event.result, self.event.version))
            else:
                apiResults.append(False)
            with open(os.path.join(basedir, 'testmodule1.py'), 'wb') as f:
                f.write(module1v2)
            for m in s.moduleloader.delegate(s.moduleloader.reloadModules(['tests.gensrc.testmodule1.TestModule1'])):
                yield m
            for m in callAPI(r, "testmodule1", "method1", {}):
                yield m
            apiResults.append(r.retvalue)
            matcher = ModuleTestEvent2.createMatcher()
            self.event = False
            def proc2_2():
                for m in callAPI(r, "testmodule1", "method3", {'a' : 1, 'b' : 2}):
                    yield m
            def callback_2(event, matcher):
                self.event = event
                if False:
                    yield
            for m in r.withCallback(proc2_2(), callback_2, matcher):
                yield m
            if not self.event:
                for m in r.waitWithTimeout(0.1, matcher):
                    yield m
                if not r.timeout:
                    self.event = r.event
            if self.event:
                apiResults.append((self.event.result, self.event.version))
            else:
                apiResults.append(False)
            with open(os.path.join(basedir, 'testmodule2.py'), 'wb') as f:
                f.write(module2v2)
            for m in s.moduleloader.delegate(s.moduleloader.reloadModules(['tests.gensrc.testmodule2.TestModule2'])):
                yield m
            matcher = ModuleTestEvent2.createMatcher()
            self.event = False
            def proc2_3():
                for m in callAPI(r, "testmodule1", "method3", {'a' : 1, 'b' : 2}):
                    yield m
            def callback_3(event, matcher):
                self.event = event
                if False:
                    yield
            for m in r.withCallback(proc2_3(), callback_3, matcher):
                yield m
            if not self.event:
                for m in r.waitWithTimeout(0.1, matcher):
                    yield m
                if not r.timeout:
                    self.event = r.event
            if self.event:
                apiResults.append((self.event.result, self.event.version))
            else:
                apiResults.append(False)
            with open(os.path.join(basedir, 'testmodule1.py'), 'wb') as f:
                f.write(module1v3)
            with open(os.path.join(basedir, 'testmodule2.py'), 'wb') as f:
                f.write(module2v3)
            for m in s.moduleloader.delegate(s.moduleloader.reloadModules(['tests.gensrc.testmodule1.TestModule1','tests.gensrc.testmodule2.TestModule2'])):
                yield m
            for m in callAPI(r, "testmodule1", "method1", {}):
                yield m
            apiResults.append(r.retvalue)
            matcher = ModuleTestEvent2.createMatcher()
            self.event = False
            def proc2_4():
                for m in callAPI(r, "testmodule1", "method3", {'a' : 1, 'b' : 2}):
                    yield m
            def callback_4(event, matcher):
                self.event = event
                if False:
                    yield
            for m in r.withCallback(proc2_4(), callback_4, matcher):
                yield m
            if not self.event:
                for m in r.waitWithTimeout(0.1, matcher):
                    yield m
                if not r.timeout:
                    self.event = r.event
            if self.event:
                apiResults.append((self.event.result, self.event.version))
            else:
                apiResults.append(False)
            for m in s.moduleloader.delegate(s.moduleloader.unloadByPath("tests.gensrc.testmodule1.TestModule1")):
                yield m
        r.main = testproc
        r.start()
        s.serve()
        self.assertEqual(apiResults, ['version1', 3, 'test', (3, 'version1'), 'version2', (3, 'version1'), (3, 'version2'), 'version3', (3, 'version3')])


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
'''
Created on 2015/10/14

:author: hubo
'''
import unittest
from vlcp.server import Server
from vlcp.server.module import call_api, ModuleLoadStateChanged
import logging
import os.path
from vlcp.event.runnable import RoutineContainer
from vlcp.config import manager
from vlcp.utils.pycache import remove_cache
import traceback
from vlcp.utils.exceptions import APIRejectedException

try:
    reload
except Exception:
    from vlcp.utils.pycache import reload

module1 = b'''
from vlcp.server.module import Module, api
from vlcp.config import defaultconfig
from vlcp.event import Event, withIndices
from vlcp.event.runnable import RoutineContainer

@withIndices()
class ModuleTestEvent(Event):
    pass

@defaultconfig
class TestModule1(Module):
    class MyHandler(RoutineContainer):
        async def method2(self, a, b):
            "Run method2"
            return a + b
        async def method3(self, a, b):
            "Run method3"
            await self.wait_for_send(ModuleTestEvent(a = a, b = b))
    def __init__(self, server):
        Module.__init__(self, server)
        self.handlerRoutine = self.MyHandler(self.scheduler)
        self.createAPI(api(self.method1),
                api(self.handlerRoutine.method2,self.handlerRoutine),
                api(self.handlerRoutine.method3,self.handlerRoutine),
                api(self.method4)
                )
    def method1(self):
        "Run method1"
        return 'version1'
    def method4(self):
        "Run method4"
        raise ValueError('test')
'''

module2 = b'''
from vlcp.server.module import Module, api, depend
from vlcp.config import defaultconfig
from vlcp.event import Event, withIndices
from vlcp.event.runnable import RoutineContainer
from . import testmodule1

@withIndices()
class ModuleTestEvent2(Event):
    pass

@defaultconfig
@depend(testmodule1.TestModule1)
class TestModule2(Module):
    class MyHandler(RoutineContainer):
        async def main(self):
            matcher = testmodule1.ModuleTestEvent.createMatcher()
            while True:
                ev = await matcher
                self.subroutine(self.wait_for_send(ModuleTestEvent2(result=ev.a + ev.b, version = 'version1')), False)
    def __init__(self, server):
        Module.__init__(self, server)
        self.routines.append(self.MyHandler(self.scheduler))
'''

module1v2 = b'''
from vlcp.server.module import Module, api
from vlcp.config import defaultconfig
from vlcp.event import Event, withIndices
from vlcp.event.runnable import RoutineContainer

@withIndices()
class ModuleTestEvent(Event):
    pass

@defaultconfig
class TestModule1(Module):
    class MyHandler(RoutineContainer):
        async def method2(self, a, b):
            return a + b
        async def method3(self, a, b):
            await self.wait_for_send(ModuleTestEvent(a = a, b = b))
    def __init__(self, server):
        Module.__init__(self, server)
        self.handlerRoutine = self.MyHandler(self.scheduler)
        self.createAPI(api(self.method1),
                api(self.handlerRoutine.method2,self.handlerRoutine),
                api(self.handlerRoutine.method3,self.handlerRoutine),
                api(self.method4)
                )
    def method1(self):
        return 'version2'
    def method4(self):
        raise ValueError('test')
'''

module2v2 = b'''
from vlcp.server.module import Module, api, depend
from vlcp.config import defaultconfig
from vlcp.event import Event, withIndices
from vlcp.event.runnable import RoutineContainer
from . import testmodule1

@withIndices()
class ModuleTestEvent2(Event):
    pass

@defaultconfig
@depend(testmodule1.TestModule1)
class TestModule2(Module):
    class MyHandler(RoutineContainer):
        async def main(self):
            matcher = testmodule1.ModuleTestEvent.createMatcher()
            while True:
                ev = await matcher
                self.subroutine(self.wait_for_send(ModuleTestEvent2(result=ev.a + ev.b, version = 'version2')), False)
    def __init__(self, server):
        Module.__init__(self, server)
        self.routines.append(self.MyHandler(self.scheduler))
'''


module1v3 = b'''
from vlcp.server.module import Module, api
from vlcp.config import defaultconfig
from vlcp.event import Event, withIndices
from vlcp.event.runnable import RoutineContainer

@withIndices()
class ModuleTestEvent(Event):
    pass

@defaultconfig
class TestModule1(Module):
    class MyHandler(RoutineContainer):
        async def method2(self, a, b):
            return a + b
        async def method3(self, a, b):
            await self.wait_for_send(ModuleTestEvent(a = a, b = b))
    def __init__(self, server):
        Module.__init__(self, server)
        self.handlerRoutine = self.MyHandler(self.scheduler)
        self.createAPI(api(self.method1),
                api(self.handlerRoutine.method2,self.handlerRoutine),
                api(self.handlerRoutine.method3,self.handlerRoutine),
                api(self.method4)
                )
    def method1(self):
        return 'version3'
    def method4(self):
        raise ValueError('test')
'''

module2v3 = b'''
from vlcp.server.module import Module, api, depend
from vlcp.config import defaultconfig
from vlcp.event import Event, withIndices
from vlcp.event.runnable import RoutineContainer
from . import testmodule1

@withIndices()
class ModuleTestEvent2(Event):
    pass

@defaultconfig
@depend(testmodule1.TestModule1)
class TestModule2(Module):
    class MyHandler(RoutineContainer):
        async def main(self):
            matcher = testmodule1.ModuleTestEvent.createMatcher()
            while True:
                ev = await matcher
                self.subroutine(self.wait_for_send(ModuleTestEvent2(result=ev.a + ev.b, version = 'version3')), False)
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
        remove_cache(tests.gensrc.testmodule1)
        remove_cache(tests.gensrc.testmodule2)
        reload(tests.gensrc.testmodule1)
        reload(tests.gensrc.testmodule2)
        # Sometimes the timestamp is not working, make sure python re-compile the source file
        r = RoutineContainer(s.scheduler)
        apiResults = []
        async def testproc():
            await ModuleLoadStateChanged.createMatcher()
            apiResults.append(await call_api(r, "testmodule1", "method1", {}))
            apiResults.append(await call_api(r, "testmodule1", "method2", {'a' : 1, 'b' : 2}))
            try:
                await call_api(r, "testmodule1", "method4", {})
                apiResults.append(None)
            except ValueError as exc:
                apiResults.append(exc.args[0])
            from .gensrc.testmodule2 import ModuleTestEvent2
            matcher = ModuleTestEvent2.createMatcher()
            self.event = False
            async def proc2():
                return await call_api(r, "testmodule1", "method3", {'a' : 1, 'b' : 2})
            def callback(event, matcher):
                self.event = event
            await r.with_callback(proc2(), callback, matcher)
            if not self.event:
                timeout, ev, m = await r.wait_with_timeout(0.1, matcher)
                if not timeout:
                    self.event = ev
            if self.event:
                apiResults.append((self.event.result, self.event.version))
            else:
                apiResults.append(False)
            apiResults.append(await call_api(r, "testmodule1", "discover", {}))
            with open(os.path.join(basedir, 'testmodule1.py'), 'wb') as f:
                f.write(module1v2)
            await s.moduleloader.reload_modules(['tests.gensrc.testmodule1.TestModule1'])
            apiResults.append(await call_api(r, "testmodule1", "method1", {}))
            matcher = ModuleTestEvent2.createMatcher()
            self.event = False
            async def proc2_2():
                return await call_api(r, "testmodule1", "method3", {'a' : 1, 'b' : 2})
            def callback_2(event, matcher):
                self.event = event
            await r.with_callback(proc2_2(), callback_2, matcher)
            if not self.event:
                timeout, ev, m = await r.wait_with_timeout(0.1, matcher)
                if not timeout:
                    self.event = ev
            if self.event:
                apiResults.append((self.event.result, self.event.version))
            else:
                apiResults.append(False)
            with open(os.path.join(basedir, 'testmodule2.py'), 'wb') as f:
                f.write(module2v2)
            await s.moduleloader.reload_modules(['tests.gensrc.testmodule2.TestModule2'])
            matcher = ModuleTestEvent2.createMatcher()
            self.event = False
            async def proc2_3():
                return await call_api(r, "testmodule1", "method3", {'a' : 1, 'b' : 2})
            def callback_3(event, matcher):
                self.event = event
            await r.with_callback(proc2_3(), callback_3, matcher)
            if not self.event:
                timeout, ev, m = await r.wait_with_timeout(0.1, matcher)
                if not timeout:
                    self.event = ev
            if self.event:
                apiResults.append((self.event.result, self.event.version))
            else:
                apiResults.append(False)
            with open(os.path.join(basedir, 'testmodule1.py'), 'wb') as f:
                f.write(module1v3)
            with open(os.path.join(basedir, 'testmodule2.py'), 'wb') as f:
                f.write(module2v3)
            await s.moduleloader.reload_modules(['tests.gensrc.testmodule1.TestModule1','tests.gensrc.testmodule2.TestModule2'])
            apiResults.append(await call_api(r, "testmodule1", "method1", {}))
            matcher = ModuleTestEvent2.createMatcher()
            self.event = False
            async def proc2_4():
                return await call_api(r, "testmodule1", "method3", {'a' : 1, 'b' : 2})
            def callback_4(event, matcher):
                self.event = event
            await r.with_callback(proc2_4(), callback_4, matcher)
            if not self.event:
                timeout, ev, m = await r.wait_with_timeout(0.1, matcher)
                if not timeout:
                    self.event = ev
            if self.event:
                apiResults.append((self.event.result, self.event.version))
            else:
                apiResults.append(False)
            try:
                await r.execute_with_timeout(1.0, call_api(r, "testmodule1", "notexists", {}))
            except APIRejectedException:
                apiResults.append(True)
            except Exception:
                apiResults.append(False)
            else:
                apiResults.append(False)
            await s.moduleloader.unload_by_path("tests.gensrc.testmodule1.TestModule1")
        r.main = testproc
        r.start()
        s.serve()
        print(repr(apiResults))
        self.assertEqual(apiResults, ['version1', 3, 'test', (3, 'version1'),
                                      {'method1':'Run method1', 'method2':'Run method2', 'method3':'Run method3', 'method4': 'Run method4', 'discover':'Discover API definitions. Set details=true to show details'},
                                      'version2', (3, 'version1'), (3, 'version2'), 'version3', (3, 'version3'), True])


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
'''
Created on 2015/6/29

:author: hubo
'''
from __future__ import print_function
import unittest
from vlcp.event.event import Event, withIndices
from vlcp.event.core import Scheduler, PollEvent, TimerEvent, SystemControlEvent, SystemControlLowPriorityEvent
from vlcp.event import DefaultPolling
from vlcp.event.connection import Client, TcpServer, ConnectionWriteEvent, ConnectionControlEvent,\
    ResolveRequestEvent, ResolveResponseEvent
from vlcp.utils.connector import Resolver
from vlcp.protocol.protocol import Protocol
from vlcp.event.runnable import RoutineContainer
from socket import SOL_SOCKET, SO_ERROR
from vlcp.event.pqueue import CBQueue
from vlcp.event.runnable import RoutineControlEvent
import errno
import logging
import socket
import os

@withIndices('connection')
class TestDataEvent(Event):
    canignore = False

@withIndices()
class TestWriteEvent(ConnectionWriteEvent):
    pass

class TestProtocol(Protocol):
    _logger = logging.getLogger('TestProtocol')
    class ResponderRoutine(RoutineContainer):
        def __init__(self, connection, passive):
            scheduler = connection.scheduler
            RoutineContainer.__init__(self, scheduler)
            self.passive = passive
            self.connection = connection
        def main(self):
            datamatcher = TestDataEvent.createMatcher(connection=self.connection)
            if self.passive:
                yield (datamatcher,)
                self.event.canignore = True
            for i in range(0,9):
                event = TestWriteEvent(self.connection, self.connection.connmark)
                for m in self.waitForSend(event):
                    yield m
                yield (datamatcher,)
                self.event.canignore = True
            event = TestWriteEvent(self.connection, self.connection.connmark)
            for m in self.waitForSend(event):
                yield m
            if not self.passive:
                yield (datamatcher,)
                self.event.canignore = True
            for m in self.connection.shutdown():
                yield m
    def __init__(self, passive):
        Protocol.__init__(self)
        self.passive = passive
    def parse(self, connection, data, laststart):
        l = len(data)
        events = []
        for i in range(0, l//1000):
            events.append(TestDataEvent(connection))
        return (events, l % 1000)
    def serialize(self, connection, event):
        return (b'a' * 1000, False)
    def init(self, connection):
        for m in Protocol.init(self, connection):
            yield m
        rr = self.ResponderRoutine(connection, self.passive)
        connection.responder = rr
        rr.start(False)
        if False:
            yield
    def error(self, connection):
        for m in Protocol.error(self, connection):
            yield m
        err = connection.socket.getsockopt(SOL_SOCKET, SO_ERROR)
        self._logger.warning('Connection error status: %d(%s)', err, errno.errorcode.get(err, 'Not found'))
        connection.responder.mainroutine.close()
        if False:
            yield
    def notconnected(self, connection):
        self._logger.warning('Connect failed and not retrying for url: %s', connection.rawurl)
        if False:
            yield
    def closed(self, connection):
        for m in Protocol.closed(self, connection):
            yield m
        connection.responder.mainroutine.close()
        if False:
            yield
    def accept(self, server, newaddr, newsocket):
        self._logger.debug('Connection accepted from ' + repr(newaddr))
        return Protocol.accept(self, server, newaddr, newsocket)

class TestConnection(unittest.TestCase):


    def setUp(self):
        #self.scheduler 
        logging.basicConfig()
        self.scheduler = Scheduler(DefaultPolling())
        #self.scheduler.debugging = True
        #self.scheduler.logger.setLevel('DEBUG')
        #Client.logger.setLevel('DEBUG')
        import tests
        import os.path
        rootpath, _ = os.path.split(tests.__path__[0])
        if rootpath:
            os.chdir(rootpath)
        self.scheduler.queue.addSubQueue(3, PollEvent.createMatcher(category=PollEvent.WRITE_READY), 'write', None, None, CBQueue.AutoClassQueue.initHelper('fileno'))
        self.scheduler.queue.addSubQueue(1, PollEvent.createMatcher(category=PollEvent.READ_READY), 'read', None, None, CBQueue.AutoClassQueue.initHelper('fileno'))
        self.scheduler.queue.addSubQueue(5, PollEvent.createMatcher(category=PollEvent.ERROR), 'error')
        self.scheduler.queue.addSubQueue(2, ConnectionControlEvent.createMatcher(), 'control')
        self.scheduler.queue.addSubQueue(4, ConnectionWriteEvent.createMatcher(), 'connectionwrite', 40, 40, CBQueue.AutoClassQueue.initHelper('connection'))
        self.scheduler.queue.addSubQueue(10, RoutineControlEvent.createMatcher(), 'routine')
        self.scheduler.queue.addSubQueue(9, TimerEvent.createMatcher(), 'timer')
        self.scheduler.queue.addSubQueue(8, ResolveResponseEvent.createMatcher(), 'resolve')
        self.scheduler.queue.addSubQueue(8, ResolveRequestEvent.createMatcher(), 'resolvereq')
        self.scheduler.queue.addSubQueue(20, SystemControlEvent.createMatcher(), 'sysctl')
        self.scheduler.queue.addSubQueue(0, SystemControlLowPriorityEvent.createMatcher(), 'sysctllow')
        #Client.logger.setLevel('DEBUG')
        #TcpServer.logger.setLevel('DEBUG')
        self.protocolServer = TestProtocol(True)
        self.protocolClient = TestProtocol(False)
        self.resolver = Resolver(scheduler=self.scheduler)
        self.resolver.start()
    def tearDown(self):
        pass

    def testSelfConnection(self):
        c1 = Client('tcp://localhost:199', self.protocolClient, self.scheduler)
        c2 = Client('ptcp://localhost:199', self.protocolServer, self.scheduler)
        r = RoutineContainer(self.scheduler, True)
        ret = bytearray()
        def mainA():
            m = TestDataEvent.createMatcher()
            while True:
                yield (m,)
                if r.event.connection is c2:
                    ret.extend(b'A')
                else:
                    ret.extend(b'B')
        r.main = mainA
        r.start()
        def waitAndStart():
            for m in r.waitWithTimeout(0.5):
                yield m
            c1.start()
        r.subroutine(waitAndStart())
        c2.start()
        self.scheduler.main()
        self.assertEqual(ret, b'ABABABABABABABABABAB')
    
    def testServerClient(self):
        c1 = Client('tcp://localhost:199', self.protocolClient, self.scheduler)
        s1 = TcpServer('ltcp://localhost:199', self.protocolServer, self.scheduler)
        r = RoutineContainer(self.scheduler, True)
        ret = bytearray()
        def mainA():
            m = TestDataEvent.createMatcher()
            stopped = False
            while True:
                yield (m,)
                if r.event.connection is c1:
                    ret.extend(b'B')
                else:
                    ret.extend(b'A')
                if not stopped:
                    for m in s1.shutdown():
                        yield m
                    stopped = True
        r.main = mainA
        r.start()
        s1.start()
        def waitAndStart():
            for m in r.waitWithTimeout(0.5):
                yield m
            c1.start()
        r.subroutine(waitAndStart())
        self.scheduler.main()
        self.assertEqual(ret, b'ABABABABABABABABABAB')

    def testMultipleClients(self):
        c1 = Client('tcp://localhost:199', self.protocolClient, self.scheduler)
        c2 = Client('tcp://localhost:199', self.protocolClient, self.scheduler)
        s1 = TcpServer('ltcp://localhost:199', self.protocolServer, self.scheduler)
        r = RoutineContainer(self.scheduler, True)
        counter = {c1:0, c2:0}
        ret = bytearray()
        def mainA():
            m = TestDataEvent.createMatcher()
            c1c = False
            c2c = False
            shutdown = False
            while True:
                yield (m,)
                counter[r.event.connection] = counter.get(r.event.connection, 0) + 1
                if r.event.connection is c1:
                    ret.extend(b'A')
                    c1c = True
                elif r.event.connection is c2:
                    ret.extend(b'B')
                    c2c = True
                if c1c and c2c and not shutdown:
                    for m in s1.shutdown():
                        yield m
                    shutdown = True
        r.main = mainA
        r.start()
        s1.start()
        def waitAndStart(c):
            for m in r.waitWithTimeout(0.5):
                yield m
            c.start()
        r.subroutine(waitAndStart(c1))
        r.subroutine(waitAndStart(c2))
        self.scheduler.main()
        print(ret)
        self.assertEqual(counter[c1], 10)
        self.assertEqual(counter[c2], 10)
    def testSelfConnectionSsl(self):
        c1 = Client('ssl://localhost:199', self.protocolClient, self.scheduler, 'testcerts/client.key','testcerts/client.crt','testcerts/root.crt')
        c2 = Client('pssl://localhost:199', self.protocolServer, self.scheduler, 'testcerts/server.key','testcerts/server.crt','testcerts/root.crt')
        r = RoutineContainer(self.scheduler, True)
        ret = bytearray()
        def mainA():
            m = TestDataEvent.createMatcher()
            while True:
                yield (m,)
                if r.event.connection is c2:
                    ret.extend(b'A')
                else:
                    ret.extend(b'B')
        r.main = mainA
        r.start()
        def waitAndStart(c):
            for m in r.waitWithTimeout(0.5):
                yield m
            c.start()
        r.subroutine(waitAndStart(c1))
        c2.start()
        self.scheduler.main()
        self.assertEqual(ret, b'ABABABABABABABABABAB')
    def testServerClientSsl(self):
        c1 = Client('ssl://localhost:199', self.protocolClient, self.scheduler, 'testcerts/client.key', 'testcerts/client.crt', 'testcerts/root.crt')
        s1 = TcpServer('lssl://localhost:199', self.protocolServer, self.scheduler, 'testcerts/server.key', 'testcerts/server.crt', 'testcerts/root.crt')
        r = RoutineContainer(self.scheduler, True)
        ret = bytearray()
        def mainA():
            m = TestDataEvent.createMatcher()
            stopped = False
            while True:
                yield (m,)
                if r.event.connection is c1:
                    ret.extend(b'B')
                else:
                    ret.extend(b'A')
                if not stopped:
                    for m in s1.shutdown():
                        yield m
                    stopped = True
        r.main = mainA
        r.start()
        s1.start()
        def waitAndStart(c):
            for m in r.waitWithTimeout(0.5):
                yield m
            c.start()
        r.subroutine(waitAndStart(c1))
        self.scheduler.main()
        self.assertEqual(ret, b'ABABABABABABABABABAB')
    def testCAVerify(self):
        c1 = Client('ssl://localhost:199', self.protocolClient, self.scheduler, 'testcerts/selfsigned.key','testcerts/selfsigned.crt','testcerts/selfsigned.crt')
        c2 = Client('pssl://localhost:199', self.protocolServer, self.scheduler, 'testcerts/server.key','testcerts/server.crt','testcerts/root.crt')
        r = RoutineContainer(self.scheduler, True)
        ret = bytearray()
        def mainA():
            m = TestDataEvent.createMatcher()
            while True:
                yield (m,)
                if r.event.connection is c2:
                    ret.extend(b'A')
                else:
                    ret.extend(b'B')
        self.notconnected = False
        def notConnected(connection):
            if connection is c1:
                self.notconnected = True
            if False:
                yield
        self.protocolClient.notconnected = notConnected
        r.main = mainA
        r.start()
        def waitAndStart(c):
            for m in r.waitWithTimeout(0.5):
                yield m
            c.start()
        r.subroutine(waitAndStart(c1))
        c2.start()
        self.scheduler.main()
        self.assertTrue(self.notconnected)
        self.assertEqual(ret, b'')
    def testCAVerify2(self):
        c1 = Client('ssl://localhost:199', self.protocolClient, self.scheduler, 'testcerts/selfsigned.key','testcerts/selfsigned.crt',None)
        c2 = Client('pssl://localhost:199', self.protocolServer, self.scheduler, 'testcerts/server.key','testcerts/server.crt','testcerts/root.crt')
        r = RoutineContainer(self.scheduler, True)
        ret = bytearray()
        def mainA():
            m = TestDataEvent.createMatcher()
            while True:
                yield (m,)
                if r.event.connection is c2:
                    ret.extend(b'A')
                else:
                    ret.extend(b'B')
        self.notconnected = False
        def notConnected(connection):
            if connection is c1:
                self.notconnected = True
            if False:
                yield
        self.protocolClient.notconnected = notConnected
        r.main = mainA
        r.start()
        def waitAndStart(c):
            for m in r.waitWithTimeout(0.5):
                yield m
            c.start()
        r.subroutine(waitAndStart(c1))
        c2.start()
        self.scheduler.main()
        self.assertTrue(self.notconnected)
        self.assertEqual(ret, b'')
    def testCAVerify3(self):
        c1 = Client('ssl://localhost:199', self.protocolClient, self.scheduler, None, None, 'testcerts/root.crt')
        c2 = Client('pssl://localhost:199', self.protocolServer, self.scheduler, 'testcerts/server.key','testcerts/server.crt','testcerts/root.crt')
        r = RoutineContainer(self.scheduler, True)
        ret = bytearray()
        def mainA():
            m = TestDataEvent.createMatcher()
            while True:
                yield (m,)
                if r.event.connection is c2:
                    ret.extend(b'A')
                else:
                    ret.extend(b'B')
        self.notconnected = False
        def notConnected(connection):
            if connection is c1:
                self.notconnected = True
            if False:
                yield
        self.protocolClient.notconnected = notConnected
        r.main = mainA
        r.start()
        def waitAndStart(c):
            for m in r.waitWithTimeout(0.5):
                yield m
            c.start()
        r.subroutine(waitAndStart(c1))
        c2.start()
        self.scheduler.main()
        self.assertTrue(self.notconnected)
        self.assertEqual(ret, b'')
    def testSelfConnectionSslWithoutClientCertificate(self):
        c1 = Client('ssl://localhost:199', self.protocolClient, self.scheduler, None, None,'testcerts/root.crt')
        c2 = Client('pssl://localhost:199', self.protocolServer, self.scheduler, 'testcerts/server.key','testcerts/server.crt',None)
        r = RoutineContainer(self.scheduler, True)
        ret = bytearray()
        def mainA():
            m = TestDataEvent.createMatcher()
            while True:
                yield (m,)
                if r.event.connection is c2:
                    ret.extend(b'A')
                else:
                    ret.extend(b'B')
        r.main = mainA
        r.start()
        def waitAndStart(c):
            for m in r.waitWithTimeout(0.5):
                yield m
            c.start()
        r.subroutine(waitAndStart(c1))
        c2.start()
        self.scheduler.main()
        self.assertEqual(ret, b'ABABABABABABABABABAB')
    def testSelfConnectionUdp(self):
        c1 = Client('udp://localhost:199', self.protocolClient, self.scheduler)
        c2 = Client('pudp://localhost:199', self.protocolServer, self.scheduler)
        r = RoutineContainer(self.scheduler, True)
        ret = bytearray()
        def mainA():
            m = TestDataEvent.createMatcher()
            while True:
                yield (m,)
                if r.event.connection is c2:
                    ret.extend(b'A')
                else:
                    ret.extend(b'B')
        r.main = mainA
        r.start()
        def waitAndStart(c):
            for m in r.waitWithTimeout(0.5):
                yield m
            c.start()
        r.subroutine(waitAndStart(c1))
        c2.start()
        self.scheduler.main()
        self.assertEqual(ret, b'ABABABABABABABABABAB')
    def testSelfConnectionUnix(self):
        if not hasattr(socket, 'AF_UNIX'):
            print('Skip UNIX socket test because not supported')
            return
        try:
            os.remove('/var/run/unixsocktest.sock')
        except:
            pass
        c1 = Client('unix:/var/run/unixsocktest.sock', self.protocolClient, self.scheduler)
        c2 = Client('punix:/var/run/unixsocktest.sock', self.protocolServer, self.scheduler)
        r = RoutineContainer(self.scheduler, True)
        ret = bytearray()
        def mainA():
            m = TestDataEvent.createMatcher()
            while True:
                yield (m,)
                if r.event.connection is c2:
                    ret.extend(b'A')
                else:
                    ret.extend(b'B')
        r.main = mainA
        r.start()
        def waitAndStart(c):
            for m in r.waitWithTimeout(0.5):
                yield m
            c.start()
        r.subroutine(waitAndStart(c1))
        c2.start()
        self.scheduler.main()
        self.assertEqual(ret, b'ABABABABABABABABABAB')
    def testSelfConnectionUnixDgram(self):
        if not hasattr(socket, 'AF_UNIX'):
            print('Skip UNIX socket test because not supported')
            return
        try:
            os.remove('/var/run/unixsocktestudp1.sock')
        except:
            pass
        try:
            os.remove('/var/run/unixsocktestudp2.sock')
        except:
            pass
        c1 = Client('dunix:/var/run/unixsocktestudp2.sock', self.protocolClient, self.scheduler, bindaddress = ((socket.AF_UNIX, '/var/run/unixsocktestudp1.sock'),))
        c2 = Client('pdunix:/var/run/unixsocktestudp2.sock', self.protocolServer, self.scheduler)
        r = RoutineContainer(self.scheduler, True)
        ret = bytearray()
        def mainA():
            m = TestDataEvent.createMatcher()
            while True:
                yield (m,)
                if r.event.connection is c2:
                    ret.extend(b'A')
                else:
                    ret.extend(b'B')
        r.main = mainA
        r.start()
        def waitAndStart(c):
            for m in r.waitWithTimeout(0.5):
                yield m
            c.start()
        r.subroutine(waitAndStart(c1))
        c2.start()
        self.scheduler.main()
        self.assertEqual(ret, b'ABABABABABABABABABAB')

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()

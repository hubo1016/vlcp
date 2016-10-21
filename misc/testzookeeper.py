'''
Created on 2016/9/20

:author: hubo
'''
from __future__ import print_function
from vlcp.server import main
from vlcp.event import Client
from vlcp.server.module import Module
from vlcp.config import defaultconfig
from vlcp.protocol.zookeeper import ZooKeeper, ZooKeeperConnectionStateEvent,\
    ZooKeeperWatcherEvent
import vlcp.utils.zookeeper as zk
from vlcp.event.runnable import RoutineContainer
from namedstruct import dump
from pprint import pprint

@defaultconfig
class TestModule(Module):
    _default_url = 'tcp://localhost/'
    _default_sessiontimeout = 30
    def __init__(self, server):
        Module.__init__(self, server)
        self.protocol = ZooKeeper()
        self.client = Client(self.url, self.protocol, self.scheduler)
        self.connections.append(self.client)
        self.apiroutine = RoutineContainer(self.scheduler)
        self.apiroutine.main = self.main
        self.routines.append(self.apiroutine)
    def watcher(self):
        watcher = ZooKeeperWatcherEvent.createMatcher(connection = self.client)
        while True:
            yield (watcher,)
            print('WatcherEvent: %r' % (dump(self.apiroutine.event.message),))
    def main(self):
        self.apiroutine.subroutine(self.watcher(), False, daemon = True)
        up = ZooKeeperConnectionStateEvent.createMatcher(ZooKeeperConnectionStateEvent.UP, self.client)
        notconn = ZooKeeperConnectionStateEvent.createMatcher(ZooKeeperConnectionStateEvent.NOTCONNECTED, self.client)
        yield (up, notconn)
        if self.apiroutine.matcher is notconn:
            print('Not connected')
            return
        else:
            print('Connection is up: %r' % (self.client,))
        # Handshake
        for m in self.protocol.handshake(self.client, zk.ConnectRequest(
                                                        timeOut = int(self.sessiontimeout * 1000),
                                                        passwd = b'\x00' * 16,      # Why is it necessary...
                                                    ), self.apiroutine, []):
            yield m
        for m in self.protocol.requests(self.client, [zk.create(b'/vlcptest', b'test'),
                                                      zk.getdata(b'/vlcptest', True)], self.apiroutine):
            yield m
        pprint(dump(self.apiroutine.retvalue[0]))
        for m in self.apiroutine.waitWithTimeout(0.2):
            yield m
        for m in self.protocol.requests(self.client, [zk.delete(b'/vlcptest'),
                                                      zk.getdata(b'/vlcptest', watch = True)], self.apiroutine):
            yield m
        pprint(dump(self.apiroutine.retvalue[0]))
        for m in self.protocol.requests(self.client, [zk.multi(
                                                            zk.multi_create(b'/vlcptest2', b'test'),
                                                            zk.multi_create(b'/vlcptest2/subtest', 'test2')
                                                        ),
                                                      zk.getchildren2(b'/vlcptest2', True)], self.apiroutine):
            yield m
        pprint(dump(self.apiroutine.retvalue[0]))
        for m in self.protocol.requests(self.client, [zk.multi(
                                                            zk.multi_delete(b'/vlcptest2/subtest'),
                                                            zk.multi_delete(b'/vlcptest2')),
                                                      zk.getchildren2(b'/vlcptest2', True)], self.apiroutine):
            yield m
        pprint(dump(self.apiroutine.retvalue[0]))
        
        
if __name__ == '__main__':
    main()
    
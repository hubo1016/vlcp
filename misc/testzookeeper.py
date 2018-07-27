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
#from pprint import pprint
import json
from vlcp.event.event import M_
def pprint(v):
    print(json.dumps(v, indent=2))

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

    async def watcher(self):
        watcher = ZooKeeperWatcherEvent.createMatcher(connection = self.client)
        while True:
            ev = await watcher
            print('WatcherEvent: %r' % (dump(ev.message),))

    async def main(self):
        self.apiroutine.subroutine(self.watcher(), False, daemon = True)
        up = ZooKeeperConnectionStateEvent.createMatcher(ZooKeeperConnectionStateEvent.UP, self.client)
        notconn = ZooKeeperConnectionStateEvent.createMatcher(ZooKeeperConnectionStateEvent.NOTCONNECTED, self.client)
        _, m = await M_(up, notconn)
        if m is notconn:
            print('Not connected')
            return
        else:
            print('Connection is up: %r' % (self.client,))
        # Handshake
        await self.protocol.handshake(self.client, zk.ConnectRequest(
                                                        timeOut = int(self.sessiontimeout * 1000),
                                                        passwd = b'\x00' * 16,      # Why is it necessary...
                                                    ), self.apiroutine, [])
        result = await self.protocol.requests(self.client, [zk.create(b'/vlcptest', b'test'),
                                                      zk.getdata(b'/vlcptest', True)], self.apiroutine)
        pprint(dump(result[0]))
        await self.apiroutine.wait_with_timeout(0.2)
        result = await self.protocol.requests(self.client, [zk.delete(b'/vlcptest'),
                                                      zk.getdata(b'/vlcptest', watch = True)], self.apiroutine)
        pprint(dump(result[0]))
        result = await self.protocol.requests(self.client, [zk.multi(
                                                            zk.multi_create(b'/vlcptest2', b'test'),
                                                            zk.multi_create(b'/vlcptest2/subtest', 'test2')
                                                        ),
                                                      zk.getchildren2(b'/vlcptest2', True)], self.apiroutine)
        pprint(dump(result[0]))
        result = await self.protocol.requests(self.client, [zk.multi(
                                                            zk.multi_delete(b'/vlcptest2/subtest'),
                                                            zk.multi_delete(b'/vlcptest2')),
                                                      zk.getchildren2(b'/vlcptest2', True)], self.apiroutine)
        pprint(dump(result[0]))
        
        
if __name__ == '__main__':
    main()
    
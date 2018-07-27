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
from vlcp.utils.zkclient import ZooKeeperClient, ZooKeeperSessionStateChanged
from vlcp.event.runnable import RoutineContainer
from namedstruct import dump
# from pprint import pprint
import json
def pprint(v):
    print(json.dumps(v, indent=2))


@defaultconfig
class TestModule(Module):
    _default_serverlist = ['tcp://localhost:3181/','tcp://localhost:3182/','tcp://localhost:3183/']
    def __init__(self, server):
        Module.__init__(self, server)
        self.apiroutine = RoutineContainer(self.scheduler)
        self.client = ZooKeeperClient(self.apiroutine, self.serverlist)
        self.connections.append(self.client)
        self.apiroutine.main = self.main
        self.routines.append(self.apiroutine)
    async def watcher(self):
        watcher = ZooKeeperWatcherEvent.createMatcher()
        while True:
            ev = await watcher
            print('WatcherEvent: %r' % (dump(ev.message),))
    async def main(self):
        async def _watch(w):
            r = await w.wait(self.apiroutine)
            print('Watcher returns:', dump(r))
        async def _watchall(watchers):
            for w in watchers:
                if w is not None:
                    self.apiroutine.subroutine(_watch(w))
        self.apiroutine.subroutine(self.watcher(), False, daemon = True)
        up = ZooKeeperSessionStateChanged.createMatcher(ZooKeeperSessionStateChanged.CREATED, self.client)
        await up
        print('Connection is up: %r' % (self.client.currentserver,))
        r = await self.client.requests([zk.create(b'/vlcptest', b'test'),
                                       zk.getdata(b'/vlcptest', True)], self.apiroutine)
        print(r)
        pprint(dump(r[0]))
        _watchall(r[3])
        await self.apiroutine.wait_with_timeout(0.2)
        r = await self.client.requests([zk.delete(b'/vlcptest'),
                                        zk.getdata(b'/vlcptest', watch = True)], self.apiroutine)
        print(r)
        pprint(dump(r[0]))
        _watchall(r[3])
        r = await self.client.requests([zk.multi(
                                            zk.multi_create(b'/vlcptest2', b'test'),
                                            zk.multi_create(b'/vlcptest2/subtest', 'test2')
                                        ),
                                        zk.getchildren2(b'/vlcptest2', True)], self.apiroutine)
        print(r)
        pprint(dump(r[0]))
        _watchall(r[3])
        r = await self.client.requests([zk.multi(
                                            zk.multi_delete(b'/vlcptest2/subtest'),
                                            zk.multi_delete(b'/vlcptest2')),
                                        zk.getchildren2(b'/vlcptest2', True)], self.apiroutine)
        print(r)
        pprint(dump(r[0]))
        _watchall(r[3])
        
        
if __name__ == '__main__':
    main()
    
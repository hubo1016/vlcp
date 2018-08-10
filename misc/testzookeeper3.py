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


@defaultconfig
class TestModule(Module):
    _default_serverlist = ['tcp://localhost:3181/','tcp://localhost:3182/','tcp://localhost:3183/']

    def __init__(self, server):
        Module.__init__(self, server)
        self.apiroutine = RoutineContainer(self.scheduler)
        self.apiroutine.main = self.main
        self.routines.append(self.apiroutine)

    async def main(self):
        clients = [ZooKeeperClient(self.apiroutine, self.serverlist) for _ in range(0,10)]
        for c in clients:
            c.start()
        async def test_loop(number):
            maindir = ('vlcptest_' + str(number)).encode('utf-8')
            client = clients[number % len(clients)]
            for _ in range(0, 100):
                await client.requests([zk.multi(
                                            zk.multi_create(maindir, b'test'),
                                            zk.multi_create(maindir + b'/subtest', 'test2')
                                       ),
                                       zk.getchildren2(maindir, True)], self.apiroutine)
                await client.requests([zk.multi(
                                            zk.multi_delete(maindir + b'/subtest'),
                                            zk.multi_delete(maindir)),
                                       zk.getchildren2(maindir, True)], self.apiroutine)
        from time import time
        starttime = time()
        await self.apiroutine.execute_all([test_loop(i) for i in range(0, 100)])
        print('10000 loops in %r seconds, with %d connections' % (time() - starttime, len(clients)))
        for c in clients:
            await c.shutdown()

        
if __name__ == '__main__':
    main()

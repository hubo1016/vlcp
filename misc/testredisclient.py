'''
Created on 2016/1/11

:author: hubo
'''
from __future__ import print_function
from vlcp.config import manager
from vlcp.server.module import Module
from vlcp.event.runnable import RoutineContainer
from vlcp.utils.redisclient import RedisClient
from vlcp.server import main
from time import time
from vlcp.event.event import M_

class MainRoutine(RoutineContainer):
    def __init__(self, client, scheduler=None, daemon=False):
        RoutineContainer.__init__(self, scheduler=scheduler, daemon=daemon)
        self.client = client

    async def main(self):
        with self.client.context(self):
            print(await self.client.execute_command(self, 'ping'))
            print(await self.client.execute_command(self, 'set', 'abc', 'def'))
            print(await self.client.execute_command(self, 'get', 'abc'))
            async def sub():
                await self.wait_with_timeout(1)
                return await self.client.execute_command(self, 'lpush', 'test', 'value')
            self.subroutine(sub(), True)
            print(await self.client.execute_command(self, 'brpop', 'test', '0'))
            matchers = await self.client.subscribe(self, 'skey')
            async def sub2():
                await self.client.execute_command(self, 'publish', 'skey', 'svalue')
            self.subroutine(sub2(), True)
            ev, m = await M_(*matchers)
            print(ev.message)
            newconn = await self.client.get_connection(self)
            with newconn.context(self):
                print(await newconn.batch_execute(self, ('get','abc'),('set','abc','ghi'),('get','abc'))) 
            await self.client.unsubscribe(self, 'skey')
            t = time()
            for i in range(0, 10000):
                await self.client.execute_command(self, 'set', 'abc', 'def')
            print(time() - t)
            bc = (('set', 'abc', 'def'),) * 100
            t = time()
            for i in range(0, 100):
                await self.client.batch_execute(self, *bc)
            print(time() - t)
            bc = (('set', 'abc', 'def'),) * 10000
            t = time()
            await self.client.batch_execute(self, *bc)
            print(time() - t)
            async def sub3():
                for i in range(0, 100):
                    await self.client.execute_command(self, 'set', 'abc', 'def')
            subroutines = [sub3() for i in range(0,100)]
            t = time()
            await self.execute_all(subroutines)
            print(time() - t)
            connections = []
            for i in range(0, 100):
                connections.append(await self.client.get_connection(self))
            async def sub4(c):
                for i in range(0, 100):
                    await c.execute_command(self, 'set', 'abc', 'def')
            subroutines = [sub4(c) for c in connections]
            t = time()
            await self.execute_all(subroutines)
            print(time() - t)
            for c in connections:
                await c.release(self)


class MainModule(Module):
    def __init__(self, server):
        Module.__init__(self, server)
        self.client = RedisClient()
        self.routines.append(MainRoutine(self.client, self.scheduler))
 

if __name__ == '__main__':
    #manager['protocol.redis.hiredis'] = False
    #manager['server.debugging'] = True
    main()

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

class MainRoutine(RoutineContainer):
    def __init__(self, client, scheduler=None, daemon=False):
        RoutineContainer.__init__(self, scheduler=scheduler, daemon=daemon)
        self.client = client
    def main(self):
        with self.client.context(self):
            for m in self.client.execute_command(self, 'ping'):
                yield m
            print(self.retvalue)
            for m in self.client.execute_command(self, 'set', 'abc', 'def'):
                yield m
            print(self.retvalue)
            for m in self.client.execute_command(self, 'get', 'abc'):
                yield m
            print(self.retvalue)
            def sub():
                for m in self.waitWithTimeout(1):
                    yield m
                for m in self.client.execute_command(self, 'lpush', 'test', 'value'):
                    yield m
            self.subroutine(sub(), True)
            for m in self.client.execute_command(self, 'brpop', 'test', '0'):
                yield m
            print(self.retvalue)
            for m in self.client.subscribe(self, 'skey'):
                yield m
            matchers = self.retvalue
            def sub2():
                for m in self.client.execute_command(self, 'publish', 'skey', 'svalue'):
                    yield m
            self.subroutine(sub2(), True)
            yield matchers
            print(self.event.message)
            for m in self.client.get_connection(self):
                yield m
            newconn = self.retvalue
            with newconn.context(self):
                for m in newconn.batch_execute(self, ('get','abc'),('set','abc','ghi'),('get','abc')):
                    yield m
                print(self.retvalue) 
            for m in self.client.unsubscribe(self, 'skey'):
                yield m
            t = time()
            for i in range(0, 10000):
                for m in self.client.execute_command(self, 'set', 'abc', 'def'):
                    yield m
            print(time() - t)
            bc = (('set', 'abc', 'def'),) * 100
            t = time()
            for i in range(0, 100):
                for m in self.client.batch_execute(self, *bc):
                    yield m
            print(time() - t)
            bc = (('set', 'abc', 'def'),) * 10000
            t = time()
            for m in self.client.batch_execute(self, *bc):
                yield m
            print(time() - t)
            def sub3():
                for i in range(0, 100):
                    for m in self.client.execute_command(self, 'set', 'abc', 'def'):
                        yield m
            subroutines = [sub3() for i in range(0,100)]
            t = time()
            for m in self.executeAll(subroutines):
                yield m
            print(time() - t)
            connections = []
            for i in range(0, 100):
                for m in self.client.get_connection(self):
                    yield m
                connections.append(self.retvalue)
            def sub4(c):
                for i in range(0, 100):
                    for m in c.execute_command(self, 'set', 'abc', 'def'):
                        yield m
            subroutines = [sub4(c) for c in connections]
            t = time()
            for m in self.executeAll(subroutines):
                yield m
            print(time() - t)
            for c in connections:
                for m in c.release(self):
                    yield m

class MainModule(Module):
    def __init__(self, server):
        Module.__init__(self, server)
        self.client = RedisClient()
        self.routines.append(MainRoutine(self.client, self.scheduler))
 

if __name__ == '__main__':
    #manager['protocol.redis.hiredis'] = False
    #manager['server.debugging'] = True
    main()

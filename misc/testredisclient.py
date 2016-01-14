'''
Created on 2016/1/11

:author: hubo
'''
from __future__ import print_function
from vlcp.server.module import Module
from vlcp.event.runnable import RoutineContainer
from vlcp.utils.redisclient import RedisClient

class MainRoutine(RoutineContainer):
    def __init__(self, client, scheduler=None, daemon=False):
        RoutineContainer.__init__(self, scheduler=scheduler, daemon=daemon)
        self.client = client
    def main(self):
        for m in self.client.execute_command(self, 'PING'):
            yield m
        print(self.retvalue)
        for m in self.client.execute_command(self, 'SET', 'abc', 'def'):
            yield m
        print(self.retvalue)
        for m in self.client.execute_command(self, 'GET', 'abc'):
            yield m
        print(self.retvalue)

class MainModule(Module):
    def __init__(self, server):
        Module.__init__(self, server)

if __name__ == '__main__':
    pass
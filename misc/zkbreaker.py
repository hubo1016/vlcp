'''
Created on 2016/10/25

:author: hubo
'''
from vlcp.config import config
from vlcp.protocol.zookeeper import ZooKeeper
import vlcp.protocol.zookeeper
from random import random
from vlcp.event.core import syscall_clearqueue

@config('zookeeper')
class BreakingZooKeeper(ZooKeeper):
    '''
    This evil protocol breaks ZooKeeper connection from time to time to validate your client
    and service code
    '''
    _default_senddrop = 0.001
    _default_receivedrop = 0.01
    
    def _senddata(self, connection, data, container):
        if random() < self.senddrop:
            for m in connection.reset(True):
                yield m
        for m in ZooKeeper._senddata(self, connection, data, container):
            yield m
    def requests(self, connection, requests, container, callback=None):
        def evil_callback(request, response):
            if random() < self.receivedrop:
                connection.subroutine(connection.reset(True), False)
                connection.subroutine(connection.syscall_noreturn(syscall_clearqueue(connection.scheduler.queue[('message', connection)])))
            if callback:
                callback(request, response)
        for m in ZooKeeper.requests(self, connection, requests, container, callback=callback):
            yield m
            
def patch_zookeeper():
    vlcp.protocol.zookeeper.ZooKeeper = BreakingZooKeeper

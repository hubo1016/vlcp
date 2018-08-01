'''
Created on 2016/10/25

:author: hubo
'''
from vlcp.config import config
from vlcp.protocol.zookeeper import ZooKeeper
import vlcp.protocol.zookeeper
from random import random
from vlcp.event.core import syscall_clearqueue

from logging import getLogger


_logger = getLogger(__name__)


@config('protocol.zookeeper')
class BreakingZooKeeper(ZooKeeper):
    '''
    This evil protocol breaks ZooKeeper connection from time to time to validate your client
    and service code
    '''
    _default_senddrop = 0.001
    _default_receivedrop = 0.01
    
    async def _senddata(self, connection, data, container, priority = 0):
        if random() < self.senddrop:
            _logger.warning("Oops, I break a connection when sending")
            await connection.reset(True)
        return await ZooKeeper._senddata(self, connection, data, container, priority)

    async def requests(self, connection, requests, container, callback=None, priority = 0):
        def evil_callback(request, response):
            if random() < self.receivedrop:
                _logger.warning("Oops, I break a connection when receiving")
                connection.subroutine(connection.reset(True), False)
                connection.subroutine(connection.syscall_noreturn(syscall_clearqueue(connection.scheduler.queue[('message', connection)])))
            if callback:
                callback(request, response)
        return await ZooKeeper.requests(self, connection, requests, container, callback, priority)


def patch_zookeeper():
    vlcp.protocol.zookeeper.ZooKeeper = BreakingZooKeeper

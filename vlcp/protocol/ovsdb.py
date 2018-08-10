'''
Created on 2016/2/19

:author: hubo
'''

from vlcp.config import defaultconfig
from vlcp.protocol.jsonrpc import JsonRPC, JsonRPCRequestEvent,\
    JsonRPCNotificationEvent
from vlcp.event.connection import ConnectionResetException, ConnectionWriteEvent
from contextlib import closing
from vlcp.event.event import M_

@defaultconfig
class OVSDB(JsonRPC):
    '''
    OVSDB protocol, this is a specialized JSON-RPC 1.0 protocol
    '''
    _default_defaultport = 6632
    # Only accept "echo" requests
    _default_allowedrequests = ('echo',)
    # Send "echo" requests when connection is idle
    _default_keepalivetime = 10
    # Disconnect when reply is not received for "echo" requests
    _default_keepalivetimeout = 3
    _default_tcp_nodelay = True
    async def _respond_echo(self, connection):
        try:
            request_matcher = JsonRPCRequestEvent.createMatcher('echo', connection)
            connstate = self.statematcher(connection)
            while True:
                ev, m = await M_(request_matcher, connstate)
                if m is request_matcher:
                    ev.canignore = True
                    reply = self.formatreply(ev.params, ev.id, connection)
                    reply.echoreply = True
                    await connection.write(reply, False)
                else:
                    break
        except ConnectionResetException:
            pass
        
    async def _extra_queues(self, connection):
        connection.createdqueues.append(connection.scheduler.queue.addSubQueue(\
                self.writepriority + 10, ConnectionWriteEvent.createMatcher(connection = connection, _ismatch = lambda x: hasattr(x, 'echoreply') and x.echoreply), ('echoreply', connection)))
        
    async def reconnect_init(self, connection):
        await JsonRPC.reconnect_init(self, connection)
        connection.subroutine(self._respond_echo(connection))

    async def keepalive(self, connection):
        try:
            timeout, _ = await connection.execute_with_timeout(self.keepalivetimeout,
                                                               self.querywithreply('echo', [], connection, connection))
            if timeout:
                await connection.reset(True)
        except Exception:
            await connection.reset(True)

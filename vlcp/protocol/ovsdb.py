'''
Created on 2016/2/19

:author: hubo
'''

from vlcp.config import defaultconfig
from vlcp.protocol.jsonrpc import JsonRPC, JsonRPCRequestEvent
from vlcp.event.connection import ConnectionResetException

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
    def _respond_echo(self, connection):
        try:
            request_matcher = JsonRPCRequestEvent.createMatcher('echo', connection)
            connstate = self.statematcher(connection)
            while True:
                yield (request_matcher, connstate)
                if connection.matcher is request_matcher:
                    connection.event.canignore = True
                    reply = self.formatreply(connection.event.params, connection.event.id, connection)
                    for m in connection.write(reply, False):
                        yield m
                else:
                    break
        except ConnectionResetException:
            pass
    def reconnect_init(self, connection):
        for m in JsonRPC.reconnect_init(self, connection):
            yield m
        connection.subroutine(self._respond_echo(connection))
    def keepalive(self, connection):
        try:
            for m in connection.executeWithTimeout(self.keepalivetimeout, self.querywithreply('echo', [], connection, connection)):
                yield m
            if connection.timeout:
                for m in connection.reset(True):
                    yield m
        except Exception:
            for m in connection.reset(True):
                yield m

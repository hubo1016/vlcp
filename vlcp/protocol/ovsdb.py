'''
Created on 2016/2/19

:author: hubo
'''

from vlcp.config import defaultconfig
from vlcp.protocol.jsonrpc import JsonRPC, JsonRPCRequestEvent
from vlcp.event.connection import ConnectionResetException

@defaultconfig
class OVSDB(JsonRPC):
    _default_defaultport = 6632
    _default_allowedrequests = ('echo',)
    _default_keepalivetime = 60
    _default_echotimeout = 10
    def _respond_echo(self, connection):
        try:
            request_matcher = JsonRPCRequestEvent.createMatcher('echo', connection)
            connstate = self.statematcher(connection)
            while True:
                yield (request_matcher, connstate)
                if connection.matcher == request_matcher:
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
            for m in connection.executeWithTimeout(self.echotimeout, self.querywithreply('echo', [], connection, connection)):
                yield m
            if connection.timeout:
                for m in connection.reset(True):
                    yield m
        except:
            for m in connection.reset(True):
                yield m

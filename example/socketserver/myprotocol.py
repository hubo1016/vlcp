from vlcp.protocol import Protocol
from vlcp.config import defaultconfig
from vlcp.event import Event, withIndices, ConnectionWriteEvent
import logging
import protocoldef as d

@withIndices('state', 'connection', 'connmark', 'createby')
class MyProtocolConnectionStateEvent(Event):
    UP = 'up'
    DOWN = 'down'
    
@withIndices('type', 'version', 'connection', 'connmark', 'createby')
class MyProtocolRequestEvent(Event):
    pass
    
@withIndices('xid', 'connection', 'connmark', 'createby', 'type', 'iserror')
class MyProtocolReplyEvent(Event):
    pass

class MyProtocolException(Exception):
    pass

def _str(s):
    if isinstance(s, str):
        return s
    else:
        return s.decode('utf-8')
    
@defaultconfig
class MyProtocol(Protocol):
    _default_persist = True
    _default_defaultport = 9723
    _default_buffersize = 65536
    _default_keepalivetime = 10
    _default_keepalivetimeout = 3
    _logger = logging.getLogger(__name__ + '.MyProtocol')
    def init(self, connection):
        for m in Protocol.init(self, connection):
            yield m
        for m in self.reconnect_init(connection):
            yield m
    def reconnect_init(self, connection):
        connection.xid = 0
        for m in connection.waitForSend(
                MyProtocolConnectionStateEvent(
                    MyProtocolConnectionStateEvent.UP,
                    connection,
                    connection.connmark,
                    self)):
            yield m
    def closed(self, connection):
        for m in Protocol.closed(self, connection):
            yield m
        for m in connection.waitForSend(
                MyProtocolConnectionStateEvent(
                    MyProtocolConnectionStateEvent.DOWN,
                    connection,
                    connection.connmark,
                    self)):
            yield m
    def error(self, connection):
        for m in Protocol.error(self, connection):
            yield m
        for m in connection.waitForSend(
                MyProtocolConnectionStateEvent(
                    MyProtocolConnectionStateEvent.DOWN,
                    connection,
                    connection.connmark,
                    self)):
            yield m
    def parse(self, connection, data, laststart):
        events = []
        currstart = 0
        while True:
            r = d.message.parse(data[currstart:])
            if not r:
                break
            msg, size = r
            if msg.type == d.ECHO_REQUEST and msg.version == d.MESSAGE_VERSION_10:
                # Direct reply
                msg.type = d.ECHO_REPLY
                events.append(ConnectionWriteEvent(connection,
                                                    connection.connmark,
                                                    data = msg._tobytes()))
            elif msg.type & 1:
                events.append(MyProtocolRequestEvent(
                        msg.type,
                        msg.version,
                        connection,
                        connection.connmark,
                        self,
                        message = msg))
            else:
                events.append(MyProtocolReplyEvent(
                        msg.xid,
                        connection,
                        connection.connmark,
                        self,
                        msg.type,
                        msg.type == d.ERROR,
                        message = msg))
            currstart += size
        if laststart == len(data):
            # Remote write close
            events.append(ConnectionWriteEvent(connection, connection.connmark, data = b'', EOF = True))
        return (events, len(data) - currstart)
    def sendrequest(self, connection, request, container):
        connection.xid += 1
        xid = connection.xid
        request.xid = xid
        for m in connection.write(ConnectionWriteEvent(
                    connection, connection.connmark,
                    data = request._tobytes())):
            yield m
        container.retvalue = xid
    def statematcher(self, connection, state = MyProtocolConnectionStateEvent.DOWN,
                           currentconn = True):
        return MyProtocolConnectionStateEvent.createMatcher(
                    state, connection,
                    connection.connmark if currentconn else None)
    def request(self, connection, request, container):
        for m in self.sendrequest(connection, request, container):
            yield m
        xid = container.retvalue
        reply_matcher = MyProtocolReplyEvent.createMatcher(xid, connection, connection.connmark)
        conn_matcher = self.statematcher(connection)
        yield (reply_matcher, conn_matcher)
        if container.matcher is conn_matcher:
            # Connection down before reply received
            raise IOError('Connection down before reply received')
        else:
            msg = container.event.message
            if msg.type == d.ERROR:
                raise MyProtocolException('%s(%s)' %
                        (_str(msg.details),
                        str(d.err_type.formatter(msg.err_type))))
            else:
                container.retvalue = msg
    def reply_to(self, connection, reply, request, container):
        xid = request.xid
        reply.xid = xid
        for m in connection.write(ConnectionWriteEvent(
                    connection, connection.connmark,
                    data = reply._tobytes())):
            yield m
        container.retvalue = xid
    def keepalive(self, connection):
        try:
            for m in connection.executeWithTimeout(self.keepalivetimeout,
                            self.request(connection, d.message_echo(), connection)):
                yield m
            if connection.timeout:
                for m in connection.reset(True):
                    yield m
        except Exception:
            for m in connection.reset(True):
                yield m

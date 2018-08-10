'''
Created on 2015/12/25

:author: hubo
'''

from vlcp.protocol import Protocol
from vlcp.config import defaultconfig
from vlcp.event import Event, withIndices, ConnectionWriteEvent
import logging
import os
from vlcp.event.stream import Stream, StreamDataEvent
from vlcp.event.connection import Client
from vlcp.event.event import M_

@withIndices('state', 'connection', 'connmark', 'createby')
class RawConnectionStateEvent(Event):
    CONNECTION_UP = 'up'
    CONNECTION_DOWN = 'down'
    CONNECTION_NOTCONNECTED = 'notconnected'

def _copy(buffer):
    try:
        if isinstance(buffer, memoryview):
            return buffer.tobytes()
        else:
            return buffer[:]
    except Exception:
        return buffer[:]

@defaultconfig
class Raw(Protocol):
    '''
    Raw protocol, provide two streams for input and output
    '''
    _default_persist = False
    _default_defaultport = 0
    _default_createqueue = True
    # Enable/disable buffering for the output stream.
    # It is dangerous to use buffering in output stream because small amount of data might
    # stay in buffer and not be sent
    _default_buffering = False
    _default_writebufferlimit = 4096
    # Split very large data to chunks to balance the output streaming
    _default_splitsize = 1048576
    _logger = logging.getLogger(__name__ + '.JsonRPC')
    def __init__(self):
        '''
        Constructor
        '''
        Protocol.__init__(self)
    async def init(self, connection):
        await Protocol.init(self, connection)
        connection.createdqueues.append(connection.scheduler.queue.addSubQueue(\
                self.messagepriority, RawConnectionStateEvent.createMatcher(connection = connection), ('connstate', connection)))
        await self.reconnect_init(connection)
    async def _raw_writer(self, connection):
        try:
            while True:
                await connection.outputstream.prepareRead(connection)
                try:
                    data = connection.outputstream.readonce()
                except EOFError:
                    await connection.write(ConnectionWriteEvent(connection, connection.connmark, data=b'', EOF=True))
                    break
                except IOError:
                    await connection.reset()
                    break
                else:
                    await connection.write(ConnectionWriteEvent(connection, connection.connmark, data=data, EOF=False))
        finally:
            connection.outputstream.close(connection.scheduler)
    async def reconnect_init(self, connection):
        connection.inputstream = Stream()
        connection.outputstream = Stream(writebufferlimit=(self.writebufferlimit if self.buffering else 0),
                                         splitsize=self.splitsize)
        connection.subroutine(self._raw_writer(connection), False, '_raw_writer')
        await connection.wait_for_send(RawConnectionStateEvent(RawConnectionStateEvent.CONNECTION_UP, connection, connection.connmark, self))
    async def notconnected(self, connection):
        await Protocol.notconnected(self, connection)
        await connection.waitForSend(RawConnectionStateEvent(RawConnectionStateEvent.CONNECTION_NOTCONNECTED, connection, connection.connmark, self))
    async def closed(self, connection):
        await Protocol.closed(self, connection)
        await connection.inputstream.write(b'', connection, True, True)
        connection.terminate(connection._raw_writer)
        connection._raw_writer = None
        await connection.wait_for_send(RawConnectionStateEvent(RawConnectionStateEvent.CONNECTION_DOWN, connection, connection.connmark, self))
    async def error(self, connection):
        await Protocol.error(self, connection)
        await connection.inputstream.error(connection, True)
        connection.terminate(connection._raw_writer)
        connection._raw_writer = None
        await connection.wait_for_send(RawConnectionStateEvent(RawConnectionStateEvent.CONNECTION_DOWN, connection, connection.connmark, self))
    def statematcher(self, connection, state = RawConnectionStateEvent.CONNECTION_DOWN, currentconn = True):
        if currentconn:
            return RawConnectionStateEvent.createMatcher(state, connection, connection.connmark)
        else:
            return RawConnectionStateEvent.createMatcher(state, connection)
    async def client_connect(self, container, url, *args, **kwargs):
        '''
        Create a connection with raw protocol
        
        :param container: current routine container
        
        :param url: url to connect to (see Client)
        
        :param \*args, \*\*kwargs: other parameters to create a Client (except url, protocol and scheduler)
        
        :returns: `(connection, inputstream, outputstream)` where client is the created connection, inputstream
                  is the stream to read from the socket, outputstream is the stream to write to socket
        '''
        c = Client(url, self, container.scheduler, *args, **kwargs)
        c.start()
        ev, m = await M_(self.statematcher(c, RawConnectionStateEvent.CONNECTION_UP, False), self.statematcher(c, RawConnectionStateEvent.CONNECTION_NOTCONNECTED, False))
        if ev.state == RawConnectionStateEvent.CONNECTION_UP:
            return (c, ev.inputstream, ev.outputstream)
        else:
            raise IOError('Connection failed')
    def redirect_outputstream(self, connection, stream):
        "Close current outputstream and output from the new stream"
        if not connection.connected:
            raise IOError('Connection is closed')
        if connection._raw_writer:
            connection.terminate(connection._raw_writer)
            del connection._raw_writer
        connection.outputstream = stream
        connection.subroutine(self._raw_writer(connection), False, '_raw_writer')
    def parse(self, connection, data, laststart):
        if connection.inputstream and not connection.inputstream.closed:
            if len(data) == laststart:
                return ([StreamDataEvent(connection.inputstream, StreamDataEvent.STREAM_EOF, data=b'')], 0)
            else:
                return ([StreamDataEvent(connection.inputstream, StreamDataEvent.STREAM_DATA, data=_copy(data))], 0)
        else:
            return ([], 0)

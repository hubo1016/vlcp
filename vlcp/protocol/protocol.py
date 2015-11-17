'''
Created on 2015/6/29

@author: hubo
'''
from vlcp.config import Configurable, defaultconfig
from vlcp.event.connection import ConnectionWriteEvent
from vlcp.event.core import syscall_clearqueue, syscall_removequeue, syscall_clearremovequeue
from logging import getLogger
from socket import SOL_SOCKET, SO_ERROR
import errno

@defaultconfig
class Protocol(Configurable):
    '''
    Protocol base class
    '''
    _default_messagepriority = 400
    _default_writepriority = 600
    _default_createqueue = False
    _default_cleanuptimeout = 60
    _default_writequeuesize = 10
    _default_messagequeuesize = 10
    _logger = getLogger(__name__ + '.Protocol')
    def __init__(self):
        '''
        Constructor
        '''
        Configurable.__init__(self)
    def parse(self, connection, data, laststart):
        '''
        Parse input data into events
        @param connection: connection object
        @param data: view for input data
        @param laststart: last parsed position
        @return: (events, keep) where events are parsed events to send, keep is the unused data length to be keeped for next parse.
        '''
        raise NotImplementedError
    def serialize(self, connection, event):
        '''
        Serialize a write event to bytes, and return if it is EOF
        @param connection: connection object
        @param event: write event
        @return: (bytes, EOF)
        '''
        return (event.data, getattr(event, 'EOF', False))
    def init(self, connection):
        '''
        routine for connection initialization
        '''
        try:
            connection.createdqueues = []
            if self.createqueue:
                connection.queue = connection.scheduler.queue.addSubQueue(self.writepriority, ConnectionWriteEvent.createMatcher(connection = connection), ('write', connection), self.writequeuesize)
                connection.createdqueues.append(connection.queue)
        except IndexError:
            pass
        if False:
            yield
    def _clearwritequeue(self, connection):
        if hasattr(connection, 'queue'):
            for m in connection.syscall(syscall_clearqueue(connection.queue)):
                yield m
    def error(self, connection):
        '''
        routine for connection error
        '''
        err = connection.socket.getsockopt(SOL_SOCKET, SO_ERROR)
        self._logger.warning('Connection error status: %d(%s)', err, errno.errorcode.get(err, 'Not found'))
        for m in self._clearwritequeue(connection):
            yield m
    def closed(self, connection):
        '''
        routine for connection closed
        '''
        connection.scheduler.ignore(ConnectionWriteEvent.createMatcher(connection = connection))
        for m in self._clearwritequeue(connection):
            yield m
    def notconnected(self, connection):
        '''
        routine for connect failed and not retrying
        '''
        self._logger.warning('Connect failed and not retrying for url: %s', connection.rawurl)
        if False:
            yield
    def reconnect_init(self, connection):
        '''
        routine for reconnect
        '''
        if False:
            yield
    def accept(self, server, newaddr, newsocket):
        '''
        server accept
        @return: new protocol object
        '''
        self._logger.debug('Connection accepted from ' + repr(newaddr))
        return self
    def final(self, connection):
        '''
        routine for a connection finally ends: all connections are closed and not retrying
        '''
        if hasattr(connection, 'createdqueues') and connection.createdqueues:
            for m in connection.executeWithTimeout(self.cleanuptimeout, connection.waitForAllEmpty(*connection.createdqueues)):
                yield m
            if connection.timeout:
                self._logger.warning('Events are still not processed after timeout, Protocol = %r, Connection = %r', self, connection)
            for q in connection.createdqueues:
                for m in connection.syscall(syscall_clearremovequeue(connection.scheduler.queue, q)):
                    yield m
            del connection.createdqueues[:]
    def beforelisten(self, tcpserver, newsocket):
        '''
        routine before a socket entering listen mode
        '''
        if False:
            yield

    def serverfinal(self, tcpserver):
        '''
        routine for a tcpserver finally shutdown or not connected
        '''
        if False:
            yield


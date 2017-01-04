'''
Created on 2015/6/29

:author: hubo
'''
from vlcp.config import Configurable, defaultconfig
from vlcp.event.connection import ConnectionWriteEvent
from vlcp.event.core import syscall_clearqueue, syscall_removequeue, syscall_clearremovequeue
from logging import getLogger
from socket import SOL_SOCKET, SO_ERROR
from ssl import PROTOCOL_SSLv23
import errno

@defaultconfig
class Protocol(Configurable):
    '''
    Protocol base class
    '''
    # Message event priority for this protocol
    _default_messagepriority = 400
    # Data write event priority for this protocol
    _default_writepriority = 600
    # Create separated queues for data write events from each connection
    _default_createqueue = False
    # Wait before cleanup the created queues for each connection
    _default_cleanuptimeout = 60
    # Data write event queue size for each connection
    _default_writequeuesize = 10
    # Message event queue size for each connection
    _default_messagequeuesize = 10
    # Enable keep-alive for this protocol: send protocol specified keep-alive packages when
    # the connection idles to detect the connection liveness
    _default_keepalivetime = None
    # Use SO_REUSEPORT socket option for the connections, so that multiple processes can bind to
    # the same port; can be used to create load-balanced services
    _default_reuseport = False
    # This protocol should automatically reconnect when the connection is disconnected unexpectedly
    _default_persist = False
    # Default read buffer size for this protocol. When the buffer is not large enough to contain a
    # single message, the buffer will automatically be enlarged.
    _default_buffersize = 4096
    # Connect timeout for this protocol
    _default_connect_timeout = 30
    # Enable TCP_NODELAY option for this protocol
    _default_tcp_nodelay = False
    # Enabled SSL version, default to PROTOCOL_SSLv23, configure it to a TLS version for more security
    _default_sslversion = PROTOCOL_SSLv23
    # Server socket should retry listening if failed to bind to the specified address
    _default_listen_persist = True
    # Retry interval for listen
    _default_retrylisten_interval = 3
    # Default listen backlog size
    _default_backlogsize = 2048
    
    vhost = '<other>'
    _logger = getLogger(__name__ + '.Protocol')
    def __init__(self):
        '''
        Constructor
        '''
        Configurable.__init__(self)
    def parse(self, connection, data, laststart):
        '''
        Parse input data into events
        :param connection: connection object
        :param data: view for input data
        :param laststart: last parsed position
        :returns: (events, keep) where events are parsed events to send, keep is the unused data length to be keeped for next parse.
        '''
        raise NotImplementedError
    def serialize(self, connection, event):
        '''
        Serialize a write event to bytes, and return if it is EOF
        :param connection: connection object
        :param event: write event
        :returns: (bytes, EOF)
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
        :returns: new protocol object
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
    def keepalive(self, connection):
        '''
        routine executed when there has been a long time since last data arrival.
        Check if the connection is down.
        '''
        if False:
            yield

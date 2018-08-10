'''
Created on 2016/9/13

:author: think
'''

from vlcp.protocol.protocol import Protocol
from vlcp.config import defaultconfig
from vlcp.event import Event, withIndices, ConnectionWriteEvent
from collections import deque
from vlcp.utils.zookeeper import ZooKeeperRequest, ZooKeeperReply, CONNECT_PACKET, HEADER_PACKET,\
        WATCHER_EVENT_XID, ZOO_SYNC_CONNECTED_STATE
import vlcp.utils.zookeeper as zk
import logging
import os
from vlcp.event.connection import ConnectionResetException
from contextlib import closing
from namedstruct import dump
from json import dumps
from vlcp.event.ratelimiter import RateLimiter
from vlcp.event.future import RoutineFuture
from vlcp.event.runnable import RoutineException
from vlcp.event.runnable import _close_generator

@withIndices('state', 'connection', 'connmark', 'createby')
class ZooKeeperConnectionStateEvent(Event):
    UP = 'up'
    DOWN = 'down'
    NOTCONNECTED = 'notconnected'

@withIndices('connection', 'connmark', 'createby')
class ZooKeeperMessageEvent(Event):
    pass

@withIndices('xid')
class ZooKeeperResponseEvent(ZooKeeperMessageEvent):
    pass

@withIndices('type', 'state', 'path')
class ZooKeeperWatcherEvent(ZooKeeperMessageEvent):
    pass

@withIndices()
class ZooKeeperHandshakeEvent(ZooKeeperMessageEvent):
    pass

class ZooKeeperProtocolException(Exception):
    pass

class ZooKeeperException(Exception):
    pass

class ZooKeeperRetryException(ZooKeeperException):
    '''
    Connection lost or not connected on handshake
    '''
    pass

class ZooKeeperSessionExpiredException(ZooKeeperException):
    '''
    Handshake reports the session is expired
    '''
    pass

class ZooKeeperRequestTooLargeException(ZooKeeperException):
    '''
    Request is too large, which may break every thing, so we reject it
    '''
    pass

@withIndices('priority')
class ZooKeeperWriteEvent(ConnectionWriteEvent):
    LOW = 0
    MIDDLE = 1
    HIGH = 2

@defaultconfig
class ZooKeeper(Protocol):
    '''
    ZooKeeper protocol
    '''
    # Usually we should connect to another server, this is done by ZooKeeperClient
    _default_persist = False
    # default ZooKeeper port
    _default_defaultport = 2181
    _default_createqueue = True
    _default_buffersize = 524288
    # Limit the response and watcher queue size
    _default_messagequeuesize = 1024
    # Send ping command when the connection is idle
    _default_keepalivetime = None
    _default_writekeepalivetime = 5
    # Disconnect when the ping command does not receive response
    _default_keepalivetimeout = 10
    _default_connect_timeout = 5
    _default_tcp_nodelay = True
    # Limit the data write queue size
    _default_writequeuesize = 1024
    _logger = logging.getLogger(__name__ + '.ZooKeeper')
    def __init__(self):
        Protocol.__init__(self)

    async def init(self, connection):
        await Protocol.init(self, connection)
        connection.createdqueues.append(connection.scheduler.queue.addSubQueue(\
                self.messagepriority, ZooKeeperConnectionStateEvent.createMatcher(connection = connection), ('connstate', connection)))
        connection.createdqueues.append(connection.scheduler.queue.addSubQueue(\
                self.messagepriority + 1, ZooKeeperMessageEvent.createMatcher(connection = connection), ('message', connection), self.messagequeuesize))
        connection.createdqueues.append(connection.scheduler.queue.addSubQueue(\
                self.messagepriority + 2, ZooKeeperResponseEvent.createMatcher(connection = connection, xid = zk.PING_XID)))
        if hasattr(connection, 'queue'):
            connection.queue.addSubQueue(1, ZooKeeperWriteEvent.createMatcher(connection = connection,
                                                                                        priority = ZooKeeperWriteEvent.MIDDLE),
                                         None, self.writequeuesize)
            connection.queue.addSubQueue(2, ZooKeeperWriteEvent.createMatcher(connection = connection,
                                                                                        priority = ZooKeeperWriteEvent.HIGH),
                                         None, self.writequeuesize)
        # Use limiter to limit the request serialization in one iteration
        connection._rate_limiter = (RateLimiter(256, connection), RateLimiter(256, connection))
        await self.reconnect_init(connection)

    async def reconnect_init(self, connection):
        await Protocol.reconnect_init(self, connection)
        connection.xid = ord(os.urandom(1)) + 1
        connection.zookeeper_requests = {}
        connection.zookeeper_handshake = False
        connection.zookeeper_lastzxid = 0
        connection.zookeeper_last_watch_zxid = 0
        await connection.wait_for_send(ZooKeeperConnectionStateEvent(ZooKeeperConnectionStateEvent.UP,
                                                                      connection,
                                                                      connection.connmark,
                                                                      self))

    def serialize(self, connection, event):
        event._zookeeper_sent = True
        return Protocol.serialize(self, connection, event)

    def parse(self, connection, data, laststart):
        events = []
        start = 0
        while True:
            result = ZooKeeperReply.parse(data[start:])
            if result is None:
                break
            reply, size = result
            start += size
            if not connection.zookeeper_handshake:
                reply.zookeeper_type = CONNECT_PACKET
                reply._autosubclass()
                connection.zookeeper_handshake = True
                events.append(ZooKeeperHandshakeEvent(connection, connection.connmark, self, message = reply))
            else:
                reply.zookeeper_type = HEADER_PACKET
                reply._autosubclass()
                if reply.zxid > 0:
                    connection.zookeeper_lastzxid = reply.zxid
                if reply.xid >= 0:
                    xid = reply.xid
                    if xid not in connection.zookeeper_requests:
                        raise ZooKeeperProtocolException('xid does not match: receive %r' % (reply.xid,))
                    request_type = connection.zookeeper_requests.pop(xid)
                    reply.zookeeper_request_type = request_type
                    reply._autosubclass()
                if reply.xid == WATCHER_EVENT_XID:
                    connection.zookeeper_last_watch_zxid = connection.zookeeper_lastzxid
                    reply.last_zxid = connection.zookeeper_lastzxid
                    if reply.state != ZOO_SYNC_CONNECTED_STATE:
                        self._logger.warning("Receive abnormal watch event: %s", dumps(dump(reply, tostr=True)))
                    events.append(ZooKeeperWatcherEvent(connection, connection.connmark,
                                                        self, reply.type, reply.state, b'' if reply.path is None else reply.path,
                                                        message = reply))
                else:
                    events.append(ZooKeeperResponseEvent(connection, connection.connmark,
                                                         self, reply.xid, message = reply))
        if laststart == len(data):
            # Remote write close
            events.append(ConnectionWriteEvent(connection, connection.connmark, data = b'', EOF = True))
        return (events, len(data) - start)

    def _send(self, connection, request, container, priority = 0):
        return self._senddata(connection, request._tobytes(), container, priority)

    async def _senddata(self, connection, data, container = None, priority = 0):
        connwrite = ZooKeeperWriteEvent(connection, connection.connmark, priority, data = data)
        connwrite._zookeeper_sent = False
        try:
            await connection.write(connwrite, False)
        except ConnectionResetException:
            raise ZooKeeperRetryException
        return connwrite

    def _pre_assign_xid(self, connection, request):
        if hasattr(request, 'xid') and request.xid >= 0:
            connection.xid += 1
            if connection.xid > 0x7fffffff:
                connection.xid = 1
            request.xid = connection.xid
        return request.xid

    def _register_xid(self, connection, request):
        if hasattr(request, 'xid') and request.xid >= 0:
            connection.zookeeper_requests[request.xid] = request.type

    async def handshake(self, connection, connrequest, container, extrapackets = []):
        connmark = connection.connmark
        handshake_matcher = ZooKeeperHandshakeEvent.createMatcher(connection, connection.connmark)
        await self._send(connection, connrequest, container, ZooKeeperWriteEvent.HIGH)
        handshake_received = [None]
        if extrapackets:
            def callback(event, matcher):
                handshake_received[0] = event.message
            timeout, r = await container.execute_with_timeout(
                            10,
                            container.with_callback(
                                self.requests(connection, extrapackets, container, priority=ZooKeeperWriteEvent.HIGH),
                                callback,
                                handshake_matcher
                            )
                        )
            if timeout:
                raise ZooKeeperRetryException
            receive, lost, retry = r
            if lost or retry:
                if handshake_received[0] is not None:
                    # We have received the handshake packet, but not the extra responses
                    # Maybe the session is expired
                    if handshake_received[0].timeOut <= 0:
                        raise ZooKeeperSessionExpiredException
                raise ZooKeeperRetryException
        else:
            receive = []
        if handshake_received[0] is None:
            if not connection.connected or connection.connmark != connmark:
                raise ZooKeeperRetryException
            else:
                conn_matcher = ZooKeeperConnectionStateEvent.createMatcher(ZooKeeperConnectionStateEvent.DOWN,
                                                                           connection,
                                                                           connmark)
                timeout, ev, m = await container.wait_with_timeout(10, handshake_matcher, conn_matcher)
                if timeout:
                    self._logger.warning('Handshake timeout, connection = %r', connection)
                    raise ZooKeeperRetryException
                elif m is conn_matcher:
                    raise ZooKeeperRetryException
                else:
                    handshake_received[0] = ev.message
                    if handshake_received[0].timeOut <= 0:
                        raise ZooKeeperSessionExpiredException
        return (handshake_received[0], receive)

    async def async_requests(self, connection, requests, container = None, priority = 0):
        '''
        :return: (matchers, sendall), where matchers are event matchers for the requests; sendall
                 is an async function to send to requests. Use `await sendall()` to send the requests.
        '''
        matchers = []
        for r in requests:
            xid = self._pre_assign_xid(connection, r)
            resp_matcher = ZooKeeperResponseEvent.createMatcher(connection, connection.connmark, None, xid)
            matchers.append(resp_matcher)
        alldata = []
        for i in range(0, len(requests), 100):
            size = min(100, len(requests) - i)
            if priority < ZooKeeperWriteEvent.HIGH:
                await connection._rate_limiter[priority].limit(size)
            for j in range(i, i + size):
                r = requests[j]
                data = r._tobytes()
                if len(data) >= 0xfffff:
                    # This is the default limit of ZooKeeper, reject this request
                    raise ZooKeeperRequestTooLargeException('The request is %d bytes which is too large for ZooKeeper' % len(data))
                alldata.append(data)
        for r in requests:
            self._register_xid(connection, r)
        async def _sendall():
            sent_requests = []
            for data in alldata:
                try:
                    sent_requests.append(await self._senddata(connection, data, container, priority))
                except ZooKeeperRetryException:
                    raise ZooKeeperRetryException(sent_requests)
            return sent_requests
        return (matchers, _sendall)

    async def requests(self, connection, requests, container = None, callback = None, priority = 0):
        '''
        Send requests by sequence, return all the results (including the lost ones)
        
        :params connection: ZooKeeper connection
        
        :params requests: a sequence of ZooKeeper requests
        
        :params container: routine container of current routine
        
        :params callback: if not None, `callback(request, response)` is called immediately after
                          each response received
        
        :return: `(responses, lost_responses, retry_requests)`, where responses is a list of responses corresponded
                    to the requests (None if response is not received); lost_responses is a list of requests that are sent
                    but the responses are lost due to connection lost, it is the caller's responsibility to determine whether
                    the call is succeeded or failed; retry_requests are the requests which are not sent and are safe to retry. 
        '''
        matchers, _sendall = await self.async_requests(connection, requests, container, priority)
        requests_dict = dict((m,r) for m,r in zip(matchers, requests))
        connmark = connection.connmark
        if not connection.connected:
            
            return (), (), requests
        conn_matcher = ZooKeeperConnectionStateEvent.createMatcher(ZooKeeperConnectionStateEvent.DOWN,
                                                                   connection,
                                                                   connmark)
        replydict = {}
        if callback:
            def matcher_callback(event, matcher):
                callback(requests_dict[matcher], event.message)
        else:
            matcher_callback = None
        wait_result = RoutineFuture(
                            connection.with_exception(
                                    connection.wait_for_all(
                                        *matchers,
                                        eventdict=replydict,
                                        callback=matcher_callback
                                    ),
                                    conn_matcher
                                ),
                            connection
                        )
        try:
            try:
                sent_events = await _sendall()
            except ZooKeeperRetryException as exc:
                sent_events = exc.args[0]
            try:
                await wait_result
            except RoutineException:
                receive_all = False
            else:
                receive_all = True
            responses = [replydict[m].message if m in replydict else None for m in matchers]
            received_responses = dict((k,v) for k,v in zip(requests, responses) if v is not None)
            if receive_all:
                return (responses, [], [])
            else:
                # Some results are missing
                lost_responses = [r for r,c in zip(requests, sent_events) if c._zookeeper_sent and r not in received_responses]
                retry_requests = [r for r,c in zip(requests, sent_events) if not c._zookeeper_sent] + requests[len(sent_events):]
                return (responses, lost_responses, retry_requests)
        finally:
            wait_result.close()

    async def error(self, connection):
        await connection.wait_for_send(ZooKeeperConnectionStateEvent(ZooKeeperConnectionStateEvent.DOWN,
                                                                      connection,
                                                                      connection.connmark,
                                                                      self))
        await Protocol.error(self, connection)

    async def closed(self, connection):
        await connection.wait_for_send(ZooKeeperConnectionStateEvent(ZooKeeperConnectionStateEvent.DOWN,
                                                                      connection,
                                                                      connection.connmark,
                                                                      self))
        await Protocol.closed(self, connection)

    async def notconnected(self, connection):
        await connection.wait_for_send(ZooKeeperConnectionStateEvent(ZooKeeperConnectionStateEvent.NOTCONNECTED,
                                                                      connection,
                                                                      connection.connmark,
                                                                      self))
        await Protocol.notconnected(self, connection)

    async def keepalive(self, connection):
        try:
            timeout, result = await connection.execute_with_timeout(
                                    self.keepalivetimeout,
                                    self.requests(connection,
                                        [zk.RequestHeader(xid = zk.PING_XID, type = zk.ZOO_PING_OP)],
                                        connection,
                                        priority=ZooKeeperWriteEvent.HIGH)
                                )
            if timeout:
                await connection.reset(True)
            else:
                _, lost, retry = result
                if lost or retry:
                    await connection.reset(True)
        except Exception as exc:
            await connection.reset(True)
            raise exc

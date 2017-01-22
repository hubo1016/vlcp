'''
Created on 2015/8/12

:author: hubo
'''

from vlcp.protocol import Protocol
from vlcp.config import defaultconfig
from vlcp.event import Event, withIndices, ConnectionWriteEvent
import logging
import os
import json
import re

@withIndices('state', 'connection', 'connmark', 'createby')
class JsonRPCConnectionStateEvent(Event):
    """
    Connection state change
    """
    # Connection up
    CONNECTION_UP = 'up'
    # Connection down
    CONNECTION_DOWN = 'down'


@withIndices('method', 'connection', 'connmark', 'createby')
class JsonRPCRequestEvent(Event):
    """
    Request received from the connection
    """
    canignore = False
    def canignorenow(self):
        return not self.connection.connected or self.connection.connmark != self.connmark

@withIndices('connection', 'connmark', 'id', 'iserror', 'createby')
class JsonRPCResponseEvent(Event):
    """
    Response received from the connection
    """
    pass

@withIndices('method', 'connection', 'connmark', 'createby')
class JsonRPCNotificationEvent(Event):
    """
    Notification received from the connection
    """
    pass

class JsonFormatException(Exception):
    pass

class JsonRPCProtocolException(Exception):
    pass

class JsonRPCErrorResultException(Exception):
    pass

@defaultconfig
class JsonRPC(Protocol):
    '''
    JSON-RPC 1.0 Protocol
    '''
    _default_persist = True
    # This is the OVSDB default port
    _default_defaultport = 6632
    _default_createqueue = True
    # Print debugging log
    _default_debugging = False
    # JSON encoding
    _default_encoding = 'utf-8'
    _default_buffersize = 65536
    # Default limit a JSON message to 16MB for security purpose
    _default_messagelimit = 16777216
    # Default limit JSON scan level to 1024 levels
    _default_levellimit = 1024
    # Limit the allowed request methods
    _default_allowedrequests = None
    _logger = logging.getLogger(__name__ + '.JsonRPC')
    def __init__(self):
        '''
        Constructor
        '''
        Protocol.__init__(self)
    def init(self, connection):
        for m in Protocol.init(self, connection):
            yield m
        connection.createdqueues.append(connection.scheduler.queue.addSubQueue(\
                self.messagepriority, JsonRPCRequestEvent.createMatcher(connection = connection), ('request', connection), self.messagequeuesize))
        connection.createdqueues.append(connection.scheduler.queue.addSubQueue(\
                self.messagepriority, JsonRPCConnectionStateEvent.createMatcher(connection = connection), ('connstate', connection)))
        connection.createdqueues.append(connection.scheduler.queue.addSubQueue(\
                self.messagepriority + 1, JsonRPCResponseEvent.createMatcher(connection = connection), ('response', connection), self.messagequeuesize))
        connection.createdqueues.append(connection.scheduler.queue.addSubQueue(\
                self.messagepriority, JsonRPCNotificationEvent.createMatcher(connection = connection), ('notification', connection), self.messagequeuesize))
        for m in self.reconnect_init(connection):
            yield m
    def reconnect_init(self, connection):
        connection.xid = ord(os.urandom(1)) + 1
        connection.jsonrpc_parserlevel = 0
        connection.jsonrpc_parserstate = 'begin'
        for m in connection.waitForSend(JsonRPCConnectionStateEvent(JsonRPCConnectionStateEvent.CONNECTION_UP, connection, connection.connmark, self)):
            yield m
    def closed(self, connection):
        for m in Protocol.closed(self, connection):
            yield m
        connection.scheduler.ignore(JsonRPCRequestEvent.createMatcher(connection = connection))
        self._logger.info('JSON-RPC connection is closed on %r', connection)
        for m in connection.waitForSend(JsonRPCConnectionStateEvent(JsonRPCConnectionStateEvent.CONNECTION_DOWN, connection, connection.connmark, self)):
            yield m
    def error(self, connection):
        for m in Protocol.error(self, connection):
            yield m
        connection.scheduler.ignore(JsonRPCRequestEvent.createMatcher(connection = connection))
        self._logger.warning('JSON-RPC connection is reset on %r', connection)
        for m in connection.waitForSend(JsonRPCConnectionStateEvent(JsonRPCConnectionStateEvent.CONNECTION_DOWN, connection, connection.connmark, self)):
            yield m
    _BEGIN_PATTERN = re.compile(br'\s*')
    _OBJECT_PATTERN = re.compile(br'[^"{}]*')
    _STRING_PATTERN = re.compile(br'[^"^\\]*')
    def formatrequest(self, method, params, connection):
        msgid = connection.xid
        msg = {'method': method, 'params': params, 'id': msgid}
        connection.xid += 1
        if connection.xid > 0x7fffffff:
            # Skip xid = 0 for special response
            connection.xid = 1
        c = ConnectionWriteEvent(connection = connection, connmark = connection.connmark, data = json.dumps(msg).encode(self.encoding))
        if self.debugging:
            self._logger.debug('message formatted: %r', msg)
        return (c, msgid)
    def formatnotification(self, method, params, connection):
        msg = {'method': method, 'params': params, 'id': None}
        c = ConnectionWriteEvent(connection = connection, connmark = connection.connmark, data = json.dumps(msg).encode(self.encoding))
        if self.debugging:
            self._logger.debug('message formatted: %r', msg)
        return c
    def formatreply(self, result, requestid, connection):
        msg = {'result': result, 'error': None, 'id': requestid}
        c = ConnectionWriteEvent(connection = connection, connmark = connection.connmark, data = json.dumps(msg).encode(self.encoding))
        if self.debugging:
            self._logger.debug('message formatted: %r', msg)
        return c
    def formaterror(self, error, requestid, connection):
        msg = {'result': None, 'error': error, 'id': requestid}
        c = ConnectionWriteEvent(connection = connection, connmark = connection.connmark, data = json.dumps(msg).encode(self.encoding))
        if self.debugging:
            self._logger.debug('message formatted: %r', msg)
        return c
    def replymatcher(self, requestid, connection, iserror = None):
        """
        Create a matcher to match a reply
        """
        matcherparam = {'connection' : connection, 'connmark': connection.connmark, 
                        'id': requestid}
        if iserror is not None:
            matcherparam['iserror'] = iserror
        return JsonRPCResponseEvent.createMatcher(**matcherparam)
    def notificationmatcher(self, method, connection):
        """
        Create an event matcher to match specified notifications
        """
        return JsonRPCNotificationEvent.createMatcher(method = method, connection = connection, connmark = connection.connmark)
    def statematcher(self, connection, state = JsonRPCConnectionStateEvent.CONNECTION_DOWN, currentconn = True):
        """
        Create an event matcher to match the connection state
        """
        if currentconn:
            return JsonRPCConnectionStateEvent.createMatcher(state, connection, connection.connmark)
        else:
            return JsonRPCConnectionStateEvent.createMatcher(state, connection)
    def querywithreply(self, method, params, connection, container, raiseonerror = True):
        """
        Send a JSON-RPC request and wait for the reply. The reply result is stored at
        `container.jsonrpc_result` and the reply error is stored at `container.jsonrpc_error`.
        """
        (c, rid) = self.formatrequest(method, params, connection)
        for m in connection.write(c, False):
            yield m
        reply = self.replymatcher(rid, connection)
        conndown = self.statematcher(connection)
        yield (reply, conndown)
        if container.matcher is conndown:
            raise JsonRPCProtocolException('Connection is down before reply received')
        container.jsonrpc_result = container.event.result
        container.jsonrpc_error = container.event.error
        if raiseonerror and container.event.error:
            raise JsonRPCErrorResultException(str(container.event.error))
    def waitfornotify(self, method, connection, container):
        """
        Wait for next notification
        """
        notify = self.notificationmatcher(method, connection)
        conndown = self.statematcher(connection)
        yield (notify, conndown)
        if container.matcher is conndown:
            raise JsonRPCProtocolException('Connection is down before notification received')
        container.jsonrpc_notifymethod = container.event.method
        container.jsonrpc_notifyparams = container.event.params
    def parse(self, connection, data, laststart):
        jsonstart = 0
        start = laststart
        end = len(data)
        events = []
        level = connection.jsonrpc_parserlevel
        state = connection.jsonrpc_parserstate
        _OBJECT_START = b'{'[0]
        _STRING_MARK = b'"'[0]
        _ESCAPE_MARK = b'\\'[0]
        _OBJECT_END = b'}'[0]
        while start < end:
            # We only match {} to find the end position
            if state == 'begin':
                m = self._BEGIN_PATTERN.match(data, start)
                start = m.end()
                if start < end:
                    if data[start] == _OBJECT_START:
                        start += 1
                        level += 1
                        state = 'object'
                    else:
                        raise JsonFormatException('"{" is not found')
            elif state == 'object':
                m = self._OBJECT_PATTERN.match(data, start)
                start = m.end()
                if start < end:
                    if data[start] == _STRING_MARK:
                        start += 1
                        state = 'string'
                    elif data[start] == _OBJECT_START:
                        start += 1
                        level += 1
                    elif data[start] == _OBJECT_END:
                        start += 1
                        level -= 1
                        if level <= 0:
                            state = 'begin'
                            jsondata = data[jsonstart:start]
                            if hasattr(jsondata, 'tobytes'):
                                jsondata = jsondata.tobytes()
                            jsondata = jsondata.decode(self.encoding)
                            if self.debugging:
                                self._logger.debug('Parsing json text:\n%s', jsondata)
                            jsondata = json.loads(jsondata)
                            if 'method' in jsondata:
                                if jsondata['method'] is None:
                                    raise JsonFormatException('method is None in input json')
                                if jsondata['id'] is not None:
                                    # Unprocessed requests will block the JSON-RPC connection message queue,
                                    # as a security consideration, the parser can automatically reject unknown
                                    # requests
                                    if self.allowedrequests is not None and str(jsondata['method']) not in self.allowedrequests:
                                        events.append(self.formaterror('method is not supported', jsondata['id'], connection))
                                    else:
                                        events.append(JsonRPCRequestEvent(method = str(jsondata['method']), params = jsondata['params'],
                                                                          id = jsondata['id'], connection = connection, connmark = connection.connmark, createby = self))
                                        self._logger.debug('Request received(method = %r, id = %r, connection = %r)', jsondata['method'], jsondata['id'], connection)
                                else:
                                    events.append(JsonRPCNotificationEvent(method = str(jsondata['method']), params = jsondata['params'],
                                                                      connection = connection, connmark = connection.connmark, createby = self))
                                    self._logger.debug('Notification received(method = %r, connection = %r)', str(jsondata['method']), connection)
                            elif 'result' in jsondata:
                                if jsondata['id'] is None:
                                    raise JsonFormatException('id is None for a response')
                                events.append(JsonRPCResponseEvent(connection = connection, connmark = connection.connmark,
                                                                   id = jsondata['id'], iserror = jsondata['error'] is not None,
                                                                   result = jsondata['result'], error = jsondata['error'], createby = self))
                                self._logger.debug('Response received(id = %r, connection = %r)', jsondata['id'], connection)
                            jsonstart = start
                    else:
                        # Never really reach
                        raise JsonFormatException('How can this be reached...')
            elif state == 'string':
                m = self._STRING_PATTERN.match(data, start)
                start = m.end()
                if start < end:
                    if data[start] == _STRING_MARK:
                        start += 1
                        state = 'object'
                    elif data[start] == _ESCAPE_MARK:
                        start += 1
                        state = 'escape'
                    else:
                        # Never really reach
                        raise JsonFormatException('How can this be reached...')
            else:
                # Escape
                start += 1
                state = 'string'
            # Security check
            if start - jsonstart > self.messagelimit:
                raise JsonFormatException('JSON message size exceeds limit')
            if level > self.levellimit:
                raise JsonFormatException('JSON message level exceeds limit')
        connection.jsonrpc_parserlevel = level
        connection.jsonrpc_parserstate = state
        if laststart == len(data):
            # Remote write close
            events.append(ConnectionWriteEvent(connection, connection.connmark, data = b'', EOF = True))
        return (events, len(data) - jsonstart)
    

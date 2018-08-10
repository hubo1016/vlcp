'''
Created on 2015/7/8

:author: hubo
'''
from vlcp.protocol import Protocol
from vlcp.config import defaultconfig
from .defs import common, openflow10
from .defs.definations import definations
from vlcp.event.core import Event, withIndices, QuitException
from vlcp.event.runnable import RoutineContainer, RoutineException
from vlcp.event.connection import ConnectionWriteEvent, ConnectionControlEvent
import logging
import os
from namedstruct import dump
from contextlib import closing
from vlcp.event.ratelimiter import RateLimiter
from vlcp.event.event import M_


@withIndices('datapathid', 'auxiliaryid', 'state', 'connection', 'connmark', 'createby')
class OpenflowConnectionStateEvent(Event):
    """
    Event when connection state changes
    """
    # Connection Setup
    CONNECTION_SETUP = 'setup'
    # Connection Down
    CONNECTION_DOWN = 'down'

@withIndices('datapathid', 'auxiliaryid', 'connection', 'connmark', 'xid', 'iserror', 'createby')
class OpenflowResponseEvent(Event):
    """
    Event for an OpenFlow response is received
    """
    pass

@withIndices('type', 'datapathid', 'auxiliaryid', 'tableid', 'cookie', 'connection', 'connmark', 'createby')
class OpenflowAsyncMessageEvent(Event):
    """
    Event for an async message is received
    """
    pass

@withIndices('connection', 'type', 'connmark', 'createby')
class OpenflowPresetupMessageEvent(Event):
    """
    Event for messages before connection setup
    """
    pass

@withIndices('experimenter', 'exptype', 'datapathid', 'auxiliaryid', 'connection', 'connmark', 'createby')
class OpenflowExperimenterMessageEvent(Event):
    """
    Event for experimenter messages
    """
    pass

class OpenflowProtocolException(Exception):
    """
    Critical protocol break exception
    """
    pass

class OpenflowErrorResultException(Exception):
    """
    OpenFlow returns error
    """
    def __init__(self, errormsg, prompt = 'An error message is returned: ', result = None):
        if result is None:
            result = errormsg
        Exception.__init__(self, prompt + repr(dump(errormsg)))
        self.result = result

@defaultconfig
class Openflow(Protocol):
    '''
    Openflow control protocol
    '''
    _default_persist = True
    # Default OpenFlow port
    _default_defaultport = common.OFP_TCP_PORT
    # Allowed versions for OpenFlow handshake; should be one or both of OFP10_VERSION and OFP13_VERSION
    _default_allowedversions = (common.OFP10_VERSION, common.OFP13_VERSION)
    # Disconnect when OFPT_HELLO message is not received for a long time
    _default_hellotimeout = 10
    # Disconnect when OFPT_FEATURES_REQUEST message does not get response
    _default_featurerequesttimeout = 30
    # Send OFPT_ECHO packet when connection is idle
    _default_keepalivetime = 10
    # When OFPT_ECHO packet does not get response in the specified time, disconnect
    _default_keepalivetimeout = 3
    _default_createqueue = True
    _default_writequeuesize = 200
    _default_buffersize = 65536
    # Show debugging messages in log
    _default_debugging = False
    # Disable nicira extension; when you are not using VLCP with OpenvSwitch (e.g. using with
    # physical switches) you should set this to True 
    _default_disablenxext = False
    # Disable using ofp_action_group in another group: this is only supported in OpenvSwitch 2.5+,
    # so it is disabled by default
    _default_disablechaining = True
    _logger = logging.getLogger(__name__ + '.Openflow')
    _default_tcp_nodelay = True
    def __init__(self, allowedVersions = None):
        '''
        Constructor
        
        :param allowedVersions: if specified, should be a tuple of allowed OpenFlow versions.
        '''
        Protocol.__init__(self)
        if allowedVersions is not None:
            self.allowedversions = allowedVersions
    async def handler(self, connection):
        try:
            # Create a hello message
            connmark = connection.connmark
            hello = common.ofp_hello.new()
            hello.header.version = max(self.allowedversions)
            versionbitmap = common.ofp_hello_elem_versionbitmap.new()
            versionStart = 0
            thisBitmap = 0
            for v in sorted(self.allowedversions):
                while v > versionStart + 32:
                    versionbitmap.bitmaps.append(thisBitmap)
                    thisBitmap = 0
                    versionStart += 32
                thisBitmap = thisBitmap | (1<<(v - versionStart))
            versionbitmap.bitmaps.append(thisBitmap)
            hello.elements.append(versionbitmap)
            write = self.formatrequest(hello, connection)
            await connection.write(write, False)
            # Wait for a hello
            hellomatcher = OpenflowPresetupMessageEvent.createMatcher(connection = connection)
            timeout, ev, _ = await connection.wait_with_timeout(self.hellotimeout, hellomatcher)
            if timeout:
                # Drop the connection
                raise OpenflowProtocolException('Did not receive hello message before timeout')
            else:
                msg = ev.message
                if msg.header.type != common.OFPT_HELLO:
                    raise OpenflowProtocolException('The first packet on this connection is not OFPT_HELLO')
                else:
                    helloversion = None
                    usebitmap = False
                    for e in msg.elements:
                        if e.type == common.OFPHET_VERSIONBITMAP:
                            # There is a bitmap
                            for v in reversed(sorted(self.allowedversions)):
                                bitmapIndex = v // 32
                                bitmapPos = (v & 31)
                                if len(e.bitmaps) < bitmapIndex:
                                    continue
                                if e.bitmaps[bitmapIndex] & (1 << bitmapPos):
                                    helloversion = v
                                    break
                            usebitmap = True
                            break
                    if not usebitmap:
                        helloversion = min(max(self.allowedversions), msg.header.version)
                    if helloversion is None or helloversion not in self.allowedversions:
                        self._logger.warning('Remote switch openflow protocol version is not compatible. Their hello message: %r, we expect version: %r. Connection = %r', common.dump(msg), self.allowedversions, connection)
                        # Hello fail
                        hellofail = common.ofp_error_msg.new()
                        hellofail.header.version = max(self.allowedversions)
                        hellofail.type = common.OFPET_HELLO_FAILED
                        hellofail.code = common.OFPHFC_INCOMPATIBLE
                        if helloversion is None:
                            hellofail.data = b'A common version is not found from the bitmap\x00'
                        else:
                            hellofail.data = ('Openflow version is not supported\x00' % (common.ofp_version.getName(helloversion, str(helloversion)),)).encode()
                        write = self.formatreply(hellofail, msg, connection)
                        await connection.write(write)
                        await connection.reset(False, connmark)
                        return
                    else:
                        # Still we may receive a hello fail from the other side, we should expect that.
                        # The error message may come before feature request is sent.
                        err_matcher = OpenflowPresetupMessageEvent.createMatcher(connection = connection, type = common.OFPT_ERROR)
                        # Send a feature request message
                        connection.openflowversion = helloversion
                        currdef = definations[helloversion]
                        connection.openflowdef = currdef
                        # Feature request message has no body
                        featurereq = currdef.ofp_msg.new()
                        featurereq.header.type = currdef.OFPT_FEATURES_REQUEST
                        write = self.formatrequest(featurereq, connection)
                        try:
                            await connection.with_exception(connection.write(write, False), err_matcher)
                            featurereply_matcher = OpenflowPresetupMessageEvent.createMatcher(connection = connection, type = currdef.OFPT_FEATURES_REPLY)
                            timeout, ev, m = await connection.wait_with_timeout(self.featurerequesttimeout, featurereply_matcher, err_matcher)
                            if timeout:
                                raise OpenflowProtocolException('Remote switch did not response to feature request.')
                            elif m is err_matcher:
                                self._logger.warning('Error while request feature: %r Connection = %r', ev.message, connection)
                                raise OpenflowProtocolException('Error while request feature: %r' % (ev.message,))
                            else:
                                msg = ev.message
                                connection.openflow_featuresreply = msg
                                connection.openflow_datapathid = msg.datapath_id
                                connection.openflow_auxiliaryid = getattr(msg, 'auxiliary_id', 0)
                                connection.openflow_capabilities = msg.capabilities
                                connection.openflow_n_buffers = msg.n_buffers
                                connection.openflow_n_tables = msg.n_tables
                                statechange = OpenflowConnectionStateEvent(connection.openflow_datapathid, connection.openflow_auxiliaryid, OpenflowConnectionStateEvent.CONNECTION_SETUP, connection, connection.connmark, self)
                                await connection.wait_for_send(statechange)
                                for msg in connection.openflow_msgbuffer:
                                    e = self._createevent(connection, msg)
                                    if e is not None:
                                        await connection.wait_for_send(e)
                        except RoutineException as exc:
                            self._logger.warning('Remote report hello fail: %r Connection = %r', common.dump(exc.arguments[1].message), connection)
                            await connection.reset(True, connmark)
                            return
        except QuitException:
            pass
        except:
            self._logger.exception('Unexpected exception on processing openflow protocols, Connection = %r', connection)
            async def _cleanup():
                await connection.reset(True, connmark)
            connection.subroutine(_cleanup(), False)
            raise
    def formatrequest(self, request, connection, assignxid = True):
        if assignxid:
            self.assignxid(request, connection)
        c = ConnectionWriteEvent(connection = connection, connmark = connection.connmark, data = request._tobytes())
        if self.debugging:
            self._logger.debug('message formatted: %r', common.dump(request))
        return c
    def assignxid(self, request, connection):
        request.header.xid = connection.xid
        connection.xid += 1
        if connection.xid > 0xffffffffffffffff:
            # Skip xid = 0 for special response
            connection.xid = 1        
    def formatreply(self, reply, request, connection):
        reply.header.xid = request.header.xid
        c = ConnectionWriteEvent(connection = connection, connmark = connection.connmark, data = reply._tobytes())
        if self.debugging:
            self._logger.debug('message formatted: %r', common.dump(request))
        return c
    def replymatcher(self, request, connection, iserror = None):
        """
        Create an event matcher to match a reply to this request
        """
        matcherparam = {'datapathid': connection.openflow_datapathid, 'auxiliaryid': connection.openflow_auxiliaryid, 'connection' : connection, 'connmark': connection.connmark, 
                        'xid': request.header.xid}
        if iserror is not None:
            matcherparam['iserror'] = iserror
        return OpenflowResponseEvent.createMatcher(**matcherparam)
    def statematcher(self, connection, state = OpenflowConnectionStateEvent.CONNECTION_DOWN, currentconn = True):
        """
        Create an event matcher to match the connection state of this connection
        """
        if currentconn:
            return OpenflowConnectionStateEvent.createMatcher(connection.openflow_datapathid, connection.openflow_auxiliaryid, state, connection, connection.connmark)
        else:
            return OpenflowConnectionStateEvent.createMatcher(connection.openflow_datapathid, connection.openflow_auxiliaryid, state)
    async def querywithreply(self, request, connection, container = None, raiseonerror = True):
        """
        Send an OpenFlow normal request, wait for the response of this request. The request must have
        exactly one response.
        """
        await connection.write(self.formatrequest(request, connection), False)
        reply = self.replymatcher(request, connection)
        conndown = self.statematcher(connection)
        ev, m = await M_(reply, conndown)
        if m is conndown:
            raise OpenflowProtocolException('Connection is down before reply received')
        if ev.iserror:
            raise OpenflowErrorResultException(ev.message)
        else:
            return ev.message

    async def querymultipart(self, request, connection, container = None, raiseonerror = True):
        """
        Send a multipart request, wait for all the responses. Return a list of reply messages
        """
        await connection.write(self.formatrequest(request, connection), False)
        reply = self.replymatcher(request, connection)
        conndown = self.statematcher(connection)
        messages = []
        while True:
            ev, m = await M_(reply, conndown)
            if m is conndown:
                raise OpenflowProtocolException('Connection is down before reply received')
            msg = ev.message
            messages.append(msg)
            if msg.header.type == common.OFPT_ERROR or not (msg.flags & common.OFPSF_REPLY_MORE):
                if msg.header.type == common.OFPT_ERROR and raiseonerror:
                    raise OpenflowErrorResultException(msg, result=messages)
                break
        return messages

    async def batch(self, requests, connection, container, raiseonerror = True):
        """
        Send multiple requests, return when all the requests are done. Requests can have no responses.
        The attributes are set even if an OpenflowErrorResultException is raised.
        
        :return: (openflow_reply, openflow_replydict) in which `openflow_reply` is the list of messages in receiving order.
                `openflow_replydict` is the dictionary `{request:reply}`.
        
        :raise OpenflowErrorResultException: when some replies are errors. `exc.result` returns
                                             (openflow_reply, openflow_replydict)
        """
        for r in requests:
            self.assignxid(r, connection)
        replymatchers = dict((self.replymatcher(r, connection), r) for r in requests)
        replydict = {}
        replymessages = []
        conndown = self.statematcher(connection)
        firsterror = [None]
        def callback(event, matcher):
            if matcher is conndown:
                raise OpenflowProtocolException('Connection is down before reply received')
            msg = event.message
            if event.iserror and not firsterror[0]:
                firsterror[0] = msg
            replydict.setdefault(matcher, []).append(msg)
            replymessages.append(msg)
        async def batchprocess():
            for r in requests:
                await connection._rate_limiter.limit()
                await connection.write(self.formatrequest(r, connection, False), False)
            barrier = connection.openflowdef.ofp_msg.new()
            barrier.header.type = connection.openflowdef.OFPT_BARRIER_REQUEST
            await container.wait_for_send(self.formatrequest(barrier, connection))
            barrierreply = self.replymatcher(barrier, connection)
            await barrierreply
        await container.with_callback(batchprocess(), callback, conndown, *replymatchers.keys())
        openflow_reply = replymessages
        openflow_replydict = dict((replymatchers[k],v) for k,v in replydict.items())
        if firsterror[0] and raiseonerror:
            raise OpenflowErrorResultException(firsterror[0], 'One or more error message is returned from a batch process, the first is: ',
                                               result=(openflow_reply, openflow_replydict))
        else:
            return (openflow_reply, openflow_replydict)

    async def init(self, connection):
        await Protocol.init(self, connection)
        connection.createdqueues.append(connection.scheduler.queue.addSubQueue(\
                self.messagepriority, OpenflowPresetupMessageEvent.createMatcher(connection = connection), ('presetup', connection)))
        connection.createdqueues.append(connection.scheduler.queue.addSubQueue(\
                self.messagepriority, OpenflowConnectionStateEvent.createMatcher(connection = connection), ('connstate', connection)))
        connection.createdqueues.append(connection.scheduler.queue.addSubQueue(\
                self.messagepriority + 1, OpenflowResponseEvent.createMatcher(connection = connection), ('response', connection), self.messagequeuesize))
        connection.createdqueues.append(connection.scheduler.queue.addSubQueue(\
                self.messagepriority, OpenflowAsyncMessageEvent.createMatcher(connection = connection), ('async', connection), self.messagequeuesize))
        connection.createdqueues.append(connection.scheduler.queue.addSubQueue(\
                self.messagepriority, OpenflowExperimenterMessageEvent.createMatcher(connection = connection), ('experimenter', connection), self.messagequeuesize))
        # Add priority to echo reply, or the whole connection is down
        connection.createdqueues.append(connection.scheduler.queue.addSubQueue(\
                self.writepriority + 10, ConnectionWriteEvent.createMatcher(connection = connection, _ismatch = lambda x: hasattr(x, 'echoreply') and x.echoreply), ('echoreply', connection)))
        connection._rate_limiter = RateLimiter(200, connection)
        await self.reconnect_init(connection)
    async def reconnect_init(self, connection):
        connection.xid = ord(os.urandom(1)) + 1
        connection.openflowversion = 0
        connection.openflow_datapathid = None
        connection.openflow_msgbuffer = []
        connection.subroutine(self.handler(connection), asyncStart = False, name = 'handler')
    async def closed(self, connection):
        await Protocol.closed(self, connection)
        connection.handler.close()
        self._logger.info('Openflow connection is closed on %r', connection)
        if connection.openflow_datapathid is not None:
            await connection.wait_for_send(OpenflowConnectionStateEvent(connection.openflow_datapathid, connection.openflow_auxiliaryid, OpenflowConnectionStateEvent.CONNECTION_DOWN, connection, connection.connmark, self))
    async def error(self, connection):
        await Protocol.error(self, connection)
        connection.handler.close()
        self._logger.warning('Openflow connection is reset on %r', connection)
        if connection.openflow_datapathid is not None:
            await connection.wait_for_send(OpenflowConnectionStateEvent(connection.openflow_datapathid, connection.openflow_auxiliaryid, OpenflowConnectionStateEvent.CONNECTION_DOWN, connection, connection.connmark, self))
    def _createevent(self, connection, msg):
        if msg.header.type == common.OFPT_ECHO_REQUEST:
            # Direct reply without enqueue
            msg.header.type = common.OFPT_ECHO_REPLY
            echoreply = self.formatreply(msg, msg, connection)
            echoreply.echoreply = True
            return echoreply
        elif connection.openflow_datapathid is None:
            # Connection is pre-setup
            if msg.header.type == common.OFPT_HELLO or msg.header.type == common.OFPT_ERROR:
                return OpenflowPresetupMessageEvent(connection, msg.header.type, connection.connmark, self, message = msg)
            else:
                # Other messages must be parsed in specified version
                ofdef = definations.get(msg.header.version)
                if ofdef is None:
                    # Version is not supported
                    self._logger.warning('Illegal message received from connection %r, message = %r', connection, common.dump(msg))
                    err = common.ofp_error_msg.new()
                    err.header.version = msg.header.version
                    err.type = common.OFPET_BAD_REQUEST
                    err.code = common.OFPBRC_BAD_VERSION
                    err.data = (msg._tobytes())[0:64]
                    write = self.formatreply(err, msg, connection)
                    return write
                elif msg.header.type == ofdef.OFPT_FEATURES_REPLY:
                    return OpenflowPresetupMessageEvent(connection, msg.header.type, connection.connmark, self, message = msg)
                else:
                    # Store other messages
                    connection.openflow_msgbuffer.append(msg)
                    return None
        else:
            if msg.header.version != connection.openflowversion:
                self._logger.warning('Illegal message (version not match) received from connection %r, message = %r', connection, common.dump(msg))
                err = common.ofp_error_msg.new()
                err.header.version = connection.openflowversion
                err.type = common.OFPET_BAD_REQUEST
                err.code = common.OFPBRC_BAD_VERSION
                err.data = (msg._tobytes())[0:64]
                write = self.formatreply(err, msg, connection)
                return write                
            elif msg.header.type == openflow10.OFPT_VENDOR:
                if connection.openflowversion > common.OFP10_VERSION:
                    experimenter = msg.experimenter
                    exptype = msg.exp_type
                else:
                    experimenter = msg.vendor
                    exptype = getattr(msg, 'subtype', 0)
                return OpenflowExperimenterMessageEvent(experimenter, exptype, connection.openflow_datapathid, connection.openflow_auxiliaryid, 
                                                        connection, connection.connmark, self, message = msg)
            elif msg.header.type == common.OFPT_ERROR or msg.header.type in connection.openflowdef.ofp_type_reply_set:
                iserror = (msg.header.type == common.OFPT_ERROR)
                return OpenflowResponseEvent(connection.openflow_datapathid, connection.openflow_auxiliaryid, connection, connection.connmark, msg.header.xid, iserror, self, message = msg)
            elif msg.header.type in connection.openflowdef.ofp_type_asyncmessage_set:
                return OpenflowAsyncMessageEvent(msg.header.type, connection.openflow_datapathid, connection.openflow_auxiliaryid, 
                                                 getattr(msg, 'table_id', 0), getattr(msg, 'cookie', 0), connection, connection.connmark, self, message = msg)
            else:
                # These messages are requests, send a BADREQUEST error
                self._logger.warning('Illegal message (type error) received from connection %r, message = %r', connection, common.dump(msg))
                err = common.ofp_error_msg.new()
                err.header.version = connection.openflowversion
                err.type = common.OFPET_BAD_REQUEST
                err.code = common.OFPBRC_BAD_TYPE
                err.data = (msg._tobytes())[0:64]
                return self.formatreply(err, msg, connection)
    def parse(self, connection, data, laststart):
        start = 0
        events = []
        while True:
            result = common.ofp_msg.parse(data[start:])
            if result is None:
                break
            msg, size = result
            if self.debugging:
                self._logger.debug('message received: %r', common.dump(msg))
            start += size
            e = self._createevent(connection, msg)
            if e is not None:
                events.append(e)
        if laststart == len(data):
            # Remote write close
            events.append(ConnectionWriteEvent(connection, connection.connmark, data = b'', EOF = True))
        return (events, len(data) - start)
    async def keepalive(self, connection):
        echo = common.ofp_echo()
        echo.header.version = connection.openflowversion
        try:
            timeout, _ = await connection.execute_with_timeout(self.keepalivetimeout, self.querywithreply(echo, connection, connection))
            if timeout:
                self._logger.warning("Keepalive timeout on connection %r with exception, reset the connection", connection)
                await connection.reset(True)
        except Exception:
            self._logger.warning("Keepalive failed on connection %r with exception, reset the connection", connection, exc_info=True)
            await connection.reset(True)

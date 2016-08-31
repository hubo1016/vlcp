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
from vlcp.event.lock import Lock

@withIndices('datapathid', 'auxiliaryid', 'state', 'connection', 'connmark', 'createby')
class OpenflowConnectionStateEvent(Event):
    CONNECTION_SETUP = 'setup'
    CONNECTION_DOWN = 'down'

@withIndices('datapathid', 'auxiliaryid', 'connection', 'connmark', 'xid', 'iserror', 'createby')
class OpenflowResponseEvent(Event):
    pass

@withIndices('type', 'datapathid', 'auxiliaryid', 'tableid', 'cookie', 'connection', 'connmark', 'createby')
class OpenflowAsyncMessageEvent(Event):
    pass

@withIndices('connection', 'type', 'connmark', 'createby')
class OpenflowPresetupMessageEvent(Event):
    pass

@withIndices('experimenter', 'exptype', 'datapathid', 'auxiliaryid', 'connection', 'connmark', 'createby')
class OpenflowExperimenterMessageEvent(Event):
    pass

class OpenflowProtocolException(Exception):
    pass

class OpenflowErrorResultException(Exception):
    pass

@defaultconfig
class Openflow(Protocol):
    '''
    Openflow control protocol
    '''
    _default_persist = True
    _default_defaultport = common.OFP_TCP_PORT
    _default_allowedversions = (common.OFP10_VERSION, common.OFP13_VERSION)
    _default_hellotimeout = 10
    _default_featurerequesttimeout = 30
    _default_keepalivetime = 10
    _default_keepalivetimeout = 3
    _default_createqueue = True
    _default_buffersize = 65536
    _default_debugging = False
    _default_disablenxext = False
    _default_disablechaining = True
    _logger = logging.getLogger(__name__ + '.Openflow')
    _default_tcp_nodelay = True
    def __init__(self, allowedVersions = None):
        '''
        Constructor
        '''
        Protocol.__init__(self)
        if allowedVersions is not None:
            self.allowedversions = allowedVersions
    def handler(self, connection):
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
            for m in connection.write(write, False):
                yield m
            # Wait for a hello
            hellomatcher = OpenflowPresetupMessageEvent.createMatcher(connection = connection)
            for m in connection.waitWithTimeout(self.hellotimeout, hellomatcher):
                yield m
            if connection.timeout:
                # Drop the connection
                raise OpenflowProtocolException('Did not receive hello message before timeout')
            else:
                msg = connection.event.message
                if msg.header.type != common.OFPT_HELLO:
                    raise OpenflowProtocolException('The first packet on this connection is not OFPT_HELLO')
                else:
                    helloversion = None
                    usebitmap = False
                    for e in msg.elements:
                        if e.type == OFPHET_VERSIONBITMAP:
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
                        for m in connection.write(write):
                            yield m
                        for m in connection.reset(False, connmark):
                            yield m
                        raise GeneratorExit
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
                            for m in connection.withException(connection.write(write, False), err_matcher):
                                yield m
                            featurereply_matcher = OpenflowPresetupMessageEvent.createMatcher(connection = connection, type = currdef.OFPT_FEATURES_REPLY)
                            for m in connection.waitWithTimeout(self.featurerequesttimeout, featurereply_matcher, err_matcher):
                                yield m
                            if connection.timeout:
                                raise OpenflowProtocolException('Remote switch did not response to feature request.')
                            elif connection.matcher is err_matcher:
                                self._logger.warning('Error while request feature: %r Connection = %r', connection.event.message, connection)
                                raise OpenflowProtocolException('Error while request feature: %r' % (connection.event.message,))
                            else:
                                msg = connection.event.message
                                connection.openflow_featuresreply = msg
                                connection.openflow_datapathid = msg.datapath_id
                                connection.openflow_auxiliaryid = getattr(msg, 'auxiliary_id', 0)
                                connection.openflow_capabilities = msg.capabilities
                                connection.openflow_n_buffers = msg.n_buffers
                                connection.openflow_n_tables = msg.n_tables
                                statechange = OpenflowConnectionStateEvent(connection.openflow_datapathid, connection.openflow_auxiliaryid, OpenflowConnectionStateEvent.CONNECTION_SETUP, connection, connection.connmark, self)
                                for m in connection.waitForSend(statechange):
                                    yield m
                                for msg in connection.openflow_msgbuffer:
                                    e = self._createevent(connection, msg)
                                    if e is not None:
                                        for m in connection.waitForSend(e):
                                            yield m
                        except RoutineException as exc:
                            self._logger.warning('Remote report hello fail: %r Connection = %r', common.dump(exc.arguments[1].message), connection)
                            for m in connection.reset(True, connmark):
                                yield m
                            raise GeneratorExit
        except QuitException:
            pass
        except GeneratorExit:
            pass
        except:
            self._logger.exception('Unexpected exception on processing openflow protocols, Connection = %r', connection)
            for m in connection.reset(True, connmark):
                yield m
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
        matcherparam = {'datapathid': connection.openflow_datapathid, 'auxiliaryid': connection.openflow_auxiliaryid, 'connection' : connection, 'connmark': connection.connmark, 
                        'xid': request.header.xid}
        if iserror is not None:
            matcherparam['iserror'] = iserror
        return OpenflowResponseEvent.createMatcher(**matcherparam)
    def statematcher(self, connection, state = OpenflowConnectionStateEvent.CONNECTION_DOWN, currentconn = True):
        if currentconn:
            return OpenflowConnectionStateEvent.createMatcher(connection.openflow_datapathid, connection.openflow_auxiliaryid, state, connection, connection.connmark)
        else:
            return OpenflowConnectionStateEvent.createMatcher(connection.openflow_datapathid, connection.openflow_auxiliaryid, state)
    def querywithreply(self, request, connection, container, raiseonerror = True):
        for m in connection.write(self.formatrequest(request, connection), False):
            yield m
        reply = self.replymatcher(request, connection)
        conndown = self.statematcher(connection)
        yield (reply, conndown)
        if container.matcher is conndown:
            raise OpenflowProtocolException('Connection is down before reply received')
        container.openflow_reply = container.event.message
        if container.event.iserror:
            raise OpenflowErrorResultException('An error message is returned: ' + repr(dump(container.event.message)))
    def querymultipart(self, request, connection, container, raiseonerror = True):
        for m in connection.write(self.formatrequest(request, connection), False):
            yield m
        reply = self.replymatcher(request, connection)
        conndown = self.statematcher(connection)
        messages = []
        while True:
            yield (reply, conndown)
            if container.matcher is conndown:
                raise OpenflowProtocolException('Connection is down before reply received')
            msg = container.event.message
            messages.append(msg)
            if msg.header.type == common.OFPT_ERROR or not (msg.flags & common.OFPSF_REPLY_MORE):
                if msg.header.type == common.OFPT_ERROR and raiseonerror:
                    container.openflow_reply = messages
                    raise OpenflowErrorResultException('An error message is returned: ' + repr(dump(msg)))
                break
        container.openflow_reply = messages
    def batch(self, requests, connection, container, raiseonerror = True):
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
        def batchprocess():
            for i in range(0, len(requests), 256):
                l = Lock((connection, 'openflow_batch'), container.scheduler)
                for m in l.lock(container):
                    yield m
                with l:
                    for r in requests[i:i+256]:
                        for m in connection.write(self.formatrequest(r, connection, False), False):
                            yield m
                    barrier = connection.openflowdef.ofp_msg.new()
                    barrier.header.type = connection.openflowdef.OFPT_BARRIER_REQUEST
                    for m in container.waitForSend(self.formatrequest(barrier, connection)):
                        yield m
                    barrierreply = self.replymatcher(barrier, connection)
                    yield (barrierreply,)
        for m in container.withCallback(batchprocess(), callback, conndown, *replymatchers.keys()):
            yield m
        container.openflow_reply = replymessages
        container.openflow_replydict = dict((replymatchers[k],v) for k,v in replydict.items())
        if firsterror[0] and raiseonerror:
            raise OpenflowErrorResultException('One or more error message is returned from a batch process, the first is: '
                                               + repr(dump(firsterror[0])))
    def init(self, connection):
        for m in Protocol.init(self, connection):
            yield m
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
        for m in self.reconnect_init(connection):
            yield m
    def reconnect_init(self, connection):
        connection.xid = ord(os.urandom(1)) + 1
        connection.openflowversion = 0
        connection.openflow_datapathid = None
        connection.openflow_msgbuffer = []
        connection.subroutine(self.handler(connection), asyncStart = False, name = 'handler')
        if False:
            yield
    def closed(self, connection):
        for m in Protocol.closed(self, connection):
            yield m
        connection.handler.close()
        self._logger.info('Openflow connection is closed on %r', connection)
        if connection.openflow_datapathid is not None:
            for m in connection.waitForSend(OpenflowConnectionStateEvent(connection.openflow_datapathid, connection.openflow_auxiliaryid, OpenflowConnectionStateEvent.CONNECTION_DOWN, connection, connection.connmark, self)):
                yield m
            connection.openflow_datapathid = None
    def error(self, connection):
        for m in Protocol.error(self, connection):
            yield m
        connection.handler.close()
        self._logger.warning('Openflow connection is reset on %r', connection)
        if connection.openflow_datapathid is not None:
            for m in connection.waitForSend(OpenflowConnectionStateEvent(connection.openflow_datapathid, connection.openflow_auxiliaryid, OpenflowConnectionStateEvent.CONNECTION_DOWN, connection, connection.connmark, self)):
                yield m
            connection.openflow_datapathid = None
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
    def keepalive(self, connection):
        echo = common.ofp_echo()
        echo.header.version = connection.openflowversion
        try:
            for m in connection.executeWithTimeout(self.keepalivetimeout, self.querywithreply(echo, connection, connection)):
                yield m
            if connection.timeout:
                for m in connection.reset(True):
                    yield m
        except Exception:
            for m in connection.reset(True):
                yield m
    
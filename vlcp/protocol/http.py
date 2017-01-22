'''
Created on 2015/8/18

:author: hubo
'''

from vlcp.config import defaultconfig
from vlcp.protocol import Protocol
from vlcp.event import Event, withIndices, Stream, ConnectionControlEvent, ConnectionWriteEvent, CBQueue, PollEvent
from vlcp.event.core import syscall_clearremovequeue, PollEvent
from collections import deque
import logging
import re
import time
from vlcp.event.stream import MemoryStream, StreamDataEvent
import zlib

@withIndices('state', 'connection', 'connmark', 'createby')
class HttpConnectionStateEvent(Event):
    """
    HTTP connection state changed
    """
    # Connection established from the client side
    CLIENT_CONNECTED = 'clientconnect'
    # Connection down from the client side
    CLIENT_CLOSE = 'clientclose'
    # Cannot connect to server
    CLIENT_NOTCONNECTED = 'clientnotconnected'
    # A connection is established to server side
    SERVER_CONNECTED = 'serverconnect'
    # A connection is down from the server side
    SERVER_CLOSE = 'serverclose'
    

@withIndices('host', 'path', 'method', 'connection', 'connmark', 'xid', 'createby')
class HttpRequestEvent(Event):
    """
    A HTTP request is received from the connection
    """
    canignore = False
    def canignorenow(self):
        return not self.connection.connected or self.connection.connmark != self.connmark

@withIndices('connection', 'connmark', 'xid', 'isfinal', 'iserror')
class HttpResponseEvent(Event):
    """
    A HTTP response is received from the connection
    """
    pass

@withIndices('connection', 'connmark', 'xid', 'keepalive')
class HttpResponseEndEvent(Event):
    """
    A HTTP response is fully received
    """
    pass

@withIndices('connection', 'connmark', 'type')
class HttpStateChange(Event):
    NEXTINPUT = 'nextinput'
    NEXTOUTPUT = 'nextoutput'

@withIndices('stream')
class HttpTrailersReceived(Event):
    """
    Trailers are received on an HTTPResponseStream
    """
    pass

try:
    _long = long
except:
    _long = int

class HttpProtocolException(Exception):
    """
    Critical protocol break on HTTP connections
    """
    pass

class HttpConnectionClosedException(HttpProtocolException):
    """
    Connection is closed
    """
    pass

linedelimiter = re.compile(br'\r\n')
statuscheck = re.compile(br'HTTP/\d\.\d[ \t]')
statuscheckshort = re.compile(br'H(?:T(?:T(?:P(?:/(?:\d(?:\.(?:\d[ \t]?)?)?)?)?)?)?)?$')
token = br"[\!#$%&\'*+\-\.^_`|~0-9a-zA-Z]+"
token_r = re.compile(br"^[\!#$%&\'*+\-\.^_`|~0-9a-zA-Z]+$")
requestline = re.compile(br'^(' + token + br')[ \t]+([^ \t]+)[ \t]+HTTP/(\d\.\d)$')
statusline = re.compile(br'^HTTP/(\d\.\d)[ \t]+(\d{3})[ \t]+(.*)$')
headerline = re.compile(br'^(' + token + br')\:[ \t]*(.*?)[ \t]*$')
chunkedheaderline = re.compile(br'^([0-9a-zA-Z]+)(?:;.*)?$')
standard_status = {
    100: 'Continue',
    101: 'Switching Protocols',
    102: 'Processing',
    200: 'OK',
    201: 'Created',
    202: 'Accepted',
    203: 'Non-Authoritative Information',
    204: 'No Content',
    205: 'Reset Content',
    206: 'Partial Content',
    207: 'Multi-Status',
    208: 'Already Reported',
    226: 'IM Used',
    300: 'Multiple Choices',
    301: 'Moved Permanently',
    302: 'Found',
    303: 'See Other',
    304: 'Not Modified',
    305: 'Use Proxy',
    306: '(Unused)',
    307: 'Temporary Redirect',
    308: 'Permanent Redirect',
    400: 'Bad Request',
    401: 'Unauthorized',
    402: 'Payment Required',
    403: 'Forbidden',
    404: 'Not Found',
    405: 'Method Not Allowed',
    406: 'Not Acceptable',
    407: 'Proxy Authentication Required',
    408: 'Request Timeout',
    409: 'Conflict',
    410: 'Gone',
    411: 'Length Required',
    412: 'Precondition Failed',
    413: 'Payload Too Large',
    414: 'URI Too Long',
    415: 'Unsupported Media Type',
    416: 'Range Not Satisfiable',
    417: 'Expectation Failed',
    421: 'Misdirected Request',
    422: 'Unprocessable Entity',
    423: 'Locked',
    424: 'Failed Dependency',
    426: 'Upgrade Required',
    428: 'Precondition Required',
    429: 'Too Many Requests',
    431: 'Request Header Fields Too Large',
    500: 'Internal Server Error',
    501: 'Not Implemented',
    502: 'Bad Gateway',
    503: 'Service Unavailable',
    504: 'Gateway Timeout',
    505: 'HTTP Version Not Supported',
    506: 'Variant Also Negotiates',
    507: 'Insufficient Storage',
    508: 'Loop Detected',
    510: 'Not Extended',
    511: 'Network Authentication Required'
}
weekdayname = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
monthname = [None,
             'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
             'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
# From BaseHTTPServer library
def date_time_string(timestamp=None):
    """Return the current date and time formatted for a message header."""
    if timestamp is None:
        timestamp = time.time()
    year, month, day, hh, mm, ss, wd, y, z = time.gmtime(timestamp)
    s = "%s, %02d %3s %4d %02d:%02d:%02d GMT" % (
            weekdayname[wd],
            day, monthname[month], year,
            hh, mm, ss)
    return s

# From cgi library
def escape_b(s, quote=True):
    '''Replace special characters "&", "<" and ">" to HTML-safe sequences.
    If the optional flag quote is true, the quotation mark character (")
    is also translated.'''
    s = s.replace(b"&", b"&amp;") # Must be done first!
    s = s.replace(b"<", b"&lt;")
    s = s.replace(b">", b"&gt;")
    if quote:
        s = s.replace(b'"', b"&quot;")
    return s

def escape(s, quote=True):
    '''Replace special characters "&", "<" and ">" to HTML-safe sequences.
    If the optional flag quote is true, the quotation mark character (")
    is also translated.'''
    s = s.replace("&", "&amp;") # Must be done first!
    s = s.replace("<", "&lt;")
    s = s.replace(">", "&gt;")
    if quote:
        s = s.replace('"', "&quot;")
    return s

def normalizeHeader(headername):
    return headername.lower()

specialdisplayheaders = {b'etag':b'ETag', }

def displayHeader(headername):
    return b'-'.join(p.capitalize() for p in headername.split(b'-'))

def _createstatus(status):
    if isinstance(status, int) or isinstance(status, _long):
        status = str(status) + ' ' + standard_status.get(status, 'User-defined')
    if not isinstance(status, bytes):
        status = status.encode('ascii')
    return status


@defaultconfig
class Http(Protocol):
    '''
    Basic HTTP/1.1 protocol
    Base on RFC723x, which are more strict than RFC2616
    '''
    _default_persist = False
    # Limit the pipelining requests that are executed in parallel.
    # Disable pipelining by setting pipelinelimit = 1
    _default_pipelinelimit = 10
    # Maximum length for the first line of request or response
    _default_headlinelimit = 8192
    # Maximum length for a single header
    _default_headerlimit = 1048576
    # Maximum header count for a single request/response
    _default_headercountlimit = 1024
    # Maximum length for a chunked header; it is a special header type which is rarely used.
    _default_chunkedheaderlimit = 8192
    # Maximum chunked header count
    _default_chunkednumberlimit = 16
    # Enable HTTP/0.9, in which a response does not have headers
    _default_allownoheader = True
    # Default HTTP protocol
    _default_defaultversion = '1.1'
    # Drop the connection rather than sending a 400 Bad Request
    # when there is a protocol break in the request or response
    _default_fastfail = False
    # Enable TE: deflate header. Not all implements support this.
    _default_allowtransfercompress = False
    # Close a HTTP/1.1 keep-alive connection when idles for a specified time
    _default_idletimeout = 20
    # Close a connection when "Connection: close" is received after the specified time
    _default_closetimeout = 5
    # Print debugging logs
    _default_debugging = False
    # Use Expect: 100-continue for POST requests; not all server supports this
    _default_useexpect = False
    # If useexpect = True, Use Expect: 100-continue for POST requests with data larger than this size
    _default_useexpectsize = 4096
    # When 100 Continue response is not received for the specified time, send the data
    _default_expecttimeout = 2
    # default HTTP port
    _default_defaultport = 80
    # default HTTPS port
    _default_ssldefaultport = 443
    # Show error details in error response
    _default_showerrorinfo = False
    # Use unquoteplus instead of unquote, which means + is recognized as space ' '
    _default_unquoteplus = True
    _logger = logging.getLogger(__name__ + '.HTTP')
    # A dictionary to rewrite errors in HttpHandler to specified path (without 3xx redirect)
    _default_errorrewrite = {}
    # A dictionary to redirect errors in HttpHandler to specified path (with 302 Found)
    _default_errorredirect = {}
    # Priority for response end events
    _default_responseendpriority = 690
    # Default response headers that are sent from HTTP server to client
    _default_defaultresponseheaders = [(b'Content-Type', b'text/html'), (b'Server', b'VLCP HTTP Server'),
                                    (b'Vary', b'Accept-Encoding')]
    # Default request headers that are sent from HTTP client to server
    _default_defaultrequestheaders = [(b'Accept', b'*/*'), (b'User-Agent', b'VLCP HTTP Client')]
    _default_tcp_nodelay = True
    def __init__(self, server = True, defaultversion = None):
        '''
        Constructor
        '''
        Protocol.__init__(self)
        self.server = server
        self.connectioncount = 0
        self.connectionpersource = {}
        if defaultversion is not None:
            self.defaultversion = defaultversion
    def createmessagequeue(self, scheduler):
        if 'httprequest' not in scheduler.queue:
            #scheduler.queue.addSubQueue(self.messagepriority, HttpRequestEvent.createMatcher(), 'httprequest', None, None, CBQueue.AutoClassQueue.initHelper('connection', 100, subqueuelimit = self.pipelinelimit * 2 if self.pipelinelimit is not None else None))
            scheduler.queue.addSubQueue(self.messagepriority, HttpRequestEvent.createMatcher(), 'httprequest', None, None)
            scheduler.queue.addSubQueue(self.messagepriority, HttpConnectionStateEvent.createMatcher(), 'httpconnstate', None, None)
            #scheduler.queue.addSubQueue(self.messagepriority, HttpResponseEvent.createMatcher(), 'httpresponse', None, None, CBQueue.AutoClassQueue.initHelper('connection', 100, subqueuelimit = self.pipelinelimit * 2 if self.pipelinelimit is not None else None))
            scheduler.queue.addSubQueue(self.messagepriority, HttpResponseEvent.createMatcher(), 'httpresponse', None, None)
            scheduler.queue.addSubQueue(self.responseendpriority, HttpResponseEndEvent.createMatcher(), 'httpresponseend', None, None)
            #scheduler.queue.addSubQueue(self.messagepriority, HttpTrailersReceived.createMatcher(), 'httptrailer', None, None, CBQueue.AutoClassQueue.initHelper('stream', 100, subqueuelimit = None))
            scheduler.queue.addSubQueue(self.messagepriority, HttpTrailersReceived.createMatcher(), 'httptrailer', None, None)
            scheduler.queue.addSubQueue(self.writepriority, HttpStateChange.createMatcher(), 'httpstatechange')
    def init(self, connection):
        for m in Protocol.init(self, connection):
            yield m
        self.createmessagequeue(connection.scheduler)
        for m in self.reconnect_init(connection):
            yield m
    def _request_sender(self, connection):
        try:
            connmark = connection.connmark
            nextOutput = HttpStateChange.createMatcher(connection, 
                                                      connmark, 
                                                      HttpStateChange.NEXTOUTPUT)
            def requesterror(msg, method, path):
                self._logger.error('Illegal HTTP request: %s, connection = %r, request = %r', msg, connection, method + b' ' + path + b' HTTP/' + connection.http_localversion.encode('ascii'))
                raise HttpProtocolException(msg)
            writeclose = False
            while not writeclose:
                if not connection.http_requestbuffer:
                    yield (nextOutput,)
                else:
                    # (host, path, method, headers, stream)
                    host, path, method, headers, output, keepalive = connection.http_requestbuffer.popleft()
                    reqteinfo = (method, True, True, True)
                    connection.http_reqteinfo.append(reqteinfo)
                    xid = connection.xid
                    connection.xid += 1
                    # find out how we should use the output stream
                    transfer_chunked = False
                    transfer = None
                    try:
                        if method == b'CONNECT':
                            content_length = None
                        elif output is None:
                            content_length = 0
                        else:
                            cl = [(k,v) for k,v in headers if normalizeHeader(k) == b'content-length']
                            if len(cl) > 1:
                                requesterror('multiple content-length headers', method, path)
                            elif cl:
                                try:
                                    content_length = int(cl[0][1])
                                except:
                                    requesterror('invalid content-length header', method, path)
                            else:
                                if connection.http_localversion < '1.1':
                                    requesterror('Must have content-length for HTTP/1.0 request with message body')
                                else:
                                    transfer_chunked = True
                                    transfer = b'chunked'
                        # Output headline and headers
                        buffer = []
                        def write():
                            for m in connection.write(ConnectionWriteEvent(connection,
                                                                           connmark,
                                                                           data = b''.join(buffer))):
                                yield m
                            del buffer[:]
                        buffer.append(method + b' ' + path + b' HTTP/' +
                                      connection.http_localversion.encode('ascii') + b'\r\n')
                        if transfer is not None:
                            buffer.append(b'Transfer-Encoding: ' + transfer + b'\r\n')
                        for k,v in headers:
                            buffer.append(k + b': ' + v + b'\r\n')
                        expect = self.useexpect and output is not None and \
                            (transfer is not None or content_length is not None and content_length > self.useexpectsize)
                        if expect:
                            buffer.append(b'Expect: 100-continue\r\n')
                        buffer.append(b'\r\n')
                        for m in write():
                            yield m
                        if expect:
                            def waitForContinue():
                                expect100 = self.responsematcher(connection, xid)
                                while True:
                                    yield (expect100,)
                                    if connection.event.isfinal:
                                        break
                                    elif connection.event.statuscode == 100:
                                        break
                            for m in connection.executeWithTimeout(self.expecttimeout, waitForContinue()):
                                yield m
                            if not connection.timeout:
                                # There is a response
                                if connection.event.isfinal:
                                    # We have already received a response, do not send the body
                                    for m in connection.write(ConnectionWriteEvent(connection,
                                                                                       connmark,
                                                                                       data = b'',
                                                                                       EOF = True)):
                                        yield m
                                    writeclose = True
                                    break
                        # Send data
                        if output is not None:
                            while True:
                                try:
                                    for m in output.prepareRead(connection):
                                        yield m
                                    data = output.readonce()
                                except EOFError:
                                    if transfer_chunked:
                                        # Send chunked end
                                        buffer.append(b'0\r\n')
                                        # Trailers
                                        if hasattr(output, 'trailers'):
                                            for k,v in output.trailers:
                                                buffer.append(k + b': ' + v + b'\r\n')
                                        buffer.append(b'\r\n')
                                        if buffer:
                                            for m in write():
                                                yield m
                                        if not keepalive:
                                            for m in connection.write(ConnectionWriteEvent(connection,
                                                                                           connmark,
                                                                                           data = b'',
                                                                                           EOF = True)):
                                                yield m
                                            writeclose = True
                                            break 
                                    elif content_length is None:
                                        for m in connection.write(ConnectionWriteEvent(connection,
                                                                                           connmark,
                                                                                           data = b'',
                                                                                           EOF = True)):
                                            yield m
                                        writeclose = True
                                        break
                                    elif content_length > 0:
                                        raise
                                    break
                                else:
                                    if transfer_chunked:
                                        if data:
                                            buffer.append(hex(len(data))[2:].encode('ascii') + b'\r\n')
                                            buffer.append(data)
                                            buffer.append(b'\r\n')
                                            for m in write():
                                                yield m
                                    else:
                                        if content_length is not None:
                                            if len(data) > content_length:
                                                data = data[0:content_length]
                                                
                                        for m in connection.write(ConnectionWriteEvent(connection,
                                                                                       connmark,
                                                                                       data = data)):
                                            yield m
                                        if content_length is not None:
                                            content_length -= len(data)
                                            if content_length <= 0:
                                                break
                    finally:
                        if output is not None:
                            output.close(connection.scheduler)
        except:
            for m in connection.reset(True, connmark):
                yield m
            raise            
    def _response_sender(self, connection):
        try:
            connmark = connection.connmark
            nextInput = HttpStateChange.createMatcher(connection, 
                                                      connmark, 
                                                      HttpStateChange.NEXTINPUT)
            nextOutput = HttpStateChange.createMatcher(connection, 
                                                      connmark, 
                                                      HttpStateChange.NEXTOUTPUT)
            writeclose = False
            while True:
                if writeclose:
                    if connection.http_remoteversion >= '1.1':
                        for m in connection.waitWithTimeout(self.closetimeout):
                            yield m
                    for m in connection.shutdown(False, connmark):
                        yield m
                    break
                elif connection.xid == connection.http_responsexid and connection.http_idle:
                    if connection.http_keepalive:
                        # Wait for new requests
                        for m in connection.waitWithTimeout(self.idletimeout, nextInput):
                            yield m
                    else:
                        if connection.http_remoteversion >= '1.1':
                            for m in connection.waitWithTimeout(self.closetimeout):
                                yield m
                        else:
                            connection.timeout = True
                    if connection.timeout:
                        for m in connection.shutdown(False, connmark):
                            yield m
                        break
                elif not connection.http_responsexid in connection.http_responsebuffer or\
                        not connection.http_responsebuffer[connection.http_responsexid]:
                    yield (nextOutput,)
                else:
                    resp = connection.http_responsebuffer[connection.http_responsexid].pop(0)
                    # find out how we should use the output stream
                    transfer_deflate = False
                    transfer_chunked = False
                    transfer = None
                    try:
                        output = None
                        if resp.status[:1] == b'1':
                            # 1xx does not have body
                            output = None
                            transfer = None
                            if resp.outputstream is not None:
                                resp.outputstream.close(connection.scheduler, True)
                        else:
                            teinfo = connection.http_reqteinfo.popleft()
                            if teinfo[0] == b'HEAD':
                                # HEAD response has no body
                                output = None
                                transfer = None
                                if resp.outputstream is not None:
                                    resp.outputstream.close(connection.scheduler, True)
                            elif teinfo[0] == b'CONNECT':
                                output = resp.outputstream
                                transfer = None
                                content_length = None
                            elif resp.status[:4] in (b'204 ', b'304 '):
                                # 204 and 304 response has no body
                                output = None
                                transfer = None
                                if resp.outputstream is not None:
                                    resp.outputstream.close(connection.scheduler, True)
                            else:
                                output = resp.outputstream
                                cl = [(k,v) for k,v in resp.headers if normalizeHeader(k) == b'content-length']
                                if len(cl) > 1:
                                    resp = self.createErrorResponse(connection, connection.http_responsexid, 500)
                                    cl = [(k,v) for k,v in resp.headers if normalizeHeader(k) == b'content-length']
                                    content_length = int(cl[0][1])
                                elif cl:
                                    try:
                                        content_length = int(cl[0][1])
                                    except Exception:
                                        resp = self.createErrorResponse(connection, connection.http_responsexid, 500)
                                        cl = [(k,v) for k,v in resp.headers if normalizeHeader(k) == b'content-length']
                                        content_length = int(cl[0][1])
                                elif output is None:
                                    content_length = 0
                                    resp.headers.append((b'Content-Length', b'0'))
                                else:
                                    content_length = None
                                if content_length is None and teinfo[3]:
                                    if teinfo[2] and self.allowtransfercompress and not getattr(resp, 'disabledeflate', False):
                                        transfer = b'deflate, chunked'
                                        transfer_deflate = True
                                        deflateobj = zlib.compressobj()
                                    else:
                                        transfer = b'chunked'
                                        transfer_deflate = False
                                    transfer_chunked = True
                                else:
                                    transfer = None
                                    transfer_chunked = False
                                    transfer_deflate = False
                            connection.http_responsexid += 1
                            # More requests
                            while connection.http_requestbuffer and connection.xid - len(connection.http_requestbuffer) < connection.http_responsexid + self.pipelinelimit:
                                r = connection.http_requestbuffer.popleft()
                                for m in connection.waitForSend(r):
                                    yield m
                                
                        # Output headline and headers
                        buffer = []
                        def write():
                            for m in connection.write(ConnectionWriteEvent(connection,
                                                                           connmark,
                                                                           data = b''.join(buffer))):
                                yield m
                            del buffer[:]
                        buffer.append(b'HTTP/' + connection.http_localversion.encode('ascii') + b' '
                                      + resp.status + b'\r\n')
                        if transfer is not None:
                            buffer.append(b'Transfer-Encoding: ' + transfer + b'\r\n')
                        for k,v in resp.headers:
                            buffer.append(k + b': ' + v + b'\r\n')
                        buffer.append(b'\r\n')
                        for m in write():
                            yield m
                        # Send data
                        if output is not None:
                            while True:
                                try:
                                    for m in output.prepareRead(connection):
                                        yield m
                                    data = output.readonce()
                                except EOFError:
                                    if transfer_chunked:
                                        if transfer_deflate:
                                            data = deflateobj.flush()
                                            if data:
                                                buffer.append(hex(len(data))[2:].encode('ascii'))
                                                buffer.append(b'\r\n')
                                                buffer.append(data)
                                                buffer.append(b'\r\n')
                                        # Send chunked end
                                        buffer.append(b'0\r\n')
                                        # Send trailers if any
                                        if resp.trailers is not None:
                                            for k,v in resp.trailers:
                                                buffer.append(k + b': ' + v + b'\r\n')
                                        buffer.append(b'\r\n')
                                        for m in write():
                                            yield m
                                    elif content_length is None or content_length > 0:
                                        for m in connection.write(ConnectionWriteEvent(connection,
                                                                                           connmark,
                                                                                           data = b'',
                                                                                           EOF = True)):
                                            yield m
                                        writeclose = True
                                        #if content_length is not None:
                                        #    raise
                                    break
                                else:
                                    if transfer_chunked:
                                        if transfer_deflate:
                                            data = deflateobj.compress(data)
                                        if data:
                                            buffer.append(hex(len(data))[2:].encode('ascii') + b'\r\n')
                                            buffer.append(data)
                                            buffer.append(b'\r\n')
                                            for m in write():
                                                yield m
                                    else:
                                        if content_length is not None:
                                            if len(data) > content_length:
                                                data = data[0:content_length]
                                        for m in connection.write(ConnectionWriteEvent(connection,
                                                                                       connmark,
                                                                                       data = data)):
                                            yield m
                                        if content_length is not None:
                                            content_length -= len(data)
                                            if content_length <= 0:
                                                break
                    finally:
                        if output is not None:
                            output.close(connection.scheduler)
        except:
            for m in connection.reset(True, connmark):
                yield m
            raise
    def reconnect_init(self, connection):
        connection.xid = 0
        connection.http_responsexid = 0
        connection.http_parsestage = 'headline'
        connection.http_reqteinfo = deque()
        connection.http_requestbuffer = deque()
        connection.http_responsebuffer = {}
        connection.http_localversion = self.defaultversion
        connection.http_remoteversion = None
        connection.http_currentstream = None
        connection.http_idle = True
        if connection.http_localversion >= '1.1':
            connection.http_keepalive = True
        else:
            connection.http_keepalive = False
        if self.server:
            connection.subroutine(self._response_sender(connection), name = 'http_sender')
        else:
            connection.subroutine(self._request_sender(connection), name = 'http_sender')
        state = HttpConnectionStateEvent(HttpConnectionStateEvent.SERVER_CONNECTED if self.server
                                         else HttpConnectionStateEvent.CLIENT_CONNECTED,
                                         connection, connection.connmark, self)
        for m in connection.waitForSend(state):
            yield m
    def _clearresponse(self, connection):
        if connection.http_currentstream is not None:
            for m in connection.http_currentstream.error(connection, ignoreexception = True):
                yield m
            connection.http_currentstream = None
        for i in range(connection.http_responsexid, connection.xid + 1):
            if i in connection.http_responsebuffer:
                for r in connection.http_responsebuffer[i]:
                    if r.outputstream is not None:
                        r.outputstream.close(connection.scheduler)
        connection.http_responsebuffer.clear()
    def closed(self, connection):
        connection.http_sender.close()
        connection.http_sender = None
        for m in Protocol.closed(self, connection):
            yield m
        connection.scheduler.ignore(HttpRequestEvent.createMatcher(None, None, None, connection))
        for m in connection.waitForSend(HttpConnectionStateEvent(HttpConnectionStateEvent.SERVER_CLOSE if self.server
                                                                 else HttpConnectionStateEvent.CLIENT_CLOSE, connection, connection.connmark, self)):
            yield m
        for m in self._clearresponse(connection):
            yield m
        connection.http_reqteinfo = None
        connection.http_requestbuffer = None
        connection.http_parsestage = 'end'
    def error(self, connection):
        connection.http_sender.close()
        for m in Protocol.error(self, connection):
            yield m
        connection.scheduler.ignore(HttpRequestEvent.createMatcher(None, None, None, connection))
        if self.debugging:
            self._logger.debug('Http connection is reset on %r', connection)
        for m in connection.waitForSend(HttpConnectionStateEvent(HttpConnectionStateEvent.SERVER_CLOSE if self.server
                                                                 else HttpConnectionStateEvent.CLIENT_CLOSE, connection, connection.connmark, self)):
            yield m
        for m in self._clearresponse(connection):
            yield m
        connection.http_reqteinfo = None
        connection.http_requestbuffer = None
    def _createResponseheaders(self, connection, xid, headers, status):
        defaultheaders = self.defaultresponseheaders
        if not connection.http_keepalive and xid == connection.xid - 1 and status[0] != b'1'[0]:
            newheaders = [(b'Connection', b'Close')]
        else:
            newheaders = [(b'Connection', b'Keep-Alive')]
        newheaders.append((b'Date', date_time_string().encode('ascii')))
        existingHeaders = set(normalizeHeader(k) for k,_ in headers)
        existingHeaders.add(b'date')
        existingHeaders.add(b'connection')
        for k,v in defaultheaders:
            if normalizeHeader(k) not in existingHeaders:
                newheaders.append((k, v))
        newheaders.extend((k,v) for k,v in headers if normalizeHeader(k) not in (b'connection', \
                          b'date', b'transfer-encoding'))
        return newheaders
    class _HttpResponse(object):
        def __init__(self, status, headers, outputstream, trailers = None):
            self.status = status
            self.headers = headers
            self.outputstream = outputstream
            self.trailers = trailers
    def createResponse(self, connection, xid, status, headers, outputstream):
        status = _createstatus(status)
        headers = self._createResponseheaders(connection, xid, headers, status)
        return self._HttpResponse(status, headers, outputstream)
    def createErrorResponse(self, connection, xid, status):
        status = _createstatus(status)
        content = b'<h1>' + escape_b(status) + b'</h1>'
        content_length = len(content)
        return self.createResponse(connection, xid, status, [(b'Content-Length', str(content_length).encode('ascii'))], MemoryStream(content))
    def createContinueResponse(self, connection, xid):
        return self.createResponse(connection, xid, b'100 Continue', [], None)
    class _HttpContinueStream(Stream):
        '''
        Send a 100-continue before reading data
        '''
        def __init__(self, protocol, connection, xid):
            Stream.__init__(self)
            self.http_continue = False
            self.http_protocol = protocol
            self.http_connection = connection
            self.http_xid = xid
        def prepareRead(self, container):
            if not self.http_continue:
                self.http_continue = True
                event = self.http_protocol.responseTo(self.http_connection, self.http_xid, 
                                              self.http_protocol.createContinueResponse(self.http_connection, self.http_xid))
                if event is not None:
                    for m in container.waitForSend(event):
                        yield m
            for m in Stream.prepareRead(self, container):
                yield m
    def responseTo(self, connection, xid, response):
        '''
        Return an event if notify is necessary
        '''
        if xid < connection.http_responsexid:
            raise HttpProtocolException('xid %r had responses already' % (xid,))
        if xid in connection.http_responsebuffer:
            rbuffer = connection.http_responsebuffer[xid]
            if len(rbuffer) > 0:
                last = rbuffer[-1]
                if last.status[0] != b'1'[0]:
                    if response.status[0] != b'1'[0]:
                        raise HttpProtocolException('Cannot response with a non-1xx status code more than once')
                    else:
                        rbuffer.insert(-1, response)
                else:
                    rbuffer.append(response)
            else:
                rbuffer.append(response)
                if xid == connection.http_responsexid:
                    return HttpStateChange(connection, connection.connmark, HttpStateChange.NEXTOUTPUT)
        else:
            connection.http_responsebuffer[xid] = [response]
            if xid == connection.http_responsexid:
                return HttpStateChange(connection, connection.connmark, HttpStateChange.NEXTOUTPUT)
        return None
    def startResponse(self, connection, xid, status, headers, outputstream, disabledeflate = False):
        """
        Start to response to a request with the specified xid on the connection, with status code
        and headers. The output stream is used to output the response body.
        """
        resp = self.createResponse(connection, xid, status, headers, outputstream)
        resp.disabledeflate = disabledeflate
        event = self.responseTo(connection, xid, resp)
        if event is not None:
            connection.scheduler.emergesend(event)
        return resp
    def _createrequestheaders(self, connection, host, method, headers, stream = None, keepalive = True):
        defaultheaders = self.defaultrequestheaders
        existingHeaders = set(normalizeHeader(k) for k,_ in headers)
        if connection.http_localversion < '1.1' or not keepalive:
            newheaders = [(b'Connection', b'Close')]
        else:
            newheaders = [(b'Connection', b'Keep-Alive')]
        existingHeaders.add(b'connection')
        if host is not None:
            newheaders.append((b'Host', host))
        existingHeaders.add(b'host')
        if connection.http_localversion >= '1.1':
            if self.allowtransfercompress and b'te' not in existingHeaders:
                existingHeaders.add(b'te')
                newheaders.append((b'TE', b'deflate'))
        if b'content-length' not in existingHeaders and method != b'CONNECT':
            if stream is None:
                size = 0
            else:
                try:
                    size = len(stream)
                except:
                    size = None
            if size is None:
                pass
            elif size == 0:
                if method in (b'PUT', b'POST') or stream is not None:
                    # If you give a MemoryStream(b'') to a GET request
                    # there will be a content-length header, which may cause problem
                    # for some servers
                    newheaders.append((b'Content-Length', b'0'))
            else:
                newheaders.append((b'Content-Length', str(size).encode('ascii')))
            existingHeaders.add(b'content-length')
        for k,v in defaultheaders:
            if normalizeHeader(k) not in existingHeaders:
                newheaders.append((k, v))
        newheaders.extend((k,v) for k,v in headers if normalizeHeader(k) not in (b'connection', b'transfer-encoding', b'host', b'expect'))
        return newheaders
    def sendRequest(self, connection, host, path = b'/', method = b'GET', headers = [], stream = None, keepalive = True):
        '''
        If you do not provide a content-length header, the stream will be transfer-encoded with chunked, and it is not
        always acceptable by servers.
        
        You may provide a MemoryStream, and it will provide a content-length header automatically
        
        Return xid
        '''
        method = method.upper()
        headers = self._createrequestheaders(connection, host, method, headers, stream, keepalive)
        if not connection.connected or connection.http_requestbuffer is None:
            raise HttpConnectionClosedException('Connection is closed')
        notify = not connection.http_requestbuffer
        connection.http_requestbuffer.append((host, path, method, headers, stream, keepalive))
        if notify:
            connection.scheduler.emergesend(HttpStateChange(connection, connection.connmark, HttpStateChange.NEXTOUTPUT))
        return connection.xid + len(connection.http_requestbuffer) - 1
    def responsematcher(self, connection, xid, isfinal = None, iserror = None):
        """
        Create an event matcher to match the response
        """
        return HttpResponseEvent.createMatcher(connection, connection.connmark, xid, isfinal, iserror)
    def statematcher(self, connection, state = HttpConnectionStateEvent.CLIENT_CLOSE, currentconn = True):
        """
        Create an event matcher to match the connection state
        """
        return HttpConnectionStateEvent.createMatcher(state, connection, connection.connmark if currentconn else None)
    def requestwithresponse(self, container, connection, host, path = b'/', method = b'GET', headers = [], stream = None, keepalive = True):
        """
        Send a HTTP request, and wait for the response. The last (usually wanted) response is stored in
        `container.http_finalresponse`. There may be multiple responses (1xx) for this request, they are
        stored in `container.http_responses`
        """
        xid = self.sendRequest(connection, host, path, method, headers, stream, keepalive)
        resp = self.responsematcher(connection, xid)
        stat = self.statematcher(connection)
        resps = []
        while True:
            yield (resp, stat)
            if container.matcher is stat:
                raise HttpConnectionClosedException('Connection closed before response received')
            r = container.event
            resps.append(r)
            if r.isfinal:
                container.http_finalresponse = r
                break
        container.http_responses = resps
    def parse(self, connection, data, laststart):
        events = []
        if hasattr(data, 'tobytes'):
            data = data.tobytes()
        else:
            data = data[:]

        if connection.http_parsestage == 'end':
            # Ignore more data
            return (events, 0)
        if connection.http_remoteversion is None and not self.server and \
                connection.http_parsestage == 'headline' and self.allownoheader:
            # Try to find out if there is a status line as early as possible
            # len('HTTP/1.1 ') == 9 
            if len(data) <= 9:
                hasstatus = statuscheckshort.match(data)
            else:
                hasstatus = statuscheck.match(data)
            if not hasstatus:
                connection.http_parsestage = 'body'
                connection.http_remoteversion = '0.9'
                connection.http_keepalive = False
                connection.http_contentlength = None
                connection.http_deflate = False
                connection.http_chunked = False
                connection.http_dataread = 0
                responsestream = Stream()
                responsestream.trailers = []
                events.append(HttpResponseEvent(connection,
                                             connection.connmark,
                                             connection.http_responsexid,
                                             True,
                                             False,
                                             statuscode = 200,
                                             status = b'200 OK',
                                             statustext = b'OK',
                                             headers = [],
                                             headerdict = {},
                                             setcookies = [],
                                             stream = responsestream
                                             ))
                connection.http_currentstream = responsestream
                connection.http_responsexid += 1
        stage = connection.http_parsestage
        start = 0
        end = len(data)
        def httpfail(code = 400):
            if self.debugging:
                self._logger.debug('HTTP Protocol violation detected')
            if not self.server or self.fastfail:
                events.append(ConnectionControlEvent(connection, ConnectionControlEvent.RESET, True, connection.connmark))
            else:
                connection.http_keepalive = False
                event = self.responseTo(connection, connection.xid,
                            self.createErrorResponse(connection, connection.xid, code))
                if connection.xid == connection.http_responsexid:
                    events.append(HttpStateChange(connection, connection.connmark, HttpStateChange.NEXTINPUT))
                connection.http_reqteinfo.append((b'GET', False, False, False))
                connection.xid += 1
                connection.http_idle = True
                if event is not None:
                    events.append(event)
        def stopstream(stoptype = StreamDataEvent.STREAM_ERROR):
            events.append(StreamDataEvent(connection.http_currentstream,
                          stoptype,
                          data = b''))
            if not hasattr(connection.http_currentstream, 'trailers'):
                connection.http_currentstream.trailers = []
                events.append(HttpTrailersReceived(connection.http_currentstream))
            connection.http_currentstream.writeclosed = True
            connection.http_currentstream = None
            connection.http_keepalive = False
            connection.http_idle = True
            if not self.server:
                events.append(ConnectionControlEvent(connection, ConnectionControlEvent.SHUTDOWN, False, connection.connmark))                            
                events.append(HttpResponseEndEvent(connection, connection.connmark,
                                                   connection.http_responsexid - 1,
                                                   False))
        while start < end:
            if stage == 'end':
                start = end
            elif stage == 'headers':
                ls = data.find(b'\r\n\r\n', start)
                if ls < 0:
                    break
                header_lines = data[start:ls].split(b'\r\n')
                start = ls + 4
                if self.headercountlimit is not None and len(header_lines) > self.headercountlimit:
                    self._logger.info('Bad header detected: header count limit exceeded')
                    stage = 'end'
                    httpfail(431)
                    continue
                http_headers = []
                for line in header_lines:
                    key, sep, value = line.partition(b':')
                    if not sep or not token_r.match(key):
                        # Bad header
                        self._logger.info('Bad header detected: %r', line)
                        stage = 'end'
                        httpfail(400)
                        break
                    else:
                        http_headers.append((key, value.strip()))
                if stage == 'end':
                    continue
                connection.http_headers = http_headers
                # An empty line indicates end of headers
                # Processing headers
                headerdict = {}
                setcookies = []
                for k,v in http_headers:
                    kn = normalizeHeader(k)
                    if kn == b'set-cookie':
                        # RFC said set-cookie is a special case
                        # it may appear multiple times and cannot be joined
                        setcookies.append(v)
                    elif kn in headerdict:
                        # RFC said we can join multiple headers with ,
                        # without changing the meaning
                        headerdict[kn] = headerdict[kn] + b', ' + v
                    else:
                        headerdict[kn] = v
                if connection.http_localversion >= '1.1':
                    connection_options = [normalizeHeader(k.strip()) for k in headerdict.get(b'connection', b'').split(b',') if k.strip() != b'']
                    if connection.http_remoteversion < '1.1':
                        connection.http_keepalive = b'keep-alive' in connection_options
                        #connection.http_keepalive = False
                    else:
                        connection.http_keepalive = b'close' not in connection_options
                else:
                    connection.http_keepalive = False
                # reqteinfo is in format: (method, acceptTrailers, acceptDeflate, acceptChunk) 
                if self.server:
                    if connection.http_method == b'HEAD':
                        connection.http_reqteinfo.append((connection.http_method, False, False, False))
                    elif connection.http_remoteversion >= '1.1':
                        transfer_options = tuple(normalizeHeader(k.strip()) for k in headerdict.get(b'te', b'').split(b',') if k.strip() != b'')
                        transfer_trailers = b'trailers' in transfer_options
                        transfer_te = tuple(k.split(b';',2)[0].strip() for k in transfer_options if k != b'trailers')
                        transfer_deflate = b'deflate' in transfer_te
                        connection.http_reqteinfo.append((connection.http_method, transfer_trailers, transfer_deflate, True))
                    else:
                        connection.http_reqteinfo.append((connection.http_method, False, False, False))
                # Determine message body
                if self.server:
                    stage = 'body'
                    if connection.http_method == b'CONNECT':
                        # A tunnel
                        connection.http_contentlength = None
                        connection.http_chunked = False
                        connection.http_deflate = False
                        connection.http_keepalive = False
                    elif connection.http_remoteversion >= '1.1' and b'transfer-encoding' in headerdict:
                        connection.http_contentlength = None
                        transferencoding = tuple(k.strip().lower() for k in headerdict[b'transfer-encoding'].split(b',') if k.strip() != b'')
                        if transferencoding and transferencoding[-1] == b'chunked':
                            connection.http_chunked = True
                            transferencoding = transferencoding[:-1]
                        # For security reason, we do not accept data compressed more than once
                        # Do not allow a chunked in chunked
                        if not transferencoding:
                            connection.http_deflate = False
                            if not connection.http_chunked:
                                connection.http_keepalive = False
                        elif len(transferencoding) > 1 or transferencoding[0] == b'chunked':
                            if self.debugging:
                                self._logger.debug('Unacceptable transfer encoding: %r', headerdict[b'transfer-encoding'])
                            stage = 'end'
                            httpfail(400)
                        elif transferencoding[0] != b'deflate':
                            if self.debugging:
                                self._logger.debug('Unacceptable transfer encoding: %r', headerdict[b'transfer-encoding'])
                            # We only accept deflate as the compress format
                            stage = 'end'
                            httpfail(501)
                        else:
                            connection.http_deflate = True
                            if not connection.http_chunked:
                                connection.http_keepalive = False
                    elif b'content-length' in headerdict:
                        try:
                            connection.http_contentlength = int(headerdict[b'content-length'])
                        except:
                            # Illegal content-length
                            if self.debugging:
                                self._logger.debug('Illegal content length: %r', headerdict[b'content-length'])
                            stage = 'end'
                            httpfail(400)
                        else:
                            if connection.http_contentlength < 0:
                                # Illegal content-length
                                if self.debugging:
                                    self._logger.debug('Illegal content length: %r', headerdict[b'content-length'])
                                stage = 'end'
                                httpfail(400)
                            elif connection.http_contentlength == 0:
                                # Empty body
                                connection.http_contentlength = 0
                            else:
                                connection.http_deflate = False
                                connection.http_chunked = False
                    else:
                        # Empty body
                        connection.http_contentlength = 0
                else:
                    if connection.http_statuscode >= 100 and connection.http_statuscode < 200:
                        # 1xx response has no body, and is not a final response
                        connection.http_contentlength = 0
                    else:
                        teinfo = connection.http_reqteinfo.popleft()
                        if teinfo[0] == b'HEAD':
                            # HEAD response has no body
                            connection.http_chunked = False
                            connection.http_deflate = False
                            connection.http_contentlength = 0
                        elif teinfo[0] == b'CONNECT':
                            connection.http_contentlength = None
                            connection.http_chunked = False
                            connection.http_deflate = False
                        elif connection.http_statuscode == 304 or connection.http_statuscode == 204:
                            # 204 and 304 response has no body
                            connection.http_chunked = False
                            connection.http_deflate = False
                            connection.http_contentlength = 0
                        elif connection.http_remoteversion >= '1.1' and b'transfer-encoding' in headerdict:
                            connection.http_contentlength = None
                            transferencoding = tuple(k.strip().lower() for k in headerdict[b'transfer-encoding'].split(b',') if k.strip() != b'')
                            if transferencoding and transferencoding[-1] == b'chunked':
                                connection.http_chunked = True
                                transferencoding = transferencoding[:-1]
                            # For security reason, we do not accept data compressed more than once
                            # Do not allow a chunked in chunked
                            if not transferencoding:
                                connection.http_deflate = False
                                if not connection.http_chunked:
                                    connection.http_keepalive = False
                            elif len(transferencoding) > 1 or transferencoding[0] == b'chunked':
                                if self.debugging:
                                    self._logger.debug('Unacceptable transfer encoding: %r', headerdict[b'transfer-encoding'])
                                stage = 'end'
                                httpfail()
                            elif transferencoding[0] != b'deflate':
                                # We only accept deflate as the compress format
                                if self.debugging:
                                    self._logger.debug('Unacceptable transfer encoding: %r', headerdict[b'transfer-encoding'])
                                stage = 'end'
                                httpfail()
                            else:
                                connection.http_deflate = True
                                if not connection.http_chunked:
                                    connection.http_keepalive = False
                        elif b'content-length' in headerdict:
                            try:
                                connection.http_contentlength = int(headerdict[b'content-length'])
                            except:
                                # Illegal content-length
                                stage = 'end'
                                httpfail()
                            else:
                                if connection.http_contentlength < 0:
                                    # Illegal content-length
                                    httpfail()
                                elif connection.http_contentlength == 0:
                                    # Empty body
                                    connection.http_contentlength = 0
                                else:
                                    connection.http_deflate = False
                                    connection.http_chunked = False
                        else:
                            # A undetermined-length body
                            connection.http_contentlength = None
                            connection.http_deflate = False
                            connection.http_chunked = False
                            connection.http_keepalive = False
                expect_100 = False
                if stage != 'end' and self.server:
                    if b'expect' in headerdict:
                        if headerdict[b'expect'].lower() != b'100-continue':
                            if self.debugging:
                                self._logger.debug('Unacceptable expect: %r', headerdict[b'expect'])
                            httpfail(417)
                        else:
                            expect_100 = True
                    else:
                        expect_100 = False
                if stage != 'end':
                    if connection.http_contentlength == 0:
                        inputstream = MemoryStream(b'')
                        if connection.http_keepalive:
                            stage = 'headline'
                        else:
                            stage = 'end'
                        connection.http_idle = True
                        inputstream.trailers = []
                    else:
                        if expect_100:
                            inputstream = self._HttpContinueStream(self, connection, connection.xid)
                        else:
                            inputstream = Stream()
                        if connection.http_deflate:
                            connection.http_deflateobj = zlib.decompressobj()
                        if connection.http_chunked:
                            stage = 'chunkedheader'
                        else:
                            stage = 'body'
                            inputstream.trailers = []
                            connection.http_dataread = 0
                        connection.http_currentstream = inputstream
                    if self.server:
                        self._logger.info('Request received from connection %r, xid = %d: %r', connection, connection.xid,
                                          connection.http_method + b' ' + connection.http_path + b' HTTP/' + connection.http_remoteversion.encode('ascii'))
                        r = HttpRequestEvent(headerdict.get(b'host', b''),
                                                       connection.http_path,
                                                       connection.http_method,
                                                       connection,
                                                       connection.connmark,
                                                       connection.xid,
                                                       self,
                                                       headers = connection.http_headers,
                                                       headerdict = headerdict,
                                                       setcookies = setcookies,
                                                       stream = inputstream
                                                       )
                        if connection.xid >= connection.http_responsexid + self.pipelinelimit:
                            connection.http_requestbuffer.append(r)
                        else:
                            events.append(r)
                        connection.xid += 1
                    else:
                        self._logger.info('Response received from connection %r, xid = %d: %r', connection, connection.http_responsexid,
                                          b'HTTP/' + connection.http_remoteversion.encode('utf-8') + b' ' + connection.http_status)
                        events.append(HttpResponseEvent(connection,
                                                     connection.connmark,
                                                     connection.http_responsexid,
                                                     connection.http_statuscode < 100 or connection.http_statuscode >= 200,
                                                     connection.http_statuscode >= 400,
                                                     statuscode = connection.http_statuscode,
                                                     status = connection.http_status,
                                                     statustext = connection.http_statustext,
                                                     headers = connection.http_headers,
                                                     headerdict = headerdict,
                                                     setcookies = setcookies,
                                                     stream = inputstream
                                                     ))
                        if connection.http_statuscode < 100 or connection.http_statuscode >= 200:
                            connection.http_responsexid += 1
                            if stage == 'headline':
                                events.append(HttpResponseEndEvent(connection, connection.connmark,
                                                                   connection.http_responsexid - 1,
                                                                   True))
                            elif stage == 'end':
                                events.append(HttpResponseEndEvent(connection, connection.connmark,
                                                                   connection.http_responsexid - 1,
                                                                   False))
            elif stage == 'headline' or stage == 'chunkedheader' or stage == 'trailers':
                ls = data.find(b'\r\n', start)
                if ls < 0:
                    break
                le = ls + 2
                if stage == 'headline':
                    if start == ls:
                        # RFC said that we should ignore at least one empty line
                        # to achieve robustness
                        pass
                    elif self.server:
                        match = requestline.match(data[start:ls])
                        if match is None:
                            # Bad request
                            if connection.http_remoteversion is None:
                                connection.http_remoteversion = '1.0'
                            stage = 'end'
                            if self.debugging:
                                if ls - start > 200:
                                    self._logger.info('Invalid request from connection %r: %r...', connection, data[start:start + 200])
                                else:
                                    self._logger.info('Invalid request from connection %r: %r', connection, data[start:ls])
                            httpfail(400)
                        else:
                            connection.http_remoteversion = str(match.group(3).decode('ascii'))
                            connection.http_path = match.group(2)
                            connection.http_method = match.group(1).upper()
                            connection.http_headers = []
                            stage = 'headers'
                            if self.debugging:
                                if ls - start > 200:
                                    self._logger.debug('First line of request received from connection %r: %r...', connection, data[start:start + 200])
                                else:
                                    self._logger.debug('First line of request received from connection %r: %r', connection, data[start:ls])
                            connection.http_idle = False
                            if connection.xid == connection.http_responsexid:
                                events.append(HttpStateChange(connection, connection.connmark, HttpStateChange.NEXTINPUT))
                    else:
                        match = statusline.match(data[start:ls])
                        if match is None:
                            if self.debugging:
                                self._logger.info('Bad headline detected: %r', data[start:ls])
                            # Bad response
                            stage = 'end'
                            connection.http_remoteversion = '1.0'
                            httpfail()
                        else:
                            connection.http_remoteversion = str(match.group(1).decode('ascii'))
                            connection.http_status = match.group(2) + b' ' + match.group(3)
                            connection.http_statuscode = int(match.group(2))
                            connection.http_statustext = match.group(3)
                            connection.http_headers = []
                            stage = 'headers'
                            if self.debugging:
                                if ls - start > 200:
                                    self._logger.debug('First line of response received from connection %r: %r...', connection, data[start:start + 200])
                                else:
                                    self._logger.debug('First line of response received from connection %r: %r', connection, data[start:ls])
                            connection.http_idle = False
                elif stage == 'chunkedheader':
                    chunked_header = data[start:ls]
                    chunked_header, _, _ = chunked_header.partition(b';')
                    if self.chunkednumberlimit is not None and len(chunked_header) > self.chunkednumberlimit:
                        stage = 'end'
                        stopstream()
                    else:
                        try:
                            chunked_size = int(chunked_header, 16)
                        except Exception:
                            stage = 'end'
                            stopstream(StreamDataEvent.STREAM_ERROR)
                        else:
                            connection.http_chunkedsize = chunked_size
                            if chunked_size == 0:
                                # Last chunked
                                events.append(StreamDataEvent(connection.http_currentstream,
                                                              StreamDataEvent.STREAM_EOF,
                                                              data = b''))
                                connection.http_currentstream.writeclosed = True
                                connection.http_trailers = []
                                stage = 'trailers'
                            else:
                                connection.http_dataread = 0
                                stage = 'body'
                else:
                    # Tailers
                    if ls == start:
                        # End of chunked
                        connection.http_currentstream.trailers = connection.http_trailers
                        events.append(HttpTrailersReceived(connection.http_currentstream))
                        connection.http_currentstream = None
                        connection.http_idle = True
                        if connection.http_keepalive:
                            stage = 'headline'
                            if not self.server:
                                if connection.http_statuscode < 100 or connection.http_statuscode >= 200:
                                    events.append(HttpResponseEndEvent(connection, connection.connmark,
                                                                       connection.http_responsexid - 1,
                                                                       True))
                        else:
                            stage = 'end'
                            if not self.server:
                                # Close connection
                                events.append(ConnectionControlEvent(connection, ConnectionControlEvent.SHUTDOWN, False, connection.connmark))
                                if connection.http_statuscode < 100 or connection.http_statuscode >= 200:
                                    events.append(HttpResponseEndEvent(connection, connection.connmark,
                                                                       connection.http_responsexid - 1,
                                                                       True))
                    else:
                        match = headerline.match(data[start:ls])
                        if match is None:
                            # Bad header
                            stage = 'end'
                            connection.http_keepalive = False
                            # Ignore all trailers
                            connection.http_currentstream.trailers = []
                            events.append(HttpTrailersReceived(connection.http_currentstream))
                            connection.http_currentstream = None
                        else:
                            connection.http_trailers.append((match.group(1), match.group(2)))
                            if self.headercountlimit is not None and len(connection.http_trailers) > self.headercountlimit:
                                stage = 'end'
                                connection.http_keepalive = False
                                # Ignore all trailers
                                connection.http_currentstream.trailers = []
                                events.append(HttpTrailersReceived(connection.http_currentstream))
                                connection.http_currentstream = None
                                connection.http_idle = True
                start = le
            elif stage == 'chunkedtail':
                # Always \r\n
                if start + 2 > end:
                    break
                if not data[start:start+2] == b'\r\n':
                    stage = 'end'
                    stopstream()
                else:
                    start += 2
                    stage = 'chunkedheader'
            else:
                if stage != 'body':
                    raise ValueError('stage == ' + stage)
                # Body
                if connection.http_chunked:
                    totalsize = connection.http_chunkedsize
                else:
                    totalsize = connection.http_contentlength
                if getattr(connection, 'http_dataread', None) is None:
                    raise ValueError('Invalid read on connection: ' + repr(connection) + ',' + repr(totalsize) + ',' + repr(getattr(connection, 'http_dataread', None)))
                if totalsize is not None:
                    resumesize = totalsize - connection.http_dataread
                if totalsize is not None and resumesize <= end - start:
                    readdata = data[start: start + resumesize]
                    start += resumesize
                    connection.http_dataread += resumesize
                    if connection.http_chunked:
                        stage = 'chunkedtail'
                        eof = False
                    elif connection.http_keepalive:
                        stage = 'headline'
                        eof = True
                        connection.http_idle = True
                        if not self.server:
                            if connection.http_statuscode < 100 or connection.http_statuscode >= 200:
                                events.append(HttpResponseEndEvent(connection, connection.connmark,
                                                                   connection.http_responsexid - 1,
                                                                   True))
                    else:
                        stage = 'end'
                        eof = True
                        connection.http_idle = True
                        if not self.server:
                            if connection.http_statuscode < 100 or connection.http_statuscode >= 200:
                                events.append(HttpResponseEndEvent(connection, connection.connmark,
                                                                   connection.http_responsexid - 1,
                                                                   False))
                    if connection.http_deflate:
                        try:
                            readdata = connection.http_deflateobj.decompress(readdata)
                            readdata += connection.http_deflateobj.flush()
                        except zlib.error:
                            stage = 'end'
                            stopstream()
                            continue
                    if readdata or eof:
                        events.append(StreamDataEvent(connection.http_currentstream,
                                                      StreamDataEvent.STREAM_EOF if eof else StreamDataEvent.STREAM_DATA,
                                                      data = readdata))
                    if eof:
                        connection.http_currentstream.writeclosed = True
                        connection.http_currentstream = None
                        if not self.server and stage == 'end':
                            events.append(ConnectionControlEvent(connection, ConnectionControlEvent.SHUTDOWN, False, connection.connmark))                            
                else:
                    readdata = data[start:]
                    if connection.http_deflate:
                        try:
                            readdata = connection.http_deflateobj.decompress(readdata)
                        except zlib.error:
                            stage = 'end'
                            stopstream()
                            continue
                    if readdata:
                        events.append(StreamDataEvent(connection.http_currentstream,
                                                      StreamDataEvent.STREAM_DATA,
                                                      data = readdata))
                    connection.http_dataread += end - start
                    start = end
        if stage == 'headline' and self.headlinelimit is not None and end - start > self.headlinelimit:
            stage = 'end'
            start = end
            if self.debugging:
                self._logger.debug('Headline exceeds limit on connection %r: %r...', connection, data[start:start + 100])
            httpfail(400)
        if stage == 'headers' and self.headerlimit is not None and end - start > self.headerlimit:
            stage = 'end'
            start = end
            if self.debugging:
                self._logger.debug('A single header exceeds limit on connection %r: %r...', connection, data[start:start + 100])
            httpfail(431)
        if stage == 'chunkedheader' and self.chunkedheaderlimit is not None and end - start > self.chunkedheaderlimit:
            stage = 'end'
            start = end
            if self.debugging:
                self._logger.debug('A chunked header exceeds limit on connection %r: %r...', connection, data[start:start + 100])
            stopstream()
        if stage == 'body' and laststart == end:
            stage = 'end'
            if not connection.http_chunked and connection.http_contentlength is None:
                stopstream(StreamDataEvent.STREAM_EOF)
            else:
                stopstream(StreamDataEvent.STREAM_ERROR)
        elif laststart == end:
            # Remote write-close.
            if not self.server:
                # server closed the connection, we should also close the connection
                httpfail()
        connection.http_parsestage = stage
        return (events, end - start)
    def beforelisten(self, tcpserver, newsock):
        try:
            tcpserver.scheduler.queue['read'].addSubQueue(5, PollEvent.createMatcher(newsock.fileno(), PollEvent.READ_READY), tcpserver)
        except:
            pass
        if False:
            yield

    def serverfinal(self, tcpserver):
        try:
            for m in tcpserver.syscall(syscall_clearremovequeue(tcpserver.scheduler.queue['read'], tcpserver)):
                yield m
        except:
            pass
    def notconnected(self, connection):
        for m in connection.waitForSend(HttpConnectionStateEvent(HttpConnectionStateEvent.CLIENT_NOTCONNECTED,
                                                                 connection,
                                                                 connection.connmark,
                                                                 self)):
            yield m
    def final(self, connection):
        if hasattr(connection, 'http_sender') and connection.http_sender:
            connection.http_sender.close()
        for m in Protocol.final(self, connection):
            yield m
    def waitForResponseEnd(self, container, connection, connmark, xid):
        if not connection.connected or connection.connmark != connmark:
            container.retvalue = False
        elif connection.http_responsexid > xid + 1:
            container.retvalue = True
        elif connection.http_responsexid == xid + 1 and connection.http_parsestage in ('end', 'headline', 'headers'):
            container.retvalue = connection.http_parsestage != 'end'
        else:
            re = HttpResponseEndEvent.createMatcher(connection, connmark, xid)
            rc = self.statematcher(connection)
            yield (re, rc)
            if container.matcher is re:
                container.retvalue = container.event.keepalive
            else:
                container.retvalue = False

'''
Created on 2015/11/24

:author: hubo
'''
from vlcp.config import config
from vlcp.protocol.http import Http, HttpResponseEvent, HttpConnectionStateEvent,\
    HttpConnectionClosedException, HttpProtocolException, HttpResponseEndEvent
from email.message import Message
from vlcp.event.stream import MemoryStream
from vlcp.event.event import Event, withIndices
from vlcp.event.connection import Client
from vlcp.event.core import TimerEvent
from vlcp.config.config import Configurable
from contextlib import closing

try:
    from Cookie import Morsel
    from cookielib import CookieJar, request_host
    from urlparse import urlsplit, urlunsplit, urljoin
    from urllib import quote
except:
    from http.cookiejar import CookieJar, request_host
    from urllib.parse import urlsplit, urlunsplit, quote, urljoin
from vlcp.utils import encoders
import re
# From Python 2.7 ssl library

try:
    unicode
except Exception:
    unicode = str

def _dnsname_match(dn, hostname, max_wildcards=1):
    """Matching according to RFC 6125, section 6.4.3

    http://tools.ietf.org/html/rfc6125#section-6.4.3
    """
    pats = []
    if not dn:
        return False

    pieces = dn.split(r'.')
    leftmost = pieces[0]
    remainder = pieces[1:]

    wildcards = leftmost.count('*')
    if wildcards > max_wildcards:
        # Issue #17980: avoid denials of service by refusing more
        # than one wildcard per fragment.  A survery of established
        # policy among SSL implementations showed it to be a
        # reasonable choice.
        raise CertificateException(
            "too many wildcards in certificate DNS name: " + repr(dn))

    # speed up common case w/o wildcards
    if not wildcards:
        return dn.lower() == hostname.lower()

    # RFC 6125, section 6.4.3, subitem 1.
    # The client SHOULD NOT attempt to match a presented identifier in which
    # the wildcard character comprises a label other than the left-most label.
    if leftmost == '*':
        # When '*' is a fragment by itself, it matches a non-empty dotless
        # fragment.
        pats.append('[^.]+')
    elif leftmost.startswith('xn--') or hostname.startswith('xn--'):
        # RFC 6125, section 6.4.3, subitem 3.
        # The client SHOULD NOT attempt to match a presented identifier
        # where the wildcard character is embedded within an A-label or
        # U-label of an internationalized domain name.
        pats.append(re.escape(leftmost))
    else:
        # Otherwise, '*' matches any dotless string, e.g. www*
        pats.append(re.escape(leftmost).replace(r'\*', '[^.]*'))

    # add the remaining fragments, ignore any wildcards
    for frag in remainder:
        pats.append(re.escape(frag))

    pat = re.compile(r'\A' + r'\.'.join(pats) + r'\Z', re.IGNORECASE)
    return pat.match(hostname)


def match_hostname(cert, hostname):
    """Verify that *cert* (in decoded format as returned by
    SSLSocket.getpeercert()) matches the *hostname*.  RFC 2818 and RFC 6125
    rules are followed, but IP addresses are not accepted for *hostname*.

    CertificateError is raised on failure. On success, the function
    returns nothing.
    """
    if not cert:
        raise ValueError("empty or no certificate, match_hostname needs a "
                         "SSL socket or SSL context with either "
                         "CERT_OPTIONAL or CERT_REQUIRED")
    dnsnames = []
    san = cert.get('subjectAltName', ())
    for key, value in san:
        if key == 'DNS':
            if _dnsname_match(value, hostname):
                return
            dnsnames.append(value)
    if not dnsnames:
        # The subject is only checked when there is no dNSName entry
        # in subjectAltName
        for sub in cert.get('subject', ()):
            for key, value in sub:
                # XXX according to RFC 2818, the most specific Common Name
                # must be used.
                if key == 'commonName':
                    if _dnsname_match(value, hostname):
                        return
                    dnsnames.append(value)
    if len(dnsnames) > 1:
        raise CertificateException("hostname %r "
            "doesn't match either of %s"
            % (hostname, ', '.join(map(repr, dnsnames))))
    elif len(dnsnames) == 1:
        raise CertificateException("hostname %r "
            "doesn't match %r"
            % (hostname, dnsnames[0]))
    else:
        raise CertificateException("no appropriate commonName or "
            "subjectAltName fields were found")

def _str(s, encoding = 'ascii'):
    if not isinstance(s, str):
        return s.decode(encoding)
    else:
        return s

def _bytes(s, encoding = 'ascii'):
    if isinstance(s, bytes):
        return s
    else:
        return s.encode(encoding)

class WebException(IOError):
    pass

class CertificateException(IOError):
    pass

class ManualRedirectRequired(IOError):
    def __init__(self, msg, response, request, kwargs):
        IOError.__init__(msg)
        self.response = response
        self.location = response.get_header('Location')
        self.request = request
        self.kwargs = kwargs
class Request(object):
    def __init__(self, url, data = None, method = None, headers = {}, origin_req_host = None, unverifiable = False,
                 rawurl = False):
        '''
        :param url: request url
        :param data: request data, can be a str/bytes, or a stream(vlcp.event.stream.XXXStream)
        :param method: request method (GET, POST, ...)
        :param headers: request header dict ({'user-agent':'myagent'})
        :param origin_req_host: origin request host for cookie policy check
        :param unverifiable: unverifiable for cookie policy check
        '''
        self.url = _str(url, 'ascii')
        s = urlsplit(self.url, 'http')
        self.type = 'https' if s.scheme == 'https' else 'http'
        self.host = s.netloc
        if not self.host:
            raise ValueError('Invalid URL: ' + self.url)
        if rawurl:
            self.path = urlunsplit(('', '', s.path, s.query, ''))
        else:
            self.path = urlunsplit(('', '', quote(s.path), quote(s.query,'/&='), ''))
        if not self.path:
            self.path = '/'
        self.data = data
        if method is None:
            if self.data is None:
                self.method = 'GET'
            else:
                self.method = 'POST'
        else:
            self.method = method.upper()
        if self.data is not None:
            if isinstance(self.data, unicode):
                self.data = _bytes(self.data)
        headers = dict(headers)
        self.headers = dict((_str(k), _str(v, 'iso-8859-1')) for k,v in headers.items())
        self.headerdict = dict((k.lower(), v) for k,v in self.headers.items())
        self.headermap = dict((k.lower(), k) for k in self.headers.keys())
        self.undirectedheaders = {}
        self.undirectedheaderdict = {}
        self.undirectedheadermap = {}
        self.hostname = request_host(self)
        if origin_req_host is None:
            origin_req_host = request_host(self)
        self.origin_req_host = origin_req_host
        self.unverifiable = unverifiable
        self.redirect_count = 0
        if self.data and not self.has_header('Content-Type'):
            self.add_header('Content-Type', 'application/x-www-form-urlencoded')
    def get_full_url(self):
        return self.url
    def is_unverifiable(self):
        return self.unverifiable
    def get_type(self):
        return self.type
    def get_header(self, k, default = None):
        if k.lower() == 'host':
            return self.host
        else:
            return self.undirectedheaderdict.get(k.lower(), self.headerdict.get(k.lower(), default))
    def has_header(self, k):
        if k.lower() == 'host':
            return True
        else:
            return k.lower() in self.headerdict or k.lower() in self.undirectedheaderdict
    def get_origin_req_host(self):
        if self.origin_req_host is None:
            return request_host(self.host)
        else:
            return self.origin_req_host
    def header_items(self):
        d = dict(self.headerdict)
        d.update(self.undirectedheaderdict)
        return [(self.undirectedheadermap.get(k, self.headermap.get(k)),v) for k,v in d.items()]
    def add_header(self, k, v):
        self.headers[k] = v
        if k.lower() in self.headerdict:
            del self.headers[self.headermap[k.lower()]]
        self.headerdict[k.lower()] = v
        self.headermap[k.lower()] = k
    def add_unredirected_header(self, k, v):
        self.undirectedheaders[k] = v
        if k.lower() in self.undirectedheaderdict:
            del self.undirectedheaders[self.undirectedheadermap[k.lower()]]
        self.undirectedheaderdict[k.lower()] = v
        self.undirectedheadermap[k.lower()] = k
    def redirect(self, response, **kwargs):
        self.redirect_count += 1
        if self.redirect_count >= 16:
            raise WebException('Too many redirections')
        url = response.get_header('Location')
        if url is None:
            raise WebException('Receiving a 3xx response without Location header')
        oldurl = self.url
        self.url = url
        s = urlsplit(url, 'http')
        self.type = 'https' if s.scheme == 'https' else 'http'
        self.host = s.netloc
        if not self.host:
            # Redirect to a relative url
            self.url = urljoin(oldurl, url)
            s = urlsplit(self.url, 'http')
            self.host = s.netloc
        if not self.host:
            raise ValueError('Invalid redirect URL: ' + self.url)
        self.path = urlunsplit(('', '', quote(s.path), quote(s.query,'/&='), ''))
        self.undirectedheaderdict.clear()
        self.undirectedheadermap.clear()
        self.undirectedheaders.clear()
        if response.status in (307, 308):
            if self.data is not None and not isinstance(self.data, bytes):
                raise ManualRedirectRequired('Must retry post', response, self, kwargs)
        else:
            if self.method != 'HEAD':
                self.method = 'GET'
            self.data = None
class Response(object):
    def __init__(self, url, event, scheduler):
        self.url = url
        self.connection = event.connection
        self.iserror = event.iserror
        self.status = event.statuscode
        self.reason = _str(event.statustext)
        self.fullstatus = _str(event.status)
        self.code = self.status
        self.msg = self.reason
        self.version = event.connection.http_remoteversion
        self._headers = event.headers
        self._headerdict = event.headerdict
        self.headers = [(_str(k), _str(v, 'ISO-8859-1')) for k,v in event.headers]
        self.headerdict = dict((_str(k), _str(v, 'ISO-8859-1')) for k,v in event.headerdict.items())
        self._setcookies = event.setcookies
        self.setcookies = [_str(v, 'ISO-8859-1') for v in event.setcookies]
        self.stream = event.stream
        self.scheduler = scheduler
    def getcode(self):
        return self.code
    def geturl(self):
        return self.url
    def info(self):
        return self
    def get_header(self, k, default = None):
        return self.headerdict.get(k.lower(), default)
    def getheaders(self, k, default = []):
        if k.lower() == 'set-cookie':
            return self.setcookies
        elif k.lower() == 'set-cookie2':
            # Ignore these headers, since they are deprecated
            return default
        else:
            return [v for k2,v in self.headers if k2.lower() == k.lower()]
    def get_all(self, k, default = []):
        return self.getheaders(k, default)
    def has_header(self, k):
        return k.lower() in self.headerdict
    def close(self):
        "Stop the output stream, but further download will still perform"
        if self.stream:
            self.stream.close(self.scheduler)
            self.stream = None
    def shutdown(self):
        "Force stop the output stream, if there are more data to download, shutdown the connection"
        if self.stream:
            if not self.stream.dataeof and not self.stream.dataerror:
                self.stream.close(self.scheduler)
                for m in self.connection.shutdown():
                    yield m
            else:
                self.stream.close(self.scheduler)
            self.stream = None
                
    def __del__(self):
        self.close()
        

@withIndices('host', 'path', 'https')
class WebClientRequestDoneEvent(Event):
    pass

@config('webclient')
class WebClient(Configurable):
    "Convenient HTTP request processing. Proxy is not supported in current version."
    # When a cleanup task is created, the task releases dead connections by this interval
    _default_cleanupinterval = 60
    # Persist this number of connections at most for each host. If all connections are in
    # use, new requests will wait until there are free connections.
    _default_samehostlimit = 20
    # Do not allow multiple requests to the same URL at the same time. If sameurllimit=True,
    # requests to the same URL will always be done sequential.
    _default_sameurllimit = False
    # CA file used to verify HTTPS certificates. To be compatible with older Python versions,
    # the new SSLContext is not enabled currently, so with the default configuration, the
    # certificates are NOT verified. You may configure this to a .pem file in your system,
    # usually /etc/pki/tls/cert.pem in Linux.
    _default_cafile = None
    # When following redirects and the server redirects too many times, raises an exception
    # and end the process
    _default_redirectlimit = 10
    # Verify the host with the host in certificate
    _default_verifyhost = True
    def __init__(self, allowcookies = False, cookiejar = None):
        '''
        :param allowcookies: Accept and store cookies, automatically use them on further requests
        :param cookiejar: Provide a customized cookiejar instead of the default CookieJar()
        '''
        self._connmap = {}
        self._requesting = set()
        self._hostwaiting = set()
        self._pathwaiting = set()
        self._protocol = Http(False)
        self.allowcookies = allowcookies
        if cookiejar is None:
            self.cookiejar = CookieJar()
        else:
            self.cookiejar = cookiejar
        self._tasks = []
    def open(self, container, request, ignorewebexception = False, timeout = None, datagen = None, cafile = None, key = None, certificate = None,
             followredirect = True, autodecompress = False, allowcookies = None):
        '''
        Open http request with a Request object
        
        :param container: a routine container hosting this routine
        :param request: vlcp.utils.webclient.Request object
        :param ignorewebexception: Do not raise exception on Web errors (4xx, 5xx), return a response normally
        :param timeout: timeout on connection and single http request. When following redirect, new request
               does not share the old timeout, which means if timeout=2:
               connect to host: (2s)
               wait for response: (2s)
               response is 302, redirect
               connect to redirected host: (2s)
               wait for response: (2s)
               ...
               
        :param datagen: if the request use a stream as the data parameter, you may provide a routine to generate
                        data for the stream. If the request failed early, this routine is automatically terminated.
                        
        :param cafile: provide a CA file for SSL certification check. If not provided, the SSL connection is NOT verified.
        :param key: provide a key file, for client certification (usually not necessary)
        :param certificate: provide a certificate file, for client certification (usually not necessary)
        :param followredirect: if True (default), automatically follow 3xx redirections
        :param autodecompress: if True, automatically detect Content-Encoding header and decode the body
        :param allowcookies: override default settings to disable the cookies
        '''
        with closing(container.delegateOther(self._open(container, request, ignorewebexception, timeout, datagen, cafile, key, certificate,
                                                    followredirect, autodecompress, allowcookies),
                                             container)) as g:
            for m in g:
                yield m
    def _open(self, container, request, ignorewebexception = False, timeout = None, datagen = None, cafile = None, key = None, certificate = None,
             followredirect = True, autodecompress = False, allowcookies = None):
        if cafile is None:
            cafile = self.cafile
        if allowcookies is None:
            allowcookies = self.allowcookies
        forcecreate = False
        datagen_routine = None
        if autodecompress:
            if not request.has_header('Accept-Encoding'):
                request.add_header('Accept-Encoding', 'gzip, deflate')
        while True:
            # Find or create a connection
            for m in self._getconnection(container, request.host, request.path, request.get_type() == 'https',
                                                forcecreate, cafile, key, certificate, timeout):
                yield m
            (conn, created) = container.retvalue
            # Send request on conn and wait for reply
            try:
                if allowcookies:
                    self.cookiejar.add_cookie_header(request)
                if isinstance(request.data, bytes):
                    stream = MemoryStream(request.data)
                else:
                    stream = request.data
                if datagen and datagen_routine is None:
                    datagen_routine = container.subroutine(datagen)
                else:
                    datagen_routine = None
                for m in container.executeWithTimeout(timeout, self._protocol.requestwithresponse(container, conn, _bytes(request.host), _bytes(request.path), _bytes(request.method),
                                                   [(_bytes(k), _bytes(v)) for k,v in request.header_items()], stream)):
                    yield m
                if container.timeout:
                    if datagen_routine:
                        container.terminate(datagen_routine)
                    container.subroutine(self._releaseconnection(conn, request.host, request.path, request.get_type() == 'https', True), False)
                    raise WebException('HTTP request timeout')
                finalresp = container.http_finalresponse
                resp = Response(request.get_full_url(), finalresp, container.scheduler)
                if allowcookies:
                    self.cookiejar.extract_cookies(resp, request)
                if resp.iserror and not ignorewebexception:
                    try:
                        exc = WebException(resp.fullstatus)
                        if autodecompress and resp.stream:
                            ce = resp.get_header('Content-Encoding', '')
                            if ce.lower() == 'gzip' or ce.lower() == 'x-gzip':
                                resp.stream.getEncoderList().append(encoders.gzip_decoder())
                            elif ce.lower() == 'deflate':
                                resp.stream.getEncoderList().append(encoders.deflate_decoder())
                        for m in resp.stream.read(container, 4096):
                            yield m
                        exc.response = resp
                        exc.body = container.data
                        if datagen_routine:
                            container.terminate(datagen_routine)
                        for m in resp.shutdown():
                            yield m
                        container.subroutine(self._releaseconnection(conn, request.host, request.path, request.get_type() == 'https', True), False)
                        raise exc
                    finally:
                        resp.close()
                else:
                    try:
                        container.subroutine(self._releaseconnection(conn, request.host, request.path, request.get_type() == 'https', False, finalresp), False)
                        if followredirect and resp.status in (300, 301, 302, 303, 307, 308):
                            request.redirect(resp, ignorewebexception = ignorewebexception, timeout = timeout, cafile = cafile, key = key,
                                             certificate = certificate, followredirect = followredirect,
                                             autodecompress = autodecompress, allowcookies = allowcookies)
                            resp.close()
                            continue
                        if autodecompress and resp.stream:
                            ce = resp.get_header('Content-Encoding', '')
                            if ce.lower() == 'gzip' or ce.lower() == 'x-gzip':
                                resp.stream.getEncoderList().append(encoders.gzip_decoder())
                            elif ce.lower() == 'deflate':
                                resp.stream.getEncoderList().append(encoders.deflate_decoder())
                        container.retvalue = resp
                    except:
                        resp.close()
                        raise
            except HttpConnectionClosedException:
                for m in self._releaseconnection(conn, request.host, request.path, request.get_type() == 'https', False):
                    yield m
                if not created:
                    # Retry on a newly created connection
                    forcecreate = True
                    continue
                else:
                    if datagen_routine:
                        container.terminate(datagen_routine)
                    raise
            except Exception as exc:
                for m in self._releaseconnection(conn, request.host, request.path, request.get_type() == 'https', True):
                    yield m
                raise exc
            break
    def _releaseconnection(self, connection, host, path, https = False, forceclose = False, respevent = None):
        if not host:
            raise ValueError
        if forceclose:
            for m in connection.shutdown(True):
                yield m
        if not forceclose and connection.connected and respevent:
            def releaseconn():
                for m in self._protocol.waitForResponseEnd(connection, connection, respevent.connmark, respevent.xid):
                    yield m
                keepalive = connection.retvalue
                conns = self._connmap[host]
                conns[2] -= 1
                if keepalive:
                    connection.setdaemon(True)
                    conns[1 if https else 0].append(connection)
                else:
                    for m in connection.shutdown():
                        yield m
            connection.subroutine(releaseconn(), False)
        else:
            conns = self._connmap[host]
            conns[2] -= 1
        if self.sameurllimit:
            self._requesting.remove((host, path, https))
        if (host, path, https) in self._pathwaiting or host in self._hostwaiting:
            for m in connection.waitForSend(WebClientRequestDoneEvent(host, path, https)):
                yield m
            if (host, path, https) in self._pathwaiting:
                self._pathwaiting.remove((host, path, https))
            if host in self._hostwaiting:
                self._hostwaiting.remove(host)
    def _getconnection(self, container, host, path, https = False, forcecreate = False, cafile = None, key = None, certificate = None,
                       timeout = None):
        if not host:
            raise ValueError
        matcher = WebClientRequestDoneEvent.createMatcher(host, path, https)
        while self.sameurllimit and (host, path, https) in self._requesting:
            self._pathwaiting.add((host, path, https))
            yield (matcher,)
        # Lock the path
        if self.sameurllimit:
            self._requesting.add((host, path, https))
        # connmap format: (free, free_ssl, workingcount)
        conns = self._connmap.setdefault(host, [[],[], 0])
        conns[0] = [c for c in conns[0] if c.connected]
        conns[1] = [c for c in conns[1] if c.connected]
        myset = conns[1 if https else 0]
        if not forcecreate and myset:
            # There are free connections, reuse them
            conn = myset.pop()
            conn.setdaemon(False)
            container.retvalue = (conn, False)
            conns[2] += 1
            return
        matcher = WebClientRequestDoneEvent.createMatcher(host)
        while self.samehostlimit and len(conns[0]) + len(conns[1]) + conns[2] >= self.samehostlimit:
            if myset:
                # Close a old connection
                conn = myset.pop()
                for m in conn.shutdown():
                    yield m
            else:
                # Wait for free connections
                self._hostwaiting.add(host)
                yield (matcher,)
                conns = self._connmap.setdefault(host, [[],[], 0])
                myset = conns[1 if https else 0]
                if not forcecreate and myset:
                    conn = myset.pop()
                    conn.setdaemon(False)
                    container.retvalue = (conn, False)
                    conns[2] += 1
                    return
        # Create new connection
        conns[2] += 1
        conn = Client(urlunsplit(('ssl' if https else 'tcp', host, '/', '', '')), self._protocol, container.scheduler,
                      key, certificate, cafile)
        if timeout is not None:
            conn.connect_timeout = timeout
        conn.start()
        connected = self._protocol.statematcher(conn, HttpConnectionStateEvent.CLIENT_CONNECTED, False)
        notconnected = self._protocol.statematcher(conn, HttpConnectionStateEvent.CLIENT_NOTCONNECTED, False)
        yield (connected, notconnected)
        if container.matcher is notconnected:
            conns[2] -= 1
            for m in conn.shutdown(True):
                yield m
            raise IOError('Failed to connect to %r' % (conn.rawurl,))
        if https and cafile and self.verifyhost:
            try:
                # TODO: check with SSLContext
                hostcheck = re.sub(r':\d+$', '', host)
                if host == conn.socket.remoteaddr[0]:
                    # IP Address is currently now allowed
                    for m in conn.shutdown(True):
                        yield m
                    raise CertificateException('Cannot verify host with IP address')
                match_hostname(conn.socket.getpeercert(False), hostcheck)
            except:
                conns[2] -= 1
                raise
        container.retvalue = (conn, True)
    def cleanup(self, host = None):
        "Cleaning disconnected connections"
        if host is not None:
            conns = self._connmap.get(host)
            if conns is None:
                return
            # cleanup disconnected connections
            conns[0] = [c for c in conns[0] if c.connected]
            conns[1] = [c for c in conns[1] if c.connected]
            if not conns[0] and not conns[1] and not conns[2]:
                del self._connmap[host]
        else:
            hosts = list(self._connmap.keys())
            for h in hosts:
                self.cleanup(h)
    def cleanup_task(self, container, interval = None):
        '''
        If this client object is persist for a long time, and you are worrying about memory leak,
        create a routine with this method: myclient.cleanup_task(mycontainer, 60).
        But remember that if you have created at lease one task, you must call myclient.endtask()
        to completely release the webclient object.
        '''
        if interval is None:
            interval = self.cleanupinterval
        def task():
            th = container.scheduler.setTimer(interval, interval)
            tm = TimerEvent.createMatcher(th)
            try:
                while True:
                    yield (tm,)
                    self.cleanup()
            finally:
                container.scheduler.cancelTimer(th)
        t = container.subroutine(task(), False, daemon = True)
        self._tasks.append(t)
        return t
    def shutdown(self):
        "Shutdown free connections to release resources"
        for c0, c1, _ in list(self._connmap.values()):
            c0bak = list(c0)
            del c0[:]
            for c in c0bak:
                if c.connected:
                    for m in c.shutdown():
                        yield m
            c1bak = list(c1)
            del c1[:]
            for c in c1bak:
                if c.connected:
                    for m in c.shutdown():
                        yield m
    def endtask(self):
        for t in self._tasks:
            t.close()
        del self._tasks[:]
                
    def urlopen(self, container, url, data = None, method = None, headers = {}, rawurl = False, *args, **kwargs):
        '''
        Similar to urllib2.urlopen, but:
        1. is a routine
        2. data can be an instance of vlcp.event.stream.BaseStream, or str/bytes
        3. can specify method
        4. if datagen is not None, it is a routine which writes to <data>. It is automatically terminated if the connection is down.
        5. can also specify key and certificate, for client certification
        6. certificates are verified with CA if provided.
        If there are keep-alived connections, they are automatically reused.
        See open for available arguments
        
        Extra argument:
        
        :param rawurl: if True, assume the url is already url-encoded, do not encode it again.
        '''
        return self.open(container, Request(url, data, method, headers, rawurl=rawurl), *args, **kwargs)
    def manualredirect(self, container, exc, data, datagen = None):
        "If data is a stream, it cannot be used again on redirect. Catch the ManualRedirectException and call a manual redirect with a new stream."
        request = exc.request
        request.data = data
        return self.open(container, request, datagen = datagen, **exc.kwargs)
    def urlgetcontent(self, container, url, data = None, method = None, headers = {}, tostr = False,  encoding = None, rawurl = False, *args, **kwargs):
        '''
        In Python2, bytes = str, so tostr and encoding has no effect.
        In Python3, bytes are decoded into unicode str with encoding.
        If encoding is not specified, charset in content-type is used if present, or default to utf-8 if not.
        See open for available arguments

        :param rawurl: if True, assume the url is already url-encoded, do not encode it again.
        '''
        req = Request(url, data, method, headers, rawurl = rawurl)
        for m in self.open(container, req, *args, **kwargs):
            yield m
        resp = container.retvalue
        encoding = 'utf-8'
        if encoding is None:
            m = Message()
            m.add_header('Content-Type', resp.get_header('Content-Type', 'text/html'))
            encoding = m.get_content_charset('utf-8')
        if not resp.stream:
            content = b''
        else:
            for m in resp.stream.read(container):
                yield m
            content = container.data
        if tostr:
            content = _str(content, encoding)
        container.retvalue = content

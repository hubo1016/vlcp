'''
Created on 2015/11/10

@author: hubo
'''

from protocol.http import HttpProtocolException, HttpRequestEvent
import traceback
from event.stream import Stream, MemoryStream
from event import EventHandler
from event.runnable import RoutineContainer
import base64
try:
    from Cookie import SimpleCookie, Morsel
    from urlparse import parse_qs, urlunsplit, urljoin, urlsplit
    from urllib import unquote, quote, unquote_plus
    unquote_to_bytes = unquote
    quote_from_bytes = quote
    unquote_plus_to_bytes = unquote_plus
except:
    from http.cookies import SimpleCookie, Morsel
    from urllib.parse import parse_qs, urlunsplit, urljoin, urlsplit, unquote_to_bytes, quote_from_bytes
    # There is not an unquote_plus_to_bytes in urllib.parse, but it is simple
    def unquote_plus_to_bytes(s):
        if isinstance(s, str):
            return unquote_to_bytes(s.replace('+', ' '))
        else:
            return unquote_to_bytes(s.replace(b'+', b' '))
import re
from email.message import Message
from server.module import callAPI
import service.utils.session
import functools

import sys
if sys.version_info[0] >= 3:
    from email.parser import BytesFeedParser
else:
    from email.parser import FeedParser
    BytesFeedParser = FeedParser

class HttpInputException(Exception):
    pass

class HttpExitException(Exception):
    pass

class HttpRewriteLoopException(Exception):
    pass

class Environment(object):
    def __init__(self, event, container = None, defaultencoding = 'utf-8'):
        try:
            self.event = event
            self.host = event.host
            self.fullpath = event.path
            self.path_match = getattr(event, 'path_match', None)
            self.path = getattr(event, 'realpath', None)
            self.originalpath = getattr(event, 'originalpath', self.fullpath)
            self.rewritefrom = getattr(event, 'rewritefrom', None)
            self.querystring = getattr(event, 'querystring', None)
            self.method = event.method
            self.connection = event.connection
            self.container = container
            if self.container is None:
                self.container = self.connection
            if hasattr(self.connection.socket, 'ssl_version'):
                self.https = True
            else:
                self.https = False
            self.connmark = event.connmark
            self.xid = event.xid
            self.protocol = event.createby
            if not self.host:
                self.host = getattr(self.protocol, 'defaulthost', None)
                if not self.host:
                    self.host = str(self.connection.localaddr[0]) + ':' + str(self.connection.localaddr[1])
                    self.host = self.host.encode('ascii')
            self.headers = event.headers
            self.headerdict = event.headerdict
            self.setcookies = event.setcookies
            self.inputstream = event.stream
            self.encoding = defaultencoding
            self._sendHeaders = False
            self.outputstream = None
            self.showerrorinfo = self.protocol.showerrorinfo
            # Parse cookies
            self.rawcookie = self.headerdict.get(b'cookie')
            if self.rawcookie is not None and not isinstance(self.rawcookie, str):
                self.rawcookie = self.rawcookie.decode(defaultencoding)
            self.sent_headers = []
            self.sent_cookies = []
            if self.rawcookie:
                self.cookies = dict((k, v.value.encode(defaultencoding)) for k,v in SimpleCookie(self.rawcookie).items())
            else:
                self.cookies = {}
            # Parse query string
            if self.querystring:
                result = parse_qs(self.querystring)
                def convert(k,v):
                    try:
                        k = str(k.decode('ascii'))
                    except:
                        raise HttpInputException('Form-data key must be ASCII')
                    if not k.endswith('[]'):
                        v = v[-1]
                    else:
                        k = k[:-2]
                    return (k,v)
                self.args = dict(convert(k,v) for k,v in result.items())
            else:
                self.args = {}
            self.exception = None
        except Exception as exc:
            # Delay the exception
            self.exception = exc
    def __repr__(self, *args, **kwargs):
        return self.method.upper() + ' ' + self.path + ', ' + self.connection.remoteaddr[0]
    def startResponse(self, status = 200, headers = [], clearheaders = True):
        "Start to send response"
        if self._sendHeaders:
            raise HttpProtocolException('Cannot modify response, headers already sent')
        self.status = status
        if clearheaders:
            self.sent_headers = headers
            self.sent_cookies = []
        else:
            self.sent_headers.extend(headers)
    def header(self, key, value, replace = True):
        "Send a new header"
        if hasattr(key, 'encode'):
            key = key.encode('ascii')
        if hasattr(value, 'encode'):
            value = value.encode(self.encoding)
        if replace:
            self.sent_headers = [(k,v) for k,v in self.sent_headers if k.lower() != key.lower()]
        self.sent_headers.append((key, value))
    def rawheader(self, kv, replace = True):
        if hasattr(kv, 'encode'):
            kv = kv.encode(self.encoding)
        k,v = kv.split(b':', 1)
        self.header(k, v.strip(), replace)
    def setcookie(self, key, value, max_age=None, expires=None, path='/', domain=None, secure=None, httponly=False):
        newcookie = Morsel()
        newcookie.key = key
        newcookie.value = value
        newcookie.coded_value = value
        if max_age is not None:
            newcookie['max-age'] = max_age
        if expires is not None:
            newcookie['expires'] = expires
        if path is not None:
            newcookie['path'] = path
        if domain is not None:
            newcookie['domain'] = domain
        if secure:
            newcookie['secure'] = secure
        if httponly:
            newcookie['httponly'] = httponly
        self.sent_cookies = [c for c in self.sent_cookies if c.key != key]
        self.sent_cookies.append(newcookie)
    def bufferoutput(self):
        self.outputstream = Stream(writebufferlimit=None)
    def _startResponse(self):
        if not hasattr(self, 'status'):
            self.startResponse(200, clearheaders=False)
        if all(k.lower() != b'content-type' for k,_ in self.sent_headers):
            self.header('content-type', 'text/html; charset=' + self.encoding, False)
        # Process cookies
        for c in self.sent_cookies:
            self.rawheader(c.output(), False)
        self.protocol.startResponse(self.connection, self.xid, self.status, self.sent_headers, self.outputstream)
        self._sendHeaders = True
    def rewrite(self, path, method = None):
        "Rewrite this request to another processor. Must be called before header sent"
        if self._sendHeaders:
            raise HttpProtocolException('Cannot modify response, headers already sent')
        if getattr(self.event, 'rewritedepth', 0) >= getattr(self.protocol, 'rewritedepthlimit', 32):
            raise HttpRewriteLoopException
        newpath = urljoin(quote_from_bytes(self.path).encode('ascii'), path)
        if newpath == self.fullpath or newpath == self.originalpath:
            raise HttpRewriteLoopException
        r = HttpRequestEvent(self.host,
                               newpath,
                               self.method if method is None else method,
                               self.connection,
                               self.connmark,
                               self.xid,
                               self.protocol,
                               headers = self.headers,
                               headerdict = self.headerdict,
                               setcookies = self.setcookies,
                               stream = self.inputstream,
                               rewritefrom = self.fullpath,
                               originalpath = self.originalpath,
                               rewritedepth = getattr(self.event, 'rewritedepth', 0) + 1
                               )
        for m in self.connection.waitForSend(r):
            yield m
        self._sendHeaders = True
        self.outputstream = None
    def redirect(self, path, status = 302):
        location = urljoin(urlunsplit((b'https' if self.https else b'http',
                                                                     self.host,
                                                                     quote_from_bytes(self.path).encode('ascii'),
                                                                     '',
                                                                     ''
                                                                     )), path)
        self.startResponse(status, [(b'location', location)])
        for m in self.write(b'<a href="' + self.escape(location, True) + b'">' + self.escape(location) + b'</a>'):
            yield m
        for m in self.flush(True):
            yield m
    def nl2br(self, text):
        if isinstance(text, bytes):
            return text.replace(b'\n', b'<br/>\n')
        else:
            return text.replace('\n', '<br/>\n')
    def escape(self, text, quote = True):
        if isinstance(text, bytes):
            return self.protocol.escape_b(text, quote)
        else:
            return self.protocol.escape(text, quote)
    def error(self, status=500, allowredirect = True, close = True, showerror = None, headers = []):
        if showerror is None:
            showerror = self.showerrorinfo
        if self._sendHeaders:
            if showerror:
                typ, exc, tb = sys.exc_info()
                if exc:
                    for m in self.write('<span style="white-space:pre-wrap">\n', buffering = False):
                        yield m
                    for m in self.writelines((self.nl2br(self.escape(v)) for v in traceback.format_exception(typ, exc, tb)), buffering = False):
                        yield m
                    for m in self.write('</span>\n', close, False):
                        yield m
        elif allowredirect and status in self.protocol.errorrewrite:
            for m in self.rewrite(self.protocol.errorrewrite[status], b'GET'):
                yield m
        elif allowredirect and status in self.protocol.errorredirect:
            for m in self.redirect(self.protocol.errorredirect[status]):
                yield m
        else:
            self.startResponse(status, headers)
            typ, exc, tb = sys.exc_info()
            if showerror and exc:
                for m in self.write('<span style="white-space:pre-wrap">\n', buffering = False):
                    yield m
                for m in self.writelines((self.nl2br(self.escape(v)) for v in traceback.format_exception(typ, exc, tb)), buffering = False):
                    yield m
                for m in self.write('</span>\n', close, False):
                    yield m
            else:
                for m in self.write(b'<h1>' + self.protocol._createstatus(status) + b'</h1>', close, False):
                    yield m
    def write(self, data, eof = False, buffering = True):
        if not self.outputstream:
            self.outputstream = Stream()
            self._startResponse()
        elif (not buffering or eof) and not self._sendHeaders:
            self._startResponse()
        if not isinstance(data, bytes):
            data = data.encode(self.encoding)
        for m in self.outputstream.write(data, self.connection, eof, False, buffering):
            yield m
    def writelines(self, lines, eof = False, buffering = True):
        for l in lines:
            for m in self.write(l, False, buffering):
                yield m
        if eof:
            for m in self.write(b'', eof, buffering):
                yield m
    def flush(self, eof = False):
        for m in self.write(b'', eof, False):
            yield m
    def output(self, stream):
        if self._sendHeaders:
            raise HttpProtocolException('Cannot modify response, headers already sent')
        self.outputstream = stream
        try:
            content_length = len(stream)
        except:
            pass
        else:
            self.header(b'content-length', str(content_length).encode('ascii'))
        self._startResponse()
    def outputdata(self, data):
        self.output(MemoryStream(data))
    def close(self):
        if not self._sendHeaders:
            self._startResponse()
        if self.outputstream is not None:
            for m in self.flush(True):
                yield m
        if hasattr(self, 'session') and self.session:
            for m in self.session.unlock():
                yield m
    def exit(self, output=b''):
        "Exit current HTTP processing"
        raise HttpExitException(output)
    def parseform(self, limit = 67108864):
        '''
        Parse form-data with multipart/form-data or application/x-www-form-urlencoded
        In Python3, the keys of form and files are unicode, but values are bytes
        If the key ends with '[]', it is considered to be a list:
        a=1&b=2&b=3          =>    {'a':1,'b':3}
        a[]=1&b[]=2&b[]=3    =>    {'a':[1],'b':[2,3]}
        '''
        try:
            form = {}
            files = {}
            # If there is not a content-type header, maybe there is not a content.
            if b'content-type' in self.headerdict and self.inputstream is not None:
                contenttype = self.headerdict[b'content-type']
                m = Message()
                # Email library expects string, which is unicode in Python 3
                try:
                    m.add_header('content-type', str(contenttype.decode('ascii')))
                except UnicodeDecodeError:
                    raise HttpInputException('Content-Type has non-ascii characters')
                if m.get_content_type() == 'multipart/form-data':
                    fp = BytesFeedParser()
                    fp.feed(b'content-type: ' + contenttype + b'\r\n\r\n')
                    total_length = 0
                    while True:
                        try:
                            for m in self.inputstream.prepareRead(self.container):
                                yield m
                            data = self.inputstream.readonce()
                            total_length += len(data)
                            if total_length > limit:
                                raise HttpInputException('Data is too large')
                            fp.feed(data)
                        except EOFError:
                            break
                    msg = fp.close()
                    if not msg.is_multipart() or msg.defects:
                        # Reject the data
                        raise HttpInputException('Not valid multipart/form-data format')
                    for part in msg.get_payload():
                        if part.is_multipart() or part.defects:
                            raise HttpInputException('Not valid multipart/form-data format')
                        disposition = part.get_params(header='content-disposition')
                        if not disposition:
                            raise HttpInputException('Not valid multipart/form-data format')
                        disposition = dict(disposition)
                        if 'form-data' not in disposition or 'name' not in disposition:
                            raise HttpInputException('Not valid multipart/form-data format')
                        if 'filename' in disposition:
                            name = disposition['name']
                            if name.endswith('[]'):
                                files.setdefault(name[:-2], []).append({'filename': disposition['filename'], 'content': part.get_payload(decode=True)})
                            else:
                                files[name] = {'filename': disposition['filename'], 'content': part.get_payload(decode=True)}
                        else:
                            name = disposition['name']
                            if name.endswith('[]'):
                                form.setdefault(name[:-2], []).append(part.get_payload(decode=True))
                            else:
                                form[name] = part.get_payload(decode=True)
                elif m.get_content_type() == 'application/x-www-form-urlencoded' or \
                        m.get_content_type() == 'application/x-url-encoded':
                    for m in self.inputstream.read(self.container, limit + 1):
                        yield m
                    data = self.container.data
                    if len(data) > limit:
                        raise HttpInputException('Data is too large')
                    result = parse_qs(data)
                    def convert(k,v):
                        try:
                            k = str(k.decode('ascii'))
                        except:
                            raise HttpInputException('Form-data key must be ASCII')
                        if not k.endswith('[]'):
                            v = v[-1]
                        else:
                            k = k[:-2]
                        return (k,v)
                    form = dict(convert(k,v) for k,v in result.items())
                else:
                    # Other formats, treat like no data
                    pass
            self.form = form
            self.files = files                
        except Exception as exc:
            raise HttpInputException('Failed to parse form-data: ' + str(exc))
    def sessionstart(self):
        "Start session. Must start service.utils.session.Session to use this method"
        for m in callAPI(self.container, 'session', 'start', {'cookies':self.rawcookie}):
            yield m
        self.session, setcookies = self.container.retvalue
        for nc in setcookies:
            self.sent_cookies = [c for c in self.sent_cookies if c.key != nc.key]
            self.sent_cookies.append(nc)
    def sessiondestroy(self):
        if hasattr(self, 'session') and self.session:
            for m in callAPI(self.container, 'session', 'destroy', {'id':self.session.id}):
                yield m
            for m in self.session.unlock(self.container):
                yield m
            del self.session
            setcookies = self.container.retvalue
            for nc in setcookies:
                self.sent_cookies = [c for c in self.sent_cookies if c.key != nc.key]
                self.sent_cookies.append(nc)
    def basicauth(self, realm = b'all', nofail = False):
        "Try to get the basic authorize info, return (username, password) if succeeded, return 401 otherwise"
        if b'authorization' in self.headerdict:
            auth = self.headerdict[b'authorization']
            auth_pair = auth.split(b' ', 1)
            if len(auth_pair) < 2:
                raise HttpInputException('Authorization header is malformed')
            if auth_pair[0].lower() == b'basic':
                try:
                    userpass = base64.b64decode(auth_pair[1])
                except:
                    raise HttpInputException('Invalid base-64 string')
                userpass_pair = userpass.split(b':', 1)
                if len(userpass_pair) != 2:
                    raise HttpInputException('Authorization header is malformed')
                return userpass_pair
        if nofail:
            return (None, None)
        else:
            self.basicauthfail(realm)
    def basicauthfail(self, realm = b'all'):
        if not isinstance(realm, bytes):
            realm = realm.encode('ascii')
        self.startResponse(401, [(b'WWW-Authenticate', b'Basic realm="' + realm + b'"')])
        self.exit(b'<h1>' + self.protocol._createstatus(401) + b'</h1>')

def http(container = None):
    "wrap a WSGI-style class method to a HTTPRequest event handler"
    def decorator(func):
        @functools.wraps(func)
        def handler(self, event):
            try:
                env = Environment(event, container)
                try:
                    if env.exception:
                        raise HttpInputException('Bad request')
                    for m in func(self, env):
                        yield m
                except HttpExitException as exc:
                    if exc.args[0]:
                        for m in env.write(exc.args[0]):
                            yield m
                except HttpInputException:
                    # HTTP 400 Bad Request
                    for m in env.error(400):
                        yield m
                except:
                    if hasattr(self, 'logger'):
                        self.logger.exception('Unhandled exception in HTTP processing, env=%r:', env)
                    for m in env.error(500):
                        yield m
                for m in env.close():
                    yield m
            except:
                # Must ignore all exceptions, or the whole handler is unregistered
                pass
        return handler
    return decorator

def statichttp(container = None):
    "wrap a WSGI-style function to a HTTPRequest event handler"
    def decorator(func):
        @functools.wraps(func)
        def handler(event):
            try:
                env = Environment(event, container)
                try:
                    if env.exception:
                        raise HttpInputException('Bad request')
                    for m in func(env):
                        yield m
                except HttpExitException:
                    pass
                except HttpInputException:
                    # HTTP 400 Bad Request
                    for m in env.error(400):
                        yield m
                except:
                    for m in env.error(500):
                        yield m
                for m in env.close():
                    yield m
            except:
                # Must ignore all exceptions, or the whole handler is unregistered
                pass
        if hasattr(func, '__self__'):
            handler.__self__ = func.__self__
        return handler
    return decorator


class Dispatcher(EventHandler):
    def __init__(self, scheduler=None, daemon=False, vhost = ''):
        EventHandler.__init__(self, scheduler=scheduler, daemon=daemon)
        self.vhost = vhost
    def routeevent(self, path, routinemethod, container = None, host = None, vhost = None, method = [b'GET', b'HEAD']):
        '''
        Route specified path to a routine factory
        @param path: path to match, can be a regular expression 
        @param routinemethod: factory function routinemethod(event), event is the HttpRequestEvent
        @param container: routine container. If None, default to self for bound method, or event.connection if not 
        @param host: if specified, only response to request to specified host
        @param vhost: if specified, only response to request to specified vhost.
                      If not specified, response to dispatcher default vhost.
        @param method: if specified, response to specified methods
        '''
        regm = re.compile(path + b'$')
        if vhost is None:
            vhost = self.vhost
        if container is None:
            container = getattr(routinemethod, '__self__', None)
        def ismatch(event):
            # Check vhost
            if vhost is not None and getattr(event.createby, 'vhost', '') != vhost:
                return False
            # First parse the path
            # RFC said we should accept absolute path
            psplit = urlsplit(event.path)
            if not psplit.path.startswith(b'/'):
                # For security reason, ignore unrecognized path
                return False
            if psplit.netloc and host is not None and host != psplit.netloc:
                # Maybe a proxy request, ignore it
                return False
            if getattr(event.createby, 'unquoteplus', True):
                realpath = unquote_plus_to_bytes(psplit.path)
            else:
                realpath = unquote_to_bytes(psplit.path)
            m = regm.match(realpath)
            if m is None:
                return False
            event.realpath = realpath
            event.querystring = psplit.query
            event.path_match = m
            return True
        def func(event, scheduler):
            try:
                if event.canignore:
                    # Already processed
                    return
                event.canignore = True
                c = event.connection if container is None else container
                c.subroutine(routinemethod(event), False)
            except:
                pass
        for m in method:
            self.registerHandler(HttpRequestEvent.createMatcher(host, None, m, _ismatch = ismatch), func)
    def route(self, path, routinemethod, container = None, host = None, vhost = None, method = [b'GET', b'HEAD']):
        '''
        Route specified path to a WSGI-styled routine factory
        @param path: path to match, can be a regular expression 
        @param routinemethod: factory function routinemethod(env), env is a Environment object
                see also utils.http.Environment
        @param container: routine container
        @param host: if specified, only response to request to specified host
        @param vhost: if specified, only response to request to specified vhost.
                      If not specified, response to dispatcher default vhost.
        @param method: if specified, response to specified methods
        '''
        self.routeevent(path, statichttp(container)(routinemethod), container, host, vhost, method)
    class _EncodedMatch(object):
        "Hacker for match.expand"
        def __init__(self, innerobj):
            self.__innerobj = innerobj
        def __getattr__(self, key):
            return getattr(self.__innerobj, key)
        def group(self, index = 0):
            return quote_from_bytes(self.__innerobj.group(index)).encode('ascii')
    def rewrite(self, path, expand, newmethod = None, host = None, vhost = None, method = [b'GET', b'HEAD'], keepquery = True):
        "Rewrite a request to another location"
        def func(env):
            # If use expand directly, the url-decoded context will be decoded again, which create a security
            # issue. Hack expand to quote the text before expanding
            newpath = re._expand(env.path_match.re, self._EncodedMatch(env.path_match), expand)
            if keepquery and getattr(env, 'querystring', None):
                if b'?' in newpath:
                    newpath += b'&' + env.querystring
                else:
                    newpath += b'?' + env.querystring
            for m in env.rewrite(newpath, newmethod):
                yield m
        self.route(path, func)
    def redirect(self, path, expand, status = 302, host = None, vhost = None, method = [b'GET', b'HEAD'], keepquery = True):
        "Redirect a request to another location"
        def func(env):
            newpath = re._expand(env.path_match.re, self._EncodedMatch(env.path_match), expand)
            if keepquery and getattr(env, 'querystring', None):
                if b'?' in newpath:
                    newpath += b'&' + env.querystring
                else:
                    newpath += b'?' + env.querystring
            for m in env.redirect(newpath, status):
                yield m
        self.route(path, func)

class HttpHandler(RoutineContainer):
    def __init__(self, scheduler=None, daemon=False, vhost = ''):
        RoutineContainer.__init__(self, scheduler=scheduler, daemon=daemon)
        self.dispatcher = Dispatcher(scheduler, daemon, vhost)
    @staticmethod
    def route(path, host = None, vhost = None, method = [b'GET', b'HEAD']):
        def decorator(func):
            func.routemode = 'route'
            func.route_path = path
            func.route_host = host
            func.route_vhost = vhost
            func.route_method = method
            return func
        return decorator
    @staticmethod
    def routeevent(path, host = None, vhost = None, method = [b'GET', b'HEAD']):
        def decorator(func):
            func.routemode = 'routeevent'
            func.route_path = path
            func.route_host = host
            func.route_vhost = vhost
            func.route_method = method
            return func
        return decorator
    def start(self, asyncStart=False):
        for key in dir(self):
            val = getattr(self, key)
            if hasattr(val, '__func__') and hasattr(val.__func__, 'routemode'):
                getattr(self.dispatcher, val.__func__.routemode)(val.__func__.route_path,
                                                               val,
                                                               self,
                                                               val.__func__.route_host,
                                                               val.__func__.route_vhost,
                                                               val.__func__.route_method)
        if hasattr(self, 'rewrites'):
            for rw in self.rewrites:
                if len(rw) > 2:
                    self.dispatcher.rewrite(rw[0], rw[1], **rw[2])
                else:
                    self.dispatcher.rewrite(rw[0], rw[1])
        if hasattr(self, 'redirects'):
            for rw in self.redirects:
                if len(rw) > 2:
                    self.dispatcher.redirect(rw[0], rw[1], **rw[2])
                else:
                    self.dispatcher.redirect(rw[0], rw[1])
    def close(self):
        self.dispatcher.close()

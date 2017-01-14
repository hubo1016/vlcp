'''
Created on 2015/11/18

:author: hubo
'''
from vlcp.config import defaultconfig
from vlcp.server.module import Module, api, depend
import vlcp.service.connection.httpserver
from vlcp.utils.http import Dispatcher, HttpInputException
from vlcp.event.stream import MemoryStream, FileStream
from time import time
import os.path
import sys
import logging
from vlcp.event.runnable import RoutineContainer
try:
    from urlparse import urlsplit, urljoin
except:
    from urllib.parse import urlsplit, urljoin
import re
import mimetypes
from email.utils import quote
import stat

withweight = re.compile(br'^([^\s;]+)\s*(?:;\s*[qQ]=((?:1(?:\.0{0,3})?)|(?:0(?:\.[0-9]{0,3})?)))?$')
rangeformat = re.compile(br'([0-9]*)-([0-9]*)')
statusname = re.compile(r'[1-5][0-9]{2}')

def _parseacceptencodings(env):
    elist = env.headerdict.get(b'accept-encoding')
    if elist is None:
        return []
    encoding_list = [e.strip() for e in elist.split(b',')]
    encodings = []
    for enc in encoding_list:
        m = withweight.match(enc)
        if not m:
            return ()
        encodings.append((-1.0 if m.group(2) is None else -float(m.group(2)), m.group(1)))
    allows = []
    identityq = 0
    for q, enc in sorted(encodings):
        if q >= 0:
            # 0 means not acceptable
            break
        enc = enc.lower()
        if enc in allows:
            continue
        if enc == b'identity':
            identityq = q
        elif q > identityq:
            # Ignore encoding with q lower than identity, because identity is always available
            break
        else:
            allows.append(enc)
    return allows
def _parseetag(etag):
    # Ignore whether it is a weak tag
    if etag[:2] == b'W/':
        etag = etag[2:]
    if etag[:1] == b'"' and etag[-1:] == b'"':
        return etag[1:-1]
    else:
        raise ValueError
def _checkrange(env, etag, size):
    if b'range' in env.headerdict:
        if b'if-range' in env.headerdict:
            try:
                if _parseetag(env.headerdict[b'if-range']) != etag:
                    return None
            except ValueError:
                return None
        rangeiden = env.headerdict[b'range']
        # Ignore unrecognized range identifier
        if rangeiden[:6] != b'bytes=':
            return None
        ranges = [r.strip() for r in rangeiden[6:].split(b',')]
        # Accept the first range
        r = ranges[0]
        m = rangeformat.match(r)
        if m is None:
            return None
        if not m.group(1):
            if not m.group(2):
                return None
            else:
                begin = size - int(m.group(2))
                end = size
        else:
            if not m.group(2):
                begin = int(m.group(1))
                end = size
            else:
                # 0-0 means 0:1
                begin = int(m.group(1))
                end = int(m.group(2)) + 1
        # Slice the range to match the correct size
        if begin < 0:
            begin = 0
        if begin > size:
            begin = size
        if end < begin:
            end = begin
        if end > size:
            end = size
        # Ignore invalid ranges
        if end <= begin:
            return None
        return (begin, end)
    else:
        return None

def topath(dirname, tail = None):
    if dirname[:1] != b'/':
        dirname = b'/' + dirname
    if tail:
        if dirname[-1:] != b'/':
            dirname = dirname + b'/'
        dirname = dirname + tail
    return dirname

def _checketag(env, etag):
    nonmatch = env.headerdict.get(b'if-none-match', None)
    if not nonmatch:
        return False
    if nonmatch == b'*':
        return True
    try:
        return etag in [_parseetag(e.strip()) for e in nonmatch.split(b',')]
    except ValueError:
        return False

def safehex(v):
    h = hex(v)[2:]
    if h.endswith('L'):
        h = h[:-1]
    return h

def _createetag(stat_info):
    etag = 'vlcp-' + safehex(stat_info.st_ino) + '-' + safehex(int(stat_info.st_mtime)) + '-' + safehex(int(stat_info.st_size))
    if not isinstance(etag, bytes):
        return etag.encode('ascii')
    return etag

def _generaterange(env, rng, size):
    env.header('Content-Range', 'bytes %d-%d/%d' % (rng[0], rng[1] - 1, size))

@defaultconfig
@depend(vlcp.service.connection.httpserver.HttpServer)
class Static(Module):
    "Map specified path to local files"
    # Not a service
    service = False
    # Check HTTP Referer header to protect against external site links.
    # The Referer header must present and match allowed sites
    _default_checkreferer = False
    # Grant access when Referer header match Host header
    _default_refererallowlocal = True
    # Set allowed referers
    _default_refererallows = []
    # Respond to Range header request
    _default_allowrange = True
    # Generate ETag for static resources, so the browser can use a If-Not-Match request
    # to save bandwidth
    _default_etag = True
    # If there is a "xxxx.gz" file in the folder, it would be recognized as a "xxxx" file
    # with "Content-Encoding: gzip". If gzip = False, it would be recognized as a normal file
    _default_gzip = True
    # Cache expiration time
    _default_maxage = 5
    # Enable a memory cache for small files to improve performance
    _default_memorycache = True
    # Maximum file size allowed to be cached in memory
    _default_memorycachelimit = 4096
    # Maximum cached file number. When it is exceeded, old caches will be cleared.
    _default_memorycacheitemlimit = 4096
    # The directory name for the static resource folder.
    # 
    # Static module supports multiple directories with different configurations, just
    # like vHosts. Use .vdir node to create separated configurations for different
    # folders, e.g.::
    #
    #     module.static.vdir.rewrite.dir="rewrite"
    #     module.static.vdir.rewrite.rewriteonly=True
    # 
    # You may also specify multiple directories with .dirs configuration instead, e.g.::
    #
    #     module.static.dir=None
    #     module.static.dirs=["js","css","images","download"]
    #
    # The directory name can be a relative path against a Python module file, or an absolute path.
    # 
    # .dir and .dirs maps a HTTP GET/HEAD request with path /*dir*/*subpath* to disk file
    # *relativeroot*/*dir*/*subpath*. The *subpath* may be a file in sub directories.
    # ".", ".." is also accepted as current folder/parent folder, but it always map to a path inside
    # *relativeroot*/*dir*, which means:
    # 
    #     /*dir*/*subpath*/a         =>     *relativeroot*/*dir*/a
    #     /*dir*/*subpath*/b/a       =>     *relativeroot*/*dir*/b/a
    #     /*dir*/*subpath*/b/../a    =>     *relativeroot*/*dir*/a
    #     /*dir*/*subpath*/b/../../a =>     *relativeroot*/*dir*/a
    #     
    # So it is not possible to use a static file map to access files outside the mapped folder.
    #
    # If you want to map a HTTP path to a directory with different names, use .map instead
    _default_dir = 'static'
    # Specify the mapped directory is relative to a Python module file. If .relativeroot is also
    # configured and is not empty, .relativeroot takes effect and .relativemodule is ignored.
    # If both are not configured, the current working directory is the relative root.
    # If you start your server with a customized Python script, the default "__main__" will fit.
    # If you start your server with python -m vlcp.start, you should configure the
    # main module manually.
    #
    # Notice that this may import the named Python module.
    _default_relativemodule = '__main__'
    # The relative root of the mapped directory, should be an absolute path if configured.
    _default_relativeroot = None
    # Bind this vdir to a specified HTTP vHost. Create multiple vdirs if you need to provide
    # static file services for multiple HTTP servers.
    _default_vhostbind = ''
    # Bind this vdir to a specified HTTP host, so only requests with corresponding "Host:" header
    # will be responded
    _default_hostbind = None
    # Customized MIME type configuration files, this should be a list (tuple) of file names.
    # Static module use *mimetypes* for guessing MIME information for static file.
    # See mimetypes (https://docs.python.org/2.7/library/mimetypes.html) for more details
    _default_mimetypes = ()
    # When guessing MIME types, use strict mode
    _default_mimestrict = True
    # Customized map for static files, it is an advanced replacement for .dir and .dirs,
    # but they can work at the same time.
    # 
    # The .map configuration should be a dictionary {*http-path*: *file-path*, ...}
    # where *file-path* may be:
    # 
    #     * a tuple (*directory*, *filename*), where *directory* is a directory name similar
    #       to names in .dir or .dirs, and *filename* is a filename or subpath. This maps
    #       *http-path* to *directory*/*filename*. *http-path* and *filename* may use
    #       regular expressions, you may use group capture (brackets in regular expressions)
    #       to capture values, and use them in *filename* with \1, \2, etc.
    # 
    #     * a directory name, in which case *http-path*=>*directory* is equal to
    #       *http-path*/(.\*) => (*directory*, "\1")
    _default_map = {}
    # Use a configured Content-Type, instead of guessing MIME types from file names
    _default_contenttype = None
    # This path cannot be directly accessed from HTTP requests; it only accept a request
    # which is rewritten to this path either by configuration or env.rewrite.
    _default_rewriteonly = False
    # Send extra HTTP headers
    _default_extraheaders = []
    # This directory contains customized error pages. The files should be HTML pages which
    # names start with status numbers, like:
    #
    #     400-badrequest.html
    #     403-forbidden.html
    #     404-notfound.html
    #     500-internalerror.html
    #
    # The status number in the file name will be used for the responding status code.
    # You may configure protocol.http.errorrewrite or protocol.http.errorredirect to
    # rewrite or redirect to error pages:
    # 
    #     protocol.http.errorrewrite={404:b'/error/404-notfound.html',
    #                                 400:b'/error/400-badrequest.html',
    #                                 403:b'/error/403-forbidden.html'}
    _default_errorpage = False
    # Use nginx "X-Accel-Redirect" header to handle the static file request. You must put this server
    # behind nginx proxy, and configure nginx correctly
    _default_xaccelredirect = False
    # Redirect to this path. A request to *dir*/*filename* will be redirected to *redirect_root*/*filename*
    _default_xaccelredirect_root = b'/static'
    # Use Apache X-Sendfile function to handle the static file request. You must put this server
    # behind Apache proxy and configure Apache correctly.
    _default_xsendfile = False
    # Use lighttpd X-LIGHTTPD-send-file to handle the static file request. Newer versions of lighttpd server
    # uses X-Sendfile, so you should set xsendfile=True instead. You must put this server behind lighttpd
    # and configure it correctly.
    _default_xlighttpdsendfile = False
    # Should be None, "attachment", or "inline". If None, "Content-Disposition" header is not used.
    # If set as "attachment", this will usually open a "save as" dialog in browser to let user download
    # the file. If set as "inline", it is processed as normal, but the real file name is sent by
    # the HTTP header, so when user chooses "save as" from browser to save the content,
    # the real file name is used
    _default_contentdisposition = None
    _logger = logging.getLogger(__name__ + '.Static')
    def _clearcache(self, currenttime):
        if self.memorycacheitemlimit <= 0:
            self._cache = {}
            return False
        if currenttime - self.lastcleartime < 1.0:
            # Do not clear too often
            return False
        while len(self._cache) >= self.memorycacheitemlimit:
            del self._cache[min(self._cache.items(), key=lambda x: x[1][2])[0]]
        self.lastcleartime = currenttime
        return True
    def _handlerConfig(self, expand, relativeroot, checkreferer, refererallowlocal, refererallows,
                                           allowrange, etag, gzip, maxage, memorycache,
                                           memorycachelimit, contenttype, rewriteonly, extraheaders,
                                           errorpage, contentdisposition, mimestrict, xaccelredirect,
                                           xsendfile, xlighttpdsendfile, xaccelredirect_root):
        def handler(env):
            currenttime = time()
            if rewriteonly:
                if not env.rewritefrom:
                    for m in env.error(404):
                        yield m
                    env.exit()
            if not errorpage and checkreferer:
                try:
                    referer = env.headerdict.get(b'referer')
                    if referer is None:
                        referer_host = None
                    else:
                        referer_host = urlsplit(referer).netloc
                    if not ((refererallowlocal and referer_host == env.host) or
                        referer_host in refererallows):
                        for m in env.error(403, showerror = False):
                            yield m
                        env.exit()
                except:
                    for m in env.error(403, showerror = False):
                        yield m
                    env.exit()
            localpath = env.path_match.expand(expand)
            realpath = env.getrealpath(relativeroot, localpath)
            filename = os.path.basename(realpath)
            if xsendfile or xlighttpdsendfile or xaccelredirect:
                # Apache send a local file
                env.startResponse(200)
                if contenttype:
                    env.header('Content-Type', contenttype)
                else:
                    mime = self.mimetypedatabase.guess_type(filename, mimestrict)
                    if mime[1]:
                        # There should not be a content-encoding here, maybe the file itself is compressed
                        # set mime to application/octet-stream
                        mime_type = 'application/octet-stream'
                    elif not mime[0]:
                        mime_type = 'application/octet-stream'
                    else:
                        mime_type = mime[0]
                    env.header('Content-Type', mime_type, False)
                if not errorpage and contentdisposition:
                    env.header('Content-Disposition', contentdisposition + '; filename=' + quote(filename))
                if xsendfile:
                    env.header('X-Sendfile', realpath)
                if xaccelredirect:
                    env.header(b'X-Accel-Redirect', urljoin(xaccelredirect_root, self.dispatcher.expand(env.path_match, expand)))
                if xlighttpdsendfile:
                    env.header(b'X-LIGHTTPD-send-file', realpath)
                env.exit()
                
            use_gzip = False
            if gzip:
                if realpath.endswith('.gz'):
                    # GZIP files are preserved for gzip encoding
                    for m in env.error(403, showerror = False):
                        yield m
                    env.exit()
                encodings = _parseacceptencodings(env)
                if b'gzip' in encodings or b'x-gzip' in encodings:
                    use_gzip = True
            use_etag = etag and not errorpage
            # First time cache check
            if memorycache:
                # Cache data: (data, headers, cachedtime, etag)
                cv = self._cache.get((realpath, use_gzip))
                if cv and cv[2] + max(0 if maxage is None else maxage, 3) > currenttime:
                    # Cache is valid
                    if use_etag:
                        if _checketag(env, cv[3]):
                            env.startResponse(304, cv[1])
                            env.exit()
                    size = len(cv[0])
                    rng = None
                    if not errorpage and allowrange:
                        rng = _checkrange(env, cv[3], size)
                    if rng is not None:
                        env.startResponse(206, cv[1])
                        _generaterange(env, rng, size)
                        env.output(MemoryStream(cv[0][rng[0]:rng[1]]), use_gzip)
                    else:
                        if errorpage:
                            m = statusname.match(filename)
                            if m:
                                env.startResponse(int(m.group()), cv[1])
                            else:
                                # Show 200-OK is better than 500
                                env.startResponse(200, cv[1])
                        else:
                            env.startResponse(200, cv[1])
                        env.output(MemoryStream(cv[0]), use_gzip)
                    env.exit()
            # Test file
            if use_gzip:
                try:
                    stat_info = os.stat(realpath + '.gz')
                    if not stat.S_ISREG(stat_info.st_mode):
                        raise ValueError('Not regular file')
                    realpath += '.gz'
                except:
                    try:
                        stat_info = os.stat(realpath)
                        if not stat.S_ISREG(stat_info.st_mode):
                            raise ValueError('Not regular file')
                        use_gzip = False
                    except:
                        for m in env.error(404, showerror = False):
                            yield m
                        env.exit()
            else:
                try:
                    stat_info = os.stat(realpath)
                    if not stat.S_ISREG(stat_info.st_mode):
                        raise ValueError('Not regular file')
                    use_gzip = False
                except:
                    for m in env.error(404, showerror = False):
                        yield m
                    env.exit()
            newetag = _createetag(stat_info)
            # Second memory cache test
            if memorycache:
                # use_gzip may change
                cv = self._cache.get((realpath, use_gzip))
                if cv and cv[3] == newetag:
                    # Cache is valid
                    if use_etag:
                        if _checketag(env, cv[3]):
                            env.startResponse(304, cv[1])
                            env.exit()
                    self._cache[(realpath, use_gzip)] = (cv[0], cv[1], currenttime, newetag)
                    size = len(cv[0])
                    rng = None
                    if not errorpage and allowrange:
                        rng = _checkrange(env, cv[3], size)
                    if rng is not None:
                        env.startResponse(206, cv[1])
                        _generaterange(env, rng, size)
                        env.output(MemoryStream(cv[0][rng[0]:rng[1]]), use_gzip)
                    else:
                        if errorpage:
                            m = statusname.match(filename)
                            if m:
                                env.startResponse(int(m.group()), cv[1])
                            else:
                                # Show 200-OK is better than 500
                                env.startResponse(200, cv[1])
                        else:
                            env.startResponse(200, cv[1])
                        env.output(MemoryStream(cv[0]), use_gzip)
                    env.exit()
                elif cv:
                    # Cache is invalid, remove it to prevent another hit
                    del self._cache[(realpath, use_gzip)]
            # No cache available, get local file
            # Create headers
            if contenttype:
                env.header('Content-Type', contenttype)
            else:
                mime = self.mimetypedatabase.guess_type(filename, mimestrict)
                if mime[1]:
                    # There should not be a content-encoding here, maybe the file itself is compressed
                    # set mime to application/octet-stream
                    mime_type = 'application/octet-stream'
                elif not mime[0]:
                    mime_type = 'application/octet-stream'
                else:
                    mime_type = mime[0]
                env.header('Content-Type', mime_type, False)
            if use_etag:
                env.header(b'ETag', b'"' + newetag + b'"', False)
            if maxage is not None:
                env.header('Cache-Control', 'max-age=' + str(maxage), False)
            if use_gzip:
                env.header(b'Content-Encoding', b'gzip', False)
            if not errorpage and contentdisposition:
                env.header('Content-Disposition', contentdisposition + '; filename=' + quote(filename))
            if allowrange:
                env.header(b'Accept-Ranges', b'bytes')
            if extraheaders:
                env.sent_headers.extend(extraheaders)
            if use_etag:
                if _checketag(env, newetag):
                    env.startResponse(304, clearheaders = False)
                    env.exit()
            if memorycache and stat_info.st_size <= memorycachelimit:
                # Cache
                cache = True
                if len(self._cache) >= self.memorycacheitemlimit:
                    if not self._clearcache(currenttime):
                        cache = False
                if cache:
                    with open(realpath, 'rb') as fobj:
                        data = fobj.read()
                    self._cache[(realpath, use_gzip)] = (data, env.sent_headers[:], currenttime, newetag)
                    size = len(data)
                    rng = None
                    if not errorpage and allowrange:
                        rng = _checkrange(env, newetag, size)
                    if rng is not None:
                        env.startResponse(206, clearheaders = False)
                        _generaterange(env, rng, size)
                        env.output(MemoryStream(data[rng[0]:rng[1]]), use_gzip)
                    else:
                        if errorpage:
                            m = statusname.match(filename)
                            if m:
                                env.startResponse(int(m.group()), clearheaders = False)
                            else:
                                # Show 200-OK is better than 500
                                env.startResponse(200, clearheaders = False)
                        else:
                            env.startResponse(200, clearheaders = False)
                        env.output(MemoryStream(data), use_gzip)
                    env.exit()
            size = stat_info.st_size
            if not errorpage and allowrange:
                rng = _checkrange(env, newetag, size)
            if rng is not None:
                env.startResponse(206, clearheaders = False)
                _generaterange(env, rng, size)
                fobj = open(realpath, 'rb')
                try:
                    fobj.seek(rng[0])
                except:
                    fobj.close()
                    raise
                else:
                    env.output(FileStream(fobj, isunicode=False, size=rng[1] - rng[0]), use_gzip)
            else:
                if errorpage:
                    m = statusname.match(filename)
                    if m:
                        env.startResponse(int(m.group()), clearheaders = False)
                    else:
                        # Show 200-OK is better than 500
                        env.startResponse(200, clearheaders = False)
                else:
                    env.startResponse(200, clearheaders = False)
                env.output(FileStream(open(realpath, 'rb'), isunicode = False), use_gzip)
        return handler
    _configurations = ['checkreferer', 'refererallowlocal', 'refererallows',
            'allowrange', 'etag', 'gzip', 'maxage', 'memorycache',
            'memorycachelimit', 'contenttype', 'rewriteonly', 'extraheaders',
            'errorpage', 'contentdisposition', 'mimestrict', 'xaccelredirect',
            'xsendfile', 'xlighttpdsendfile', 'xaccelredirect_root']
    def _createHandlers(self, config, defaultconfig = {}):
        dirs = list(getattr(config, 'dirs', []))
        if hasattr(config, 'dir') and config.dir and config.dir.strip():
            dirs.append(config.dir)
        # Change to str
        dirs = [d.decode('utf-8') if not isinstance(d, str) else d for d in dirs]
        maps = dict((topath(d.encode('utf-8'), b'(.*)'), (d, br'\1')) for d in dirs)
        if hasattr(config, 'map') and config.map:
            for k,v in config.map.items():
                if not isinstance(k, bytes):
                    k = k.encode('utf-8')
                if isinstance(v, str) or isinstance(v, bytes):
                    if not isinstance(v, str):
                        v = v.decode('utf-8')
                    # (b'/abc' => 'def') is equal to (b'/abc/(.*)' => ('def', br'\1')
                    maps[topath(k, b'(.*)')] = (v, br'\1')
                else:
                    # Raw map
                    # (b'/' => (b'static', b'index.html'))
                    d = v[0]
                    if not isinstance(d, str):
                        d = d.decode('utf-8')
                    expand = v[1]
                    if not isinstance(d, bytes):
                        expand = expand.encode('utf-8')
                    maps[topath(k)] = (d, expand)
        getconfig = lambda k: getattr(config, k) if hasattr(config, k) else defaultconfig.get(k)
        hostbind = getconfig('hostbind')
        vhostbind = getconfig('vhostbind')
        relativeroot = getconfig('relativeroot')
        relativemodule = getconfig('relativemodule')
        if maps:
            # Create configuration
            newconfig = dict((k,getconfig(k)) for k in self._configurations)
            if not relativeroot:
                if relativemodule:
                    try:
                        __import__(relativemodule)
                        mod = sys.modules[relativemodule]
                        filepath = getattr(mod, '__file__', None)
                        if filepath:
                            relativeroot = os.path.dirname(filepath)
                        else:
                            self._logger.warning('Relative module %r has no __file__, use cwd %r instead',
                                                 relativemodule,
                                                 os.getcwd())
                            relativeroot = os.getcwd()
                    except:
                        self._logger.exception('Cannot locate relative module %r', relativemodule)
                        raise
                else:
                    relativeroot = os.getcwd()
            relativeroot = os.path.abspath(relativeroot)
            for k,v in maps.items():
                drel = os.path.normpath(os.path.join(relativeroot, v[0]))
                if not os.path.isdir(drel):
                    self._logger.error('Cannot find directory: %r', drel)
                    continue
                # For security reason, do not allow a package directory to be exported
                if os.path.isfile(os.path.join(drel, '__init__.py')):
                    self._logger.error('Path %r is a package', drel)
                    continue
                self.dispatcher.route(k, self._handlerConfig(v[1], drel, **newconfig), self.apiroutine,
                                      hostbind, vhostbind)
        if hasattr(config, 'vdir'):
            newconfig.update((('hostbind', hostbind), ('vhostbind', vhostbind),
                              ('relativeroot', getconfig('relativeroot')),('relativemodule', relativemodule)))
            for k,v in config.vdir.items():
                self._createHandlers(v, newconfig)
    def __init__(self, server):
        Module.__init__(self, server)
        self.dispatcher = Dispatcher(self.scheduler)
        self.mimetypedatabase = mimetypes.MimeTypes(self.mimetypes)
        self._cache = {}
        self.apiroutine = RoutineContainer(self.scheduler)
        self.lastcleartime = 0
        def start(asyncStart = False):
            self._createHandlers(self)
        def close():
            self.dispatcher.close()
        self.apiroutine.start = start
        self.apiroutine.close = close
        self.routines.append(self.apiroutine)
        self.createAPI(api(self.updateconfig))
    def updateconfig(self):
        "Reload configurations, remove non-exist servers, add new servers, and leave others unchanged"
        self.dispatcher.unregisterAllHandlers()
        self._createHandlers(self)
        return None

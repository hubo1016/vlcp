'''
Created on 2015/11/18

@author: hubo
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
    if etag.startswith(b'W/'):
        etag = etag[2:]
    if etag.startswith(b'"') and etag.endswith(b'"'):
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
        if not rangeiden.startswith(b'bytes='):
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
    if not dirname.startswith(b'/'):
        dirname = b'/' + dirname
    if tail:
        if not dirname.endswith(b'/'):
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
    _default_checkreferer = False
    _default_refererallowlocal = True
    _default_refererallows = []
    _default_allowrange = True
    _default_etag = True
    _default_gzip = True
    _default_maxage = 5
    _default_memorycache = True
    _default_memorycachelimit = 4096
    _default_memorycacheitemlimit = 4096
    _default_dir = 'static'
    _default_relativemodule = '__main__'
    _default_vhostbind = ''
    _default_hostbind = None
    _default_mimetypes = ()
    _default_mimestrict = True
    _default_map = {}
    _default_contenttype = None
    _default_rewriteonly = False
    _default_extraheaders = []
    _default_errorpage = False
    _default_xaccelredirect = False
    _default_xaccelredirect_root = b'/static'
    _default_xsendfile = False
    _default_xlighttpdsendfile = False
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
                    referer_s = urlsplit(env.headerdict[b'referer'])
                    if not ((refererallowlocal and referer_s.netloc == env.host) or
                        referer_s.netloc in refererallows):
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
                                env.startResponse(int(m.group(1)), cv[1])
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
                                env.startResponse(int(m.group(1)), cv[1])
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
                                env.startResponse(int(m.group(1)), clearheaders = False)
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
                        env.startResponse(int(m.group(1)), clearheaders = False)
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
        getconfig = lambda k: (k,getattr(config, k)) if hasattr(config, k) else defaultconfig.get(k)
        hostbind = getconfig('hostbind')
        vhostbind = getconfig('vhostbind')
        if maps:
            # Create configuration
            newconfig = dict(getconfig(k) for k in self._configurations)
            if hasattr(config, 'relativeroot'):
                relativeroot = config.relativeroot
            elif hasattr(config, 'relativemodule'):
                try:
                    __import__(config.relativemodule)
                    mod = sys.modules[config.relativemodule]
                    filepath = getattr(mod, '__file__', None)
                    if filepath:
                        relativeroot = os.path.dirname(filepath)
                    else:
                        self._logger.warning('Relative module %r has no __file__, use cwd %r instead',
                                             config.relativemodule,
                                             os.getcwd())
                        relativeroot = os.getcwd()
                except:
                    self._logger.exception('Cannot locate relative module %r', config.relativemodule)
                    raise
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
                self.dispatcher.route(k, self._handlerConfig(v[1], drel, **newconfig), self.apiroutine)
        if hasattr(config, 'vdir'):
            newconfig.update(('hostbind', hostbind), ('vhostbind', vhostbind))
            for k,v in config.vdir.items():
                self._createServers(self, v, newconfig)
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

'''
Created on 2015/12/2

:author: hubo
'''

from vlcp.config import defaultconfig
from vlcp.server.module import Module, api, depend, callAPI
from vlcp.event.core import TimerEvent
from vlcp.event.runnable import RoutineContainer
import vlcp.service.connection.httpserver
from time import time
import os.path
from vlcp.utils.http import HttpHandler
from email.message import Message
import json
import ast
from namedstruct import NamedStruct, dump

def _str(b, encoding = 'ascii'):
    if isinstance(b, str):
        return b
    else:
        return b.decode(encoding)

class WebAPIHandler(HttpHandler):
    def __init__(self, parent):
        HttpHandler.__init__(self, scheduler=parent.scheduler, daemon=False, vhost=parent.vhostbind)
        self.parent = parent
    def apiHandler(self, env, targetname, methodname, **kwargs):
        params = kwargs
        parent = self.parent
        if not params:
            if b'content-type' in env.headerdict and env.inputstream is not None and parent.acceptjson:
                m = Message()
                m['content-type'] = _str(env.headerdict[b'content-type'])
                if m.get_content_type() == 'application/json':
                    charset = m.get_content_charset('utf-8')
                    for m in env.inputstream.read(self):
                        yield m
                    params = json.loads(self.data, charset)
        elif parent.typeextension:
            for k in params.keys():
                v = params[k]
                if v[:1] == '`' and v[-1:] == '`':
                    try:
                        params[k] = ast.literal_eval(v[1:-1])
                    except:
                        pass
        if parent.allowtargets is not None:
            if targetname not in parent.allowtargets:
                for m in env.error(403):
                    yield m
                raise StopIteration
        elif parent.denytargets is not None:
            if targetname in parent.denytargets:
                for m in env.error(403):
                    yield m
                raise StopIteration
        if parent.authmethod:
            for m in callAPI(self, parent.authtarget, parent.authmethod,
                             {'env':env, 'targetname':targetname, 'name':methodname, 'params': params}):
                yield m
        for m in callAPI(self, targetname, methodname, params):
            yield m
        env.header('Content-Type', 'application/json')
        env.outputdata(json.dumps({'result':self.retvalue}, default=parent.jsonencoder).encode('ascii'))
    def start(self, asyncStart=False):
        HttpHandler.start(self, asyncStart=asyncStart)
        path = self.parent.rootpath.encode('utf-8')
        if path[-1:] != b'/':
            path += b'/'
        path += b'(?P<targetname>[^/]*)/(?P<methodname>[^/]*)'
        self.dispatcher.routeargs(path, self.apiHandler, self, self.parent.hostbind,
                                  self.parent.vhostbind, self.parent.acceptmethods,
                                  matchargs = ('targetname', 'methodname'), csrfcheck = False)
@depend(vlcp.service.connection.httpserver.HttpServer)
@defaultconfig
class WebAPI(Module):
    '''
    Call module API from web. Free access to any module APIs may create serious security problems,
    make sure to configure this module properly.
    '''
    _default_vhostbind = 'api'
    _default_hostbind = None
    _default_rootpath = '/'
    _default_acceptmethods = [b'GET', b'POST']
    _default_acceptjson = True
    _default_authtarget = 'public'
    _default_authmethod = None
    _default_allowtargets = None
    _default_denytargets = None
    _default_namedstruct = True
    _default_humanread = True
    _default_bytesdecode = 'ascii'
    _default_byteslimit = 256
    _default_dumpextra = False
    _default_dumptypeinfo = 'flat'
    _default_typeextension = True
    service = False
    def __init__(self, server):
        '''
        Constructor
        '''
        Module.__init__(self, server)
        self.routines.append(WebAPIHandler(self))
    def jsonencoder(self, obj):
        if isinstance(obj, NamedStruct) and self.namedstruct:
            return dump(obj, self.humanread, self.dumpextra, self.dumptypeinfo)
        elif isinstance(obj, bytes):
            if self.humanread and len(obj) > self.byteslimit:
                return '<%d bytes...>' % (len(obj),)
            else:
                if self.bytesdecode:
                    try:
                        return obj.decode(self.bytesdecode)
                    except:
                        return repr(obj)
                else:
                    return repr(obj)
        else:
            return repr(obj)

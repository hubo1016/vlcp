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
from vlcp.utils.jsonencoder import encode_default, decode_object, JsonFormat
import traceback

def _str(b, encoding = 'ascii'):
    if isinstance(b, str):
        return b
    else:
        return b.decode(encoding)

class WebAPIHandler(HttpHandler):
    def __init__(self, parent):
        HttpHandler.__init__(self, scheduler=parent.scheduler, daemon=False, vhost=parent.vhostbind)
        self.parent = parent
        self.jsonencoder = JsonFormat()
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
                    params = json.loads(_str(self.data, charset), encoding=charset, object_hook=decode_object)
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
                return
        elif parent.denytargets is not None:
            if targetname in parent.denytargets:
                for m in env.error(403):
                    yield m
                return
        if parent.authmethod:
            for m in callAPI(self, parent.authtarget, parent.authmethod,
                             {'env':env, 'targetname':targetname, 'name':methodname, 'params': params}):
                yield m
        try:
            for m in callAPI(self, targetname, methodname, params):
                yield m
        except Exception as exc:
            if parent.errordetails:
                parent._logger.warning('Web API call failed for %r/%r', targetname, methodname, exc_info = True)
                env.startResponse(500, [(b'Content-Type', b'application/json')])
                err = {'error': str(exc)}
                if parent.errortrace:
                    err['trace'] = traceback.format_exc()
                env.outputjson(err)
            else:
                raise
        else:
            env.header('Content-Type', 'application/json')
            env.outputdata(json.dumps({'result':self.retvalue}, default=self.jsonencoder.jsonencoder).encode('ascii'))
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
    # Default bind HttpServer vHost to 'api', so you should use::
    # 
    #     module.httpserver.vhost.api.url='http://localhost:8080/'
    # 
    # to create the API endpoint
    _default_vhostbind = 'api'
    # Bind to a specified Host (HTTP "Host: " header)
    _default_hostbind = None
    # Bind API endpoint under this path. Each endpoint would be \<rootpath\>/\<targetname\>/\<methodname\>.
    # e.g. Manager.reloadmodules would be::
    # 
    #     http://<serverurl>/<rootpath>/manager/reloadmodules
    # 
    # targetname and methodnames are always in lower case
    _default_rootpath = '/'
    # Allowed HTTP method, GET/POST or both
    _default_acceptmethods = [b'GET', b'POST']
    # Allow to use JSON format for POST data
    _default_acceptjson = True
    # If authenticate is enabled, WebAPI module uses an extra API call to authenticate the request.
    # This is the target name.
    _default_authtarget = 'public'
    # If authenticate is enabled, WebAPI module uses an extra API call to authenticate the request.
    # This is the method name. The params will be {'env': *env*, 'targetname':*targetname*,
    # 'name': *methodname*, 'params': *parameters*}
    _default_authmethod = None
    # Only allow API calls on specified targets (usually a target name is the lower-cased module name)
    _default_allowtargets = None
    # Disallow API calls on specified targets
    _default_denytargets = None
    # Enable a type extension for GET/POST methods: use `` quoted string to represent a Python literal
    # expression like lists, tuples, dictionaries, numbers etc.
    _default_typeextension = True
    # Show error details in error responses with JSON format {"error":...}. If errordetails = False,
    # the default HTTP error response is used.
    _default_errordetails = True
    # Also output the trace information in the JSON output
    _default_errortrace = False
    service = False
    def __init__(self, server):
        '''
        Constructor
        '''
        Module.__init__(self, server)
        self.routines.append(WebAPIHandler(self))
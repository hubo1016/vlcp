'''
Created on 2015/11/9

:author: hubo
'''
from vlcp.config import defaultconfig
from vlcp.server.module import Module, api, depend, callAPI
from vlcp.event.runnable import RoutineContainer
from vlcp.event import Event, withIndices
from vlcp.service.utils import knowledge
from uuid import uuid4
try:
    from Cookie import SimpleCookie, Morsel
except:
    from http.cookies import SimpleCookie, Morsel
from vlcp.event.lock import Lock

@depend(knowledge.MemoryStorage)
@defaultconfig
class Session(Module):
    '''
    HTTP Session with cookies
    '''
    # Timeout for a session
    _default_timeout = 1800
    # Cookie name used in session id storage
    _default_cookiename = '_session_id'
    service = True
    class SessionObject(object):
        def __init__(self, sessionid):
            self.id = sessionid
            self.vars = {}
            self.lockseq = 0
            self.releaseseq = 0
    class SessionHandle(object):
        def __init__(self, sessionobj, container):
            self.sessionobj = sessionobj
            self.id = sessionobj.id
            self.vars = sessionobj.vars
            self._lock = Lock(sessionobj, container.scheduler)
            self.container = container
        def lock(self):
            "Lock session"
            for m in self._lock.lock(self.container):
                yield m
        def unlock(self):
            "Unlock session"
            self._lock.unlock()
    def __init__(self, server):
        '''
        Constructor
        '''
        Module.__init__(self, server)
        self.apiroutine = RoutineContainer(self.scheduler)
        self.createAPI(api(self.start, self.apiroutine),
                       api(self.create, self.apiroutine),
                       api(self.get, self.apiroutine),
                       api(self.destroy, self.apiroutine))
    def start(self, cookies, cookieopts = None):
        """
        Session start operation. First check among the cookies to find existed sessions;
        if there is not an existed session, create a new one.
        
        :param cookies: cookie header from the client
        
        :param cookieopts: extra options used when creating a new cookie
        
        :return: ``(session_handle, cookies)`` where session_handle is a SessionHandle object,
                 and cookies is a list of created Set-Cookie headers (may be empty)
        """
        c = SimpleCookie(cookies)
        sid = c.get(self.cookiename)
        create = True
        if sid is not None:
            for m in self.get(sid.value):
                yield m
            if self.apiroutine.retvalue is not None:
                self.apiroutine.retvalue = (self.SessionHandle(self.apiroutine.retvalue, self.apiroutine), [])
                create = False
        if create:
            for m in self.create():
                yield m
            sh = self.apiroutine.retvalue
            m = Morsel()
            m.key = self.cookiename
            m.value = sh.id
            m.coded_value = sh.id
            opts = {'path':'/', 'httponly':True}
            if cookieopts:
                opts.update(cookieopts)
                if not cookieopts['httponly']:
                    del cookieopts['httponly']
            m.update(opts)
            self.apiroutine.retvalue = (sh, [m])
    def get(self, sessionid, refresh = True):
        """
        Get the seesion object of the session id
        
        :param sessionid: a session ID
        
        :param refresh: if True, refresh the expiration time of this session
        
        :return: Session object or None if not exists
        """
        for m in callAPI(self.apiroutine, 'memorystorage', 'get', {'key': __name__ + '.' + sessionid, 'timeout': self.timeout if refresh else None}):
            yield m
    def create(self):
        """
        Create a new session object
        
        :return: Session handle for the created session object.
        """
        sid = uuid4().hex
        sobj = self.SessionObject(sid)
        for m in callAPI(self.apiroutine, 'memorystorage', 'set', {'key': __name__ + '.' + sid, 'value': sobj, 'timeout': self.timeout}):
            yield m
        self.apiroutine.retvalue = self.SessionHandle(sobj, self.apiroutine)
    def destroy(self, sessionid):
        """
        Destroy a session
        
        :param sessionid: session ID
        
        :return: a list of Set-Cookie headers to be sent to the client
        """
        for m in callAPI(self.apiroutine, 'memorystorage', 'delete', {'key': __name__ + '.' + sessionid}):
            yield m
        m = Morsel()
        m.key = self.cookiename
        m.value = 'deleted'
        m.coded_value = 'deleted'
        opts = {'path':'/', 'httponly':True, 'max-age':0}
        m.update(opts)
        self.apiroutine.retvalue = [m]
    
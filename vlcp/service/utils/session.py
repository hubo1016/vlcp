'''
Created on 2015/11/9

@author: hubo
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


@withIndices('id', 'seq')
class SessionLockReleased(Event):
    pass

@depend(knowledge.Knowledge)
@defaultconfig
class Session(Module):
    '''
    HTTP Session with cookies
    '''
    _default_timeout = 1800
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
            self.lockseq = None
            self.container = container
        def lock(self):
            "Lock session"
            self.lockseq = self.sessionobj.lockseq
            self.locked = False
            self.sessionobj.lockseq += 1
            if self.lockseq > self.sessionobj.releaseseq:
                # Wait for unlock
                yield (SessionLockReleased.createMatcher(self.id, self.lockseq),)
                self.locked = True
        def unlock(self):
            "Unlock session"
            if self.lockseq is not None:
                if not self.locked:
                    # Unlock on future
                    def future_unlock(c, sid, lockseq):
                        yield (SessionLockReleased.createMatcher(sid, lockseq),)
                        for m in c.waitForSend(SessionLockReleased(sid, lockseq + 1)):
                            yield m
                    
                for m in self.container.waitForSend(SessionLockReleased(self.id, self.lockseq + 1)):
                    yield m
                self.sessionobj.releaseseq = self.lockseq + 1
                self.lockseq = None
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
        for m in callAPI(self.apiroutine, 'knowledge', 'get', {'key': __name__ + '.' + sessionid, 'refresh': refresh}):
            yield m
    def create(self):
        sid = uuid4().hex
        sobj = self.SessionObject(sid)
        for m in callAPI(self.apiroutine, 'knowledge', 'set', {'key': __name__ + '.' + sid, 'value': sobj, 'timeout': self.timeout}):
            yield m
        self.apiroutine.retvalue = self.SessionHandle(sobj, self.apiroutine)
    def destroy(self, sessionid):
        for m in callAPI(self.apiroutine, 'knowledge', 'delete', {'key': __name__ + '.' + sessionid}):
            yield m
        m = Morsel()
        m.key = self.cookiename
        m.value = 'deleted'
        m.coded_value = 'deleted'
        opts = {'path':'/', 'httponly':True, 'max-age':0}
        m.update(opts)
        self.apiroutine.retvalue = [m]
    
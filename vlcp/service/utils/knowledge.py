'''
Created on 2015/11/9

@author: hubo
'''
from vlcp.config import defaultconfig
from vlcp.server.module import Module, api
from vlcp.event.core import TimerEvent
from vlcp.event.runnable import RoutineContainer
from time import time
import functools

@defaultconfig
class Knowledge(Module):
    '''
    Simple KV-cache in memory. A base for other KV-DB.
    Data is automatically removed after timeout.
    Use knowledge instead of local storage in modules so data is not lost on module restarting.
    '''
    _default_checkinterval = 300
    service = True
    def __init__(self, server):
        '''
        Constructor
        '''
        Module.__init__(self, server)
        self.timeoutroutine = RoutineContainer(self.scheduler)
        self.timeoutroutine.main = self._timeout
        self.db = {}
        self.routines.append(self.timeoutroutine)
        self.createAPI(api(self.get),
                       api(self.set),
                       api(self.mget),
                       api(self.mset),
                       api(self.update),
                       api(self.mupdate))
    def _timeout(self):
        th = self.scheduler.setTimer(self.checkinterval, self.checkinterval)
        try:
            tm = TimerEvent.createMatcher(th)
            while True:
                yield (tm,)
                t = time()
                timeouts = [k for k,v in self.db.items() if v[2] is not None and v[1] + v[2] < t]
                for k in timeouts:
                    del self.db[k]
        finally:
            self.scheduler.cancelTimer(th)
    def get(self, key, refresh = False):
        "Get value from key"
        v = self.db.get(key)
        if v is None:
            return None
        if refresh:
            self.db[key] = (v[0], time(), v[2])
        return v[0]
    def set(self, key, value, timeout = None):
        "Set value to key, with an optional timeout"
        self.db[key] = (value, time(), timeout)
        return None
    def delete(self, key):
        try:
            del self.db[key]
        except KeyError:
            pass
        return None
    def mget(self, keys):
        "Get multiple values from multiple keys"
        return [self.db.get(k)[0] for k in keys]
    def mset(self, kvpairs, timeout = None):
        "Set multiple values on multiple keys"
        d = kvpairs
        if hasattr(d, 'items'):
            d = d.items()
        t = time()
        self.db.update(((k, (v, t, timeout)) for k,v in d))
        return None
    def update(self, key, updater, timeout = None):
        '''
        Update in-place with a custom function
        @param key: key to update
        @param updater: func(k,v), should return a new value to update, or return None to delete
        @param timeout: new timeout
        '''
        v = self.db.get(key)
        if v is not None:
            v = v[0]
        newv = updater(key, v)
        if newv:
            self.db[key] = (newv, time(), timeout)
        else:
            try:
                del self.db[key]
            except KeyError:
                pass
        return newv
    def mupdate(self, keys, updater, timeout = None):
        "Update multiple keys in-place with a custom function, see update"
        t = time()
        for k in keys:
            v = self.db.get(k)
            if v is not None:
                v = v[0]
            newv = updater(k, v)
            if newv:
                self.db[k] = (newv, time(), timeout)
            else:
                try:
                    del self.db[k]
                except KeyError:
                    pass

def return_self_updater(func):
    '''Run func, but still return v. Useful for using knowledge.update with operates like append, extend, etc.
    e.g. return_self(lambda k,v: v.append('newobj'))
    '''
    @functools.wraps(func)
    def decorator(k,v):
        func(k,v)
        return v
    return decorator

def escape_key(k):
    "Escape k, ensuring there is not a '.' in the string."
    return k.replace('+','+_').replace('.','++')

def unescape_key(k):
    "Unescape key to get '.' back"
    return k.replace('++', '.').replace('+_','+')

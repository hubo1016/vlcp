'''
Created on 2016/3/25

:author: think
'''

from vlcp.service.utils.knowledge import escape_key, unescape_key
from vlcp.event.event import withIndices, Event
from contextlib import contextmanager
from vlcp.server.module import callAPI
import functools
from copy import deepcopy
from vlcp.event.core import QuitException
import itertools
try:
    from itertools import izip
except ImportError:
    def izip(*args, **kwargs):
        return iter(zip(*args, **kwargs))

@withIndices('key', 'transactid', 'type')
class DataObjectUpdateEvent(Event):
    UPDATED = 'updated'
    DELETED = 'deleted'

class ReferenceObject(object):
    "A strong reference. The referenced object should be automatically retrieved from KVDB."
    def __init__(self, key, refobj = None):
        self._key = key
        self._ref = refobj
    def getkey(self):
        return self._key
    def kvdb_retrievelist(self):
        if self._ref is None:
            return [self.getkey()]
        else:
            return []
    def kvdb_retrievefinished(self, result):
        self._ref = result.get(self._key)
    def kvdb_internalref(self):
        return set((self._key,))
    def kvdb_detach(self):
        self._ref = None
    def isdeleted(self):
        return self._ref is None or self._ref._deleted
    def __getattr__(self, key):
        if key[:1] == '_' and not key.startswith('kvdb_'):
            raise AttributeError("type object %r has no attribute %r" % (type(self).__name__, key))
        if self._ref is None:
            raise AttributeError("Target object is not connected")
        return getattr(self._ref, key)
    def jsonencode(self):
        return self._key
    @classmethod
    def jsondecode(cls, data):
        return cls(data, None)
    def __getstate__(self):
        return self._key
    def __setstate__(self, state):
        self._key = state
        self._ref = None
    def __hash__(self, *args, **kwargs):
        return hash(self._key) ^ hash('ReferenceObject')
    def __eq__(self, obj):
        return isinstance(obj, ReferenceObject) and (obj is self or obj._key == self._key)
    def __ne__(self, obj):
        return not self.__eq__(obj)
    def __repr__(self, *args, **kwargs):
        return '<ReferenceObject: %r at %r>' % (self._ref, self._key)
    def wait(self, container):
        if self.isdeleted():
            yield (DataObjectUpdateEvent.createMatcher(self.getkey(), None, DataObjectUpdateEvent.UPDATED),)
            self._ref = container.event.object
    def waitif(self, container, expr, nextchange = False):
        flag = nextchange
        while True:
            r = not flag and expr(self)
            flag = False
            if r:
                container.retvalue = r
                break
            yield (DataObjectUpdateEvent.createMatcher(self.getkey()),)
            if self._ref is not container.event.object:
                self._ref = container.event.object
    
class WeakReferenceObject(object):
    "A weak reference. The referenced object must be retrieved manually."
    def __init__(self, key):
        self._key = key
    def getkey(self):
        return self._key
    def jsonencode(self):
        return self._key
    @classmethod
    def jsondecode(cls, data):
        return cls(data)
    def __getstate__(self):
        return self._key
    def __setstate__(self, state):
        self._key = state
    def __hash__(self, *args, **kwargs):
        return hash(self._key) ^ hash('WeakReferenceObject')
    def __eq__(self, obj):
        return isinstance(obj, WeakReferenceObject) and (obj is self or obj._key == self._key)
    def __ne__(self, obj):
        return not self.__eq__(obj)
    def __repr__(self, *args, **kwargs):
        return '<WeakReferenceObject: %r>' % (self._key,)

class DataObject(object):
    "A base class to serialize data into KVDB"
    _prefix = ''
    _indices = ()
    def __init__(self, prefix = None, deleted = False):
        if prefix is not None:
            self._prefix = prefix
        self._deleted = deleted
    def getkey(self):
        if not self._indices:
            return self._prefix
        else:
            return self._prefix + '.' + '.'.join(escape_key(str(getattr(self, ind))) for ind in self._indices)
    @classmethod
    def _getIndices(cls, key):
        if not cls._indices:
            return (key, [])
        keys = key.split('.')
        indices = cls._indices
        if len(keys) < len(indices):
            raise ValueError('Malformed key: indices %r not found' % (indices,))
        else:
            prefix = '.'.join(keys[:-len(indices)])
        return (prefix, [unescape_key(k) for k in keys[-len(indices):]])
    @classmethod
    def ismatch(cls, key, matchvalues):
        _, values = cls._getIndices(key)
        return all(v == '%' or mv is None or v == mv for v, mv in zip(values, matchvalues))
    @classmethod
    def create_instance(cls, *args):
        obj = cls()
        for k,v in zip(obj._indices, args):
            setattr(obj, k, v)
        return obj
    @classmethod
    def create_from_key(cls, key):
        obj = cls()
        obj.setkey(key)
        return obj
    @classmethod
    def default_key(cls, *args):
        if not cls._indices:
            return cls._prefix
        else:
            return cls._prefix + '.' + '.'.join(escape_key(str(a)) for a in args)
    def setkey(self, key):
        self._prefix, values = self._getIndices(key)
        self.__dict__.update(zip(self._indices, values))
    def kvdb_retrievelist(self):
        if self._deleted:
            return []
        return_list = set()
        for k,v in self.__dict__.items():
            if k[:1] != '_' and hasattr(v, 'kvdb_retrievelist'):
                return_list.update(v.kvdb_retrievelist())
        return list(return_list)
    def kvdb_retrievefinished(self, result):
        for k,v in self.__dict__.items():
            if k[:1] != '_' and hasattr(v, 'kvdb_retrievefinished'):
                v.kvdb_retrievefinished(result)
    def jsonencode(self):
        return dict((k,v) for k,v in self.__dict__.items() if k[:1] != '_' and k not in self._indices)
    @classmethod
    def jsondecode(cls, data):
        obj = cls(None, False)
        obj.__dict__.update((str(k),v) for k,v in data.items())
        return obj
    def __getstate__(self):
        return (dict((k,v) for k,v in self.__dict__.items() if k[:1] != '_' and k not in self._indices), False)
    def __setstate__(self, state):
        self.__init__(None, False)
        self.__dict__.update(state[0])
    def clone_instance(self):
        c = deepcopy(self)
        c.setkey(self.getkey())
        return c
    def create_reference(self):
        return ReferenceObject(self.getkey(), self)
    def create_weakreference(self):
        return WeakReferenceObject(self.getkey())
    def __repr__(self, *args, **kwargs):
        return '<DataObject (%s) at %r>' % (object.__repr__(self, *args, **kwargs), self.getkey())
    def __eq__(self, obj):
        return isinstance(obj, DataObject) and (obj is self or \
            dict((k,v) for k,v in self.__dict__.items() if k[:1] != '_') == dict((k,v) for k,v in obj.__dict__.items() if k[:1] != '_'))
    def __hash__(self):
        raise TypeError('unhashable type: DataObject')
    def __ne__(self, obj):
        return not self.__eq__(obj)
    def kvdb_update(self, obj):
        for k,v in list(self.__dict__.items()):
            if k[:1] != '_':
                if k in obj.__dict__ and v != obj.__dict__[k]:
                    if hasattr(v, 'kvdb_update'):
                        v.kvdb_update(obj.__dict__[k])
                    else:
                        if hasattr(v, 'kvdb_detach'):
                            v.kvdb_detach()
                        self.__dict__[k] = obj.__dict__[k]
                elif k not in obj.__dict__:
                    if hasattr(v, 'kvdb_detach'):
                        v.kvdb_detach()
                    del self.__dict__[k]
        for k,v in obj.__dict__.items():
            if k not in self.__dict__:
                self.__dict__[k] = v
    def kvdb_detach(self):
        if not self._deleted:
            for k in [k for k in self.__dict__.keys() if k[:1] != '_']:
                if k in self.__dict__ and hasattr(self.__dict__[k], 'kvdb_detach'):
                    self.__dict__[k].kvdb_detach()
            self._deleted = True
    def kvdb_internalref(self):
        r = set()
        for k,v in self.__dict__.items():
            if k[:1] != '_' and v is not None and hasattr(v, 'kvdb_internalref'):
                r.update(v.kvdb_internalref())
        return r
        

class DataObjectSet(object):
    "A set of data objects, usually of a same type. Allow weak references only."
    def __init__(self):
        self._dataset = set()
    def jsonencode(self):
        return list(self._dataset)
    @classmethod
    def jsondecode(cls, data):
        obj = cls.__new__(cls)
        obj._dataset = set(data)
        return obj
    def __getstate__(self):
        return (list(self._dataset), True)
    def __setstate__(self, state):
        self._dataset = set(state[0])
    def find(self, cls, *args):
        return [robj for robj in self._dataset if cls.ismatch(robj.getkey(), args)]
    def __repr__(self, *args, **kwargs):
        return '<DataObjectSet %r>' % (self._dataset,)
    def __eq__(self, obj):
        return isinstance(obj, DataObjectSet) and (obj is self or self._dataset == obj._dataset)
    def __ne__(self, obj):
        return not self.__eq__(obj)
    def __hash__(self):
        raise TypeError('unhashable type: DataObject')
    def kvdb_update(self, obj):
        self._dataset.clear()
        self._dataset.update(obj._dataset)
    def dataset(self):
        return self._dataset

@contextmanager
def watch_context(keys, result, reqid, container, module = 'objectdb'):
    try:
        keys = [k for k,r in zip(keys, result) if r is not None]
        yield result
    finally:
        if keys:
            def clearup():
                try:
                    for m in callAPI(container, module, 'munwatch', {'keys': keys, 'requestid': reqid}):
                        yield m
                except QuitException:
                    pass
            container.subroutine(clearup())
        
def multiwaitif(references, container, expr, nextchange = False):
    keys = set(r.getkey() for r in references)
    matchers = tuple(DataObjectUpdateEvent.createMatcher(k) for k in keys)
    flag = nextchange
    def updateref():
        e = container.event
        k = e.key
        o = e.object
        for r in references:
            if r.getkey() == k:
                if r._ref is not o:
                    r._ref = o
    while True:
        r = not flag and expr(references)
        flag = False
        if r:
            container.retvalue = r
            break
        yield matchers
        transid = container.event.transactid
        updated_keys = keys.intersection(container.event.allkeys)
        transact_matcher = DataObjectUpdateEvent.createMatcher(None, transid, _ismatch = lambda x: x.key in updated_keys)
        while True:
            updateref()
            updated_keys.remove(container.event.key)
            if not updated_keys:
                break
            yield (transact_matcher,)

def updater(f):
    "Decorate a function with named arguments into updater for transact"
    @functools.wraps(f)
    def wrapped_updater(keys, values):
        result = f(*values)
        return (keys[:len(result)], result)
    return wrapped_updater

def list_updater(*args):
    """
    Decorate a function with named lists into updater for transact.
    
    :params args: parameter list sizes. -1 means all other items. None means a single item instead of a list.
    only one -1 is allowed.
    """
    neg_index = [i for v,i in izip(args, itertools.count()) if v is not None and v < 0]
    if len(neg_index) > 1:
        raise ValueError("Cannot use negative values more than once")
    if not neg_index:
        slice_list = []
        sum = 0
        for arg in args:
            if arg is None:
                slice_list.append(sum)
                sum += 1
            else:
                slice_list.append(slice(sum, sum + arg))
                sum += arg
    else:
        sep = neg_index[0]
        accum = list(_accum(args[:sep]))
    def inner_wrapper(f):
        @functools.wraps(f)
        def wrapped_updater(keys, values):
            

class AlreadyExistsException(Exception):
    pass

def create_new(cls, oldvalue, *args):
    "Raise  if the old value already exists"
    if oldvalue is not None:
        raise AlreadyExistsException('%r already exists' % (oldvalue,))
    return cls.create_instance(*args)

def create_from_key(cls, oldvalue, key):
    "Raise  if the old value already exists"
    if oldvalue is not None:
        raise AlreadyExistsException('%r already exists' % (oldvalue,))
    return cls.create_from_key(key)

def set_new(oldvalue, newvalue):
    if oldvalue is not None:
        raise AlreadyExistsException('%r already exists' % (oldvalue,))
    return newvalue.clone_instance()

def dump(obj, attributes = True, _refset = None):
    "Show full value of a data object"
    if _refset is None:
        _refset = set()
    if obj is None:
        return None
    elif isinstance(obj, DataObject):        
        if id(obj) in _refset:
            attributes = False
        else:
            _refset.add(id(obj))
        cls = type(obj)
        clsname = getattr(cls, '__module__', '<unknown>') + '.' + getattr(cls, '__name__', '<unknown>')
        baseresult = {'_type': clsname, '_key': obj.getkey()}
        if not attributes:
            _refset.remove(id(obj))
            return baseresult
        else:
            baseresult.update((k,dump(v, attributes, _refset)) for k,v in vars(obj).items() if k[:1] != '_')
            _refset.remove(id(obj))
        return baseresult
    elif isinstance(obj, ReferenceObject):
        if obj._ref is not None:
            return dump(obj._ref, attributes, _refset)
        else:
            return {'_ref':obj.getkey()}
    elif isinstance(obj, WeakReferenceObject):
        return {'_weakref':obj.getkey()}
    elif isinstance(obj, DataObjectSet):
        return dump(list(obj.dataset()))
    elif isinstance(obj, dict):
        return dict((k, dump(v, attributes, _refset)) for k,v in obj.items())
    elif isinstance(obj, list) or isinstance(obj, tuple) or isinstance(obj, set):
        return [dump(v, attributes, _refset) for v in obj]
    else:
        return obj

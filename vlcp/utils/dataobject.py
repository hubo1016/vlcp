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
        pass
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
        if not isinstance(obj, ReferenceObject):
            return NotImplemented
        else:
            return (obj is self or obj._key == self._key)
    def __ne__(self, obj):
        if not isinstance(obj, ReferenceObject):
            return NotImplemented
        else:
            return not (obj is self or obj._key == self._key)
    def __repr__(self, *args, **kwargs):
        return '<ReferenceObject: %r at %r>' % (self._ref, self._key)
    def __str__(self, *args, **kwargs):
        return self._key
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
        if not isinstance(obj, WeakReferenceObject):
            return NotImplemented
        else:
            return (obj is self or obj._key == self._key)
    def __ne__(self, obj):
        if not isinstance(obj, WeakReferenceObject):
            return NotImplemented
        else:
            return not (obj is self or obj._key == self._key)
    def __repr__(self, *args, **kwargs):
        return '<WeakReferenceObject: %r>' % (self._key,)
    def __str__(self, *args, **kwargs):
        return self._key

class DataObject(object):
    "A base class to serialize data into KVDB"
    _prefix = ''
    _indices = ()
    # Unique key and multiple key should like: (('keyname1', ('keyattr1', 'keyattr2', ...),), ('keyname2', ...), ...)
    _unique_keys = ()
    _multi_keys = ()
    _auto_removes = {}
    @classmethod
    def _register_auto_remove(cls, key, func):
        if '_auto_removes' not in cls.__dict__:
            cls._auto_removes = {key: func}
        else:
            cls.__dict__['_auto_removes'][key] = func
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
    def isinstance(cls, cls2):
        # Compare by name
        def clsname(c):
            return c.__module__ + '.' + c.__name__
        try:
            return clsname(cls2) in (clsname(c) for c in cls.__mro__)
        except Exception:
            return isinstance(cls, cls2)
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
        if not isinstance(obj, DataObject):
            return NotImplemented
        else:
            return (obj is self or \
                dict((k,v) for k,v in self.__dict__.items() if k[:1] != '_') == dict((k,v) for k,v in obj.__dict__.items() if k[:1] != '_'))
    def __hash__(self):
        raise TypeError('unhashable type: DataObject')
    def __ne__(self, obj):
        r = self.__eq__(obj)
        if r is NotImplemented:
            return r
        else:
            return not r
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
    @classmethod
    def unique_key(cls, keyname, *args):
        return UniqueKeyReference.default_key(cls._prefix, keyname, '.'.join(escape_key(str(a)) for a in args))
    @classmethod
    def multi_key(cls, keyname, *args):
        return MultiKeyReference.default_key(cls._prefix, keyname, '.'.join(escape_key(str(a)) for a in args))
    def kvdb_uniquekeys(self):
        return [self.unique_key(k, *[getattr(self, ind) for ind in indices])
                for k,indices in self._unique_keys
                if all(hasattr(self, ind) for ind in indices)]
    def kvdb_multikeys(self):
        return [self.multi_key(k, *[getattr(self, ind) for ind in indices])
                for k,indices in self._multi_keys
                if all(hasattr(self, ind) for ind in indices)]
    def kvdb_autoremove(self):
        auto_removes = {}
        for c in reversed(type(self).__mro__):
            if '_auto_removes' in c.__dict__:
                auto_removes.update(c.__dict__['_auto_removes'])
        return set(itertools.chain.from_iterable(v(self) for v in auto_removes.values()))
        
class DataObjectSet(object):
    "A set of data objects, usually of a same type. Allow weak references only."
    def __init__(self):
        self._dataset = set()
        self._dataindices = None
        self._lastclass = None
    def jsonencode(self):
        return list(self._dataset)
    @classmethod
    def jsondecode(cls, data):
        obj = cls.__new__(cls)
        obj.__init__()
        obj._dataset = set(data)
        return obj
    def __getstate__(self):
        return (list(self._dataset), True)
    def __setstate__(self, state):
        self.__init__()
        self._dataset = set(state[0])
    def _create_indices(self, cls):
        if self._dataindices is not None and cls is self._lastclass:
            return
        self._dataindices = None
        self._lastclass = cls
        for robj in self._dataset:
            _, values = cls._getIndices(robj.getkey())
            if self._dataindices is None:
                self._dataindices = [{} for _ in range(0, len(values))]
            for i in range(0, len(values)):
                self._dataindices[i].setdefault(values[i], set()).add(robj)
    def find(self, cls, *args):
        self._create_indices(cls)
        if self._dataindices is None:
            # No Data
            return []
        curr = None
        for i in range(0, len(args)):
            if args[i] is not None:
                curr_match = self._dataindices[i].get(args[i], set()).union(self._dataindices[i].get('%', set()))
                if curr is None:
                    curr = curr_match
                else:
                    curr.intersection_update(curr_match)
        if curr is None:
            return list(self._dataset)
        else:
            return list(curr)
    def __repr__(self, *args, **kwargs):
        return '<DataObjectSet %r>' % (self._dataset,)
    def __eq__(self, obj):
        if not isinstance(obj, DataObjectSet):
            return NotImplemented
        else:
            return (obj is self or self._dataset == obj._dataset)
    def __ne__(self, obj):
        if not isinstance(obj, DataObjectSet):
            return NotImplemented
        else:
            return not (obj is self or self._dataset == obj._dataset)
    def __hash__(self):
        raise TypeError('unhashable type: DataObject')
    def kvdb_update(self, obj):
        self._dataset.clear()
        self._dataset.update(obj._dataset)
        self._dataindices = None
        self._lastclass = None
    def dataset(self):
        return self._dataset

class UniqueKeySet(DataObject):
    _prefix = 'indicesset'
    _indices = ('prefix', 'keyname')
    def __init__(self, prefix=None, deleted=False):
        super(UniqueKeySet, self).__init__(prefix=prefix, deleted=deleted)
        self.set = DataObjectSet()


class MultiKeySet(DataObject):
    _prefix = 'multiindicesset'
    _indices = ('prefix', 'keyname')
    def __init__(self, prefix=None, deleted=False):
        super(MultiKeySet, self).__init__(prefix=prefix, deleted=deleted)
        self.set = DataObjectSet()


class UniqueKeyReference(DataObject):
    _prefix = 'indices'
    _indices = ('prefix', 'keyname', 'value')
    @classmethod
    def get_keyset_from_key(cls, key):
        _, (prefix, keyname, _) = cls._getIndices(key)
        return UniqueKeySet.default_key(prefix, keyname)

        
class MultiKeyReference(DataObject):
    _prefix = 'multiindices.'
    _indices = ('prefix', 'keyname', 'value')
    @classmethod
    def get_keyset_from_key(cls, key):
        _, (prefix, keyname, _) = cls._getIndices(key)
        return MultiKeySet.default_key(prefix, keyname)
    

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
    updated_keys = ()
    while True:
        updated_values = [ref for ref in references if ref.getkey() in updated_keys]
        r = not flag and expr(references, updated_values)
        flag = False
        if r:
            container.retvalue = ([ref for ref in references if ref.getkey() in updated_keys], r)
            break
        yield matchers
        transid = container.event.transactid
        updated_keys = keys.intersection(container.event.allkeys)
        last_key = [k for k in container.event.allkeys if k in keys][-1]
        transact_matcher = DataObjectUpdateEvent.createMatcher(None, transid, _ismatch = lambda x: x.key in updated_keys)
        while True:
            updateref()
            if container.event.key == last_key:
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
    
    :params \*args: parameter list sizes. -1 means all other items. None means a single item instead of a list.
                    only one -1 is allowed.
    """
    neg_index = [i for v,i in izip(args, itertools.count()) if v is not None and v < 0]
    if len(neg_index) > 1:
        raise ValueError("Cannot use negative values more than once")
    if not neg_index:
        slice_list = []
        size = 0
        for arg in args:
            if arg is None:
                slice_list.append(size)
                size += 1
            else:
                slice_list.append(slice(size, size + arg))
                size += arg
    else:
        sep = neg_index[0]
        slice_list = []
        size = 0
        for arg in args[:sep]:
            if arg is None:
                slice_list.append(size)
                size += 1
            else:
                slice_list.append(slice(size, size + arg))
                size += arg
        rslice_list = []
        rsize = 0
        for arg in args[:sep:-1]:
            if arg is None:
                rslice_list.append(-1-rsize)
                rsize += 1
            else:
                rslice_list.append(slice(None if not rsize else -rsize, -(rsize + arg)))
                rsize += arg
        slice_list.append(slice(size, rsize))
        slice_list.extend(reversed(rslice_list))
    def inner_wrapper(f):
        @functools.wraps(f)
        def wrapped_updater(keys, values):
            result = f(*[values[s] for s in slice_list])
            return (keys[:len(result)], result)
        return wrapped_updater
    return inner_wrapper

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

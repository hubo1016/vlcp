'''
Created on 2015/06/01

:author: hubo
'''
from __future__ import print_function, absolute_import, division 

try:
    from collections import OrderedDict
except Exception:
    # Backport of OrderedDict() class that runs on Python 2.4, 2.5, 2.6, 2.7 and pypy.
    # Passes Python2.7's test suite and incorporates all the latest updates.
    try:    
        from thread import get_ident as _get_ident
    except ImportError:
        from dummy_thread import get_ident as _get_ident
    
    try:
        from _abcoll import KeysView, ValuesView, ItemsView
    except ImportError:
        pass
    
    
    class OrderedDict(dict):
        'Dictionary that remembers insertion order'
        # An inherited dict maps keys to values.
        # The inherited dict provides __getitem__, __len__, __contains__, and get.
        # The remaining methods are order-aware.
        # Big-O running times for all methods are the same as for regular dictionaries.
    
        # The internal self.__map dictionary maps keys to links in a doubly linked list.
        # The circular doubly linked list starts and ends with a sentinel element.
        # The sentinel element never gets deleted (this simplifies the algorithm).
        # Each link is stored as a list of length three:  [PREV, NEXT, KEY].
    
        def __init__(self, *args, **kwds):
            '''Initialize an ordered dictionary.  Signature is the same as for
            regular dictionaries, but keyword arguments are not recommended
            because their insertion order is arbitrary.
    
            '''
            if len(args) > 1:
                raise TypeError('expected at most 1 arguments, got %d' % len(args))
            try:
                self.__root
            except AttributeError:
                self.__root = root = []                     # sentinel node
                root[:] = [root, root, None]
                self.__map = {}
            self.__update(*args, **kwds)
    
        def __setitem__(self, key, value, dict_setitem=dict.__setitem__):
            'od.__setitem__(i, y) <==> od[i]=y'
            # Setting a new item creates a new link which goes at the end of the linked
            # list, and the inherited dictionary is updated with the new key/value pair.
            if key not in self:
                root = self.__root
                last = root[0]
                last[1] = root[0] = self.__map[key] = [last, root, key]
            dict_setitem(self, key, value)
    
        def __delitem__(self, key, dict_delitem=dict.__delitem__):
            'od.__delitem__(y) <==> del od[y]'
            # Deleting an existing item uses self.__map to find the link which is
            # then removed by updating the links in the predecessor and successor nodes.
            dict_delitem(self, key)
            link_prev, link_next, key = self.__map.pop(key)
            link_prev[1] = link_next
            link_next[0] = link_prev
    
        def __iter__(self):
            'od.__iter__() <==> iter(od)'
            root = self.__root
            curr = root[1]
            while curr is not root:
                yield curr[2]
                curr = curr[1]
    
        def __reversed__(self):
            'od.__reversed__() <==> reversed(od)'
            root = self.__root
            curr = root[0]
            while curr is not root:
                yield curr[2]
                curr = curr[0]
    
        def clear(self):
            'od.clear() -> None.  Remove all items from od.'
            try:
                for node in self.__map.itervalues():
                    del node[:]
                root = self.__root
                root[:] = [root, root, None]
                self.__map.clear()
            except AttributeError:
                pass
            dict.clear(self)
    
        def popitem(self, last=True):
            '''od.popitem() -> (k, v), return and remove a (key, value) pair.
            Pairs are returned in LIFO order if last is true or FIFO order if false.
    
            '''
            if not self:
                raise KeyError('dictionary is empty')
            root = self.__root
            if last:
                link = root[0]
                link_prev = link[0]
                link_prev[1] = root
                root[0] = link_prev
            else:
                link = root[1]
                link_next = link[1]
                root[1] = link_next
                link_next[0] = root
            key = link[2]
            del self.__map[key]
            value = dict.pop(self, key)
            return key, value
    
        # -- the following methods do not depend on the internal structure --
    
        def keys(self):
            'od.keys() -> list of keys in od'
            return list(self)
    
        def values(self):
            'od.values() -> list of values in od'
            return [self[key] for key in self]
    
        def items(self):
            'od.items() -> list of (key, value) pairs in od'
            return [(key, self[key]) for key in self]
    
        def iterkeys(self):
            'od.iterkeys() -> an iterator over the keys in od'
            return iter(self)
    
        def itervalues(self):
            'od.itervalues -> an iterator over the values in od'
            for k in self:
                yield self[k]
    
        def iteritems(self):
            'od.iteritems -> an iterator over the (key, value) items in od'
            for k in self:
                yield (k, self[k])
    
        def update(*args, **kwds):
            '''od.update(E, **F) -> None.  Update od from dict/iterable E and F.
    
            If E is a dict instance, does:           for k in E: od[k] = E[k]
            If E has a .keys() method, does:         for k in E.keys(): od[k] = E[k]
            Or if E is an iterable of items, does:   for k, v in E: od[k] = v
            In either case, this is followed by:     for k, v in F.items(): od[k] = v
    
            '''
            if len(args) > 2:
                raise TypeError('update() takes at most 2 positional '
                                'arguments (%d given)' % (len(args),))
            elif not args:
                raise TypeError('update() takes at least 1 argument (0 given)')
            self = args[0]
            # Make progressively weaker assumptions about "other"
            other = ()
            if len(args) == 2:
                other = args[1]
            if isinstance(other, dict):
                for key in other:
                    self[key] = other[key]
            elif hasattr(other, 'keys'):
                for key in other.keys():
                    self[key] = other[key]
            else:
                for key, value in other:
                    self[key] = value
            for key, value in kwds.items():
                self[key] = value
    
        __update = update  # let subclasses override update without breaking __init__
    
        __marker = object()
    
        def pop(self, key, default=__marker):
            '''od.pop(k[,d]) -> v, remove specified key and return the corresponding value.
            If key is not found, d is returned if given, otherwise KeyError is raised.
    
            '''
            if key in self:
                result = self[key]
                del self[key]
                return result
            if default is self.__marker:
                raise KeyError(key)
            return default
    
        def setdefault(self, key, default=None):
            'od.setdefault(k[,d]) -> od.get(k,d), also set od[k]=d if k not in od'
            if key in self:
                return self[key]
            self[key] = default
            return default
    
        def __repr__(self, _repr_running={}):
            'od.__repr__() <==> repr(od)'
            call_key = id(self), _get_ident()
            if call_key in _repr_running:
                return '...'
            _repr_running[call_key] = 1
            try:
                if not self:
                    return '%s()' % (self.__class__.__name__,)
                return '%s(%r)' % (self.__class__.__name__, self.items())
            finally:
                del _repr_running[call_key]
    
        def __reduce__(self):
            'Return state information for pickling'
            items = [[k, self[k]] for k in self]
            inst_dict = vars(self).copy()
            for k in vars(OrderedDict()):
                inst_dict.pop(k, None)
            if inst_dict:
                return (self.__class__, (items,), inst_dict)
            return self.__class__, (items,)
    
        def copy(self):
            'od.copy() -> a shallow copy of od'
            return self.__class__(self)
    
        @classmethod
        def fromkeys(cls, iterable, value=None):
            '''OD.fromkeys(S[, v]) -> New ordered dictionary with keys from S
            and values equal to v (which defaults to None).
    
            '''
            d = cls()
            for key in iterable:
                d[key] = value
            return d
    
        def __eq__(self, other):
            '''od.__eq__(y) <==> od==y.  Comparison to another OD is order-sensitive
            while comparison to a regular mapping is order-insensitive.
    
            '''
            if isinstance(other, OrderedDict):
                return len(self)==len(other) and self.items() == other.items()
            return dict.__eq__(self, other)
    
        def __ne__(self, other):
            return not self == other
    
        # -- the following methods are only used in Python 2.7 --
    
        def viewkeys(self):
            "od.viewkeys() -> a set-like object providing a view on od's keys"
            return KeysView(self)
    
        def viewvalues(self):
            "od.viewvalues() -> an object providing a view on od's values"
            return ValuesView(self)
    
        def viewitems(self):
            "od.viewitems() -> a set-like object providing a view on od's items"
            return ItemsView(self)    

class MatchTree(object):
    '''
    A dictionary tree for fast event match
    '''
    def __init__(self, parent = None):
        '''
        Constructor
        '''
        self.index = {}
        self.matchers = OrderedDict()
        self.parent = parent
        if parent is not None:
            self.depth = parent.depth + 1
        else:
            self.depth = 0
    def subtree(self, matcher, create = False):
        '''
        Find a subtree from a matcher
        :param matcher: the matcher to locate the subtree. If None, return the root of the tree.
        :param create: if True, the subtree is created if not exists; otherwise return None if not exists
        '''
        if matcher is None:
            return self
        current = self
        for i in range(self.depth, len(matcher.indices)):
            ind = matcher.indices[i]
            if ind is None:
                # match Any
                if hasattr(current, 'any'):
                    current = current.any
                else:
                    if create:
                        cany = MatchTree(current)
                        cany.parentIndex = None
                        current.any = cany
                        current = cany
                    else:
                        return None
            else:
                current2 = current.index.get(ind)
                if current2 is None:
                    if create:
                        cind = MatchTree(current)
                        cind.parentIndex = ind 
                        current.index[ind] = cind
                        current = cind
                    else:
                        return None
                else:
                    current = current2
        return current
    def insert(self, matcher, obj):
        '''
        Insert a new matcher
        :param matcher: an EventMatcher
        :param obj: object to return
        '''
        current = self.subtree(matcher, True)
        current.matchers[obj] = matcher
        return current
    def remove(self, matcher, obj):
        '''
        Remove the matcher
        :param matcher: an EventMatcher
        :param obj: the object to remove
        '''
        current = self.subtree(matcher, False)
        if current is None:
            return
        del current.matchers[obj]
        while not current.matchers and not hasattr(current,'any') \
                and not current.index and current.parent is not None:
            # remove self from parents
            ind = current.parentIndex
            if ind is None:
                del current.parent.any
            else:
                del current.parent.index[ind]
            p = current.parent
            current.parent = None
            current = p
    def matchesWithMatchers(self, event):
        '''
        Return all matches for this event. The first matcher is also returned for each matched object.
        :param event: an input event
        '''
        ret = []
        self._matches(event, set(), ret)
        return tuple(ret)
    def matches(self, event):
        '''
        Return all matches for this event. The first matcher is also returned for each matched object.
        :param event: an input event
        '''
        ret = []
        self._matches(event, set(), ret)
        return tuple(r[0] for r in ret)
    def _matches(self, event, duptest, retlist):
        # 1. matches(self.index[ind], event)
        # 2. matches(self.any, event)
        # 3. self.matches
        if self.depth < len(event.indices):
            ind = event.indices[self.depth]
            if ind in self.index:
                self.index[ind]._matches(event, duptest, retlist)
            if hasattr(self, 'any'):
                self.any._matches(event, duptest, retlist)
        for o, m in self.matchers.items():
            if o not in duptest and m.judge(event):
                duptest.add(o)
                retlist.append((o, m))
    def matchfirst(self, event):
        '''
        Return first match for this event
        :param event: an input event
        '''
        # 1. matches(self.index[ind], event)
        # 2. matches(self.any, event)
        # 3. self.matches
        if self.depth < len(event.indices):
            ind = event.indices[self.depth]
            if ind in self.index:
                m = self.index[ind].matchfirst(event)
                if m is not None:
                    return m
            if hasattr(self, 'any'):
                m = self.any.matchfirst(event)
                if m is not None:
                    return m
        for o, m in self.matchers.items():
            if m is None or m.judge(event):
                return o

class EventTree(object):
    '''
    Store events; match matchers
    '''
    def __init__(self, parent = None, branch = 5):
        '''
        Constructor
        '''
        self.events = []
        self.subevents = []
        self.parent = parent
        if parent is not None:
            self.depth = parent.depth + 1
        else:
            self.depth = 0
        self.branch = 5
    def subtree(self, event, create = False):
        '''
        Find a subtree from an event
        '''
        current = self
        for i in range(self.depth, len(event.indices)):
            if not hasattr(current, 'index'):
                return current
            ind = event.indices[i]
            if create:
                current = current.index.setdefault(ind, EventTree(current, self.branch))
                current.parentIndex = ind
            else:
                current = current.index.get(ind)
                if current is None:
                    return None
        return current
    def insert(self, event):
        current = self.subtree(event, True)
        if current.depth == len(event.indices):
            current.events.append(event)
        else:
            current.subevents.append(event)
            if len(current.subevents) > self.branch:
                current.index = {}
                for e in current.subevents:
                    current.insert(e)
                del current.subevents[:]
    def _returnAll(self, ret):
        ret.extend(self.events)
        if hasattr(self, 'index'):
            for st in self.index.values():
                st._returnAll(ret)
        else:
            ret.extend(self.subevents)
    def _removeFromParent(self):
        current = self
        while current.parent is not None:
            del current.parent.index[current.parentIndex]
            p = current.parent
            current.parent = None
            current = p
            if current.index or current.events:
                break
        if hasattr(current, 'index') and not current.index:
            del current.index
    def _findAndRemove(self, matcher, ret):
        current = self
        while current.depth < len(matcher.indices):
            ind = matcher.indices[current.depth]
            if not hasattr(current, 'index'):
                newsub = []
                for e in current.subevents:
                    if matcher.isMatch(e, current.depth):
                        ret.append(e)
                    else:
                        newsub.append(e)
                current.subevents = newsub
                if not current.subevents and not current.events:
                    current._removeFromParent()
                return
            elif ind is None:
                for st in current.index.values():
                    st._findAndRemove(matcher, ret)
                return
            else:
                if ind not in current.index:
                    return
                current = current.index[ind]
        current._returnAll(ret)
        current._removeFromParent()
    def findAndRemove(self, matcher):
        ret = []
        self._findAndRemove(matcher, ret)
        return tuple(ret)
    def remove(self, event):
        current = self.subtree(event)
        if current.depth == len(event.indices):
            current.events.remove(event)
        else:
            current.subevents.remove(event)

            

'''
Created on 2015/6/25

@author: hubo
'''

class ConfigTree(object):
    def __init__(self):
        pass
    def keys(self):
        return self.__dict__.keys()
    def items(self):
        return self.__dict__.items()
    def __len__(self):
        return len(self.__dict__)
    def config_keys(self, sortkey = False):
        if sortkey:
            items = sorted(self.items())
        else:
            items = self.items()
        for k,v in items:
            if isinstance(v, ConfigTree):
                for k2 in v.config_keys(sortkey):
                    yield k + '.' + k2
            else:
                yield k
    def config_items(self, sortkey = False):
        if sortkey:
            items = sorted(self.items())
        else:
            items = self.items()
        for k,v in items:
            if isinstance(v, ConfigTree):
                for k2,v2 in v.config_items(sortkey):
                    yield (k + '.' + k2, v2)
            else:
                yield (k,v)
    def config_value_keys(self, sortkey = False):
        if sortkey:
            items = sorted(self.items())
        else:
            items = self.items()
        return (k for k,v in items if not isinstance(v,ConfigTree))        
    def config_value_items(self, sortkey = False):
        if sortkey:
            items = sorted(self.items())
        else:
            items = self.items()        
        return ((k,v) for k,v in items if not isinstance(v,ConfigTree))
    def loadconfig(self, keysuffix, obj):
        subtree = self.get(keysuffix)
        if subtree is not None and isinstance(subtree, ConfigTree):
            for k,v in subtree.items():
                if isinstance(v, ConfigTree):
                    if hasattr(obj, k) and not isinstance(getattr(obj, k), ConfigTree):
                        v.loadconfig(getattr(obj,k))
                    else:
                        setattr(obj, k, v)
                elif not hasattr(obj, k):
                    setattr(obj, k, v)
    def withconfig(self, keysuffix):
        def decorator(cls):
            return self.loadconfig(keysuffix, cls)
        return decorator
    def __iter__(self):
        return iter(self.__dict__)
    def _getsubitem(self, key, create = False):
        keylist = [k for k in key.split('.') if k != '']
        if not keylist:
            raise KeyError('Config key is empty')
        current = self
        for k in keylist[:-1]:
            if not hasattr(current, k):
                if create:
                    c2 = ConfigTree()
                    setattr(current, k, c2)
                    current = c2
                else:
                    return (None, None)
            else:
                v = getattr(current, k)
                if not isinstance(v, ConfigTree):
                    if create:
                        c2 = ConfigTree()
                        setattr(current, k, c2)
                        current = c2
                    else:
                        return (None, None)
                else:
                    current = v
        return (current, keylist[-1])
    def __setitem__(self, key, value):
        (t, k) = self._getsubitem(key, True)
        setattr(t, k, value)
    def __delitem__(self, key):
        (t, k) = self._getsubitem(key, False)
        if t is None:
            raise KeyError(key)
        else:
            delattr(t, k)
    def __getitem__(self, key):
        (t, k) = self._getsubitem(key, False)
        if t is None:
            raise KeyError(key)
        else:
            return t.__dict__[k]
    def get(self, key, defaultvalue = None):
        (t, k) = self._getsubitem(key, False)
        if t is None:
            return defaultvalue
        else:
            return t.__dict__.get(k, defaultvalue)
    def setdefault(self, key, defaultvalue = None):        
        (t, k) = self._getsubitem(key, True)
        return t.__dict__.setdefault(k, defaultvalue)
    def __contains__(self, key):
        (t, k) = self._getsubitem(key, False)
        if t is None:
            return False
        else:
            return k in t.__dict__

class Manager(ConfigTree):
    '''
    Config manager
    '''
    def __init__(self):
        #TODO: load config
        ConfigTree.__init__(self)

manager = Manager()

class Configurable(object):
    def __init__(self):
        pass
    def __getattr__(self, key):
        if key.startswith('_'):
            raise AttributeError("type object '%s' has no attribute '$s'" % (type(self).__name__, key))
        cls = type(self)
        while True:
            try:
                return manager[getattr(cls, 'configkey') + '.' + key]
            except AttributeError:
                pass
            except KeyError:
                pass
            try:
                return cls.__dict__['_default_' + key]
            except KeyError:
                pass
            parent = None
            for c in cls.__bases__:
                if issubclass(c, Configurable):
                    parent = c
            if parent is None:
                break
            cls = parent
        raise AttributeError("type object '%s' has no attribute '%s'" % (type(self).__name__, key))
    @classmethod
    def getConfigurableParent(cls):
        for p in cls.__bases__:
            if isinstance(p, Configurable) and p is not Configurable:
                return p
        return None

def configbase(key):
    def decorator(cls):
        parent = cls.getConfigurableParent()
        if parent is None:
            parentbase = None
        else:
            parentbase = getattr(parent, 'configbase', None)
        if parentbase is None:
            base = key
        else:
            base = parentbase + '.' + key
        cls.configbase = base
        cls.configkey = base + '.default'
        return cls
    return decorator

def config(key):
    def decorator(cls):
        parent = cls.getConfigurableParent()
        if parent is None:
            parentbase = None
        else:
            parentbase = getattr(parent, 'configbase', None)
        if parentbase is None:
            cls.configkey = key
        else:
            cls.configkey = parentbase + '.' + key
        return cls
    return decorator

def defaultconfig(cls):
    parentbase = None
    for p in cls.__bases__:
        if issubclass(p, Configurable):
            parentbase = getattr(p, 'configbase', None)
            break
    if parentbase is None:
        base = cls.__name__.lower()
        cls.configbase = base
        cls.configkey = base + '.default'
    else:
        key = cls.__name__.lower()
        parentkeys = parentbase.split('.')
        for pk in parentkeys:
            if key.endswith(pk):
                key = key[0:-len(pk)]
            elif key.startswith(pk):
                key = key[len(pk):]
        cls.configkey = parentbase + "." + key
    return cls

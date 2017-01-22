'''
Created on 2015/6/25

:author: hubo
'''
import re
import ast
import sys
import io

class ConfigTree(object):
    """
    A basic config node. A node supports both attributes get/set and dict-like operations. When
    using dict-like interfaces, configurations in child nodes can directly be used::
    
        node['child.childconfig'] = 42
        node.child.childconfig  # 42
    """
    def __init__(self):
        pass
    def keys(self):
        """
        Return all children in this node, either sub nodes or configuration values
        """
        return self.__dict__.keys()
    def items(self):
        """
        Return `(key, value)` tuples for children in this node, either sub nodes or configuration values
        """
        return self.__dict__.items()
    def __len__(self):
        """
        Return size of children stored in this node, either sub nodes or configuration values
        """
        return len(self.__dict__)
    def config_keys(self, sortkey = False):
        """
        Return all configuration keys in this node, including configurations on children nodes.
        """
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
        """
        Return all `(key, value)` tuples for configurations in this node, including configurations on children nodes.
        """
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
        """
        Return configuration keys directly stored in this node. Configurations in child nodes are not included.
        """
        if sortkey:
            items = sorted(self.items())
        else:
            items = self.items()
        return (k for k,v in items if not isinstance(v,ConfigTree))        
    def config_value_items(self, sortkey = False):
        """
        Return `(key, value)` tuples for configuration directly stored in this node. Configurations in child nodes are not included.
        """
        if sortkey:
            items = sorted(self.items())
        else:
            items = self.items()        
        return ((k,v) for k,v in items if not isinstance(v,ConfigTree))
    def loadconfig(self, keysuffix, obj):
        """
        Copy all configurations from this node into obj
        """
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
        """
        Load configurations with this decorator
        """
        def decorator(cls):
            return self.loadconfig(keysuffix, cls)
        return decorator
    def __iter__(self):
        """
        Support a dict-like iterate
        """
        return iter(self.__dict__)
    def gettree(self, key, create = False):
        """
        Get a subtree node from the key (path relative to this node)
        """
        tree, _ = self._getsubitem(key + '.tmp', create)
        return tree
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
        """
        Support dict-like assignment
        """
        (t, k) = self._getsubitem(key, True)
        setattr(t, k, value)
    def __delitem__(self, key):
        """
        Support dict-like deletion
        """
        (t, k) = self._getsubitem(key, False)
        if t is None:
            raise KeyError(key)
        else:
            delattr(t, k)
    def __getitem__(self, key):
        """
        Support dict-like retrieval
        """
        (t, k) = self._getsubitem(key, False)
        if t is None:
            raise KeyError(key)
        else:
            return t.__dict__[k]
    def get(self, key, defaultvalue = None):
        """
        Support dict-like get (return a default value if not found)
        """
        (t, k) = self._getsubitem(key, False)
        if t is None:
            return defaultvalue
        else:
            return t.__dict__.get(k, defaultvalue)
    def setdefault(self, key, defaultvalue = None):
        """
        Support dict-like setdefault (create if not existed)
        """
        (t, k) = self._getsubitem(key, True)
        return t.__dict__.setdefault(k, defaultvalue)
    def __contains__(self, key):
        """
        Support dict-like `in` operator
        """
        (t, k) = self._getsubitem(key, False)
        if t is None:
            return False
        else:
            return k in t.__dict__
    def clear(self):
        """
        Support dict-like clear
        """
        self.__dict__.clear()
    def todict(self):
        """
        Convert this node to a dictionary tree.
        """
        dict_entry = []
        for k,v in self.items():
            if isinstance(v, ConfigTree):
                dict_entry.append((k, v.todict()))
            else:
                dict_entry.append((k, v))
        return dict(dict_entry)

class Manager(ConfigTree):
    '''
    Configuration manager. Use the global variable `manager` to access the configuration system.
    '''
    def __init__(self):
        ConfigTree.__init__(self)
    def loadfromfile(self, filelike):
        """
        Read configurations from a file-like object, or a sequence of strings. Old values are not
        cleared, if you want to reload the configurations completely, you should call `clear()`
        before using `load*` methods.
        """
        line_format = re.compile(r'((?:[a-zA-Z][a-zA-Z0-9_]*\.)*[a-zA-Z][a-zA-Z0-9_]*)\s*=\s*')
        space = re.compile(r'\s')
        line_no = 0
        # Suport multi-line data
        line_buffer = []
        line_key = None
        last_line_no = None
        for l in filelike:
            line_no += 1
            ls = l.strip()
            # If there is a # in start, the whole line is remarked.
            # ast.literal_eval already processed any # after '='.
            if not ls or ls.startswith('#'):
                continue
            if space.match(l):
                if not line_key:
                    # First line cannot start with space
                    raise ValueError('Error format in line %d: first line cannot start with space%s\n' % (line_no, l))
                line_buffer.append(l)
            else:
                if line_key:
                    # Process buffered lines
                    try:
                        value = ast.literal_eval(''.join(line_buffer))
                    except:
                        typ, val, tb = sys.exc_info()
                        raise ValueError('Error format in line %d(%s: %s):\n%s' % (last_line_no, typ.__name__, str(val), ''.join(line_buffer)))
                    self[line_key] = value
                # First line
                m = line_format.match(l)
                if not m:
                    raise ValueError('Error format in line %d:\n%s' % (line_no, l))
                line_key = m.group(1)
                last_line_no = line_no
                del line_buffer[:]
                line_buffer.append(l[m.end():])
        if line_key:
            # Process buffered lines
            try:
                value = ast.literal_eval(''.join(line_buffer))
            except:
                typ, val, tb = sys.exc_info()
                raise ValueError('Error format in line %d(%s: %s):\n%s' % (last_line_no, typ.__name__, str(val), ''.join(line_buffer)))
            self[line_key] = value
    def loadfrom(self, path):
        """
        Read configurations from path
        """
        with open(path, 'r') as f:
            self.loadfromfile(f)
    def loadfromstr(self, string):
        """
        Read configurations from string
        """
        self.loadfromfile(string.splitlines(keepends=True))
    def save(self, sortkey = True):
        """
        Save configurations to a list of strings
        """
        return [k + '=' + repr(v) for k,v in self.config_items(sortkey)]
    def savetostr(self, sortkey = True):
        """
        Save configurations to a single string
        """
        return ''.join(k + '=' + repr(v) + '\n' for k,v in self.config_items(sortkey))
    def savetofile(self, filelike, sortkey = True):
        """
        Save configurations to a file-like object which supports `writelines`
        """
        filelike.writelines(k + '=' + repr(v) + '\n' for k,v in self.config_items(sortkey))
    def saveto(self, path, sortkey = True):
        """
        Save configurations to path
        """
        with open(path, 'w') as f:
            self.savetofile(f, sortkey)

# Global configuration manager
manager = Manager()

class Configurable(object):
    """
    Base class for a configurable object. Undefined attributes of a configurable object is mapped to
    global configurations. The attribute value of a configurable object is:
    
    1. The original attribute value if it is set on the instance or class
    
    2. The configuration value `manager[self.configkey + '.' + attrname]` if exists
    
    3. The configuration value `manager[parent.configkey + '.' + attrname]` if parent classes have
       `configkey` defined and configuration exists.
    
    4. The `_default_<attrname>` attribute value of the instance
    
    5. raises AttributeError
    
    Attributes begins with '_' is not mapped.
    
    `configkey` and `configbase` attribute should be set on this class. Usually they are set by decorators
    `@defaultconfig`, `@configbase` or `@config`
    """
    def __init__(self):
        pass
    def __getattr__(self, key):
        if key.startswith('_'):
            raise AttributeError("type object '%s' has no attribute '%s'" % (type(self).__name__, key))
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
        """
        Return the parent from which this class inherits configurations
        """
        for p in cls.__bases__:
            if isinstance(p, Configurable) and p is not Configurable:
                return p
        return None
    @classmethod
    def getConfigRoot(cls, create = False):
        """
        Return the mapped configuration root node
        """
        try:
            return manager.gettree(getattr(cls, 'configkey'), create)
        except AttributeError:
            return None
    def config_value_keys(self, sortkey = False):
        """
        Return all mapped configuration keys for this object
        """
        ret = set()
        cls = type(self)
        while True:
            root = cls.getConfigRoot()
            if root:
                ret = ret.union(set(root.config_value_keys()))
            parent = None
            for c in cls.__bases__:
                if issubclass(c, Configurable):
                    parent = c
            if parent is None:
                break
            cls = parent
        if sortkey:
            return sorted(list(ret))
        else:
            return list(ret)
        
    def config_value_items(self, sortkey = False):
        """
        Return `(key, value)` tuples for all mapped configurations for this object
        """
        return ((k, getattr(self, k)) for k in self.config_value_keys(sortkey))
            
def configbase(key):
    """
    Decorator to set this class to configuration base class. A configuration base class
    uses `<parentbase>.key.` for its configuration base, and uses `<parentbase>.key.default` for configuration mapping.
    """
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
    """
    Decorator to map this class directly to a configuration node. It uses `<parentbase>.key` for configuration
    base and configuration mapping.
    """
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
    """
    Generate a default configuration mapping bases on the class name. If this class does not have a
    parent with `configbase` defined, it is set to a configuration base with
    `configbase=<lowercase-name>` and `configkey=<lowercase-name>.default`; otherwise it inherits
    `configbase` of its parent and set `configkey=<parentbase>.<lowercase-name>`
    
    Refer to :ref::`configurations` for normal rules.
    """
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
        #=======================================================================
        # parentkeys = parentbase.split('.')
        # for pk in parentkeys:
        #     if key.endswith(pk):
        #         key = key[0:-len(pk)]
        #     elif key.startswith(pk):
        #         key = key[len(pk):]
        #=======================================================================
        cls.configkey = parentbase + "." + key
    return cls

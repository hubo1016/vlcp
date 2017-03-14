'''
Created on 2015/9/30/

:author: hubo
'''
from vlcp.config import Configurable, defaultconfig
from vlcp.event import Event, withIndices
import logging
from vlcp.event.runnable import RoutineContainer, EventHandler
import sys
from vlcp.utils.pycache import removeCache
import functools
import copy
from vlcp.config.config import manager
from vlcp.event.core import QuitException
from inspect import cleandoc

try:
    reload
except:
    try:
        from importlib import reload
    except:
        from imp import reload


def depend(*args):
    """
    Decorator to declare dependencies to other modules. Recommended usage is::
    
        import other_module
        
        @depend(other_module.ModuleClass)
        class MyModule(Module):
            ...
    
    :param \*args: depended module classes.
    """
    def decfunc(cls):
        if not 'depends' in cls.__dict__:
            cls.depends = []
        cls.depends.extend(list(args))
        for a in args:
            if not hasattr(a, 'referencedBy'):
                a.referencedBy = []
            a.referencedBy.append(cls)
        return cls
    return decfunc

@withIndices('target', 'name')
class ModuleNotification(Event):
    pass

@withIndices('handle', 'target', 'name')
class ModuleAPICall(Event):
    canignore = False

@withIndices('handle')
class ModuleAPIReply(Event):
    pass

class ModuleAPICallTimeoutException(Exception):
    pass

@withIndices('module', 'state', 'instance')
class ModuleLoadStateChanged(Event):
    NOTLOADED = 'notloaded'
    LOADING = 'loading'
    LOADED = 'loaded'
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'
    UNLOADING = 'unloading'
    UNLOADED = 'unloaded'

def create_discover_info(func):
    code = func.__code__
    if code.co_flags & 0x08:
        haskwargs = True
    else:
        haskwargs = False
    # Remove argument env
    arguments = code.co_varnames[0:code.co_argcount]
    if hasattr(func, '__self__') and func.__self__:
        # First argument is self, remove an extra argument
        arguments=arguments[1:]
    # Optional arguments
    if hasattr(func, '__defaults__') and func.__defaults__:
        requires = arguments[:-len(func.__defaults__)]
        optionals = arguments[-len(func.__defaults__):]
    else:
        requires = arguments[:]
        optionals = []
    return {'description': cleandoc(func.__doc__) if func.__doc__ is not None else '',
            'parameters':
                [{'name':n,'optional':False} for n in requires]
                + [{'name':optionals[i],'optional':True,'default':func.__defaults__[i]}
                   for i in range(0,len(optionals))],
            'extraparameters': haskwargs
                }
    

def api(func, container = None, criteria = None):
    '''
    Return an API def for a generic function
    
    :param func: a function or bounded method
    
    :param container: if None, this is used as a synchronous method, the return value of the method
                      is used for the return value. If not None, this is used as an asynchronous method,
                      the return value should be a generator, and it is executed in `container` as a routine.
                      The return value should be set to `container.retvalue`.
    
    :param criteria: An extra function used to test whether this function should process the API. This allows
                     multiple API definitions to use the same API method name.
    '''
    return (func.__name__.lower(), functools.update_wrapper(lambda n,p: func(**p), func), container,
            create_discover_info(func), criteria)

def publicapi(func, container = None, criteria = None):
    '''
    Create an API def for public API processing. Target name of a public API is `'public'`.

    :param func: a function or bounded method
    
    :param container: if None, this is used as a synchronous method, the return value of the method
                      is used for the return value. If not None, this is used as an asynchronous method,
                      the return value should be a generator, and it is executed in `container` as a routine.
                      The return value should be set to `container.retvalue`.
    
    :param criteria: An extra function used to test whether this function should process the API. This allows
                     multiple API definitions to use the same API method name.
    '''
    return ("public/" + func.__name__.lower(), functools.update_wrapper(lambda n,p: func(**p), func), container,
            create_discover_info(func), criteria)

class ModuleAPIHandler(RoutineContainer):
    """
    API Handler for modules
    """
    def __init__(self, moduleinst, apidefs = None, allowdiscover = True, rejectunknown = True):
        RoutineContainer.__init__(self, scheduler=moduleinst.scheduler, daemon=False)
        self.handler = EventHandler(self.scheduler)
        self.servicename = moduleinst.getServiceName()
        self.apidefs = apidefs
        self.registeredAPIs = {}
        self.discoverinfo = {}
        self.allowdiscover = allowdiscover
        self.rejectunknown = True
    @staticmethod
    def createReply(handle, result):
        return ModuleAPIReply(handle, result=result)
    @staticmethod
    def createExceptionReply(handle, exception):
        return ModuleAPIReply(handle, exception = exception)
    def _createHandler(self, name, handler, container = None, discoverinfo = None, criteria = None):
        extra_params = {}
        if criteria:
            extra_params['_ismatch'] = lambda e: not e.canignore and criteria(**e.params)
        if name is None:
            matcher = ModuleAPICall.createMatcher(target = self.servicename, **extra_params)
            matcher = ModuleAPICall.createMatcher(target = self.servicename, **extra_params)
        elif name.startswith('public/'):
            matcher = ModuleAPICall.createMatcher(target = 'public', name = name[len('public/'):], **extra_params)
        else:
            matcher = ModuleAPICall.createMatcher(target = self.servicename, name = name, **extra_params)
        if container is not None:
            def wrapper(event):
                try:
                    for m in handler(event.name, event.params):
                        yield m
                    for m in container.waitForSend(self.createReply(event.handle, container.retvalue)):
                        yield m
                except Exception as val:
                    for m in container.waitForSend(self.createExceptionReply(event.handle, val)):
                        yield m
            def event_handler(event, scheduler):
                event.canignore = True
                container.subroutine(wrapper(event), False)
        else:
            def wrapper(event):
                try:
                    result = handler(event.name, event.params)
                    for m in self.waitForSend(self.createReply(event.handle, result)):
                        yield m
                except Exception as val:
                    for m in self.waitForSend(self.createExceptionReply(event.handle, val)):
                        yield m
            def event_handler(event, scheduler):
                event.canignore = True
                self.subroutine(wrapper(event), False)
        return (matcher, event_handler)
    def registerAPIs(self, apidefs):
        '''
        API definition is in format: `(name, handler, container, discoverinfo)`
        
        if the handler is a generator, container should be specified
        handler should accept two arguments::
        
            def handler(name, params):
                ...
        
        `name` is the method name, `params` is a dictionary contains the parameters.
        
        the handler can either return the result directly, or be a generator (async-api),
        and write the result to container.retvalue on exit.
        e.g::
        
            ('method1', self.method1),    # method1 directly returns the result
            ('method2', self.method2, self) # method2 is an async-api
        
        Use api() to automatically generate API definitions.
        '''
        handlers = [self._createHandler(*apidef) for apidef in apidefs]
        self.handler.registerAllHandlers(handlers)
        self.discoverinfo.update((apidef[0], apidef[3] if len(apidef) > 3 else {'description':cleandoc(apidef[1].__doc__)}) for apidef in apidefs)
    def registerAPI(self, name, handler, container = None, discoverinfo = None, criteria = None):
        """
        Append new API to this handler
        """
        self.handler.registerHandler(*self._createHandler(name, handler, container, criteria))
        if discoverinfo is None:
            self.discoverinfo[name] = {'description': cleandoc(handler.__doc__)}
        else:
            self.discoverinfo[name] = discoverinfo
    def unregisterAPI(self, name):
        """
        Remove an API from this handler
        """
        if name.startswith('public/'):
            target = 'public'
            name = name[len('public/'):]
        else:
            target = self.servicename
            name = name
        removes = [m for m in self.handler.handlers.keys() if m.target == target and m.name == name]
        for m in removes:
            self.handler.unregisterHandler(m)
    def discover(self, details = False):
        'Discover API definitions. Set details=true to show details'
        if details and not (isinstance(details, str) and details.lower() == 'false'):
            return copy.deepcopy(self.discoverinfo)
        else:
            return dict((k,v.get('description', '')) for k,v in self.discoverinfo.items())
    def reject(self, name, args):
        raise ValueError('%r is not defined in module %r' % (name, self.servicename))
    def start(self, asyncStart=False):
        if self.apidefs:
            self.registerAPIs(self.apidefs)
        if self.allowdiscover:
            self.registerAPI(*api(self.discover))
        if self.rejectunknown:
            self.handler.registerHandler(*self._createHandler(None, self.reject, None))
    def close(self):
        self.handler.close()

@defaultconfig
class Module(Configurable):
    '''
    A functional part which can be loaded or unloaded dynamically
    '''
    # Service modules are automatically unloaded if all the dependencies are unloaded
    _default_service = False
    # Force stop all the RoutineContainers that are started by this module.
    # Subroutines are not stopped.
    _default_forcestop = True
    # Automatically change module status to SUCCEEDED when load() is finished
    _default_autosuccess = True
    depends = []
    def __init__(self, server):
        '''
        Constructor
        '''
        Configurable.__init__(self)
        self.server = server
        self.scheduler = server.scheduler
        self.connections = []
        self.routines = []
        self.dependedBy = set()
        self.state = ModuleLoadStateChanged.NOTLOADED
        self.target = type(self)
        self._logger = logging.getLogger(type(self).__module__ + '.' + type(self).__name__)
    @classmethod
    def getFullPath(cls):
        return cls.__module__ + '.' + cls.__name__
    def createAPI(self, *apidefs):
        """
        Create API definitions on this module. This creates a ModuleAPIHandler and register
        these `apidefs` to it.
        
        :param \*apidefs: should be return values of `api()` or `publicapi()` functions.
        """
        if hasattr(self, 'apiHandler'):
            self.appendAPI(*apidefs)
        self.apiHandler = ModuleAPIHandler(self, apidefs)
        self.routines.append(self.apiHandler)
    def appendAPI(self, *apidefs):
        t = list(self.apiHandler.apidefs)
        t.extend(apidefs)
        self.apiHandler.apidefs = t
    def getServiceName(self):
        """
        Return the targetname (or servicename) for this module
        """
        if hasattr(self, 'servicename'):
            return self.servicename
        else:
            return self.target.__name__.lower()
    def load(self, container):
        '''
        Load module
        '''
        self.target._instance = self
        try:
            for r in self.routines:
                r.start()
            for c in self.connections:
                c.start()
            try:
                for m in self.changestate(ModuleLoadStateChanged.LOADED, container):
                    yield m
                if self.autosuccess:
                    for m in self.changestate(ModuleLoadStateChanged.SUCCEEDED, container):
                        yield m
            except ValueError:
                pass
        except:
            for m in self.changestate(ModuleLoadStateChanged.FAILED, container):
                yield m
            raise
    def unload(self, container, force = False):
        '''
        Unload module
        '''
        for m in self.changestate(ModuleLoadStateChanged.UNLOADING, container):
            yield m
        for c in self.connections:
            try:
                for m in c.shutdown():
                    yield m
            except:
                self._logger.warning('Except when shutting down connection %r', c, exc_info = True)
        if self.forcestop or force:
            for r in self.routines:
                try:
                    r.close()
                except:
                    self._logger.warning('Except when unloading module', exc_info = True)
        for m in self.changestate(ModuleLoadStateChanged.UNLOADED, container):
            yield m
        if hasattr(self.target, '_instance') and self.target._instance is self:
            del self.target._instance
    _changeMap = set(((ModuleLoadStateChanged.NOTLOADED, ModuleLoadStateChanged.LOADING),
                 (ModuleLoadStateChanged.LOADING, ModuleLoadStateChanged.LOADED),
                 (ModuleLoadStateChanged.LOADING, ModuleLoadStateChanged.SUCCEEDED),
                 (ModuleLoadStateChanged.LOADED, ModuleLoadStateChanged.SUCCEEDED),
                 (ModuleLoadStateChanged.LOADING, ModuleLoadStateChanged.FAILED),
                 (ModuleLoadStateChanged.LOADED, ModuleLoadStateChanged.FAILED),
                 (ModuleLoadStateChanged.SUCCEEDED, ModuleLoadStateChanged.UNLOADING),
                 (ModuleLoadStateChanged.FAILED, ModuleLoadStateChanged.UNLOADING),
                 (ModuleLoadStateChanged.UNLOADING, ModuleLoadStateChanged.UNLOADED)))
    def changestate(self, state, container):
        """
        Change the current load state.
        """
        if self.state != state:
            if not (self.state, state) in self._changeMap:
                raise ValueError('Cannot change state from %r to %r' % (self.state, state))
            self.state = state
            self._logger.debug('Module state changed to %r', state)
            for m in container.waitForSend(ModuleLoadStateChanged(self.target, state, self)):
                yield m


class ModuleLoadException(Exception):
    """
    Raised when module loading failed.
    """
    pass

def findModule(path, autoimport = True):
    dotpos = path.rfind('.')
    if dotpos == -1:
        raise ModuleLoadException('Must specify module with full path, including package name')
    package = path[:dotpos]
    classname = path[dotpos+1:]
    p = None
    module = None
    try:
        p = sys.modules[package]
        module = getattr(p, classname)
    except KeyError:
        pass
    except AttributeError:
        pass
    if p is None and autoimport:
        p = __import__(package, fromlist=(classname,))
        module = getattr(p, classname)
    return (p, module)

class _ProxyMetaClass(type):
    '''
    Metaclass for delayed depend
    '''
    @property
    def depends(self):
        if not hasattr(self, '_depends'):
            self._depends = []
            path = manager.get('proxy.' + self.__name__.lower())
            if path is not None:
                try:
                    _, module = findModule(path, True)
                except KeyError as exc:
                    raise ModuleLoadException('Cannot load module ' + repr(path) + ': ' + str(exc) + 'is not defined in the package')
                except Exception as exc:
                    raise ModuleLoadException('Cannot load module ' + repr(path) + ': ' + str(exc))
                if module is None:
                    raise ModuleLoadException('Cannot find module: ' + repr(path))
            else:
                module = self._default
                if module is None:
                    raise ModuleLoadException('Cannot find module: proxy.' + self.__name__ + ' is not specified')
            self._proxytarget = module
            self.depends.append(module)
            if not hasattr(module, 'referencedBy'):
                module.referencedBy = []
            module.referencedBy.append(self)
        return self._depends
        
class _ProxyModule(Module):
    '''
    A proxy to create dependencies on configurable module
    '''
    service = True
    def __init__(self, server):
        '''
        Constructor
        '''
        Module.__init__(self, server)        
        self.proxyhandler = EventHandler(self.scheduler)
    def load(self, container):
        self._targetname = self._proxytarget._instance.getServiceName()
        self.proxyhandler.registerHandler(ModuleAPICall.createMatcher(None, self.getServiceName()), self._proxyhandler)
        for m in Module.load(self, container):
            yield m
    def unload(self, container, force=False):
        self.proxyhandler.close()
        for m in Module.unload(self, container, force=force):
            yield m
    def _proxyhandler(self, event, scheduler):
        event.canignore = True
        scheduler.emergesend(ModuleAPICall(event.handle, self._targetname, event.name, params = event.params))        

def proxy(name, default = None):
    """
    Create a proxy module. A proxy module has a default implementation, but can be redirected to other
    implementations with configurations. Other modules can depend on proxy modules.
    """
    proxymodule = _ProxyMetaClass(name, (_ProxyModule,), {'_default': default})
    proxymodule.__module__ = sys._getframe(1).f_globals.get('__name__')
    return proxymodule

class ModuleLoader(RoutineContainer):
    """
    Module loader to load modules. The server object creates this instance automatically, usually
    you can retrieve the pre-created object from `server.moduleloader`
    """
    _logger = logging.getLogger(__name__ + '.ModuleLoader')
    def __init__(self, server):
        self.server = server
        RoutineContainer.__init__(self, scheduler=server.scheduler, daemon=True)
        self.activeModules = {}
    def main(self):
        try:
            # Reject unmatched public API
            public_api = ModuleAPICall.createMatcher(target = "public")
            while True:
                yield (public_api,)
                if not self.event.canignore:
                    self.event.canignore = True
                    self.scheduler.emergesend(ModuleAPIHandler.createExceptionReply(self.event.handle, ValueError("public API is not processed")))
        except QuitException:
            c = ModuleAPICall.createMatcher()
            while True:
                yield (c,)
                if not self.event.canignore:
                    self.event.canignore = True
                    self.scheduler.emergesend(ModuleAPIHandler.createExceptionReply(self.event.handle, QuitException()))
    def _removeDepend(self, module, depend):
        depend._instance.dependedBy.remove(module)
        if not depend._instance.dependedBy and depend._instance.service:
            # Automatically unload a service which is not used
            self.subroutine(self.unloadmodule(depend), False)
    def loadmodule(self, module):
        '''
        Load a module class. Need delegate to call from other containers.
        '''
        self._logger.debug('Try to load module %r', module)
        if hasattr(module, '_instance'):
            self._logger.debug('Module is already initialized, module state is: %r', module._instance.state)
            if module._instance.state == ModuleLoadStateChanged.UNLOADING or module._instance.state == ModuleLoadStateChanged.UNLOADED:
                # Wait for unload
                um = ModuleLoadStateChanged.createMatcher(module._instance.target, ModuleLoadStateChanged.UNLOADED)
                yield (um,)
            elif module._instance.state == ModuleLoadStateChanged.SUCCEEDED:
                pass
            elif module._instance.state == ModuleLoadStateChanged.FAILED:
                raise ModuleLoadException('Cannot load module %r before unloading the failed instance' % (module,))
            elif module._instance.state == ModuleLoadStateChanged.NOTLOADED or module._instance.state == ModuleLoadStateChanged.LOADED or module._instance.state == ModuleLoadStateChanged.LOADING:
                # Wait for succeeded or failed
                sm = ModuleLoadStateChanged.createMatcher(module._instance.target, ModuleLoadStateChanged.SUCCEEDED)
                fm = ModuleLoadStateChanged.createMatcher(module._instance.target, ModuleLoadStateChanged.FAILED)
                yield (sm, fm)
                if self.matcher is sm:
                    pass
                else:
                    raise ModuleLoadException('Module load failed for %r' % (module,))
        if not hasattr(module, '_instance'):
            self._logger.info('Loading module %r', module)
            inst = module(self.server)
            # Avoid duplicated loading
            module._instance = inst
            # When reloading, some of the dependencies are broken, repair them
            if hasattr(module, 'referencedBy'):
                inst.dependedBy = set([m for m in module.referencedBy if hasattr(m, '_instance') and m._instance.state != ModuleLoadStateChanged.UNLOADED])
            # Load Module
            for d in module.depends:
                if hasattr(d, '_instance') and d._instance.state == ModuleLoadStateChanged.FAILED:
                    raise ModuleLoadException('Cannot load module %r, it depends on %r, which is in failed state.' % (module, d))
            try:
                for d in module.depends:
                    if hasattr(d, '_instance') and d._instance.state == ModuleLoadStateChanged.SUCCEEDED:
                        d._instance.dependedBy.add(module)
                preloads = [d for d in module.depends if not hasattr(d, '_instance') or \
                            d._instance.state != ModuleLoadStateChanged.SUCCEEDED]
                for d in preloads:
                    self.subroutine(self.loadmodule(d), False)
                sms = [ModuleLoadStateChanged.createMatcher(d, ModuleLoadStateChanged.SUCCEEDED) for d in preloads]
                fms = [ModuleLoadStateChanged.createMatcher(d, ModuleLoadStateChanged.FAILED) for d in preloads]
                ms = sms + fms
                while sms:
                    yield ms
                    if self.matcher in fms:
                        raise ModuleLoadException('Cannot load module %r, it depends on %r, which is in failed state.' % (module, self.event.module))
                    else:
                        sms.remove(self.matcher)
                        ms.remove(self.matcher)
            except:
                for d in module.depends:
                    if hasattr(d, '_instance') and module in d._instance.dependedBy:
                        self._removeDepend(module, d)
                self._logger.warning('Loading module %r stopped', module, exc_info=True)
                # Not loaded, send a message to notify that parents can not 
                for m in self.waitForSend(ModuleLoadStateChanged(module, ModuleLoadStateChanged.UNLOADED, inst)):
                    yield m
                del module._instance
                raise
            for d in preloads:
                d._instance.dependedBy.add(module)
            self.activeModules[inst.getServiceName()] = module
            for m in module._instance.changestate(ModuleLoadStateChanged.LOADING, self):
                yield m
            for m in module._instance.load(self):
                yield m
            if module._instance.state == ModuleLoadStateChanged.LOADED or module._instance.state == ModuleLoadStateChanged.LOADING:
                sm = ModuleLoadStateChanged.createMatcher(module._instance.target, ModuleLoadStateChanged.SUCCEEDED)
                fm = ModuleLoadStateChanged.createMatcher(module._instance.target, ModuleLoadStateChanged.FAILED)
                yield (sm, fm)
                if self.matcher is sm:
                    pass
                else:
                    raise ModuleLoadException('Module load failed for %r' % (module,))
            self._logger.info('Loading module %r completed, module state is %r', module, module._instance.state)
            if module._instance.state == ModuleLoadStateChanged.FAILED:
                raise ModuleLoadException('Module load failed for %r' % (module,))
    def unloadmodule(self, module, ignoreDependencies = False):
        '''
        Unload a module class. Need delegate to call from other containers.
        '''
        self._logger.debug('Try to unload module %r', module)
        if hasattr(module, '_instance'):
            self._logger.debug('Module %r is loaded, module state is %r', module, module._instance.state)
            inst = module._instance
            if inst.state == ModuleLoadStateChanged.LOADING or inst.state == ModuleLoadStateChanged.LOADED:
                # Wait for loading
                # Wait for succeeded or failed
                sm = ModuleLoadStateChanged.createMatcher(module._instance.target, ModuleLoadStateChanged.SUCCEEDED)
                fm = ModuleLoadStateChanged.createMatcher(module._instance.target, ModuleLoadStateChanged.FAILED)
                yield (sm, fm)
            elif inst.state == ModuleLoadStateChanged.UNLOADING or inst.state == ModuleLoadStateChanged.UNLOADED:
                um = ModuleLoadStateChanged.createMatcher(module, ModuleLoadStateChanged.UNLOADED)
                yield (um,)
        if hasattr(module, '_instance') and (module._instance.state == ModuleLoadStateChanged.SUCCEEDED or
                                             module._instance.state == ModuleLoadStateChanged.FAILED):
            self._logger.info('Unloading module %r', module)
            inst = module._instance
            # Change state to unloading to prevent more dependencies
            for m in inst.changestate(ModuleLoadStateChanged.UNLOADING, self):
                yield m
            if not ignoreDependencies:
                deps = [d for d in inst.dependedBy if hasattr(d, '_instance') and d._instance.state != ModuleLoadStateChanged.UNLOADED]
                ums = [ModuleLoadStateChanged.createMatcher(d, ModuleLoadStateChanged.UNLOADED) for d in deps]
                for d in deps:
                    self.subroutine(self.unloadmodule(d), False)
                while ums:
                    yield ums
                    ums.remove(self.matcher)
            for m in inst.unload(self):
                yield m
            del self.activeModules[inst.getServiceName()]
            self._logger.info('Module %r is unloaded', module)
            if not ignoreDependencies:
                for d in module.depends:
                    if hasattr(d, '_instance') and module in d._instance.dependedBy:
                        self._removeDepend(module, d)
    def loadByPath(self, path):
        """
        Load a module by full path. If there are dependencies, they are also loaded.
        """
        try:
            p, module = findModule(path, True)
        except KeyError as exc:
            raise ModuleLoadException('Cannot load module ' + repr(path) + ': ' + str(exc) + 'is not defined in the package')
        except Exception as exc:
            raise ModuleLoadException('Cannot load module ' + repr(path) + ': ' + str(exc))
        if module is None:
            raise ModuleLoadException('Cannot find module: ' + repr(path))
        for m in self.loadmodule(module):
            yield m
    def unloadByPath(self, path):
        """
        Unload a module by full path. Dependencies are automatically unloaded if they are marked to be
        services.
        """
        p, module = findModule(path, False)
        if module is None:
            raise ModuleLoadException('Cannot find module: ' + repr(path))
        for m in self.unloadmodule(module):
            yield m
    def reloadModules(self, pathlist):
        """
        Reload modules with a full path in the pathlist
        """
        loadedModules = []
        failures = []
        for path in pathlist:
            p, module = findModule(path, False)
            if module is not None and hasattr(module, '_instance') and module._instance.state != ModuleLoadStateChanged.UNLOADED:
                loadedModules.append(module)
        # Unload all modules
        ums = [ModuleLoadStateChanged.createMatcher(m, ModuleLoadStateChanged.UNLOADED) for m in loadedModules]
        for m in loadedModules:
            # Only unload the module itself, not its dependencies, since we will restart the module soon enough
            self.subroutine(self.unloadmodule(m, True), False)
        while ums:
            yield tuple(ums)
            ums.remove(self.matcher)
        # Group modules by package
        grouped = {}
        for path in pathlist:
            dotpos = path.rfind('.')
            if dotpos == -1:
                raise ModuleLoadException('Must specify module with full path, including package name')
            package = path[:dotpos]
            classname = path[dotpos + 1:]
            mlist = grouped.setdefault(package, [])
            p, module = findModule(path, False)
            mlist.append((classname, module))
        for package, mlist in grouped.items():
            # Reload each package only once
            try:
                p = sys.modules[package]
                # Remove cache to ensure a clean import from source file
                removeCache(p)
                p = reload(p)
            except KeyError:
                try:
                    p = __import__(package, fromlist=[m[0] for m in mlist])
                except:
                    self._logger.warning('Failed to import a package: %r, resume others', package, exc_info = True)
                    failures.append('Failed to import: ' + package)
                    continue
            except:
                self._logger.warning('Failed to import a package: %r, resume others', package, exc_info = True)
                failures.append('Failed to import: ' + package)
                continue                
            for cn, module in mlist:
                try:
                    module2 = getattr(p, cn)
                except AttributeError:
                    self._logger.warning('Cannot find module %r in package %r, resume others', package, cn)
                    failures.append('Failed to import: ' + package + '.' + cn)
                    continue
                if module is not None and module is not module2:
                    # Update the references
                    try:
                        lpos = loadedModules.index(module)
                        loaded = True
                    except:
                        loaded = False
                    for d in module.depends:
                        # The new reference is automatically added on import, only remove the old reference
                        d.referencedBy.remove(module)
                        if loaded and hasattr(d, '_instance'):
                            try:
                                d._instance.dependedBy.remove(module)
                                d._instance.dependedBy.add(module2)
                            except ValueError:
                                pass
                    if hasattr(module, 'referencedBy'):
                        for d in module.referencedBy:
                            pos = d.depends.index(module)
                            d.depends[pos] = module2
                            if not hasattr(module2, 'referencedBy'):
                                module2.referencedBy = []
                            module2.referencedBy.append(d)
                    if loaded:
                        loadedModules[lpos] = module2
        # Start the uploaded modules
        for m in loadedModules:
            self.subroutine(self.loadmodule(m))
        if failures:
            raise ModuleLoadException('Following errors occurred during reloading, check log for more details:\n' + '\n'.join(failures))
    def getModuleByName(self, targetname):
        """
        Return the module instance for a target name.
        """
        if targetname == 'public':
            target = None
        elif not targetname not in self.activeModules:
            raise KeyError('Module %r not exists or is not loaded' % (targetname,))
        else:
            target = self.activeModules[targetname]
        return target

def callAPI(container, targetname, name, params = {}, timeout = 60.0):
    """
    Call module API `targetname/name` with parameters. The return value is stored at `container.retvalue`.
    
    :param targetname: module targetname. Usually the lower-cased name of the module class, or 'public' for
                       public APIs.
    
    :param name: method name
    
    :param params: module API parameters, should be a dictionary of `{parameter: value}`
    
    :param timeout: raise an exception if the API call is not returned for a long time
    """
    handle = object()
    apiEvent = ModuleAPICall(handle, targetname, name, params = params)
    for m in container.waitForSend(apiEvent):
        yield m
    replyMatcher = ModuleAPIReply.createMatcher(handle)
    for m in container.waitWithTimeout(timeout, replyMatcher):
        yield m
    if container.timeout:
        # Ignore the Event
        apiEvent.canignore = True
        container.scheduler.ignore(ModuleAPICall.createMatcher(handle))
        raise ModuleAPICallTimeoutException('API call timeout')
    else:
        container.retvalue = getAPIResult(container.event)

def batchCallAPI(container, apis, timeout = 60.0):
    apiHandles = [(object(), api) for api in apis]
    apiEvents = [ModuleAPICall(handle, targetname, name, params = params)
                 for handle, (targetname, name, params) in apiHandles]
    apiMatchers = [ModuleAPIReply.createMatcher(handle) for handle, _ in apiHandles]
    def process():
        for e in apiEvents:
            for m in container.waitForSend(e):
                yield m
    container.subroutine(process(), False)
    eventdict = {}
    def process2():
        while apiMatchers:
            yield tuple(apiMatchers)
            apiMatchers.remove(container.matcher)
            eventdict[container.event.handle] = container.event
    for m in container.executeWithTimeout(timeout, process2()):
        yield m
    for e in apiEvents:
        if e.handle not in eventdict:
            e.canignore = True
            container.scheduler.ignore(ModuleAPICall.createMatcher(e.handle))
    container.retvalue = [eventdict.get(handle, None) for handle, _ in apiHandles]


def getAPIResult(event):
    if hasattr(event, 'exception'):
        raise event.exception
    return event.result

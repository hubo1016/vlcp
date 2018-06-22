'''
Created on 2015/12/2

:author: hubo
'''
from vlcp.config import defaultconfig
from vlcp.server.module import Module, api, findModule
from vlcp.event.core import TimerEvent
from vlcp.event.runnable import RoutineContainer
from time import time
import os.path

@defaultconfig
class Manager(Module):
    '''
    Manage module loading/unloading. Optionally reload a module when modified.
    '''
    # Check files change with this interval.
    _default_checkinterval = 5
    # Automatically check the loaded module files, reload them if they are changed on the disk.
    # Notice that only the file contains the Module class is reloaded, other files will not be
    # reloaded automatically. You should reload them manually with *reloadmodules* API if necessary.
    _default_autoreload = False
    service = False
    def __init__(self, server):
        '''
        Constructor
        '''
        Module.__init__(self, server)
        self.apiroutine = RoutineContainer(self.scheduler)
        self.apiroutine.main = self._autoreload
        self._lastcheck = time()
        if self.autoreload:
            self.routines.append(self.apiroutine)
        self.createAPI(api(self.enableAutoReload),
                       api(self.activeModules),
                       api(self.reloadmodules, self.apiroutine),
                       api(self.loadmodule, self.apiroutine),
                       api(self.unloadmodule, self.apiroutine))
    def activeModules(self):
        "Return current loaded modules"
        return dict((k, v.getFullPath()) for k,v in self.server.moduleloader.activeModules.items())
    async def _autoreload(self):
        th = self.scheduler.setTimer(self.checkinterval, self.checkinterval)
        try:
            tm = TimerEvent.createMatcher(th)
            while True:
                await tm
                t = time()
                reloads = []
                loaded = self.activeModules().values()
                self._logger.debug('Checking loaded modules: %r', loaded)
                for k in loaded:
                    p, _ = findModule(k, False)
                    if not p or not hasattr(p, '__file__') or not p.__file__:
                        continue
                    if p.__file__.endswith('.pyc'):
                        source = p.__file__[:-1]
                    else:
                        source = p.__file__
                    try:
                        mtime = os.path.getmtime(source)
                        if mtime <= t and mtime > self._lastcheck:
                            reloads.append(k)
                    except Exception:
                        pass
                if reloads:
                    self._logger.warning('Auto reload following modules: %r', reloads)
                    try:
                        await self.reloadmodules(reloads)
                    except Exception:
                        self._logger.warning('Exception occurs on auto reload', exc_info=True)
                self._lastcheck = t
        finally:
            self.scheduler.cancelTimer(th)
    async def loadmodule(self, path):
        '''
        Load specified module
        
        :param path: module path (e.g. ``vlcp.service.connection.httpserver.HttpServer``)
        '''
        return await self.server.moduleloader.load_by_path(path)
        
    async def reloadmodules(self, pathlist):
        '''
        Reload specified modules.
        
        :param pathlist: list of module path
        '''
        return await self.server.moduleloader.reload_modules(pathlist)

    async def unloadmodule(self, path):
        '''
        Unload specified module
        
        :param path: module path (e.g. ``vlcp.service.connection.httpserver.HttpServer``)
        '''
        return await self.server.moduleloader.unload_by_path(path)

    def enableAutoReload(self, enabled = True):
        '''
        Enable or disable auto reload.
        
        :param enabled: enable if True, disable if False
        '''
        enabled_now = self.apiroutine in self.routines
        if enabled != enabled_now:
            if enabled:
                self.apiroutine.start()
                self.routines.append(self.apiroutine)
            else:
                self.apiroutine.terminate()
                self.routines.remove(self.apiroutine)
        return None

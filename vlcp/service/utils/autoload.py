'''
Created on 2016/5/27

:author: hubo
'''

from vlcp.server.module import Module, ModuleLoadStateChanged
from vlcp.config.config import defaultconfig
import pkgutil

@defaultconfig
class AutoLoad(Module):
    '''
    Auto load some modules from a package. Usually used to load network plugins.
    '''
    autosuccess = False
    # Auto load packages from some packages
    _default_autoloadpackages = ('vlcp.service.sdn.plugins',)
    def __init__(self, server):
        Module.__init__(self, server)

    async def load(self, container):
        await Module.load(self, container)
        loadmodules = []
        for p in self.autoloadpackages:
            try:
                def onerror(name):
                    self._logger.warning("Autoload package %r on package %r failed", name, p)
                pkg = __import__(p, fromlist = ('dummy',))
                for _, name, ispkg in pkgutil.walk_packages(pkg.__path__, p + '.', onerror):
                    if not ispkg:
                        try:
                            pymod = __import__(name, fromlist = ('dummy',))
                            for m in vars(pymod).values():
                                if isinstance(m, type) and issubclass(m, Module) and getattr(m, '__module__', '') == name:
                                    loadmodules.append(m)
                        except Exception:
                            self._logger.warning('Autoload module %r failed', name, exc_info = True)
            except Exception:
                self._logger.warning('Autoload package %r failed', p, exc_info = True)
        if loadmodules:
            await container.execute_all([self.server.moduleloader.loadmodule(m) for m in loadmodules])
        await self.changestate(ModuleLoadStateChanged.SUCCEEDED, container)

    async def unload(self, container, force=False):
        await Module.unload(self, container, force=force)

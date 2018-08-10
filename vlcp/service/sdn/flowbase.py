'''
Created on 2016/4/11

:author: hubo
'''
from vlcp.server.module import Module, api, call_api, ModuleLoadStateChanged
from vlcp.event.runnable import RoutineContainer
from vlcp.service.sdn.ofpmanager import TableAcquireUpdate
from vlcp.event.core import QuitException


class FlowBase(Module):
    # Table request should be list of (name, (ancester, ancester,...), pathname). For default path, pathname = ''
    _tablerequest = ()
    # Default binding to OpenFlow vHosts
    _default_vhostbind = [""]
    autosuccess = False

    def __init__(self, server):
        Module.__init__(self, server)
        self._tableacquire_routine = RoutineContainer(self.scheduler)
        self._tableacquire_routine.main = self._acquiretable
        self.routines.append(self._tableacquire_routine)
        self.createAPI(api(self.gettablerequest))

    def _gettableindex(self, name, vhost = ""):
        vs = [v for k,v in self._all_tables[vhost] if k == name]
        if not vs:
            raise KeyError(name)
        else:
            return vs[0]

    def _getnexttable(self, pathname, name = None, default = KeyError, vhost = ""):
        if pathname not in self._path_tables[vhost]:
            if default is KeyError:
                raise KeyError(pathname)
            else:
                return default
        else:
            pathtable = self._path_tables[vhost][pathname]
            for i in range(0, len(pathtable) - 1):
                if pathtable[i][0] == name:
                    return pathtable[i+1][1]
            if default is KeyError:
                raise KeyError(name)
            else:
                return default

    def table_acquired(self):
        pass

    async def _acquiretable(self):
        try:
            if not self._tablerequest:
                return
            def update_table(event):
                self._all_tables = dict((v,r[0]) for v,r in event.result.items())
                self._path_tables = dict((v, r[1]) for v,r in event.result.items())
                self.table_acquired()
            await call_api(self._tableacquire_routine, 'openflowmanager', 'acquiretable', {'modulename': self.getServiceName()})
            table_update = TableAcquireUpdate.createMatcher()
            try:
                while True:
                    ev = await table_update
                    if hasattr(ev, 'exception'):
                        raise ev.exception
                    elif not ev.result:
                        continue
                    else:
                        update_table(ev)
                        break
            except Exception as exc:
                await self.changestate(ModuleLoadStateChanged.FAILED, self._tableacquire_routine)
                raise exc
            else:
                await self.changestate(ModuleLoadStateChanged.SUCCEEDED, self._tableacquire_routine)
            while True:
                ev = await table_update
                if hasattr(ev, 'exception'):
                    # Ignore a failed table acquire
                    continue
                elif not ev.result:
                    await call_api(self._tableacquire_routine, 'openflowmanager', 'acquiretable', {'modulename': self.getServiceName()})
                else:
                    update_table(ev)
        finally:
            async def unacquire():
                try:
                    await call_api(self._tableacquire_routine, 'openflowmanager', 'unacquiretable', {'modulename': self.getServiceName()})
                except QuitException:
                    pass
            self._tableacquire_routine.subroutine(unacquire(), False)

    def gettablerequest(self):
        "Table requirement for this module"
        return (self._tablerequest, self.vhostbind)

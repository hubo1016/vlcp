'''
Created on 2016/4/11

:author: hubo
'''
from vlcp.server.module import Module, api, callAPI, ModuleLoadStateChanged
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
    def _acquiretable(self):
        try:
            if not self._tablerequest:
                return
            def update_table():
                self._all_tables = dict((v,r[0]) for v,r in self._tableacquire_routine.event.result.items())
                self._path_tables = dict((v, r[1]) for v,r in self._tableacquire_routine.event.result.items())
                self.table_acquired()
            for m in callAPI(self._tableacquire_routine, 'openflowmanager', 'acquiretable', {'modulename': self.getServiceName()}):
                yield m
            table_update = TableAcquireUpdate.createMatcher()
            try:
                while True:
                    yield (table_update,)
                    if hasattr(self._tableacquire_routine.event, 'exception'):
                        raise self._tableacquire_routine.event.exception
                    elif not self._tableacquire_routine.event.result:
                        continue
                    else:
                        update_table()
                        break
            except Exception as exc:
                for m in self.changestate(ModuleLoadStateChanged.FAILED, self._tableacquire_routine):
                    yield m
                raise exc
            else:
                for m in self.changestate(ModuleLoadStateChanged.SUCCEEDED, self._tableacquire_routine):
                    yield m
            while True:
                yield (table_update,)
                if self._tableacquire_routine.matcher is table_update:
                    if hasattr(self._tableacquire_routine.event, 'exception'):
                        # Ignore a failed table acquire
                        continue
                    elif not self._tableacquire_routine.event.result:
                        for m in callAPI(self._tableacquire_routine, 'openflowmanager', 'acquiretable', {'modulename': self.getServiceName()}):
                            yield m
                    else:
                        update_table()
        finally:
            def unacquire():
                try:
                    for m in callAPI(self._tableacquire_routine, 'openflowmanager', 'unacquiretable', {'modulename': self.getServiceName()}):
                        yield m
                except QuitException:
                    pass
            self._tableacquire_routine.subroutine(unacquire(), False)
    def gettablerequest(self):
        "Table requirement for this module"
        return (self._tablerequest, self.vhostbind)

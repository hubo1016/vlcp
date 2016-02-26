'''
Created on 2016/2/19

:author: hubo
'''

from vlcp.config import defaultconfig
from vlcp.server.module import Module, api, depend, callAPI, ModuleNotification
from vlcp.event.runnable import RoutineContainer
from vlcp.service.connection import openflowserver
from vlcp.protocol.openflow import OpenflowConnectionStateEvent
from vlcp.event.connection import ConnectionResetException
import itertools

@defaultconfig
@depend(openflowserver.OpenflowServer)
class OpenflowManager(Module):
    '''
    Manage Openflow Connections
    '''
    service = True
    _default_vhostbind = None
    def __init__(self, server):
        Module.__init__(self, server)
        self.apiroutine = RoutineContainer(self.scheduler)
        self.apiroutine.main = self._manage_conns
        self.routines.append(self.apiroutine)
        self.managed_conns = {}
        self._synchronized = False
        self.createAPI(api(self.getconnections, self.apiroutine),
                       api(self.getconnection, self.apiroutine),
                       api(self.waitconnection, self.apiroutine),
                       api(self.getdatapathids, self.apiroutine),
                       api(self.getalldatapathids, self.apiroutine),
                       api(self.getallconnections, self.apiroutine)
                       )
    def _manage_existing(self):
        for m in callAPI(self.apiroutine, "openflowserver", "getconnections", {}):
            yield m
        vb = self.vhostbind
        for c in self.apiroutine.retvalue:
            if vb is None or c.protocol.vhost in vb:
                conns = self.managed_conns.setdefault((c.protocol.vhost, c.openflow_datapathid), [])
                for i in range(0, len(conns)):
                    if conns[i].openflow_auxiliaryid == c.openflow_auxiliaryid:
                        del conns[i]
                        break
                conns.append(c)
        self._synchronized = True
        for m in self.apiroutine.waitForSend(ModuleNotification(self.getServiceName(), 'synchronized')):
            yield m
    def _wait_for_sync(self):
        if not self._synchronized:
            yield (ModuleNotification.createMatcher(self.getServiceName(), 'synchronized'),)
    def _manage_conns(self):
        vb = self.vhostbind
        self.apiroutine.subroutine(self._manage_existing(), False)
        try:
            if vb is not None:
                conn_up = OpenflowConnectionStateEvent.createMatcher(state = OpenflowConnectionStateEvent.CONNECTION_SETUP,
                                                                     _ismatch = lambda x: x.createby.vhost in vb)
                conn_down = OpenflowConnectionStateEvent.createMatcher(state = OpenflowConnectionStateEvent.CONNECTION_DOWN,
                                                                     _ismatch = lambda x: x.createby.vhost in vb)
            else:
                conn_up = OpenflowConnectionStateEvent.createMatcher(state = OpenflowConnectionStateEvent.CONNECTION_SETUP)
                conn_down = OpenflowConnectionStateEvent.createMatcher(state = OpenflowConnectionStateEvent.CONNECTION_DOWN)
            while True:
                yield (conn_up, conn_down)
                if self.apiroutine.matcher is conn_up:
                    e = self.apiroutine.event
                    conns = self.managed_conns.setdefault((e.createby.vhost, e.datapathid), [])
                    remove = []
                    for i in range(0, len(conns)):
                        if conns[i].openflow_auxiliaryid == e.auxiliaryid:
                            remove = [conns[i]]
                            del conns[i]
                            break
                    conns.append(e.connection)
                    self.scheduler.emergesend(ModuleNotification(self.getServiceName(), 'update', add = [e.connection], remove = remove))
                else:
                    e = self.apiroutine.event
                    conns = self.managed_conns.get((e.createby.vhost, e.datapathid))
                    remove = []
                    if conns is not None:
                        for i in range(0, len(conns)):
                            if conns[i] is e.connection:
                                remove.append(conns[i])
                                del conns[i]
                                break
                        if not conns:
                            del self.managed_conns[(e.createby.vhost, e.datapathid)]
                    if remove:
                        self.scheduler.emergesend(ModuleNotification(self.getServiceName(), 'update', add = [], remove = remove))
        finally:
            self.scheduler.emergesend(ModuleNotification(self.getServiceName(), 'unsynchronized'))
    def getconnections(self, datapathid, vhost = ''):
        "Return all connections of datapath"
        for m in self._wait_for_sync():
            yield m
        self.apiroutine.retvalue = list(self.managed_conns.get((vhost, datapathid), []))
    def getconnection(self, datapathid, auxiliaryid = 0, vhost = ''):
        "Get current connection of datapath"
        for m in self._wait_for_sync():
            yield m
        self.apiroutine.retvalue = self._getconnection(datapathid, auxiliaryid, vhost)
    def _getconnection(self, datapathid, auxiliaryid = 0, vhost = ''):
        conns = self.managed_conns.get((vhost, datapathid))
        if conns is None:
            return None
        else:
            for c in conns:
                if c.openflow_auxiliaryid == auxiliaryid:
                    return c
            return None
    def waitconnection(self, datapathid, auxiliaryid = 0, timeout = 30, vhost = ''):
        "Wait for a datapath connection"
        for m in self._wait_for_sync():
            yield m
        c = self._getconnection(datapathid, auxiliaryid, vhost)
        if c is None:
            for m in self.apiroutine.waitWithTimeout(timeout, 
                            OpenflowConnectionStateEvent.createMatcher(datapathid, auxiliaryid,
                                    OpenflowConnectionStateEvent.CONNECTION_SETUP,
                                    _ismatch = lambda x: x.createby.vhost == vhost)):
                yield m
            if self.apiroutine.timeout:
                raise ConnectionResetException('Datapath %016x is not connected' % datapathid)
            self.apiroutine.retvalue = self.apiroutine.event.connection
        else:
            self.apiroutine.retvalue = c
    def getdatapathids(self, vhost = ''):
        "Get All datapath IDs"
        for m in self._wait_for_sync():
            yield m
        self.apiroutine.retvalue = [k[1] for k in self.managed_conns.keys() if k[0] == vhost]
    def getalldatapathids(self):
        "Get all datapath IDs from any vhost. Return (vhost, datapathid) pair."
        for m in self._wait_for_sync():
            yield m
        self.apiroutine.retvalue = list(self.managed_conns.keys())
    def getallconnections(self, vhost = ''):
        "Get all connections from vhost. If vhost is None, return all connections from any host"
        for m in self._wait_for_sync():
            yield m
        if vhost is None:
            self.apiroutine.retvalue = list(itertools.chain(self.managed_conns.values()))
        else:
            self.apiroutine.retvalue = list(itertools.chain(v for k,v in self.managed_conns.items() if k[0] == vhost))
    
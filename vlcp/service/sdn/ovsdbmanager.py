'''
Created on 2016/2/19

:author: hubo
'''

from vlcp.config import defaultconfig
from vlcp.server.module import Module, api, depend, callAPI, ModuleNotification
from vlcp.event.runnable import RoutineContainer
from vlcp.service.connection import jsonrpcserver
from vlcp.protocol.jsonrpc import JsonRPCConnectionStateEvent,\
    JsonRPCProtocolException, JsonRPCErrorResultException,\
    JsonRPCNotificationEvent
from vlcp.event.connection import ConnectionResetException
from vlcp.event.event import Event, withIndices
from vlcp.utils import ovsdb

@withIndices('systemid', 'connection', 'connmark', 'vhost')
class OVSDBConnectionSetup(Event):
    pass

@withIndices('state', 'datapathid', 'systemid', 'name', 'connection', 'connmark', 'vhost')
class OVSDBBridgeSetup(Event):
    UP = 'up'
    DOWN = 'down'

@defaultconfig
@depend(jsonrpcserver.JsonRPCServer)
class OVSDBManager(Module):
    '''
    Manage Openflow Connections
    '''
    service = True
    _default_vhostbind = None
    _default_bridgenames = None
    def __init__(self, server):
        Module.__init__(self, server)
        self.apiroutine = RoutineContainer(self.scheduler)
        self.apiroutine.main = self._manage_conns
        self.routines.append(self.apiroutine)
        self.managed_conns = {}
        self.managed_systemids = {}
        self.managed_bridges = {}
        self.managed_routines = []
        self.createAPI(api(self.getconnection, self.apiroutine),
                       api(self.waitconnection, self.apiroutine),
                       api(self.getdatapathids, self.apiroutine),
                       api(self.getalldatapathids, self.apiroutine),
                       api(self.getallconnections, self.apiroutine),
                       api(self.getbridges, self.apiroutine),
                       api(self.getbridge, self.apiroutine),
                       api(self.getsystemids, self.apiroutine),
                       api(self.getallsystemids, self.apiroutine),
                       api(self.getconnectionbysystemid, self.apiroutine),
                       api(self.waitconnectionbysystemid, self.apiroutine)
                       )
        self._synchronized = False
    def _update_bridge(self, connection, protocol, bridge_uuid, vhost):
        try:
            method, params = ovsdb.transact('Open_vSwitch',
                                            ovsdb.wait('Bridge', [["_uuid", "==", ovsdb.uuid(bridge_uuid)]],
                                                        ["datapath_id"], [{"datapath_id": ovsdb.oset()}], False, 5),
                                            ovsdb.select('Bridge', [["_uuid", "==", ovsdb.uuid(bridge_uuid)]],
                                                                         ["datapath_id","name"]))
            for m in protocol.querywithreply(method, params, connection, self.apiroutine):
                yield m
            r = self.apiroutine.jsonrpc_result[0]
            if 'error' in r:
                raise JsonRPCErrorResultException('Error while acquiring datapath-id: ' + repr(r['error']))
            r = self.apiroutine.jsonrpc_result[1]
            if 'error' in r:
                raise JsonRPCErrorResultException('Error while acquiring datapath-id: ' + repr(r['error']))
            if r['rows']:
                r0 = r['rows'][0]
                name = r0['name']
                dpid = int(r0['datapath_id'], 16)
                if self.bridgenames is None or name in self.bridgenames:
                    self.managed_bridges[connection].append((vhost, dpid, name, bridge_uuid))
                    self.managed_conns[(vhost, dpid)] = connection
        except JsonRPCProtocolException:
            pass
    def _get_bridges(self, connection, protocol):
        try:
            vhost = protocol.vhost
            if not hasattr(connection, 'ovsdb_systemid'):
                method, params = ovsdb.transact('Open_vSwitch', ovsdb.select('Open_vSwitch', [], ['external_ids']))
                for m in protocol.querywithreply(method, params, connection, self.apiroutine):
                    yield m
                result = self.apiroutine.jsonrpc_result[0]
                system_id = ovsdb.omap_getvalue(result['rows'][0]['external_ids'], 'system-id')
                connection.ovsdb_systemid = system_id
            else:
                system_id = connection.ovsdb_systemid
            self.managed_systemids[(vhost, system_id)] = connection
            self.managed_bridges[connection] = []
            for m in self.apiroutine.waitForSend(OVSDBConnectionSetup(system_id, connection, connection.connmark, vhost)):
                yield m
            method, params = ovsdb.monitor('Open_vSwitch', 'ovsdb_manager_bridges_monitor', {'Open_vSwitch':ovsdb.monitor_request(['bridges'])})
            for m in protocol.querywithreply(method, params, connection, self.apiroutine):
                yield m
            if 'error' in self.apiroutine.jsonrpc_result:
                # The monitor is already set, cancel it first
                method, params = ovsdb.monitor_cancel('ovsdb_manager_bridges_monitor')
                for m in protocol.querywithreply(method, params, connection, self.apiroutine, False):
                    yield m
                method, params = ovsdb.monitor('Open_vSwitch', 'ovsdb_manager_bridges_monitor', {'Open_vSwitch':ovsdb.monitor_request(['bridges'], True, False, False, True)})
                for m in protocol.querywithreply(method, params, connection, self.apiroutine):
                    yield m
                if 'error' in self.apiroutine.jsonrpc_result:
                    raise JsonRPCErrorResultException('OVSDB request failed: ' + repr(self.apiroutine.jsonrpc_result))
            # Process inital bridges
            for v in self.apiroutine.jsonrpc_result['Open_vSwitch'].values():
                for _, buuid in v['new']['bridges'][1]:
                    self.apiroutine.subroutine(self._update_bridge(connection, protocol, buuid, vhost))
            # Wait for notify
            notification = JsonRPCNotificationEvent.createMatcher('update', connection, connection.connmark, _ismatch = lambda x: x.params[0] == 'ovsdb_manager_bridges_monitor')
            conn_down = protocol.statematcher(connection)
            while True:
                yield (conn_down, notification)
                if self.apiroutine.matcher is conn_down:
                    break
                else:
                    for v in self.apiroutine.event.params[1]['Open_vSwitch'].values():
                        if 'old' not in v:
                            old = set()
                        else:
                            old = set((oid for _, oid in v['old']['bridges'][1]))
                        if 'new' not in v:
                            new = set()
                        else:
                            new = set((oid for _, oid in v['new']['bridges'][1]))
                        for buuid in (new - old):
                            self.apiroutine.subroutine(self._update_bridge(connection, protocol, buuid, vhost))
                        for buuid in (old - new):
                            bridges = self.managed_bridges[connection]
                            for i in range(0, len(bridges)):
                                if buuid == bridges[i][3]:
                                    self.scheduler.emergesend(OVSDBBridgeSetup(OVSDBBridgeSetup.DOWN,
                                                                               bridges[i][1],
                                                                               system_id,
                                                                               bridges[i][2],
                                                                               connection,
                                                                               connection.connmark,
                                                                               vhost))
                                    del self.managed_conns[(vhost, bridges[i][1])]
                                    del bridges[i]
                                    break
        except JsonRPCProtocolException:
            pass
    def _manage_existing(self):
        for m in callAPI(self.apiroutine, "jsonrpcserver", "getconnections", {}):
            yield m
        vb = self.vhostbind
        for m in self.apiroutine.executeAll([self.apiroutine.subroutine(self._get_bridges(c, c.protocol),
                                               '_ovsdb_manager_get_bridges')
                                             for c in self.apiroutine.retvalue
                                             if vb is None or c.protocol.vhost in vb], retnames = ()):
            yield m
        self._synchronized = True
        for m in self.apiroutine.waitForSend(ModuleNotification(self.getServiceName(), 'synchronized')):
            yield m
    def _wait_for_sync(self):
        if not self._synchronized:
            yield (ModuleNotification.createMatcher(self.getServiceName(), 'synchronized'),)    
    def _manage_conns(self):
        try:
            self.apiroutine.subroutine(self._manage_existing())
            vb = self.vhostbind
            if vb is not None:
                conn_up = JsonRPCConnectionStateEvent.createMatcher(state = JsonRPCConnectionStateEvent.CONNECTION_UP,
                                                                     _ismatch = lambda x: x.createby.vhost in vb)
                conn_down = JsonRPCConnectionStateEvent.createMatcher(state = JsonRPCConnectionStateEvent.CONNECTION_DOWN,
                                                                     _ismatch = lambda x: x.createby.vhost in vb)
            else:
                conn_up = JsonRPCConnectionStateEvent.createMatcher(state = JsonRPCConnectionStateEvent.CONNECTION_UP)
                conn_down = JsonRPCConnectionStateEvent.createMatcher(state = JsonRPCConnectionStateEvent.CONNECTION_DOWN)
            while True:
                yield (conn_up, conn_down)
                if self.apiroutine.matcher is conn_up:
                    self.apiroutine.event.connection._ovsdb_manager_get_bridges = self.apiroutine.subroutine(self._get_bridges(self.apiroutine.event.connection, self.apiroutine.event.createby))
                else:
                    e = self.apiroutine.event
                    conn = e.connection
                    bridges = self.managed_bridges.get(conn)
                    if bridges is not None:
                        del self.managed_systemids[(e.createby.vhost, conn.ovsdb_systemid)]
                        del self.managed_bridges[conn]
                        for vhost, dpid, name, _ in bridges:
                            del self.managed_conns[(vhost, dpid)]
                            self.scheduler.emergesend(OVSDBBridgeSetup(OVSDBBridgeSetup.DOWN, dpid, conn.ovsdb_systemid, name, conn, conn.connmark, e.createby.vhost))
        finally:
            for c in self.managed_bridges.keys():
                c._ovsdb_manager_get_bridges.close()
                bridges = self.managed_bridges.get(c)
                if bridges is not None:
                    for vhost, dpid, name, _ in bridges:
                        del self.managed_conns[(vhost, dpid)]
                        self.scheduler.emergesend(OVSDBBridgeSetup(OVSDBBridgeSetup.DOWN, dpid, c.ovsdb_systemid, name, c, c.connmark, c.protocol.vhost))
    def getconnection(self, datapathid, vhost = ''):
        "Get current connection of datapath"
        for m in self._wait_for_sync():
            yield m
        self.apiroutine.retvalue = self.managed_conns.get((vhost, datapathid))
    def waitconnection(self, datapathid, timeout = 30, vhost = ''):
        "Wait for a datapath connection"
        for m in self.getconnection(datapathid, vhost):
            yield m
        c = self.apiroutine.retvalue
        if c is None:
            for m in self.apiroutine.waitWithTimeout(timeout, 
                            OVSDBBridgeSetup.createMatcher(
                                    state = OVSDBBridgeSetup.UP,
                                    datapathid = datapathid, vhost = vhost)):
                yield m
            if self.apiroutine.timeout:
                raise ConnectionResetException('Datapath is not connected')
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
            self.apiroutine.retvalue = list(self.managed_bridges.keys())
        else:
            self.apiroutine.retvalue = list(k for k in self.managed_bridges.keys() if k.protocol.vhost == vhost)
    def getbridges(self, connection):
        "Get all (dpid, name) pair on this connection"
        for m in self._wait_for_sync():
            yield m
        bridges = self.managed_bridges.get(connection)
        if bridges is not None:
            self.apiroutine.retvalue = [(dpid, name) for _, dpid, name, _ in bridges]
        else:
            self.apiroutine.retvalue = None
    def getbridge(self, connection, name):
        "Get datapath ID on this connection with specified name"
        for m in self._wait_for_sync():
            yield m
        bridges = self.managed_bridges.get(connection)
        if bridges is not None:
            for _, dpid, n, _ in bridges:
                if n == name:
                    self.apiroutine.retvalue = dpid
                    raise StopIteration
            self.apiroutine.retvalue = None
        else:
            self.apiroutine.retvalue = None
    def getsystemids(self, vhost = ''):
        "Get All system-ids"
        for m in self._wait_for_sync():
            yield m
        self.apiroutine.retvalue = [k[1] for k in self.managed_systemids.keys() if k[0] == vhost]
    def getallsystemids(self):
        "Get all system-ids from any vhost. Return (vhost, system-id) pair."
        for m in self._wait_for_sync():
            yield m
        self.apiroutine.retvalue = list(self.managed_systemids.keys())
    def getconnectionbysystemid(self, systemid, vhost = ''):
        for m in self._wait_for_sync():
            yield m
        self.apiroutine.retvalue = self.managed_systemids.get((vhost, systemid))
    def waitconnectionbysystemid(self, systemid, timeout = 30, vhost = ''):
        "Wait for a connection with specified system-id"
        for m in self.getconnectionbysystemid(systemid, vhost):
            yield m
        c = self.apiroutine.retvalue
        if c is None:
            for m in self.apiroutine.waitWithTimeout(timeout, 
                            OVSDBConnectionSetup.createMatcher(
                                    systemid, None, None, vhost)):
                yield m
            if self.apiroutine.timeout:
                raise ConnectionResetException('Datapath is not connected')
            self.apiroutine.retvalue = self.apiroutine.event.connection
        else:
            self.apiroutine.retvalue = c
        
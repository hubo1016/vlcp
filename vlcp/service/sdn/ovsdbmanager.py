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
from vlcp.event.connection import ConnectionResetException, ResolveRequestEvent,\
    ResolveResponseEvent
from vlcp.event.event import Event, withIndices
from vlcp.utils import ovsdb
import socket
from contextlib import closing

@withIndices('systemid', 'connection', 'connmark', 'vhost')
class OVSDBConnectionSetup(Event):
    pass

@withIndices('state', 'datapathid', 'systemid', 'name', 'connection', 'connmark', 'vhost', 'bridgeuuid')
class OVSDBBridgeSetup(Event):
    UP = 'up'
    DOWN = 'down'

class OVSDBBridgeNotAppearException(Exception):
    pass

def _get_endpoint(conn):
    raddr = getattr(conn, 'remoteaddr', None)
    if raddr:
        if isinstance(raddr, tuple):
            # Ignore port
            return raddr[0]
        else:
            # Unix socket
            return raddr
    else:
        return ''


@defaultconfig
@depend(jsonrpcserver.JsonRPCServer)
class OVSDBManager(Module):
    '''
    Manage Openflow Connections
    '''
    service = True
    # Bind to JsonRPCServer vHosts. If not None, should be a list of vHost names e.g. ``['']``
    _default_vhostbind = None
    # Only acquire information from bridges with this names
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
        self.endpoint_conns = {}
        self.createAPI(api(self.getconnection, self.apiroutine),
                       api(self.waitconnection, self.apiroutine),
                       api(self.getdatapathids, self.apiroutine),
                       api(self.getalldatapathids, self.apiroutine),
                       api(self.getallconnections, self.apiroutine),
                       api(self.getbridges, self.apiroutine),
                       api(self.getbridge, self.apiroutine),
                       api(self.getbridgebyuuid, self.apiroutine),
                       api(self.waitbridge, self.apiroutine),
                       api(self.waitbridgebyuuid, self.apiroutine),
                       api(self.getsystemids, self.apiroutine),
                       api(self.getallsystemids, self.apiroutine),
                       api(self.getconnectionbysystemid, self.apiroutine),
                       api(self.waitconnectionbysystemid, self.apiroutine),
                       api(self.getconnectionsbyendpoint, self.apiroutine),
                       api(self.getconnectionsbyendpointname, self.apiroutine),
                       api(self.getendpoints, self.apiroutine),
                       api(self.getallendpoints, self.apiroutine),
                       api(self.getallbridges, self.apiroutine),
                       api(self.getbridgeinfo, self.apiroutine),
                       api(self.waitbridgeinfo, self.apiroutine)
                       )
        self._synchronized = False
    def _update_bridge(self, connection, protocol, bridge_uuid, vhost):
        try:
            method, params = ovsdb.transact('Open_vSwitch',
                                            ovsdb.wait('Bridge', [["_uuid", "==", ovsdb.uuid(bridge_uuid)]],
                                                        ["datapath_id"], [{"datapath_id": ovsdb.oset()}], False, 5000),
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
                    for m in self.apiroutine.waitForSend(OVSDBBridgeSetup(OVSDBBridgeSetup.UP,
                                                               dpid,
                                                               connection.ovsdb_systemid,
                                                               name,
                                                               connection,
                                                               connection.connmark,
                                                               vhost,
                                                               bridge_uuid)):
                        yield m
        except JsonRPCProtocolException:
            pass
    def _get_bridges(self, connection, protocol):
        try:
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
                if (vhost, system_id) in self.managed_systemids:
                    oc = self.managed_systemids[(vhost, system_id)]
                    ep = _get_endpoint(oc)
                    econns = self.endpoint_conns.get((vhost, ep))
                    if econns:
                        try:
                            econns.remove(oc)
                        except ValueError:
                            pass
                    del self.managed_systemids[(vhost, system_id)]
                self.managed_systemids[(vhost, system_id)] = connection
                self.managed_bridges[connection] = []
                ep = _get_endpoint(connection)
                self.endpoint_conns.setdefault((vhost, ep), []).append(connection)
                method, params = ovsdb.monitor('Open_vSwitch', 'ovsdb_manager_bridges_monitor', {'Bridge':ovsdb.monitor_request(['name', 'datapath_id'])})
                try:
                    for m in protocol.querywithreply(method, params, connection, self.apiroutine):
                        yield m
                except JsonRPCErrorResultException:
                    # The monitor is already set, cancel it first
                    method, params = ovsdb.monitor_cancel('ovsdb_manager_bridges_monitor')
                    for m in protocol.querywithreply(method, params, connection, self.apiroutine, False):
                        yield m
                    method, params = ovsdb.monitor('Open_vSwitch', 'ovsdb_manager_bridges_monitor', {'Bridge':ovsdb.monitor_request(['name', 'datapath_id'])})
                    for m in protocol.querywithreply(method, params, connection, self.apiroutine):
                        yield m
            except Exception:
                for m in self.apiroutine.waitForSend(OVSDBConnectionSetup(system_id, connection, connection.connmark, vhost)):
                    yield m
                raise
            else:
                # Process initial bridges
                init_subprocesses = []
                if self.apiroutine.jsonrpc_result and 'Bridge' in self.apiroutine.jsonrpc_result:
                    init_subprocesses = [self._update_bridge(connection, protocol, buuid, vhost)
                                        for buuid in self.apiroutine.jsonrpc_result['Bridge'].keys()]
                def init_process():
                    try:
                        with closing(self.apiroutine.executeAll(init_subprocesses, retnames = ())) as g:
                            for m in g:
                                yield m
                    except Exception:
                        for m in self.apiroutine.waitForSend(OVSDBConnectionSetup(system_id, connection, connection.connmark, vhost)):
                            yield m
                        raise
                    else:
                        for m in self.apiroutine.waitForSend(OVSDBConnectionSetup(system_id, connection, connection.connmark, vhost)):
                            yield m
                self.apiroutine.subroutine(init_process())
            # Wait for notify
            notification = JsonRPCNotificationEvent.createMatcher('update', connection, connection.connmark, _ismatch = lambda x: x.params[0] == 'ovsdb_manager_bridges_monitor')
            conn_down = protocol.statematcher(connection)
            while True:
                yield (conn_down, notification)
                if self.apiroutine.matcher is conn_down:
                    break
                else:
                    for buuid, v in self.apiroutine.event.params[1]['Bridge'].items():
                        # If a bridge's name or datapath-id is changed, we remove this bridge and add it again
                        if 'old' in v:
                            # A bridge is deleted
                            bridges = self.managed_bridges[connection]
                            for i in range(0, len(bridges)):
                                if buuid == bridges[i][3]:
                                    self.scheduler.emergesend(OVSDBBridgeSetup(OVSDBBridgeSetup.DOWN,
                                                                               bridges[i][1],
                                                                               system_id,
                                                                               bridges[i][2],
                                                                               connection,
                                                                               connection.connmark,
                                                                               vhost,
                                                                               bridges[i][3],
                                                                               new_datapath_id =
                                                                                int(v['new']['datapath_id'], 16) if 'new' in v and 'datapath_id' in v['new']
                                                                                else None))
                                    del self.managed_conns[(vhost, bridges[i][1])]
                                    del bridges[i]
                                    break
                        if 'new' in v:
                            # A bridge is added
                            self.apiroutine.subroutine(self._update_bridge(connection, protocol, buuid, vhost))
        except JsonRPCProtocolException:
            pass
        finally:
            del connection._ovsdb_manager_get_bridges
    def _manage_existing(self):
        for m in callAPI(self.apiroutine, "jsonrpcserver", "getconnections", {}):
            yield m
        vb = self.vhostbind
        conns = self.apiroutine.retvalue
        for c in conns:
            if vb is None or c.protocol.vhost in vb:
                if not hasattr(c, '_ovsdb_manager_get_bridges'):
                    c._ovsdb_manager_get_bridges = self.apiroutine.subroutine(self._get_bridges(c, c.protocol))
        matchers = [OVSDBConnectionSetup.createMatcher(None, c, c.connmark) for c in conns
                    if vb is None or c.protocol.vhost in vb]
        for m in self.apiroutine.waitForAll(*matchers):
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
                    if not hasattr(self.apiroutine.event.connection, '_ovsdb_manager_get_bridges'):
                        self.apiroutine.event.connection._ovsdb_manager_get_bridges = self.apiroutine.subroutine(self._get_bridges(self.apiroutine.event.connection, self.apiroutine.event.createby))
                else:
                    e = self.apiroutine.event
                    conn = e.connection
                    bridges = self.managed_bridges.get(conn)
                    if bridges is not None:
                        del self.managed_systemids[(e.createby.vhost, conn.ovsdb_systemid)]
                        del self.managed_bridges[conn]
                        for vhost, dpid, name, buuid in bridges:
                            del self.managed_conns[(vhost, dpid)]
                            self.scheduler.emergesend(OVSDBBridgeSetup(OVSDBBridgeSetup.DOWN,
                                                                       dpid,
                                                                       conn.ovsdb_systemid,
                                                                       name,
                                                                       conn,
                                                                       conn.connmark,
                                                                       e.createby.vhost,
                                                                       buuid))
                        econns = self.endpoint_conns.get(_get_endpoint(conn))
                        if econns is not None:
                            try:
                                econns.remove(conn)
                            except ValueError:
                                pass
        finally:
            for c in self.managed_bridges.keys():
                if hasattr(c, '_ovsdb_manager_get_bridges'):
                    c._ovsdb_manager_get_bridges.close()
                bridges = self.managed_bridges.get(c)
                if bridges is not None:
                    for vhost, dpid, name, buuid in bridges:
                        del self.managed_conns[(vhost, dpid)]
                        self.scheduler.emergesend(OVSDBBridgeSetup(OVSDBBridgeSetup.DOWN,
                                                                   dpid, 
                                                                   c.ovsdb_systemid, 
                                                                   name, 
                                                                   c, 
                                                                   c.connmark, 
                                                                   c.protocol.vhost,
                                                                   buuid))
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
        "Get all datapath IDs from any vhost. Return ``(vhost, datapathid)`` pair."
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
        "Get all ``(dpid, name, _uuid)`` tuple on this connection"
        for m in self._wait_for_sync():
            yield m
        bridges = self.managed_bridges.get(connection)
        if bridges is not None:
            self.apiroutine.retvalue = [(dpid, name, buuid) for _, dpid, name, buuid in bridges]
        else:
            self.apiroutine.retvalue = None
    def getallbridges(self, vhost = None):
        "Get all ``(dpid, name, _uuid)`` tuple for all connections, optionally filtered by vhost"
        for m in self._wait_for_sync():
            yield m
        if vhost is not None:
            self.apiroutine.retvalue = [(dpid, name, buuid)
                                        for c, bridges in self.managed_bridges.items()
                                        if c.protocol.vhost == vhost
                                        for _, dpid, name, buuid in bridges]
        else:
            self.apiroutine.retvalue = [(dpid, name, buuid)
                                        for c, bridges in self.managed_bridges.items()
                                        for _, dpid, name, buuid in bridges]
    def getbridge(self, connection, name):
        "Get datapath ID on this connection with specified name"
        for m in self._wait_for_sync():
            yield m
        bridges = self.managed_bridges.get(connection)
        if bridges is not None:
            for _, dpid, n, _ in bridges:
                if n == name:
                    self.apiroutine.retvalue = dpid
                    return
            self.apiroutine.retvalue = None
        else:
            self.apiroutine.retvalue = None
    def waitbridge(self, connection, name, timeout = 30):
        "Wait for bridge with specified name appears and return the datapath-id"
        bnames = self.bridgenames
        if bnames is not None and name not in bnames:
            raise OVSDBBridgeNotAppearException('Bridge ' + repr(name) + ' does not appear: it is not in the selected bridge names')
        for m in self.getbridge(connection, name):
            yield m
        if self.apiroutine.retvalue is None:
            bridge_setup = OVSDBBridgeSetup.createMatcher(OVSDBBridgeSetup.UP,
                                                         None,
                                                         None,
                                                         name,
                                                         connection
                                                         )
            conn_down = JsonRPCConnectionStateEvent.createMatcher(JsonRPCConnectionStateEvent.CONNECTION_DOWN,
                                                                  connection,
                                                                  connection.connmark)
            for m in self.apiroutine.waitWithTimeout(timeout, bridge_setup, conn_down):
                yield m
            if self.apiroutine.timeout:
                raise OVSDBBridgeNotAppearException('Bridge ' + repr(name) + ' does not appear')
            elif self.apiroutine.matcher is conn_down:
                raise ConnectionResetException('Connection is down before bridge ' + repr(name) + ' appears')
            else:
                self.apiroutine.retvalue = self.apiroutine.event.datapathid
    def getbridgebyuuid(self, connection, uuid):
        "Get datapath ID of bridge on this connection with specified _uuid"
        for m in self._wait_for_sync():
            yield m
        bridges = self.managed_bridges.get(connection)
        if bridges is not None:
            for _, dpid, _, buuid in bridges:
                if buuid == uuid:
                    self.apiroutine.retvalue = dpid
                    return
            self.apiroutine.retvalue = None
        else:
            self.apiroutine.retvalue = None
    def waitbridgebyuuid(self, connection, uuid, timeout = 30):
        "Wait for bridge with specified _uuid appears and return the datapath-id"
        for m in self.getbridgebyuuid(connection, uuid):
            yield m
        if self.apiroutine.retvalue is None:
            bridge_setup = OVSDBBridgeSetup.createMatcher(state = OVSDBBridgeSetup.UP,
                                                         connection = connection,
                                                         bridgeuuid = uuid
                                                         )
            conn_down = JsonRPCConnectionStateEvent.createMatcher(JsonRPCConnectionStateEvent.CONNECTION_DOWN,
                                                                  connection,
                                                                  connection.connmark)
            for m in self.apiroutine.waitWithTimeout(timeout, bridge_setup, conn_down):
                yield m
            if self.apiroutine.timeout:
                raise OVSDBBridgeNotAppearException('Bridge ' + repr(uuid) + ' does not appear')
            elif self.apiroutine.matcher is conn_down:
                raise ConnectionResetException('Connection is down before bridge ' + repr(uuid) + ' appears')
            else:
                self.apiroutine.retvalue = self.apiroutine.event.datapathid
    def getsystemids(self, vhost = ''):
        "Get All system-ids"
        for m in self._wait_for_sync():
            yield m
        self.apiroutine.retvalue = [k[1] for k in self.managed_systemids.keys() if k[0] == vhost]
    def getallsystemids(self):
        "Get all system-ids from any vhost. Return ``(vhost, system-id)`` pair."
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
    def getconnectionsbyendpoint(self, endpoint, vhost = ''):
        "Get connection by endpoint address (IP, IPv6 or UNIX socket address)"
        for m in self._wait_for_sync():
            yield m
        self.apiroutine.retvalue = self.endpoint_conns.get((vhost, endpoint))
    def getconnectionsbyendpointname(self, name, vhost = '', timeout = 30):
        "Get connection by endpoint name (Domain name, IP or IPv6 address)"
        # Resolve the name
        if not name:
            endpoint = ''
            for m in self.getconnectionbyendpoint(endpoint, vhost):
                yield m
        else:
            request = (name, 0, socket.AF_UNSPEC, socket.SOCK_STREAM, socket.IPPROTO_TCP, socket.AI_ADDRCONFIG | socket.AI_V4MAPPED)
            # Resolve hostname
            for m in self.apiroutine.waitForSend(ResolveRequestEvent(request)):
                yield m
            for m in self.apiroutine.waitWithTimeout(timeout, ResolveResponseEvent.createMatcher(request)):
                yield m
            if self.apiroutine.timeout:
                # Resolve is only allowed through asynchronous resolver
                #try:
                #    self.addrinfo = socket.getaddrinfo(self.hostname, self.port, socket.AF_UNSPEC, socket.SOCK_DGRAM if self.udp else socket.SOCK_STREAM, socket.IPPROTO_UDP if self.udp else socket.IPPROTO_TCP, socket.AI_ADDRCONFIG|socket.AI_NUMERICHOST)
                #except:
                raise IOError('Resolve hostname timeout: ' + name)
            else:
                if hasattr(self.apiroutine.event, 'error'):
                    raise IOError('Cannot resolve hostname: ' + name)
                resp = self.apiroutine.event.response
                for r in resp:
                    raddr = r[4]
                    if isinstance(raddr, tuple):
                        # Ignore port
                        endpoint = raddr[0]
                    else:
                        # Unix socket? This should not happen, but in case...
                        endpoint = raddr
                    for m in self.getconnectionsbyendpoint(endpoint, vhost):
                        yield m
                    if self.apiroutine.retvalue is not None:
                        break
    def getendpoints(self, vhost = ''):
        "Get all endpoints for vhost"
        for m in self._wait_for_sync():
            yield m
        self.apiroutine.retvalue = [k[1] for k in self.endpoint_conns if k[0] == vhost]
    def getallendpoints(self):
        "Get all endpoints from any vhost. Return ``(vhost, endpoint)`` pairs."
        for m in self._wait_for_sync():
            yield m
        self.apiroutine.retvalue = list(self.endpoint_conns.keys())
    def getbridgeinfo(self, datapathid, vhost = ''):
        "Get ``(bridgename, systemid, bridge_uuid)`` tuple from bridge datapathid"
        for m in self.getconnection(datapathid, vhost):
            yield m
        if self.apiroutine.retvalue is not None:
            c = self.apiroutine.retvalue
            bridges = self.managed_bridges.get(c)
            if bridges is not None:
                for _, dpid, n, buuid in bridges:
                    if dpid == datapathid:
                        self.apiroutine.retvalue = (n, c.ovsdb_systemid, buuid)
                        return
                self.apiroutine.retvalue = None
            else:
                self.apiroutine.retvalue = None
    def waitbridgeinfo(self, datapathid, timeout = 30, vhost = ''):
        "Wait for bridge with datapathid, and return ``(bridgename, systemid, bridge_uuid)`` tuple"
        for m in self.getbridgeinfo(datapathid, vhost):
            yield m
        if self.apiroutine.retvalue is None:
            for m in self.apiroutine.waitWithTimeout(timeout,
                        OVSDBBridgeSetup.createMatcher(
                                    OVSDBBridgeSetup.UP, datapathid,
                                    None, None, None, None,
                                    vhost)):
                yield m
            if self.apiroutine.timeout:
                raise OVSDBBridgeNotAppearException('Bridge 0x%016x does not appear before timeout' % (datapathid,))
            e = self.apiroutine.event
            self.apiroutine.retvalue = (e.name, e.systemid, e.bridgeuuid)

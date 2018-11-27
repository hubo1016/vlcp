'''
Created on 2016/2/19

:author: hubo
'''

from vlcp.config import defaultconfig
from vlcp.server.module import Module, api, depend, call_api, ModuleNotification
from vlcp.event.runnable import RoutineContainer
from vlcp.service.connection import jsonrpcserver
from vlcp.protocol.jsonrpc import JsonRPCConnectionStateEvent,\
    JsonRPCProtocolException, JsonRPCErrorResultException,\
    JsonRPCNotificationEvent
from vlcp.event.connection import ConnectionResetException, ResolveRequestEvent,\
    ResolveResponseEvent
from vlcp.event.event import Event, withIndices, M_
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
                       api(self.waitanyconnection, self.apiroutine),
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
    async def _update_bridge(self, connection, protocol, bridge_uuid, vhost):
        try:
            method, params = ovsdb.transact('Open_vSwitch',
                                            ovsdb.wait('Bridge', [["_uuid", "==", ovsdb.uuid(bridge_uuid)]],
                                                        ["datapath_id"], [{"datapath_id": ovsdb.oset()}], False, 5000),
                                            ovsdb.select('Bridge', [["_uuid", "==", ovsdb.uuid(bridge_uuid)]],
                                                                         ["datapath_id","name"]))
            jsonrpc_result, _ = await protocol.querywithreply(method, params, connection, self.apiroutine)
            r = jsonrpc_result[0]
            if 'error' in r:
                raise JsonRPCErrorResultException('Error while acquiring datapath-id: ' + repr(r['error']))
            r = jsonrpc_result[1]
            if 'error' in r:
                raise JsonRPCErrorResultException('Error while acquiring datapath-id: ' + repr(r['error']))
            if r['rows']:
                r0 = r['rows'][0]
                name = r0['name']
                dpid = int(r0['datapath_id'], 16)
                if self.bridgenames is None or name in self.bridgenames:
                    self.managed_bridges[connection].append((vhost, dpid, name, bridge_uuid))
                    self.managed_conns[(vhost, dpid)] = connection
                    await self.apiroutine.wait_for_send(OVSDBBridgeSetup(OVSDBBridgeSetup.UP,
                                                               dpid,
                                                               connection.ovsdb_systemid,
                                                               name,
                                                               connection,
                                                               connection.connmark,
                                                               vhost,
                                                               bridge_uuid))
        except JsonRPCProtocolException:
            pass

    async def _get_bridges(self, connection, protocol):
        try:
            try:
                vhost = protocol.vhost
                if not hasattr(connection, 'ovsdb_systemid'):
                    method, params = ovsdb.transact('Open_vSwitch', ovsdb.select('Open_vSwitch', [], ['external_ids']))
                    jsonrpc_result, _ = await protocol.querywithreply(method, params, connection, self.apiroutine)
                    result = jsonrpc_result[0]
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
                    jsonrpc_result, _ = await protocol.querywithreply(method, params, connection, self.apiroutine)
                except JsonRPCErrorResultException:
                    # The monitor is already set, cancel it first
                    method, params = ovsdb.monitor_cancel('ovsdb_manager_bridges_monitor')
                    await protocol.querywithreply(method, params, connection, self.apiroutine, False)
                    method, params = ovsdb.monitor('Open_vSwitch', 'ovsdb_manager_bridges_monitor', {'Bridge':ovsdb.monitor_request(['name', 'datapath_id'])})
                    jsonrpc_result, _ = await protocol.querywithreply(method, params, connection, self.apiroutine)
            except Exception:
                await self.apiroutine.wait_for_send(OVSDBConnectionSetup(system_id, connection, connection.connmark, vhost))
                raise
            else:
                # Process initial bridges
                init_subprocesses = []
                if jsonrpc_result and 'Bridge' in jsonrpc_result:
                    init_subprocesses = [self._update_bridge(connection, protocol, buuid, vhost)
                                        for buuid in jsonrpc_result['Bridge'].keys()]
                async def init_process():
                    try:
                        await self.apiroutine.execute_all(init_subprocesses)
                    except Exception:
                        await self.apiroutine.wait_for_send(OVSDBConnectionSetup(system_id, connection, connection.connmark, vhost))
                        raise
                    else:
                        await self.apiroutine.waitForSend(OVSDBConnectionSetup(system_id, connection, connection.connmark, vhost))
                self.apiroutine.subroutine(init_process())
            # Wait for notify
            notification = JsonRPCNotificationEvent.createMatcher('update', connection, connection.connmark, _ismatch = lambda x: x.params[0] == 'ovsdb_manager_bridges_monitor')
            conn_down = protocol.statematcher(connection)
            while True:
                ev, m = await M_(conn_down, notification)
                if m is conn_down:
                    break
                else:
                    for buuid, v in ev.params[1]['Bridge'].items():
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

    async def _manage_existing(self):
        conns = await call_api(self.apiroutine, "jsonrpcserver", "getconnections", {})
        vb = self.vhostbind
        for c in conns:
            if vb is None or c.protocol.vhost in vb:
                if not hasattr(c, '_ovsdb_manager_get_bridges'):
                    c._ovsdb_manager_get_bridges = self.apiroutine.subroutine(self._get_bridges(c, c.protocol))
        matchers = [OVSDBConnectionSetup.createMatcher(None, c, c.connmark) for c in conns
                    if vb is None or c.protocol.vhost in vb]
        await self.apiroutine.wait_for_all(*matchers)
        self._synchronized = True
        await self.apiroutine.wait_for_send(ModuleNotification(self.getServiceName(), 'synchronized'))

    async def _wait_for_sync(self):
        if not self._synchronized:
            await ModuleNotification.createMatcher(self.getServiceName(), 'synchronized')
    
    async def _manage_conns(self):
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
                ev, m = await M_(conn_up, conn_down)
                if m is conn_up:
                    if not hasattr(ev.connection, '_ovsdb_manager_get_bridges'):
                        ev.connection._ovsdb_manager_get_bridges = self.apiroutine.subroutine(self._get_bridges(ev.connection, ev.createby))
                else:
                    conn = ev.connection
                    bridges = self.managed_bridges.get(conn)
                    if bridges is not None:
                        del self.managed_systemids[(ev.createby.vhost, conn.ovsdb_systemid)]
                        del self.managed_bridges[conn]
                        for vhost, dpid, name, buuid in bridges:
                            del self.managed_conns[(vhost, dpid)]
                            self.scheduler.emergesend(OVSDBBridgeSetup(OVSDBBridgeSetup.DOWN,
                                                                       dpid,
                                                                       conn.ovsdb_systemid,
                                                                       name,
                                                                       conn,
                                                                       conn.connmark,
                                                                       ev.createby.vhost,
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
    async def getconnection(self, datapathid, vhost = ''):
        "Get current connection of datapath"
        await self._wait_for_sync()
        return self.managed_conns.get((vhost, datapathid))

    async def waitconnection(self, datapathid, timeout = 30, vhost = ''):
        "Wait for a datapath connection"
        c = await self.getconnection(datapathid, vhost)
        if c is None:
            timeout_, ev, m = await self.apiroutine.wait_with_timeout(timeout,
                                            OVSDBBridgeSetup.createMatcher(
                                                    state = OVSDBBridgeSetup.UP,
                                                    datapathid = datapathid, vhost = vhost))
            if timeout_:
                raise ConnectionResetException('Datapath is not connected')
            return ev.connection
        else:
            return c

    async def getdatapathids(self, vhost = ''):
        "Get All datapath IDs"
        await self._wait_for_sync()
        return [k[1] for k in self.managed_conns.keys() if k[0] == vhost]

    async def getalldatapathids(self):
        "Get all datapath IDs from any vhost. Return ``(vhost, datapathid)`` pair."
        await self._wait_for_sync()
        return list(self.managed_conns.keys())

    async def getallconnections(self, vhost = ''):
        "Get all connections from vhost. If vhost is None, return all connections from any host"
        await self._wait_for_sync()
        if vhost is None:
            return list(self.managed_bridges.keys())
        else:
            return list(k for k in self.managed_bridges.keys() if k.protocol.vhost == vhost)

    async def getbridges(self, connection):
        "Get all ``(dpid, name, _uuid)`` tuple on this connection"
        await self._wait_for_sync()
        bridges = self.managed_bridges.get(connection)
        if bridges is not None:
            return [(dpid, name, buuid) for _, dpid, name, buuid in bridges]
        else:
            return None

    async def getallbridges(self, vhost = None):
        "Get all ``(dpid, name, _uuid)`` tuple for all connections, optionally filtered by vhost"
        await self._wait_for_sync()
        if vhost is not None:
            return [(dpid, name, buuid)
                    for c, bridges in self.managed_bridges.items()
                    if c.protocol.vhost == vhost
                    for _, dpid, name, buuid in bridges]
        else:
            return [(dpid, name, buuid)
                    for c, bridges in self.managed_bridges.items()
                    for _, dpid, name, buuid in bridges]

    async def getbridge(self, connection, name):
        "Get datapath ID on this connection with specified name"
        await self._wait_for_sync()
        bridges = self.managed_bridges.get(connection)
        if bridges is not None:
            for _, dpid, n, _ in bridges:
                if n == name:
                    return dpid
            return None
        else:
            return None

    async def waitbridge(self, connection, name, timeout = 30):
        "Wait for bridge with specified name appears and return the datapath-id"
        bnames = self.bridgenames
        if bnames is not None and name not in bnames:
            raise OVSDBBridgeNotAppearException('Bridge ' + repr(name) + ' does not appear: it is not in the selected bridge names')
        dpid = await self.getbridge(connection, name)
        if dpid is None:
            bridge_setup = OVSDBBridgeSetup.createMatcher(OVSDBBridgeSetup.UP,
                                                         None,
                                                         None,
                                                         name,
                                                         connection
                                                         )
            conn_down = JsonRPCConnectionStateEvent.createMatcher(JsonRPCConnectionStateEvent.CONNECTION_DOWN,
                                                                  connection,
                                                                  connection.connmark)
            timeout_, ev, m = await self.apiroutine.wait_with_timeout(timeout, bridge_setup, conn_down)
            if timeout_:
                raise OVSDBBridgeNotAppearException('Bridge ' + repr(name) + ' does not appear')
            elif m is conn_down:
                raise ConnectionResetException('Connection is down before bridge ' + repr(name) + ' appears')
            else:
                return ev.datapathid
        else:
            return dpid

    async def getbridgebyuuid(self, connection, uuid):
        "Get datapath ID of bridge on this connection with specified _uuid"
        await self._wait_for_sync()
        bridges = self.managed_bridges.get(connection)
        if bridges is not None:
            for _, dpid, _, buuid in bridges:
                if buuid == uuid:
                    return dpid
            return None
        else:
            return None

    async def waitbridgebyuuid(self, connection, uuid, timeout = 30):
        "Wait for bridge with specified _uuid appears and return the datapath-id"
        dpid = await self.getbridgebyuuid(connection, uuid)
        if dpid is None:
            bridge_setup = OVSDBBridgeSetup.createMatcher(state = OVSDBBridgeSetup.UP,
                                                         connection = connection,
                                                         bridgeuuid = uuid
                                                         )
            conn_down = JsonRPCConnectionStateEvent.createMatcher(JsonRPCConnectionStateEvent.CONNECTION_DOWN,
                                                                  connection,
                                                                  connection.connmark)
            timeout_, ev, m = await self.apiroutine.wait_with_timeout(timeout, bridge_setup, conn_down)
            if timeout_:
                raise OVSDBBridgeNotAppearException('Bridge ' + repr(uuid) + ' does not appear')
            elif m is conn_down:
                raise ConnectionResetException('Connection is down before bridge ' + repr(uuid) + ' appears')
            else:
                return ev.datapathid
        else:
            return dpid

    async def getsystemids(self, vhost = ''):
        "Get All system-ids"
        await self._wait_for_sync()
        return [k[1] for k in self.managed_systemids.keys() if k[0] == vhost]

    async def getallsystemids(self):
        "Get all system-ids from any vhost. Return ``(vhost, system-id)`` pair."
        await self._wait_for_sync()
        return list(self.managed_systemids.keys())

    async def getconnectionbysystemid(self, systemid, vhost = ''):
        await self._wait_for_sync()
        return self.managed_systemids.get((vhost, systemid))

    async def waitconnectionbysystemid(self, systemid, timeout = 30, vhost = ''):
        "Wait for a connection with specified system-id"
        c = await self.getconnectionbysystemid(systemid, vhost)
        if c is None:
            timeout_, ev, _ = await self.apiroutine.wait_with_timeout(
                                            timeout, 
                                            OVSDBConnectionSetup.createMatcher(
                                                    systemid, None, None, vhost))
            if timeout_:
                raise ConnectionResetException('Datapath is not connected')
            return ev.connection
        else:
            return c

    async def getconnectionsbyendpoint(self, endpoint, vhost = ''):
        "Get connection by endpoint address (IP, IPv6 or UNIX socket address)"
        await self._wait_for_sync()
        return self.endpoint_conns.get((vhost, endpoint))

    async def getconnectionsbyendpointname(self, name, vhost = '', timeout = 30):
        "Get connection by endpoint name (Domain name, IP or IPv6 address)"
        # Resolve the name
        if not name:
            endpoint = ''
            return await self.getconnectionbyendpoint(endpoint, vhost)
        else:
            request = (name, 0, socket.AF_UNSPEC, socket.SOCK_STREAM, socket.IPPROTO_TCP, socket.AI_ADDRCONFIG | socket.AI_V4MAPPED)
            # Resolve hostname
            await self.apiroutine.wait_for_send(ResolveRequestEvent(request))
            timeout_, ev, _ = await self.apiroutine.wait_with_timeout(timeout, ResolveResponseEvent.createMatcher(request))
            if timeout_:
                # Resolve is only allowed through asynchronous resolver
                #try:
                #    self.addrinfo = socket.getaddrinfo(self.hostname, self.port, socket.AF_UNSPEC, socket.SOCK_DGRAM if self.udp else socket.SOCK_STREAM, socket.IPPROTO_UDP if self.udp else socket.IPPROTO_TCP, socket.AI_ADDRCONFIG|socket.AI_NUMERICHOST)
                #except:
                raise IOError('Resolve hostname timeout: ' + name)
            else:
                if hasattr(ev, 'error'):
                    raise IOError('Cannot resolve hostname: ' + name)
                resp = ev.response
                for r in resp:
                    raddr = r[4]
                    if isinstance(raddr, tuple):
                        # Ignore port
                        endpoint = raddr[0]
                    else:
                        # Unix socket? This should not happen, but in case...
                        endpoint = raddr
                    r = await self.getconnectionsbyendpoint(endpoint, vhost)
                    if r is not None:
                        return r

    async def getendpoints(self, vhost = ''):
        "Get all endpoints for vhost"
        await self._wait_for_sync()
        return [k[1] for k in self.endpoint_conns if k[0] == vhost]

    async def getallendpoints(self):
        "Get all endpoints from any vhost. Return ``(vhost, endpoint)`` pairs."
        await self._wait_for_sync()
        return list(self.endpoint_conns.keys())

    async def getbridgeinfo(self, datapathid, vhost = ''):
        "Get ``(bridgename, systemid, bridge_uuid)`` tuple from bridge datapathid"
        c = await self.getconnection(datapathid, vhost)
        if c is not None:
            bridges = self.managed_bridges.get(c)
            if bridges is not None:
                for _, dpid, n, buuid in bridges:
                    if dpid == datapathid:
                        return (n, c.ovsdb_systemid, buuid)
                return None
            else:
                return None
        else:
            return None

    async def waitbridgeinfo(self, datapathid, timeout = 30, vhost = ''):
        "Wait for bridge with datapathid, and return ``(bridgename, systemid, bridge_uuid)`` tuple"
        bridge = await self.getbridgeinfo(datapathid, vhost)
        if bridge is None:
            timeout_, ev, m = await self.apiroutine.wait_with_timeout(
                                            timeout,
                                            OVSDBBridgeSetup.createMatcher(
                                                        OVSDBBridgeSetup.UP, datapathid,
                                                        None, None, None, None,
                                                        vhost))
            if timeout_:
                raise OVSDBBridgeNotAppearException('Bridge 0x%016x does not appear before timeout' % (datapathid,))
            return (ev.name, ev.systemid, ev.bridgeuuid)
        else:
            return bridge
    
    async def waitanyconnection(self, timeout = 30, vhost = ''):
        "Wait for at lease one connection"
        time_start = self.apiroutine.scheduler.current_time
        while True:
            conns = await self.getallconnections(vhost)
            if not conns:
                time_left = max(timeout - (self.apiroutine.scheduler.current_time - time_start), 0)
                timeout_, _, _ = await self.apiroutine.wait_with_timeout(
                                                time_left,
                                                OVSDBBridgeSetup.createMatcher(
                                                        state = OVSDBBridgeSetup.UP,
                                                        vhost = vhost))
                if timeout_:
                    raise ConnectionResetException('Waiting for a connection timeouts')
            else:
                return conns

    
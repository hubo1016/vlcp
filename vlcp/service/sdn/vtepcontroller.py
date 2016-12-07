'''
Created on 2016/12/1

:author: hubo
'''
from vlcp.config import defaultconfig
from vlcp.server.module import Module, api, depend, ModuleNotification, callAPI
from vlcp.event.runnable import RoutineContainer
import vlcp.utils.ovsdb as ovsdb
import vlcp.service.connection.jsonrpcserver as jsonrpcserver
from vlcp.protocol.jsonrpc import JsonRPCErrorResultException,\
    JsonRPCConnectionStateEvent
from vlcp.event.event import Event, withIndices
from contextlib import closing

@withIndices('connection')
class VtepConnectionSynchronized(Event):
    pass


@withIndices('state', 'name', 'connection', 'connmark', 'vhost')
class VtepPhysicalSwitchStateChanged(Event):
    UP = 'up'
    DOWN = 'down'


@defaultconfig
@depend(jsonrpcserver.JsonRPCServer)
class VtepController(Module):
    _default_vhostbind = ['vtep']
    def __init__(self, server):
        Module.__init__(self, server)
        self.apiroutine = RoutineContainer(self.scheduler)
        self.apiroutine.main = self._main
        self.routines.append(self.apiroutine)
        self.createAPI(api(self.listphysicalports, self.apiroutine),
                       api(self.listphysicalswitchs, self.apiroutine),
                       api(self.updatelogicalswitch, self.apiroutine),
                       api(self.unbindlogicalswitch, self.apiroutine))
        self._physical_switchs = {}
        self._connection_ps = {}
        self._synchronized = False
        self._monitor_routines = set()
        
    def _wait_for_sync(self):
        if not self._synchronized:
            yield (ModuleNotification.createMatcher(self.getServiceName(), 'synchronized'),)
    
    def _monitor_conn(self, conn, notification = False):
        current_routine = self.apiroutine.currentroutine
        self._monitor_routines.add(current_routine)
        initialized = False
        def _process_result(result):
            if 'old' in result:
                # Removed or modified
                if 'name' in result['old']:
                    oldname = result['old']['name']
                else:
                    oldname = result['new']['name']
                try:
                    del self._physical_switchs[oldname]
                except KeyError:
                    pass
                try:
                    self._connection_ps[conn].discard(oldname)
                except KeyError:
                    pass
            if 'new' in result:
                # Created or modified
                nr = result['new']
                self._connection_ps.setdefault(conn, set()).add(nr['name'])
                tunnel_ips = ovsdb.getlist(nr['tunnel_ips'])
                if not tunnel_ips:
                    self._logger.warning('Physical switch %r does not have a configured tunnel IP', )
                    tunnelip = None
                else:
                    tunnelip = tunnel_ips[0]
                self._physical_switchs[nr['name']] = (conn, tunnelip)
                
        try:
            method, params = ovsdb.monitor(
                                'hardware_vtep',
                                'vlcp_vtepcontroller_physicalswitch_monitor',
                                {
                                    'Physical_Switch':
                                        ovsdb.monitor_request(['name', 'tunnel_ips'], True, True, True, True)
                                })
            protocol = conn.protocol
            try:
                for m in protocol.querywithreply(method, params, conn, self.apiroutine):
                    yield m
                result = self.apiroutine.jsonrpc_result
            except JsonRPCErrorResultException:
                # The monitor is already set, cancel it first
                method2, params2 = ovsdb.monitor_cancel('vlcp_vtepcontroller_physicalswitch_monitor')
                for m in protocol.querywithreply(method2, params2, conn, self.apiroutine, False):
                    yield m
                for m in protocol.querywithreply(method, params, conn, self.apiroutine):
                    yield m
                result = self.apiroutine.jsonrpc_result
            for r in result['Physical_Switch'].values():
                _process_result(r)
            initialized = True
            if notification:
                self.scheduler.emergesend(VtepConnectionSynchronized(conn))
            monitor_matcher = ovsdb.monitor_matcher(conn, 'vlcp_vtepcontroller_physicalswitch_monitor')
            conn_state = protocol.statematcher(conn)
            while True:
                yield (monitor_matcher, conn_state)
                if self.apiroutine.matcher is conn_state:
                    break
                else:
                    for r in self.apiroutine.event.params[1]['Physical_Switch'].values():
                        _process_result(r)
        except Exception:
            self._logger.warning('Initialize OVSDB vtep connection failed, maybe this is not a valid vtep endpoint',
                                 exc_info = True)
        finally:
            if notification and not initialized:
                self.scheduler.emergesend(VtepConnectionSynchronized(conn))
            if conn in self._connection_ps:
                ps_set = self._connection_ps[conn]
                for ps in ps_set:
                    if ps in self._physical_switchs:
                        del self._physical_switchs[ps]
                del self._connection_ps[conn]
            self._monitor_routines.discard(current_routine)

    def _manage_existing(self):
        for m in callAPI(self.apiroutine, "jsonrpcserver", "getconnections", {}):
            yield m
        vb = self.vhostbind
        conns = self.apiroutine.retvalue
        matchers = []
        for c in conns:
            if vb is None or c.protocol.vhost in vb:
                self.apiroutine.subroutine(self._monitor_conn(c, True))
                matchers.append(VtepConnectionSynchronized.createMatcher(c))
        for m in self.apiroutine.waitForAll(*matchers):
            yield m
        self._synchronized = True
        for m in self.apiroutine.waitForSend(ModuleNotification(self.getServiceName(), 'synchronized')):
            yield m
            
    def _main(self):
        self.apiroutine.subroutine(self._manage_existing())
        try:
            vb = self.vhostbind
            if vb is not None:
                conn_up = JsonRPCConnectionStateEvent.createMatcher(state = JsonRPCConnectionStateEvent.CONNECTION_UP,
                                                                     _ismatch = lambda x: x.createby.vhost in vb)
            else:
                conn_up = JsonRPCConnectionStateEvent.createMatcher(state = JsonRPCConnectionStateEvent.CONNECTION_UP)
            while True:
                yield (conn_up,)
                self.apiroutine.subroutine(self._monitor_conn(self.apiroutine.event.connection))
        finally:
            for r in list(self._monitor_routines):
                r.close()
        
    def listphysicalports(self, physicalswitch = None):
        '''
        Get physical ports list from this controller, grouped by physical switch name
        
        :param physicalswitch: physicalswitch name. Return all switches if is None.
        
        :return: dictionary: {physicalswitch: [physicalports]} e.g. {'ps1': ['port1', 'port2']}
        '''
        for m in self._wait_for_sync():
            yield m
        def _getports(conn):
            try:
                method, params = ovsdb.transact('hardware_vtep',
                                                ovsdb.select('Physical_Switch', [], ["name", "ports"]),
                                                ovsdb.select('Physical_Port', [], ["_uuid", "name"]))
                for m in conn.protocol.querywithreply(method, params, conn, self.apiroutine):
                    yield m
                result = self.apiroutine.jsonrpc_result
                if 'error' in result[0] or 'error' in result[1]:
                    raise JsonRPCErrorResultException('OVSDB request failed: ' + repr(result))
                switches = result[0]['rows']
                ports = result[1]['rows']
                ports_dict = dict((r['_uuid'][1], r['name']) for r in ports)
                self.apiroutine.retvalue = dict((switch['name'],
                                                 [ports_dict[p['_uuid'][1]] for p in ovsdb.getlist(switch['ports'])
                                                  if p['_uuid'][1] in ports_dict])
                                                for switch in switches)
            except Exception:
                self._logger.warning('Query OVSDB on %r failed with exception; will ignore this connection',
                                     conn, exc_info = True)
                self.apiroutine.retvalue = {}
        if physicalswitch is None:
            routines = [_getports(c) for c in self._connection_ps]
            with closing(self.apiroutine.executeAll(routines)) as g:
                for m in g:
                    yield m
            all_result = {}
            for (r,) in self.apiroutine.retvalue:
                all_result.update(r)
            self.apiroutine.retvalue = all_result
        else:
            if physicalswitch not in self._physical_switchs:
                self.apiroutine.retvalue = {}
            else:
                for m in _getports(self._physical_switchs[physicalswitch]):
                    yield m
                result = self.apiroutine.retvalue
                if physicalswitch in result:
                    self.apiroutine.retvalue = {physicalswitch: result[physicalswitch]}
                else:
                    self.apiroutine.retvalue = {}
    
    def listphysicalswitchs(self, physicalswitch = None):
        '''
        Get physical switch info
        
        :param physicalswitch: physicalswitch name. Return all switches if is None.
        
        :return: dictionary: {physicalswitch: {key: value}} keys include: management_ips,
        tunnel_ips, description, switch_fault_status
        '''
        for m in self._wait_for_sync():
            yield m
        def _getswitch(conn):
            try:
                method, params = ovsdb.transact('hardware_vtep',
                                                ovsdb.select('Physical_Switch', [],
                                                             ["name", "management_ips", "tunnel_ips",
                                                              "description", "switch_fault_status"]))
                for m in conn.protocol.querywithreply(method, params, conn, self.apiroutine):
                    yield m
                result = self.apiroutine.jsonrpc_result
                if 'error' in result[0]:
                    raise JsonRPCErrorResultException('OVSDB request failed: ' + repr(result))
                switches = result[0]['rows']
                self.apiroutine.retvalue = dict((s['name'],
                                                 {'management_ips': ovsdb.getlist(s['management_ips']),
                                                  'tunnel_ips': ovsdb.getlist(s['tunnel_ips']),
                                                  'description': s['description'],
                                                  'switch_fault_status': ovsdb.getlist(s['switch_fault_status'])})
                                                for s in switches)
            except Exception:
                self._logger.warning('Query OVSDB on %r failed with exception; will ignore this connection',
                                     conn, exc_info = True)
                self.apiroutine.retvalue = {}
        if physicalswitch is None:
            routines = [_getswitch(c) for c in self._connection_ps]
            with closing(self.apiroutine.executeAll(routines)) as g:
                for m in g:
                    yield m
            all_result = {}
            for (r,) in self.apiroutine.retvalue:
                all_result.update(r)
            self.apiroutine.retvalue = all_result
        else:
            if physicalswitch not in self._physical_switchs:
                self.apiroutine.retvalue = {}
            else:
                for m in _getswitch(self._physical_switchs[physicalswitch]):
                    yield m
                result = self.apiroutine.retvalue
                if physicalswitch in result:
                    self.apiroutine.retvalue = {physicalswitch: result[physicalswitch]}
                else:
                    self.apiroutine.retvalue = {}
        
    def updatelogicalswitch(self, physicalswitch, physicalport, vlanid, logicalnetwork, logicalports):
        '''
        Bind VLAN on physicalport to specified logical network, and update logical port vxlan info
        
        :param physicalswitch: physical switch name, should be the name in PhysicalSwitch table of OVSDB vtep database
        
        :param physicalport: physical port name, should be the name in OVSDB vtep database
        
        :param vlanid: the vlan tag used for this logicalswitch
        
        :param logicalnetwork: the logical network id, will also be the logical switch id
        
        :param logicalports: a list of logical port IDs. The VXLAN info of these ports will be updated.
        '''
        
    
    def unbindlogicalswitch(self, physicalswitch, physicalport, vlanid, logicalnetwork):
        '''
        Remove bind of a physical port
        
        :param physicalswitch: physical switch name, should be the name in PhysicalSwitch table of OVSDB vtep database
        
        :param physicalport: physical port name, should be the name in OVSDB vtep database
        
        :param vlanid: the vlan tag used for this logicalswitch
        
        :param logicalnetwork: the logical network id, will also be the logical switch id
        '''
        
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
from vlcp.event.connection import ConnectionResetException

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
    _default_recycleinterval = 300
    def __init__(self, server):
        Module.__init__(self, server)
        self.apiroutine = RoutineContainer(self.scheduler)
        self.apiroutine.main = self._main
        self.routines.append(self.apiroutine)
        self.createAPI(api(self.listphysicalports, self.apiroutine),
                       api(self.listphysicalswitches, self.apiroutine),
                       api(self.updatelogicalswitch, self.apiroutine),
                       api(self.unbindlogicalswitch, self.apiroutine),
                       api(self.unbindphysicalport, self.apiroutine))
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
        protocol = conn.protocol
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
                if 'new' not in result or 'name' in result['old']:
                    self.scheduler.emergesend(VtepPhysicalSwitchStateChanged(VtepPhysicalSwitchStateChanged.DOWN,
                                                                             oldname,
                                                                             conn,
                                                                             conn.connmark,
                                                                             protocol.vhost
                                                                             ))
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
                if 'old' not in result or 'name' in result['old']:
                    self.scheduler.emergesend(VtepPhysicalSwitchStateChanged(VtepPhysicalSwitchStateChanged.UP,
                                                                             nr['name'],
                                                                             conn,
                                                                             conn.connmark,
                                                                             protocol.vhost
                                                                             ))                
        try:
            method, params = ovsdb.monitor(
                                'hardware_vtep',
                                'vlcp_vtepcontroller_physicalswitch_monitor',
                                {
                                    'Physical_Switch':
                                        ovsdb.monitor_request(['name', 'tunnel_ips'], True, True, True, True)
                                })
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
                                                 [ports_dict[p[1]] for p in ovsdb.getlist(switch['ports'])
                                                  if p[1] in ports_dict])
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
    
    def listphysicalswitches(self, physicalswitch = None):
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
        for m in self._wait_for_sync():
            yield m
        vlanid = int(vlanid)
        if physicalswitch is None:
            raise ValueError('Physical switch cannot be None')
        # We may retry some times
        while True:
            if physicalswitch not in self._physical_switchs:
                for m in self.apiroutine.waitWithTimeout(5,
                                                         VtepPhysicalSwitchStateChanged.createMatcher(
                                                            VtepPhysicalSwitchStateChanged.UP,
                                                            physicalswitch)):
                    yield m
                if self.apiroutine.timeout or physicalswitch not in self._physical_switchs:
                    raise ValueError('Physical switch %r is not found' % (physicalswitch,))
            conn, tunnelip = self._physical_switchs[physicalswitch]
            protocol = conn.protocol
            # First use a select to validate the request
            try:
                method, params = ovsdb.transact('hardware_vtep',
                                                ovsdb.select('Physical_Switch',
                                                             [["name", "==", physicalswitch]],
                                                             ["_uuid", "name", "ports"]),
                                                ovsdb.select('Physical_Port',
                                                             [["name", "==", physicalport]],
                                                             ["_uuid", "name", "vlan_bindings"]),
                                                ovsdb.select('Logical_Switch',
                                                             [["name", "==", logicalnetwork]],
                                                             ["_uuid", "name"])
                                                )
                for m in protocol.querywithreply(method, params, conn, self.apiroutine):
                    yield m
                result = self.apiroutine.jsonrpc_result
                if 'rows' not in result[0]:
                    raise JsonRPCErrorResultException('select from Physical_Switch failed: ' + repr(result[0]))
                if 'rows' not in result[1]:
                    raise JsonRPCErrorResultException('select from Physical_Switch failed: ' + repr(result[1]))
                if 'rows' not in result[2]:
                    raise JsonRPCErrorResultException('select from Physical_Switch failed: ' + repr(result[2]))
                if not result[0]['rows']:
                    raise ValueError('Physical switch %r is not found, connection = %r' % (physicalswitch, conn))
                pswitch = result[0]['rows'][0]
                port_uuids = set(p[1] for p in ovsdb.getlist(pswitch['ports']))
                ports = [p for p in result[1]['rows'] if p['_uuid'][1] in port_uuids]
                if not ports:
                    raise ValueError('Physical port %r is not found in physical switch %r' % (physicalport, physicalswitch))
                port = ports[0]
                ls_list = result[2]['rows']
                if ls_list:
                    ls_uuid = ls_list[0]['_uuid'][1]
                    # Logical switch is already created
                    curr_network = ovsdb.omap_getvalue(port['vlan_bindings'], vlanid)
                    if curr_network is None:
                        # Not binded
                        binded = False
                        created = True
                    elif curr_network[1] == ls_uuid:
                        # Already binded
                        binded = True
                        created = True
                    else:
                        raise ValueError('VLAN tag %r on physical port %r (%r) already binds to other networks, uuid=%r' \
                                         % (vlanid, physicalport, physicalswitch, curr_network[1]))
                else:
                    # Logical switch is not created
                    binded = False
                    created = False
                    curr_network = ovsdb.omap_getvalue(port['vlan_bindings'], vlanid)
                    if curr_network is not None:
                        raise ValueError('VLAN tag %r on physical port %r (%r) already binds to other networks, uuid=%r' \
                                         % (vlanid, physicalport, physicalswitch, curr_network[1]))
                if binded:
                    break
                operations = []
                # First we must check that the situations are not changed
                operations.extend([ovsdb.wait("Physical_Switch",
                                              [["name", "==", physicalswitch]],
                                              ["_uuid"],
                                              [{"_uuid": pswitch['_uuid']}],
                                              timeout = 0),
                                   ovsdb.wait("Physical_Port",
                                              [["name", "==", physicalport]],
                                              ["_uuid"],
                                              [{"_uuid": port['_uuid']}],
                                              timeout = 0)])
                if created:
                    operations.append(ovsdb.wait("Logical_Switch",
                                                [["name", "==", logicalnetwork]],
                                                ["_uuid"],
                                                [{"_uuid": ovsdb.uuid(ls_uuid)}],
                                                timeout = 0))
                else:
                    operations.append(ovsdb.wait("Logical_Switch",
                                                [["name", "==", logicalnetwork]],
                                                ["_uuid"],
                                                [],
                                                timeout = 0))
                    # Create the new logical switch
                    operations.append(ovsdb.insert('Logical_Switch',
                                                   {"name": logicalnetwork},
                                                   "new_logicalnetwork"))
                operations.append(ovsdb.mutate('Physical_Port', [["_uuid", "==", port['_uuid']]],
                                               [["vlan_bindings","insert",
                                                    ovsdb.omap(
                                                        (vlanid, ovsdb.uuid(ls_uuid) if created
                                                                 else ovsdb.named_uuid('new_logicalnetwork'))
                                                    )]]))
                method, params = ovsdb.transact('hardware_vtep', *operations)
                for m in protocol.querywithreply(method, params, conn, self.apiroutine):
                    yield m
                result = self.apiroutine.jsonrpc_result
                if any(r for r in result[:3] if 'error' in r):
                    # Wait failed, the OVSDB is modified, retry
                    continue
                for i,r in enumerate(result[3:]):
                    if 'error' in r:
                        if i + 3 >= len(operations):
                            raise JsonRPCErrorResultException('Transact failed with error: %r' % (r['error'],))
                        else:
                            raise JsonRPCErrorResultException(('Transact failed with error: %r, '\
                                    'corresponding operation: %r') % (r['error'], operations[i + 3]))
                break
            except ConnectionResetException:
                for m in self.apiroutine.waitWithTimeout(1):
                    yield m
                continue
            except IOError:
                for m in self.apiroutine.waitWithTimeout(1):
                    yield m
                continue
        self.apiroutine.retvalue = None
        
    
    def unbindlogicalswitch(self, physicalswitch, physicalport, vlanid, logicalnetwork):
        '''
        Remove bind of a physical port
        
        :param physicalswitch: physical switch name, should be the name in PhysicalSwitch table of OVSDB vtep database
        
        :param physicalport: physical port name, should be the name in OVSDB vtep database
        
        :param vlanid: the vlan tag used for this logicalswitch
        
        :param logicalnetwork: the logical network id, will also be the logical switch id
        '''
        for m in self._wait_for_sync():
            yield m
        if physicalswitch is None:
            raise ValueError('Physical switch cannot be None')
        # We may retry some times
        while True:
            if physicalswitch not in self._physical_switchs:
                for m in self.apiroutine.waitWithTimeout(5,
                                                         VtepPhysicalSwitchStateChanged.createMatcher(
                                                            VtepPhysicalSwitchStateChanged.UP,
                                                            physicalswitch)):
                    yield m
                if self.apiroutine.timeout or physicalswitch not in self._physical_switchs:
                    raise ValueError('Physical switch %r is not found' % (physicalswitch,))
            conn, tunnelip = self._physical_switchs[physicalswitch]
            protocol = conn.protocol
            try:
                method, params = ovsdb.transact('hardware_vtep',
                                                ovsdb.select('Physical_Switch',
                                                             [["name", "==", physicalswitch]],
                                                             ["_uuid", "name", "ports"]),
                                                ovsdb.select('Physical_Port',
                                                             [["name", "==", physicalport]],
                                                             ["_uuid", "name", "vlan_bindings"]),
                                                ovsdb.select('Logical_Switch',
                                                             [["name", "==", logicalnetwork]],
                                                             ["_uuid", "name"])
                                                )
                for m in protocol.querywithreply(method, params, conn, self.apiroutine):
                    yield m
                result = self.apiroutine.jsonrpc_result
                if 'rows' not in result[0]:
                    raise JsonRPCErrorResultException('select from Physical_Switch failed: ' + repr(result[0]))
                if 'rows' not in result[1]:
                    raise JsonRPCErrorResultException('select from Physical_Switch failed: ' + repr(result[1]))
                if 'rows' not in result[2]:
                    raise JsonRPCErrorResultException('select from Physical_Switch failed: ' + repr(result[2]))
                if not result[0]['rows']:
                    raise ValueError('Physical switch %r is not found, connection = %r' % (physicalswitch, conn))
                pswitch = result[0]['rows'][0]
                port_uuids = set(p[1] for p in ovsdb.getlist(pswitch['ports']))
                ports = [p for p in result[1]['rows'] if p['_uuid'][1] in port_uuids]
                if not ports:
                    raise ValueError('Physical port %r is not found in physical switch %r' % (physicalport, physicalswitch))
                port = ports[0]
                ls_list = result[2]['rows']
                if not ls_list:
                    # Network not exists, might already be unbinded, ignore
                    break
                ls_uuid = ls_list[0]['_uuid'][1]
                curr_network = ovsdb.omap_getvalue(port['vlan_bindings'], vlanid)
                if curr_network[1] != ls_uuid:
                    # Not binded
                    break
                operations = []
                # First we must check that the situations are not changed
                operations.extend([ovsdb.wait("Physical_Switch",
                                              [["name", "==", physicalswitch]],
                                              ["_uuid"],
                                              [{"_uuid": pswitch['_uuid']}],
                                              timeout = 0),
                                   ovsdb.wait("Physical_Port",
                                              [["name", "==", physicalport],
                                               ["vlan_bindings", "includes", ovsdb.omap((vlanid, ovsdb.uuid(ls_uuid)))]],
                                              ["_uuid"],
                                              [{"_uuid": port['_uuid']}],
                                              timeout = 0),
                                   ovsdb.wait("Logical_Switch",
                                                [["name", "==", logicalnetwork]],
                                                ["_uuid"],
                                                [{"_uuid": ovsdb.uuid(ls_uuid)}],
                                                timeout = 0)])
                operations.append(ovsdb.mutate('Physical_Port', [["_uuid", "==", port['_uuid']]],
                                               [["vlan_bindings","delete",
                                                    ovsdb.omap(
                                                        (vlanid, ovsdb.uuid(ls_uuid))
                                                    )]]))
                # We do not delete the logical switch; it will be deleted by the recycling process if not used
                method, params = ovsdb.transact('hardware_vtep', *operations)
                for m in protocol.querywithreply(method, params, conn, self.apiroutine):
                    yield m
                result = self.apiroutine.jsonrpc_result
                if any(r for r in result[:3] if 'error' in r):
                    # Wait failed, the OVSDB is modified, retry
                    continue
                for i,r in enumerate(result[3:]):
                    if 'error' in r:
                        if i + 3 >= len(operations):
                            raise JsonRPCErrorResultException('Transact failed with error: %r' % (r['error'],))
                        else:
                            raise JsonRPCErrorResultException(('Transact failed with error: %r, '\
                                    'corresponding operation: %r') % (r['error'], operations[i + 3]))
                break
            except ConnectionResetException:
                for m in self.apiroutine.waitWithTimeout(1):
                    yield m
                continue
            except IOError:
                for m in self.apiroutine.waitWithTimeout(1):
                    yield m
                continue
        self.apiroutine.retvalue = None
    
    def unbindphysicalport(self, physicalswitch, physicalport):
        '''
        Remove all bindings for a physical port

        :param physicalswitch: physical switch name, should be the name in PhysicalSwitch table of OVSDB vtep database
        
        :param physicalport: physical port name, should be the name in OVSDB vtep database        
        '''
        for m in self._wait_for_sync():
            yield m
        if physicalswitch is None:
            raise ValueError('Physical switch cannot be None')
        # We may retry some times
        while True:
            if physicalswitch not in self._physical_switchs:
                for m in self.apiroutine.waitWithTimeout(5,
                                                         VtepPhysicalSwitchStateChanged.createMatcher(
                                                            VtepPhysicalSwitchStateChanged.UP,
                                                            physicalswitch)):
                    yield m
                if self.apiroutine.timeout or physicalswitch not in self._physical_switchs:
                    raise ValueError('Physical switch %r is not found' % (physicalswitch,))
            conn, tunnelip = self._physical_switchs[physicalswitch]
            protocol = conn.protocol
            try:
                method, params = ovsdb.transact('hardware_vtep',
                                                ovsdb.select('Physical_Switch',
                                                             [["name", "==", physicalswitch]],
                                                             ["_uuid", "name", "ports"]),
                                                ovsdb.select('Physical_Port',
                                                             [["name", "==", physicalport]],
                                                             ["_uuid", "name", "vlan_bindings"])
                                                )
                for m in protocol.querywithreply(method, params, conn, self.apiroutine):
                    yield m
                result = self.apiroutine.jsonrpc_result
                if 'rows' not in result[0]:
                    raise JsonRPCErrorResultException('select from Physical_Switch failed: ' + repr(result[0]))
                if 'rows' not in result[1]:
                    raise JsonRPCErrorResultException('select from Physical_Switch failed: ' + repr(result[1]))
                if not result[0]['rows']:
                    raise ValueError('Physical switch %r is not found, connection = %r' % (physicalswitch, conn))
                pswitch = result[0]['rows'][0]
                port_uuids = set(p[1] for p in ovsdb.getlist(pswitch['ports']))
                ports = [p for p in result[1]['rows'] if p['_uuid'][1] in port_uuids]
                if not ports:
                    raise ValueError('Physical port %r is not found in physical switch %r' % (physicalport, physicalswitch))
                port = ports[0]
                # First we must check that the situations are not changed
                operations = [ovsdb.wait("Physical_Switch",
                                              [["name", "==", physicalswitch]],
                                              ["_uuid"],
                                              [{"_uuid": pswitch['_uuid']}],
                                              timeout = 0),
                               ovsdb.wait("Physical_Port",
                                          [["name", "==", physicalport]],
                                          ["_uuid"],
                                          [{"_uuid": port['_uuid']}],
                                          timeout = 0),
                               ovsdb.update("Physical_Port",
                                            [["_uuid", "==", port["_uuid"]]],
                                            {"vlan_bindings": ovsdb.omap()})]
                # We do not delete the logical switch; it will be deleted by the recycling process if not used
                method, params = ovsdb.transact('hardware_vtep', *operations)
                for m in protocol.querywithreply(method, params, conn, self.apiroutine):
                    yield m
                result = self.apiroutine.jsonrpc_result
                if any(r for r in result[:2] if 'error' in r):
                    # Wait failed, the OVSDB is modified, retry
                    continue
                for i,r in enumerate(result[2:]):
                    if 'error' in r:
                        if i + 2 >= len(operations):
                            raise JsonRPCErrorResultException('Transact failed with error: %r' % (r['error'],))
                        else:
                            raise JsonRPCErrorResultException(('Transact failed with error: %r, '\
                                    'corresponding operation: %r') % (r['error'], operations[i + 2]))
                break
            except ConnectionResetException:
                for m in self.apiroutine.waitWithTimeout(1):
                    yield m
                continue
            except IOError:
                for m in self.apiroutine.waitWithTimeout(1):
                    yield m
                continue
        self.apiroutine.retvalue = None


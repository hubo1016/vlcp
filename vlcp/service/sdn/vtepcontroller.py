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
    JsonRPCConnectionStateEvent, JsonRPCNotificationEvent
from vlcp.event.event import Event, withIndices
from contextlib import closing
from vlcp.event.connection import ConnectionResetException
from vlcp.utils.vxlandiscover import lognet_vxlan_walker, update_vxlaninfo,\
    get_broadcast_ips
import vlcp.service.kvdb.objectdb as objectdb
from vlcp.utils.networkmodel import LogicalNetwork, VXLANEndpointSet,\
    LogicalNetworkMap, LogicalPortVXLANInfo, LogicalPort
from vlcp.utils.dataobject import multiwaitif
from vlcp.utils.ethernet import ip4_addr

@withIndices('connection')
class VtepConnectionSynchronized(Event):
    pass


@withIndices('state', 'name', 'connection', 'connmark', 'vhost')
class VtepPhysicalSwitchStateChanged(Event):
    UP = 'up'
    DOWN = 'down'


@withIndices('identifier')
class _DataUpdateEvent(Event):
    pass

def _check_transact_result(result, operations):
    if any('error' in r for r in result if r is not None):
        err_info = next((r['error'],
                     operations[i] if i < len(operations) else None)
                    for i,r in enumerate(result)
                    if r is not None and 'error' in r)
        raise JsonRPCErrorResultException('Error in OVSDB select operation: %r. Corresponding operation: %r' % err_info)

@defaultconfig
@depend(jsonrpcserver.JsonRPCServer, objectdb.ObjectDB)
class VtepController(Module):
    """
    Controll a physical switch which supports OVSDB hardware_vtep protocol.
    """
    # Default bind controller to OVSDB vHost
    _default_vhostbind = ['vtep']
    # Recycle unused logical switches from hardware_vtep OVSDB
    _default_recycleinterval = 300
    # Every logical network has a broadcast list which contains all related server nodes,
    # this is the refresh interval to keep current node in the list
    _default_refreshinterval = 3600
    # When a logical port is migrating from one node to another, there may be multiple
    # instances of the logical port on different or same nodes. This is the maximum time
    # allowed for the migration.
    _default_allowedmigrationtime = 120
    # When there are multiple controllers started for the same hardware_vtep OVSDB,
    # every instance will try to acquire this lock in the OVSDB, and the one who
    # acquired the lock will be the master. 
    _default_masterlock = "vlcp_vtepcontroller_masterlock"
    # Prepush unicast information for MACs on other nodes. If this is not set, the
    # physical switch should support and enable MAC-learning on VXLAN.
    _default_prepush = True
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
        self._requestid = 0
        
    def _wait_for_sync(self):
        if not self._synchronized:
            yield (ModuleNotification.createMatcher(self.getServiceName(), 'synchronized'),)
    
    def _monitor_conn(self, conn, notification = False):
        current_routine = self.apiroutine.currentroutine
        self._monitor_routines.add(current_routine)
        ls_monitor = self.apiroutine.subroutine(self._monitor_logicalswitches(conn))
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
            ls_monitor.close()
            if notification and not initialized:
                self.scheduler.emergesend(VtepConnectionSynchronized(conn))
            if conn in self._connection_ps:
                ps_set = self._connection_ps[conn]
                for ps in ps_set:
                    if ps in self._physical_switchs:
                        del self._physical_switchs[ps]
                del self._connection_ps[conn]
            self._monitor_routines.discard(current_routine)

    def _monitor_logicalswitches(self, conn):
        protocol = conn.protocol
        lockid = self.masterlock
        # There may be multiple vtep controllers running, only the one which successfully acquired the
        # master lock should update the ObjectDB
        method, params = ovsdb.lock(lockid)
        def _unlock():
            method, params = ovsdb.unlock(lockid)
            if conn.connected:
                try:
                    for m in protocol.querywithreply(method, params, conn, self.apiroutine):
                        yield m
                except Exception:
                    # Any result is acceptable, including: error result; connection down; etc.
                    # We don't report an exception that we cannot do anything for it.
                    pass
        try:
            for m in protocol.querywithreply(method, params, conn, self.apiroutine):
                yield m
        except JsonRPCErrorResultException:
            # For some reason the lock is not released? let's try it out
            for m in _unlock():
                yield m
            for m in protocol.querywithreply(method, params, conn, self.apiroutine):
                yield m                
        result = self.apiroutine.jsonrpc_result
        switch_routines = {}
        try:
            if not result['locked']:
                # Wait for lock
                lock_notification = JsonRPCNotificationEvent.createMatcher(
                                            "locked",
                                            conn,
                                            conn.connmark,
                                            _ismatch = lambda x: x.params[0] == lockid)
                conn_matcher = protocol.statematcher(conn)
                yield (lock_notification, conn_matcher)
                if self.apiroutine.matcher is conn_matcher:
                    return
            # Now we acquired the lock, start monitoring the logical switches
            method, params = ovsdb.monitor(
                                'hardware_vtep',
                                'vlcp_vtepcontroller_logicalswitch_monitor',
                                {
                                    'Logical_Switch':
                                        ovsdb.monitor_request(['name'], True, True, True, False)
                                })
            try:
                for m in protocol.querywithreply(method, params, conn, self.apiroutine):
                    yield m
                result = self.apiroutine.jsonrpc_result
            except JsonRPCErrorResultException:
                # The monitor is already set, cancel it first
                method2, params2 = ovsdb.monitor_cancel('vlcp_vtepcontroller_logicalswitch_monitor')
                for m in protocol.querywithreply(method2, params2, conn, self.apiroutine, False):
                    yield m
                for m in protocol.querywithreply(method, params, conn, self.apiroutine):
                    yield m
                result = self.apiroutine.jsonrpc_result
            if 'Logical_Switch' in result:
                for k, r in result['Logical_Switch'].items():
                    switch_routines[k] = self.apiroutine.subroutine(self._update_logicalswitch_info(conn, r['new']['name'], k))
            monitor_matcher = ovsdb.monitor_matcher(conn, 'vlcp_vtepcontroller_logicalswitch_monitor')
            conn_state = protocol.statematcher(conn)
            while True:
                yield (monitor_matcher, conn_state)
                if self.apiroutine.matcher is conn_state:
                    break
                updates = self.apiroutine.event.params[1]
                if 'Logical_Switch' in updates:
                    for k,r in self.apiroutine.event.params[1]['Logical_Switch'].items():
                        if 'old' in r:
                            if k in switch_routines:
                                switch_routines[k].close()
                                del switch_routines[k]
                        else:
                            if k in switch_routines:
                                switch_routines[k].close()
                            switch_routines[k] = self.apiroutine.subroutine(self._update_logicalswitch_info(conn, r['new']['name'], k))
        finally:
            for r in switch_routines.values():
                r.close()
            self.apiroutine.subroutine(_unlock(), False)
    
    def _push_logicalswitch_endpoints(self, conn, logicalswitchid):
        while True:
            if conn in self._connection_ps:
                ps_list = [(name, self._physical_switchs[name][1])
                           for name in self._connection_ps[conn]]
                for name, tunnel_ip in ps_list:
                    with closing(update_vxlaninfo(
                                    self.apiroutine,
                                    {logicalswitchid: tunnel_ip},
                                    {},
                                    {},
                                    conn.protocol.vhost,
                                    name,
                                    'hardware_vtep',
                                    self.allowedmigrationtime,
                                    self.refreshinterval)) as g:
                        for m in g:
                            yield m
            for m in self.apiroutine.waitWithTimeout(self.refreshinterval):
                yield m
    
    def _poll_logicalswitch_endpoints(self, conn, logicalswitchid, lsuuid):
        protocol = conn.protocol
        walker = lognet_vxlan_walker(self.prepush)
        self._requestid += 1
        requestid = ('vtepcontroller', self._requestid)
        lognet_key = LogicalNetwork.default_key(logicalswitchid)
        endpointset_key = VXLANEndpointSet.default_key(logicalswitchid)
        lognetmap_key = LogicalNetworkMap.default_key(logicalswitchid)
        rewalk_keys = (lognet_key, endpointset_key, lognetmap_key)
        _update_set = set()
        _detect_update_routine = None
        identifier = object()
        def _detect_update(saved_result):
            while True:
                for m in multiwaitif(saved_result, self.apiroutine, lambda x,y: True, True):
                    yield m
                updated_values, _ = self.apiroutine.retvalue
                if not _update_set:
                    self.apiroutine.scheduler.emergesend(_DataUpdateEvent(identifier))
                _update_set.update(updated_values)
        update_matcher = _DataUpdateEvent.createMatcher(identifier)
        _savedkeys = ()
        _savedresult = ()
        # These are: set of network tunnelips; dictionary for mac: tunnelip; last_physical_switch
        _last_endpoints = [set(), {}, None]
        
        def _update_hardware_vtep(saved_result):
            if conn not in self._connection_ps or not self._connection_ps[conn]:
                _last_endpoints[:] = [set(), {}, None]
                return
            # There should be only one physical switch in this connection
            ps = next(iter(self._connection_ps[conn]))
            allobjs = [v for v in saved_result if v is not None and not v.isdeleted()]
            lognet_list = [o for o in allobjs if o.getkey() == lognet_key]
            if not lognet_list or not hasattr(lognet_list[0], 'vni'):
                # Logical network maybe deleted, or not a VXLAN network, do not need to update
                return
            vni = lognet_list[0].vni
            endpointset_list = [v for v in allobjs if v.getkey() == endpointset_key]
            local_ip = self._physical_switchs[ps][1]
            if endpointset_list:
                broadcast_ips = set(ip for ip,_ in get_broadcast_ips(endpointset_list[0], local_ip,
                                                  protocol.vhost,
                                                  ps,
                                                  'hardware_vtep'))
            else:
                broadcast_ips = set()
            if self.prepush:
                logport_tunnel_ip_dict = dict((o.id, o.endpoints[0]['tunnel_dst'])
                                              for o in allobjs
                                              if o.isinstance(LogicalPortVXLANInfo) \
                                              and hasattr(o, 'endpoints') and o.endpoints \
                                              and (o.endpoints[0]['vhost'],
                                                   o.endpoints[0]['systemid'],
                                                   o.endpoints[0]['bridge'])
                                                != (protocol.vhost, ps, 'hardware_vtep'))
                mac_ip_dict = dict((p.mac_address, logport_tunnel_ip_dict[p.id])
                                   for p in allobjs
                                   if p.isinstance(LogicalPort) \
                                   and hasattr(p, 'mac_address') \
                                   and p.id in logport_tunnel_ip_dict)
            else:
                mac_ip_dict = {}
            if _last_endpoints[2] == ps:
                # Update
                update = True
                broadcasts_updated = (broadcast_ips != _last_endpoints[0])
                add_ucasts = dict((m, mac_ip_dict[m])
                                  for m in mac_ip_dict
                                  if m not in _last_endpoints[1] or \
                                    _last_endpoints[1][m] != mac_ip_dict[m])
                remove_ucasts = dict((m, _last_endpoints[1][m])
                                     for m in _last_endpoints[1]
                                     if m not in mac_ip_dict)
            else:
                update = False
                broadcasts_updated = True
                remove_broadcasts = set()
                add_ucasts = mac_ip_dict
                remove_ucasts = {}
            using_ips = list(broadcast_ips.union(add_ucasts.values()))
            try:
                while True:
                    # Check and set
                    check_operates = [ovsdb.select('Logical_Switch',
                                                     [["_uuid", "==", ovsdb.uuid(lsuuid)],
                                                      ["name", "==", logicalswitchid]],
                                                     ["_uuid", "name", "tunnel_key"]),
                                      ovsdb.select('Mcast_Macs_Remote',
                                                     [["MAC", "==", "unknown-dst"],
                                                      ["logical_switch", "==", ovsdb.uuid(lsuuid)]],
                                                     ["_uuid", "locator_set"])]
                    # Check if each destination location exists
                    check_operates.extend(ovsdb.select('Physical_Locator',
                                                       [["dst_ip", "==", ip],
                                                        ["encapsulation_type", "==", "vxlan_over_ipv4"]],
                                                       ["_uuid",  "dst_ip"])
                                          for ip in using_ips)
                    method, params = ovsdb.transact('hardware_vtep',
                                                    *check_operates)
                    for m in protocol.querywithreply(method, params, conn, self.apiroutine):
                        yield m
                    result = self.apiroutine.jsonrpc_result
                    _check_transact_result(result, check_operates)
                    if not result[0]['rows']:
                        # Logical switch is removed
                        break
                    wait_operates = [ovsdb.wait('Logical_Switch',
                                               [["_uuid", "==", ovsdb.uuid(lsuuid)]],
                                               ["name"],
                                               [{"name": logicalswitchid}], True, 0)]
                    set_operates = []
                    # Check destinations
                    locator_uuid_dict = {}
                    if result[0]['rows'][0]['tunnel_key'] != vni:
                        set_operates.append(ovsdb.update('Logical_Switch',
                                               [["_uuid", "==", ovsdb.uuid(lsuuid)]],
                                               {"tunnel_key": vni}))
                    for r,ip in zip(result[2:len(check_operates)], using_ips):
                        if r['rows']:
                            locator = r['rows'][0]
                            # This locator is already created
                            locator_uuid_dict[ip] = locator['_uuid']
                            wait_operates.append(ovsdb.wait('Physical_Locator',
                                                       [["_uuid", "==", locator['_uuid']]],
                                                       ["dst_ip", "encapsulation_type"],
                                                       [{"dst_ip": ip,
                                                         "encapsulation_type": "vxlan_over_ipv4"}],
                                                        True, 0))
                        else:
                            # Create the locator
                            wait_operates.append(ovsdb.wait('Physical_Locator',
                                                            [["dst_ip", "==", ip],
                                                             ["encapsulation_type", "==", "vxlan_over_ipv4"]],
                                                            ["_uuid"],
                                                            [], True, 0))
                            name = 'locator_uuid_' + hex(ip4_addr(ip))
                            locator_uuid_dict[ip] = ovsdb.named_uuid(name)
                            set_operates.append(ovsdb.insert('Physical_Locator',
                                                             {"dst_ip": ip,
                                                              "encapsulation_type": "vxlan_over_ipv4"},
                                                             name))
                    if result[1]['rows'] and (not update or broadcasts_updated):
                        # locator set cannot be modified, it can only be replaced with a new set
                        mcast_uuid = result[1]['rows'][0]['locator_set']
                        # locator set is already created
                        if not broadcast_ips:
                            # Remove the whole locator set. We do not do check; it is always removed
                            set_operates.append(ovsdb.delete('Mcast_Macs_Remote',
                                                             [["MAC", "==", "unknown-dst"],
                                                              ["logical_switch", "==", ovsdb.uuid(lsuuid)]]))
                        else:
                            wait_operates.append(ovsdb.wait('Mcast_Macs_Remote',
                                                          [["MAC", "==", "unknown-dst"],
                                                          ["logical_switch", "==", ovsdb.uuid(lsuuid)]],
                                                          ["_uuid"],
                                                          [{"_uuid": result[1]['rows'][0]['_uuid']}],
                                                          True, 0))
                            # Create a new set
                            set_operates.append(ovsdb.insert('Physical_Locator_Set',
                                                             {"locators": ovsdb.oset(*[locator_uuid_dict[ip] for ip in broadcast_ips])},
                                                             'mcast_unknown_uuid'))
                            mcast_uuid = ovsdb.named_uuid('mcast_unknown_uuid')
                            set_operates.append(ovsdb.update('Mcast_Macs_Remote',
                                                            [["_uuid", "==", result[1]['rows'][0]['_uuid']]],
                                                             {"locator_set": mcast_uuid}))
                    else:
                        if broadcast_ips:
                            wait_operates.append(ovsdb.wait('Mcast_Macs_Remote',
                                                          [["MAC", "==", "unknown-dst"],
                                                          ["logical_switch", "==", ovsdb.uuid(lsuuid)]],
                                                          ["_uuid"],
                                                          [],
                                                          True, 0))
                            set_operates.append(ovsdb.insert('Physical_Locator_Set',
                                                             {"locators": ovsdb.oset(*[locator_uuid_dict[ip] for ip in broadcast_ips])},
                                                             'mcast_unknown_uuid'))
                            mcast_uuid = ovsdb.named_uuid('mcast_unknown_uuid')
                            set_operates.append(ovsdb.insert('Mcast_Macs_Remote',
                                                             {"MAC": "unknown-dst",
                                                              "logical_switch": ovsdb.uuid(lsuuid),
                                                              "locator_set": mcast_uuid}))
                    # modify unicast table
                    if update:
                        set_operates.extend(ovsdb.delete('Ucast_Macs_Remote',
                                                         [["logical_switch", "==", ovsdb.uuid(lsuuid)],
                                                          ["MAC", "==", mac]])
                                            for mac in remove_ucasts)
                        set_operates.extend(ovsdb.delete('Ucast_Macs_Remote',
                                                         [["logical_switch", "==", ovsdb.uuid(lsuuid)],
                                                          ["MAC", "==", mac]])
                                            for mac in add_ucasts)
                        set_operates.extend(ovsdb.insert('Ucast_Macs_Remote',
                                                         {"MAC": mac,
                                                          "logical_switch": ovsdb.uuid(lsuuid),
                                                          "locator": locator_uuid_dict[tunnel]})
                                            for mac, tunnel in add_ucasts.items())
                    else:
                        set_operates.append(ovsdb.delete('Ucast_Macs_Remote',
                                                         [["logical_switch", "==", ovsdb.uuid(lsuuid)]]))
                        set_operates.extend(ovsdb.insert('Ucast_Macs_Remote',
                                                         {"MAC": mac,
                                                          "logical_switch": ovsdb.uuid(lsuuid),
                                                          "locator": locator_uuid_dict[tunnel]})
                                            for mac, tunnel in add_ucasts.items())
                    method, params = ovsdb.transact('hardware_vtep', *(wait_operates + set_operates))
                    for m in protocol.querywithreply(method, params, conn, self.apiroutine):
                        yield m
                    result = self.apiroutine.jsonrpc_result
                    if any(r is None or 'error' in r for r in result[:len(wait_operates)]):
                        # Some wait operates failed, retry
                        continue
                    _check_transact_result(result[len(wait_operates):], set_operates)
                    break
            except Exception:
                self._logger.warning('Update hardware_vtep failed with exception', exc_info = True)
                # Force a full update next time
                _last_endpoints[:] = [set(), {}, None]
            else:
                _last_endpoints[:] = [broadcast_ips, mac_ip_dict, ps]
        try:
            while True:
                for m in callAPI(self.apiroutine, 'objectdb', 'walk',
                                 {'keys': (lognet_key,
                                           endpointset_key,
                                           lognetmap_key),
                                  'walkerdict': {lognet_key: walker},
                                  'requestid': requestid
                                  }):
                    yield m
                if _update_set:
                    _update_set.clear()
                    continue
                lastkeys = set(_savedkeys)
                _savedkeys, _savedresult = self.apiroutine.retvalue
                if _detect_update_routine is not None:
                    _detect_update_routine.close()
                _detect_update_routine = self.apiroutine.subroutine(_detect_update(_savedresult), False)
                removekeys = tuple(lastkeys.difference(_savedkeys))
                if removekeys:
                    # Unwatch unnecessary keys
                    for m in callAPI(self.apiroutine, 'objectdb', 'munwatch', {'keys': removekeys,
                                                                    'requestid': requestid}):
                        yield m
                for m in _update_hardware_vtep(_savedresult):
                    yield m
                while True:
                    if not _update_set:
                        yield (update_matcher,)
                    should_rewalk = any(v.getkey() in rewalk_keys for v in _update_set if v is not None)
                    _update_set.clear()
                    if should_rewalk:
                        break
                    else:
                        for m in _update_hardware_vtep(_savedresult):
                            yield m
        finally:
            if _detect_update_routine is not None:
                _detect_update_routine.close()
            self.apiroutine.subroutine(callAPI(self.apiroutine,'objectdb', 'unwatchall', {'requestid': requestid}))
    
    def _update_logicalswitch_info(self, conn, logicalswitch, lsuuid):
        with closing(self.apiroutine.executeAll([self._push_logicalswitch_endpoints(conn, logicalswitch),
                                                 self._poll_logicalswitch_endpoints(conn, logicalswitch, lsuuid)], None, ())) as g:
            for m in g:
                yield m
    
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
            
    def _recycling(self):
        interval = self.recycleinterval
        if interval is None:
            return
        def _recycle_logical_switches(conn):
            try:
                protocol = conn.protocol
                method, params = ovsdb.transact('hardware_vtep',
                                                ovsdb.select('Logical_Switch',
                                                             [],
                                                             ["_uuid", "name"]))
                for m in protocol.querywithreply(method, params, conn, self.apiroutine):
                    yield m
                ls_list = [(r['_uuid'][1], r['name']) for r in self.apiroutine.jsonrpc_result[0]['rows']]
                for l, name in ls_list:
                    method, params = ovsdb.transact('hardware_vtep',
                                                    ovsdb.delete('Ucast_Macs_Remote',
                                                                 [["logical_switch", "==", ovsdb.uuid(l)]]),
                                                    ovsdb.delete('Mcast_Macs_Remote',
                                                                 [["logical_switch", "==", ovsdb.uuid(l)]]),
                                                    ovsdb.delete('Ucast_Macs_Local',
                                                                 [["logical_switch", "==", ovsdb.uuid(l)]]),
                                                    ovsdb.delete('Mcast_Macs_Local',
                                                                 [["logical_switch", "==", ovsdb.uuid(l)]]),
                                                    ovsdb.delete('Logical_Switch',
                                                                 [["_uuid", "==", ovsdb.uuid(l)]]))
                    for m in protocol.querywithreply(method, params, conn, self.apiroutine):
                        yield m
                    # If the logical switch is still referenced, the transact will fail
                    if 'error' in self.apiroutine.jsonrpc_result[0]:
                        self._logger.warning('Recycling logical switch %r (uuid = %r) failed: %r',
                                             name, l, self.apiroutine.jsonrpc_result[0]['error'])
                    elif not any(r for r in self.apiroutine.jsonrpc_result if 'error' in r):
                        self._logger.info('Recycle logical switch %r for no longer in use', name)
            except Exception:
                self._logger.warning('Recycling failed on connecton %r', conn, exc_info = True)
        while True:
            for m in self.apiroutine.waitWithTimeout(interval):
                yield m
            with closing(self.apiroutine.executeAll([_recycle_logical_switches(c)
                                                     for c in self._connection_ps], None, ())) as g:
                for m in g:
                    yield m
                    
    def _main(self):
        self.apiroutine.subroutine(self._manage_existing())
        self.apiroutine.subroutine(self._recycling(), True, '_recycling_routine')
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
            if hasattr(self.apiroutine, '_recycling_routine'):
                self.apiroutine._recycling_routine.close()
        
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
        
    def updatelogicalswitch(self, physicalswitch, physicalport, vlanid, logicalnetwork, vni, logicalports):
        '''
        Bind VLAN on physicalport to specified logical network, and update logical port vxlan info
        
        :param physicalswitch: physical switch name, should be the name in PhysicalSwitch table of OVSDB vtep database
        
        :param physicalport: physical port name, should be the name in OVSDB vtep database
        
        :param vlanid: the vlan tag used for this logicalswitch
        
        :param logicalnetwork: the logical network id, will also be the logical switch id
        
        :param vni: the VXLAN VNI of the logical network
        
        :param logicalports: a list of logical port IDs. The VXLAN info of these ports will be updated.
        '''
        for m in self._wait_for_sync():
            yield m
        vlanid = int(vlanid)
        vni = int(vni)
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
                    operations.append(ovsdb.update("Logical_Switch",
                                                   [["_uuid", "==", ovsdb.uuid(ls_uuid)]],
                                                   {"tunnel_key": vni}))
                else:
                    operations.append(ovsdb.wait("Logical_Switch",
                                                [["name", "==", logicalnetwork]],
                                                ["_uuid"],
                                                [],
                                                timeout = 0))
                    # Create the new logical switch
                    operations.append(ovsdb.insert('Logical_Switch',
                                                   {"name": logicalnetwork,
                                                    "tunnel_key": vni},
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
                _check_transact_result(result[3:], operations[3:])
                break
            except ConnectionResetException:
                for m in self.apiroutine.waitWithTimeout(1):
                    yield m
                continue
            except IOError:
                for m in self.apiroutine.waitWithTimeout(1):
                    yield m
                continue
        if logicalports:
            # Refresh network with monitor, only update the logical port information
            for m in update_vxlaninfo(self.apiroutine,
                                      {},
                                      dict((p, tunnelip) for p in logicalports),
                                      {},
                                      protocol.vhost,
                                      physicalswitch,
                                      'hardware_vtep',
                                      self.allowedmigrationtime,
                                      self.refreshinterval):
                yield m
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
                if curr_network is None or curr_network[1] != ls_uuid:
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
                _check_transact_result(result[3:], operations[3:])
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
                _check_transact_result(result[2:], operations[2:])
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


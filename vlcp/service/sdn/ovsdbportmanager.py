'''
Created on 2016/2/26

:author: think
'''


from vlcp.config import defaultconfig
from vlcp.server.module import Module, api, depend, callAPI, ModuleNotification
from vlcp.event.runnable import RoutineContainer
from vlcp.service.sdn import ovsdbmanager
from vlcp.event.connection import ConnectionResetException
from vlcp.event.event import Event, withIndices
from vlcp.service.sdn.ovsdbmanager import OVSDBBridgeSetup, OVSDBConnectionSetup,\
    OVSDBBridgeNotAppearException
import vlcp.utils.ovsdb as ovsdb
from vlcp.protocol.jsonrpc import JsonRPCErrorResultException,\
    JsonRPCNotificationEvent, JsonRPCProtocolException
import itertools
from contextlib import closing

def _bytes(s):
    return s.encode('ascii')

@withIndices('connection')
class OVSDBConnectionPortsSynchronized(Event):
    pass

@withIndices('connection', 'name', 'ofport', 'id', 'vhost', 'datapathid')
class OVSDBPortUpNotification(Event):
    pass

class OVSDBPortNotAppearException(Exception):
    pass


@defaultconfig
@depend(ovsdbmanager.OVSDBManager)
class OVSDBPortManager(Module):
    '''
    Manage Ports from OVSDB Protocol
    '''
    service = True
    def __init__(self, server):
        Module.__init__(self, server)
        self.apiroutine = RoutineContainer(self.scheduler)
        self.apiroutine.main = self._manage_ports
        self.routines.append(self.apiroutine)
        self.managed_ports = {}
        self.managed_ids = {}
        self.monitor_routines = set()
        self.ports_uuids = {}
        self.wait_portnos = {}
        self.wait_names = {}
        self.wait_ids = {}
        self.bridge_datapathid = {}
        self.createAPI(api(self.getports, self.apiroutine),
                       api(self.getallports, self.apiroutine),
                       api(self.getportbyid, self.apiroutine),
                       api(self.waitportbyid, self.apiroutine),
                       api(self.getportbyname, self.apiroutine),
                       api(self.waitportbyname, self.apiroutine),
                       api(self.getportbyno, self.apiroutine),
                       api(self.waitportbyno, self.apiroutine),
                       api(self.resync, self.apiroutine)
                       )
        self._synchronized = False
    def _get_interface_info(self, connection, protocol, buuid, interface_uuid, port_uuid):
        try:
            method, params = ovsdb.transact('Open_vSwitch',
                                            ovsdb.wait('Interface', [["_uuid", "==", ovsdb.uuid(interface_uuid)]],
                                                       ["ofport"], [{"ofport":ovsdb.oset()}], False, 5000),
                                            ovsdb.wait('Interface', [["_uuid", "==", ovsdb.uuid(interface_uuid)]],
                                                       ["ifindex"], [{"ifindex":ovsdb.oset()}], False, 5000),
                                            ovsdb.select('Interface', [["_uuid", "==", ovsdb.uuid(interface_uuid)]],
                                                                         ["_uuid", "name", "ifindex", "ofport", "type", "external_ids"]))
            for m in protocol.querywithreply(method, params, connection, self.apiroutine):
                yield m
            r = self.apiroutine.jsonrpc_result[0]
            if 'error' in r:
                raise JsonRPCErrorResultException('Error while acquiring interface: ' + repr(r['error']))            
            r = self.apiroutine.jsonrpc_result[1]
            if 'error' in r:
                raise JsonRPCErrorResultException('Error while acquiring interface: ' + repr(r['error']))            
            r = self.apiroutine.jsonrpc_result[2]
            if 'error' in r:
                raise JsonRPCErrorResultException('Error while acquiring interface: ' + repr(r['error']))
            if not r['rows']:
                self.apiroutine.retvalue = []
                return
            r0 = r['rows'][0]
            if r0['ofport'] < 0:
                # Ignore this port because it is in an error state
                self.apiroutine.retvalue = []
                return
            r0['_uuid'] = r0['_uuid'][1]
            r0['ifindex'] = ovsdb.getoptional(r0['ifindex'])
            r0['external_ids'] = ovsdb.getdict(r0['external_ids'])
            if buuid not in self.bridge_datapathid:
                self.apiroutine.retvalue = []
                return
            else:
                datapath_id = self.bridge_datapathid[buuid]
            if 'iface-id' in r0['external_ids']:
                eid = r0['external_ids']['iface-id']
                r0['id'] = eid
                id_ports = self.managed_ids.setdefault((protocol.vhost, eid), [])
                id_ports.append((datapath_id, r0))
            else:
                r0['id'] = None
            self.managed_ports.setdefault((protocol.vhost, datapath_id),[]).append((port_uuid, r0))
            notify = False
            if (protocol.vhost, datapath_id, r0['ofport']) in self.wait_portnos:
                notify = True
                del self.wait_portnos[(protocol.vhost, datapath_id, r0['ofport'])]
            if (protocol.vhost, datapath_id, r0['name']) in self.wait_names:
                notify = True
                del self.wait_names[(protocol.vhost, datapath_id, r0['name'])]
            if (protocol.vhost, r0['id']) in self.wait_ids:
                notify = True
                del self.wait_ids[(protocol.vhost, r0['id'])]
            if notify:
                for m in self.apiroutine.waitForSend(OVSDBPortUpNotification(connection, r0['name'],
                                                                             r0['ofport'], r0['id'],
                                                                             protocol.vhost, datapath_id,
                                                                             port = r0)):
                    yield m
            self.apiroutine.retvalue = [r0]
        except JsonRPCProtocolException:
            self.apiroutine.retvalue = []
    def _remove_interface_id(self, connection, protocol, datapath_id, port):
        eid = port['id']
        eid_list = self.managed_ids.get((protocol.vhost, eid))
        for i in range(0, len(eid_list)):
            if eid_list[i][1]['_uuid'] == port['_uuid']:
                del eid_list[i]
                break
    def _remove_interface(self, connection, protocol, datapath_id, interface_uuid, port_uuid):
        ports = self.managed_ports.get((protocol.vhost, datapath_id))
        r = None
        if ports is not None:
            for i in range(0, len(ports)):
                if ports[i][1]['_uuid'] == interface_uuid:
                    r = ports[i][1]
                    if r['id']:
                        self._remove_interface_id(connection, protocol, datapath_id, r)
                    del ports[i]
                    break
            if not ports:
                del self.managed_ports[(protocol.vhost, datapath_id)]
        return r
    def _remove_all_interface(self, connection, protocol, datapath_id, port_uuid, buuid):
        ports = self.managed_ports.get((protocol.vhost, datapath_id))
        if ports is not None:
            removed_ports = [r for puuid, r in ports if puuid == port_uuid]
            not_removed_ports = [(puuid, r) for puuid, r in ports if puuid != port_uuid]
            ports[:len(not_removed_ports)] = not_removed_ports
            del ports[len(not_removed_ports):]
            for r in removed_ports:
                if r['id']:
                    self._remove_interface_id(connection, protocol, datapath_id, r)
            if not ports:
                del self.managed_ports[(protocol.vhost, datapath_id)]
            return removed_ports
        if port_uuid in self.ports_uuids and self.ports_uuids[port_uuid] == buuid:
            del self.ports_uuids[port_uuid]
        return []
    def _update_interfaces(self, connection, protocol, updateinfo, update = True):
        """
        There are several kinds of updates, they may appear together:
        
        1. New bridge created (or from initial updateinfo). We should add all the interfaces to the list.
        
        2. Bridge removed. Remove all the ports.
        
        3. name and datapath_id may be changed. We will consider this as a new bridge created, and an old
           bridge removed.
        
        4. Bridge ports modification, i.e. add/remove ports.
           a) Normally a port record is created/deleted together. A port record cannot exist without a
              bridge containing it.
               
           b) It is also possible that a port is removed from one bridge and added to another bridge, in
              this case the ports do not appear in the updateinfo
            
        5. Port interfaces modification, i.e. add/remove interfaces. The bridge record may not appear in this
           situation.
           
        We must consider these situations carefully and process them in correct order.
        """
        port_update = updateinfo.get('Port', {})
        bridge_update = updateinfo.get('Bridge', {})
        working_routines = []
        def process_bridge(buuid, uo):
            try:
                nv = uo['new']
                if 'datapath_id' in nv:
                    if ovsdb.getoptional(nv['datapath_id']) is None:
                        # This bridge is not initialized. Wait for the bridge to be initialized.
                        for m in callAPI(self.apiroutine, 'ovsdbmanager', 'waitbridge', {'connection': connection,
                                                                        'name': nv['name'],
                                                                        'timeout': 5}):
                            yield m
                        datapath_id = self.apiroutine.retvalue
                    else:
                        datapath_id = int(nv['datapath_id'], 16)
                    self.bridge_datapathid[buuid] = datapath_id
                elif buuid in self.bridge_datapathid:
                    datapath_id = self.bridge_datapathid[buuid]
                else:
                    # This should not happen, but just in case...
                    for m in callAPI(self.apiroutine, 'ovsdbmanager', 'waitbridge', {'connection': connection,
                                                                    'name': nv['name'],
                                                                    'timeout': 5}):
                        yield m
                    datapath_id = self.apiroutine.retvalue
                    self.bridge_datapathid[buuid] = datapath_id
                if 'ports' in nv:
                    nset = set((p for _,p in ovsdb.getlist(nv['ports'])))
                else:
                    nset = set()
                if 'old' in uo:
                    ov = uo['old']
                    if 'ports' in ov:
                        oset = set((p for _,p in ovsdb.getlist(ov['ports'])))
                    else:
                        # new ports are not really added; it is only sent because datapath_id is modified
                        nset = set()
                        oset = set()
                    if 'datapath_id' in ov and ovsdb.getoptional(ov['datapath_id']) is not None:
                        old_datapathid = int(ov['datapath_id'], 16)
                    else:
                        old_datapathid = datapath_id
                else:
                    oset = set()
                    old_datapathid = datapath_id
                # For every deleted port, remove the interfaces with this port _uuid
                remove = []
                add_routine = []
                for puuid in oset - nset:
                    remove += self._remove_all_interface(connection, protocol, old_datapathid, puuid, buuid)
                # For every port not changed, check if the interfaces are modified;
                for puuid in oset.intersection(nset):
                    if puuid in port_update:
                        # The port is modified, there should be an 'old' set and 'new' set
                        pu = port_update[puuid]
                        if 'old' in pu:
                            poset = set((p for _,p in ovsdb.getlist(pu['old']['interfaces'])))
                        else:
                            poset = set()
                        if 'new' in pu:
                            pnset = set((p for _,p in ovsdb.getlist(pu['new']['interfaces'])))
                        else:
                            pnset = set()
                        # Remove old interfaces
                        remove += [r for r in 
                                   (self._remove_interface(connection, protocol, datapath_id, iuuid, puuid)
                                    for iuuid in (poset - pnset)) if r is not None]
                        # Prepare to add new interfaces
                        add_routine += [self._get_interface_info(connection, protocol, buuid, iuuid, puuid)
                                        for iuuid in (pnset - poset)]
                # For every port added, add the interfaces
                def add_port_interfaces(puuid):
                    # If the uuid does not appear in update info, we have no choice but to query interfaces with select
                    # we cannot use data from other bridges; the port may be moved from a bridge which is not tracked
                    try:
                        method, params = ovsdb.transact('Open_vSwitch', ovsdb.select('Port',
                                                                                     [["_uuid", "==", ovsdb.uuid(puuid)]],
                                                                                     ["interfaces"]))
                        for m in protocol.querywithreply(method, params, connection, self.apiroutine):
                            yield m
                        r = self.apiroutine.jsonrpc_result[0]
                        if 'error' in r:
                            raise JsonRPCErrorResultException('Error when query interfaces from port ' + repr(puuid) + ': ' + r['error'])
                        if r['rows']:
                            interfaces = ovsdb.getlist(r['rows'][0]['interfaces'])
                            with closing(self.apiroutine.executeAll([self._get_interface_info(connection, protocol, buuid, iuuid, puuid)
                                                                 for _,iuuid in interfaces])) as g:
                                for m in g:
                                    yield m
                            self.apiroutine.retvalue = list(itertools.chain(r[0] for r in self.apiroutine.retvalue))
                        else:
                            self.apiroutine.retvalue = []
                    except JsonRPCProtocolException:
                        self.apiroutine.retvalue = []
                    except ConnectionResetException:
                        self.apiroutine.retvalue = []
                
                for puuid in nset - oset:
                    self.ports_uuids[puuid] = buuid
                    if puuid in port_update and 'new' in port_update[puuid] \
                            and 'old' not in port_update[puuid]:
                        # Add all the interfaces in 'new'
                        interfaces = ovsdb.getlist(port_update[puuid]['new']['interfaces'])
                        add_routine += [self._get_interface_info(connection, protocol, buuid, iuuid, puuid)
                                        for _,iuuid in interfaces]
                    else:
                        add_routine.append(add_port_interfaces(puuid))
                # Execute the add_routine
                try:
                    with closing(self.apiroutine.executeAll(add_routine)) as g:
                        for m in g:
                            yield m
                except:
                    add = []
                    raise
                else:
                    add = list(itertools.chain(r[0] for r in self.apiroutine.retvalue))
                finally:
                    if update:
                        self.scheduler.emergesend(
                                ModuleNotification(self.getServiceName(), 'update',
                                                   datapathid = datapath_id,
                                                   connection = connection,
                                                   vhost = protocol.vhost,
                                                   add = add, remove = remove,
                                                   reason = 'bridgemodify'
                                                            if 'old' in uo
                                                            else 'bridgeup'
                                                   ))
            except JsonRPCProtocolException:
                pass
            except ConnectionResetException:
                pass
            except OVSDBBridgeNotAppearException:
                pass
        ignore_ports = set()
        for buuid, uo in bridge_update.items():
            # Bridge removals are ignored because we process OVSDBBridgeSetup event instead
            if 'old' in uo:
                if 'ports' in uo['old']:
                    oset = set((puuid for _, puuid in ovsdb.getlist(uo['old']['ports'])))
                    ignore_ports.update(oset)
                if 'new' not in uo:
                    if buuid in self.bridge_datapathid:
                        del self.bridge_datapathid[buuid]
            if 'new' in uo:
                # If bridge contains this port is updated, we process the port update totally in bridge,
                # so we ignore it later
                if 'ports' in uo['new']:
                    nset = set((puuid for _, puuid in ovsdb.getlist(uo['new']['ports'])))
                    ignore_ports.update(nset)
                working_routines.append(process_bridge(buuid, uo))                
        def process_port(buuid, port_uuid, interfaces, remove_ids):
            if buuid not in self.bridge_datapathid:
                return
            datapath_id = self.bridge_datapathid[buuid]
            ports = self.managed_ports.get((protocol.vhost, datapath_id))
            remove = []
            if ports is not None:
                remove = [p for _,p in ports if p['_uuid'] in remove_ids]
                not_remove = [(_,p) for _,p in ports if p['_uuid'] not in remove_ids]
                ports[:len(not_remove)] = not_remove
                del ports[len(not_remove):]
            if interfaces:
                try:
                    with closing(self.apiroutine.executeAll([self._get_interface_info(connection, protocol, buuid, iuuid, port_uuid)
                                                         for iuuid in interfaces])) as g:
                        for m in g:
                            yield m
                    add = list(itertools.chain((r[0] for r in self.apiroutine.retvalue if r[0])))
                except Exception:
                    self._logger.warning("Cannot get new port information", exc_info = True)
                    add = []
            else:
                add = []
            if update:
                for m in self.apiroutine.waitForSend(ModuleNotification(self.getServiceName(), 'update', datapathid = datapath_id,
                                                                                                          connection = connection,
                                                                                                          vhost = protocol.vhost,
                                                                                                          add = add, remove = remove,
                                                                                                          reason = 'bridgemodify'
                                                                                                          )):
                    yield m
        for puuid, po in port_update.items():
            if puuid not in ignore_ports:
                bridge_id = self.ports_uuids.get(puuid)
                if bridge_id is not None:
                    datapath_id = self.bridge_datapathid[bridge_id]
                    if datapath_id is not None:
                        # This port is modified
                        if 'new' in po:
                            nset = set((iuuid for _, iuuid in ovsdb.getlist(po['new']['interfaces'])))
                        else:
                            nset = set()                    
                        if 'old' in po:
                            oset = set((iuuid for _, iuuid in ovsdb.getlist(po['old']['interfaces'])))
                        else:
                            oset = set()
                        working_routines.append(process_port(bridge_id, puuid, nset - oset, oset - nset))
        if update:
            for r in working_routines:
                self.apiroutine.subroutine(r)
        else:
            try:
                with closing(self.apiroutine.executeAll(working_routines, None, ())) as g:
                    for m in g:
                        yield m
            finally:
                self.scheduler.emergesend(OVSDBConnectionPortsSynchronized(connection))
    def _get_ports(self, connection, protocol):
        try:
            try:
                method, params = ovsdb.monitor('Open_vSwitch', 'ovsdb_port_manager_interfaces_monitor', {
                                                    'Bridge':[ovsdb.monitor_request(["name", "datapath_id", "ports"])],
                                                    'Port':[ovsdb.monitor_request(["interfaces"])]
                                                })
                try:
                    for m in protocol.querywithreply(method, params, connection, self.apiroutine):
                        yield m
                except JsonRPCErrorResultException:
                    # The monitor is already set, cancel it first
                    method2, params2 = ovsdb.monitor_cancel('ovsdb_port_manager_interfaces_monitor')
                    for m in protocol.querywithreply(method2, params2, connection, self.apiroutine, False):
                        yield m
                    for m in protocol.querywithreply(method, params, connection, self.apiroutine):
                        yield m
                r = self.apiroutine.jsonrpc_result
            except:
                for m in self.apiroutine.waitForSend(OVSDBConnectionPortsSynchronized(connection)):
                    yield m
                raise
            # This is the initial state, it should contains all the ids of ports and interfaces
            self.apiroutine.subroutine(self._update_interfaces(connection, protocol, r, False))
            update_matcher = JsonRPCNotificationEvent.createMatcher('update', connection, connection.connmark,
                                                                    _ismatch = lambda x: x.params[0] == 'ovsdb_port_manager_interfaces_monitor')
            conn_state = protocol.statematcher(connection)
            while True:
                yield (update_matcher, conn_state)
                if self.apiroutine.matcher is conn_state:
                    break
                else:
                    self.apiroutine.subroutine(self._update_interfaces(connection, protocol, self.apiroutine.event.params[1], True))
        except JsonRPCProtocolException:
            pass
        finally:
            if self.apiroutine.currentroutine in self.monitor_routines:
                self.monitor_routines.remove(self.apiroutine.currentroutine)
    def _get_existing_ports(self):
        for m in callAPI(self.apiroutine, 'ovsdbmanager', 'getallconnections', {'vhost':None}):
            yield m
        matchers = []
        for c in self.apiroutine.retvalue:
            self.monitor_routines.add(self.apiroutine.subroutine(self._get_ports(c, c.protocol)))
            matchers.append(OVSDBConnectionPortsSynchronized.createMatcher(c))
        for m in self.apiroutine.waitForAll(*matchers):
            yield m
        self._synchronized = True
        for m in self.apiroutine.waitForSend(ModuleNotification(self.getServiceName(), 'synchronized')):
            yield m
    def _wait_for_sync(self):
        if not self._synchronized:
            yield (ModuleNotification.createMatcher(self.getServiceName(), 'synchronized'),)
    def _manage_ports(self):
        try:
            self.apiroutine.subroutine(self._get_existing_ports())
            connsetup = OVSDBConnectionSetup.createMatcher()
            bridgedown = OVSDBBridgeSetup.createMatcher(OVSDBBridgeSetup.DOWN)
            while True:
                yield (connsetup, bridgedown)
                e = self.apiroutine.event
                if self.apiroutine.matcher is connsetup:
                    self.monitor_routines.add(self.apiroutine.subroutine(self._get_ports(e.connection, e.connection.protocol)))
                else:
                    # Remove ports of the bridge
                    ports =  self.managed_ports.get((e.vhost, e.datapathid))
                    if ports is not None:
                        ports_original = ports
                        ports = [p for _,p in ports]
                        for p in ports:
                            if p['id']:
                                self._remove_interface_id(e.connection,
                                                          e.connection.protocol, e.datapathid, p)
                        newdpid = getattr(e, 'new_datapath_id', None)
                        buuid = e.bridgeuuid
                        if newdpid is not None:
                            # This bridge changes its datapath id
                            if buuid in self.bridge_datapathid and self.bridge_datapathid[buuid] == e.datapathid:
                                self.bridge_datapathid[buuid] = newdpid
                            def re_add_interfaces():
                                with closing(self.apiroutine.executeAll(
                                    [self._get_interface_info(e.connection, e.connection.protocol, buuid,
                                                              r['_uuid'], puuid)
                                     for puuid, r in ports_original])) as g:
                                    for m in g:
                                        yield m
                                add = list(itertools.chain(r[0] for r in self.apiroutine.retvalue))
                                for m in self.apiroutine.waitForSend(ModuleNotification(self.getServiceName(),
                                                  'update', datapathid = e.datapathid,
                                                  connection = e.connection,
                                                  vhost = e.vhost,
                                                  add = add, remove = [],
                                                  reason = 'bridgeup'
                                                  )):
                                    yield m
                            self.apiroutine.subroutine(re_add_interfaces())
                        else:
                            # The ports are removed
                            for puuid, _ in ports_original:
                                if puuid in self.ports_uuids[puuid] and self.ports_uuids[puuid] == buuid:
                                    del self.ports_uuids[puuid]
                        del self.managed_ports[(e.vhost, e.datapathid)]
                        self.scheduler.emergesend(ModuleNotification(self.getServiceName(),
                                                  'update', datapathid = e.datapathid,
                                                  connection = e.connection,
                                                  vhost = e.vhost,
                                                  add = [], remove = ports,
                                                  reason = 'bridgedown'
                                                  ))                            
        finally:
            for r in list(self.monitor_routines):
                r.close()
            self.scheduler.emergesend(ModuleNotification(self.getServiceName(), 'unsynchronized'))
    def getports(self, datapathid, vhost = ''):
        "Return all ports of a specifed datapath"
        for m in self._wait_for_sync():
            yield m
        self.apiroutine.retvalue = [p for _,p in self.managed_ports.get((vhost, datapathid), [])]
    def getallports(self, vhost = None):
        "Return all ``(datapathid, port, vhost)`` tuples, optionally filterd by vhost"
        for m in self._wait_for_sync():
            yield m
        if vhost is None:
            self.apiroutine.retvalue = [(dpid, p, vh) for (vh, dpid),v in self.managed_ports.items() for _,p in v]
        else:
            self.apiroutine.retvalue = [(dpid, p, vh) for (vh, dpid),v in self.managed_ports.items() if vh == vhost for _,p in v]
    def getportbyno(self, datapathid, portno, vhost = ''):
        "Return port with specified portno"
        portno &= 0xffff
        for m in self._wait_for_sync():
            yield m
        self.apiroutine.retvalue = self._getportbyno(datapathid, portno, vhost)
    def _getportbyno(self, datapathid, portno, vhost = ''):
        ports = self.managed_ports.get((vhost, datapathid))
        if ports is None:
            return None
        else:
            for _, p in ports:
                if p['ofport'] == portno:
                    return p
            return None
    def waitportbyno(self, datapathid, portno, timeout = 30, vhost = ''):
        "Wait for port with specified portno"
        portno &= 0xffff
        for m in self._wait_for_sync():
            yield m
        def waitinner():
            p = self._getportbyno(datapathid, portno, vhost)
            if p is not None:
                self.apiroutine.retvalue = p
            else:
                try:
                    self.wait_portnos[(vhost, datapathid, portno)] = \
                            self.wait_portnos.get((vhost, datapathid, portno),0) + 1
                    yield (OVSDBPortUpNotification.createMatcher(None, None, portno, None, vhost, datapathid),)
                except:
                    v = self.wait_portnos.get((vhost, datapathid, portno))
                    if v is not None:
                        if v <= 1:
                            del self.wait_portnos[(vhost, datapathid, portno)]
                        else:
                            self.wait_portnos[(vhost, datapathid, portno)] = v - 1
                    raise
                else:
                    self.apiroutine.retvalue = self.apiroutine.event.port
        for m in self.apiroutine.executeWithTimeout(timeout, waitinner()):
            yield m
        if self.apiroutine.timeout:
            raise OVSDBPortNotAppearException('Port ' + repr(portno) + ' does not appear before timeout')
    def getportbyname(self, datapathid, name, vhost = ''):
        "Return port with specified name"
        if isinstance(name, bytes):
            name = name.decode('utf-8')
        for m in self._wait_for_sync():
            yield m
        self.apiroutine.retvalue = self._getportbyname(datapathid, name, vhost)
    def _getportbyname(self, datapathid, name, vhost = ''):
        ports = self.managed_ports.get((vhost, datapathid))
        if ports is None:
            return None
        else:
            for _, p in ports:
                if p['name'] == name:
                    return p
            return None
    def waitportbyname(self, datapathid, name, timeout = 30, vhost = ''):
        "Wait for port with specified name"
        for m in self._wait_for_sync():
            yield m
        def waitinner():
            p = self._getportbyname(datapathid, name, vhost)
            if p is not None:
                self.apiroutine.retvalue = p
            else:
                try:
                    self.wait_names[(vhost, datapathid, name)] = \
                            self.wait_portnos.get((vhost, datapathid, name) ,0) + 1
                    yield (OVSDBPortUpNotification.createMatcher(None, name, None, None, vhost, datapathid),)
                except:
                    v = self.wait_names.get((vhost, datapathid, name))
                    if v is not None:
                        if v <= 1:
                            del self.wait_names[(vhost, datapathid, name)]
                        else:
                            self.wait_names[(vhost, datapathid, name)] = v - 1
                    raise
                else:
                    self.apiroutine.retvalue = self.apiroutine.event.port
        for m in self.apiroutine.executeWithTimeout(timeout, waitinner()):
            yield m
        if self.apiroutine.timeout:
            raise OVSDBPortNotAppearException('Port ' + repr(name) + ' does not appear before timeout')
    def getportbyid(self, id, vhost = ''):
        "Return port with the specified id. The return value is a pair: ``(datapath_id, port)``"
        for m in self._wait_for_sync():
            yield m
        self.apiroutine = self._getportbyid(id, vhost)
    def _getportbyid(self, id, vhost = ''):
        ports = self.managed_ids.get((vhost, id))
        if ports:
            return ports[0]
        else:
            return None
    def waitportbyid(self, id, timeout = 30, vhost = ''):
        "Wait for port with the specified id. The return value is a pair ``(datapath_id, port)``"
        for m in self._wait_for_sync():
            yield m
        def waitinner():
            p = self._getportbyid(id, vhost)
            if p is None:
                try:
                    self.wait_ids[(vhost, id)] = self.wait_ids.get((vhost, id), 0) + 1
                    yield (OVSDBPortUpNotification.createMatcher(None, None, None, id, vhost),)
                except:
                    v = self.wait_ids.get((vhost, id))
                    if v is not None:
                        if v <= 1:
                            del self.wait_ids[(vhost, id)]
                        else:
                            self.wait_ids[(vhost, id)] = v - 1
                    raise
                else:
                    self.apiroutine.retvalue = (self.apiroutine.event.datapathid,
                                                self.apiroutine.event.port)
            else:
                self.apiroutine.retvalue = p
        for m in self.apiroutine.executeWithTimeout(timeout, waitinner()):
            yield m
        if self.apiroutine.timeout:
            raise OVSDBPortNotAppearException('Port ' + repr(id) + ' does not appear before timeout')
    def resync(self, datapathid, vhost = ''):
        '''
        Resync with current ports
        '''
        # Sometimes when the OVSDB connection is very busy, monitor message may be dropped.
        # We must deal with this and recover from it
        # Save current manged_ports
        if (vhost, datapathid) not in self.managed_ports:
            self.apiroutine.retvalue = None
            return
        else:
            for m in callAPI(self.apiroutine, 'ovsdbmanager', 'getconnection', {'datapathid': datapathid, 'vhost':vhost}):
                yield m
            c = self.apiroutine.retvalue
            if c is not None:
                # For now, we restart the connection...
                for m in c.reconnect(False):
                    yield m
                for m in self.apiroutine.waitWithTimeout(0.1):
                    yield m
                for m in callAPI(self.apiroutine, 'ovsdbmanager', 'waitconnection', {'datapathid': datapathid,
                                                                                     'vhost': vhost}):
                    yield m
        self.apiroutine.retvalue = None

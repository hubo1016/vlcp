'''
Created on 2016/5/30

:author: hubo
'''


from vlcp.config.config import defaultconfig
from vlcp.server.module import depend, publicapi, callAPI
from vlcp.service.sdn.flowbase import FlowBase
from vlcp.service.sdn.ofpmanager import FlowInitialize
from vlcp.utils.flowupdater import FlowUpdater
from vlcp.utils.networkmodel import PhysicalPort, LogicalPort, LogicalNetwork, VXLANEndpointSet,\
    LogicalNetworkMap, LogicalPortVXLANInfo
import vlcp.service.kvdb.objectdb as objectdb
import vlcp.service.sdn.ofpportmanager as ofpportmanager
from vlcp.event.runnable import RoutineContainer
from vlcp.protocol.openflow.openflow import OpenflowConnectionStateEvent,\
    OpenflowAsyncMessageEvent, OpenflowErrorResultException
from vlcp.utils.ethernet import ethernet_l2, mac_addr
import vlcp.service.sdn.ioprocessing as iop
import itertools
from namedstruct.namedstruct import dump
from pprint import pformat
from vlcp.event.event import Event, withIndices
from vlcp.service.sdn import ovsdbportmanager
from vlcp.utils import ovsdb
from vlcp.utils.dataobject import updater, ReferenceObject
from time import time

@withIndices('connection', 'logicalnetworkid', 'type')
class VXLANGroupChanged(Event):
    UPDATED = 'updated'
    DELETED = 'deleted'

def _is_vxlan(obj):
    try:
        return obj.physicalnetwork.type == 'vxlan'
    except AttributeError:
        return False

def _get_ip(ip, ofdef):
    try:
        return ofdef.ip4_addr(ip)
    except Exception:
        return None

class VXLANUpdater(FlowUpdater):
    def __init__(self, connection, parent):
        FlowUpdater.__init__(self, connection, (), ('VXLANUpdater', connection), parent._logger)
        self._parent = parent
        self._lastlognets = ()
        self._lastphyports = ()
        self._lastlogports = ()
        self._lastphyportinfo = {}
        self._lastlognetinfo = {}
        self._lastlogportinfo = {}
        self._lastvxlaninfo = {}
        self._orig_initialkeys = ()
        self._watched_maps = ()
        # For first-upload mode, save the watching MAC addresses
        # a dictionary (logicalnetwork, mac_address) -> expire time
        self._watched_macs = {}
        # Current buffered packets, a dictionary (logicalnetwork, mac_address) -> [packet, packet, ...]
        # where packet is (buffer_id, data, expire, in_port, metadata)
        self._buffered_packets = {}
        # LogicalNetworkID -> PhysicalPortNo map
        self._current_groups = {}
    def wait_for_group(self, container, networkid, timeout = 30):
        if networkid in self._current_groups:
            container.retvalue = self._current_groups[networkid]
        else:
            groupchanged = VXLANGroupChanged.createMatcher(self._connection, networkid, VXLANGroupChanged.UPDATED)
            for m in container.waitWithTimeout(timeout, groupchanged):
                yield m
            if container.timeout:
                raise ValueError('VXLAN group is still not created after a long time')
            else:
                container.retvalue = container.event.physicalportid
    def main(self):
        try:
            if self._connection.protocol.disablenxext:
                raise StopIteration
            self.subroutine(self._update_handler(), name = '_update_handler_routine')
            self.subroutine(self._refresh_handler(), name = '_refresh_handler_routine')
            for m in FlowUpdater.main(self):
                yield m
        finally:
            if hasattr(self, '_update_handler_routine'):
                self._update_handler_routine.close()
            if hasattr(self, '_refresh_handler_routine'):
                self._refresh_handler_routine.close()
    def _walk_lognet(self, key, value, walk, save):
        save(key)
        if value is None:
            return
        try:
            phynet = walk(value.physicalnetwork.getkey())
        except KeyError:
            pass
        else:
            if phynet is not None and getattr(phynet, 'type') == 'vxlan':
                try:
                    vxlan_endpoint_key = VXLANEndpointSet.default_key(value.id)
                    walk(vxlan_endpoint_key)
                except KeyError:
                    pass
                else:
                    save(vxlan_endpoint_key)
                if self._parent.prepush:
                    # Acquire all logical ports
                    try:
                        netmap = walk(LogicalNetworkMap.default_key(value.id))
                    except KeyError:
                        pass
                    else:
                        save(netmap.getkey())
                        for logport in netmap.ports.dataset():
                            try:
                                _ = walk(logport.getkey())
                            except KeyError:
                                pass
                            else:
                                save(logport.getkey())
                            try:
                                _, (portid,) = LogicalPort._getIndices(logport.getkey())
                                portinfokey = LogicalPortVXLANInfo.default_key(portid)
                                _ = walk(portinfokey)
                            except KeyError:
                                pass
                            else:
                                save(portinfokey)
    def walkcomplete(self, keys, values):
        if self._parent.prepush:
            self._watched_maps = [k for k,v in zip(keys, values)
                                  if v is not None and not v.isdeleted() and v.isinstance(LogicalNetworkMap)]
            # If the logical network map changed, restart the walk process
            self._initialkeys = tuple(itertools.chain(self._orig_initialkeys, self._watched_maps))
        if False:
            yield
    def _walk_phyport(self, key, value, walk, save):
        save(key)
    def _walk_logport(self, key, value, walk, save):
        save(key)
    def _walk_mac_key(self, key, value, walk, save):
        save(key)
        if value is not None:
            logport_key = value.ref.getkey()
            try:
                _ = walk(logport_key)
            except KeyError:
                pass
            else:
                save(logport_key)
            _, (portid,) = LogicalPort._getIndices(logport_key)
            try:
                portinfokey = LogicalPortVXLANInfo.default_key(portid)
                _ = walk(portinfokey)
            except KeyError:
                pass
            else:
                save(portinfokey)
    def _get_watched_mac_keys(self):
        keys = []
        current_time = time()
        changed = False
        for k,t in list(self._watched_macs.items()):
            if t < current_time:
                # Expired
                del self._watched_macs[k]
                changed = True
                if k in self._buffered_packets:
                    # The packets should be expired far ago
                    del self._buffered_packets[k]
            elif k[0] not in (n for n,_ in self._lastlognets):
                # Network is removed
                del self._watched_macs[k]
                changed = True
                if k in self._buffered_packets:
                    del self._buffered_packets[k]
            else:
                keys.append(LogicalPort.unique_key('_mac_address_index', *k))
        return (keys, changed)
    def _update_handler(self):
        dataobjectchanged = iop.DataObjectChanged.createMatcher(None, None, self._connection, None, True)
        dataobjectchanged2 = iop.DataObjectChanged.createMatcher(None, None, self._connection, None, False, True)
        while True:
            yield (dataobjectchanged, dataobjectchanged2)
            self._lastlogports, self._lastphyports, self._lastlognets, _ = self.event.current
            self._update_walk()
    def _update_walk(self):
        phyport_keys = [p.getkey() for p,_ in self._lastphyports]
        lognet_keys = [n.getkey() for n,_ in self._lastlognets]
        logport_keys = [p.getkey() for p,_ in self._lastlogports]
        mac_keys, _ = self._get_watched_mac_keys()
        self._orig_initialkeys = phyport_keys + lognet_keys + logport_keys + mac_keys
        self._initialkeys = self._orig_initialkeys + self._watched_maps + mac_keys
        self._walkerdict = dict(itertools.chain(((n, self._walk_lognet) for n in lognet_keys),
                                                ((p, self._walk_phyport) for p in phyport_keys),
                                                ((p, self._walk_logport) for p in logport_keys),
                                                ((p, self._walk_mac_key) for p in mac_keys)))
        self.subroutine(self.restart_walk(), False)
    def _query_packet_handler(self):
        ofdef = self._connection.openflowdef
        vhost = self._connection.protocol.vhost
        vo = self._parent._gettableindex('vxlanoutput', vhost)
        packet_in = OpenflowAsyncMessageEvent.createMatcher(ofdef.OFPT_PACKET_IN, None, None,
                                                            vo, 2, self._connection, self._connection.connmark)
        flow_remove = OpenflowAsyncMessageEvent.createMatcher(ofdef.OFPT_FLOW_REMOVED, None, None,
                                                              vo, 2, self._connection, self._connection.connmark,
                                                              _ismatch = lambda x: x.message.reason == ofdef.OFPRR_IDLE_TIMEOUT)
        while True:
            yield (packet_in, flow_remove)
            if self.matcher is packet_in:
                msg = self.event.message
                try:
                    packet = ethernet_l2.create(msg.data)
                except Exception:
                    self._logger.warning('Invalid packet received: %r', msg.data, exc_info = True)
                else:
                    in_port = ofdef.ofp_port_no.parse(ofdef.get_oxm(packet.match.oxm_fields, ofdef.OXM_OF_IN_PORT))
                    metadata = [o for o in packet.match.oxm_fields if o.header in (ofdef.NXM_NX_REG4,
                                                                                   ofdef.NXM_NX_REG5,
                                                                                   ofdef.NXM_NX_REG6)]
                    mac_address = mac_addr.formatter(packet.dl_dst)
                    nid = ofdef.uint32.parse(ofdef.get_oxm(packet.match.oxm_fields, ofdef.NXM_NX_REG5))
                    networks = [n for n,nid2 in self._lastlognets if nid2 == nid]
                    if networks:
                        network = networks[0]
                        current_time = time()
                        if (network, mac_address) not in self._watched_macs:
                            self._watched_macs[(network, mac_address)] = current_time + self._parent.pushtimeout
                            self._buffered_packets[(network, mac_address)] = [(msg.buffer_id,
                                                                               msg.data if msg.buffer_id == ofdef.OFP_NO_BUFFER
                                                                               else b'',
                                                                               current_time + self._parent.packetexpre,
                                                                               in_port,
                                                                               metadata
                                                                               )]
                            # Restart walk since we have already changed the watch list
                            self._update_walk()
                        else:
                            if self._watched_macs[(network, mac_address)] is not None:
                                # Update expire time
                                self._watched_macs[(network, mac_address)] = current_time + self._parent.pushtimeout
                            # If the buffers are already removed, the flow is already sent. Ignore this packet.
                            # This should rarely happen, so we do not bother drop it with a PACKET_OUT message
                            if (network, mac_address) in self._buffered_packets:
                                l = self._buffered_packets[(network, mac_address)]
                                # Drop expired packets
                                l = [p for p in l if p[2] > current_time]
                                l.append((msg.buffer_id,
                                           msg.data if msg.buffer_id == ofdef.OFP_NO_BUFFER
                                           else b'',
                                           current_time + self._parent.packetexpre,
                                           in_port,
                                           metadata
                                           ))
                                self._buffered_packets[(network, mac_address)] = l
            else:
                # A flow is expired, also remove the watch list
                msg = self.event.message
                nid = ofdef.uint32.parse(ofdef.get_oxm(msg.match.oxm_fields, ofdef.NXM_NX_REG5))
                mac_address = mac_addr.formatter(ofdef.get_oxm(msg.match.oxm_fields, ofdef.OXM_OF_ETH_DST))
                networks = [n for n,nid2 in self._lastlognets if nid2 == nid]
                if networks:
                    network = networks[0]
                    if (network, mac_address) in self._watched_macs:
                        del self._watched_macs[(network, mac_address)]
                        if (network, mac_address) in self._buffered_packets:
                            del self._buffered_packets[(network, mac_address)]
                        self._update_walk()
    def _refresh_handler(self):
        while True:
            for m in self.waitWithTimeout(self._parent.refreshinterval):
                yield m
            self.updateobjects(n for n,_ in self._lastlognets)
    def updateflow(self, conn, addvalues, removevalues, updatedvalues):
        # Following works are done in parallel:
        # 1. Modify the VXLANEndpointSet in KVDB
        # 2. Create/Modify the broadcast group for every logical network in VXLAN
        # 3. Create MAC-address <-> Tunnel IP address map in KVDB
        # 4. Create/Modify learning flows, or push the flows for every logical port
        ofdef = conn.openflowdef
        vhost = conn.protocol.vhost
        vi = self._parent._gettableindex('vxlaninput', vhost)
        vi_next = self._parent._getnexttable('', 'vxlaninput', vhost = vhost)
        vo = self._parent._gettableindex('vxlanoutput', vhost)
        vo_next = self._parent._getnexttable('', 'vxlanoutput', vhost = vhost)
        learning = self._parent._gettableindex('vxlanlearning', vhost)
        egress = self._parent._gettableindex('egress', vhost)
        allresult = set(v for v in self._savedresult if v is not None and not v.isdeleted())
        lastlognetinfo = self._lastlognetinfo
        lastphyportinfo = self._lastphyportinfo
        lastlogportinfo = self._lastlogportinfo
        currentlognetinfo = dict((n,(n.physicalnetwork,nid)) for n,nid in self._lastlognets if n in allresult and _is_vxlan(n))
        currentlogportinfo = dict((p, (p.network, pid, getattr(p, 'mac_address', None))) for p,pid in self._lastlogports
                                  if p.network in currentlognetinfo)
        phyportdict = dict((p,pid) for p,pid in self._lastphyports if p in allresult and _is_vxlan(p))
        # We only accept one physical port for VXLAN network
        unique_phyports = dict((p.physicalnetwork, p) for p,v in sorted(phyportdict.items(), key = lambda x: x[1]))
        phyportdict = dict((p,phyportdict[p]) for p in unique_phyports.values())
        currentphyportinfo = dict((p,(p.physicalnetwork,pid,lastphyportinfo[p][2])) for p,pid in phyportdict.items()
                                  if p in lastphyportinfo and lastphyportinfo[p][1] == pid)
        newports = set(phyportdict.keys())
        newports.difference_update(currentphyportinfo.keys())
        removed_ports = set(lastphyportinfo.keys()).difference(currentphyportinfo.keys())
        # Retrieve VXLAN settings from OVSDB
        newphyports = []
        datapath_id = conn.openflow_datapathid
        ovsdb_vhost = self._parent.vhostmap.get(vhost, "")
        try:
            for m in callAPI(self, 'ovsdbmanager', 'waitbridgeinfo', {'datapathid' : datapath_id,
                                                                     'vhost' : ovsdb_vhost}):
                yield m
        except Exception:
            self._logger.warning("OVSDB bridge is not ready", exc_info = True)
            raise StopIteration
        else:
            bridge, system_id, _ = self.retvalue
        if newports:
            try:
                for m in callAPI(self, 'ovsdbmanager', 'waitconnection', {'datapathid': datapath_id,
                                                                         'vhost': ovsdb_vhost}):
                    yield m
            except Exception:
                self._parent._logger.warning("OVSDB connection is not ready for datapathid = %016x, vhost = %r(%r)",
                                             datapath_id, vhost, ovsdb_vhost, exc_info = True)
            else:
                try:
                    ovsdb_conn = self.retvalue
                    port_requests = [(p, phyportdict[p]) for p in newports]
                    def wait_and_ignore(portno):
                        try:
                            for m in callAPI(self, 'ovsdbportmanager', 'waitportbyno', {'datapathid': datapath_id,
                                                                                 'portno': portno,
                                                                                 'timeout': 1,
                                                                                 'vhost': ovsdb_vhost}):
                                yield m
                        except ovsdbportmanager.OVSDBPortNotAppearException:
                            self.retvalue = None
                    for m in self.executeAll([wait_and_ignore(pid) for _,pid in port_requests]):
                        yield m
                    uuids = [r[0]['_uuid'] for r in self.retvalue]
                    method, params = ovsdb.transact('Open_vSwitch',
                                                    *[ovsdb.select('Interface', [["_uuid", "==", ovsdb.uuid(u)]],
                                                                                 ["_uuid", "options"])
                                                      for u in uuids])
                    for m in ovsdb_conn.protocol.querywithreply(method, params, ovsdb_conn, self):
                        yield m
                    src_ips = [ovsdb.omap_getvalue(r['rows'][0]['options'], "local_ip")
                               if 'error' not in r and r['rows'] and 'options' in r['rows'][0]
                               else None
                               for r in self.jsonrpc_result]
                    newphyports = [(p, (p.physicalnetwork, pid, src_ip))
                                  for (p, pid),src_ip in zip(port_requests, src_ips)
                                  if _get_ip(src_ip, ofdef) is not None]
                    currentphyportinfo.update(newphyports)
                except Exception:
                    self._parent._logger.warning("Get VXLAN configurations from OVSDB failed for datapathid = %016x, vhost = %r(%r)",
                                                 datapath_id, vhost, ovsdb_vhost, exc_info = True)
        # If there are any added or removed physical ports, do transact on all logical networks;
        # Do transact on added and removed logical networks else
        transact_networks = set(currentlognetinfo.keys()).symmetric_difference(lastlognetinfo.keys())
        if newphyports:
            newphynets = set(v[0] for _,v in newphyports)
            transact_networks.update(lognet for lognet, (phynet, _) in currentlognetinfo.items()
                                     if phynet in newphynets)
        if removed_ports:
            removephynets = set(p.physicalnetwork for p in removed_ports)
            transact_networks.update(lognet for lognet, (phynet, _) in lastlognetinfo.items()
                                     if phynet in removephynets)
        transact_networks.update(n for n in updatedvalues if isinstance(n, LogicalNetwork) and n in currentlognetinfo)
        # LogicalPortVXLANInfo
        lastvxlaninfo = self._lastvxlaninfo
        otherlogports = dict((p.id, p) for p in allresult
                           if p.isinstance(LogicalPort) and hasattr(p, 'mac_address') and p not in currentlogportinfo)
        currentvxlaninfo = dict((obj, (otherlogports[obj.id], otherlogports[obj.id].network,
                                       currentlognetinfo[otherlogports[obj.id].network][1],
                                       otherlogports[obj.id].network.vni,
                                       otherlogports[obj.id].mac_address,
                                       obj.endpoints[0],
                                       unique_phyports[otherlogports[obj.id].network.physicalnetwork],
                                       currentphyportinfo[unique_phyports[otherlogports[obj.id].network.physicalnetwork]][1]))
                                for obj in allresult
                                if obj.isinstance(LogicalPortVXLANInfo) and obj.id in otherlogports \
                                    and hasattr(obj, 'endpoints') and obj.endpoints \
                                    and (obj.endpoints[0]['vhost'], obj.endpoints[0]['systemid'], obj.endpoints[0]['bridge'])
                                            != (ovsdb_vhost, system_id, bridge) \
                                    and hasattr(otherlogports[obj.id].network, 'vni') \
                                    and otherlogports[obj.id].network in currentlognetinfo
                                    and otherlogports[obj.id].network.physicalnetwork in unique_phyports)
        self._lastvxlaninfo = currentvxlaninfo
        self._lastphyportinfo = currentphyportinfo
        self._lastlognetinfo = currentlognetinfo
        self._lastlogportinfo = currentlogportinfo
        subroutines = []
        removed_ports = {}
        created_ports = {}
        def _remove_port_tun(lognet, logport):
            if lognet in lastlognetinfo:
                phynet, _ = lastlognetinfo[lognet]
                phyport = [(p,v) for p,v in lastphyportinfo.items() if v[0] == phynet]
                if phyport:
                    phyport = phyport[0]
                    removed_ports[logport] = phyport[1][2]
        def _create_port_tun(lognet, logport):
            if lognet in currentlognetinfo:
                phynet, _ = currentlognetinfo[lognet]
                phyport = [(p,v) for p,v in currentphyportinfo.items() if v[0] == phynet]
                if phyport:
                    phyport = phyport[0]
                    created_ports[logport] = phyport[1][2]
        for logport, (lognet, _, _) in lastlogportinfo.items():
            if lognet in transact_networks:
                _remove_port_tun(lognet, logport)
        for logport in removevalues:
            if logport.isinstance(LogicalPort):
                if logport in lastlogportinfo:
                    lognet, _, _ = lastlogportinfo[logport]
                    _remove_port_tun(lognet, logport)
        for logport in updatedvalues:
            if logport.isinstance(LogicalPort):
                if logport in lastlogportinfo:
                    lognet, _, _ = lastlogportinfo[logport]
                    _remove_port_tun(lognet, logport)
        for logport in addvalues:
            if logport.isinstance(LogicalPort):
                if logport in currentlogportinfo:
                    lognet, _, _ = currentlogportinfo[logport]
                    _create_port_tun(lognet, logport)
        for logport in updatedvalues:
            if logport.isinstance(LogicalPort):
                if logport in currentlogportinfo:
                    lognet, _, _ = currentlogportinfo[logport]
                    _create_port_tun(lognet, logport)
        for logport, (lognet, _, _) in currentlogportinfo.items():
            if lognet in transact_networks:
                _create_port_tun(lognet, logport)
        all_tun_ports = set(created_ports.keys()).union(set(removed_ports.keys()))
        if transact_networks or all_tun_ports:
            def do_transact():
                network_list = list(transact_networks)
                vxlanendpoint_list = [VXLANEndpointSet.default_key(n.id) for n in network_list]
                all_tun_ports2 = list(all_tun_ports)
                def update_vxlanendpoints(keys, values, timestamp):
                    # values = List[VXLANEndpointSet]
                    # endpointlist is [src_ip, vhost, systemid, bridge, expire]
                    for v,n in zip(values[0:len(network_list)], network_list):
                        if v is not None:
                            v.endpointlist = [ep for ep in v.endpointlist
                                              if (ep[1], ep[2], ep[3]) != (ovsdb_vhost, system_id, bridge)
                                              and ep[4] >= timestamp]
                            if n.physicalnetwork in unique_phyports:
                                phyport = unique_phyports[n.physicalnetwork]
                                if phyport in currentphyportinfo:
                                    v.endpointlist.append([currentphyportinfo[phyport][2],
                                              ovsdb_vhost,
                                              system_id,
                                              bridge,
                                              None if self._parent.refreshinterval is None else
                                                    self._parent.refreshinterval * 1000000 * 2 + timestamp
                                              ])
                    written_values = {}
                    if all_tun_ports2:
                        for k,v,vxkey,vxinfo in zip(keys[len(network_list):len(network_list) + len(all_tun_ports2)],
                                       values[len(network_list):len(network_list) + len(all_tun_ports2)],
                                       keys[len(network_list) + len(all_tun_ports2):len(network_list) + 2 * len(all_tun_ports2)],
                                       values[len(network_list) + len(all_tun_ports2):len(network_list) + 2 * len(all_tun_ports2)]):
                            if v is None:
                                if vxinfo is not None:
                                    # The port is deleted? Then we should also delete the vxinfo
                                    written_values[vxkey] = None
                            else:
                                portref = v.create_reference()
                                if portref in created_ports:
                                    if vxinfo is None:
                                        vxinfo = LogicalPortVXLANInfo.create_from_key(vxkey)
                                    # There maybe more than one endpoint at the same time (on migrating)
                                    # so we keep all possible endpoints, but move our endpoint to the first place
                                    myendpoint = {'vhost': ovsdb_vhost,
                                                  'systemid': system_id,
                                                  'bridge': bridge,
                                                  'tunnel_dst': created_ports[portref],
                                                  'updated_time': timestamp}
                                    vxinfo.endpoints = [ep for ep in vxinfo.endpoints
                                                        if ep['updated_time'] + self._parent.allowedmigrationtime * 1000000 >= timestamp
                                                        and (ep['vhost'], ep['systemid'], ep['bridge']) != (ovsdb_vhost, system_id, bridge)]
                                    vxinfo.endpoints = [myendpoint] + vxinfo.endpoints
                                    written_values[vxkey] = vxinfo
                                elif portref in removed_ports:
                                    if vxinfo is not None:
                                        # Remove endpoint
                                        vxinfo.endpoints = [ep for ep in vxinfo.endpoints
                                                            if (ep['vhost'], ep['systemid'], ep['bridge']) != (ovsdb_vhost, system_id, bridge)]
                                        if not vxinfo.endpoints:
                                            written_values[vxkey] = None
                                        else:
                                            written_values[vxkey] = vxinfo
                    written_values_list = tuple(written_values.items())
                    return (tuple(itertools.chain(keys[:len(network_list)], (k for k,_ in written_values_list))),
                            tuple(itertools.chain(values[:len(network_list)], (v for _,v in written_values_list))))
                for m in callAPI(self, 'objectdb', 'transact', {'keys': tuple(vxlanendpoint_list + [p.getkey() for p in all_tun_ports2] +
                                                                              [LogicalPortVXLANInfo.default_key(p.id) for p in all_tun_ports2]),
                                                                'updater': update_vxlanendpoints,
                                                                'withtime': True
                                                                }):
                    yield m
            subroutines.append(do_transact())
        # We must create broadcast groups for VXLAN logical networks, using the information in VXLANEndpointSet
        group_cmds = []
        created_groups = {}
        deleted_groups = set()
        for lognet in removevalues:
            if lognet.isinstance(LogicalNetwork):
                if lognet in lastlognetinfo:
                    phynet, netid = lastlognetinfo[lognet]
                    if netid in self._current_groups:
                        deleted_groups.add(netid)
                        del self._current_groups[netid]
                    # Always delete to ensure non-exists
                    group_cmds.append(ofdef.ofp_group_mod(command = ofdef.OFPGC_DELETE,
                                                          type = ofdef.OFPGT_ALL,
                                                          group_id = (netid & 0xffff) | 0x10000))
        for ve in itertools.chain(addvalues, updatedvalues):
            if ve.isinstance(VXLANEndpointSet):
                lognet = ReferenceObject(LogicalNetwork.default_key(ve.id))
                if lognet in currentlognetinfo:
                    phynet, netid = currentlognetinfo[lognet]
                    if phynet in unique_phyports:
                        phyport = unique_phyports[phynet]
                        if phyport in currentphyportinfo:
                            _, portid, localip = currentphyportinfo[phyport]
                            localip_addr = _get_ip(localip, ofdef)
                            allips = [ip for ip in (_get_ip(ep[0], ofdef) for ep in ve.endpointlist
                                      if (ep[1], ep[2], ep[3]) != (ovsdb_vhost, system_id, bridge))
                                      if ip is not None and ip != localip_addr]
                            created_groups[netid] = portid
                            group_cmds.append(
                                ofdef.ofp_group_mod(
                                      command = ofdef.OFPGC_MODIFY
                                                if netid in self._current_groups
                                                else ofdef.OFPGC_ADD,
                                      type = ofdef.OFPGT_ALL,
                                      group_id = (netid & 0xffff) | 0x10000,
                                      buckets =
                                        [ofdef.ofp_bucket(
                                                actions = [
                                                    ofdef.ofp_action_set_field(
                                                        field = ofdef.create_oxm(
                                                                    ofdef.NXM_NX_TUN_IPV4_DST,
                                                                    ipaddress
                                                                )
                                                    ),
                                                    ofdef.ofp_action_output(port = portid)
                                                    ]
                                            )
                                        for ipaddress in allips
                                        ]
                                ))
        if group_cmds:
            def group_mod():
                try:
                    for m in conn.protocol.batch(group_cmds, conn, self):
                        yield m
                except Exception:
                    self._parent._logger.warning("Some Openflow commands return error result on connection %r, will ignore and continue.\n"
                                                 "Details:\n%s", conn,
                                                 "\n".join("REQUEST = \n%s\nERRORS = \n%s\n" % (pformat(dump(k)), pformat(dump(v)))
                                                           for k,v in self.openflow_replydict.items()))
                self._current_groups.update(created_groups)
                for g in deleted_groups:
                    for m in self.waitForSend(VXLANGroupChanged(conn, g, VXLANGroupChanged.DELETED)):
                        yield m
                for g,pid in created_groups.items():
                    for m in self.waitForSend(VXLANGroupChanged(conn, g, VXLANGroupChanged.UPDATED, physicalportid = pid)):
                        yield m
            subroutines.append(group_mod())
        # Finally, lets create flow entries for VXLAN input/output
        # There are three patterns with different options:
        # 1. learning: use nx_action_learn action for learning
        # 2. prepush: for each LogicalPortVXLANInfo, send a flow entry to set the tunnel destination
        # 3. prepush = False and learning = False: use output(CONTROLLER) to query at the first packet
        # We always require nicira extension, so we are not using output(CONTROLLER) for learning.
        if conn.protocol.disablenxext:
            def _create_oxms(*fields):
                return [ofdef.create_oxm(ofdef.OXM_OF_METADATA_W, reduce(lambda x,y: x | y, [f[0] for f in fields]),
                                                                reduce(lambda x,y: x | y, [f[1] for f in fields]))]
            def _outport_oxm(pid):
                return (pid & 0xffff, 0xffff)
            def _innet_oxm(nid):
                return ((nid & 0xffff) << 48, 0xffff000000000000)
            def _outnet_oxm(nid):
                return ((nid & 0xffff) << 32, 0xffff00000000)
            def _learned_flag(flag):
                return (int(bool(flag)) << 16, 0x10000)
        else:
            def _create_oxms(*fields):
                return list(fields)
            def _outport_oxm(pid):
                return ofdef.create_oxm(ofdef.NXM_NX_REG6, pid)
            def _innet_oxm(nid):
                return ofdef.create_oxm(ofdef.NXM_NX_REG4, nid)
            def _outnet_oxm(nid):
                return ofdef.create_oxm(ofdef.NXM_NX_REG5, nid)
            def _learned_flag(flag):
                return ofdef.create_oxm(ofdef.NXM_NX_REG7, int(bool(flag)))
        def flow_mod():
            cmds = []
            for p in itertools.chain(removevalues, updatedvalues):
                if p.isinstance(PhysicalPort) and p in lastphyportinfo and not p in currentphyportinfo:
                    _, pid, _ = lastphyportinfo[p]
                    if self._parent.learning:
                        cmds.append(ofdef.ofp_flow_mod(table_id = vi,
                                                       command = ofdef.OFPFC_DELETE,
                                                       priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                       buffer_id = ofdef.OFP_NO_BUFFER,
                                                       out_port = ofdef.OFPP_ANY,
                                                       out_group = ofdef.OFPG_ANY,
                                                       match = ofdef.ofp_match_oxm(
                                                                    oxm_fields = [
                                                                        ofdef.create_oxm(ofdef.OXM_OF_IN_PORT, pid)
                                                                        ]
                                                                )))
                    cmds.append(ofdef.ofp_flow_mod(table_id = vo,
                                                   command = ofdef.OFPFC_DELETE_STRICT,
                                                   priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                   buffer_id = ofdef.OFP_NO_BUFFER,
                                                   out_port = ofdef.OFPP_ANY,
                                                   out_group = ofdef.OFPG_ANY,
                                                   match = ofdef.ofp_match_oxm(
                                                                oxm_fields = _create_oxms(
                                                                        _outport_oxm(pid)
                                                                    )
                                                            )))
                    cmds.append(ofdef.ofp_flow_mod(table_id = egress,
                                                   command = ofdef.OFPFC_DELETE_STRICT,
                                                   priority = ofdef.OFP_DEFAULT_PRIORITY + 10,
                                                   buffer_id = ofdef.OFP_NO_BUFFER,
                                                   out_port = ofdef.OFPP_ANY,
                                                   out_group = ofdef.OFPG_ANY,
                                                   match = ofdef.ofp_match_oxm(
                                                                oxm_fields = _create_oxms(
                                                                    _outport_oxm(pid),
                                                                    _learned_flag(True)
                                                                    )
                                                            )))
            for m in self.execute_commands(conn, cmds):
                yield m
            del cmds[:]
            if self._parent.learning:
                # Also delete all learned flows
                for p in itertools.chain(removevalues, updatedvalues):
                    if p.isinstance(PhysicalPort) and p in lastphyportinfo:
                        _, pid, _ = lastphyportinfo[p]
                        cmds.append(ofdef.ofp_flow_mod(table_id = learning,
                                                       command = ofdef.OFPFC_DELETE,
                                                       priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                       buffer_id = ofdef.OFP_NO_BUFFER,
                                                       out_port = ofdef.OFPP_ANY,
                                                       out_group = ofdef.OFPG_ANY,
                                                       match = ofdef.ofp_match_oxm(
                                                                    oxm_fields = _create_oxms(
                                                                        _outport_oxm(pid)
                                                                        )
                                                                )))
                    elif p.isinstance(LogicalNetwork) and p in lastlognetinfo and not p in currentlognetinfo:
                        _, nid = lastlognetinfo[p]
                        cmds.append(ofdef.ofp_flow_mod(table_id = learning,
                                                       command = ofdef.OFPFC_DELETE,
                                                       priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                       buffer_id = ofdef.OFP_NO_BUFFER,
                                                       out_port = ofdef.OFPP_ANY,
                                                       out_group = ofdef.OFPG_ANY,
                                                       match = ofdef.ofp_match_oxm(
                                                                    oxm_fields = _create_oxms(
                                                                        _outnet_oxm(nid)
                                                                        )
                                                                )))
                for m in self.execute_commands(conn, cmds):
                    yield m
            for p in itertools.chain(addvalues, updatedvalues):
                if p.isinstance(PhysicalPort) and p in currentphyportinfo and p not in lastphyportinfo:
                    _, pid, _ = currentphyportinfo[p]
                    if self._parent.learning:
                        cmds.append(ofdef.ofp_flow_mod(table_id = vi,
                                                       command = ofdef.OFPFC_ADD,
                                                       priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                       buffer_id = ofdef.OFP_NO_BUFFER,
                                                       out_port = ofdef.OFPP_ANY,
                                                       out_group = ofdef.OFPG_ANY,
                                                       match = ofdef.ofp_match_oxm(
                                                                    oxm_fields = [
                                                                        ofdef.create_oxm(ofdef.OXM_OF_IN_PORT, pid)
                                                                        ]
                                                                ),
                                                       instructions = [
                                                                ofdef.ofp_instruction_actions(
                                                                    actions = [
                                                                            ofdef.nx_action_learn(
                                                                                hard_timeout = self._parent.learntimeout,
                                                                                priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                                                cookie = 0,
                                                                                table_id = learning,
                                                                                specs = [ofdef.create_nxfms_matchvalue(ofdef.NXM_NX_REG6, pid),                 # Match port
                                                                                         ofdef.create_nxfms_matchfield(ofdef.OXM_OF_ETH_SRC, ofdef.OXM_OF_ETH_DST), # Match dst MAC adddress = src MAC address
                                                                                         ofdef.create_nxfms_matchfield(ofdef.NXM_NX_REG4, ofdef.NXM_NX_REG5),       # Match output network = input network
                                                                                         ofdef.create_nxfms_loadfield(ofdef.OXM_OF_TUNNEL_ID, ofdef.OXM_OF_TUNNEL_ID),   # Set tunnel ID
                                                                                         ofdef.create_nxfms_loadfield(ofdef.NXM_NX_TUN_IPV4_SRC, ofdef.NXM_NX_TUN_IPV4_DST),
                                                                                         ofdef.create_nxfms_loadvalue(ofdef.NXM_NX_REG7, 1)
                                                                                         ]
                                                                            )
                                                                        ]
                                                                        ),
                                                                ofdef.ofp_instruction_goto_table(table_id = vi_next)
                                                                       ]
                                                       ))
                        cmds.append(ofdef.ofp_flow_mod(table_id = vo,
                                                       command = ofdef.OFPFC_ADD,
                                                       priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                       buffer_id = ofdef.OFP_NO_BUFFER,
                                                       out_port = ofdef.OFPP_ANY,
                                                       out_group = ofdef.OFPG_ANY,
                                                       match = ofdef.ofp_match_oxm(
                                                                    oxm_fields = _create_oxms(
                                                                        _outport_oxm(pid)
                                                                        )
                                                                ),
                                                       instructions = [
                                                                ofdef.ofp_instruction_actions(
                                                                    actions = [
                                                                        ofdef.nx_action_resubmit(in_port = ofdef.nx_port_no.OFPP_IN_PORT,
                                                                                                 table = learning)
                                                                        ]
                                                                ),
                                                                ofdef.ofp_instruction_goto_table(table_id = vo_next)
                                                                ]))
                    elif not self._parent.prepush:
                        cmds.append(ofdef.ofp_flow_mod(table_id = vo,
                                                       cookie = 0x2,
                                                       cookie_mask = 0xffffffffffffffff,
                                                       command = ofdef.OFPFC_ADD,
                                                       priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                       buffer_id = ofdef.OFP_NO_BUFFER,
                                                       out_port = ofdef.OFPP_ANY,
                                                       out_group = ofdef.OFPG_ANY,
                                                       match = ofdef.ofp_match_oxm(
                                                                    oxm_fields = _create_oxms(
                                                                        _outport_oxm(pid)
                                                                        )
                                                                ),
                                                       instructions = [
                                                                ofdef.ofp_instruction_actions(
                                                                    actions = [
                                                                        ofdef.ofp_action_output(port = ofdef.OFPP_CONTROLLER,
                                                                                                max_len = 32)
                                                                        ]
                                                                )]))                        
                    cmds.append(ofdef.ofp_flow_mod(table_id = egress,
                                                   command = ofdef.OFPFC_ADD,
                                                   priority = ofdef.OFP_DEFAULT_PRIORITY + 10,
                                                   buffer_id = ofdef.OFP_NO_BUFFER,
                                                   out_port = ofdef.OFPP_ANY,
                                                   out_group = ofdef.OFPG_ANY,
                                                   match = ofdef.ofp_match_oxm(
                                                                oxm_fields = _create_oxms(
                                                                              _outport_oxm(pid),
                                                                              _learned_flag(True)
                                                                            )
                                                            ),
                                                   instructions = [
                                                            ofdef.ofp_instruction_actions(
                                                                actions = [ofdef.ofp_action_output(port = pid)]
                                                            )
                                                        ]
                                                   ))
            for m in self.execute_commands(conn, cmds):
                yield m
        subroutines.append(flow_mod())
        if self._parent.prepush or (not self._parent.prepush and not self._parent.learning):
            # prepush can be used together with learning
            if self._parent.prepush:
                # Never timeout
                idle_timeout = 0
                flags = 0
            else:
                idle_timeout = self._parent.pushtimeout
                flags = ofdef.OFPFF_SEND_FLOW_REM
            def flow_mod_prepush():
                remove_cmds = []
                add_cmds = []
                current_time = time()
                def _delete_flow(pid, nid, mac_address, network):
                    flows = [ofdef.ofp_flow_mod(table_id = vo,
                                               command = ofdef.OFPFC_DELETE_STRICT,
                                               priority = ofdef.OFP_DEFAULT_PRIORITY + 9,
                                               buffer_id = ofdef.OFP_NO_BUFFER,
                                               out_port = ofdef.OFPP_ANY,
                                               out_group = ofdef.OFPG_ANY,
                                               match = ofdef.ofp_match_oxm(
                                                            oxm_fields = [ofdef.create_oxm(ofdef.NXM_NX_REG6, pid),
                                                                          ofdef.create_oxm(ofdef.NXM_NX_REG5, nid),
                                                                          ofdef.create_oxm(ofdef.OXM_OF_ETH_DST, ofdef.mac_addr(mac_address)),
                                                                          ]
                                                        )
                                               )]
                    if (network, mac_address) in self._buffered_packets:
                        # Drop any packets
                        packets = self._buffered_packets.pop((network, mac_address))
                        flows.extend(ofdef.ofp_packet_out(buffer_id = p[0],
                                                          in_port = p[3],
                                                          actions = []
                                                          )
                                     for p in packets
                                     if p[2] > current_time and p[0] != ofdef.OFP_NO_BUFFER)
                    return flows
                def _create_flow(pid, nid, mac_address, tunnelid, tunnel_dst, network, modify = False):
                    flows = [ofdef.ofp_flow_mod(table_id = vo,
                                                command = ofdef.OFPFC_MODIFY_STRICT if modify else ofdef.OFPFC_ADD, 
                                                priority = ofdef.OFP_DEFAULT_PRIORITY + 9,
                                                idle_timeout = idle_timeout,
                                                buffer_id = ofdef.OFP_NO_BUFFER,
                                                out_port = ofdef.OFPP_ANY,
                                                out_group = ofdef.OFPG_ANY,
                                                flags = flags,
                                                match = ofdef.ofp_match_oxm(
                                                             oxm_fields = [ofdef.create_oxm(ofdef.NXM_NX_REG6, pid),
                                                                           ofdef.create_oxm(ofdef.NXM_NX_REG5, nid),
                                                                           ofdef.create_oxm(ofdef.OXM_OF_ETH_DST, ofdef.mac_addr(mac_address)),
                                                                           ]
                                                         ),
                                                instructions = [
                                                        ofdef.ofp_instruction_actions(
                                                            actions = [ofdef.ofp_action_set_field(field = ofdef.create_oxm(ofdef.OXM_OF_TUNNEL_ID, tunnelid)),
                                                                       ofdef.ofp_action_set_field(field = ofdef.create_oxm(ofdef.NXM_NX_TUN_IPV4_DST, ofdef.ip4_addr(tunnel_dst))),
                                                                       ofdef.ofp_action_set_field(field = ofdef.create_oxm(ofdef.NXM_NX_REG7, 1))]
                                                        ),
                                                        ofdef.ofp_instruction_goto_table(table_id = vo_next)
                                                    ]
                                               )]
                    if (network, mac_address) in self._buffered_packets:
                        # Send buffered packets
                        packets = self._buffered_packets.pop((network, mac_address))
                        flows.extend(ofdef.ofp_packet_out(buffer_id = p[0],
                                                          in_port = p[3],
                                                          actions = [ofdef.ofp_action_set_field(field = meta) for meta in p[4]]
                                                                    + [ofdef.ofp_action_set_field(field = ofdef.create_oxm(ofdef.OXM_OF_TUNNEL_ID, tunnelid)),
                                                                       ofdef.ofp_action_set_field(field = ofdef.create_oxm(ofdef.NXM_NX_TUN_IPV4_DST, ofdef.ip4_addr(tunnel_dst))),
                                                                       ofdef.ofp_action_set_field(field = ofdef.create_oxm(ofdef.NXM_NX_REG7, 1)),
                                                                       ofdef.nx_action_resubmit(in_port = ofdef.nx_port_no.OFPP_IN_PORT,
                                                                                              table = vo_next)],
                                                          data = p[1]
                                                          )
                                     for p in packets
                                     if p[2] > current_time and p[0] != ofdef.OFP_NO_BUFFER)
                    return flows
                for vxlaninfo, value in lastvxlaninfo.items():
                    _, lognet, nid, _, mac_address, _, _, pid = value
                    if vxlaninfo not in currentvxlaninfo:
                        remove_cmds.extend(_delete_flow(pid, nid, mac_address, lognet))
                    else:
                        _, lognet2, nid2, vni2, mac_address2, endpoint2, _, pid2 = currentvxlaninfo[vxlaninfo]
                        if (pid2, nid2, mac_address2) != (pid, nid, mac_address):
                            remove_cmds.extend(_delete_flow(pid, nid, mac_address, lognet))
                            add_cmds.extend(_create_flow(pid2, nid2, mac_address2, vni2, endpoint2['tunnel_dst'], lognet2))
                        else:
                            add_cmds.extend(_create_flow(pid2, nid2, mac_address2, vni2, endpoint2['tunnel_dst'], lognet2, True))
                for vxlaninfo, value in currentvxlaninfo.items():
                    if vxlaninfo not in lastvxlaninfo:
                        _, lognet, nid, vni, mac_address, endpoint, _, pid = value
                        add_cmds.extend(_create_flow(pid, nid, mac_address, vni, endpoint['tunnel_dst'], lognet))
                for m in self.execute_commands(conn, remove_cmds):
                    yield m
                for m in self.execute_commands(conn, add_cmds):
                    yield m
            subroutines.append(flow_mod_prepush())            
        try:
            for m in self.executeAll(subroutines, retnames = ()):
                yield m
        except Exception:
            self._parent._logger.warning("Update vxlancast flow for connection %r failed with exception", conn, exc_info = True)

@defaultconfig
@depend(ofpportmanager.OpenflowPortManager, objectdb.ObjectDB, ovsdbportmanager.OVSDBPortManager)
class VXLANCast(FlowBase):
    "VXLAN single-cast and broadcast functions"
    _tablerequest = (("vxlaninput", ('l2input',), ''),
                     ("l3input", ("vxlaninput",), ''),
                     ("l2output", ("l3input",), ''),
                     ("vxlanoutput", ('l2output','vxlaninput'), ''),
                     ('egress', ('vxlanoutput', 'vxlanlearning'), ''),
                     ("vxlanlearning", ('vxlanoutput',), 'vxlanlearning'))
    _default_learning = True
    _default_prepush = True
    _default_learntimeout = 300
    _default_packetexpire = 3
    _default_pushtimeout = 60
    _default_vhostmap = {}
    _default_refreshinterval = 3600
    _default_allowedmigrationtime = 120
    def __init__(self, server):
        FlowBase.__init__(self, server)
        self.apiroutine = RoutineContainer(self.scheduler)
        self.apiroutine.main = self._main
        self.routines.append(self.apiroutine)
        self._flowupdaters = {}
        self.createAPI(publicapi(self.createioflowparts, self.apiroutine,
                                 lambda connection,logicalnetwork,**kwargs:
                                        _is_vxlan(logicalnetwork)))
    def _main(self):
        flow_init = FlowInitialize.createMatcher(_ismatch = lambda x: self.vhostbind is None or x.vhost in self.vhostbind)
        conn_down = OpenflowConnectionStateEvent.createMatcher(state = OpenflowConnectionStateEvent.CONNECTION_DOWN,
                                                               _ismatch = lambda x: self.vhostbind is None or x.createby.vhost in self.vhostbind)
        while True:
            yield (flow_init, conn_down)
            if self.apiroutine.matcher is flow_init:
                c = self.apiroutine.event.connection
                self.apiroutine.subroutine(self._init_conn(self.apiroutine.event.connection))
            else:
                c = self.apiroutine.event.connection
                self.apiroutine.subroutine(self._remove_conn(c))
    def _init_conn(self, conn):
        # Default
        ofdef = conn.openflowdef
        vhost = conn.protocol.vhost
        vi = self._gettableindex('vxlaninput', vhost)
        vi_next = self._getnexttable('', 'vxlaninput', vhost = vhost)
        vo = self._gettableindex('vxlanoutput', vhost)
        vo_next = self._getnexttable('', 'vxlanoutput', vhost = vhost)
        l3 = self._gettableindex('l3input', vhost)
        l3_next = self._getnexttable('', 'l3input', vhost = vhost)
        learning = self._gettableindex('vxlanlearning', vhost)
        if hasattr(conn, '_vxlancast_learning_routine') and conn._vxlancast_learning_routine:
            conn._vxlancast_learning_routine.close()
            delattr(conn, '_vxlancast_learning_routine')
        if conn in self._flowupdaters:
            vxlanupdater = self._flowupdaters.pop(conn)
            vxlanupdater.close()
        vxlanupdater = VXLANUpdater(conn, self)
        #flowupdater = VXLANFlowUpdater(conn, self)
        self._flowupdaters[conn] = vxlanupdater
        vxlanupdater.start()
        #flowupdater.start()
        cmds = [ofdef.ofp_flow_mod(table_id = vo,
                                    command = ofdef.OFPFC_ADD,
                                    priority = ofdef.OFP_DEFAULT_PRIORITY + 10,
                                    buffer_id = ofdef.OFP_NO_BUFFER,
                                    out_port = ofdef.OFPP_ANY,
                                    out_group = ofdef.OFPG_ANY,
                                    match = ofdef.ofp_match_oxm(
                                                oxm_fields = [
                                                    # A broadcast packet
                                                    ofdef.create_oxm(ofdef.OXM_OF_ETH_DST_W, b'\x01\x00\x00\x00\x00\x00', b'\x01\x00\x00\x00\x00\x00')
                                                    ]
                                            ),
                                    instructions = [ofdef.ofp_instruction_goto_table(table_id = vo_next)]
                                    )]
        if not conn.protocol.disablenxext and self.learning:
            cmds.append(ofdef.ofp_flow_mod(table_id = learning,
                                           out_port = ofdef.OFPP_ANY,
                                           out_group = ofdef.OFPG_ANY,
                                           command = ofdef.OFPFC_ADD,
                                           priority = 0,
                                           buffer_id = ofdef.OFP_NO_BUFFER,
                                           match = ofdef.ofp_match_oxm(),
                                           instructions = [ofdef.ofp_instruction_actions(
                                                                actions = [ofdef.ofp_action_set_field(
                                                                                field = ofdef.create_oxm(ofdef.NXM_NX_REG7, 0)
                                                                            )]
                                                            )]))
        for m in conn.protocol.batch(cmds, conn, self.apiroutine):
            yield m
    def _remove_conn(self, conn):
        # Do not need to modify flows
        if conn in self._flowupdaters:
            vxlanupdater = self._flowupdaters.pop(conn)
            vxlanupdater.close()
        if False:
            yield
    def createioflowparts(self,connection,logicalnetwork,physicalport,logicalnetworkid,physicalportid):
        #
        #  1. used in IOProcessing , when physicalport add to logicalnetwork 
        #     return : input flow match vxlan vni, input flow vlan parts actions
        #              output flow vxlan parts actions, output group bucket
        #
        group_created = False
        group_portid = None
        ofdef = connection.openflowdef
        if connection in self._flowupdaters:
            # Wait for group creation
            vxlanupdater = self._flowupdaters[connection]
            try:
                for m in vxlanupdater.wait_for_group(self.apiroutine, logicalnetworkid):
                    yield m
            except Exception:
                self._logger.warning("Group is not created, connection = %r, logicalnetwork = %r, logicalnetworkid = %r", connection, logicalnetwork, logicalnetworkid, exc_info = True)
            else:
                group_created = True
                group_portid = self.apiroutine.retvalue
        if not group_created:
            self.apiroutine.retvalue = ([ofdef.create_oxm(ofdef.OXM_OF_TUNNEL_ID, getattr(logicalnetwork, 'vni', 0))],
                                        [],
                                        [],
                                        [])
        elif group_portid == physicalportid:
            self.apiroutine.retvalue = ([ofdef.create_oxm(ofdef.OXM_OF_TUNNEL_ID, getattr(logicalnetwork, 'vni', 0))],
                                        [],
                                        [ofdef.ofp_action_set_field(field = ofdef.create_oxm(ofdef.OXM_OF_TUNNEL_ID, getattr(logicalnetwork, 'vni', 0))),
                                         ofdef.ofp_action_group(group_id = (logicalnetworkid & 0xffff) | 0x10000)],
                                        [ofdef.ofp_action_set_field(field = ofdef.create_oxm(ofdef.OXM_OF_TUNNEL_ID, getattr(logicalnetwork, 'vni', 0))),
                                         ofdef.ofp_action_group(group_id = (logicalnetworkid & 0xffff) | 0x10000)])
        else:
            self.apiroutine.retvalue = ([ofdef.create_oxm(ofdef.OXM_OF_TUNNEL_ID, getattr(logicalnetwork, 'vni', 0))],
                                        [],
                                        [ofdef.ofp_action_set_field(field = ofdef.create_oxm(ofdef.OXM_OF_TUNNEL_ID, getattr(logicalnetwork, 'vni', 0))),
                                         ofdef.ofp_action_group(group_id = (logicalnetworkid & 0xffff) | 0x10000)],
                                        [])
#
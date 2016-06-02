'''
Created on 2016/5/30

:author: hubo
'''


from vlcp.config.config import defaultconfig
from vlcp.server.module import depend, publicapi, callAPI
from vlcp.service.sdn.flowbase import FlowBase
from vlcp.service.sdn.ofpmanager import FlowInitialize
from vlcp.utils.flowupdater import FlowUpdater
from vlcp.utils.networkmodel import PhysicalPort, LogicalPort, LogicalNetwork, VXLANEndpointSet
import vlcp.service.kvdb.objectdb as objectdb
import vlcp.service.sdn.ofpportmanager as ofpportmanager
from vlcp.event.runnable import RoutineContainer
from vlcp.protocol.openflow.openflow import OpenflowConnectionStateEvent,\
    OpenflowAsyncMessageEvent, OpenflowErrorResultException
from vlcp.utils.ethernet import ethernet_l2
import vlcp.service.sdn.ioprocessing as iop
import itertools
from namedstruct.namedstruct import dump
from pprint import pformat
from vlcp.event.event import Event, withIndices
from vlcp.service.sdn import ovsdbportmanager
from vlcp.utils import ovsdb
from vlcp.utils.dataobject import updater, ReferenceObject

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

class VXLANDatabaseUpdater(FlowUpdater):
    def __init__(self, connection, parent):
        FlowUpdater.__init__(self, connection, (), ('VXLANDatabaseUpdater', connection))
        self._parent = parent
        self._lastlognets = ()
        self._lastphyports = ()
        self._lastphyportinfo = {}
        self._lastlognetinfo = {}
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
        try:
            phynet = walk(value.physicalnetwork.getkey())
        except KeyError:
            pass
        else:
            if phynet is not None and getattr(phynet, 'type') == 'vxlan':
                try:
                    vxlan_endpoint = walk(VXLANEndpointSet.default_key(value.id))
                except KeyError:
                    pass
                else:
                    if vxlan_endpoint is not None:
                        save(vxlan_endpoint.getkey())
    def _walk_phyport(self, key, value, walk, save):
        save(key)
    def _update_handler(self):
        dataobjectchanged = iop.DataObjectChanged.createMatcher(None, None, self._connection, None, True)
        dataobjectchanged2 = iop.DataObjectChanged.createMatcher(None, None, self._connection, None, False, True)
        while True:
            yield (dataobjectchanged, dataobjectchanged2)
            _, self._lastphyports, self._lastlognets, _ = self.event.current
            phyport_keys = [p.getkey() for p,_ in self._lastphyports]
            lognet_keys = [n.getkey() for n,_ in self._lastlognets]
            self._initialkeys = phyport_keys + lognet_keys                                
            self._walkerdict = dict(itertools.chain(((n, self._walk_lognet) for n in lognet_keys),
                                                    ((p, self._walk_phyport) for p in phyport_keys)))
            self.subroutine(self.restart_walk(), False)
    def _refresh_handler(self):
        while True:
            for m in self.waitWithTimeout(self._parent.refreshinterval):
                yield m
            self.updateobjects(n for n,_ in self._lastlognets)
    def updateflow(self, conn, addvalues, removevalues, updatedvalues):
        ofdef = conn.openflowdef
        vhost = conn.protocol.vhost
        vi = self._parent._gettableindex('vxlaninput', vhost)
        vi_next = self._parent._getnexttable('', 'vxlaninput', vhost = vhost)
        vo = self._parent._gettableindex('vxlanoutput', vhost)
        vo_next = self._parent._getnexttable('', 'vxlanoutput', vhost = vhost)
        learning = self._parent._gettableindex('vxlanlearning', vhost)
        allresult = set(self._savedresult)
        lastlognetinfo = self._lastlognetinfo
        lastphyportinfo = self._lastphyportinfo
        currentlognetinfo = dict((n,(n.physicalnetwork,nid)) for n,nid in self._lastlognets if n in allresult and _is_vxlan(n))
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
        if newphyports or removed_ports:
            transact_networks = set(lastlognetinfo.keys()).union(currentlognetinfo.keys())
        else:
            transact_networks = set(currentlognetinfo.keys()).symmetric_difference(lastlognetinfo.keys())
        transact_networks.update(n for n in updatedvalues if isinstance(n, LogicalNetwork) and n in currentlognetinfo)
        self._lastphyportinfo = currentphyportinfo
        self._lastlognetinfo = currentlognetinfo
        subroutines = []
        if transact_networks:
            def do_transact():
                network_list = list(transact_networks)
                vxlanendpoint_list = [VXLANEndpointSet.default_key(n.id) for n in network_list]
                def update_vxlanendpoints(keys, values, timestamp):
                    # values = List[VXLANEndpointSet]
                    # endpointlist is [src_ip, vhost, systemid, bridge, expire]
                    for v,n in zip(values, network_list):
                        if v is not None:
                            v.endpointlist = [ep for ep in v.endpointlist
                                              if (ep[1], ep[2], ep[3]) == (ovsdb_vhost, system_id, bridge)
                                              or ep[4] < timestamp]
                            if n.physicalnetwork in unique_phyports:
                                phyport = unique_phyports[n.physicalnetwork]
                                if phyport in currentphyportinfo:
                                    v.endpointlist.append([currentphyportinfo[phyport][2],
                                              ovsdb_vhost,
                                              system_id,
                                              bridge,
                                              None if self._parent.refreshinterval is None else
                                                    self._parent.refreshinterval * 2 + timestamp
                                              ])
                    return (keys, values)
                for m in callAPI(self, 'objectdb', 'transact', {'keys': vxlanendpoint_list,
                                                                'updater': update_vxlanendpoints,
                                                                'withtime': True
                                                                }):
                    yield m
            subroutines.append(do_transact())
        # We must create broadcast groups for VXLAN logical networks, using the information in VXLANEndpointSet
        group_cmds = []
        created_groups = set()
        deleted_groups = set()
        for lognet in removevalues:
            if lognet.isinstance(LogicalNetwork):
                if lognet in lastlognetinfo:
                    phynet, netid = lastlognetinfo[lognet]
                    if netid in self._current_groups:
                        deleted_groups.add(netid)
                        del self._current_groups[netid]
                    # Always delete to ensure
                    group_cmds.append(ofdef.ofp_group_mod(command = ofdef.OFPGC_DELETE,
                                                          type = ofdef.OFPGT_ALL,
                                                          groupid = (netid & 0xffff) | 0x10000))
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
                            created_groups.add(netid)
                            group_cmds.append(
                                ofdef.ofp_group_mod(
                                      command = ofdef.OFPGC_MODIFY
                                                if netid in self._current_groups
                                                else ofdef.OFPGC_ADD,
                                      type = ofdef.OFPGT_ALL,
                                      groupid = (netid & 0xffff) | 0x10000,
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
                for g in created_groups:
                    for m in self.waitForSend(VXLANGroupChanged(conn, g, VXLANGroupChanged.UPDATED)):
                        yield m
            subroutines.append(group_mod())
        try:
            for m in self.executeAll(subroutines, retnames = ()):
                yield m
        except Exception:
            self._parent._logger.warning("Update l2switch flow for connection %r failed with exception", conn, exc_info = True)

@defaultconfig
@depend(ofpportmanager.OpenflowPortManager, objectdb.ObjectDB, ovsdbportmanager.OVSDBPortManager)
class VXLANCast(FlowBase):
    "VXLAN single-cast and broadcast functions"
    _tablerequest = (("vxlaninput", ('l2input',), ''),
                     ("vxlanoutput", ('l2output','vxlaninput'), ''),
                     ('egress', ('vxlanoutput', 'vxlanlearning'), ''),
                     ("vxlanlearning", ('vxlanoutput',), 'vxlanlearning'))
    _default_learning = True
    _default_prepush = True
    _default_nxlearn = True
    _default_learntimeout = 300
    _default_vhostmap = {}
    _default_refreshinterval = 3600
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
        if hasattr(conn, '_vxlancast_learning_routine') and conn._vxlancast_learning_routine:
            conn._vxlancast_learning_routine.close()
            delattr(conn, '_vxlancast_learning_routine')
        if conn in self._flowupdaters:
            dbupdater, flowupdater = self._flowupdaters[conn]
            dbupdater.close()
            #flowupdater.close()
            del self._flowupdaters[conn]
        dbupdater = VXLANDatabaseUpdater(conn, self)
        flowupdater = None
        #flowupdater = VXLANFlowUpdater(conn, self)
        self._flowupdaters[conn] = (dbupdater, flowupdater)
        dbupdater.start()
        #flowupdater.start()
        for m in conn.protocol.batch((ofdef.ofp_flow_mod(table_id = vi,
                                                cookie = 0x1,
                                                cookie_mask = 0xffffffffffffffff,
                                                   out_port = ofdef.OFPP_ANY,
                                                   out_group = ofdef.OFPG_ANY,
                                                   command = ofdef.OFPFC_ADD,
                                                   priority = 0,
                                                   buffer_id = ofdef.OFP_NO_BUFFER,
                                                   match = ofdef.ofp_match_oxm(),
                                                   instructions = [ofdef.ofp_instruction_goto_table(table_id = vi_next)]
                                                   ),
                              ofdef.ofp_flow_mod(table_id = vo,
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
                                                   ),
                              ofdef.ofp_flow_mod(table_id = vo,
                                                cookie = 0x1,
                                                cookie_mask = 0xffffffffffffffff,
                                                   out_port = ofdef.OFPP_ANY,
                                                   out_group = ofdef.OFPG_ANY,
                                                   command = ofdef.OFPFC_ADD,
                                                   priority = 0,
                                                   buffer_id = ofdef.OFP_NO_BUFFER,
                                                   match = ofdef.ofp_match_oxm(),
                                                   instructions = [ofdef.ofp_instruction_goto_table(table_id = vo_next)]
                                                   ),
                              ), conn, self.apiroutine):
            yield m
    def _remove_conn(self, conn):
        # Do not need to modify flows
        if conn in self._flowupdaters:
            dbupdater, flowupdater = self._flowupdaters[conn]
            dbupdater.close()
            #flowupdater.close()
            del self._flowupdaters[conn]
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
            dbupdater, _ = self._flowupdaters[connection]
            try:
                for m in dbupdater.wait_for_group(self.apiroutine, logicalnetworkid):
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
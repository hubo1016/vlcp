'''
Created on 2016/7/4

:author: hubo
'''


from vlcp.config.config import defaultconfig
from vlcp.server.module import depend, api
from vlcp.service.sdn.flowbase import FlowBase
from vlcp.service.sdn.ofpmanager import FlowInitialize
from vlcp.utils.flowupdater import FlowUpdater
from vlcp.utils.networkmodel import PhysicalPort, LogicalPort, LogicalNetwork,\
    LogicalNetworkMap
import vlcp.service.kvdb.objectdb as objectdb
import vlcp.service.sdn.ofpportmanager as ofpportmanager
from vlcp.event.runnable import RoutineContainer
from vlcp.protocol.openflow.openflow import OpenflowConnectionStateEvent,\
    OpenflowAsyncMessageEvent, OpenflowErrorResultException
from vlcp.utils.ethernet import ethernet_l2
import vlcp.service.sdn.ioprocessing as iop
import itertools
from vlcp.utils.dataobject import ReferenceObject
from vlcp.utils.exceptions import WalkKeyNotRetrieved
from contextlib import suppress
from vlcp.event.event import M_


class ARPUpdater(FlowUpdater):
    def __init__(self, connection, parent):
        FlowUpdater.__init__(self, connection, (), ('ARPUpdater', connection), parent._logger)
        self._parent = parent
        self._lastlognets = ()
        self._lastphyports = ()
        self._lastlogports = ()
        self._lastlogportinfo = {}
        self._lastphyportinfo = {}
        self._lastlognetinfo = {}
        self._last_arps = set()
        self._last_freearps = {}

    async def main(self):
        try:
            if self._connection.protocol.disablenxext:
                self._logger.warning("ARP responder disabled on connection %r because Nicira extension is not enabled", self._connection)
                return
            self.subroutine(self._update_handler(), True, '_update_handler_routine')
            await FlowUpdater.main(self)
        finally:
            if hasattr(self, '_update_handler_routine'):
                self._update_handler_routine.close()

    async def _update_handler(self):
        dataobjectchanged = iop.DataObjectChanged.createMatcher(None, None, self._connection)
        while True:
            ev = await dataobjectchanged
            self._lastlogports, self._lastphyports, self._lastlognets, _ = ev.current
            self._update_walk()
            self.updateobjects((p for p,_ in self._lastlogports))

    def _walk_logport(self, key, value, walk, save):
        if value is not None:
            save(key)

    def _walk_phyport(self, key, value, walk, save):
        if value is not None:
            save(key)

    def _walk_lognet(self, key, value, walk, save):
        save(key)
        if value is None:
            return
        if self._parent.prepush:
            # Acquire all logical ports
            with suppress(WalkKeyNotRetrieved):
                netmap = walk(LogicalNetworkMap.default_key(value.id))
                save(netmap.getkey())
                for logport in netmap.ports.dataset():
                    with suppress(WalkKeyNotRetrieved):
                        p = walk(logport.getkey())
                        #if p is not None and hasattr(p, 'mac_address') and hasattr(p, 'ip_address'):
                        save(logport.getkey())

    def _update_walk(self):
        logport_keys = [p.getkey() for p,_ in self._lastlogports]
        phyport_keys = [p.getkey() for p,_ in self._lastphyports]
        lognet_keys = [n.getkey() for n,_ in self._lastlognets]
        lognet_mapkeys = [LogicalNetworkMap.default_key(n.id) for n,_ in self._lastlognets]
        if self._parent.prepush:
            self._initialkeys = logport_keys + lognet_keys + phyport_keys + lognet_mapkeys
        else:
            self._initialkeys = logport_keys + lognet_keys + phyport_keys
        self._walkerdict = dict(itertools.chain(((n, self._walk_lognet) for n in lognet_keys),
                                                ((p, self._walk_logport) for p in logport_keys),
                                                ((p, self._walk_phyport) for p in phyport_keys)))
        self.subroutine(self.restart_walk(), False)

    async def updateflow(self, connection, addvalues, removevalues, updatedvalues):
        try:
            allobjs = set(o for o in self._savedresult if o is not None and not o.isdeleted())
            lastlogportinfo = self._lastlogportinfo
            lastlognetinfo = self._lastlognetinfo
            lastphyportinfo = self._lastphyportinfo
            lastfreearps = self._last_freearps
            currentlognetinfo = dict((n,(id, n.physicalnetwork)) for n,id in self._lastlognets if n in allobjs)
            netdict = dict((n.getkey(), n) for n in currentlognetinfo)
            currentlogportinfo = dict((p, (id, p.network)) for p,id in self._lastlogports if p in allobjs)
            currentphyportinfo = dict((p, (id, p.physicalnetwork)) for p,id in self._lastphyports if p in allobjs)
            last_arps = self._last_arps
            current_arps = {}
            if connection in self._parent._extra_arps:
                _arps_list = self._parent._extra_arps[connection]
                for ip, mac, lognetid, islocal in _arps_list:
                    lognet = netdict.get(LogicalNetwork.default_key(lognetid))
                    if lognet is not None:
                        if islocal is None:
                            arp_set = current_arps.setdefault(lognet, {})
                            arp_set[(ip, True)] = (mac, False)
                            arp_set[(ip, False)] = (mac, False)
                        else:
                            arp_set = current_arps.setdefault(lognet, {})
                            arp_set[(ip, islocal)] = (mac, False)
            broadcast_only = self._parent.broadcastonly
            currentfreearps = {}
            if self._parent.prepush:
                for obj in self._savedresult:
                    if obj is not None and not obj.isdeleted() and obj.isinstance(LogicalPort) and hasattr(obj, 'ip_address') and hasattr(obj, 'mac_address'):
                        if obj.network in currentlognetinfo:
                            arp_set = current_arps.setdefault(obj.network, {})
                            arp_set[(obj.ip_address, True)] = (obj.mac_address, broadcast_only)
                            if obj in currentlogportinfo:
                                # This port is in local switch, add a free ARP flow
                                pid, network = currentlogportinfo[obj]
                                currentfreearps[(network, obj.ip_address, obj)] = (obj.mac_address, broadcast_only,
                                                                                       network.physicalnetwork.type in self._parent.enable_freearp_networktypes and \
                                                                                        self._parent.enable_freearp)
            # Flow mapping:
            
            # arp_set => create_flow
            # currentfreearps => create_flow2
            self._lastlogportinfo = currentlogportinfo
            self._lastlognetinfo = currentlognetinfo
            self._lastphyportinfo = currentphyportinfo
            self._last_arps = current_arps
            self._last_freearps = currentfreearps
            cmds = []
            ofdef = connection.openflowdef
            vhost = connection.protocol.vhost
            arp = self._parent._gettableindex('arp', vhost)
            l2out_next = self._parent._getnexttable('', 'l2output', vhost = vhost)
            arp_next = self._parent._getnexttable('', 'arp', vhost = vhost)
            #===================================================================
            # if connection.protocol.disablenxext:
            #     def match_network(nid):
            #         return ofdef.create_oxm(ofdef.OXM_OF_METADATA_W, (nid & 0xFFFF) << 48, 0xFFFF000000000000)
            #     def create_instructions(actions, pid):
            #         return [ofdef.ofp_instruction_actions(actions = actions),
            #                 ofdef.ofp_instruction_write_metadata(metadata = 0x0000000080000000 | (pid & 0xffff),
            #                                                      metadata_mask = 0x000000008000ffff),
            #                 ofdef.ofp_instruction_goto_table(table_id = l2out_next)]
            #===================================================================
            #else:
            def match_network(nid):
                return ofdef.create_oxm(ofdef.NXM_NX_REG4, nid)
            def create_instructions(actions):
                return [ofdef.ofp_instruction_actions(actions = actions + \
                                                      [ofdef.nx_action_reg_load(
                                                            ofs_nbits = ofdef.create_ofs_nbits(15,1),
                                                            dst = ofdef.NXM_NX_REG7,
                                                            value = 1
                                                        ),
                                                       ofdef.nx_action_reg_move(
                                                            n_bits = 32,
                                                            src = ofdef.OXM_OF_IN_PORT,
                                                            dst = ofdef.NXM_NX_REG6
                                                            )]),
                        ofdef.ofp_instruction_goto_table(table_id = l2out_next)]
            for n in last_arps:
                if n not in current_arps:
                    if n in lastlognetinfo:
                        nid, _ = lastlognetinfo[n]
                        for ip, islocal in last_arps[n]:
                            cmds.append(ofdef.ofp_flow_mod(table_id = arp,
                                                           cookie = 0x1 | (0x2 if islocal else 0),
                                                           cookie_mask = 0x7,
                                                           command = ofdef.OFPFC_DELETE,
                                                           buffer_id = ofdef.OFP_NO_BUFFER,
                                                           out_port = ofdef.OFPP_ANY,
                                                           out_group = ofdef.OFPG_ANY,
                                                           match = ofdef.ofp_match_oxm(
                                                                        oxm_fields = [match_network(nid),
                                                                                      ofdef.create_oxm(
                                                                                          ofdef.OXM_OF_ETH_TYPE,
                                                                                          ofdef.ETHERTYPE_ARP),
                                                                                      ofdef.create_oxm(
                                                                                          ofdef.OXM_OF_ARP_TPA,
                                                                                          ofdef.ip4_addr(ip)),
                                                                                      ofdef.create_oxm(
                                                                                          ofdef.OXM_OF_ARP_OP,
                                                                                          ofdef.ARPOP_REQUEST)
                                                                                      ]
                                                                    )
                                                           ))
                else:
                    if n in lastlognetinfo and n in currentlognetinfo and currentlognetinfo[n] != lastlognetinfo[n]:
                        # Delete all because network changes
                        nid, _ = lastlognetinfo[n]
                        for ip, islocal in last_arps[n]:
                            cmds.append(ofdef.ofp_flow_mod(table_id = arp,
                                                           cookie = 0x1 | (0x2 if islocal else 0),
                                                           cookie_mask = 0x7,
                                                           command = ofdef.OFPFC_DELETE,
                                                           buffer_id = ofdef.OFP_NO_BUFFER,
                                                           out_port = ofdef.OFPP_ANY,
                                                           out_group = ofdef.OFPG_ANY,
                                                           match = ofdef.ofp_match_oxm(
                                                                        oxm_fields = [match_network(nid),
                                                                                      ofdef.create_oxm(
                                                                                          ofdef.OXM_OF_ETH_TYPE,
                                                                                          ofdef.ETHERTYPE_ARP),
                                                                                      ofdef.create_oxm(
                                                                                          ofdef.OXM_OF_ARP_TPA,
                                                                                          ofdef.ip4_addr(ip)),
                                                                                      ofdef.create_oxm(
                                                                                          ofdef.OXM_OF_ARP_OP,
                                                                                          ofdef.ARPOP_REQUEST)
                                                                                      ]
                                                                    )
                                                           ))
                    else:
                        if n in currentlognetinfo:
                            nid, _ = currentlognetinfo[n]
                            for (ip,islocal), value in last_arps[n].items():
                                if (ip,islocal) not in current_arps[n] or value != current_arps[n]:
                                    cmds.append(ofdef.ofp_flow_mod(table_id = arp,
                                                                   cookie = 0x1 | (0x2 if islocal else 0),
                                                                   cookie_mask = 0x7,
                                                                   command = ofdef.OFPFC_DELETE,
                                                                   buffer_id = ofdef.OFP_NO_BUFFER,
                                                                   out_port = ofdef.OFPP_ANY,
                                                                   out_group = ofdef.OFPG_ANY,
                                                                   match = ofdef.ofp_match_oxm(
                                                                                oxm_fields = [match_network(nid),
                                                                                              ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE, ofdef.ETHERTYPE_ARP),
                                                                                              ofdef.create_oxm(ofdef.OXM_OF_ARP_TPA, ofdef.ip4_addr(ip)),
                                                                                              ofdef.create_oxm(ofdef.OXM_OF_ARP_OP, ofdef.ARPOP_REQUEST)
                                                                                              ]
                                                                            )
                                                                   ))
            for (network, ip, port), value in lastfreearps.items():
                if (network, ip, port) not in currentfreearps or currentfreearps[(network, ip, port)] != value \
                        or lastlogportinfo.get(port) != currentlogportinfo.get(port) \
                        or lastlognetinfo.get(network) != currentlognetinfo.get(network):
                    if port in lastlogportinfo and network in lastlognetinfo:
                        pid, _ = lastlogportinfo[port]
                        nid, _ = lastlognetinfo[network]
                        cmds.append(ofdef.ofp_flow_mod(table_id = arp,
                                                       cookie = 0x5 | (0x2 if islocal else 0),
                                                       cookie_mask = 0x7,
                                                       command = ofdef.OFPFC_DELETE,
                                                       buffer_id = ofdef.OFP_NO_BUFFER,
                                                       out_port = ofdef.OFPP_ANY,
                                                       out_group = ofdef.OFPG_ANY,
                                                       match = ofdef.ofp_match_oxm(
                                                                    oxm_fields = [ofdef.create_oxm(ofdef.OXM_OF_IN_PORT, pid),
                                                                                  match_network(nid),
                                                                                  ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE, ofdef.ETHERTYPE_ARP),
                                                                                  ofdef.create_oxm(ofdef.OXM_OF_ARP_TPA, ofdef.ip4_addr(ip)),
                                                                                  ofdef.create_oxm(ofdef.OXM_OF_ARP_OP, ofdef.ARPOP_REQUEST)
                                                                                  ]
                                                                )
                                                       ))
            await self.execute_commands(connection, cmds)
            del cmds[:]
            # Create flows
            def _create_flow(ip, mac, nid, islocal, broadcast):
                return ofdef.ofp_flow_mod(
                               table_id = arp,
                               cookie = 0x1 | (0x2 if islocal else 0),
                               cookie_mask = 0xffffffffffffffff,
                               command = ofdef.OFPFC_ADD,
                               buffer_id = ofdef.OFP_NO_BUFFER,
                               out_port = ofdef.OFPP_ANY,
                               out_group = ofdef.OFPG_ANY,
                               priority = ofdef.OFP_DEFAULT_PRIORITY,
                               match = ofdef.ofp_match_oxm(
                                            oxm_fields = [
                                                  ofdef.create_oxm(ofdef.NXM_NX_REG7_W, 0x4000 if islocal else 0, 0x4000),
                                                  match_network(nid),
                                                  ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE, ofdef.ETHERTYPE_ARP),
                                                  ofdef.create_oxm(ofdef.OXM_OF_ARP_TPA, ofdef.ip4_addr(ip)),
                                                  ofdef.create_oxm(ofdef.OXM_OF_ARP_OP, ofdef.ARPOP_REQUEST)]
                                                + ([ofdef.create_oxm(ofdef.OXM_OF_ETH_DST_W, b'\x01\x00\x00\x00\x00\x00', b'\x01\x00\x00\x00\x00\x00')]
                                                  if broadcast else [])
                                        ),
                               instructions = create_instructions([ofdef.nx_action_reg_move(
                                                                n_bits = 32,
                                                                src = ofdef.OXM_OF_ARP_SPA,
                                                                dst = ofdef.OXM_OF_ARP_TPA),
                                                           ofdef.nx_action_reg_move(
                                                                n_bits = 48,
                                                                src = ofdef.OXM_OF_ARP_SHA,
                                                                dst = ofdef.OXM_OF_ARP_THA),
                                                           ofdef.nx_action_reg_move(
                                                                n_bits = 48,
                                                                src = ofdef.OXM_OF_ETH_SRC,
                                                                dst = ofdef.OXM_OF_ETH_DST),
                                                           ofdef.ofp_action_set_field(
                                                                  field = ofdef.create_oxm(
                                                                        ofdef.OXM_OF_ARP_SPA,
                                                                        ofdef.ip4_addr(ip)
                                                                    )),
                                                           ofdef.ofp_action_set_field(
                                                                  field = ofdef.create_oxm(
                                                                        ofdef.OXM_OF_ARP_SHA,
                                                                        ofdef.mac_addr(mac),
                                                                    )),
                                                           ofdef.ofp_action_set_field(
                                                                  field = ofdef.create_oxm(
                                                                        ofdef.OXM_OF_ETH_SRC,
                                                                        ofdef.mac_addr(mac),
                                                                    )),
                                                            ofdef.ofp_action_set_field(
                                                                  field = ofdef.create_oxm(
                                                                        ofdef.OXM_OF_ARP_OP,
                                                                        ofdef.ARPOP_REPLY
                                                                    )
                                                                )
                                                           ])
                               )
            def _create_flow2(ip, mac, nid, pid, islocal, broadcast,ifpass):
                if ifpass:
                    ins = [ofdef.ofp_instruction_goto_table(table_id=arp_next)]
                else:
                    ins = [ofdef.ofp_instruction_actions(type=ofdef.OFPIT_CLEAR_ACTIONS)]
                return ofdef.ofp_flow_mod(
                               table_id = arp,
                               cookie = 0x5 | (0x2 if islocal else 0),
                               cookie_mask = 0xffffffffffffffff,
                               command = ofdef.OFPFC_ADD,
                               buffer_id = ofdef.OFP_NO_BUFFER,
                               out_port = ofdef.OFPP_ANY,
                               out_group = ofdef.OFPG_ANY,
                               priority = ofdef.OFP_DEFAULT_PRIORITY + 10,
                               match = ofdef.ofp_match_oxm(
                                            oxm_fields = [
                                                  ofdef.create_oxm(ofdef.OXM_OF_IN_PORT, pid),
                                                  match_network(nid),
                                                  ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE, ofdef.ETHERTYPE_ARP),
                                                  ofdef.create_oxm(ofdef.OXM_OF_ARP_TPA, ofdef.ip4_addr(ip)),
                                                  ofdef.create_oxm(ofdef.OXM_OF_ARP_OP, ofdef.ARPOP_REQUEST)]
                                                + ([ofdef.create_oxm(ofdef.OXM_OF_ETH_DST_W, b'\x01\x00\x00\x00\x00\x00', b'\x01\x00\x00\x00\x00\x00')]
                                                  if broadcast else [])
                                        ),
                               instructions=ins
                               )
            for (network, ip, port), value in currentfreearps.items():
                if (network, ip, port) not in lastfreearps or lastfreearps[(network, ip, port)] != value \
                        or lastlogportinfo.get(port) != currentlogportinfo.get(port) \
                        or lastlognetinfo.get(network) != currentlognetinfo.get(network):
                    if port in currentlogportinfo and network in currentlognetinfo:
                        pid, _ = currentlogportinfo[port]
                        nid, _ = currentlognetinfo[network]
                        mac, broadcast, ifpass = value
                        cmds.append(_create_flow2(ip, mac, nid, pid, True, broadcast, ifpass))
            # phynetdict = {}
            # for n in current_arps:
            #     phynet = n.physicalnetwork
            #     phynetdict.setdefault(phynet, []).append(n)
            #===================================================================
            # for p in currentphyportinfo:
            #     if p not in lastphyportinfo or lastphyportinfo[p] != currentphyportinfo[p]:
            #         pid, phynet = currentphyportinfo[p]
            #         if phynet in phynetdict:
            #             for n in phynetdict[phynet]:
            #                 if n in current_arps:
            #                     nid, _ = currentlognetinfo[n]
            #                     for ip, mac, islocal, port, broadcast in current_arps[n]:
            #                         if not islocal and port != p:
            #                             cmds.append(_create_flow(ip, mac, nid, islocal, broadcast))
            #===================================================================
            for n, arps in current_arps.items():
                if n in currentlognetinfo:
                    nid, _ = currentlognetinfo[n]
                    if n not in last_arps or n not in lastlognetinfo or lastlognetinfo[n] != currentlognetinfo[n]:
                        send_arps = arps
                    else:
                        send_arps = {k:v for k,v in arps.items()
                                     if k not in last_arps[n] or last_arps[n] != v}
                    for (ip, islocal), (mac, broadcast) in send_arps.items():
                        cmds.append(_create_flow(ip, mac, nid, islocal, broadcast))
            await self.execute_commands(connection, cmds)
        except Exception:
            self._logger.warning("Unexpected exception in ARPUpdater. Will ignore and continue.", exc_info = True)

                            
        
@defaultconfig
@depend(ofpportmanager.OpenflowPortManager, objectdb.ObjectDB)
class ARPResponder(FlowBase):
    "Send ARP respond"
    _tablerequest = (("arp", ('l2input',), ''),
                     ("l3input", ('arp',), ''),
                     ("l2output", ("l3input",), ''),
                     ("egress", ("l2output",), ''))
    # Prepush ARP entries with Flows, so the switch directly responds an ARP request
    _default_prepush = True
    # When using prepush=True, only responds a broadcast ARP request; let unicast ARP request
    # reach the other side
    _default_broadcastonly = True
    # Drop ARP requests with unknown IP addresses. The ports will not be able to access external IPs,
    # so use with caution.
    _default_disableothers = False
    _default_enable_freearp = True
    _default_enable_freearp_networktypes = ("vlan", "native")

    def __init__(self, server):
        FlowBase.__init__(self, server)
        self.apiroutine = RoutineContainer(self.scheduler)
        self.apiroutine.main = self._main
        self.routines.append(self.apiroutine)
        self._flowupdaters = {}
        self._extra_arps = {}
        self.createAPI(api(self.createproxyarp),
                       api(self.removeproxyarp))

    def createproxyarp(self, connection, arpentries):
        '''
        Create ARP respond flow for specified ARP entries, each is a tuple
        ``(ip_address, mac_address, logical_network_id, local)``. When local is True,
        only respond to ARP request from logical port; when local is False,
        only respond to ARP request from physical port; respond to both else. 
        '''
        arp_list = self._extra_arps.setdefault(connection, [])
        arp_list.extend(arpentries)
        if connection in self._flowupdaters:
            self._flowupdaters[connection].updateobjects([ReferenceObject(LogicalNetwork.default_key(nid)) for _, _, nid, _ in arpentries])

    def removeproxyarp(self, connection, arpentries):
        '''
        Remove specified ARP entries.
        '''
        arp_list = self._extra_arps[connection]
        for entry in arpentries:
            try:
                arp_list.remove(entry)
            except KeyError:
                pass
        if connection in self._flowupdaters:
            self._flowupdaters[connection].updateobjects([ReferenceObject(LogicalNetwork.default_key(nid)) for _, _, nid, _ in arpentries])

    async def _main(self):
        flow_init = FlowInitialize.createMatcher(_ismatch = lambda x: self.vhostbind is None or x.vhost in self.vhostbind)
        conn_down = OpenflowConnectionStateEvent.createMatcher(state = OpenflowConnectionStateEvent.CONNECTION_DOWN,
                                                               _ismatch = lambda x: self.vhostbind is None or x.createby.vhost in self.vhostbind)
        while True:
            ev, m = await M_(flow_init, conn_down)
            if m is flow_init:
                c = ev.connection
                self.apiroutine.subroutine(self._init_conn(c))
            else:
                c = ev.connection
                self.apiroutine.subroutine(self._remove_conn(c))

    async def _init_conn(self, conn):
        # Default
        if conn in self._flowupdaters:
            arpupdater = self._flowupdaters.pop(conn)
            arpupdater.close()
        arpupdater = ARPUpdater(conn, self)
        #flowupdater = VXLANFlowUpdater(conn, self)
        self._flowupdaters[conn] = arpupdater
        arpupdater.start()
        if self.disableothers:
            ofdef = conn.openflowdef
            arptable = self._gettableindex('arp', conn.protocol.vhost)
            await conn.protocol.batch((ofdef.ofp_flow_mod(table_id = arptable,
                                                             command = ofdef.OFPFC_ADD,
                                                             priority = 1,
                                                             buffer_id = ofdef.OFP_NO_BUFFER,
                                                             out_port = ofdef.OFPP_ANY,
                                                             out_group = ofdef.OFPG_ANY,
                                                             match = ofdef.ofp_match_oxm(
                                                                    oxm_fields = [ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE,
                                                                                                   ofdef.ETHERTYPE_ARP),
                                                                                  ofdef.create_oxm(ofdef.OXM_OF_ARP_OP,
                                                                                                   ofdef.ARPOP_REQUEST)
                                                                                  ] +
                                                                                  ([ofdef.create_oxm(ofdef.OXM_OF_ETH_DST_W,
                                                                                                    b'\x01\x00\x00\x00\x00\x00',
                                                                                                    b'\x01\x00\x00\x00\x00\x00')
                                                                                   ]
                                                                                  if self.broadcastonly else []),
                                                                ),
                                                             instructions = [ofdef.ofp_instruction_actions(type = ofdef.OFPIT_CLEAR_ACTIONS)]
                                                             ),
                                          ), conn, self.apiroutine)

    async def _remove_conn(self, conn):
        # Do not need to modify flows
        if conn in self._flowupdaters:
            self._flowupdaters.pop(conn).close()

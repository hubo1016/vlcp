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
    def main(self):
        try:
            if self._connection.protocol.disablenxext:
                return
            self.subroutine(self._update_handler(), True, '_update_handler_routine')
            for m in FlowUpdater.main(self):
                yield m
        finally:
            if hasattr(self, '_update_handler_routine'):
                self._update_handler_routine.close()
    def _update_handler(self):
        dataobjectchanged = iop.DataObjectChanged.createMatcher(None, None, self._connection)
        while True:
            yield (dataobjectchanged,)
            self._lastlogports, self._lastphyports, self._lastlognets, _ = self.event.current
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
            try:
                netmap = walk(LogicalNetworkMap.default_key(value.id))
            except KeyError:
                pass
            else:
                save(netmap.getkey())
                for logport in netmap.ports.dataset():
                    try:
                        p = walk(logport.getkey())
                    except KeyError:
                        pass
                    else:
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
    def updateflow(self, connection, addvalues, removevalues, updatedvalues):
        try:
            allobjs = set(o for o in self._savedresult if o is not None and not o.isdeleted())
            lastlogportinfo = self._lastlogportinfo
            lastlognetinfo = self._lastlognetinfo
            lastphyportinfo = self._lastphyportinfo
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
                            arp_set = current_arps.setdefault(lognet, set())
                            arp_set.add((ip, mac, True, None, False))
                            arp_set.add((ip, mac, False, None, False))
                        else:
                            arp_set = current_arps.setdefault(lognet, set())
                            arp_set.add((ip, mac, islocal, None, False))
            broadcast_only = self._parent.broadcastonly
            if self._parent.prepush:
                for obj in self._savedresult:
                    if obj is not None and not obj.isdeleted() and obj.isinstance(LogicalPort) and hasattr(obj, 'ip_address') and hasattr(obj, 'mac_address'):
                        if obj.network in currentlognetinfo:
                            arp_set = current_arps.setdefault(obj.network, set())
                            arp_set.add((obj.ip_address, obj.mac_address, True, obj, broadcast_only))
            self._lastlogportinfo = currentlogportinfo
            self._lastlognetinfo = currentlognetinfo
            self._lastphyportinfo = currentphyportinfo
            self._last_arps = current_arps
            cmds = []
            ofdef = connection.openflowdef
            vhost = connection.protocol.vhost
            arp = self._parent._gettableindex('arp', vhost)
            l2out_next = self._parent._getnexttable('', 'l2output', vhost = vhost)
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
            for p in lastlogportinfo:
                if p not in currentlogportinfo or currentlogportinfo[p] != lastlogportinfo[p]:
                    pid, _ = lastlogportinfo[p]
                    cmds.append(ofdef.ofp_flow_mod(table_id = arp,
                                                   command = ofdef.OFPFC_DELETE,
                                                   buffer_id = ofdef.OFP_NO_BUFFER,
                                                   out_port = ofdef.OFPP_ANY,
                                                   out_group = ofdef.OFPG_ANY,
                                                   match = ofdef.ofp_match_oxm(
                                                                oxm_fields = [ofdef.create_oxm(ofdef.OXM_OF_IN_PORT, pid)]
                                                            )
                                                   ))
            for p in lastphyportinfo:
                if p not in currentphyportinfo or currentphyportinfo[p] != lastphyportinfo[p]:
                    pid, _ = lastphyportinfo[p]
                    cmds.append(ofdef.ofp_flow_mod(table_id = arp,
                                                   command = ofdef.OFPFC_DELETE,
                                                   buffer_id = ofdef.OFP_NO_BUFFER,
                                                   out_port = ofdef.OFPP_ANY,
                                                   out_group = ofdef.OFPG_ANY,
                                                   match = ofdef.ofp_match_oxm(
                                                                oxm_fields = [ofdef.create_oxm(ofdef.OXM_OF_IN_PORT, pid)]
                                                            )
                                                   ))
            for n in last_arps:
                if n not in current_arps:
                    if n in lastlognetinfo:
                        nid, _ = lastlognetinfo[n]
                        for ip,_,islocal,_,_ in last_arps[n]:
                            cmds.append(ofdef.ofp_flow_mod(table_id = arp,
                                                           cookie = 0x1 | (0x2 if islocal else 0),
                                                           cookie_mask = 0x3,
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
                        nid, _ = lastlognetinfo[n]
                        for ip,_,islocal,_,_ in last_arps[n]:
                            cmds.append(ofdef.ofp_flow_mod(table_id = arp,
                                                           cookie = 0x1 | (0x2 if islocal else 0),
                                                           cookie_mask = 0x3,
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
                            for ip, _, islocal, _, _ in last_arps[n].difference(current_arps[n]):
                                cmds.append(ofdef.ofp_flow_mod(table_id = arp,
                                                               cookie = 0x1 | (0x2 if islocal else 0),
                                                               cookie_mask = 0x3,
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
            for m in self.execute_commands(connection, cmds):
                yield m
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
            def _create_flow2(ip, mac, nid, pid, islocal, broadcast):
                return ofdef.ofp_flow_mod(
                               table_id = arp,
                               cookie = 0x1 | (0x2 if islocal else 0),
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
                               instructions = [ofdef.ofp_instruction_actions(type = ofdef.OFPIT_CLEAR_ACTIONS)]
                               )
            logport_arps = dict((ent[3],ent) for n,v in current_arps.items() for ent in v if ent[3] is not None)
            for p in currentlogportinfo:
                if p not in lastlogportinfo or lastlogportinfo[p] != currentlogportinfo[p]:
                    if p in logport_arps:
                        ip, mac, islocal, port, broadcast = logport_arps[p]
                        pid, lognet = currentlogportinfo[p]
                        if lognet in current_arps and lognet in currentlognetinfo:
                            nid, _ = currentlognetinfo[lognet]
                            if islocal and port == p:
                                cmds.append(_create_flow2(ip, mac, nid, pid, islocal, broadcast))
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
            phyportdict = {}
            for p in currentphyportinfo:
                phynet = p.physicalnetwork
                phyportdict.setdefault(phynet, []).append(p)
            for n, arps in current_arps.items():
                if n in currentlognetinfo:
                    nid, _ = currentlognetinfo[n]
                    if n not in last_arps or n not in lastlognetinfo or lastlognetinfo[n] != currentlognetinfo[n]:
                        send_arps = arps
                    else:
                        send_arps = arps.difference(last_arps[n])
                    for ip, mac, islocal, port, broadcast in send_arps:
                        cmds.append(_create_flow(ip, mac, nid, islocal, broadcast))
                        if islocal:
                            if port in currentlognetinfo:
                                cmds.append(_create_flow2(ip, mac, nid, pid, islocal, broadcast))
            for m in self.execute_commands(connection, cmds):
                yield m
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
            for m in conn.protocol.batch((ofdef.ofp_flow_mod(table_id = arptable,
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
                                          ), conn, self.apiroutine):
                yield m
    def _remove_conn(self, conn):
        # Do not need to modify flows
        if conn in self._flowupdaters:
            self._flowupdaters.pop(conn).close()
        if False:
            yield

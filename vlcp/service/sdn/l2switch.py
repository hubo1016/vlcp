'''
Created on 2016/5/24

:author: hubo
'''


from vlcp.config.config import defaultconfig
from vlcp.server.module import depend
from vlcp.service.sdn.flowbase import FlowBase
from vlcp.service.sdn.ofpmanager import FlowInitialize
from vlcp.utils.flowupdater import FlowUpdater
from vlcp.utils.networkmodel import PhysicalPort, LogicalPort, LogicalNetwork
import vlcp.service.kvdb.objectdb as objectdb
import vlcp.service.sdn.ofpportmanager as ofpportmanager
from vlcp.event.runnable import RoutineContainer
from vlcp.protocol.openflow.openflow import OpenflowConnectionStateEvent,\
    OpenflowAsyncMessageEvent, OpenflowErrorResultException
from vlcp.utils.ethernet import ethernet_l2
import vlcp.service.sdn.ioprocessing as iop
import itertools

class L2FlowUpdater(FlowUpdater):

    def __init__(self, connection, parent):
        FlowUpdater.__init__(self, connection, (), ('l2switch', connection), parent._logger)
        self._parent = parent
        self._lastlognets = ()
        self._lastlogports = ()
        self._lastphyports = ()
        self._lastlogportinfo = {}
        self._lastphyportinfo = {}
        self._lastlognetinfo = {}
    def main(self):
        try:
            self.subroutine(self._update_handler_prepush(), name = '_update_handler_routine')
            for m in FlowUpdater.main(self):
                yield m
        finally:
            self._update_handler_routine.close()
    def _walk_logport(self, key, value, walk, save):
        save(key)
        if value is None:
            return
        try:
            net = walk(value.network.getkey())
        except KeyError:
            pass
        else:
            save(net.getkey())
    def _walk_phyport(self, key, value, walk, save):
        save(key)
    def _update_handler_prepush(self):
        dataobjectchanged = iop.DataObjectChanged.createMatcher(None, None, self._connection)
        while True:
            yield (dataobjectchanged,)
            self._lastlogports, self._lastphyports, self._lastlognets, _ = self.event.current
            self._initialkeys = [p.getkey() for p,_ in self._lastlogports] + \
                                [p.getkey() for p,_ in self._lastphyports]
            self._walkerdict = dict(itertools.chain(((p.getkey(), self._walk_logport) for p,_ in self._lastlogports),
                                                    ((p.getkey(), self._walk_phyport) for p,_ in self._lastphyports)))
            self.subroutine(self.restart_walk(), False)
    def updateflow(self, conn, addvalues, removevalues, updatedvalues):
        ofdef = conn.openflowdef
        vhost = conn.protocol.vhost
        l2out = self._parent._gettableindex('l2output', vhost)
        l2out_next = self._parent._getnexttable('', 'l2output', vhost = vhost)
        def _create_flows(networkid, macaddr, portid):
            if conn.protocol.disablenxext:
                # Use METADATA
                masked_network = ((networkid & 0xffff) << 32)
                return (ofdef.ofp_flow_mod(table_id = l2out,
                                            cookie = 0x3,
                                            cookie_mask = 0xffffffffffffffff,
                                            command = ofdef.OFPFC_ADD,
                                            priority = ofdef.OFP_DEFAULT_PRIORITY + 2,
                                            buffer_id = ofdef.OFP_NO_BUFFER,
                                           out_port = ofdef.OFPP_ANY,
                                           out_group = ofdef.OFPG_ANY,
                                            match = ofdef.ofp_match_oxm(
                                                        oxm_fields = [
                                                            ofdef.create_oxm(ofdef.OXM_OF_METADATA_W,
                                                                             masked_network,
                                                                             b'\x00\x00\xff\xff\x00\x00\x00\x00'),
                                                            ofdef.create_oxm(ofdef.OXM_OF_ETH_DST, macaddr)
                                                            ]
                                                    ),
                                            instructions = [ofdef.ofp_instruction_write_metadata(metadata = (portid & 0xffff),
                                                                                                 metadata_mask = 0xffff),
                                                            ofdef.ofp_instruction_goto_table(table_id = l2out_next)]
                                    ),)
            else:
                # Use REG5, REG6
                out_net = networkid
                return (ofdef.ofp_flow_mod(table_id = l2out,
                                        cookie = 0x3,
                                        cookie_mask = 0xffffffffffffffff,
                                        command = ofdef.OFPFC_ADD,
                                        priority = ofdef.OFP_DEFAULT_PRIORITY + 2,
                                        buffer_id = ofdef.OFP_NO_BUFFER,
                                       out_port = ofdef.OFPP_ANY,
                                       out_group = ofdef.OFPG_ANY,
                                        match = ofdef.ofp_match_oxm(
                                                    oxm_fields = [
                                                        ofdef.create_oxm(ofdef.NXM_NX_REG5, out_net),
                                                        ofdef.create_oxm(ofdef.OXM_OF_ETH_DST, macaddr)
                                                        ]
                                                ),
                                        instructions = [ofdef.ofp_instruction_actions(
                                                                actions = [ofdef.ofp_action_set_field(
                                                                                field = ofdef.create_oxm(ofdef.NXM_NX_REG6, portid)
                                                                            )],
                                                                type = ofdef.OFPIT_APPLY_ACTIONS
                                                                                      ),
                                                        ofdef.ofp_instruction_goto_table(table_id = l2out_next)]
                                ),)
        def _delete_flows(networkid, macaddr):
            if conn.protocol.disablenxext:
                # Use METADATA
                masked_network = ((networkid & 0xffff) << 32)
                return (ofdef.ofp_flow_mod(table_id = l2out,
                                            cookie = 0x3,
                                            cookie_mask = 0xffffffffffffffff,
                                            command = ofdef.OFPFC_DELETE_STRICT,
                                            priority = ofdef.OFP_DEFAULT_PRIORITY + 2,
                                            buffer_id = ofdef.OFP_NO_BUFFER,
                                            out_port = ofdef.OFPP_ANY,
                                            out_group = ofdef.OFPG_ANY,
                                            match = ofdef.ofp_match_oxm(
                                                        oxm_fields = [
                                                            ofdef.create_oxm(ofdef.OXM_OF_METADATA_W,
                                                                             masked_network,
                                                                             b'\x00\x00\xff\xff\x00\x00\x00\x00'),
                                                            ofdef.create_oxm(ofdef.OXM_OF_ETH_DST, macaddr)
                                                            ]
                                                    )
                                    ),)
            else:
                # Use REG5, REG6
                out_net = networkid
                return (ofdef.ofp_flow_mod(table_id = l2out,
                                            cookie = 0x3,
                                            cookie_mask = 0xffffffffffffffff,
                                            command = ofdef.OFPFC_DELETE_STRICT,
                                            priority = ofdef.OFP_DEFAULT_PRIORITY + 2,
                                            buffer_id = ofdef.OFP_NO_BUFFER,
                                            out_port = ofdef.OFPP_ANY,
                                            out_group = ofdef.OFPG_ANY,
                                            match = ofdef.ofp_match_oxm(
                                                        oxm_fields = [
                                                            ofdef.create_oxm(ofdef.NXM_NX_REG5, out_net),
                                                            ofdef.create_oxm(ofdef.OXM_OF_ETH_DST, macaddr)
                                                            ]
                                                    )
                                    ),)
        def _create_default_flow(networkid, portid):
            if conn.protocol.disablenxext:
                # Use METADATA
                masked_network = ((networkid & 0xffff) << 32)
                return (ofdef.ofp_flow_mod(table_id = l2out,
                                            cookie = 0x4,
                                            cookie_mask = 0xffffffffffffffff,
                                            command = ofdef.OFPFC_ADD,
                                            priority = ofdef.OFP_DEFAULT_PRIORITY + 1,
                                            buffer_id = ofdef.OFP_NO_BUFFER,
                                            out_port = ofdef.OFPP_ANY,
                                            out_group = ofdef.OFPG_ANY,
                                            match = ofdef.ofp_match_oxm(
                                                        oxm_fields = [
                                                            ofdef.create_oxm(ofdef.OXM_OF_METADATA_W,
                                                                             masked_network,
                                                                             b'\x00\x00\xff\xff\x00\x00\x00\x00')
                                                            ]
                                                    ),
                                            instructions = [ofdef.ofp_instruction_write_metadata(metadata = (portid & 0xffff),
                                                                                                 metadata_mask = 0xffff),
                                                            ofdef.ofp_instruction_goto_table(table_id = l2out_next)]
                                    ),)
            else:
                # Use REG5, REG6
                out_net = networkid
                return (ofdef.ofp_flow_mod(table_id = l2out,
                                            cookie = 0x4,
                                            cookie_mask = 0xffffffffffffffff,
                                            command = ofdef.OFPFC_ADD,
                                            priority = ofdef.OFP_DEFAULT_PRIORITY + 1,
                                            buffer_id = ofdef.OFP_NO_BUFFER,
                                            out_port = ofdef.OFPP_ANY,
                                            out_group = ofdef.OFPG_ANY,
                                            match = ofdef.ofp_match_oxm(
                                                        oxm_fields = [
                                                            ofdef.create_oxm(ofdef.NXM_NX_REG5, out_net)
                                                            ]
                                                    ),
                                            instructions = [ofdef.ofp_instruction_actions(
                                                                    actions = [ofdef.ofp_action_set_field(
                                                                                    field = ofdef.create_oxm(ofdef.NXM_NX_REG6, portid)
                                                                                )],
                                                                    type = ofdef.OFPIT_APPLY_ACTIONS
                                                                                          ),
                                                            ofdef.ofp_instruction_goto_table(table_id = l2out_next)]
                                    ),)
        def _delete_default_flows(networkid):
            if conn.protocol.disablenxext:
                # Use METADATA
                masked_network = ((networkid & 0xffff) << 32)
                return (ofdef.ofp_flow_mod(table_id = l2out,
                                            cookie = 0x4,
                                            cookie_mask = 0xffffffffffffffff,
                                            command = ofdef.OFPFC_DELETE_STRICT,
                                            priority = ofdef.OFP_DEFAULT_PRIORITY + 1,
                                            buffer_id = ofdef.OFP_NO_BUFFER,
                                            out_port = ofdef.OFPP_ANY,
                                            out_group = ofdef.OFPG_ANY,
                                            match = ofdef.ofp_match_oxm(
                                                        oxm_fields = [
                                                            ofdef.create_oxm(ofdef.OXM_OF_METADATA_W,
                                                                             masked_network,
                                                                             b'\x00\x00\xff\xff\x00\x00\x00\x00')
                                                            ]
                                                    )
                                    ),)
            else:
                # Use REG5, REG6
                out_net = networkid
                return (ofdef.ofp_flow_mod(table_id = l2out,
                                            cookie = 0x4,
                                            cookie_mask = 0xffffffffffffffff,
                                            command = ofdef.OFPFC_DELETE_STRICT,
                                            priority = ofdef.OFP_DEFAULT_PRIORITY + 1,
                                            buffer_id = ofdef.OFP_NO_BUFFER,
                                            out_port = ofdef.OFPP_ANY,
                                            out_group = ofdef.OFPG_ANY,
                                            match = ofdef.ofp_match_oxm(
                                                        oxm_fields = [
                                                            ofdef.create_oxm(ofdef.NXM_NX_REG5, out_net)
                                                            ]
                                                    )
                                    ),)
        try:

            lastlognetinfo = self._lastlognetinfo
            lastlogportinfo = self._lastlogportinfo
            allresult = set(v for v in self._savedresult if v is not None and not v.isdeleted())

            # Select one physical port for each physical network, remove others
            currentphynets = dict(sorted(((p.physicalnetwork, p) for p, _ in self._lastphyports if p in allresult),
                                         key=lambda x: x[1].getkey()))

            currentphyportinfo = dict((p,(p.physicalnetwork, pid)) for p,pid in self._lastphyports
                                      if p in allresult and p.physicalnetwork in currentphynets
                                      and currentphynets[p.physicalnetwork] == p)

            phynet_to_phyport_id_info = dict((p.physicalnetwork,v[1]) for p,v in currentphyportinfo.items())
            currentlognetinfo = dict((n, (nid, phynet_to_phyport_id_info.get(n.physicalnetwork,None)))
                                     for n, nid in self._lastlognets
                                     if n in allresult)

            def _try_create_macaddress(port):
                mac_address = getattr(port, 'mac_address', None)
                if mac_address is None:
                    return None
                else:
                    try:
                        return ofdef.mac_addr(mac_address)
                    except Exception:
                        return None
            currentlogportinfo = dict((logport, (_try_create_macaddress(logport), logportid,
                                                 currentlognetinfo.get(logport.network)[0]))
                                      for logport, logportid in self._lastlogports
                                      if logport in allresult
                                      and logport.network in currentlognetinfo)

            self._lastlognetinfo = currentlognetinfo
            self._lastlogportinfo = currentlogportinfo

            cmds = []
            for lognet in lastlognetinfo.keys():
                if lognet not in currentlognetinfo:
                    # this lognet has been removed
                    network_id , _ = lastlognetinfo[lognet]
                    cmds.extend(_delete_default_flows(network_id))
                elif lognet in currentlognetinfo and currentlognetinfo[lognet] != lastlognetinfo[lognet]:
                    # some info has been changed , remove old
                    network_id, _ = lastlognetinfo[lognet]
                    cmds.extend(_delete_default_flows(network_id))


            for logport in lastlogportinfo.keys():
                if logport not in currentlogportinfo:
                    # this logport has been removed
                    mac, _, network_id = lastlogportinfo[logport]

                    if mac:
                        cmds.extend(_delete_flows(network_id, mac))
                elif logport in currentlogportinfo and currentlogportinfo[logport] != lastlogportinfo[logport]:
                    # some info has been changed , remove old
                    mac, _ , network_id = lastlogportinfo[logport]

                    if mac:
                        cmds.extend(_delete_flows(network_id, mac))

            for m in self.execute_commands(conn, cmds):
                yield m

            del cmds[:]

            for lognet in currentlognetinfo.keys():
                if lognet not in lastlognetinfo:
                    # this lognet is new add
                    network_id, phyport_id = currentlognetinfo[lognet]
                    if phyport_id:
                        cmds.extend(_create_default_flow(network_id,phyport_id))
                elif lognet in lastlognetinfo and lastlognetinfo[lognet] != currentlognetinfo[lognet]:
                    # this lognet info changed , add new info
                    network_id, phyport_id = currentlognetinfo[lognet]

                    if phyport_id:
                        cmds.extend(_create_default_flow(network_id, phyport_id))

            for logport in currentlogportinfo.keys():
                if logport not in lastlogportinfo:
                    # this logport is new add
                    mac, port_id, network_id = currentlogportinfo[logport]
                    if mac:
                        cmds.extend(_create_flows(network_id, mac,port_id))
                elif logport in lastlogportinfo and lastlogportinfo[logport] != currentlogportinfo[logport]:
                    # this logport info changed, add new info
                    mac, port_id, network_id = currentlogportinfo[logport]
                    if mac:
                        cmds.extend(_create_flows(network_id, mac, port_id))

            for m in self.execute_commands(conn,cmds):
                yield m

        except Exception:
            self._parent._logger.warning("Update l2switch flow for connection %r failed with exception", conn, exc_info = True)
            # We don't want the whole flow update stops, so ignore the exception and continue
            
    

@defaultconfig
@depend(ofpportmanager.OpenflowPortManager, objectdb.ObjectDB)
class L2Switch(FlowBase):
    "L2 switch functions"
    _tablerequest = (("l2input", ('ingress',), ''),
                     ("l2output", ('l2input',), ''),
                     ('egress', ('l2output', 'l2learning'), ''),
                     ("l2learning", ('l2output',), 'l2learning'))
    # Use learning or prepush. Learning strategy creates flows on necessary, but may cause
    # some delay for the first packet; prepush push all necessary flows from beginning.
    _default_learning = False
    # When using learning=True, enable OpenvSwitch learn() action. if nxlearn=False, the
    # learning strategy will be done with controller PACKET_IN processing, which may be
    # less efficient.
    _default_nxlearn = True
    # Timeout for a learned flow
    _default_learntimeout = 300
    def __init__(self, server):
        FlowBase.__init__(self, server)
        self.apiroutine = RoutineContainer(self.scheduler)
        self.apiroutine.main = self._main
        self.routines.append(self.apiroutine)
        self._flowupdaters = {}
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
        l2 = self._gettableindex('l2input', vhost)
        l2_next = self._getnexttable('', 'l2input', vhost = vhost)
        l2out = self._gettableindex('l2output', vhost)
        l2out_next = self._getnexttable('', 'l2output', vhost = vhost)
        l2learning = self._gettableindex('l2learning', vhost)
        if hasattr(conn, '_l2switch_learning_routine') and conn._l2switch_learning_routine:
            conn._l2switch_learning_routine.close()
            delattr(conn, '_l2switch_learning_routine')
        if conn in self._flowupdaters:
            self._flowupdaters[conn].close()
            del self._flowupdaters[conn]
        if not self.learning:
            new_updater = L2FlowUpdater(conn, self)
            self._flowupdaters[conn] = new_updater
            new_updater.start()
        if self.learning:
            if self.nxlearn and not conn.protocol.disablenxext:
                # Use nx_action_learn
                for m in conn.protocol.batch((ofdef.ofp_flow_mod(table_id = l2,
                                                                   command = ofdef.OFPFC_ADD,
                                                                   priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                                   buffer_id = ofdef.OFP_NO_BUFFER,
                                                                   match = ofdef.ofp_match_oxm(
                                                                                oxm_fields = [
                                                                                    # Drop packets with an broadcast MAC address as dl_src
                                                                                    ofdef.create_oxm(ofdef.OXM_OF_ETH_SRC_W, b'\x01\x00\x00\x00\x00\x00', b'\x01\x00\x00\x00\x00\x00')
                                                                                    ]
                                                                            ),
                                                                   out_port = ofdef.OFPP_ANY,
                                                                   out_group = ofdef.OFPG_ANY,
                                                                   instructions = [ofdef.ofp_instruction_actions(
                                                                                    type = ofdef.OFPIT_CLEAR_ACTIONS
                                                                                    )]
                                                                   ),
                                            ofdef.ofp_flow_mod(table_id = l2,
                                                                   command = ofdef.OFPFC_ADD,
                                                                   priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                                   buffer_id = ofdef.OFP_NO_BUFFER,
                                                                   match = ofdef.ofp_match_oxm(
                                                                                oxm_fields = [
                                                                                    # Drop 802.1D STP packets, they should never be forwarded
                                                                                    ofdef.create_oxm(ofdef.OXM_OF_ETH_DST_W, b'\x01\x80\xc2\x00\x00\x00', b'\xff\xff\xff\xff\xff\xf0')
                                                                                    ]
                                                                            ),
                                                                   out_port = ofdef.OFPP_ANY,
                                                                   out_group = ofdef.OFPG_ANY,
                                                                   instructions = [ofdef.ofp_instruction_actions(
                                                                                    type = ofdef.OFPIT_CLEAR_ACTIONS
                                                                                    )]
                                                                   ),
                                            ofdef.ofp_flow_mod(table_id = l2,
                                                                cookie = 0x1,
                                                                cookie_mask = 0xffffffffffffffff,
                                                                   command = ofdef.OFPFC_ADD,
                                                                   priority = 0,
                                                                   buffer_id = ofdef.OFP_NO_BUFFER,
                                                                   match = ofdef.ofp_match_oxm(),
                                                                   out_port = ofdef.OFPP_ANY,
                                                                   out_group = ofdef.OFPG_ANY,
                                                                   instructions = [ofdef.ofp_instruction_actions(
                                                                                    actions = [
                                                                                        ofdef.nx_action_learn(
                                                                                                hard_timeout = self.learntimeout,
                                                                                                priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                                                                cookie = 0x2,
                                                                                                table_id = l2learning,
                                                                                                specs = [ofdef.create_nxfms_matchfield(ofdef.NXM_NX_REG4, ofdef.NXM_NX_REG5),
                                                                                                         ofdef.create_nxfms_matchfield(ofdef.NXM_OF_ETH_SRC, ofdef.NXM_OF_ETH_DST),
                                                                                                         ofdef.create_nxfms_loadfield(ofdef.OXM_OF_IN_PORT, ofdef.NXM_NX_REG6)]
                                                                                                )
                                                                                               ],
                                                                                    type = ofdef.OFPIT_APPLY_ACTIONS
                                                                                    ),
                                                                                   ofdef.ofp_instruction_goto_table(table_id = l2_next)]
                                                                   ),
                                              ofdef.ofp_flow_mod(table_id = l2out,
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
                                                                   instructions = [ofdef.ofp_instruction_goto_table(table_id = l2out_next)]
                                                                   ),
                                              ofdef.ofp_flow_mod(table_id = l2out,
                                                                cookie = 0x1,
                                                                cookie_mask = 0xffffffffffffffff,
                                                                   command = ofdef.OFPFC_ADD,
                                                                   priority = 0,
                                                                   buffer_id = ofdef.OFP_NO_BUFFER,
                                                                   out_port = ofdef.OFPP_ANY,
                                                                   out_group = ofdef.OFPG_ANY,
                                                                   match = ofdef.ofp_match_oxm(),
                                                                   instructions = [ofdef.ofp_instruction_actions(
                                                                                    actions = [
                                                                                        ofdef.nx_action_resubmit(
                                                                                                table = l2learning,
                                                                                                in_port = ofdef.nx_port_no.OFPP_IN_PORT
                                                                                                )
                                                                                               ],
                                                                                    type = ofdef.OFPIT_APPLY_ACTIONS
                                                                                    ),
                                                                                   ofdef.ofp_instruction_goto_table(table_id = l2out_next)]
                                                                   )), conn, self.apiroutine):
                    yield m
            else:
                # Use PACKET_IN
                for m in conn.protocol.batch((ofdef.ofp_flow_mod(table_id = l2,
                                                                   command = ofdef.OFPFC_ADD,
                                                                   priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                                   buffer_id = ofdef.OFP_NO_BUFFER,
                                                                   out_port = ofdef.OFPP_ANY,
                                                                   out_group = ofdef.OFPG_ANY,
                                                                   match = ofdef.ofp_match_oxm(
                                                                                oxm_fields = [
                                                                                    # Drop packets with an broadcast MAC address as dl_src
                                                                                    ofdef.create_oxm(ofdef.OXM_OF_ETH_SRC_W, b'\x01\x00\x00\x00\x00\x00', b'\x01\x00\x00\x00\x00\x00')
                                                                                    ]
                                                                            ),
                                                                   instructions = [ofdef.ofp_instruction_actions(
                                                                                    type = ofdef.OFPIT_CLEAR_ACTIONS
                                                                                    )]
                                                                   ),
                                            ofdef.ofp_flow_mod(table_id = l2,
                                                                   command = ofdef.OFPFC_ADD,
                                                                   priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                                   buffer_id = ofdef.OFP_NO_BUFFER,
                                                                   out_port = ofdef.OFPP_ANY,
                                                                   out_group = ofdef.OFPG_ANY,
                                                                   match = ofdef.ofp_match_oxm(
                                                                                oxm_fields = [
                                                                                    # Drop 802.1D STP packets, they should never be forwarded
                                                                                    ofdef.create_oxm(ofdef.OXM_OF_ETH_DST_W, b'\x01\x80\xc2\x00\x00\x00', b'\xff\xff\xff\xff\xff\xf0')
                                                                                    ]
                                                                            ),
                                                                   instructions = [ofdef.ofp_instruction_actions(
                                                                                    type = ofdef.OFPIT_CLEAR_ACTIONS
                                                                                    )]
                                                                   ),
                                            ofdef.ofp_flow_mod(table_id = l2,
                                                                cookie = 0x1,
                                                                cookie_mask = 0xffffffffffffffff,
                                                                   command = ofdef.OFPFC_ADD,
                                                                   priority = 1,
                                                                   buffer_id = ofdef.OFP_NO_BUFFER,
                                                                   out_port = ofdef.OFPP_ANY,
                                                                   out_group = ofdef.OFPG_ANY,
                                                                   match = ofdef.ofp_match_oxm(),
                                                                   instructions = [ofdef.ofp_instruction_actions(
                                                                                    actions = [
                                                                                        ofdef.ofp_action_output(port = ofdef.OFPP_CONTROLLER,
                                                                                                                max_len = 32
                                                                                                                )
                                                                                               ],
                                                                                    type = ofdef.OFPIT_APPLY_ACTIONS
                                                                                    ),
                                                                                   ofdef.ofp_instruction_goto_table(table_id = l2_next)]
                                                                   ),
                                              ofdef.ofp_flow_mod(table_id = l2out,
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
                                                                   instructions = [ofdef.ofp_instruction_goto_table(table_id = l2out_next)]
                                                                   )
                                              ), conn, self.apiroutine):
                    yield m                    
                def learning_packet_handler():
                    packetin = OpenflowAsyncMessageEvent.createMatcher(ofdef.OFPT_PACKET_IN, None, None, l2, 1, conn, conn.connmark)
                    conndown = conn.protocol.statematcher(conn)
                    while True:
                        yield (packetin, conndown)
                        if conn.matcher is conndown:
                            break
                        else:
                            msg = conn.event.message
                            try:
                                p = ethernet_l2.create(msg.data)
                            except Exception:
                                self._logger.warning('Invalid packet received: %r', conn.event.message.data, exc_info = True)
                            else:
                                dl_src = p.dl_src
                                in_port = ofdef.get_oxm(msg.match.oxm_fields, ofdef.OXM_OF_IN_PORT)
                                if conn.protocol.disablenxext:
                                    # Use METADATA
                                    metadata = ofdef.get_oxm(msg.match.oxm_fields, ofdef.OXM_OF_METADATA)
                                    masked_network = b'\x00\x00' + metadata[0:2] + b'\x00\x00\x00\x00'
                                    cmds = [ofdef.ofp_flow_mod(table_id = l2,
                                                                cookie = 0x2,
                                                                cookie_mask = 0xffffffffffffffff,
                                                                   command = ofdef.OFPFC_ADD,
                                                                   priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                                   out_port = ofdef.OFPP_ANY,
                                                                   out_group = ofdef.OFPG_ANY,
                                                                   buffer_id = ofdef.OFP_NO_BUFFER,
                                                                   hard_timeout = max((self.learntimeout//4), 1),
                                                                   match = ofdef.ofp_match_oxm(
                                                                                oxm_fields = [
                                                                                    ofdef.create_oxm(ofdef.OXM_OF_METADATA_W,
                                                                                                     metadata[0:2] + b'\x00\x00\x00\x00\x00\x00',
                                                                                                     b'\xff\xff\x00\x00\x00\x00\x00\x00'),
                                                                                    ofdef.create_oxm(ofdef.OXM_OF_ETH_SRC, dl_src),
                                                                                    ofdef.create_oxm(ofdef.OXM_OF_IN_PORT, in_port)
                                                                                    ]
                                                                            ),
                                                                   instructions = [ofdef.ofp_instruction_goto_table(table_id = l2_next)]
                                                                   ),
                                                                ofdef.ofp_flow_mod(table_id = l2out,
                                                                    cookie = 0x2,
                                                                    cookie_mask = 0xffffffffffffffff,
                                                                    command = ofdef.OFPFC_ADD,
                                                                    priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                                    buffer_id = ofdef.OFP_NO_BUFFER,
                                                                    hard_timeout = self.learntimeout,
                                                                    out_port = ofdef.OFPP_ANY,
                                                                    out_group = ofdef.OFPG_ANY,
                                                                    match = ofdef.ofp_match_oxm(
                                                                                oxm_fields = [
                                                                                    ofdef.create_oxm(ofdef.OXM_OF_METADATA_W,
                                                                                                     masked_network,
                                                                                                     b'\x00\x00\xff\xff\x00\x00\x00\x00'),
                                                                                    ofdef.create_oxm(ofdef.OXM_OF_ETH_DST, dl_src)
                                                                                    ]
                                                                            ),
                                                                    instructions = [ofdef.ofp_instruction_write_metadata(metadata = ofdef.uint16.create(in_port[2:4]),
                                                                                                                         metadata_mask = 0xffff),
                                                                                    ofdef.ofp_instruction_goto_table(table_id = l2out_next)]
                                                            )]
                                else:
                                    # Use REG5, REG6
                                    out_net = ofdef.get_oxm(msg.match.oxm_fields, ofdef.NXM_NX_REG4)
                                    cmds = [ofdef.ofp_flow_mod(table_id = l2,
                                                                cookie = 0x2,
                                                                cookie_mask = 0xffffffffffffffff,
                                                                   command = ofdef.OFPFC_ADD,
                                                                   priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                                   buffer_id = ofdef.OFP_NO_BUFFER,
                                                                   hard_timeout = max((self.learntimeout//4), 1),
                                                                   out_port = ofdef.OFPP_ANY,
                                                                   out_group = ofdef.OFPG_ANY,
                                                                   match = ofdef.ofp_match_oxm(
                                                                                oxm_fields = [
                                                                                    ofdef.create_oxm(ofdef.NXM_NX_REG4, out_net),
                                                                                    ofdef.create_oxm(ofdef.OXM_OF_ETH_SRC, dl_src),
                                                                                    ofdef.create_oxm(ofdef.OXM_OF_IN_PORT, in_port)
                                                                                    ]
                                                                            ),
                                                                   instructions = [ofdef.ofp_instruction_goto_table(table_id = l2_next)]
                                                                   ),
                                                                ofdef.ofp_flow_mod(table_id = l2out,
                                                                    cookie = 0x2,
                                                                    cookie_mask = 0xffffffffffffffff,
                                                                    command = ofdef.OFPFC_ADD,
                                                                    priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                                    buffer_id = ofdef.OFP_NO_BUFFER,
                                                                    hard_timeout = self.learntimeout,
                                                                    out_port = ofdef.OFPP_ANY,
                                                                    out_group = ofdef.OFPG_ANY,
                                                                    match = ofdef.ofp_match_oxm(
                                                                                oxm_fields = [
                                                                                    ofdef.create_oxm(ofdef.NXM_NX_REG5, out_net),
                                                                                    ofdef.create_oxm(ofdef.OXM_OF_ETH_DST, dl_src)
                                                                                    ]
                                                                            ),
                                                                    instructions = [ofdef.ofp_instruction_actions(
                                                                                            actions = [ofdef.ofp_action_set_field(
                                                                                                            field = ofdef.create_oxm(ofdef.NXM_NX_REG6, in_port)
                                                                                                        )],
                                                                                            type = ofdef.OFPIT_APPLY_ACTIONS
                                                                                                                  ),
                                                                                    ofdef.ofp_instruction_goto_table(table_id = l2out_next)]
                                                            )]
                                if msg.buffer_id != ofdef.OFP_NO_BUFFER:
                                    # Send a packet out without actions to drop the packet and release the buffer
                                    cmds.append(ofdef.ofp_packet_out(buffer_id = msg.buffer_id,
                                                                     in_port = ofdef.OFPP_CONTROLLER,
                                                                     actions = []
                                                                     ))
                                conn.subroutine(conn.protocol.batch(cmds, conn, conn))
                conn.subroutine(learning_packet_handler(), name = '_l2switch_learning_routine')
        else:
            # Disable learning
            for m in conn.protocol.batch((ofdef.ofp_flow_mod(table_id = l2,
                                                                   command = ofdef.OFPFC_ADD,
                                                                   priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                                   buffer_id = ofdef.OFP_NO_BUFFER,
                                                                   out_port = ofdef.OFPP_ANY,
                                                                   out_group = ofdef.OFPG_ANY,
                                                                   match = ofdef.ofp_match_oxm(
                                                                                oxm_fields = [
                                                                                    # Drop packets with an broadcast MAC address as dl_src
                                                                                    ofdef.create_oxm(ofdef.OXM_OF_ETH_SRC_W, b'\x01\x00\x00\x00\x00\x00', b'\x01\x00\x00\x00\x00\x00')
                                                                                    ]
                                                                            ),
                                                                   instructions = [ofdef.ofp_instruction_actions(
                                                                                    type = ofdef.OFPIT_CLEAR_ACTIONS
                                                                                    )]
                                                                   ),
                                          ofdef.ofp_flow_mod(table_id = l2,
                                                                   command = ofdef.OFPFC_ADD,
                                                                   priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                                   buffer_id = ofdef.OFP_NO_BUFFER,
                                                                   out_port = ofdef.OFPP_ANY,
                                                                   out_group = ofdef.OFPG_ANY,
                                                                   match = ofdef.ofp_match_oxm(
                                                                                oxm_fields = [
                                                                                    # Drop 802.1D STP packets, they should never be forwarded
                                                                                    ofdef.create_oxm(ofdef.OXM_OF_ETH_DST_W, b'\x01\x80\xc2\x00\x00\x00', b'\xff\xff\xff\xff\xff\xf0')
                                                                                    ]
                                                                            ),
                                                                   instructions = [ofdef.ofp_instruction_actions(
                                                                                    type = ofdef.OFPIT_CLEAR_ACTIONS
                                                                                    )]
                                                                   ),
                                          ofdef.ofp_flow_mod(table_id = l2,
                                                            cookie = 0x1,
                                                            cookie_mask = 0xffffffffffffffff,
                                                               out_port = ofdef.OFPP_ANY,
                                                               out_group = ofdef.OFPG_ANY,
                                                               command = ofdef.OFPFC_ADD,
                                                               priority = 0,
                                                               buffer_id = ofdef.OFP_NO_BUFFER,
                                                               match = ofdef.ofp_match_oxm(),
                                                               instructions = [ofdef.ofp_instruction_goto_table(table_id = l2_next)]
                                                               ),
                                          ofdef.ofp_flow_mod(table_id = l2out,
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
                                                               instructions = [ofdef.ofp_instruction_goto_table(table_id = l2out_next)]
                                                               ),
                                          ofdef.ofp_flow_mod(table_id = l2out,
                                                            cookie = 0x1,
                                                            cookie_mask = 0xffffffffffffffff,
                                                               out_port = ofdef.OFPP_ANY,
                                                               out_group = ofdef.OFPG_ANY,
                                                               command = ofdef.OFPFC_ADD,
                                                               priority = 0,
                                                               buffer_id = ofdef.OFP_NO_BUFFER,
                                                               match = ofdef.ofp_match_oxm(),
                                                               instructions = [ofdef.ofp_instruction_goto_table(table_id = l2out_next)]
                                                               ),
                                          ), conn, self.apiroutine):
                yield m
    def _remove_conn(self, conn):
        # Do not need to modify flows
        if conn in self._flowupdaters:
            self._flowupdaters[conn].close()
            del self._flowupdaters[conn]
        if False:
            yield

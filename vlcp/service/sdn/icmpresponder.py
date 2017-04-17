import itertools
import os

import vlcp.service.sdn.ofpportmanager as ofpportmanager
import vlcp.service.kvdb.objectdb as objectdb
import vlcp.service.sdn.ioprocessing as iop
from vlcp.service.sdn.flowbase import FlowBase
from vlcp.server.module import depend, callAPI
from vlcp.config.config import defaultconfig
from vlcp.event.runnable import RoutineContainer
from vlcp.service.sdn.ofpmanager import FlowInitialize
from vlcp.utils.ethernet import mac_addr_bytes, ip4_addr_bytes,ip4_icmp_payload,\
    ethernet_l7, ip4_packet_l7, ip4_payload,ICMP_ECHOREPLY,icmp_bestparse,icmp_echo
from vlcp.utils.flowupdater import FlowUpdater
from vlcp.protocol.openflow.openflow import OpenflowConnectionStateEvent, OpenflowAsyncMessageEvent
from vlcp.utils.networkmodel import SubNet,RouterPort
from namedstruct.stdprim import uint16

class ICMPResponderUpdater(FlowUpdater):
    def __init__(self,connection,parent):
        super(ICMPResponderUpdater,self).__init__(connection,(),('icmpresponderupdate',connection),parent._logger)
        self.parent = parent
        self._lastlognets = ()
        self._lastlogports = ()
        self._lastsubnetsinfo = dict()
        self._orig_initialkeys = ()

    def main(self):
        try:
            self.subroutine(self._update_handler(),True,"update_handler_routine")

            # use controller to reply icmp ping ,so start routine handler packet in
            if not self.parent.prepush:
                self.subroutine(self._icmp_packetin_handler(),True,"icmp_packetin_handler_routine")

            for m in FlowUpdater.main(self):
                yield m
        finally:
            if hasattr(self,"update_handler_routine"):
                self.update_handler_routine.close()

            if hasattr(self,"icmp_packetin_handler_routine"):
                self.icmp_packetin_handler_routine.close()

    def _icmp_packetin_handler(self):
        conn = self._connection
        ofdef = self._connection.openflowdef
        l3input = self.parent._gettableindex("l3input",self._connection.protocol.vhost)
        
        transactid = uint16.create(os.urandom(2)) 

        def send_packet_out(portid,packet):
            for m in self.execute_commands(conn,
                        [
                            ofdef.ofp_packet_out(
                                buffer_id = ofdef.OFP_NO_BUFFER,
                                in_port = ofdef.OFPP_CONTROLLER,
                                actions = [
                                    ofdef.ofp_action_output(port = portid,
                                                            max_len = ofdef.OFPCML_NO_BUFFER
                                                            )
                                ],
                                data = packet._tobytes()
                            )
                        ]):
                yield m

        icmp_packetin_matcher = OpenflowAsyncMessageEvent.createMatcher(ofdef.OFPT_PACKET_IN,None,None,l3input,2,
                                                            self._connection,self._connection.connmark)

        while True:
            yield (icmp_packetin_matcher,)
            msg = self.event.message
            inport = ofdef.ofp_port_no.create(ofdef.get_oxm(msg.match.oxm_fields,ofdef.OXM_OF_IN_PORT))

            # it must be icmp packet ...
            icmp_packet = ethernet_l7.create(msg.data)
            
            transactid = (transactid + 1) & 0xffff
            reply_packet = ip4_packet_l7((ip4_payload,ip4_icmp_payload),
                                         (icmp_bestparse, icmp_echo),
                                        dl_src = icmp_packet.dl_dst,
                                        dl_dst = icmp_packet.dl_src,
                                        ip_src = icmp_packet.ip_dst,
                                        ip_dst = icmp_packet.ip_src,
                                        frag_off = icmp_packet.frag_off,
                                        ttl = 128,
                                        identifier = transactid,
                                        icmp_type = ICMP_ECHOREPLY,
                                        icmp_code = icmp_packet.icmp_code,
                                        icmp_id = icmp_packet.icmp_id,
                                        icmp_seq = icmp_packet.icmp_seq,
                                        data = icmp_packet.data
                                        )
           
            self.subroutine(send_packet_out(inport,reply_packet))
    def _update_handler(self):

        # when lgport,lgnet,phyport,phynet object change , receive this event from ioprocessing module
        dataobjectchange = iop.DataObjectChanged.createMatcher(None,None,self._connection)

        while True:
            yield (dataobjectchange,)

            # save to instance attr ,  us in other method
            self._lastlogports,_,self._lastlognets,_ = self.event.current
            self._update_walk()

    def _walk_lgport(self,key,value,walk,save):

        if value is not None:
            save(key)
            if hasattr(value,'subnet'):
                try:
                    subnetobj = walk(value.subnet.getkey())
                except KeyError:
                    pass
                else:
                    save(value.subnet.getkey())
                    if subnetobj is not None and hasattr(subnetobj,"router"):
                        try:
                            _ = walk(subnetobj.router.getkey())
                        except KeyError:
                            pass
                        else:
                            save(subnetobj.router.getkey())

    def _walk_lgnet(self,key,value,walk,save):
        save(key)
        # if value is None, also save its key
        # means watch key, when created , we will recv event

    def _update_walk(self):
        lgportkeys = [p.getkey() for p,_ in self._lastlogports]
        lgnetkeys = [p.getkey() for p,_ in self._lastlognets]

        self._initialkeys = lgportkeys + lgnetkeys
        self._orig_initialkeys = lgportkeys + lgnetkeys
        
        self._walkerdict = dict(itertools.chain(((p,self._walk_lgport) for p in lgportkeys),
                                                ((n,self._walk_lgnet) for n in lgnetkeys)))
        
        self.subroutine(self.restart_walk(),False)
    
    def reset_initialkeys(self,keys,values):
        # walk map  logicalport --> subnet ---> routerport
        # we get subnet object, add keys to initialkeys, 
        # when subnet update, it will restart walk ,, after we will get new routerport
        
        subnetkeys = [k for k,v in zip(keys,values) if v is not None and not v.isdeleted() and
                      v.isinstance(SubNet)]

        self._initialkeys = tuple(itertools.chain(self._orig_initialkeys,subnetkeys))
    
    def updateflow(self, connection, addvalues, removevalues, updatedvalues):
        try:
            allobjects = set(o for o in self._savedresult if o is not None and not o.isdeleted())

            lastsubnetsinfo = self._lastsubnetsinfo
            currentlognetsinfo = dict((n,id) for n,id in self._lastlognets if n in allobjects)
            currentrouterportsinfo = dict((o.subnet,o) for o in allobjects
                                            if o.isinstance(RouterPort))
            currentsubnetsinfo = dict((o,(getattr(currentrouterportsinfo[o],"ip_address",getattr(o,"gateway",None)),
                                          self.parent.inroutermac,o.network.id,currentlognetsinfo[o.network]))
                                        for o in allobjects if o.isinstance(SubNet)
                                            and hasattr(o,"router") and o in currentrouterportsinfo
                                            and o.network in currentlognetsinfo
                                            and (hasattr(currentrouterportsinfo[o],"ip_address")
                                                or hasattr(o,"gateway"))
                                            and ( not hasattr(o,"isexternal") or o.isexternal == False))
            self._lastsubnetsinfo = currentsubnetsinfo

            ofdef = connection.openflowdef
            vhost = connection.protocol.vhost
            l3input = self.parent._gettableindex("l3input",vhost)

            cmds = []

            if connection.protocol.disablenxext:
                def match_network(nid):
                    return ofdef.create_oxm(ofdef.OXM_OF_METADATA_W, (nid & 0xffff) << 32,
                                            b'\x00\x00\xff\xff\x00\x00\x00\x00')
            else:
                def match_network(nid):
                    return ofdef.create_oxm(ofdef.NXM_NX_REG4, nid)

            # prepush or not ,, it is same , so ..
            def _deleteicmpflows(ipaddress, macaddress, networkid):
                    return [
                        ofdef.ofp_flow_mod(
                            cookie = 0x2,
                            cookie_mask = 0xffffffffffffffff,
                            table_id = l3input,
                            command = ofdef.OFPFC_DELETE,
                            priority = ofdef.OFP_DEFAULT_PRIORITY + 1,
                            buffer_id = ofdef.OFP_NO_BUFFER,
                            out_port = ofdef.OFPP_ANY,
                            out_group = ofdef.OFPG_ANY,
                            match = ofdef.ofp_match_oxm(
                                oxm_fields = [
                                    ofdef.create_oxm(ofdef.NXM_NX_REG4,networkid),
                                    ofdef.create_oxm(ofdef.OXM_OF_ETH_DST,mac_addr_bytes(macaddress)),
                                    ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE,ofdef.ETHERTYPE_IP),
                                    ofdef.create_oxm(ofdef.OXM_OF_IPV4_DST,ip4_addr_bytes(ipaddress)),
                                    ofdef.create_oxm(ofdef.OXM_OF_IP_PROTO,ofdef.IPPROTO_ICMP),
                                    ofdef.create_oxm(ofdef.OXM_OF_ICMPV4_TYPE,8),
                                    ofdef.create_oxm(ofdef.OXM_OF_ICMPV4_CODE,0)
                                ]
                            )
                        )
                    ]

            if not self.parent.prepush:
                def _createicmpflows(ipaddress, macaddress, networkid):
                    return [
                        ofdef.ofp_flow_mod(
                            cookie = 0x2,
                            cookie_mask = 0xffffffffffffffff,
                            table_id = l3input,
                            command = ofdef.OFPFC_ADD,
                            # icmp to router matcher same as ip forward to router
                            # so priority + 1
                            priority = ofdef.OFP_DEFAULT_PRIORITY + 1,
                            buffer_id = ofdef.OFP_NO_BUFFER,
                            out_port = ofdef.OFPP_ANY,
                            out_group = ofdef.OFPG_ANY,
                            match = ofdef.ofp_match_oxm(
                                oxm_fields = [
                                    match_network(networkid),
                                    ofdef.create_oxm(ofdef.OXM_OF_ETH_DST,mac_addr_bytes(macaddress)),
                                    ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE,ofdef.ETHERTYPE_IP),
                                    ofdef.create_oxm(ofdef.OXM_OF_IPV4_DST,ip4_addr_bytes(ipaddress)),
                                    ofdef.create_oxm(ofdef.OXM_OF_IP_PROTO,ofdef.IPPROTO_ICMP),
                                    ofdef.create_oxm(ofdef.OXM_OF_ICMPV4_TYPE,8),
                                    ofdef.create_oxm(ofdef.OXM_OF_ICMPV4_CODE,0)
                                ]
                            ),
                            instructions = [
                                ofdef.ofp_instruction_actions(
                                    actions = [
                                        ofdef.ofp_action_output(
                                            port = ofdef.OFPP_CONTROLLER,
                                            max_len = ofdef.OFPCML_NO_BUFFER
                                        )
                                    ]
                                )
                            ]
                        )
                    ]
            else:
                def _createicmpflows(ipaddress, macaddress, networkid):
                    return [
                        ofdef.ofp_flow_mod(
                            cookie = 0x2,
                            cookie_mask = 0xffffffffffffffff,
                            table_id = l3input,
                            command = ofdef.OFPFC_ADD,
                            # icmp to router matcher same as ip forward to router
                            # so priority + 1
                            priority = ofdef.OFP_DEFAULT_PRIORITY + 1,
                            buffer_id = ofdef.OFP_NO_BUFFER,
                            out_port = ofdef.OFPP_ANY,
                            out_group = ofdef.OFPG_ANY,
                            match = ofdef.ofp_match_oxm(
                                oxm_fields = [
                                    match_network(networkid),
                                    ofdef.create_oxm(ofdef.OXM_OF_ETH_DST,mac_addr_bytes(macaddress)),
                                    ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE,ofdef.ETHERTYPE_IP),
                                    ofdef.create_oxm(ofdef.OXM_OF_IPV4_DST,ip4_addr_bytes(ipaddress)),
                                    ofdef.create_oxm(ofdef.OXM_OF_IP_PROTO,ofdef.IPPROTO_ICMP),
                                    ofdef.create_oxm(ofdef.OXM_OF_ICMPV4_TYPE,8),
                                    ofdef.create_oxm(ofdef.OXM_OF_ICMPV4_CODE,0)
                                ]
                            ),
                            instructions = [
                                ofdef.ofp_instruction_actions(
                                    actions = [
                                        ofdef.nx_action_reg_move(
                                            n_bits = 48,
                                            src = ofdef.OXM_OF_ETH_SRC,
                                            dst = ofdef.OXM_OF_ETH_DST
                                        ),
                                        ofdef.ofp_action_set_field(
                                            field = ofdef.create_oxm(
                                                ofdef.OXM_OF_ETH_SRC,
                                                ofdef.mac_addr(macaddress)
                                            )
                                        ),
                                        ofdef.nx_action_reg_move(
                                            n_bits = 32,
                                            src = ofdef.OXM_OF_IPV4_SRC,
                                            dst = ofdef.OXM_OF_IPV4_DST
                                        ),
                                        ofdef.ofp_action_set_field(
                                            field = ofdef.create_oxm(
                                                ofdef.OXM_OF_IPV4_SRC,
                                                ofdef.ip4_addr(ipaddress)
                                            )
                                        ),
                                        ofdef.ofp_action_set_field(
                                            field = ofdef.create_oxm(
                                                ofdef.OXM_OF_ICMPV4_TYPE,
                                                ICMP_ECHOREPLY
                                            )
                                        ),
                                        ofdef.ofp_action_nw_ttl(
                                            nw_ttl = 128
                                        ),
                                        ofdef.ofp_action_output(
                                            port = ofdef.OFPP_IN_PORT
                                        )
                                    ]
                                )
                            ]
                        )
                    ]

            for subnet in lastsubnetsinfo.keys():
                if subnet not in currentsubnetsinfo\
                        or (subnet in currentsubnetsinfo and lastsubnetsinfo[subnet] != currentsubnetsinfo[subnet]):
                    # subnet remove  or subnet info changed , remove flow info
                    ip_address, mac_address, networkid, nid = lastsubnetsinfo[subnet]

                    remove_arp = {(ip_address,mac_address,networkid,True),}
                    for m in callAPI(self, 'arpresponder', 'removeproxyarp', {'connection':connection,
                                                                              'arpentries': remove_arp}):
                        yield m

                    cmds.extend(_deleteicmpflows(ip_address,mac_address,nid))

            for m in self.execute_commands(connection, cmds):
                yield m

            for subnet in currentsubnetsinfo.keys():
                if subnet not in lastsubnetsinfo\
                        or (subnet in lastsubnetsinfo and lastsubnetsinfo[subnet] != currentsubnetsinfo[subnet]):

                    ip_address, mac_address, networkid, nid = currentsubnetsinfo[subnet]

                    add_arp = {(ip_address,mac_address,networkid,True),}
                    for m in callAPI(self, 'arpresponder', 'createproxyarp', {'connection': connection,
                                                                               'arpentries': add_arp}):
                        yield m

                    cmds.extend(_createicmpflows(ip_address,mac_address,nid))

            for m in self.execute_commands(connection, cmds):
                yield m

        except Exception:
            self._logger.warning("Unexpected exception in icmp_flow_updater, ignore it! Continue",exc_info=True)

@defaultconfig
@depend(ofpportmanager.OpenflowPortManager,objectdb.ObjectDB)
class ICMPResponder(FlowBase):
    """
    Respond ICMP echo (ping) requests to the gateway
    """
    _tablerequest = (
        ("l3input",("l2input",),""),
        ("l2output",("l3input",),"")
    )
    # True : reply icmp ping with flow
    # False: reply icmp ping with controller PACKET_IN/PACKET_OUT
    #
    # Must use prepush=True with OpenvSwitch 2.5+
    #
    _default_prepush = False

    # "Gateway" responds with this MAC address
    _default_inroutermac = '1a:23:67:59:63:33'

    def __init__(self,server):
        super(ICMPResponder,self).__init__(server)
        self.app_routine = RoutineContainer(self.scheduler)
        self.app_routine.main = self._main
        self.routines.append(self.app_routine)
        self._flowupdater = dict()

    def _main(self):

        flowinit = FlowInitialize.createMatcher(_ismatch=lambda x: self.vhostbind is None or
                                                x.vhost in self.vhostbind)
        conndown = OpenflowConnectionStateEvent.createMatcher(state = OpenflowConnectionStateEvent.CONNECTION_DOWN,
                                                _ismatch=lambda x:self.vhostbind is None or
                                                x.createby.vhost in self.vhostbind)
        while True:
            yield (flowinit,conndown)
            if self.app_routine.matcher is flowinit:
                c = self.app_routine.event.connection
                self.app_routine.subroutine(self._init_conn(c))
            if self.app_routine.matcher is conndown:
                c = self.app_routine.event.connection
                self.app_routine.subroutine(self._remove_conn(c))

    def _init_conn(self,conn):

        if conn in self._flowupdater:
            updater = self._flowupdater.pop(conn)
            updater.close()

        updater = ICMPResponderUpdater(conn,self)
        self._flowupdater[conn] = updater
        updater.start()

        if False:
            yield

    def _remove_conn(self,conn):

        if conn in self._flowupdater:
            updater = self._flowupdater.pop(conn)
            updater.close()

        if False:
            yield

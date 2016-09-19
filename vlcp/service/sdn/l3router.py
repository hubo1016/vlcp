import itertools

import time

import vlcp.service.sdn.ioprocessing as iop
from vlcp.config import defaultconfig
from vlcp.event import Event

from vlcp.event import RoutineContainer
from vlcp.event import withIndices
from vlcp.protocol.openflow import OpenflowAsyncMessageEvent
from vlcp.protocol.openflow import OpenflowConnectionStateEvent
from vlcp.server.module import callAPI, depend
from vlcp.service.sdn.flowbase import FlowBase
from vlcp.service.sdn.ofpmanager import FlowInitialize
from vlcp.service.sdn import arpresponder
from vlcp.service.sdn import icmpresponder
from vlcp.utils.ethernet import mac_addr_bytes, ip4_addr_bytes, ip4_addr, arp_packet_l4, mac_addr, ethernet_l4, \
    ethernet_l7
from vlcp.utils.flowupdater import FlowUpdater
from vlcp.utils.netutils import parse_ip4_network,get_netmask
from vlcp.utils.networkmodel import VRouter, RouterPort, SubNet


@withIndices("connection")
class ARPRequest(Event):
    pass

class RouterUpdater(FlowUpdater):
    def __init__(self, connection, parent):
        super(RouterUpdater, self).__init__(connection, (), ("routerupdater", connection), parent._logger)
        self._parent = parent
        self._lastlogicalport = dict()
        self._lastlogicalnet = dict()
        self._lastphyport = dict()

        self._lastrouterinfo = dict()
        self._lastsubnetinfo = dict()
        self._lastlgportinfo = dict()
        self._original_keys = ()

        self._packet_buffer = dict()
        self._arp_cache = dict()

    def main(self):
        try:
            self.subroutine(self._update_handler(), True, "updater_handler")
            self.subroutine(self._router_packetin_handler(), True, "router_packetin_handler")
            self.subroutine(self._arp_cache_handler(),True,"arp_cache_handler")
            self.subroutine(self._time_cycle_handler(),True,"time_cycle_handler")
            for m in FlowUpdater.main(self):
                yield m
        finally:

            if hasattr(self, "updater_handler"):
                self.updater_handler.close()

            if hasattr(self, "router_packetin_handler"):
                self.router_packetin_handler.close()

            if hasattr(self,"arp_cache_handler"):
                self.arp_cache_handler.close()
            
            if hasattr(self,"time_cycle_handler"):
                self.time_cycle_handler.close()

    def _getinterfaceinfo(self,netid):

        find = False
        mac = None
        ip = None
        phyportno = None

        for _,interfaces in self._lastrouterinfo.values():
            for macaddress,ipaddress,_,_,nid,phyport in interfaces:
                if netid == nid:
                    mac = macaddress
                    ip = ipaddress
                    phyportno = phyport
                    find = True
                    break

        if find:
            return (mac, ip, phyportno)
        else:
            return ()

    def _packet_out_message(self,netid,packet,portno):
        l2output_next = self._parent._getnexttable('', 'l2output', self._connection.protocol.vhost)
        ofdef = self._connection.openflowdef

        for m in self.execute_commands(self._connection,
                                       [
                                           ofdef.ofp_packet_out(
                                               buffer_id=ofdef.OFP_NO_BUFFER,
                                               in_port=ofdef.OFPP_CONTROLLER,
                                               actions=[
                                                   ofdef.ofp_action_set_field(
                                                       field=ofdef.create_oxm(ofdef.NXM_NX_REG4,
                                                                              netid)
                                                   ),
                                                   ofdef.ofp_action_set_field(
                                                       field=ofdef.create_oxm(ofdef.NXM_NX_REG5,
                                                                              netid)
                                                   ),
                                                   ofdef.ofp_action_set_field(
                                                       field=ofdef.create_oxm(ofdef.NXM_NX_REG6,
                                                                              portno)
                                                   ),
                                                   ofdef.nx_action_resubmit(
                                                       in_port=ofdef.OFPP_IN_PORT & 0xffff,
                                                       table=l2output_next
                                                   )
                                               ],
                                               data=packet._tobytes()
                                           )
                                       ]
                                       ):
            yield m

    def _time_cycle_handler(self):

        while True:
            for m in self.waitWithTimeout(self._parent.arp_cycle_time):
                yield m
            ct = int(time.time())

            # check incomplte arp entry ,, send arp request cycle unitl timeout
            for k,v in self._arp_cache.items():
                status,timeout,isgateway,realmac = v

                if status == 1:
                    if ct > timeout:
                        self._arp_cache.pop(k)
                        del self._packet_buffer[k]
                    else:
                        # packet out an arp request
                        outnetid,request_ip= k
                        ofdef = self._connection.openflowdef

                        info = self._getinterfaceinfo(outnetid)

                        if info:
                            mac,ipaddress,phyport = info
                            if phyport:
                                arp_request_packet = arp_packet_l4(
                                    dl_src=mac_addr(mac),
                                    dl_dst=mac_addr("FF:FF:FF:FF:FF:FF"),
                                    arp_op=ofdef.ARPOP_REQUEST,
                                    arp_sha=mac_addr(mac),
                                    arp_spa=ip4_addr(ipaddress),
                                    arp_tpa=request_ip
                                )
                                for m in self._packet_out_message(outnetid,arp_request_packet,phyport):
                                    yield m
                            # else
                            # arp_cache will not have entry which goto network has no phyport
                            # so nerver run here
                        else:
                            self._logger.warning("arp request can find avaliable network %d drop it",outnetid)
                            self._arp_cache.pop(k)
                            del self._packet_buffer[k]
                if status == 3:
                    if ct > timeout:
                        realmac = mac_addr("FF:FF:FF:FF:FF:FF")
                    # packet out an arp request
                    outnetid,request_ip= k
                    ofdef = self._connection.openflowdef

                    info = self._getinterfaceinfo(outnetid)

                    if info:
                        mac,ipaddress,phyport = info
                        if phyport:
                            arp_request_packet = arp_packet_l4(
                                dl_src=mac_addr(mac),
                                dl_dst=realmac,
                                arp_op=ofdef.ARPOP_REQUEST,
                                arp_sha=mac_addr(mac),
                                arp_spa=ip4_addr(ipaddress),
                                arp_tpa=request_ip
                            )
                            for m in self._packet_out_message(outnetid,arp_request_packet,phyport):
                                yield m
                

            # when request one arp , but there no reply ,
            # buffer will have timeout packet , so checkout it here
            for k, v in self._packet_buffer.items():
                nv = [(p,bid,t) for p,bid,t in v if ct < t]
                self._packet_buffer[k] = nv

    def _arp_cache_handler(self):

        arp_request_matcher = ARPRequest.createMatcher(connection=self._connection)
        arp_incomplete_timeout = self._parent.arp_incomplete_timeout

        while True:
            yield (arp_request_matcher,)
            ct = int(time.time())

            ipaddress = self.event.ipaddress
            netid = self.event.logicalnetworkid
            isgateway = self.event.isgateway

            if (netid,ipaddress) not in self._arp_cache:
                entry = (1,ct + arp_incomplete_timeout,isgateway,"")
                self._arp_cache[(netid,ipaddress)] = entry

                ofdef = self._connection.openflowdef
                info = self._getinterfaceinfo(netid)
                if info:
                    mac,interface_ip,phyport = info
                    if phyport:
                        arp_request_packet = arp_packet_l4(
                                dl_src=mac_addr(mac),
                                dl_dst=mac_addr("FF:FF:FF:FF:FF:FF"),
                                arp_op=ofdef.ARPOP_REQUEST,
                                arp_sha=mac_addr(mac),
                                arp_spa=ip4_addr(interface_ip),
                                arp_tpa=ipaddress
                        )

                        self.subroutine(self._packet_out_message(netid,arp_request_packet,phyport))
                    else:
                        # logicalnetwork has no phyport, don't send
                        # arp request, drop arp cache , and packet
                        del self._arp_cache[(netid,ipaddress)]

                        if (netid,ipaddress) in self._packet_buffer:
                            del self._packet_buffer[(netid,ipaddress)]

                        self._logger.warning(" lgnet %r don't have phyport, drop everything to it",netid)
            else:
                s,_,g,mac = self._arp_cache[(netid,ipaddress)]
                # this arp request have in cache , update timeout
                entry = (s, ct + arp_incomplete_timeout, g,mac)
                self._arp_cache[(netid,ipaddress)] = entry

    def _router_packetin_handler(self):
        conn = self._connection
        ofdef = self._connection.openflowdef

        l3output = self._parent._gettableindex("l3output", self._connection.protocol.vhost)
        l3input = self._parent._gettableindex("l3input", self._connection.protocol.vhost)
        l2output = self._parent._gettableindex("l2output", self._connection.protocol.vhost)

        packetin_matcher = OpenflowAsyncMessageEvent.createMatcher(ofdef.OFPT_PACKET_IN, None, None, l3output, None,
                                                                   self._connection, self._connection.connmark)

        arpreply_matcher = OpenflowAsyncMessageEvent.createMatcher(ofdef.OFPT_PACKET_IN, None, None, l3input, 0x4,
                                                                   self._connection, self._connection.connmark)

        arpflow_remove_matcher = OpenflowAsyncMessageEvent.createMatcher(ofdef.OFPT_FLOW_REMOVED, None, None,
                                                    l3output, None, self._connection, self._connection.connmark)

        arpflow_request_matcher = OpenflowAsyncMessageEvent.createMatcher(ofdef.OFPT_PACKET_IN, None, None, l3output,
                                                    0x1,self._connection, self._connection.connmark)

        def _send_broadcast_packet_out(netid,packet):
            # in_port == controller
            # input network( reg4 ) == outnetwork (reg5)
            # output port (reg6) = 0xffffffff
            for m in self.execute_commands(conn,
                        [
                            ofdef.ofp_packet_out(
                                buffer_id=ofdef.OFP_NO_BUFFER,
                                in_port=ofdef.OFPP_CONTROLLER,
                                actions=[
                                    ofdef.ofp_action_set_field(
                                        field=ofdef.create_oxm(ofdef.NXM_NX_REG4, netid)
                                    ),
                                    ofdef.ofp_action_set_field(
                                        field=ofdef.create_oxm(ofdef.NXM_NX_REG5, netid)
                                    ),
                                    ofdef.ofp_action_set_field(
                                        field=ofdef.create_oxm(ofdef.NXM_NX_REG6, 0xffffffff)
                                    ),
                                    ofdef.nx_action_resubmit(
                                        in_port=ofdef.OFPP_IN_PORT & 0xffff,
                                        table=l2output
                                    )
                                ],
                                data = packet._tobytes()
                            )
                        ]
                        ):
                yield m

        def _send_buffer_packet_out(netid,macaddress,ipaddress,srcmacaddress,packet,bid = ofdef.OFP_NO_BUFFER):
            for m in self.execute_commands(conn,
                        [
                            ofdef.ofp_packet_out(
                                buffer_id = bid,
                                in_port = ofdef.OFPP_CONTROLLER,
                                actions = [
                                    ofdef.ofp_action_set_field(
                                        field = ofdef.create_oxm(ofdef.NXM_NX_REG5,netid)
                                    ),
                                    ofdef.ofp_action_set_field(
                                        field=ofdef.create_oxm(ofdef.NXM_NX_REG6, 0xffffffff)
                                    ),
                                    ofdef.ofp_action_set_field(
                                        field = ofdef.create_oxm(ofdef.OXM_OF_ETH_SRC,srcmacaddress)
                                    ),
                                    ofdef.ofp_action_set_field(
                                        field = ofdef.create_oxm(ofdef.OXM_OF_ETH_DST,macaddress)
                                    ),
                                    ofdef.ofp_action(
                                        type = ofdef.OFPAT_DEC_NW_TTL    
                                    ),
                                    ofdef.nx_action_resubmit(
                                        in_port = ofdef.OFPP_IN_PORT & 0xffff,
                                        table = l2output
                                    )
                                ],
                                data = packet._tobytes()
                            )
                        ]
                        ):
                yield m

        def _add_host_flow(netid,macaddress,ipaddress,srcmaddress):
            for m in self.execute_commands(conn,
                        [
                            ofdef.ofp_flow_mod(
                                table_id=l3output,
                                command=ofdef.OFPFC_ADD,
                                priority=ofdef.OFP_DEFAULT_PRIORITY + 1,
                                buffer_id=ofdef.OFP_NO_BUFFER,
                                hard_timeout = self._parent.arp_complete_timeout,
                                out_port=ofdef.OFPP_ANY,
                                out_group=ofdef.OFPG_ANY,
                                match=ofdef.ofp_match_oxm(
                                    oxm_fields=[
                                        ofdef.create_oxm(ofdef.NXM_NX_REG5, netid),
                                        ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE, ofdef.ETHERTYPE_IP),
                                        ofdef.create_oxm(ofdef.OXM_OF_IPV4_DST,ipaddress)
                                        ]
                                ),
                                instructions=[
                                        ofdef.ofp_instruction_actions(
                                                actions = [
                                                    ofdef.ofp_action_set_field(
                                                        field=ofdef.create_oxm(ofdef.OXM_OF_ETH_SRC, srcmaddress)
                                                    ),
                                                    ofdef.ofp_action_set_field(
                                                        field=ofdef.create_oxm(ofdef.OXM_OF_ETH_DST,macaddress)
                                                    ),
                                                    ofdef.ofp_action(
                                                        type=ofdef.OFPAT_DEC_NW_TTL
                                                    )
                                                ]
                                            ),
                                        ofdef.ofp_instruction_goto_table(table_id=l2output)
                                        ]
                            ),
                            ofdef.ofp_flow_mod(
                                cookie = 0x1,
                                cookie_mask=0xffffffffffffffff,
                                table_id=l3output,
                                command=ofdef.OFPFC_ADD,
                                priority=ofdef.OFP_DEFAULT_PRIORITY,
                                buffer_id=ofdef.OFP_NO_BUFFER,
                                idle_timeout=self._parent.arp_complete_timeout * 2,
                                flags = ofdef.OFPFF_SEND_FLOW_REM,
                                out_port=ofdef.OFPP_ANY,
                                out_group=ofdef.OFPG_ANY,
                                match=ofdef.ofp_match_oxm(
                                    oxm_fields=[
                                        ofdef.create_oxm(ofdef.NXM_NX_REG5, netid),
                                        ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE, ofdef.ETHERTYPE_IP),
                                        ofdef.create_oxm(ofdef.OXM_OF_IPV4_DST, ipaddress)
                                    ]
                                ),
                                instructions=[
                                    ofdef.ofp_instruction_actions(
                                        actions=[
                                            ofdef.ofp_action_set_field(
                                                field=ofdef.create_oxm(ofdef.OXM_OF_ETH_SRC, srcmaddress)
                                            ),
                                            ofdef.ofp_action_set_field(
                                                field=ofdef.create_oxm(ofdef.OXM_OF_ETH_DST, macaddress)
                                            ),
                                            ofdef.ofp_action(
                                                type=ofdef.OFPAT_DEC_NW_TTL
                                            ),
                                            ofdef.ofp_action_output(
                                                port = ofdef.OFPP_CONTROLLER,
                                                max_len = 60
                                            )
                                        ]
                                    ),
                                    ofdef.ofp_instruction_goto_table(table_id=l2output)
                                ]
                            )
                        ]
                        ):
                yield m


        while True:
            yield (packetin_matcher, arpreply_matcher,arpflow_request_matcher,arpflow_remove_matcher)

            msg = self.event.message
            try:
                if self.matcher is packetin_matcher:
                    outnetworkid = ofdef.uint32.create(ofdef.get_oxm(msg.match.oxm_fields, ofdef.NXM_NX_REG5))
                    
                    ippacket = ethernet_l4.create(msg.data)

                    ct = time.time()
                    
                    if (outnetworkid,ippacket.ip_dst) in self._arp_cache:
                        status,_,_,mac = self._arp_cache[(outnetworkid,ippacket.ip_dst)]
                        
                        # this mac is real mac
                        if status == 2:
                            info = self._getinterfaceinfo(outnetworkid)
                            if info:
                                smac,ip,_= info
                                self.subroutine(_send_buffer_packet_out(outnetworkid,mac,ip,mac_addr(smac),ippacket,msg.buffer_id))
                                continue

                    if (outnetworkid,ippacket.ip_dst) in self._packet_buffer:
                        # checkout timeout packet
                        nv = [(p,bid,t) for p,bid,t in self._packet_buffer[(outnetworkid,ippacket.ip_dst)]
                                if ct < t]
                        nv.append((ippacket,msg.buffer_id,ct + self._parent.buffer_packet_timeout))
                        self._packet_buffer[(outnetworkid,ippacket.ip_dst)] = nv
                    else:
                        self._packet_buffer[(outnetworkid,ippacket.ip_dst)] = \
                            [(ippacket,msg.buffer_id,ct + self._parent.buffer_packet_timeout)]
                    e = ARPRequest(self._connection,ipaddress=ippacket.ip_dst,
                                    logicalnetworkid=outnetworkid,isgateway=False)

                    self.subroutine(self.waitForSend(e))

                if self.matcher is arpflow_request_matcher:
                    outnetworkid = ofdef.uint32.create(ofdef.get_oxm(msg.match.oxm_fields, ofdef.NXM_NX_REG5))
                    #ipaddress = ofdef.get_oxm(msg.match.oxm_fields,ofdef.OXM_OF_IPV4_DST)
                    
                    ippacket = ethernet_l4.create(msg.data)
                    ipaddress = ippacket.ip_dst
                    ct = time.time()

                    if(outnetworkid,ipaddress) in self._arp_cache:

                        status,timeout,isgateway,mac = self._arp_cache[(outnetworkid,ipaddress)]

                        if status == 2:
                            # we change this arp entry status in cache ,, next cycle will send arp request
                            entry = (3,timeout,isgateway,mac)
                            self._arp_cache[(outnetworkid,ipaddress)] = entry

                if self.matcher is arpflow_remove_matcher:
                    nid = ofdef.uint32.create(ofdef.get_oxm(msg.match.oxm_fields, ofdef.NXM_NX_REG5))
                    ip_address = ip4_addr(ip4_addr_bytes.formatter(
                                    ofdef.get_oxm(msg.match.oxm_fields, ofdef.OXM_OF_IPV4_DST)))
                    
                    if(nid,ip_address) in self._arp_cache:

                        del self._arp_cache[(nid,ip_address)]

                    if (nid,ip_address) in self._packet_buffer:
                        del self._packet_buffer[(nid,ip_address)]

                if self.matcher is arpreply_matcher:
                    netid = ofdef.uint32.create(ofdef.get_oxm(msg.match.oxm_fields,ofdef.NXM_NX_REG5))

                    arp_reply_packet = ethernet_l7.create(msg.data)
                    
                    reply_ipaddress = arp_reply_packet.arp_spa
                    reply_macaddress = arp_reply_packet.arp_sha

                    dst_macaddress = arp_reply_packet.dl_dst
                    if (netid,reply_ipaddress) in self._arp_cache:
                        status, timeout, isgateway,_ = self._arp_cache[(netid,reply_ipaddress)]
                        if isgateway:
                            # add default router in l3router
                            pass
                        else:
                            ct = time.time()
                            # this is the first arp reply
                            if status == 1 or status == 3:
                                # complete timeout ,,, after flow hard_timeout, packet will send to controller too
                                # if packet in this timeout ,  will send an unicast arp request
                                # is best  1*self._parent.arp_complete_timeout < t < 2*self._parent.arp_complete_timeout
                                self._arp_cache[(netid,reply_ipaddress)] = (2,
                                            ct + self._parent.arp_complete_timeout + 20,False,reply_macaddress)
                                
                                # search msg buffer ,, packet out msg there wait this arp reply
                                if (netid,reply_ipaddress) in self._packet_buffer:

                                    for packet,bid, t in self._packet_buffer[(netid,reply_ipaddress)]:
                                        self.subroutine(_send_buffer_packet_out(netid,reply_macaddress,
                                                                            reply_ipaddress,dst_macaddress,packet,bid))

                                    del self._packet_buffer[(netid,reply_ipaddress)]

                            # add flow about this host in l3output

                            # change asyncStart from false to true ,,  send buffer packet before add flow
                            self.subroutine(_add_host_flow(netid,reply_macaddress,reply_ipaddress,dst_macaddress))


            except Exception:
                self._logger.warning(" handler router packetin message error , ignore !",exc_info=True)

    def _update_handler(self):

        dataobjectchange = iop.DataObjectChanged.createMatcher(None, None, self._connection)

        while True:
            yield (dataobjectchange,)

            self._lastlogicalport, self._lastphyport, self._lastlogicalnet, _ = self.event.current

            self._update_walk()

    def _update_walk(self):

        logicalportkeys = [p.getkey() for p, _ in self._lastlogicalport]
        logicalnetkeys = [n.getkey() for n, _ in self._lastlogicalnet]
        phyportkeys = [p.getkey() for p,_ in self._lastphyport]    

        self._initialkeys = logicalportkeys + logicalnetkeys + phyportkeys
        self._original_keys = logicalportkeys + logicalnetkeys + phyportkeys

        self._walkerdict = dict(itertools.chain(((p, self._walk_lgport) for p in logicalportkeys),
                                                ((n, self._walk_lgnet) for n in logicalnetkeys),
                                                ((p, self._walk_phyport) for p in phyportkeys)))

        self.subroutine(self.restart_walk(), False)

    def _walk_lgport(self, key, value, walk, save):

        if value is None:
            return
        save(key)

        # lgport --> subnet --> routerport --> router --> routerport --->> subnet --->> logicalnet
        if hasattr(value, "subnet"):
            try:
                subnetobj = walk(value.subnet.getkey())
            except KeyError:
                pass
            else:
                save(subnetobj.getkey())

                if hasattr(subnetobj, "router"):
                    try:
                        routerport = walk(subnetobj.router.getkey())
                    except KeyError:
                        pass
                    else:
                        save(routerport.getkey())

                        if hasattr(routerport, "router"):
                            try:
                                router = walk(routerport.router.getkey())
                            except KeyError:
                                pass
                            else:
                                save(router.getkey())

                                if router.interfaces.dataset():

                                    for weakobj in router.interfaces.dataset():
                                        routerport_weakkey = weakobj.getkey()

                                        # we walk from this key , so except
                                        if routerport_weakkey != routerport.getkey():
                                            try:
                                                weakrouterport = walk(routerport_weakkey)
                                            except KeyError:
                                                pass
                                            else:
                                                save(routerport_weakkey)

                                                if hasattr(weakrouterport, "subnet"):
                                                    try:
                                                        weaksubnet = walk(weakrouterport.subnet.getkey())
                                                    except KeyError:
                                                        pass
                                                    else:
                                                        save(weaksubnet.getkey())

                                                        if hasattr(weaksubnet, "network"):
                                                            try:
                                                                logicalnetwork = walk(weaksubnet.network.getkey())
                                                            except KeyError:
                                                                pass
                                                            else:
                                                                save(logicalnetwork.getkey())

    def _walk_lgnet(self, key, value, walk, save):

        if value is None:
            return

        save(key)
    
    def _walk_phyport(self, key, value, walk, save):

        if value is None:
            return

        save(key)
    
    def reset_initialkeys(self,keys,values):
        
        subnetkeys = [k for k,v in zip(keys,values) if v.isinstance(SubNet)]
        routerportkeys = [k for k,v in zip(keys,values) if v.isinstance(RouterPort)]
        routerkeys = [k for k,v in zip(keys,values) if v.isinstance(VRouter)]

        self._initialkeys = tuple(itertools.chain(self._original_keys,subnetkeys,
                                    routerportkeys,routerkeys))

    def updateflow(self, connection, addvalues, removevalues, updatedvalues):

        try:
            datapath_id = connection.openflow_datapathid
            ofdef = connection.openflowdef
            vhost = connection.protocol.vhost

            lastrouterinfo = self._lastrouterinfo
            lastsubnetinfo = self._lastsubnetinfo
            lastlgportinfo = self._lastlgportinfo

            allobjects = set(o for o in self._savedresult if o is not None and not o.isdeleted())

            # phyport : phynet = 1:1, so we use phynet as key
            currentphyportinfo = dict((p.physicalnetwork, (p,id)) for p, id in self._lastphyport if p in allobjects)

            currentlognetinfo = {}

            lognetinfo = dict((n,id) for n,id in self._lastlogicalnet if n in allobjects)
            
            for n,id in lognetinfo.items():
                # this lognetwork has phyport, we should get phyport mac
                # as the base mac to produce mac that when router send packet used!
                # else , use innmac
                if n.physicalnetwork in currentphyportinfo:
                    _,phyportid = currentphyportinfo[n.physicalnetwork]

                    for m in callAPI(self, "openflowportmanager", "waitportbyno",
                                     {"datapathid": datapath_id, "vhost": vhost, "portno": phyportid}):
                        yield m

                    portmac = self.retvalue.hw_addr

                    # convert physicalport mac as router out mac
                    outmac = [s ^ m for s, m in zip(portmac, mac_addr(self._parent.outroutermacmask))]

                    currentlognetinfo[n] = (id,mac_addr.formatter(outmac),phyportid)
                else:
                    currentlognetinfo[n] = (id,self._parent.inroutermac,None)

            currentrouterportinfo = dict((r.getkey(),(r,r.subnet,r.router)) for r in allobjects
                                            if r.isinstance(RouterPort))

            currentsubnetinfo = dict((s, (currentrouterportinfo[s.router.getkey()][2],currentlognetinfo[s.network][1],
                                          getattr(s,"gateway",None),s.cidr,
                                          getattr(s,"external",False),
                                          currentlognetinfo[s.network][0],
                                          currentlognetinfo[s.network][2]))
                                     for s in allobjects if s.isinstance(SubNet) and s.network in currentlognetinfo
                                     and hasattr(s,'router') and s.router.getkey() in currentrouterportinfo)

            currentlgportinfo = dict((p,(p.ip_address,p.mac_address,currentlognetinfo[p.network][0],
                                         currentlognetinfo[p.network][1],p.network.id)) for p,_ in self._lastlogicalport
                                     if p in allobjects and hasattr(p,"ip_address")
                                     and hasattr(p,"mac_address") and hasattr(p,"subnet")
                                     and p.subnet in currentsubnetinfo
                                     and p.network in currentlognetinfo)

            currentrouterinfo = dict((r, (r.routes,
                                            [(currentsubnetinfo[currentrouterportinfo[interface_key][0].subnet][1],
                                            getattr(currentrouterportinfo[interface_key][0], "ip_address",
                                                currentsubnetinfo[currentrouterportinfo[interface_key][0].subnet][2]),
                                            currentsubnetinfo[currentrouterportinfo[interface_key][0].subnet][3],
                                            currentsubnetinfo[currentrouterportinfo[interface_key][0].subnet][4],
                                            currentsubnetinfo[currentrouterportinfo[interface_key][0].subnet][5],
                                            currentsubnetinfo[currentrouterportinfo[interface_key][0].subnet][6])
                                            for interface_key in currentrouterportinfo
                                            if currentrouterportinfo[interface_key][0].router.getkey() == r.getkey()
                                            and hasattr(currentrouterportinfo[interface_key][0], "subnet") and
                                            (hasattr(currentrouterportinfo[interface_key][0], "ip_address")
                                                or hasattr(currentrouterportinfo[interface_key][0].subnet, "gateway"))
                                            and currentrouterportinfo[interface_key][0].subnet in currentsubnetinfo
                                           ])
                                      ) for r in allobjects if r.isinstance(VRouter)
                                     )

            self._lastrouterinfo = currentrouterinfo
            self._lastsubnetinfo = currentsubnetinfo
            self._lastlgportinfo = currentlgportinfo

            l3input = self._parent._gettableindex("l3input", vhost)
            l3router = self._parent._gettableindex("l3router", vhost)
            l3output = self._parent._gettableindex("l3output", vhost)
            l2output = self._parent._gettableindex("l2output", vhost)
            arpreply = self._parent._gettableindex("arp",vhost)

            cmds = []

            if connection.protocol.disablenxext:
                def match_network(nid):
                    return ofdef.create_oxm(ofdef.OXM_OF_METADATA_W, (nid & 0xffff) << 32,
                                            b'\x00\x00\xff\xff\x00\x00\x00\x00')
            else:
                def match_network(nid):
                    return ofdef.create_oxm(ofdef.NXM_NX_REG4, nid)

            def _createinputflow(macaddress, ipaddress, netid):
                return [
                    ofdef.ofp_flow_mod(
                        cookie=0x3,
                        cookie_mask=0xffffffffffffffff,
                        table_id=l3input,
                        command=ofdef.OFPFC_ADD,
                        priority=ofdef.OFP_DEFAULT_PRIORITY,
                        buffer_id=ofdef.OFP_NO_BUFFER,
                        out_port=ofdef.OFPP_ANY,
                        out_group=ofdef.OFPG_ANY,
                        match=ofdef.ofp_match_oxm(
                            oxm_fields=[
                                match_network(netid),
                                ofdef.create_oxm(ofdef.OXM_OF_ETH_DST, mac_addr_bytes(macaddress)),
                                ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE, ofdef.ETHERTYPE_IP)
                                #ofdef.create_oxm(ofdef.OXM_OF_IPV4_DST, ip4_addr_bytes(ipaddress))
                            ]
                        ),
                        instructions=[
                            ofdef.ofp_instruction_goto_table(table_id=l3router)
                        ]
                    )
                ]

            def _deleteinputflow(macaddress, ipaddress, netid):
                return [
                    ofdef.ofp_flow_mod(
                        cookie=0x3,
                        cookie_mask=0xffffffffffffffff,
                        table_id=l3input,
                        command=ofdef.OFPFC_DELETE,
                        out_port=ofdef.OFPP_ANY,
                        out_group=ofdef.OFPG_ANY,
                        match=ofdef.ofp_match_oxm(
                            oxm_fields=[
                                match_network(netid),
                                ofdef.create_oxm(ofdef.OXM_OF_ETH_DST, mac_addr_bytes(macaddress)),
                                ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE, ofdef.ETHERTYPE_IP)
                            ]
                        )
                    )
                ]

            def _createrouterflow(routes, nid):
                ret = []

                for cidr, prefix, netid in routes:
                    flow = ofdef.ofp_flow_mod(
                        table_id=l3router,
                        command=ofdef.OFPFC_ADD,
                        priority=ofdef.OFP_DEFAULT_PRIORITY + prefix,
                        buffer_id=ofdef.OFP_NO_BUFFER,
                        out_port=ofdef.OFPP_ANY,
                        out_group=ofdef.OFPG_ANY,
                        match=ofdef.ofp_match_oxm(
                            oxm_fields=[
                                match_network(nid),
                                ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE, ofdef.ETHERTYPE_IP),
                                ofdef.create_oxm(ofdef.OXM_OF_IPV4_DST_W,
                                                 cidr,
                                                 get_netmask(prefix))
                            ]
                        ),
                        instructions=[
                            ofdef.ofp_instruction_actions(
                                actions=[
                                    ofdef.ofp_action_set_field(
                                        field=ofdef.create_oxm(ofdef.NXM_NX_REG5, netid)
                                    )
                                ]
                            ),
                            ofdef.ofp_instruction_goto_table(table_id=l3output)
                        ]
                    )

                    ret.append(flow)

                return ret

            def _deleterouterflow(netid):

                return [
                    ofdef.ofp_flow_mod(
                        table_id=l3router,
                        command=ofdef.OFPFC_DELETE,
                        out_port=ofdef.OFPP_ANY,
                        out_group=ofdef.OFPG_ANY,
                        match=ofdef.ofp_match_oxm(
                            oxm_fields=[
                                match_network(netid)
                            ]
                        )
                    )
                ]

            def _createarpreplyflow(macaddress, ipaddress, netid):
                return [
                    ofdef.ofp_flow_mod(
                        cookie=0x4,
                        cookie_mask=0xffffffffffffffff,
                        table_id=l3input,
                        command=ofdef.OFPFC_ADD,
                        priority=ofdef.OFP_DEFAULT_PRIORITY,
                        buffer_id=ofdef.OFP_NO_BUFFER,
                        out_port=ofdef.OFPP_ANY,
                        out_group=ofdef.OFPG_ANY,
                        match=ofdef.ofp_match_oxm(
                            oxm_fields=[
                                match_network(netid),
                                ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE, ofdef.ETHERTYPE_ARP),
                                ofdef.create_oxm(ofdef.OXM_OF_ETH_DST, mac_addr_bytes(macaddress)),
                                ofdef.create_oxm(ofdef.OXM_OF_ARP_OP, ofdef.ARPOP_REPLY),
                                ofdef.create_oxm(ofdef.OXM_OF_ARP_TPA, ip4_addr(ipaddress))
                            ]
                        ),
                        instructions=[
                            ofdef.ofp_instruction_actions(
                                actions=[
                                    ofdef.ofp_action_output(
                                        port=ofdef.OFPP_CONTROLLER,
                                        max_len=ofdef.OFPCML_NO_BUFFER
                                    )
                                ]
                            )
                        ]
                    )
                ]

            def _deletearpreplyflow(macaddress, ipaddress, netid):
                return [
                    ofdef.ofp_flow_mod(
                        cookie=0x4,
                        cookie_mask=0xffffffffffffffff,
                        table_id=l3input,
                        command=ofdef.OFPFC_DELETE,
                        priority=ofdef.OFP_DEFAULT_PRIORITY,
                        buffer_id=ofdef.OFP_NO_BUFFER,
                        out_port=ofdef.OFPP_ANY,
                        out_group=ofdef.OFPG_ANY,
                        match=ofdef.ofp_match_oxm(
                            oxm_fields=[
                                match_network(netid),
                                ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE, ofdef.ETHERTYPE_ARP),
                                ofdef.create_oxm(ofdef.OXM_OF_ETH_DST, mac_addr_bytes(macaddress)),
                                ofdef.create_oxm(ofdef.OXM_OF_ARP_OP, ofdef.ARPOP_REPLY),
                                ofdef.create_oxm(ofdef.OXM_OF_ARP_TPA, ip4_addr(ipaddress))
                            ]
                        )
                    )
                ]

            def _add_host_flow(netid, macaddress, ipaddress, srcmaddress):
                return  [
                            ofdef.ofp_flow_mod(
                                table_id=l3output,
                                command=ofdef.OFPFC_ADD,
                                priority=ofdef.OFP_DEFAULT_PRIORITY + 1,
                                buffer_id=ofdef.OFP_NO_BUFFER,
                                out_port=ofdef.OFPP_ANY,
                                out_group=ofdef.OFPG_ANY,
                                match=ofdef.ofp_match_oxm(
                                    oxm_fields=[
                                            ofdef.create_oxm(ofdef.NXM_NX_REG5, netid),
                                            ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE,ofdef.ETHERTYPE_IP),
                                            ofdef.create_oxm(ofdef.OXM_OF_IPV4_DST, ip4_addr(ipaddress))
                                                ]
                                    ),
                                instructions=[
                                    ofdef.ofp_instruction_actions(
                                        actions=[
                                            ofdef.ofp_action_set_field(
                                                field=ofdef.create_oxm(ofdef.OXM_OF_ETH_SRC,mac_addr(srcmaddress))
                                                ),
                                            ofdef.ofp_action_set_field(
                                                field=ofdef.create_oxm(ofdef.OXM_OF_ETH_DST,mac_addr(macaddress))
                                                ),
                                            ofdef.ofp_action(
                                                type=ofdef.OFPAT_DEC_NW_TTL
                                            )
                                            ]
                                        ),
                                    ofdef.ofp_instruction_goto_table(table_id=l2output)
                                    ]
                                        )
                        ]

            def _remove_host_flow(netid, macaddress, ipaddress, srcmaddress):
                return [
                            ofdef.ofp_flow_mod(
                                table_id=l3output,
                                command=ofdef.OFPFC_DELETE,
                                priority=ofdef.OFP_DEFAULT_PRIORITY,
                                buffer_id=ofdef.OFP_NO_BUFFER,
                                out_port=ofdef.OFPP_ANY,
                                out_group=ofdef.OFPG_ANY,
                                match=ofdef.ofp_match_oxm(
                                    oxm_fields=[
                                            ofdef.create_oxm(ofdef.NXM_NX_REG5, netid),
                                            ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE,ofdef.ETHERTYPE_IP),
                                            ofdef.create_oxm(ofdef.OXM_OF_IPV4_DST, ip4_addr(ipaddress))
                                                ]
                                    )
                            )
                        ]

            def _createfilterarprequestflow(netid):
                return [
                            ofdef.ofp_flow_mod(
                                table_id=arpreply,
                                command=ofdef.OFPFC_ADD,
                                priority=ofdef.OFP_DEFAULT_PRIORITY - 1,
                                buffer_id=ofdef.OFP_NO_BUFFER,
                                out_port=ofdef.OFPP_ANY,
                                out_group=ofdef.OFPG_ANY,
                                match=ofdef.ofp_match_oxm(
                                    oxm_fields=[
                                        ofdef.create_oxm(ofdef.NXM_NX_REG7_W, 0,0x4000),
                                        ofdef.create_oxm(ofdef.NXM_NX_REG5, netid),
                                        ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE, ofdef.ETHERTYPE_ARP),
                                        ofdef.create_oxm(ofdef.OXM_OF_ARP_OP, ofdef.ARPOP_REQUEST),
                                        ofdef.create_oxm(ofdef.OXM_OF_ETH_DST_W, b'\x01\x00\x00\x00\x00\x00',
                                                         b'\x01\x00\x00\x00\x00\x00')
                                    ]
                                ),
                                instructions=[ofdef.ofp_instruction_actions(type=ofdef.OFPIT_CLEAR_ACTIONS)]
                            )
                ]
            def _removefilterarprequestflow(netid):
                return [
                            ofdef.ofp_flow_mod(
                                table_id=arpreply,
                                command=ofdef.OFPFC_DELETE_STRICT,
                                priority=ofdef.OFP_DEFAULT_PRIORITY - 1,
                                buffer_id=ofdef.OFP_NO_BUFFER,
                                out_port=ofdef.OFPP_ANY,
                                out_group=ofdef.OFPG_ANY,
                                match=ofdef.ofp_match_oxm(
                                    oxm_fields=[
                                        ofdef.create_oxm(ofdef.NXM_NX_REG7_W, 0,0x4000),
                                        ofdef.create_oxm(ofdef.NXM_NX_REG5, netid),
                                        ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE, ofdef.ETHERTYPE_ARP),
                                        ofdef.create_oxm(ofdef.OXM_OF_ARP_OP, ofdef.ARPOP_REQUEST),
                                        ofdef.create_oxm(ofdef.OXM_OF_ETH_DST_W, b'\x01\x00\x00\x00\x00\x00',
                                                         b'\x01\x00\x00\x00\x00\x00')
                                    ]
                                )
                            )
                ]
            for obj in lastrouterinfo:
                if obj not in currentrouterinfo or currentrouterinfo[obj] != lastrouterinfo[obj]:
                    # this means should remove flows
                     static_routes,interfaces = lastrouterinfo[obj]
                     for mac, ipaddress, cidr, isexternal, netid, phyportid in interfaces:
                         cmds.extend(_deleteinputflow(mac, ipaddress, netid))
                         cmds.extend(_deletearpreplyflow(mac,ipaddress,netid))
                         cmds.extend(_removefilterarprequestflow(netid))
                         cmds.extend(_deleterouterflow(netid))
                         if mac != self._parent.inroutermac:
                             cmds.extend(_deleteinputflow(self._parent.inroutermac, ipaddress, netid))
                             cmds.extend(_deletearpreplyflow(self._parent.inroutermac, ipaddress, netid))

            for obj in lastlgportinfo:
                if obj not in currentlgportinfo or currentlgportinfo[obj] != lastlgportinfo[obj]:

                    ipaddress,macaddrss,netid,smacaddress, keyid = lastlgportinfo[obj]

                    # remove host learn
                    cmds.extend(_remove_host_flow(netid,macaddrss,ipaddress,self._parent.inroutermac))

                    # remove arp proxy
                    for m in callAPI(self, 'arpresponder', 'removeproxyarp', {'connection': connection,
                                            'arpentries': [(ipaddress,macaddrss,keyid,False)]}):
                        yield m


            for m in self.execute_commands(connection, cmds):
                yield m

            del  cmds[:]
            for obj in currentrouterinfo:
                if obj not in lastrouterinfo or currentrouterinfo[obj] != lastrouterinfo[obj]:
                    # this means should add flows
                    link_routes = []
                    add_routes = []

                    static_routes,interfaces = currentrouterinfo[obj]

                    for mac,ipaddress,cidr,isexternal,netid, phyportid in interfaces:

                        network, prefix = parse_ip4_network(cidr)
                        link_routes.append((network, prefix, netid))

                        # add router mac + ipaddress ---->>> l3input
                        cmds.extend(_createinputflow(mac, ipaddress, netid))

                        # add arp reply flow ---->>>  l3input
                        cmds.extend(_createarpreplyflow(mac, ipaddress, netid))

                        # inner host will add arp request auto reply on phyport,
                        # other broadcast arp request will impact innter host mac
                        # so drop it here
                        cmds.extend(_createfilterarprequestflow(netid))

                        if mac != self._parent.inroutermac:
                            cmds.extend(_createinputflow(self._parent.inroutermac,ipaddress,netid))
                            cmds.extend(_createarpreplyflow(self._parent.inroutermac,ipaddress,netid))

                    # add router flow into l3router table
                    for _, _, _, _, netid,_ in interfaces:
                        cmds.extend(_createrouterflow(link_routes, netid))


            for obj in currentlgportinfo:
                if obj not in lastlgportinfo or currentlgportinfo[obj] != lastlgportinfo[obj]:
                    ipaddress,macaddrss,netid,smacaddress,keyid = currentlgportinfo[obj]

                    #add arp proxy in physicalport
                    for m in callAPI(self, 'arpresponder', 'createproxyarp', {'connection': connection,
                                            'arpentries': [(ipaddress,macaddrss,keyid,False)]}):
                        yield m

                    #add host learn
                    cmds.extend(_add_host_flow(netid,macaddrss,ipaddress,self._parent.inroutermac))


            for m in self.execute_commands(connection, cmds):
                yield m

        except Exception:
            self._logger.warning("router update flow exception, ignore it! continue", exc_info=True)

@defaultconfig
@depend(arpresponder.ARPResponder,icmpresponder.ICMPResponder)
class L3Router(FlowBase):
    _tablerequest = (
        ("l3router", ("l3input",), "router"),
        ("l3output", ("l3router",), "l3"),
        ("l2output", ("l3output",), "")
    )

    _default_inroutermac = '1a:23:67:59:63:33'
    _default_outroutermacmask = '0a:00:00:00:00:00'
    _default_arp_cycle_time = 5

    # if arp entry have no reply ,  it will send in arp cycle until timeout
    # but if new packet request arp ,, it will flush this timeout in arp entry
    _default_arp_incomplete_timeout = 60

    _default_arp_complete_timeout = 30

    _default_buffer_packet_timeout = 30


    def __init__(self, server):
        super(L3Router, self).__init__(server)
        self.app_routine = RoutineContainer(self.scheduler)
        self.app_routine.main = self._main
        self.routines.append(self.app_routine)
        self._flowupdater = dict()

    def _main(self):
        flowinit = FlowInitialize.createMatcher(_ismatch=lambda x: self.vhostbind is None or
                                                                   x.vhost in self.vhostbind)

        conndown = OpenflowConnectionStateEvent.createMatcher(state=OpenflowConnectionStateEvent.CONNECTION_DOWN,
                                                              _ismatch=lambda x: self.vhostbind is None
                                                                                 or x.createby.vhost in self.vhostbind)

        while True:
            yield (flowinit, conndown)

            if self.app_routine.matcher is flowinit:
                c = self.app_routine.event.connection
                self.app_routine.subroutine(self._init_conn(c))
            if self.app_routine.matcher is conndown:
                c = self.app_routine.event.connection
                self.app_routine.subroutine(self._uninit_conn(c))

    def _init_conn(self, conn):

        if conn in self._flowupdater:
            updater = self._flowupdater.pop(conn)
            updater.close()

        updater = RouterUpdater(conn, self)

        self._flowupdater[conn] = updater
        updater.start()

        ofdef = conn.openflowdef
        l3router = self._gettableindex("l3router", conn.protocol.vhost)
        l3output = self._gettableindex("l3output", conn.protocol.vhost)

        l3router_default_flow = ofdef.ofp_flow_mod(
            table_id=l3router,
            command = ofdef.OFPFC_ADD,
            priority=0,
            buffer_id = ofdef.OFP_NO_BUFFER,
            match = ofdef.ofp_match_oxm(),
            instructions=[
                ofdef.ofp_instruction_actions(
                    type=ofdef.OFPIT_CLEAR_ACTIONS
                )
            ]
        )
        
        # as default mis flow,  max_len = mis_send_len
        # ofdef.OFPCML_NO_BUFFER is invaild
        l3output_default_flow = ofdef.ofp_flow_mod(
            table_id=l3output,
            command = ofdef.OFPFC_ADD,
            priority=0,
            buffer_id = ofdef.OFP_NO_BUFFER,
            match = ofdef.ofp_match_oxm(),
            instructions=[
                ofdef.ofp_instruction_actions(
                    actions=[
                        ofdef.ofp_action_output(
                            port=ofdef.OFPP_CONTROLLER,
                            max_len=ofdef.OFPCML_NO_BUFFER
                        )
                    ]
                )
            ]
        )

        for m in conn.protocol.batch([l3router_default_flow, l3output_default_flow],conn,self.app_routine):
            yield m

    def _uninit_conn(self, conn):

        if conn in self._flowupdater:
            updater = self._flowupdater.pop(conn)
            updater.close()

        if False:
            yield

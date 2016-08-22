import itertools

import time

import vlcp.service.sdn.ioprocessing as iop

from vlcp.event import RoutineContainer
from vlcp.protocol.openflow import OpenflowAsyncMessageEvent
from vlcp.protocol.openflow import OpenflowConnectionStateEvent
from vlcp.service.sdn.flowbase import FlowBase
from vlcp.service.sdn.ofpmanager import FlowInitialize
from vlcp.utils.ethernet import mac_addr_bytes, ip4_addr_bytes, ip4_addr, arp_packet_l4, mac_addr, ethernet_l4, \
    ethernet_l7
from vlcp.utils.flowupdater import FlowUpdater
from vlcp.utils.netutils import parse_ip4_network, ip_in_network, get_netmask
from vlcp.utils.networkmodel import VRouter, RouterPort, SubNet


class RouterUpdater(FlowUpdater):
    def __init__(self, connection, parent):
        super(RouterUpdater, self).__init__(connection, (), ("routerupdater", connection), parent._logger)
        self._parent = parent
        self._lastlogicalport = dict()
        self._lastlogicalnet = dict()

        self._lastrouterinfo = dict()
        self._original_keys = ()

        self._packet_buffer = dict()

    def main(self):
        try:
            self.subroutine(self._update_handler(), True, "updater_handler")
            self.subroutine(self._router_packetin_handler(), True, "router_packetin_handler")
            self.subroutine(self._time_cycle_handler(),True,"time_cycle_handler")

            for m in FlowUpdater.main(self):
                yield m
        finally:

            if hasattr(self, "updater_handler"):
                self.updater_handler.close()

            if hasattr(self, "router_packetin_handler"):
                self.router_packetin_handler.close()

            if hasattr(self,"time_cycle_handler"):
                self.time_cycle_handler.close()

    def _time_cycle_handler(self):

        while True:
            
            for m in self.waitWithTimeout(self._parent.flush_cycle_time):
                yield m
            ct = time.time()

            for _, v in self._packet_buffer.items():
                for i, (_, t) in enumerate(v):
                    if ct - t >= self._parent.flush_cycle_time:
                        del v[i]
            
            

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

        def _send_buffer_packet_out(netid,macaddress,ipaddress,srcmacaddress,packet):

            for m in self.execute_commands(conn,
                        [
                            ofdef.ofp_packet_out(
                                buffer_id = ofdef.OFP_NO_BUFFER,
                                in_port = ofdef.OFPP_CONTROLLER,
                                actions = [
                                    ofdef.ofp_action_set_field(
                                        field = ofdef.create_oxm(ofdef.NXM_NX_REG5,netid)
                                    ),
                                    ofdef.ofp_action_set_field(
                                        field = ofdef.create_oxm(ofdef.OXM_OF_ETH_SRC,srcmacaddress)
                                    ),
                                    ofdef.ofp_action_set_field(
                                        field = ofdef.create_oxm(ofdef.OXM_OF_ETH_DST,macaddress)
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
                                priority=ofdef.OFP_DEFAULT_PRIORITY,
                                buffer_id=ofdef.OFP_NO_BUFFER,
                                idle_timeout = self._parent.host_learn_timeout,
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
            yield (packetin_matcher, arpreply_matcher)

            msg = self.event.message
            try:
                if self.matcher is packetin_matcher:
                    outnetworkid = ofdef.uint32.create(ofdef.get_oxm(msg.match.oxm_fields, ofdef.NXM_NX_REG5))

                    findFlag = False
                    outsrcmacaddress = None
                    interface_ipaddress = None
                    
                    for routerinfo in self._lastrouterinfo.values():
                        for _, macaddress, ipaddress, _, _, netid in routerinfo:

                            if outnetworkid == netid:
                                outsrcmacaddress = macaddress
                                interface_ipaddress = ipaddress
                                findFlag = True

                    if findFlag:
                        # store msg in buffer
                        ippacket = ethernet_l4.create(msg.data)

                        self._packet_buffer.setdefault((outnetworkid,ippacket.ip_dst),[]).append(
                            (ippacket,int(time.time()))
                        )

                        # send ARP request to get dst_ip mac
                        arp_request_packet = arp_packet_l4(
                            dl_src = mac_addr(outsrcmacaddress),
                            dl_dst = mac_addr("FF:FF:FF:FF:FF:FF"),
                            arp_op=ofdef.ARPOP_REQUEST,
                            arp_sha=mac_addr(outsrcmacaddress),
                            arp_spa=ip4_addr(interface_ipaddress),
                            arp_tpa=ippacket.ip_dst
                        )

                        self.subroutine(_send_broadcast_packet_out(outnetworkid,arp_request_packet))
                    else:
                        # drop packet
                        # when packetin don't send all to controller ,, buffer is used for many times
                        # packet out drop it right now
                        pass
                else:
                    netid = ofdef.uint32.create(ofdef.get_oxm(msg.match.oxm_fields,ofdef.NXM_NX_REG5))

                    arp_reply_packet = ethernet_l7.create(msg.data)
                    
                    reply_ipaddress = arp_reply_packet.arp_spa
                    reply_macaddress = arp_reply_packet.arp_sha

                    dst_macaddress = arp_reply_packet.dl_dst

                    # search msg buffer ,, packet out msg there wait this arp reply
                    if (netid,reply_ipaddress) in self._packet_buffer:
                        for packet, t in self._packet_buffer[(netid,reply_ipaddress)]:
                            self.subroutine(_send_buffer_packet_out(netid,reply_macaddress,reply_ipaddress,dst_macaddress,packet))

                    # add flow about this host in l3output
                    self.subroutine(_add_host_flow(netid,reply_macaddress,reply_ipaddress,dst_macaddress))

            except Exception:
                self._logger.warning(" handler router packetin message error , ignore !")

    def _update_handler(self):

        dataobjectchange = iop.DataObjectChanged.createMatcher(None, None, self._connection)

        while True:
            yield (dataobjectchange,)

            self._lastlogicalport, _, self._lastlogicalnet, _ = self.event.current

            self._update_walk()

    def _update_walk(self):

        logicalportkeys = [p.getkey() for p, _ in self._lastlogicalport]
        logicalnetkeys = [n.getkey() for n, _ in self._lastlogicalnet]

        self._initialkeys = logicalportkeys + logicalnetkeys
        self._original_keys = logicalportkeys + logicalnetkeys

        self._walkerdict = dict(itertools.chain(((p, self._walk_lgport) for p in logicalportkeys),
                                                ((n, self._walk_lgnet) for n in logicalnetkeys)))

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
    
    def reset_initialkeys(self,keys,values):
        
        subnetkeys = [k for k,v in zip(keys,values) if v.isinstance(SubNet)]
        routerportkeys = [k for k,v in zip(keys,values) if v.isinstance(RouterPort)]
        routerkeys = [k for k,v in zip(keys,values) if v.isinstance(VRouter)]

        self._initialkeys = tuple(itertools.chain(self._original_keys,subnetkeys,
                                    routerportkeys,routerkeys))

    def updateflow(self, connection, addvalues, removevalues, updatedvalues):

        try:
            lastrouterinfo = self._lastrouterinfo

            allobjects = set(o for o in self._savedresult if o is not None and not o.isdeleted())

            currentlognetinfo = dict((n, id) for n, id in self._lastlogicalnet if n in allobjects)

            currentsubnetinfo = dict((s, currentlognetinfo[s.network]) for s in allobjects
                                     if s.isinstance(SubNet) and s.network in currentlognetinfo)

            currentrouterportinfo = dict((r, r.subnet) for r in allobjects
                                         if r.isinstance(RouterPort)
                                         )
            # currentrouter = set(n for n in allobjects if n.isinstance(VRouter))

            currentrouterinfo = dict((r, ([(r.routes, self._parent.inroutermac,
                                            getattr(interface, "ip_address", interface.subnet.gateway),
                                            interface.subnet.cidr,
                                            getattr(interface.subnet, "external", False),
                                            currentsubnetinfo[interface.subnet])
                                           for interface in currentrouterportinfo
                                           if interface.router.getkey() == r.getkey() and
                                           hasattr(interface, "subnet") and (hasattr(interface, "ip_address")
                                                                             or hasattr(interface.subnet, "gateway"))
                                           and interface.subnet in currentsubnetinfo
                                           ])
                                      ) for r in allobjects if r.isinstance(VRouter)
                                     )

            self._lastrouterinfo = currentrouterinfo

            ofdef = connection.openflowdef
            vhost = connection.protocol.vhost
            l3input = self._parent._gettableindex("l3input", vhost)
            l3router = self._parent._gettableindex("l3router", vhost)
            l3output = self._parent._gettableindex("l3output", vhost)
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
                                #ofdef.create_oxm(ofdef.OXM_OF_IPV4_DST, ip4_addr_bytes(ipaddress))
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
                                                 ip4_addr_bytes(ip4_addr.formatter(cidr)),
                                                 ip4_addr_bytes(ip4_addr.formatter(get_netmask(prefix))))
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

            for obj in removevalues:
                #
                # remove an router ,,  delete flows ,,
                #
                # remove router must be remove interface first,
                # remove interface will update router info , add / remove flow
                # when remove router ,, it must be update and remove flow
                #
                # for xx in routerinfo[obj] will never run ....
                if obj in lastrouterinfo:
                    for routes, mac, ipaddress, cidr, isexternal, netid in lastrouterinfo[obj]:
                        # delete router mac + ipaddress ---->>> l3input
                        cmds.extend(_deleteinputflow(mac, ipaddress, netid))

                        # delete arp reply flow from l3input
                        cmds.extend(_deletearpreplyflow(mac, ipaddress, netid))

                        # delete router flow from l3router table
                        cmds.extend(_deleterouterflow(netid))

            for obj in updatedvalues:
                if obj in lastrouterinfo and (obj not in currentrouterinfo or
                                                      lastrouterinfo[obj] != currentrouterinfo[obj]):
                    #  update obj in lastinfo ,,  when recreate current info it maybe filter
                    #  so maybe in last not in current
                    for routes, mac, ipaddress, cidr, isexternal, netid in lastrouterinfo[obj]:
                        cmds.extend(_deleteinputflow(mac, ipaddress, netid))
                        cmds.extend(_deletearpreplyflow(mac,ipaddress,netid))
                        cmds.extend(_deleterouterflow(netid))
                    
            for m in self.execute_commands(connection, cmds):
                yield m

            del cmds[:]
            for obj in addvalues:
                if obj in currentrouterinfo and obj not in lastrouterinfo:
                    link_routes = []
                    static_routes = []
                    add_routes = []

                    for routes, mac, ipaddress, cidr, isexternal, netid in currentrouterinfo[obj]:

                        # every interface have same routes in routerinfo
                        static_routes = routes

                        network, prefix = parse_ip4_network(cidr)
                        link_routes.append((network, prefix, netid))

                        if isexternal:
                            network, prefix = parse_ip4_network("0.0.0.0/0")
                            link_routes.append((network, prefix, netid))

                        # add router mac + ipaddress ---->>> l3input
                        cmds.extend(_createinputflow(mac, ipaddress, netid))

                        # add arp reply flow ---->>>  l3input
                        cmds.extend(_createarpreplyflow(mac, ipaddress, netid))

                    for network, prefix, netid in link_routes:
                        add_routes.append((network, prefix, netid))
                        for cidr, nethop in static_routes:

                            if ip_in_network(nethop, network, prefix):
                                c, f = parse_ip4_network(cidr)
                                add_routes.append((c, f, netid))

                    # add router flow into l3router table
                    for _, _, _, _, _, netid in currentrouterinfo[obj]:
                        cmds.extend(_createrouterflow(add_routes, netid))

            for obj in updatedvalues:
                if obj in currentrouterinfo and (obj not in lastrouterinfo or
                                                         currentrouterinfo[obj] != lastrouterinfo):
                    link_routes = []
                    static_routes = []
                    add_routes = []

                    for routes, mac, ipaddress, cidr, isexternal, netid in currentrouterinfo[obj]:

                        # every interface have same routes in routerinfo
                        static_routes = routes

                        network, prefix = parse_ip4_network(cidr)
                        link_routes.append((network, prefix, netid))

                        if isexternal:
                            network, prefix = parse_ip4_network("0.0.0.0/0")
                            link_routes.append((network, prefix, netid))

                        # add router mac + ipaddress ---->>> l3input
                        cmds.extend(_createinputflow(mac, ipaddress, netid))

                        # add arp reply flow ---->>>  l3input
                        cmds.extend(_createarpreplyflow(mac, ipaddress, netid))

                    for network, prefix, netid in link_routes:
                        add_routes.append((network, prefix, netid))
                        for cidr, nethop in static_routes:

                            if ip_in_network(nethop, network, prefix):
                                c, f = parse_ip4_network(cidr)
                                add_routes.append((c, f, netid))

                    # add router flow into l3router table
                    for _, _, _, _, _, netid in currentrouterinfo[obj]:
                        cmds.extend(_createrouterflow(add_routes, netid))

            for m in self.execute_commands(connection, cmds):
                yield m



        except Exception:
            self._logger.warning("router update flow exception, ignore it! continue", exc_info=True)


class L3Router(FlowBase):
    _tablerequest = (
        ("l3router", ("l3input",), "router"),
        ("l3output", ("l3router",), "l3"),
        ("l2output", ("l3output",), "")
    )

    _default_inroutermac = '1a:23:67:59:63:33'
    _default_flush_cycle_time = 10
    _default_host_learn_timeout = 30

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
                                                                                 or x.vhost in self.vhostbind)

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

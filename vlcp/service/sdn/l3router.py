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
from vlcp.service.kvdb import objectdb
from vlcp.service.sdn.flowbase import FlowBase
from vlcp.service.sdn.ofpmanager import FlowInitialize
from vlcp.service.sdn import arpresponder
from vlcp.service.sdn import icmpresponder
from vlcp.utils.dataobject import set_new, WeakReferenceObject
from vlcp.utils.ethernet import mac_addr_bytes, ip4_addr_bytes, ip4_addr, arp_packet_l4, mac_addr, ethernet_l4, \
    ethernet_l7
from vlcp.utils.flowupdater import FlowUpdater
from vlcp.utils.netutils import parse_ip4_network,get_netmask, parse_ip4_address, ip_in_network
from vlcp.utils.networkmodel import VRouter, RouterPort, SubNet, SubNetMap,DVRouterForwardInfo, \
    DVRouterForwardSet, DVRouterForwardInfoRef, DVRouterExternalAddressInfo, LogicalNetworkMap, LogicalNetwork, \
    LogicalPort


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
        self._lastphynet = dict()

        self._lastrouterinfo = dict()
        self._lastsubnetinfo = dict()
        self._lastlgportinfo = dict()
        self._lastexternallgportinfo = dict()
        self._lastrouterstoreinterfacenetinfo = dict()
        self._lastnetworkrouterinfo = dict()
        self._lastnetworkroutertableinfo = dict()
        self._lastnetworkstaticroutesinfo = dict()
        self._laststaticroutes = dict()
        self._lastallrouterinfo = dict()
        self._laststoreinfo = dict()
        self._lastnetworkforwardinfo = dict()
        self._lastdvrforwardinfo = dict()

        self._original_keys = ()

        self._packet_buffer = dict()
        self._arp_cache = dict()

    def main(self):
        try:
            self.subroutine(self._update_handler(), True, "updater_handler")
            self.subroutine(self._router_packetin_handler(), True, "router_packetin_handler")
            self.subroutine(self._arp_cache_handler(),True,"arp_cache_handler")
            self.subroutine(self._time_cycle_handler(),True,"time_cycle_handler")
            if self._parent.enable_router_forward:
                self.subroutine(self._keep_forwardinfo_alive_handler(),True,"keep_forwardinfo_alive_handler")
                self.subroutine(self._keep_addressinfo_alive_handler(),True,"keep_addressinfo_alive_handler")
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

            if self._parent.enable_router_forward:
                if hasattr(self,"keep_forwardinfo_alive_handler"):
                    self.keep_forwardinfo_alive_handler.close()

                if hasattr(self,"keep_addressinfo_alive_handler"):
                    self.keep_addressinfo_alive_handler.close()

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

    def _getinterfaceinfobynetid(self,netid):

        for r,v in self._lastallrouterinfo.items():
            for e in v:
                _,isexternal,gateway,_,outmac,external_ip,nid,phyport,_,_ = e
                if nid == netid:
                    if isexternal:
                        return outmac, external_ip, phyport
                    else:
                        return outmac, gateway, phyport

    def _getallinterfaceinfobynetid(self,netid):
        router = None
        ret_info = []
        for r,v in self._lastallrouterinfo.items():
            for e in v:
                if e[6] == netid:
                    router = r
                    break

        if router:
            v = self._lastallrouterinfo[router]
            for e in v:
                ret_info.append((e[4],e[6]))

        return  ret_info

    def _keep_forwardinfo_alive_handler(self):
        while True:
            for m in self.waitWithTimeout(self._parent.forwardinfo_discover_update_time):
                yield m

            datapath_id = self._connection.openflow_datapathid
            vhost = self._connection.protocol.vhost
            try:
                for m in callAPI(self, 'ovsdbmanager', 'waitbridgeinfo', {'datapathid': datapath_id,
                                                                          'vhost': vhost}):
                    yield m
            except Exception:
                self._logger.warning("OVSDB bridge is not ready", exc_info=True)
                return
            else:
                bridge, system_id, _ = self.retvalue

            forward_keys = [DVRouterForwardInfo.default_key(k[0],k[1]) for k in self._laststoreinfo.keys()]
            ref_forward_keys = [DVRouterForwardInfoRef.default_key(k[0],k[1]) for k in self._laststoreinfo.keys()]
            transact_keys = [DVRouterForwardSet.default_key()] + forward_keys + ref_forward_keys

            def updater(keys,values,timestamp):
                retdict = {}
                for i in range((len(transact_keys) - 1) // 2):
                    if values[i + 1]:
                        values[i + 1].info = [e for e in values[i + 1].info
                                              if (e[0], e[1], e[2]) != (system_id, bridge, vhost)
                                              and e[4] > timestamp]
                        indices = DVRouterForwardInfo._getIndices(keys[i + 1])[1]


                        e = (system_id,bridge,vhost,list(self._laststoreinfo[(indices[0],indices[1])])[0],
                                 timestamp + self._parent.forwardinfo_discover_update_time * 2 * 1000000)
                        values[i + 1].info.append(e)

                        values[i + 1].info = sorted(values[i + 1].info,key=lambda x: x[3])

                        if values[i + 1].info:
                            retdict[keys[i + 1]] = values[i + 1]
                            refe = [e[3] for e in values[i + 1].info]
                            if values[i + 1 + (len(transact_keys) - 1) // 2].info != refe:
                                values[i + 1 + (len(transact_keys) - 1) // 2].info = refe
                                retdict[keys[i + 1 + (len(transact_keys) - 1) // 2]] = \
                                    values[i + 1 + (len(transact_keys) - 1) // 2]

                        # else:
                        #     # there is no info in this struct , drop it from db
                        #     retdict[keys[i + 1]] = None
                        #     retdict[keys[i + 1 + (len(transact_keys) - 1) // 2]] = None
                        #     if WeakReferenceObject(keys[i + 1 + (len(transact_keys) - 1) // 2]) in \
                        #             values[0].set.dataset():
                        #         values[0].set.dataset().discard(
                        #             WeakReferenceObject(keys[i + 1 + (len(transact_keys) - 1) // 2]))
                        #         retdict[keys[0]] = values[0]

                return retdict.keys(), retdict.values()

            if forward_keys + ref_forward_keys:
                for m in callAPI(self,"objectdb","transact",{"keys":transact_keys,"updater":updater,"withtime":True}):
                    yield m

    def _keep_addressinfo_alive_handler(self):
        while True:
            for m in self.waitWithTimeout(self._parent.addressinfo_discover_update_time):
                yield m

            datapath_id = self._connection.openflow_datapathid
            vhost = self._connection.protocol.vhost
            try:
                for m in callAPI(self, 'ovsdbmanager', 'waitbridgeinfo', {'datapathid': datapath_id,
                                                                          'vhost': vhost}):
                    yield m
            except Exception:
                self._logger.warning("OVSDB bridge is not ready", exc_info=True)
                return
            else:
                bridge, system_id, _ = self.retvalue

            for k,v in self._lastsubnetinfo.items():
                if v[1] and v[7]:
                    allocated_ip_address = v[5]
                    subnetmapkey = SubNetMap.default_key(k.id)
                    DVRouterExternalAddressInfokey = DVRouterExternalAddressInfo.default_key()

                    def updater(keys,values,timestamp):

                        newlist = []
                        for e in values[0].info:
                            # remove self , add new timestamp after
                            if (e[0],e[1],e[2],e[3]) == (system_id,bridge,vhost,values[1].id):
                                x = (system_id, bridge, vhost, values[1].id, allocated_ip_address,
                                     timestamp + self._parent.addressinfo_discover_update_time * 2 * 1000000)
                                newlist.append(x)
                            elif e[5] < timestamp and e[3] == values[1].id:
                                ipaddress = parse_ip4_address(e[4])
                                if str(ipaddress) in values[1].allocated_ips:
                                    del values[1].allocated_ips[str(ipaddress)]
                            else:
                                newlist.append(e)

                        values[0].info = newlist

                        return keys,values

                    for m in callAPI(self,"objectdb","transact",
                                     {"keys":[DVRouterExternalAddressInfokey,subnetmapkey],
                                      "updater":updater,"withtime":True}):
                        yield m

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
                status,timeout,isstatic,realmac,cidr = v

                if isstatic:
                    if status == 1:
                        outnetid, request_ip = k
                        ofdef = self._connection.openflowdef

                        info = self._getinterfaceinfobynetid(outnetid)

                        if info:
                            mac, ipaddress, phyport = info
                            if phyport:
                                arp_request_packet = arp_packet_l4(
                                    dl_src=mac_addr(mac),
                                    dl_dst=mac_addr("FF:FF:FF:FF:FF:FF"),
                                    arp_op=ofdef.ARPOP_REQUEST,
                                    arp_sha=mac_addr(mac),
                                    arp_spa=ip4_addr(ipaddress),
                                    arp_tpa=request_ip
                                )
                                for m in self._packet_out_message(outnetid, arp_request_packet, phyport):
                                    yield m
                    if status == 2:
                        if ct > timeout:
                            if ct - timeout >= self._parent.static_host_arp_refresh_interval:
                                realmac = mac_addr("FF:FF:FF:FF:FF:FF")

                            outnetid, request_ip = k
                            ofdef = self._connection.openflowdef

                            info = self._getinterfaceinfobynetid(outnetid)

                            if info:
                                mac, ipaddress, phyport = info
                                if phyport:
                                    arp_request_packet = arp_packet_l4(
                                        dl_src=mac_addr(mac),
                                        dl_dst=realmac,
                                        arp_op=ofdef.ARPOP_REQUEST,
                                        arp_sha=mac_addr(mac),
                                        arp_spa=ip4_addr(ipaddress),
                                        arp_tpa=request_ip
                                    )
                                    for m in self._packet_out_message(outnetid, arp_request_packet, phyport):
                                        yield m
                else:
                    if status == 1:
                        if ct > timeout:
                            self._arp_cache.pop(k)
                            if k in self._packet_buffer:
                                del self._packet_buffer[k]
                        else:
                            # packet out an arp request
                            outnetid,request_ip= k
                            ofdef = self._connection.openflowdef

                            info = self._getinterfaceinfobynetid(outnetid)

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

                        info = self._getinterfaceinfobynetid(outnetid)

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

            # isstatic : this type arp entry will add when arp request static router and gateway
            # cidr : when isstatic arp entry reply , use cidr to add flows into sw
            isstatic = self.event.isstatic
            cidr = self.event.cidr

            if (netid,ipaddress) not in self._arp_cache:
                entry = (1,ct + arp_incomplete_timeout,isstatic,"",cidr)
                self._arp_cache[(netid,ipaddress)] = entry

                ofdef = self._connection.openflowdef
                info = self._getinterfaceinfobynetid(netid)
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
                s,_,isstatic,mac,cidr = self._arp_cache[(netid,ipaddress)]
                # this arp request have in cache , update timeout
                entry = (s, ct + arp_incomplete_timeout, isstatic,mac,cidr)
                self._arp_cache[(netid,ipaddress)] = entry

    def _router_packetin_handler(self):
        conn = self._connection
        ofdef = self._connection.openflowdef

        l3output = self._parent._gettableindex("l3output", self._connection.protocol.vhost)
        l3input = self._parent._gettableindex("l3input", self._connection.protocol.vhost)
        l2output = self._parent._gettableindex("l2output", self._connection.protocol.vhost)
        l3router = self._parent._gettableindex("l3router", self._connection.protocol.vhost)

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

        def _add_static_routes_flow(from_net_id,cidr,to_net_id,smac,dmac):
            network,prefix = parse_ip4_network(cidr)

            for m in self.execute_commands(conn,[
                ofdef.ofp_flow_mod(
                    table_id=l3router,
                    command=ofdef.OFPFC_ADD,
                    priority=ofdef.OFP_DEFAULT_PRIORITY + prefix,
                    buffer_id=ofdef.OFP_NO_BUFFER,
                    out_port=ofdef.OFPP_ANY,
                    out_group=ofdef.OFPG_ANY,
                    match=ofdef.ofp_match_oxm(
                        oxm_fields=[
                            ofdef.create_oxm(ofdef.NXM_NX_REG4, from_net_id),
                            ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE, ofdef.ETHERTYPE_IP),
                            ofdef.create_oxm(ofdef.OXM_OF_IPV4_DST_W,
                                             network,
                                             get_netmask(prefix))
                        ]
                    ),
                    instructions=[
                        ofdef.ofp_instruction_actions(
                            actions=[
                                ofdef.ofp_action_set_field(
                                    field=ofdef.create_oxm(ofdef.NXM_NX_REG5, to_net_id)
                                ),
                                ofdef.ofp_action_set_field(
                                    field=ofdef.create_oxm(ofdef.OXM_OF_ETH_SRC, smac)
                                ),
                                ofdef.ofp_action_set_field(
                                    field=ofdef.create_oxm(ofdef.OXM_OF_ETH_DST, dmac)
                                ),
                                ofdef.ofp_action(
                                    type=ofdef.OFPAT_DEC_NW_TTL
                                )
                            ]
                        ),
                        ofdef.ofp_instruction_goto_table(table_id=l2output)
                    ]
                )
            ]):
                yield m

        def _add_static_host_flow(ipaddress, dmac, netid, smac):

            for m in self.execute_commands(conn, [
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
                            ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE, ofdef.ETHERTYPE_IP),
                            ofdef.create_oxm(ofdef.OXM_OF_IPV4_DST, ip4_addr(ipaddress))
                        ]
                    ),
                    instructions=[
                        ofdef.ofp_instruction_actions(
                            actions=[
                                ofdef.ofp_action_set_field(
                                    field=ofdef.create_oxm(ofdef.OXM_OF_ETH_SRC, smac)
                                ),
                                ofdef.ofp_action_set_field(
                                    field=ofdef.create_oxm(ofdef.OXM_OF_ETH_DST, dmac)
                                ),
                                ofdef.ofp_action(
                                    type=ofdef.OFPAT_DEC_NW_TTL
                                )
                            ]
                        ),
                        ofdef.ofp_instruction_goto_table(table_id=l2output)
                    ]
                )
            ]):
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
                        status,_,_,mac,_ = self._arp_cache[(outnetworkid,ippacket.ip_dst)]

                        # this mac is real mac
                        if status == 2:
                            info = self._getinterfaceinfobynetid(outnetworkid)
                            if info:
                                smac,ip,_= info
                                self.subroutine(_send_buffer_packet_out(outnetworkid,mac,ip,mac_addr(smac),
                                                                        ippacket,msg.buffer_id))
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
                                    logicalnetworkid=outnetworkid,isstatic=False,
                                    cidr=ip4_addr.formatter(ippacket.ip_dst))

                    self.subroutine(self.waitForSend(e))

                if self.matcher is arpflow_request_matcher:
                    outnetworkid = ofdef.uint32.create(ofdef.get_oxm(msg.match.oxm_fields, ofdef.NXM_NX_REG5))
                    #ipaddress = ofdef.get_oxm(msg.match.oxm_fields,ofdef.OXM_OF_IPV4_DST)

                    ippacket = ethernet_l4.create(msg.data)
                    ipaddress = ippacket.ip_dst
                    ct = time.time()

                    if(outnetworkid,ipaddress) in self._arp_cache:

                        status,timeout,isstatic,mac,cidr = self._arp_cache[(outnetworkid,ipaddress)]

                        if status == 2:
                            # we change this arp entry status in cache ,, next cycle will send arp request
                            entry = (3,timeout,isstatic,mac,cidr)
                            self._arp_cache[(outnetworkid,ipaddress)] = entry

                if self.matcher is arpflow_remove_matcher:
                    nid = ofdef.uint32.create(ofdef.get_oxm(msg.match.oxm_fields, ofdef.NXM_NX_REG5))
                    ip_address = ip4_addr(ip4_addr_bytes.formatter(
                                    ofdef.get_oxm(msg.match.oxm_fields, ofdef.OXM_OF_IPV4_DST)))

                    if(nid,ip_address) in self._arp_cache:
                        _, _, isstatic, _, _ = self._arp_cache[(nid,ip_address)]

                        # never delete static arp entry ..
                        if not isstatic:
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
                        status, timeout, isstatic,_,cidr = self._arp_cache[(netid,reply_ipaddress)]
                        ct = time.time()
                        if isstatic:
                            entry = (2,ct + self._parent.static_host_arp_refresh_interval,
                                    isstatic,reply_macaddress,cidr)
                            self._arp_cache[(netid,reply_ipaddress)] = entry

                            # add static routes in l3router
                            for smac,nid in self._getallinterfaceinfobynetid(netid):
                                self.subroutine(_add_static_routes_flow(nid,cidr,netid,mac_addr(smac),reply_macaddress))

                                if netid == nid:
                                    self.subroutine(_add_static_host_flow(ip4_addr.formatter(reply_ipaddress),
                                                                      reply_macaddress,nid,mac_addr(smac)))
                        else:
                            # this is the first arp reply
                            if status == 1 or status == 3:
                                # complete timeout ,,, after flow hard_timeout, packet will send to controller too
                                # if packet in this timeout ,  will send an unicast arp request
                                # is best  1*self._parent.arp_complete_timeout < t < 2*self._parent.arp_complete_timeout
                                self._arp_cache[(netid,reply_ipaddress)] = (2,
                                            ct + self._parent.arp_complete_timeout + 20,False,reply_macaddress,cidr)

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

            self._lastlogicalport, self._lastphyport, self._lastlogicalnet, self._lastphynet = self.event.current

            self._update_walk()

            self.updateobjects((p for p,_ in self._lastlogicalport))

    def _update_walk(self):

        logicalportkeys = [p.getkey() for p, _ in self._lastlogicalport]
        logicalnetkeys = [n.getkey() for n, _ in self._lastlogicalnet]
        phyportkeys = [p.getkey() for p,_ in self._lastphyport]
        phynetkeys = [n.getkey() for n,_ in self._lastphynet]
        dvrforwardinfokeys = [DVRouterForwardSet.default_key()]

        self._initialkeys = logicalportkeys + logicalnetkeys + phyportkeys + phyportkeys + dvrforwardinfokeys
        self._original_keys = logicalportkeys + logicalnetkeys + phyportkeys + phyportkeys + dvrforwardinfokeys

        self._walkerdict = dict(itertools.chain(((p, self._walk_lgport) for p in logicalportkeys),
                                                ((n, self._walk_lgnet) for n in logicalnetkeys),
                                                ((n, self._walk_phynet) for n in phynetkeys),
                                                ((f, self._walk_dvrforwardinfo) for f in dvrforwardinfokeys),
                                                ((p, self._walk_phyport) for p in phyportkeys)))

        self.subroutine(self.restart_walk(), False)

    def _walk_dvrforwardinfo(self,key,value,walk,save):
        save(key)
        for weakref in value.set.dataset():
            try:
                weakobj = walk(weakref.getkey())
            except KeyError:
                pass
            else:
                save(weakobj.getkey())

    def _walk_lgport(self, key, value, walk, save):

        if value is None:
            return
        save(key)


    def _walk_lgnet(self, key, value, walk, save):

        if value is None:
            return

        save(key)
        lgnetmapkey = LogicalNetworkMap.default_key(LogicalNetwork._getIndices(key)[1][0])

        try:
            lgnetmap = walk(lgnetmapkey)
        except KeyError:
            pass
        else:
            save(lgnetmap.getkey())

            if self._parent.prepush:
                for lgport_weak in lgnetmap.ports.dataset():
                    try:
                        lgport = walk(lgport_weak.getkey())
                    except KeyError:
                        pass
                    else:
                        save(lgport.getkey())

            for subnet_weak in lgnetmap.subnets.dataset():
                try:
                    subnetobj = walk(subnet_weak.getkey())
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


    def _walk_phyport(self, key, value, walk, save):

        if value is None:
            return

        save(key)

    def _walk_phynet(self,key,value,walk,save):
        if value is None:
            return
        save(key)

    def reset_initialkeys(self,keys,values):

        subnetkeys = [k for k,v in zip(keys,values) if v is not None and not v.isdeleted() and
                      v.isinstance(SubNet)]
        routerportkeys = [k for k,v in zip(keys,values) if v is not None and not v.isdeleted() and
                          v.isinstance(RouterPort)]
        routerkeys = [k for k,v in zip(keys,values) if v is not None and not v.isdeleted() and
                      v.isinstance(VRouter)]
        forwardinfokeys = [k for k,v in zip(keys,values) if v is not None and not v.isdeleted() and
                           v.isinstance(DVRouterForwardInfoRef)]

        self._initialkeys = tuple(itertools.chain(self._original_keys,subnetkeys,
                                    routerportkeys,routerkeys,forwardinfokeys))

    def updateflow(self, connection, addvalues, removevalues, updatedvalues):

        try:
            datapath_id = connection.openflow_datapathid
            ofdef = connection.openflowdef
            vhost = connection.protocol.vhost

            lastsubnetinfo = self._lastsubnetinfo
            lastlgportinfo = self._lastlgportinfo
            lastrouterstoreinterfaceinfo = self._lastrouterstoreinterfacenetinfo
            lastnetworkrouterinfo = self._lastnetworkrouterinfo
            lastnetworkroutertableinfo = self._lastnetworkroutertableinfo
            lastnetworkstaticroutesinfo= self._lastnetworkstaticroutesinfo
            laststaticroutes= self._laststaticroutes
            laststoreinfo = self._laststoreinfo
            lastnetworkforwardinfo = self._lastnetworkforwardinfo
            lastexternallgportinfo = self._lastexternallgportinfo

            allobjects = set(o for o in self._savedresult if o is not None and not o.isdeleted())

            dvrforwardinfo = dict(((f.from_pynet,f.to_pynet),f.info) for f in allobjects
                                  if f.isinstance(DVRouterForwardInfoRef))

            self._lastdvrforwardinfo = dvrforwardinfo

            currentphynetinfo = dict((n,n.id) for n,_ in self._lastphynet if n in allobjects)

            # phyport : phynet = 1:1, so we use phynet as key
            currentphyportinfo = dict((p.physicalnetwork, (p,id)) for p, id in self._lastphyport if p in allobjects
                                      and p.physicalnetwork in currentphynetinfo)

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

            currentlgportinfo = dict((p,(p.ip_address,p.mac_address,currentlognetinfo[p.network][0],p.network.id))
                                        for p,id in self._lastlogicalport if p in allobjects
                                        and hasattr(p,"ip_address")
                                        and hasattr(p,"mac_address")
                                        and p.network in currentlognetinfo)

            currentexternallgportinfo = dict((p,(p.ip_address,p.mac_address,currentlognetinfo[p.network][0],
                                                 currentlognetinfo[p.network][1]))
                                             for p in allobjects if p.isinstance(LogicalPort)
                                             and hasattr(p,"ip_address")
                                             and hasattr(p,"mac_address")
                                             and p.network in currentlognetinfo
                                             and p not in currentlgportinfo)

            self._lastlgportinfo = currentlgportinfo
            self._lastexternallgportinfo = currentexternallgportinfo

            subnet_to_routerport = dict((p.subnet,p) for p in allobjects if p.isinstance(RouterPort))
            router_to_routerport = dict((p.router,p) for p in allobjects if p.isinstance(RouterPort))
            routerport_to_subnet = dict((p, p.subnet) for p in allobjects if p.isinstance(RouterPort))
            routerport_to_router = dict((p,p.router) for p in allobjects if p.isinstance(RouterPort))

            staticroutes = dict((r,r.routes) for r in allobjects if r.isinstance(VRouter)
                                       and r in router_to_routerport)

            currentsubnetinfo = dict((s,(
                                        s.cidr,
                                        getattr(s,"isexternal",False),
                                        getattr(s,"gateway",None),
                                        self._parent.inroutermac,
                                        currentlognetinfo[s.network][1],    # outroutermac
                                        None,                               # external_ip_address
                                        currentlognetinfo[s.network][0],    # network id
                                        currentlognetinfo[s.network][2],    # physical port no
                                        s.id,                               # subnet id
                                        s.network                           # logical network
                                        )) for s in allobjects if s.isinstance(SubNet)
                                                and s.network in currentlognetinfo
                                                and s in subnet_to_routerport)

            try:
                for m in callAPI(self, 'ovsdbmanager', 'waitbridgeinfo', {'datapathid': datapath_id,
                                                                          'vhost': vhost}):
                    yield m


            except Exception:
                self._logger.warning("OVSDB bridge is not ready", exc_info=True)
                return
            else:
                bridge, system_id, _ = self.retvalue

            if self._parent.enable_router_forward:
                update_external_subnet = dict()
                for k, v in currentsubnetinfo.items():
                    if v[1] and v[7]:
                        # this subnet is external , we should allocate ip from cidr
                        if k in lastsubnetinfo and lastsubnetinfo[k][1] and lastsubnetinfo[k][5]:
                            #this subnet have allocated ip in last
                            allocated_ip_address = lastsubnetinfo[k][5]

                        else:
                            subnetkey = SubNet.default_key(k.id)
                            subnetmapkey = SubNetMap.default_key(k.id)
                            DVRouterExternalAddressInfokey = DVRouterExternalAddressInfo.default_key()
                            allocated_ip_address = [None]

                            def allocate_ip(keys,values,timestamp):
                                start = parse_ip4_address(values[1].allocated_start)
                                end = parse_ip4_address(values[1].allocated_end)

                                #values[0].info = [e for e in values[0].info if e[5] > timestamp]

                                # filter timeout info,
                                # only discard info has some subnet id , so we can release ip address to subnet
                                for e in list(values[0].info):
                                    if e[5] < timestamp and values[2].id == e[3]:
                                        ipaddress = parse_ip4_address(e[4])
                                        if str(ipaddress) in values[2].allocated_ips:
                                            del values[2].allocated_ips[str(ipaddress)]

                                        values[0].info.remove(e)

                                if (system_id,bridge,vhost,values[1].id) in [(e[0],e[1],e[2],e[3]) for e in values[0].info]:
                                    for e in list(values[0].info):
                                        if (e[0],e[1],e[2],e[3]) == (system_id,bridge,vhost,values[1].id):
                                            allocated_ip_address[0] = e[4]
                                            values[0].info.remove(e)

                                    values[0].info.append((system_id,bridge,vhost,values[1].id,allocated_ip_address[0],
                                                timestamp + self._parent.addressinfo_discover_update_time * 2  * 1000000))

                                else:
                                    for ipaddress in range(start,end):
                                        if str(ipaddress) not in values[2].allocated_ips:
                                            allocated_ip_address[0] = ip4_addr.formatter(ipaddress)
                                            values[2].allocated_ips[str(ipaddress)] = (system_id,bridge,vhost)
                                            break
                                    else:
                                        raise ValueError("allocate external subnet ipaddress error!")

                                    values[0].info.append((system_id,bridge,vhost,values[1].id,allocated_ip_address[0],
                                                timestamp + self._parent.addressinfo_discover_update_time * 2 * 1000000))

                                return tuple([keys[0],keys[2]]),tuple([values[0],values[2]])

                            for m in callAPI(self,"objectdb","transact",
                                             {"keys":[DVRouterExternalAddressInfokey,subnetkey, subnetmapkey],
                                              "updater":allocate_ip,"withtime":True}):
                                yield m

                            allocated_ip_address = allocated_ip_address[0]

                        nv = list(v)
                        nv[5] = allocated_ip_address
                        update_external_subnet[k] = tuple(nv)

                currentsubnetinfo.update(update_external_subnet)

                for k,v in lastsubnetinfo.items():
                    if v[1] and v[7]:
                        if k not in currentsubnetinfo or (k in currentsubnetinfo
                                                          and not currentsubnetinfo[k][1]
                                                          ):
                            # this external subnet off line , release ip address to subnet
                            allocated_ip_address = v[5]
                            subnetmapkey = SubNetMap.default_key(k.id)
                            DVRouterExternalAddressInfokey = DVRouterExternalAddressInfo.default_key()

                            def release_ip(keys,values,timestamp):

                                # ipaddress = parse_ip4_address(allocated_ip_address)
                                #
                                # if str(ipaddress) in values[0].allocated_ips:
                                #     del values[0].allocated_ips[str(ipaddress)]
                                #
                                # values[0].info = [e for e in values[0].info if e[5] > timestamp and
                                #                   (e[0],e[1],e[2],e[3]) !=(system_id,bridge,vhost,values[1].id)]
                                new_list = []
                                for e in values[0].info:
                                    if (e[0],e[1],e[2],e[3]) == (system_id,bridge,vhost,values[1].id):
                                        ipaddress = parse_ip4_address(allocated_ip_address)

                                        if str(ipaddress) in values[1].allocated_ips:
                                            del values[1].allocated_ips[str(ipaddress)]
                                    elif e[5] < timestamp and e[3] == values[1].id:
                                        ipaddress = parse_ip4_address(e[4])

                                        if str(ipaddress) in values[1].allocated_ips:
                                            del values[1].allocated_ips[str(ipaddress)]
                                    else:
                                        new_list.append(e)

                                values[0].info = new_list

                                return keys,values

                            for m in callAPI(self,"objectdb","transact",
                                             {"keys":[DVRouterExternalAddressInfokey,subnetmapkey],
                                              "updater":release_ip,"withtime":True}):
                                yield m

            self._lastsubnetinfo = currentsubnetinfo

            allrouterinterfaceinfo = dict()
            for router in staticroutes.keys():
                for k,v in routerport_to_router.items():
                    if v == router:
                        s = routerport_to_subnet[k]
                        interface = currentsubnetinfo[s]
                        if hasattr(k,"ip_address"):
                            interface = list(interface)
                            interface[2] = k.ip_address
                            interface = tuple(interface)

                        allrouterinterfaceinfo.setdefault(router,[]).append(interface)

            #self._lastallrouterinfo = allrouterinterfaceinfo

            router_to_phynet = dict()
            router_to_no_phynet = dict()
            for k, v in allrouterinterfaceinfo.items():
                for e in v:
                    if e[7] and e[9].physicalnetwork:
                        #
                        # if router interface have same physicalnetwork, physicalport,
                        # it must be have same outmac
                        #
                        router_to_phynet.setdefault(k, set()).add((e[9].physicalnetwork, e[4],e[9],e[0],e[1],e[6]))
                    else:
                        router_to_no_phynet.setdefault(k,set()).add((e[9].physicalnetwork, e[4],e[9],e[0],e[1],e[6]))

            currentnetworkforwardinfo = dict()
            for k,v in router_to_no_phynet.items():
                if k in router_to_phynet:
                    for x in v:
                        for e in router_to_phynet[k]:
                            if (e[0].id,x[0].id) in dvrforwardinfo:
                                if dvrforwardinfo[(e[0].id,x[0].id)]:
                                    if x[4]:
                                        currentnetworkforwardinfo.setdefault(e[2],set()).\
                                            add((e[5],x[5],dvrforwardinfo[(e[0].id,x[0].id)][0],"0.0.0.0/0"))

                                    currentnetworkforwardinfo.setdefault(e[2],set()).\
                                            add((e[5],x[5],dvrforwardinfo[(e[0].id,x[0].id)][0],x[3]))

            self._lastnetworkforwardinfo = currentnetworkforwardinfo

            if self._parent.enable_router_forward:
                # enable router forward , we should update router forward capacity to db

                currentstoreinfo = dict()
                for k,v in router_to_phynet.items():
                    for e in v:
                        for x in v:
                            if x[0].id != e[0].id:
                                currentstoreinfo.setdefault((e[0].id,x[0].id),set()).add(e[1])
                                currentstoreinfo.setdefault((x[0].id, e[0].id), set()).add(x[1])

                self._laststoreinfo = currentstoreinfo

                add_store_info = dict()
                remove_store_info = dict()

                for k in laststoreinfo.keys():
                    if k not in currentstoreinfo or (k in currentstoreinfo and currentstoreinfo[k] != laststoreinfo[k]):
                        remove_store_info[k] = laststoreinfo[k]

                for k in currentstoreinfo.keys():
                    if k not in laststoreinfo or (k in laststoreinfo and laststoreinfo[k] != currentstoreinfo[k]):
                        add_store_info[k] = currentstoreinfo[k]

                if add_store_info or remove_store_info:

                    forward_keys = list(set([DVRouterForwardInfo.default_key(k[0],k[1]) for k in
                                             list(add_store_info.keys())+ list(remove_store_info.keys())]))

                    ref_forward_keys = [DVRouterForwardInfoRef.default_key(DVRouterForwardInfo._getIndices(k)[1][0],
                                                    DVRouterForwardInfo._getIndices(k)[1][1]) for k in forward_keys]

                    transact_keys = [DVRouterForwardSet.default_key()] + forward_keys + ref_forward_keys
                    try:
                        for m in callAPI(self, 'ovsdbmanager', 'waitbridgeinfo', {'datapathid': datapath_id,
                                                                                  'vhost': vhost}):
                            yield m
                    except Exception:
                        self._logger.warning("OVSDB bridge is not ready", exc_info=True)
                        return
                    else:
                        bridge, system_id, _ = self.retvalue


                    def store_transact(keys,values,timestamp):

                        transact_object = dict()
                        #transact_object[keys[0]] = values[0]

                        for i in range((len(transact_keys) - 1) //2):
                            if values[i + 1] is None:
                                # means this phy-> phy info is first
                                indices = DVRouterForwardInfo._getIndices(keys[i + 1])[1]

                                if (indices[0],indices[1]) in add_store_info:
                                    obj = DVRouterForwardInfo.create_from_key(keys[i + 1])
                                    e = (system_id,bridge,vhost,list(add_store_info[(indices[0],indices[1])])[0],
                                         timestamp + self._parent.forwardinfo_discover_update_time * 2 * 1000000)
                                    obj.info.append(e)
                                    values[i + 1] = set_new(values[i + 1], obj)
                                    transact_object[keys[i + 1]] = values[i + 1]

                                    refobj = DVRouterForwardInfoRef.create_from_key(keys[i + 1 +
                                                                                         (len(transact_keys) - 1)//2])
                                    refobj.info.append(e[3])
                                    values[i + 1 + (len(transact_keys) - 1)//2] = set_new(
                                        values[i + 1 + (len(transact_keys) - 1)//2],refobj)

                                    transact_object[keys[i + 1 + (len(transact_keys) - 1) // 2]] = \
                                        values[i + 1 + (len(transact_keys) - 1)//2]

                                    values[0].set.dataset().add(refobj.create_weakreference())
                                    transact_object[keys[0]] = values[0]
                            else:

                                # DVRouterForwardInfo and DVRouterForwardRefinfo is existed
                                # and it must be in DVRouterForwardInfoSet
                                # checkout timeout
                                values[i+1].info = [e for e in values[i+1].info
                                                    if (e[0],e[1],e[2]) != (system_id,bridge,vhost)
                                                    and e[4] > timestamp]

                                indices = DVRouterForwardInfo._getIndices(keys[i + 1])[1]

                                if (indices[0],indices[1]) in add_store_info:
                                    e = (system_id,bridge,vhost,list(add_store_info[(indices[0],indices[1])])[0],
                                         timestamp + self._parent.forwardinfo_discover_update_time * 2 * 1000000)
                                    values[i+1].info.append(e)

                                values[i+1].info = sorted(values[i+1].info,key=lambda x: x[3])

                                if values[i+1].info:
                                    transact_object[keys[i+1]] = values[i+1]
                                    refe = [e[3] for e in values[i + 1].info]
                                    if values[i + 1 + (len(transact_keys) - 1)//2].info != refe:
                                        values[i + 1 + (len(transact_keys) - 1)//2].info = refe
                                        transact_object[keys[i + 1 + (len(transact_keys) - 1)//2]] = \
                                            values[i + 1 + (len(transact_keys) - 1)//2]
                                else:
                                    transact_object[keys[i+1]] = None
                                    transact_object[keys[i + 1 + (len(transact_keys) - 1) // 2]] = None
                                    if WeakReferenceObject(keys[i + 1 + (len(transact_keys) - 1) // 2]) in \
                                            values[0].set.dataset():
                                        values[0].set.dataset().discard(
                                            WeakReferenceObject(keys[i + 1 + (len(transact_keys) - 1) // 2]))
                                        transact_object[keys[0]] = values[0]


                        return transact_object.keys(),transact_object.values()

                    for m in callAPI(self,"objectdb","transact",
                                     {"keys":transact_keys,"updater":store_transact,"withtime":True}):
                        yield m

            currentnetworkrouterinfo = dict()
            network_to_router = dict()

            for k,v in allrouterinterfaceinfo.items():
                for e in v:
                    #isexternal,gateway,inmac,outmac,external_ip,networkid
                    entry = (e[1],e[2],e[3],e[4],e[5],e[6])
                    currentnetworkrouterinfo[e[9]] = entry
                    network_to_router[e[9]] = k

            self._lastnetworkrouterinfo = currentnetworkrouterinfo

            currentnetworkroutertableinfo = dict()
            for network in currentnetworkrouterinfo.keys():
                for e in allrouterinterfaceinfo[network_to_router[network]]:
                    if e[9] != network:
                        entry = (currentnetworkrouterinfo[network][5],e[0],e[6])
                        currentnetworkroutertableinfo.setdefault(network,set()).add(entry)

            self._lastnetworkroutertableinfo = currentnetworkroutertableinfo

            currentstaticroutes = dict()
            for r,routes in staticroutes.items():
                # for e in routes:
                #     prefix,nexthop = e
                #     for v in allrouterinterfaceinfo[r]:
                #         cidr = v[0]
                #         network,mask = parse_ip4_network(cidr)
                #         if ip_in_network(parse_ip4_address(nexthop),network,mask):
                #             currentstaticroutes.setdefault(r,set()).add((prefix,nexthop,v[6]))
                #
                #         # external interface , add default router to static routes
                #         if v[1]:
                #             currentstaticroutes.setdefault(r,set()).add(("0.0.0.0/0",v[5],v[6]))

                for v in allrouterinterfaceinfo[r]:
                    cidr = v[0]
                    for e in routes:
                        prefix,nexthop = e
                        network,mask = parse_ip4_network(cidr)
                        if ip_in_network(parse_ip4_address(nexthop),network,mask):
                            currentstaticroutes.setdefault(r,set()).add((prefix,nexthop,v[6]))
                    if v[1] and v[7]:
                        currentstaticroutes.setdefault(r, set()).add(("0.0.0.0/0", v[2], v[6]))

            self._laststaticroutes = currentstaticroutes

            currentnetworkstaticroutesinfo = dict()
            for network in currentnetworkrouterinfo.keys():
                if network_to_router[network] in currentstaticroutes:
                    for route in currentstaticroutes[network_to_router[network]]:
                        currentnetworkstaticroutesinfo.setdefault(network,set()).\
                            add((currentnetworkrouterinfo[network][5],route[0],route[1],route[2]))

            self._lastnetworkstaticroutesinfo = currentnetworkstaticroutesinfo

            # add_transact_router_store = dict()
            # remove_transact_router_store = dict()
            # for o in lastrouterstoreinterfaceinfo:
            #     if o not in currentrouterstoreinterfaceinfo:
            #         remove_transact_router_store[o] = lastrouterstoreinterfaceinfo[o]
            #
            # for o in currentrouterstoreinterfaceinfo:
            #     if o not in lastrouterstoreinterfaceinfo or (o in lastrouterstoreinterfaceinfo
            #                         and lastrouterstoreinterfaceinfo[o] != currentrouterstoreinterfaceinfo[o]):
            #         add_transact_router_store[o] = currentrouterstoreinterfaceinfo[o]
            #
            # transact_dvrouter_info_keys = [DVRouterInfo.default_key(r.id)
            #                                for r in list(add_transact_router_store.keys())+
            #                                list(remove_transact_router_store.keys())]
            #
            # if transact_dvrouter_info_keys:
            #     try:
            #         for m in callAPI(self, 'ovsdbmanager', 'waitbridgeinfo', {'datapathid': datapath_id,
            #                                                                   'vhost': vhost}):
            #             yield m
            #     except Exception:
            #         self._logger.warning("OVSDB bridge is not ready", exc_info=True)
            #         return
            #     else:
            #         bridge, system_id, _ = self.retvalue
            #
            #     def transact_store__dvr_info(keys,values,timestamp):
            #         for i in range(0,len(transact_dvrouter_info_keys)):
            #             v = [e for e in values[i].dvrinfo
            #                     if (e[0],e[1],[2]) != (system_id,vhost,bridge) and e[8] > timestamp]
            #             values[i].dvrinfo = v
            #
            #             if keys[i] in [DVRouterInfo.default_key(r.id) for r in list(add_transact_router_store.keys())]:
            #                 k = DVRouterInfo._getIndices(keys[i])[1][0]
            #                 info = add_transact_router_store[ReferenceObject(VRouter.default_key(k))]
            #                 for x in info:
            #                     e = (system_id,vhost,bridge,x[0],x[1],x[2],x[3],x[4],
            #                         timestamp + 5 * 1000000)
            #                     values[i].dvrinfo.append(e)
            #
            #         return keys,values
            #
            #
            #     for m in callAPI(self,"objectdb","transact",
            #                      {"keys":transact_dvrouter_info_keys,"updater":transact_store__dvr_info,
            #                       "withtime":True}):
            #         yield m

            cmds = []

            l3input = self._parent._gettableindex("l3input", vhost)
            l3router = self._parent._gettableindex("l3router", vhost)
            l3output = self._parent._gettableindex("l3output", vhost)
            l2output = self._parent._gettableindex("l2output", vhost)
            arpreply = self._parent._gettableindex("arp",vhost)

            if connection.protocol.disablenxext:
                def match_network(nid):
                    return ofdef.create_oxm(ofdef.OXM_OF_METADATA_W, (nid & 0xffff) << 32,
                                            b'\x00\x00\xff\xff\x00\x00\x00\x00')
            else:
                def match_network(nid):
                    return ofdef.create_oxm(ofdef.NXM_NX_REG4, nid)

            def _createinputflow(mac,netid):
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
                                    ofdef.create_oxm(ofdef.OXM_OF_ETH_DST, mac_addr_bytes(mac)),
                                    ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE, ofdef.ETHERTYPE_IP)
                                ]
                            ),
                            instructions=[
                                ofdef.ofp_instruction_goto_table(table_id=l3router)
                            ]
                        )
                ]

            def _deleteinputflow(mac,netid):
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
                                    ofdef.create_oxm(ofdef.OXM_OF_ETH_DST, mac_addr_bytes(mac)),
                                    ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE, ofdef.ETHERTYPE_IP)
                                ]
                            )
                        )
                ]

            def _createarpreplyflow(ipaddress,mac,netid):
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
                                    ofdef.create_oxm(ofdef.OXM_OF_ETH_DST, mac_addr_bytes(mac)),
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

            def _deletearpreplyflow(ipaddress,mac,netid):
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
                                ofdef.create_oxm(ofdef.OXM_OF_ETH_DST, mac_addr_bytes(mac)),
                                ofdef.create_oxm(ofdef.OXM_OF_ARP_OP, ofdef.ARPOP_REPLY),
                                ofdef.create_oxm(ofdef.OXM_OF_ARP_TPA, ip4_addr(ipaddress))
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
                                ofdef.create_oxm(ofdef.NXM_NX_REG4, netid),
                                ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE, ofdef.ETHERTYPE_ARP),
                                ofdef.create_oxm(ofdef.OXM_OF_ARP_OP, ofdef.ARPOP_REQUEST),
                                ofdef.create_oxm(ofdef.OXM_OF_ETH_DST_W, b'\x01\x00\x00\x00\x00\x00',
                                                 b'\x01\x00\x00\x00\x00\x00')
                            ]
                        ),
                        instructions=[ofdef.ofp_instruction_actions(type=ofdef.OFPIT_CLEAR_ACTIONS)]
                    )
                ]

            def _deletefilterarprequestflow(netid):
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
                                ofdef.create_oxm(ofdef.NXM_NX_REG4, netid),
                                ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE, ofdef.ETHERTYPE_ARP),
                                ofdef.create_oxm(ofdef.OXM_OF_ARP_OP, ofdef.ARPOP_REQUEST),
                                ofdef.create_oxm(ofdef.OXM_OF_ETH_DST_W, b'\x01\x00\x00\x00\x00\x00',
                                                 b'\x01\x00\x00\x00\x00\x00')
                            ]
                        )
                    )
                ]

            def _add_router_route(fnetid,cidr,tnetid):

                network, prefix = parse_ip4_network(cidr)

                return [
                    ofdef.ofp_flow_mod(
                        table_id=l3router,
                        command=ofdef.OFPFC_ADD,
                        priority=ofdef.OFP_DEFAULT_PRIORITY + prefix,
                        buffer_id=ofdef.OFP_NO_BUFFER,
                        out_port=ofdef.OFPP_ANY,
                        out_group=ofdef.OFPG_ANY,
                        match=ofdef.ofp_match_oxm(
                            oxm_fields=[
                                match_network(fnetid),
                                ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE, ofdef.ETHERTYPE_IP),
                                ofdef.create_oxm(ofdef.OXM_OF_IPV4_DST_W,
                                                 network,
                                                 get_netmask(prefix))
                            ]
                        ),
                        instructions=[
                            ofdef.ofp_instruction_actions(
                                actions=[
                                    ofdef.ofp_action_set_field(
                                        field=ofdef.create_oxm(ofdef.NXM_NX_REG5, tnetid)
                                    )
                                ]
                            ),
                            ofdef.ofp_instruction_goto_table(table_id=l3output)
                        ]
                    )
                ]

            def _delete_router_route(fnetid,cidr,tnetid):
                network, prefix = parse_ip4_network(cidr)
                return [
                    ofdef.ofp_flow_mod(
                        table_id=l3router,
                        command=ofdef.OFPFC_DELETE_STRICT,
                        priority=ofdef.OFP_DEFAULT_PRIORITY + prefix,
                        buffer_id=ofdef.OFP_NO_BUFFER,
                        out_port=ofdef.OFPP_ANY,
                        out_group=ofdef.OFPG_ANY,
                        match=ofdef.ofp_match_oxm(
                            oxm_fields=[
                                    match_network(fnetid),
                                    ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE,ofdef.ETHERTYPE_IP),
                                    ofdef.create_oxm(ofdef.OXM_OF_IPV4_DST_W, network,get_netmask(prefix))
                                        ]
                            )
                    )
                ]

            def _add_host_flow(ipaddress,dmac,netid,smac):
                return [
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
                                        field=ofdef.create_oxm(ofdef.OXM_OF_ETH_SRC,mac_addr(smac))
                                        ),
                                    ofdef.ofp_action_set_field(
                                        field=ofdef.create_oxm(ofdef.OXM_OF_ETH_DST,mac_addr(dmac))
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

            def _remove_host_flow(ipaddress,dmac,netid,smac):
                return [
                    ofdef.ofp_flow_mod(
                        table_id=l3output,
                        command=ofdef.OFPFC_DELETE,
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
                            )
                    )
                ]

            def _add_static_routes_flow(from_net_id,cidr,to_net_id,smac,dmac):
                network, prefix = parse_ip4_network(cidr)
                return [
                    ofdef.ofp_flow_mod(
                        table_id=l3router,
                        command=ofdef.OFPFC_ADD,
                        priority=ofdef.OFP_DEFAULT_PRIORITY + prefix,
                        buffer_id=ofdef.OFP_NO_BUFFER,
                        out_port=ofdef.OFPP_ANY,
                        out_group=ofdef.OFPG_ANY,
                        match=ofdef.ofp_match_oxm(
                            oxm_fields=[
                                ofdef.create_oxm(ofdef.NXM_NX_REG4, from_net_id),
                                ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE, ofdef.ETHERTYPE_IP),
                                ofdef.create_oxm(ofdef.OXM_OF_IPV4_DST_W,
                                                 network,
                                                 get_netmask(prefix))
                            ]
                        ),
                        instructions=[
                            ofdef.ofp_instruction_actions(
                                actions=[
                                    ofdef.ofp_action_set_field(
                                        field=ofdef.create_oxm(ofdef.NXM_NX_REG5, to_net_id)
                                    ),
                                    ofdef.ofp_action_set_field(
                                        field=ofdef.create_oxm(ofdef.OXM_OF_ETH_SRC, smac)
                                    ),
                                    ofdef.ofp_action_set_field(
                                        field=ofdef.create_oxm(ofdef.OXM_OF_ETH_DST, dmac)
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

            def _add_forward_route(from_net_id,cidr,outmac):
                network,prefix = parse_ip4_network(cidr)
                if network:
                    priority = ofdef.OFP_DEFAULT_PRIORITY + 33 + prefix
                else:
                    # add default forward route , keep priority as small
                    priority = ofdef.OFP_DEFAULT_PRIORITY + prefix

                return [
                    ofdef.ofp_flow_mod(
                        table_id = l3router,
                        command = ofdef.OFPFC_ADD,
                        priority = priority,
                        buffer_id = ofdef.OFP_NO_BUFFER,
                        out_port = ofdef.OFPP_ANY,
                        out_group = ofdef.OFPG_ANY,
                        match = ofdef.ofp_match_oxm(
                            oxm_fields = [
                                match_network(from_net_id),
                                ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE,ofdef.ETHERTYPE_IP),
                                ofdef.create_oxm(ofdef.OXM_OF_IPV4_DST_W,network,get_netmask(prefix))
                            ]
                        ),
                        instructions = [
                            ofdef.ofp_instruction_actions(
                                actions=[
                                    ofdef.ofp_action_set_field(
                                        field=ofdef.create_oxm(ofdef.OXM_OF_ETH_DST, mac_addr(outmac))
                                    )
                                ]
                            ),
                            ofdef.ofp_instruction_goto_table(table_id=l2output)
                        ]
                    )
                ]
            def _delete_forward_route(from_net_id,cidr,outmac):
                network,prefix = parse_ip4_network(cidr)
                if network:
                    priority = ofdef.OFP_DEFAULT_PRIORITY + 33 + prefix
                else:
                    # add default forward route , keep priority as small
                    priority = ofdef.OFP_DEFAULT_PRIORITY + prefix
                return [
                    ofdef.ofp_flow_mod(
                        table_id = l3router,
                        command = ofdef.OFPFC_DELETE_STRICT,
                        priority = priority,
                        buffer_id = ofdef.OFP_NO_BUFFER,
                        out_port = ofdef.OFPP_ANY,
                        out_group = ofdef.OFPG_ANY,
                        match = ofdef.ofp_match_oxm(
                            oxm_fields = [
                                match_network(from_net_id),
                                ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE,ofdef.ETHERTYPE_IP),
                                ofdef.create_oxm(ofdef.OXM_OF_IPV4_DST_W,network,get_netmask(prefix))
                            ]
                        )
                    )
                ]

            for n in lastnetworkrouterinfo:
                if n not in currentnetworkrouterinfo or (n in currentnetworkrouterinfo
                        and currentnetworkrouterinfo[n] != lastnetworkrouterinfo[n]):
                    isexternal,gateway,inmac,outmac,external_ip,networkid = lastnetworkrouterinfo[n]

                    if not isexternal:
                        # remove flow innermac + ip >>> l3input
                        cmds.extend(_deleteinputflow(inmac,networkid))

                        cmds.extend(_deleteinputflow(outmac,networkid))

                        # remove arp reply flow on outmac >>> l3input
                        cmds.extend(_deletearpreplyflow(gateway,outmac,networkid))

                        # remove arp filter discard broadcast arp request to inner host
                        cmds.extend(_deletefilterarprequestflow(networkid))
                    else:
                        if external_ip:
                            cmds.extend(_deleteinputflow(outmac,networkid))

                            # remove arp reply flow on outmac >>> l3input
                            cmds.extend(_deletearpreplyflow(external_ip,outmac,networkid))

                            # remove arp proxy for external ip
                            for m in callAPI(self, 'arpresponder', 'removeproxyarp', {'connection': connection,
                                                            'arpentries': [(external_ip,outmac,n.id,False)]}):
                                yield m

            for n in lastnetworkroutertableinfo:
                if n not in currentnetworkroutertableinfo:
                    # this router network delete ,  clear router table
                    for from_network_id,cidr,to_network_id in lastnetworkroutertableinfo[n]:
                        cmds.extend(_delete_router_route(from_network_id,cidr,to_network_id))

                elif lastnetworkroutertableinfo[n] != currentnetworkroutertableinfo:

                    last_route_set = lastnetworkroutertableinfo[n]
                    new_route_set = currentnetworkroutertableinfo[n]

                    for from_network_id,cidr,to_network_id in last_route_set.difference(new_route_set):
                        cmds.extend(_delete_router_route(from_network_id,cidr,to_network_id))

            for r in laststaticroutes:
                if r not in currentstaticroutes:
                    for prefix,nexthop,netid in laststaticroutes[r]:
                        # delete arp cache entry
                        if (netid,ip4_addr(nexthop)) in self._arp_cache:
                            del self._arp_cache[(netid,ip4_addr(nexthop))]

                        # delete all network routes from this prefix
                        for mac,nid in self._getallinterfaceinfobynetid(netid):
                            # this static routes mybe in this netwrok, delete it try!
                            cmds.extend(_delete_router_route(nid,prefix,netid))
                            if nid == netid:
                                cmds.extend(_remove_host_flow(nexthop,mac,nid,mac))

                elif laststaticroutes[r] != currentstaticroutes[r]:
                    last_router_routes_set = laststaticroutes[r]
                    new_router_routes_set = currentstaticroutes[r]

                    for prefix,nexthop,netid in last_router_routes_set.difference(new_router_routes_set):
                        if (netid,ip4_addr(nexthop)) in self._arp_cache:
                            del self._arp_cache[(netid,ip4_addr(nexthop))]

                        # delete all network routes from this prefix
                        for mac,nid in self._getallinterfaceinfobynetid(netid):
                            # this static routes mybe in this netwrok, delete it try!
                            cmds.extend(_delete_router_route(nid,prefix,netid))
                            if nid == netid:
                                cmds.extend(_remove_host_flow(nexthop,mac,nid,mac))

            for n in lastnetworkstaticroutesinfo:
                if n not in currentnetworkstaticroutesinfo:
                    for from_network_id,prefix,nexthop,to_network_id in lastnetworkstaticroutesinfo[n]:
                        cmds.extend(_delete_router_route(from_network_id,prefix,to_network_id))

            for p in lastlgportinfo:
                if p not in currentlgportinfo or (p in currentlgportinfo
                                and currentlgportinfo[p] != lastlgportinfo[p]):
                    ipaddress,macaddrss,netid,netkey = lastlgportinfo[p]

                    # remove host learn
                    cmds.extend(_remove_host_flow(ipaddress,macaddrss,netid,self._parent.inroutermac))

                    # remove arp proxy
                    for m in callAPI(self, 'arpresponder', 'removeproxyarp', {'connection': connection,
                                                    'arpentries': [(ipaddress,macaddrss,netkey,False)]}):
                        yield m

            for p in lastexternallgportinfo:
                if p not in currentexternallgportinfo or\
                        (p in currentexternallgportinfo and currentexternallgportinfo[p] != lastexternallgportinfo[p]):
                    ipaddress,macaddrss,netid,outmac = lastexternallgportinfo[p]
                    cmds.extend(_remove_host_flow(ipaddress,macaddrss,netid,outmac))

            for n in lastnetworkforwardinfo:
                if n not in currentnetworkforwardinfo or (n in currentnetworkforwardinfo
                        and currentnetworkforwardinfo[n] != lastnetworkforwardinfo[n]):

                    for x in lastnetworkforwardinfo[n]:
                        from_network_id = x[0]
                        outmac = x[2]
                        cidr = x[3]
                        cmds.extend(_delete_forward_route(from_network_id,cidr,outmac))


            for m in self.execute_commands(connection, cmds):
                yield m

            del cmds[:]
            for n in currentnetworkrouterinfo:
                if n not in lastnetworkrouterinfo or (n in lastnetworkrouterinfo
                        and lastnetworkrouterinfo[n] != currentnetworkrouterinfo[n]):
                    isexternal,gateway,inmac,outmac,external_ip,networkid = currentnetworkrouterinfo[n]

                    if not isexternal:
                        # add flow innermac + ip >>> l3input
                        cmds.extend(_createinputflow(inmac,networkid))

                        cmds.extend(_createinputflow(outmac, networkid))

                        # add arp reply flow on outmac >>> l3input
                        cmds.extend(_createarpreplyflow(gateway,outmac,networkid))

                        # add arp filter discard broadcast arp request to inner host
                        cmds.extend(_createfilterarprequestflow(networkid))
                    else:
                        # external_ip is None means, this external subnet has no phyport
                        if external_ip:
                            # external network, packet will recv from outmac , so add ..
                            cmds.extend(_createinputflow(outmac, networkid))

                            # add arp reply flow on outmac >>> l3input
                            cmds.extend(_createarpreplyflow(external_ip,outmac,networkid))

                            #add arp proxy for external ip
                            for m in callAPI(self, 'arpresponder', 'createproxyarp', {'connection': connection,
                                                            'arpentries': [(external_ip,outmac,n.id,False)]}):
                                yield m

            for n in currentnetworkroutertableinfo:
                if n not in lastnetworkroutertableinfo:
                    # this router network add , add all router table
                    for from_network_id,cidr,to_network_id in currentnetworkroutertableinfo[n]:
                        cmds.extend(_add_router_route(from_network_id,cidr,to_network_id))

                elif currentnetworkroutertableinfo[n] != lastnetworkroutertableinfo[n]:
                    new_route_set = currentnetworkroutertableinfo[n]
                    last_route_set = lastnetworkroutertableinfo[n]

                    for from_network_id,cidr,to_network_id in new_route_set.difference(last_route_set):
                        cmds.extend(_add_router_route(from_network_id,cidr,to_network_id))

            arp_request_event = []

            for n in currentnetworkstaticroutesinfo:
                if n not in lastnetworkstaticroutesinfo:
                    for from_network_id,prefix,nexthop,to_network_id in currentnetworkstaticroutesinfo[n]:

                        if (to_network_id,ip4_addr(nexthop)) in self._arp_cache \
                                and (self._arp_cache[(to_network_id,ip4_addr(nexthop))][0] == 2 or
                                     self._arp_cache[(to_network_id,ip4_addr(nexthop))][0] == 3):
                            _, _, _, mac, _ = self._arp_cache[(to_network_id, ip4_addr(nexthop))]

                            smac, _, _ = self._getinterfaceinfobynetid(to_network_id)

                            cmds.extend(_add_static_routes_flow(from_network_id,prefix,
                                                                to_network_id,mac_addr(smac),mac))

                            if from_network_id == to_network_id:
                                cmds.extend(_add_host_flow(nexthop,mac_addr.formatter(mac),
                                                           from_network_id,smac))

                            # change this arp entry from host to static entry ..
                            entry = (2,time.time() + self._parent.arp_incomplete_timeout,True,mac,prefix)
                            self._arp_cache[(to_network_id,ip4_addr(nexthop))] = entry
                        else:
                            e = ARPRequest(self._connection, ipaddress=ip4_addr(nexthop),
                                           logicalnetworkid=to_network_id, isstatic=True,
                                           cidr=prefix)
                            arp_request_event.append(e)

                elif currentnetworkstaticroutesinfo[n] != lastnetworkstaticroutesinfo[n]:
                    last_router_routes_set = lastnetworkstaticroutesinfo[n]
                    new_router_routes_set = currentnetworkstaticroutesinfo[n]
                    for from_network_id,prefix, nexthop, to_network_id in \
                            new_router_routes_set.difference(last_router_routes_set):

                        if (to_network_id,ip4_addr(nexthop)) in self._arp_cache \
                                and (self._arp_cache[(to_network_id,ip4_addr(nexthop))][0] == 2 or
                                     self._arp_cache[(to_network_id,ip4_addr(nexthop))][0] == 3):
                            _, _, _, mac, _ = self._arp_cache[(to_network_id, ip4_addr(nexthop))]

                            smac, _, _ = self._getinterfaceinfobynetid(to_network_id)

                            cmds.extend(_add_static_routes_flow(from_network_id,prefix,
                                                                to_network_id,mac_addr(smac),mac))

                            if from_network_id == to_network_id:
                                cmds.extend(_add_host_flow(nexthop,mac_addr.formatter(mac),
                                                           from_network_id,smac))

                            # change this arp entry from host to static entry ..
                            entry = (2,time.time() + self._parent.arp_incomplete_timeout,True,mac,prefix)
                            self._arp_cache[(to_network_id,ip4_addr(nexthop))] = entry
                        else:
                            e = ARPRequest(self._connection, ipaddress=ip4_addr(nexthop),
                                           logicalnetworkid=to_network_id, isstatic=True,
                                           cidr=prefix)
                            arp_request_event.append(e)

            for n in currentnetworkforwardinfo:
                if n not in lastnetworkforwardinfo or (n in currentnetworkforwardinfo
                        and currentnetworkforwardinfo[n] != lastnetworkforwardinfo[n]):

                    for x in currentnetworkforwardinfo[n]:
                        from_network_id = x[0]
                        outmac = x[2]
                        cidr = x[3]
                        cmds.extend(_add_forward_route(from_network_id,cidr,outmac))

            for p in currentlgportinfo:
                if p not in lastlgportinfo or (p in lastlgportinfo
                            and currentlgportinfo[p] != lastlgportinfo[p]):
                    ipaddress,macaddrss,netid,netkey = currentlgportinfo[p]

                    #add arp proxy in physicalport
                    for m in callAPI(self, 'arpresponder', 'createproxyarp', {'connection': connection,
                                            'arpentries': [(ipaddress,macaddrss,netkey,False)]}):
                        yield m

                    #add host learn
                    cmds.extend(_add_host_flow(ipaddress,macaddrss,netid,self._parent.inroutermac))

            for p in currentexternallgportinfo:
                if p not in lastexternallgportinfo or\
                        (p in lastexternallgportinfo and currentexternallgportinfo[p] != lastexternallgportinfo[p]):
                    ipaddress,macaddrss,netid,outmac = currentexternallgportinfo[p]
                    cmds.extend(_add_host_flow(ipaddress,macaddrss,netid,outmac))

            if arp_request_event:
                for e in arp_request_event:
                    self.subroutine(self.waitForSend(e))
                del arp_request_event[:]

            # because function '_getinterfaceinfobynetid' use self._lastallrouterinfo above
            # so change to new in last
            self._lastallrouterinfo = allrouterinterfaceinfo

            for m in self.execute_commands(connection, cmds):
                yield m

        except Exception:
            self._logger.warning("router update flow exception, ignore it! continue", exc_info=True)

@defaultconfig
@depend(arpresponder.ARPResponder,icmpresponder.ICMPResponder,objectdb.ObjectDB)
class L3Router(FlowBase):
    """
    L3 connectivities with virtual router.
    """
    _tablerequest = (
        ("l3router", ("l3input",), "router"),
        ("l3output", ("l3router",), "l3"),
        ("l2output", ("l3output",), "")
    )
    # Router responding MAC address for logical ports on this switch
    _default_inroutermac = '1a:23:67:59:63:33'
    # Router responding MAC address mask for outside network. The MAC address
    # is formed with the physical MAC address (NIC MAC address) XORed with this
    # mask
    _default_outroutermacmask = '0a:00:00:00:00:00'
    # Retry ARP requests with this interval when there is no respond
    _default_arp_cycle_time = 5
    # Prepush ARP entries for all the logical ports which are accessible from the router
    _default_prepush = False

    # if arp entry have no reply ,  it will send in arp cycle until timeout
    # but if new packet request arp ,, it will flush this timeout in arp entry
    _default_arp_incomplete_timeout = 60
    # an ARP entry stays in "COMPLETE" state without sending further ARP requests
    # until this time
    _default_arp_complete_timeout = 30
    # The L3 gateway buffers a packet and wait for ARP responds until this time
    _default_buffer_packet_timeout = 30
    # Refresh external IPs (external gateway address) ARP (MAC-IP corresponding)
    _default_static_host_arp_refresh_interval = 60
    # Enable forwarding in this server, so it becomes a forwarding node (also known as a N/S gateway)
    # This should be set together with module.ioprocessing.enable_router_forward
    _default_enable_router_forward = False
    # A forwarding node will acquire a IP address from an external network, and refresh the information
    # to keep the acquire state. This is the refresh interval.
    _default_addressinfo_discover_update_time = 150
    # A forwarding node will acknowledge other nodes that it is ready to forward network traffic from
    # other nodes, this is the fresh interval
    _default_forwardinfo_discover_update_time = 15

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

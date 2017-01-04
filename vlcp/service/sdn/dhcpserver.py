'''
Created on 2016/7/19

:author: hubo
'''



from vlcp.config.config import defaultconfig
from vlcp.server.module import depend, api, callAPI
from vlcp.service.sdn.flowbase import FlowBase
from vlcp.service.sdn.ofpmanager import FlowInitialize
from vlcp.utils.flowupdater import FlowUpdater
from vlcp.utils.networkmodel import PhysicalPort, LogicalPort, LogicalNetwork,\
    LogicalNetworkMap, SubNet
import vlcp.service.kvdb.objectdb as objectdb
import vlcp.service.sdn.ofpportmanager as ofpportmanager
from vlcp.event.runnable import RoutineContainer
from vlcp.protocol.openflow.openflow import OpenflowConnectionStateEvent,\
    OpenflowAsyncMessageEvent, OpenflowErrorResultException
from vlcp.utils.ethernet import ethernet_l2, ip4_addr, mac_addr, mac_addr_bytes,\
    ip4_addr_bytes, ethernet_l7, ip4_packet_l7, ip4_payload, ip4_udp_payload
import vlcp.service.sdn.ioprocessing as iop
import itertools
from vlcp.utils.dataobject import ReferenceObject
from vlcp.service.sdn import arpresponder
import vlcp.utils.dhcp as d
from vlcp.utils.netutils import parse_ip4_network, get_broadcast, get_netmask
import os
from namedstruct.stdprim import uint16

class DHCPUpdater(FlowUpdater):
    def __init__(self, connection, parent):
        FlowUpdater.__init__(self, connection, (), ('DHCPUpdater', connection), parent._logger)
        self._parent = parent
        self._lastlognets = ()
        self._lastlogports = ()
        self._lastlogportinfo = {}
        self._lastlognetinfo = {}
        self._lastserveraddresses = set()
        self._dhcpentries = {}
    def main(self):
        try:
            if self._connection.protocol.disablenxext:
                return
            self.subroutine(self._update_handler(), True, '_update_handler_routine')
            self.subroutine(self._dhcp_handler(), True, '_dhcp_handler_routine')
            for m in FlowUpdater.main(self):
                yield m
        finally:
            if hasattr(self, '_update_handler_routine'):
                self._update_handler_routine.close()
            if hasattr(self, '_dhcp_handler_routine'):
                self._dhcp_handler_routine.close()
    def _dhcp_handler(self):
        conn = self._connection
        ofdef = self._connection.openflowdef
        l3 = self._parent._gettableindex('l3input', self._connection.protocol.vhost)
        dhcp_packet_matcher = OpenflowAsyncMessageEvent.createMatcher(ofdef.OFPT_PACKET_IN, None, None, l3, 1,
                                                              self._connection, self._connection.connmark)
        required_tags = [d.OPTION_MESSAGE_TYPE, d.OPTION_SERVER_IDENTIFIER,
                         d.OPTION_NETMASK, d.OPTION_ROUTER,
                         d.OPTION_DNSSERVER, d.OPTION_BROADCAST, d.OPTION_MTU,
                         d.OPTION_LEASE_TIME, d.OPTION_T1, d.OPTION_T2]
        server_mac = mac_addr(self._parent.servermac)
        trans_id = uint16.create(os.urandom(2))
        def set_options(payload, option_dict, provide_options, message_type, remove_lease = False):
            message_type_opt = d.dhcp_option_message_type(value = message_type)
            if d.OPTION_REQUESTED_OPTIONS in option_dict:
                reqs = set(option_dict[d.OPTION_REQUESTED_OPTIONS].value)
                send_tags = [t for t in option_dict[d.OPTION_REQUESTED_OPTIONS].value
                             if t == d.OPTION_MESSAGE_TYPE or t in provide_options] \
                            + [t for t in required_tags
                               if (t in provide_options or t == d.OPTION_MESSAGE_TYPE) and t not in reqs] \
                            + [t for t in provide_options
                               if t not in reqs and t not in required_tags]
            else:
                send_tags = [t for t in required_tags
                             if t in provide_options or t == d.OPTION_MESSAGE_TYPE] \
                            + [t for t in set(provide_options.keys()).difference(required_tags)]
            d.build_options(payload, [message_type_opt if t == d.OPTION_MESSAGE_TYPE
                                      else provide_options[t] for t in send_tags
                                      if not remove_lease or (t != d.OPTION_LEASE_TIME and t != d.OPTION_T1 and t != OPTION_T2)],
                            max(min(option_dict[d.OPTION_MAX_MESSAGE_SIZE].value, 1400), 576)
                            if d.OPTION_MAX_MESSAGE_SIZE in option_dict
                            else 576)
        def send_packet(pid, packet):
            for m in self.execute_commands(conn,
                            [ofdef.ofp_packet_out(
                                    buffer_id = ofdef.OFP_NO_BUFFER,
                                    in_port = ofdef.OFPP_CONTROLLER,
                                    actions = [ofdef.ofp_action_output(port = pid,
                                                                       max_len = ofdef.OFPCML_NO_BUFFER)],
                                    data = packet._tobytes()
                                )]):
                yield m
        while True:
            yield (dhcp_packet_matcher,)
            msg = self.event.message
            try:
                in_port = ofdef.ofp_port_no.create(ofdef.get_oxm(msg.match.oxm_fields, ofdef.OXM_OF_IN_PORT))
                if in_port not in self._dhcpentries:
                    continue
                port_mac, port_ip, server_ip, provide_options = self._dhcpentries[in_port]
                l7_packet = ethernet_l7.create(msg.data)
                dhcp_packet = d.dhcp_payload.create(l7_packet.data)
                if dhcp_packet.op != d.BOOTREQUEST or \
                        dhcp_packet.hlen != 6 or dhcp_packet.htype != 1 or \
                        dhcp_packet.magic_cookie != d.BOOTP_MAGIC_COOKIE or \
                        dhcp_packet.giaddr != 0:
                    raise ValueError('Unsupported DHCP packet')
                options = d.reassemble_options(dhcp_packet)
                option_dict = dict((o.tag, o) for o in options)
                if d.OPTION_MESSAGE_TYPE not in option_dict:
                    raise ValueError('Message type not found')
                message_type = option_dict[d.OPTION_MESSAGE_TYPE].value
                is_nak = False
                if message_type == d.DHCPDISCOVER:
                    if dhcp_packet.chaddr[:6].ljust(6, b'\x00') != mac_addr.tobytes(port_mac):
                        # Ignore this packet
                        continue
                    dhcp_reply = d.dhcp_payload(op = d.BOOTREPLY,
                                                htype = 1,
                                                hlen = 6,
                                                hops = 0,
                                                xid = dhcp_packet.xid,
                                                secs = 0,
                                                flags = dhcp_packet.flags,
                                                ciaddr = 0,
                                                yiaddr = port_ip,
                                                siaddr = 0,
                                                giaddr = dhcp_packet.giaddr,
                                                chaddr = dhcp_packet.chaddr,
                                                magic_cookie = d.BOOTP_MAGIC_COOKIE
                                                )
                    set_options(dhcp_reply, option_dict, provide_options, d.DHCPOFFER)
                elif message_type == d.DHCPREQUEST:
                    if d.OPTION_SERVER_IDENTIFIER in option_dict and option_dict[d.OPTION_SERVER_IDENTIFIER].value != server_ip:
                        # Ignore packets to wrong address
                        continue
                    if dhcp_packet.chaddr[:6].ljust(6, b'\x00') != mac_addr.tobytes(port_mac) \
                            or (d.OPTION_REQUESTED_IP in option_dict and option_dict[d.OPTION_REQUESTED_IP].value != port_ip) \
                            or (dhcp_packet.ciaddr != 0 and dhcp_packet.ciaddr != port_ip):
                        dhcp_reply = d.dhcp_payload(op = d.BOOTREPLY,
                                                    htype = 1,
                                                    hlen = 6,
                                                    hops = 0,
                                                    xid = dhcp_packet.xid,
                                                    secs = 0,
                                                    flags = dhcp_packet.flags,
                                                    ciaddr = 0,
                                                    yiaddr = 0,
                                                    siaddr = 0,
                                                    giaddr = dhcp_packet.giaddr,
                                                    chaddr = dhcp_packet.chaddr,
                                                    magic_cookie = d.BOOTP_MAGIC_COOKIE
                                                    )
                        d.build_options(dhcp_reply, [d.dhcp_option_message_type(value = d.DHCPNAK),
                                                     d.dhcp_option_address(tag = d.OPTION_SERVER_IDENTIFIER,
                                                                           value = server_ip)], 576, 0)
                        is_nak = True
                    else:
                        dhcp_reply = d.dhcp_payload(op = d.BOOTREPLY,
                                                    htype = 1,
                                                    hlen = 6,
                                                    hops = 0,
                                                    xid = dhcp_packet.xid,
                                                    secs = 0,
                                                    flags = dhcp_packet.flags,
                                                    ciaddr = dhcp_packet.ciaddr,
                                                    yiaddr = port_ip,
                                                    siaddr = 0,
                                                    giaddr = dhcp_packet.giaddr,
                                                    chaddr = dhcp_packet.chaddr,
                                                    magic_cookie = d.BOOTP_MAGIC_COOKIE
                                                    )
                        set_options(dhcp_reply, option_dict, provide_options, d.DHCPACK)
                elif message_type == d.DHCPDECLINE:
                    self._logger.warning('DHCP client reports DHCPDECLINE, there should be some problems.'\
                                         ' Connection = %r(%016x), port = %d.',
                                         self._connection, self._connection.openflow_datapathid)
                elif message_type == d.DHCPRELEASE:
                    # Safe to ignore
                    continue
                elif message_type == d.DHCPINFORM:
                    dhcp_reply = d.dhcp_payload(op = d.BOOTREPLY,
                                                htype = 1,
                                                hlen = 6,
                                                hops = 0,
                                                xid = dhcp_packet.xid,
                                                secs = 0,
                                                flags = dhcp_packet.flags,
                                                ciaddr = dhcp_packet.ciaddr,
                                                yiaddr = 0,
                                                siaddr = 0,
                                                giaddr = dhcp_packet.giaddr,
                                                chaddr = dhcp_packet.chaddr,
                                                magic_cookie = d.BOOTP_MAGIC_COOKIE
                                                )
                    set_options(dhcp_reply, option_dict, provide_options, d.DHCPACK, True)
                trans_id = (trans_id + 1) & 0xffff
                if (dhcp_packet.flags & d.DHCPFLAG_BROADCAST) or is_nak:
                    dl_dst = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
                    ip_dst = 0xffffffff
                else:
                    dl_dst = l7_packet.dl_src
                    ip_dst = port_ip
                reply_packet = ip4_packet_l7((ip4_payload, ip4_udp_payload),
                                           dl_src = server_mac,
                                           dl_dst = dl_dst,
                                           identifier = trans_id,
                                           ttl = 128,
                                           ip_src = server_ip,
                                           ip_dst = ip_dst,
                                           sport = 67,
                                           dport = 68,
                                           data = dhcp_reply._tobytes()
                                           )
                self.subroutine(send_packet(in_port, reply_packet), True)
            except Exception:
                self._logger.info('Invalid DHCP packet received: %r', msg.data, exc_info = True)
            
    def _update_handler(self):
        dataobjectchanged = iop.DataObjectChanged.createMatcher(None, None, self._connection)
        while True:
            yield (dataobjectchanged,)
            self._lastlogports, _, self._lastlognets, _ = self.event.current
            self._update_walk()
    def _walk_logport(self, key, value, walk, save):
        if value is not None:
            save(key)
            if hasattr(value, 'subnet'):
                try:
                    _ = walk(value.subnet.getkey())
                except KeyError:
                    pass
                else:
                    save(value.subnet.getkey())
    def _walk_lognet(self, key, value, walk, save):
        save(key)
        if value is None:
            return
    def _update_walk(self):
        logport_keys = [p.getkey() for p,_ in self._lastlogports]
        lognet_keys = [n.getkey() for n,_ in self._lastlognets]
        lognet_mapkeys = [LogicalNetworkMap.default_key(n.id) for n,_ in self._lastlognets]
        self._initialkeys = logport_keys + lognet_keys + lognet_mapkeys
        self._walkerdict = dict(itertools.chain(((n, self._walk_lognet) for n in lognet_keys),
                                                ((p, self._walk_logport) for p in logport_keys)))
        self.subroutine(self.restart_walk(), False)
    def updateflow(self, connection, addvalues, removevalues, updatedvalues):
        try:
            allobjs = set(o for o in self._savedresult if o is not None and not o.isdeleted())
            # Create DHCP entries
            lastlognetinfo = self._lastlognetinfo
            lastlogportinfo = self._lastlogportinfo
            lastserveraddresses = self._lastserveraddresses
            currentlognetinfo = dict((n, id) for n,id in self._lastlognets if n in allobjs)
            currentlogportinfo = dict((p, (id, currentlognetinfo[p.network], p.ip_address, p.mac_address, getattr(p.subnet, 'dhcp_server', self._parent.serveraddress)))
                                      for p,id in self._lastlogports
                                      if p in allobjs and p.network in currentlognetinfo and hasattr(p,'subnet'))
            currentserveraddresses = set((v[4], self._parent.servermac, p.network.id, True)
                                         for p,v in currentlogportinfo.items())
            self._lastlognetinfo = currentlognetinfo
            self._lastlogportinfo = currentlogportinfo
            self._lastserveraddresses = currentserveraddresses
            dhcp_entries = {}
            t1 = self._parent.t1
            t2 = self._parent.t2
            for p in self._savedresult:
                if p is not None and not p.isdeleted() and p.isinstance(LogicalPort) and hasattr(p, 'subnet') \
                        and hasattr(p, 'ip_address') and hasattr(p, 'mac_address'):
                    if p.network in currentlognetinfo and p in currentlogportinfo:
                        try:
                            pid, _, ip_address, mac_address, server_address = currentlogportinfo[p]
                            if getattr(p.subnet, 'dhcp_enabled', False):
                                continue
                            cidr = p.subnet.cidr
                            network, mask = parse_ip4_network(cidr)
                            # options from default settings
                            entries = {d.OPTION_LEASE_TIME: (lambda x: d.DHCPTIME_INFINITE if x is None else x)(self._parent.leasetime)
                                       }
                            # options from network
                            if hasattr(p.network, 'dns_nameservers'):
                                entries[d.OPTION_DNSSERVER] = p.network.dns_nameservers
                            if hasattr(p.network, 'domain_name'):
                                entries[d.OPTION_DOMAINNAME] = p.network.domain_name
                            if hasattr(p.network, 'mtu'):
                                entries[d.OPTION_MTU] = p.network.mtu
                            if hasattr(p.network, 'ntp_servers'):
                                entries[d.OPTION_NTPSERVER] = p.network.ntp_servers
                            if hasattr(p.network, 'lease_time'):
                                entries[d.OPTION_LEASE_TIME] = p.network.lease_time
                            if hasattr(p.network, 'extra_dhcp_options'):
                                entries.update(p.network.extra_dhcp_options)
                            options = d.create_dhcp_options(entries, True, True)
                            # options from subnet
                            entries = {d.OPTION_NETMASK : ip4_addr.formatter(get_netmask(mask)),
                                       d.OPTION_BROADCAST : ip4_addr.formatter(get_broadcast(network, mask))}
                            if hasattr(p.subnet, 'gateway'):
                                entries[d.OPTION_ROUTER] = p.subnet.gateway
                            if hasattr(p.subnet, 'dns_nameservers'):
                                entries[d.OPTION_DNSSERVER] = p.subnet.dns_nameservers
                            if hasattr(p.subnet, 'domain_name'):
                                entries[d.OPTION_DOMAINNAME] = p.subnet.domain_name
                            if hasattr(p.subnet, 'mtu'):
                                entries[d.OPTION_MTU] = p.subnet.mtu
                            if hasattr(p.subnet, 'ntp_servers'):
                                entries[d.OPTION_NTPSERVER] = p.subnet.ntp_servers
                            if hasattr(p.subnet, 'lease_time'):
                                entries[d.OPTION_LEASE_TIME] = p.subnet.lease_time
                            # Routes is special
                            if hasattr(p.subnet, 'host_routes'):
                                routes = list(p.subnet.host_routes)
                                if routes and not any(parse_ip4_network(r[0]) == (0,0) for r in routes):
                                    routes.append(['0.0.0.0/0', p.subnet.gateway])
                                entries[d.OPTION_CLASSLESSROUTE] = routes
                            # TODO: add extra routes from routers
                            if hasattr(p.subnet, 'extra_dhcp_options'):
                                entries.update(p.subnet.extra_dhcp_options)
                            options.update(d.create_dhcp_options(entries, True, True))
                            entries = {}
                            if hasattr(p, 'hostname'):
                                entries[d.OPTION_HOSTNAME] = p.hostname
                            if hasattr(p, 'extra_dhcp_options'):
                                entries.update(p.extra_dhcp_options)
                            options.update(d.create_dhcp_options(entries, True, True))
                            entries = {d.OPTION_SERVER_IDENTIFIER: server_address}
                            options.update(d.create_dhcp_options(entries, True, True))
                            options = dict((k,v) for k,v in options.items() if v is not None)
                            if d.OPTION_LEASE_TIME not in options:
                                options[d.OPTION_LEASE_TIME] = d.create_option_from_value(d.OPTION_LEASE_TIME, d.DHCPTIME_INFINITE)
                            if options[d.OPTION_LEASE_TIME].value != d.DHCPTIME_INFINITE:
                                leasetime = options[d.OPTION_LEASE_TIME].value
                                if d.OPTION_T1 not in options and t1 is not None and t1 < leasetime:
                                    options[d.OPTION_T1] = d.create_option_from_value(d.OPTION_T1, t1)
                                if d.OPTION_T2 not in options and t2 is not None and t2 < leasetime:
                                    options[d.OPTION_T1] = d.create_option_from_value(d.OPTION_T1, t2)
                            dhcp_entries[pid] = (mac_addr(mac_address), ip4_addr(ip_address),
                                                ip4_addr(server_address), options)
                        except Exception:
                            self._logger.warning("Failed to create DHCP options for port id=%r. Will disable DHCP on this port",
                                                 p.id, exc_info = True)
            self._dhcpentries = dhcp_entries
            # Create flows
            # For every logical port, create two flows
            ofdef = connection.openflowdef
            vhost = connection.protocol.vhost
            l3 = self._parent._gettableindex('l3input', vhost)
            cmds = []
            if connection.protocol.disablenxext:
                def match_network(nid):
                    return [ofdef.create_oxm(ofdef.OXM_OF_METADATA_W, ((nid & 0xffff) << 32) | (0xffff << 16),
                                            b'\x00\x00\xff\xff\x40\x00\x00\x00')]
            else:
                def match_network(nid):
                    return [ofdef.create_oxm(ofdef.NXM_NX_REG5, nid),
                            ofdef.create_oxm(ofdef.NXM_NX_REG7_W, 0x4000, 0x4000)]
            def _delete_flows(nid):
                return ofdef.ofp_flow_mod(cookie = 0x1,
                                               cookie_mask = 0xffffffffffffffff,
                                               table_id = l3,
                                               command = ofdef.OFPFC_DELETE,
                                               priority = ofdef.OFP_DEFAULT_PRIORITY,
                                               buffer_id = ofdef.OFP_NO_BUFFER,
                                               out_port = ofdef.OFPP_ANY,
                                               out_group = ofdef.OFPG_ANY,
                                               match = ofdef.ofp_match_oxm(
                                                            oxm_fields = 
                                                                match_network(nid) +
                                                                [ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE, ofdef.ETHERTYPE_IP),
                                                                ofdef.create_oxm(ofdef.OXM_OF_IP_PROTO, ofdef.IPPROTO_UDP),
                                                                ofdef.create_oxm(ofdef.OXM_OF_UDP_DST, 67)
                                                                ]
                                                        )
                                               )
            def _delete_flows2(nid, serveraddr):
                return ofdef.ofp_flow_mod(cookie = 0x1,
                                               cookie_mask = 0xffffffffffffffff,
                                               table_id = l3,
                                               command = ofdef.OFPFC_DELETE,
                                               priority = ofdef.OFP_DEFAULT_PRIORITY,
                                               buffer_id = ofdef.OFP_NO_BUFFER,
                                               out_port = ofdef.OFPP_ANY,
                                               out_group = ofdef.OFPG_ANY,
                                               match = ofdef.ofp_match_oxm(
                                                            oxm_fields = 
                                                                match_network(nid) +
                                                                [ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE, ofdef.ETHERTYPE_IP),
                                                                ofdef.create_oxm(ofdef.OXM_OF_IP_PROTO, ofdef.IPPROTO_UDP),
                                                                ofdef.create_oxm(ofdef.OXM_OF_IPV4_DST,
                                                                                 ip4_addr_bytes(serveraddr)),
                                                                ofdef.create_oxm(ofdef.OXM_OF_UDP_DST, 67)
                                                                ]
                                                        )
                                               )
            def _create_flows(nid):
                return ofdef.ofp_flow_mod(cookie = 0x1,
                                           cookie_mask = 0xffffffffffffffff,
                                           table_id = l3,
                                           command = ofdef.OFPFC_ADD,
                                           priority = ofdef.OFP_DEFAULT_PRIORITY,
                                           buffer_id = ofdef.OFP_NO_BUFFER,
                                           out_port = ofdef.OFPP_ANY,
                                           out_group = ofdef.OFPG_ANY,
                                           match = ofdef.ofp_match_oxm(
                                                        oxm_fields = 
                                                            match_network(nid) +
                                                            [ofdef.create_oxm(ofdef.OXM_OF_ETH_DST_W, b'\x01\x00\x00\x00\x00\x00', b'\x01\x00\x00\x00\x00\x00'),
                                                            ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE, ofdef.ETHERTYPE_IP),
                                                            ofdef.create_oxm(ofdef.OXM_OF_IP_PROTO, ofdef.IPPROTO_UDP),
                                                            ofdef.create_oxm(ofdef.OXM_OF_UDP_DST, 67)
                                                            ]
                                                    ),
                                           instructions = [
                                                ofdef.ofp_instruction_actions(
                                                            actions = [
                                                                ofdef.ofp_action_output(port = ofdef.OFPP_CONTROLLER,
                                                                                        max_len = ofdef.OFPCML_NO_BUFFER
                                                                                        )
                                                            ]
                                                        )
                                                    ]
                                           )
            def _create_flows2(nid, serveraddr):
                return ofdef.ofp_flow_mod(cookie = 0x1,
                                           cookie_mask = 0xffffffffffffffff,
                                           table_id = l3,
                                           command = ofdef.OFPFC_ADD,
                                           priority = ofdef.OFP_DEFAULT_PRIORITY,
                                           buffer_id = ofdef.OFP_NO_BUFFER,
                                           out_port = ofdef.OFPP_ANY,
                                           out_group = ofdef.OFPG_ANY,
                                           match = ofdef.ofp_match_oxm(
                                                        oxm_fields = match_network(nid) +
                                                            [ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE, ofdef.ETHERTYPE_IP),
                                                            ofdef.create_oxm(ofdef.OXM_OF_IP_PROTO, ofdef.IPPROTO_UDP),
                                                            ofdef.create_oxm(ofdef.OXM_OF_IPV4_DST,
                                                                             ip4_addr_bytes(serveraddr)),
                                                            ofdef.create_oxm(ofdef.OXM_OF_UDP_DST, 67)
                                                            ]
                                                    ),
                                           instructions = [
                                                ofdef.ofp_instruction_actions(
                                                            actions = [
                                                                ofdef.ofp_action_output(port = ofdef.OFPP_CONTROLLER,
                                                                                        max_len = ofdef.OFPCML_NO_BUFFER
                                                                                        )
                                                            ]
                                                        )
                                                    ]
                                           )
            for n in removevalues:
                if n in lastlognetinfo:
                    nid = lastlognetinfo[n]
                    cmds.append(_delete_flows(nid))
            lastnetdict = dict((n.id,n) for n in lastlognetinfo)
            for serveraddr in lastserveraddresses:
                if serveraddr not in currentserveraddresses:
                    addr, _, networkid, _ = serveraddr
                    if networkid in lastnetdict:
                        n = lastnetdict[networkid]
                        nid = lastlognetinfo[n]
                        cmds.append(_delete_flows2(nid, addr))
            for m in self.execute_commands(connection, cmds):
                yield m
            
            # Remove old ARP entries; Add new ARP entries
            remove_arps = lastserveraddresses.difference(currentserveraddresses)
            if remove_arps:
                for m in callAPI(self, 'arpresponder', 'removeproxyarp', {'connection': connection,
                                                                          'arpentries': remove_arps}):
                    yield m
            add_arps = currentserveraddresses.difference(lastserveraddresses)
            if add_arps:
                for m in callAPI(self, 'arpresponder', 'createproxyarp', {'connection': connection,
                                                                          'arpentries': add_arps}):
                    yield m
            del cmds[:]
            for n in addvalues:
                if n in currentlognetinfo:
                    nid = currentlognetinfo[n]
                    cmds.append(_create_flows(nid))
            currnetdict = dict((n.id,n) for n in currentlognetinfo)
            for serveraddr in currentserveraddresses:
                if serveraddr not in lastserveraddresses:
                    addr, _, networkid, _ = serveraddr
                    if networkid in currnetdict:
                        n = currnetdict[networkid]
                        nid = currentlognetinfo[n]
                        cmds.append(_create_flows2(nid, addr))
            for m in self.execute_commands(connection, cmds):
                yield m
        except Exception:
            self._logger.warning("Unexpected exception in DHCPUpdater. Will ignore and continue.", exc_info = True)



@defaultconfig
@depend(ofpportmanager.OpenflowPortManager, objectdb.ObjectDB, arpresponder.ARPResponder)
class DHCPServer(FlowBase):
    "DHCP server that responds the DHCP discover/request with static IP address settings"
    _tablerequest = (("l3input", ('l2input',), ''),
                     ("l2output", ("l3input",), ''))
    # Responding DHCP server address
    _default_serveraddress = '169.254.169.254'
    # Responding DHCP server MAC address
    _default_servermac = '1a:23:67:59:63:33'
    # DHCP leases timeout time
    _default_leasetime = None
    # DHCP default T1 option
    _default_t1 = None
    # DHCP default T2 option
    _default_t2 = None
    def __init__(self, server):
        FlowBase.__init__(self, server)
        self.apiroutine = RoutineContainer(self.scheduler)
        self.apiroutine.main = self._main
        self.routines.append(self.apiroutine)
        self._flowupdaters = {}
        self._extra_arps = {}
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
            updater = self._flowupdaters.pop(conn)
            updater.close()
        updater = DHCPUpdater(conn, self)
        #flowupdater = VXLANFlowUpdater(conn, self)
        self._flowupdaters[conn] = updater
        updater.start()
        if False:
            yield
    def _remove_conn(self, conn):
        # Do not need to modify flows
        if conn in self._flowupdaters:
            self._flowupdaters.pop(conn).close()
        if False:
            yield

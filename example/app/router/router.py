from vlcp.config.config import manager
from vlcp.server.module import Module,depend
from vlcp.server import main
from vlcp.event import RoutineContainer
from vlcp.event.core import TimerEvent

from vlcp.service.connection import openflowserver
from vlcp.protocol.openflow import common,OpenflowConnectionStateEvent,OpenflowAsyncMessageEvent

from vlcp.utils.ethernet import ethernetPacket,arpPacket,ETHERTYPE_ARP,ETHERTYPE_IP,ipPacket,icmpPacket
from vlcp.utils.ethernet import ETHERTYPE_8021Q,ethernetPacket_8021Q_OTHER,ethernetPacket_8021Q,ARPOP_REQUEST,ARPOP_REPLY,ethernetinner
from vlcp.utils.ethernet import IPPROTO_ICMP,ICMP_ECHO_REQUEST,ICMP_ECHO_REPLY,icmpEchoPacket

from enum import *

import logging
import netaddr
import array
import socket

@depend(openflowserver.OpenflowServer)
class routerModule(Module):
    def __init__(self,server):
        super(routerModule,self).__init__(server)
        self.routines.append(router(self.scheduler))

class router(RoutineContainer):
    
    logger = logging.getLogger('router')

    def __init__(self,scheduler,daemon= None):
        super(router,self).__init__(scheduler,daemon)
        self.routers = {}
   
    def registerRouter(self,dpid,cls):
        self.logger.info("register router dpid %s",dpid_to_str(dpid))
        routine = cls.start()
        self.routers.setdefault(dpid,(routine,cls))
    
    def unregisterRouter(self,dpid):
        self.logger.info("unregister router dpid %s",dpid_to_str(dpid))

        if dpid in self.routers:
           routine = self.routers[dpid][0]
           self.terminate(routine)
           del self.routers[dpid]

    def main(self):
        while True:
            connectEventMatcher = OpenflowConnectionStateEvent.createMatcher()
            yield(connectEventMatcher,)

            if self.matcher == connectEventMatcher:
                 self.connectStateHandler(self.event)
    
    def connectStateHandler(self,event):
            if event.state == 'setup':
                router = l3switch(self.scheduler,None,event.datapathid,event.connection,
                                event.connection.openflowdef,event.createby,self.logger)
                self.registerRouter(event.datapathid,router)

            if event.state == 'down':
                self.unregisterRouter(event.datapathid)


class l3switch(RoutineContainer):
    def __init__(self,scheduler,daemon,dpid,conn,parser,protocol,logger):
        super(l3switch,self).__init__(scheduler,None)
        self.dpid = dpid
        self.ofp_parser = parser
        self.ofp_proto = protocol
        self.connection = conn
        self.logger = logger
        # store switch port info
        self.ports = {}
        # store switch address info
        self.address = []
        
        # store switch routing table info
        self.routerTable = {}
        # sorted routerTable by last mask IPNetwork, search route will match the first, the last mask
        self.routerList = []
        
        self.flag = 0
        """
        for m in self.add_address("192.168.1.1/24"):
            yield m
        for m in self.add_address("192.168.2.1/24"):
            yield m
        """
    def main(self):
        self.logger.info('l3switch add dpid = %r',self.dpid)
        
        portDescRequest = self.ofp_parser.ofp_multipart_request(type=self.ofp_parser.OFPMP_PORT_DESC)
        for m in self.ofp_proto.querymultipart(portDescRequest,self.connection,self):
            yield m
        
        for portpart in self.openflow_reply:
            for port in portpart.ports:
                #self.logger.info('port no = %r',common.dump(port))
                self.logger.info('port no = %r',port.port_no)
                self.logger.info('port name = %r',port.name)
                self.logger.info('port hw = %r',port.hw_addr)
                
                
                # test code

                if port.port_no == 5:
                    p = portinfo(port.port_no,port.name,port.hw_addr,10)
                    for m in self.add_port(p):
                        yield m
                elif port.port_no == 3:
                    p = portinfo(port.port_no,port.name,port.hw_addr)
                    for m in self.add_port(p):
                        yield m
                elif port.port_no == 4:
                    p = portinfo(port.port_no,port.name,port.hw_addr,10)
                    for m in self.add_port(p):
                        yield m
                elif port.port_no == 7:
                    p = portinfo(port.port_no,port.name,port.hw_addr,11,1)
                    for m in self.add_port(p):
                        yield m
                """
                # get port config info
                
                p = portinfo(port.port_no,port.name,port.hw_addr)
                
                for m in self.add_port(p):
                    yield m
                """

        for m in self.add_default_flow():
            yield m
       
        # # #  add_address is test code

        for m in self.add_address("192.168.1.1/24"):
            yield m

        for m in self.add_address("192.168.2.1/24"):
            yield m
        
        for m in self.add_address("100.66.54.144/22"):
            yield m
        """
        for m in self.add_router('192.168.2.2'):
            yield m
        """
        
        for port_no in self.ports:
            port_hw = self.ports[port_no].port_hw

            for m in self.add_router_flow(port_hw,None):
                yield m
        
        # # # add_rule_flow is test code
        for m in self.add_rule_flow("192.168.1.2","100.66.54.144",7):
            yield m

        time = self.scheduler.setTimer(5,30)
        timerMatch = TimerEvent.createMatcher(time)
        try:
            
            while True:
                # match self dpid connect down event
                connDownMatch = OpenflowConnectionStateEvent.createMatcher(datapathid = self.dpid,state = 'down')

                packetInMatch = OpenflowAsyncMessageEvent.createMatcher(datapathid = self.dpid,type = self.ofp_parser.OFPT_PACKET_IN)

                portStatusMatch = OpenflowAsyncMessageEvent.createMatcher(datapathid = self.dpid,type = self.ofp_parser.OFPT_PORT_STATUS)
                
                """
                for m in self.add_router('192.168.2.2'):
                    yield m
                """
                yield (connDownMatch,packetInMatch,portStatusMatch,timerMatch)

                if self.matcher == connDownMatch:
                    # if connection down , every thing is over ...
                    break
                
                #  subroutine func ..  other will discard this while event !
                if self.matcher == packetInMatch:
                    self.subroutine(self.packetIn_handler(self.event))

                if self.matcher == portStatusMatch:
                    self.subroutine(self.portStatus_handler(self.event))
                
                if self.matcher == timerMatch:
                    self.logger.info('timer matcher ---')
                    
                    """
                    for m in self.add_router("192.168.2.2"):
                        yield m
                    """

                    # $ # add router is test code
                    if self.flag == 0:
                        self.subroutine(self.add_router("100.66.52.1"))
                        self.flag = 1
                    self.subroutine(self.router_check_cycle())
        finally:
            self.scheduler.cancelTimer(time)

    def add_address(self,str_address):

        # address 192.168.1.2/24
        
        address = netaddr.IPNetwork(str_address)

        if address not in self.address:
            self.logger.info("add address %r , %s",address.ip,str_address)
            self.address.append(address)

            # add flow dst address --->>> controller
            match = self.ofp_parser.ofp_match_oxm()
            oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_ETH_TYPE,ETHERTYPE_IP)
            match.oxm_fields.append(oxm)

            oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_IPV4_DST_W,address.network.value,address.netmask.value)
            match.oxm_fields.append(oxm)

            ins = self.ofp_parser.ofp_instruction_actions(type = self.ofp_parser.OFPIT_APPLY_ACTIONS)
            action = self.ofp_parser.ofp_action_output(port = self.ofp_parser.OFPP_CONTROLLER,
                                        max_len = self.ofp_parser.OFPCML_NO_BUFFER )
            ins.actions.append(action)
            
            for m in self.add_flow(table_id = IP_TABLE_ID,match = match ,ins = [ins],priority = 50,
                        buffer_id = self.ofp_parser.OFP_NO_BUFFER):
                yield m
    
    def add_router(self,nexthop,network = "0.0.0.0/0"):
        
        assert network != None
        assert nexthop != None
        
        self.logger.info(" add router network %r , nexthop %r",network,nexthop)

        if True != check_address_in_network(nexthop,self.address):
            error_msg = 'gateway not in our network'
            return
             
        if True == is_gateway(nexthop,self.address):
            error_msg = 'gateway has been subnet gateway'
            return 

        # after check, send arp request to get gateway mac
        gateway = get_gateway_in_network(nexthop,self.address)  
        
        # we can not use untag table to flood arp ,, because we do not know port vlan id

        for port_no in self.ports:
            smac = self.ports[port_no].port_hw
            dmac = [255,255,255,255,255,255]
            vid = self.ports[port_no].vlan_id
            
            # only get gateway mac from public network
            if self.ports[port_no].public_flag == 1:
                if vid != 0:
                    eth = ethernetPacket_8021Q((ethernetinner,arpPacket),dstMac = dmac,srcMac = smac,type = ETHERTYPE_8021Q,
                            type2 = ETHERTYPE_ARP,vlan = vid,arp_hwtype=1,arp_proto=ETHERTYPE_IP,hw_len=6,proto_len=4,
                            arp_op=ARPOP_REQUEST,arp_smac=smac,arp_sip=gateway,arp_tip=netaddr.IPAddress(nexthop).value)
                else:
                    eth = ethernetPacket_8021Q_OTHER((ethernetinner,arpPacket),dstMac = dmac,srcMac = smac,
                            type = ETHERTYPE_ARP,arp_hwtype=1,arp_proto=ETHERTYPE_IP,hw_len=6,proto_len=4,
                            arp_op=ARPOP_REQUEST,arp_smac=smac,arp_sip=gateway,arp_tip=netaddr.IPAddress(nexthop).value)
                
                action1 = self.ofp_parser.nx_action_reg_load(ofs_nbits = (0 << 16)|(32 - 1),dst = self.ofp_parser.NXM_NX_REG0,value = port_no )
                action2 = self.ofp_parser.nx_action_resubmit(in_port = self.ofp_parser.OFPP_IN_PORT & 0xffff,table = UNTAG_TABLE_ID)

                for m in self.packet_out(None,self.ofp_parser.OFPP_CONTROLLER,self.ofp_parser.OFP_NO_BUFFER,eth._tobytes(),[action1,action2]):
                    yield m
        
        #rt = routerItem(netaddr.IPNetwork(network),netaddr.IPAddress(nexthop))
        if netaddr.IPNetwork(network) not in self.routerTable:
            self.routerTable[netaddr.IPNetwork(network)] = {'mac':'','gateway':netaddr.IPAddress(nexthop)}
            self.routerList = sorted(self.routerTable.iteritems(),key = lambda s:s[0],reverse = True)

            self.logger.info("router table %r",self.routerTable)
            self.logger.info("router list %r",self.routerList)
                
        # when we recieve arp reply ,  we add router flow to switch , here do nothing
    
    def add_rule_flow(self,src_ip,dst_ip,port_no):
        
        # only support 1:1 SNAT

        #  add in postrouteing
        match = self.ofp_parser.ofp_match_oxm()
        oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_ETH_TYPE,ETHERTYPE_IP)
        match.oxm_fields.append(oxm)
        
        oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_IPV4_SRC,netaddr.IPAddress(src_ip).value)
        match.oxm_fields.append(oxm)
        
        oxm = self.ofp_parser.create_oxm(self.ofp_parser.NXM_NX_REG0,port_no)
        match.oxm_fields.append(oxm)

        ins = self.ofp_parser.ofp_instruction_actions(type = self.ofp_parser.OFPIT_APPLY_ACTIONS)

        set_sip_field = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_IPV4_SRC,netaddr.IPAddress(dst_ip).value)
        action = self.ofp_parser.ofp_action_set_field(field = set_sip_field) 
        ins.actions.append(action)

        action = self.ofp_parser.nx_action_resubmit(in_port = self.ofp_parser.OFPP_IN_PORT & 0xffff,table = UNTAG_TABLE_ID)
        ins.actions.append(action)

        for m in self.add_flow(table_id = POSTROUTERING_TABLE_ID,match = match,ins = [ins],priority = 50,
                buffer_id = self.ofp_parser.OFP_NO_BUFFER):
            yield m

        # add in prerouteing
        match = self.ofp_parser.ofp_match_oxm()
        oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_ETH_TYPE,ETHERTYPE_IP)
        match.oxm_fields.append(oxm)

        oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_IPV4_DST,netaddr.IPAddress(dst_ip).value)
        match.oxm_fields.append(oxm)

        oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_IN_PORT,port_no)
        match.oxm_fields.append(oxm)

        ins = self.ofp_parser.ofp_instruction_actions(type = self.ofp_parser.OFPIT_APPLY_ACTIONS)
        
        set_dip_field = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_IPV4_DST,netaddr.IPAddress(src_ip).value)
        action = self.ofp_parser.ofp_action_set_field(field = set_dip_field)
        ins.actions.append(action)
        
        action = self.ofp_parser.nx_action_resubmit(in_port = self.ofp_parser.OFPP_IN_PORT & 0xffff,table = ROUTER_TABLE_ID)
        ins.actions.append(action)

        for m in self.add_flow(table_id = PREROUTERING_TABLE_ID,match = match,ins = [ins],priority = 50,
                buffer_id = self.ofp_parser.OFP_NO_BUFFER):
            yield m


    def router_check_cycle(self):

        for key in self.routerTable:
            nexthop = self.routerTable[key]['gateway']

            gateway = get_gateway_in_network(nexthop,self.address)
            
            self.logger.info(" nexthop = %r,gateway = %r",nexthop,gateway)
            for port_no in self.ports:
                smac = self.ports[port_no].port_hw
                dmac = [255,255,255,255,255,255]
                vid = self.ports[port_no].vlan_id
                
                if self.ports[port_no].public_flag == 1:
                    if vid != 0:
                        eth = ethernetPacket_8021Q((ethernetinner,arpPacket),dstMac = dmac,srcMac = smac,type = ETHERTYPE_8021Q,
                                type2 = ETHERTYPE_ARP,vlan = vid,arp_hwtype = 1,arp_proto = ETHERTYPE_IP,hw_len = 6,proto_len = 4,
                                arp_op = ARPOP_REQUEST,arp_smac = smac,arp_sip = gateway,arp_tip = nexthop.value)
                    else:
                        eth = ethernetPacket_8021Q_OTHER((ethernetinner,arpPacket),dstMac = dmac,srcMac = smac,
                                type = ETHERTYPE_ARP,arp_hwtype = 1,arp_proto = ETHERTYPE_IP,hw_len = 6,proto_len = 4,
                                arp_op = ARPOP_REQUEST,arp_smac = smac,arp_sip = gateway,arp_tip = nexthop.value)
                    
                    action1 = self.ofp_parser.nx_action_reg_load(ofs_nbits = (0 << 16) | (32 - 1),dst = self.ofp_parser.NXM_NX_REG0,value = port_no)
                    action2 = self.ofp_parser.nx_action_resubmit(in_port = self.ofp_parser.OFPP_IN_PORT & 0xffff,table = UNTAG_TABLE_ID)
                    
                    for m in self.packet_out(None, self.ofp_parser.OFPP_CONTROLLER,self.ofp_parser.OFP_NO_BUFFER,eth._tobytes(),[action1,action2]):
                        yield m

    def update_router_flow(self,vid,smac,sip,in_port):
        

        self.logger.info(" ##############") 
        self.logger.info(" update_router_flow %r",netaddr.IPAddress(sip))
        self.logger.info(" ##############") 

        port_mac = self.ports[in_port].port_hw

        for key in self.routerTable:
            if netaddr.IPAddress(sip) == self.routerTable[key]['gateway']:
                self.routerTable[key]['mac'] = format_mac_to_string(smac) 
                            
                # add router flow
                match = self.ofp_parser.ofp_match_oxm()
                oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_ETH_TYPE,ETHERTYPE_IP)
                match.oxm_fields.append(oxm)

                oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_IPV4_DST_W,key.network.value,key.netmask.value)
                match.oxm_fields.append(oxm)

                oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_VLAN_VID_W,self.ofp_parser.OFPVID_PRESENT,self.ofp_parser.OFPVID_PRESENT)
                match.oxm_fields.append(oxm)

                ins = self.ofp_parser.ofp_instruction_actions(type = self.ofp_parser.OFPIT_APPLY_ACTIONS)

                set_vid_field = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_VLAN_VID,vid | self.ofp_parser.OFPVID_PRESENT)
                action = self.ofp_parser.ofp_action_set_field(field = set_vid_field) 
                ins.actions.append(action)
                
                set_smac_field = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_ETH_SRC,port_mac)
                action = self.ofp_parser.ofp_action_set_field(field = set_smac_field)
                ins.actions.append(action)

                set_dmac_field = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_ETH_DST,smac)
                action = self.ofp_parser.ofp_action_set_field(field = set_dmac_field)
                ins.actions.append(action)
                
                action = self.ofp_parser.nx_action_reg_load(ofs_nbits = (0 << 16) | (32 - 1),dst = self.ofp_parser.NXM_NX_REG0,value = in_port)
                ins.actions.append(action)
                action = self.ofp_parser.nx_action_resubmit(in_port = self.ofp_parser.OFPP_IN_PORT & 0xffff,table = POSTROUTERING_TABLE_ID)
                ins.actions.append(action)
                
                self.logger.info("\n\n\n --- update router ---- %r",netaddr.IPAddress(sip))

                for m in self.add_flow(table_id = IP_TABLE_ID,match = match ,ins = [ins],priority = 1 + key.prefixlen,
                        buffer_id = self.ofp_parser.OFP_NO_BUFFER):
                    yield m

    def packetIn_handler(self,event):
        
        in_port = get_portno_from_event(event)
        assert in_port != None

        ethernet = ethernetPacket.create(event.message.data)
        #self.logger.info(" message = %r",common.dump(event.message))
        self.logger.info(" message ethernet = %r",common.dump(ethernet))
        # arp packet in  
        if ethernet.type == ETHERTYPE_ARP:
            for m in self.arp_packetIn_handler(event,ethernet.srcMac,ethernet.dstMac,ethernet.arp_sip,
                    ethernet.arp_tip,ethernet.arp_op,in_port):
                yield m
            raise StopIteration
        
        # 8021Q  ARP  
        if ethernet.type == ETHERTYPE_8021Q and ethernet.type2 == ETHERTYPE_ARP:
            for m in self.arp_packetIn_handler(event,ethernet.srcMac,ethernet.dstMac,ethernet.arp_sip,
                    ethernet.arp_tip,ethernet.arp_op,in_port,ethernet.vlan):
                yield m
            raise StopIteration
        
        # ip packet in
        if ethernet.type == ETHERTYPE_IP:
            if True == is_gateway(netaddr.IPAddress(ethernet.dstaddr),self.address):
                # ip request to port gateway
                if ethernet.proto == IPPROTO_ICMP:
                   for m in self.icmp_packetin_handler(event,ethernet.srcMac,ethernet.dstMac,
                            ethernet.version_length,ethernet.tos,ethernet.flag_offset,ethernet.ttl,ethernet.identification,ethernet.srcaddr,ethernet.dstaddr,
                            ethernet.data,in_port):
                        yield m
                elif ethernet.proto == IPPROTO_TCP or ethernet.proto == IPPROTO_UDP:
                    # cause icmp dst unreach error
                    pass
                else:
                    # do nothing ignore this package
                    pass
                # icmp unreach error    
            else:
                # ip goto other subnet
                for m in self.packet_to_node(event,ethernet.dstaddr,in_port):
                    yield m
            raise StopIteration
            
        if ethernet.type == ETHERTYPE_8021Q and ethernet.type2 == ETHERTYPE_IP:
            if True == is_gateway(netaddr.IPAddress(ethernet.dstaddr),self.address):
                if ethernet.proto == IPPROTO_ICMP:
                   for m in self.icmp_packetin_handler(event,ethernet.srcMac,ethernet.dstMac,
                            ethernet.version_length,ethernet.tos,ethernet.flag_offset,ethernet.ttl,ethernet.identification,ethernet.srcaddr,ethernet.dstaddr,
                            ethernet.data,in_port,vid = ethernet.vlan):
                        yield m
                elif ethernet.proto == IPPROTO_TCP or ethernet.proto == IPPROTO_UDP :
                    # cause icmp dst unreach error
                    pass
                else:
                    # do nothing ignore this packeage
                    pass
            else:
                
                for m in self.packet_to_node(event,ethernet.dstaddr,in_port,ethernet.vlan):
                    yield m
            raise StopIteration
    
    def packet_to_node(self,event,dst_ip,in_port,vid = 0):
        
        if True == check_address_in_network(netaddr.IPAddress(dst_ip),self.address):
           # dst is our subnet, broadcast arp  
           gateway = get_gateway_in_network(netaddr.IPAddress(dst_ip),self.address)
                
        else:
            # 
            #check route table
            #

            # routerTable is sort by last netmask , so find the first
            
            # router_item eg:(IPNetwork('192.168.1.1/24'), {'mac': '55:55', 'gateway': IPAddress('192.168.2.3')})
            # but I think it will never run here
           for router_item in self.routerList:
               if netaddr.IPAddress(dst_ip) in router_item[0]:
                    gateway = router_item[1]['gateway']
                    break
        """ 
        for port_no in self.ports:
            if port_no != in_port and self.ports[port_no].public_flag != 1:
                
                #self.logger.info(" \n\n output port %r, sip = %r, dip = %r",port_no,netaddr.IPAddress(gateway),netaddr.IPAddress(dst_ip))

                smac = self.ports[port_no].port_hw
                dmac = [255, 255, 255, 255, 255, 255]
                vid = self.ports[port_no].vlan_id
                if vid != 0:
                    eth = ethernetPacket_8021Q((ethernetinner,arpPacket),dstMac = dmac,srcMac = smac,type=ETHERTYPE_8021Q,
                                    type2=ETHERTYPE_ARP,vlan=vid,arp_hwtype=1,arp_proto=ETHERTYPE_IP,hw_len=6,proto_len=4,
                                    arp_op = ARPOP_REQUEST,arp_smac = smac,arp_sip=gateway,arp_tip=dst_ip)
                else:
                    eth = ethernetPacket_8021Q_OTHER((ethernetinner,arpPacket),dstMac = dmac,srcMac = smac,
                                    type=ETHERTYPE_ARP,arp_hwtype=1,arp_proto=ETHERTYPE_IP,hw_len=6,proto_len=4,
                                    arp_op = ARPOP_REQUEST,arp_smac = smac,arp_sip=gateway,arp_tip=dst_ip)
                    
                action1 = self.ofp_parser.nx_action_reg_load(ofs_nbits = (0 << 16) | (32 - 1),dst = self.ofp_parser.NXM_NX_REG0,value = port_no)
                action2 = self.ofp_parser.nx_action_resubmit(in_port = self.ofp_parser.OFPP_IN_PORT & 0xffff,table = POSTROUTERING_TABLE_ID)
                for m in self.packet_out(event,self.ofp_parser.OFPP_CONTROLLER,self.ofp_parser.OFP_NO_BUFFER,eth._tobytes(),[action1,action2]):
                    yield m
        """

        for port_no in self.ports:
            if port_no != in_port and self.ports[port_no].public_flag != 1:
                smac = self.ports[port_no].port_hw
                dmac = [255,255,255,255,255,255]
                vlan_vid = self.ports[port_no].vlan_id

                if vid == 0 :
                    eth = ethernetPacket_8021Q((ethernetinner,arpPacket),dstMac = dmac,srcMac = smac,type = ETHERTYPE_8021Q,
                                type2 = ETHERTYPE_ARP,vlan = vlan_vid,arp_hwtype = 1,arp_proto=ETHERTYPE_IP,hw_len = 6,proto_len = 4,
                                arp_op = ARPOP_REQUEST,arp_smac = smac,arp_sip = gateway,arp_tip = dst_ip)
                else:
                    eth = ethernetPacket_8021Q((ethernetinner,arpPacket),dstMac = dmac,srcMac = smac,type=ETHERTYPE_8021Q,
                                    type2=ETHERTYPE_ARP,vlan=vid,arp_hwtype=1,arp_proto=ETHERTYPE_IP,hw_len=6,proto_len=4,
                                    arp_op = ARPOP_REQUEST,arp_smac = smac,arp_sip=gateway,arp_tip=dst_ip)
                
                action1 = self.ofp_parser.nx_action_reg_load(ofs_nbits = (0 << 16) | (32 - 1),dst = self.ofp_parser.NXM_NX_REG0,value = port_no)
                action2 = self.ofp_parser.nx_action_resubmit(in_port = self.ofp_parser.OFPP_IN_PORT & 0xffff,table = POSTROUTERING_TABLE_ID)
                for m in self.packet_out(event,self.ofp_parser.OFPP_CONTROLLER,self.ofp_parser.OFP_NO_BUFFER,eth._tobytes(),[action1,action2]):
                    yield m

                
        """
        if vid != 0:
            eth = ethernetPacket_8021Q((ethernetinner,arpPacket),dstMac=smac,srcMac=targetMac,
                                type=ETHERTYPE_8021Q,type2=ETHERTYPE_ARP,vlan=vid,arp_hwtype=1,arp_proto=ETHERTYPE_IP,
                                    hw_len=6,proto_len=4,arp_op=ARPOP_REPLY,arp_smac=targetMac,arp_tmac=smac,
                                    arp_sip = tip,arp_tip = sip)
        else:
            #
           # we must use port mac as smac, so we don't untag table flood 
            # iterator all port packet out 
            #
            #
            
            eth = ethernetPacket_8021Q_OTHER((ethernetinner,arpPacket),dstMac=smac,srcMac=targetMac,
                                type=ETHERTYPE_ARP,arp_hwtype=1,arp_proto=ETHERTYPE_IP,hw_len=6,
                                proto_len=4,arp_op=ARPOP_REPLY,arp_smac=targetMac,arp_tmac=smac,arp_sip=tip,arp_tip = sip)
        """
    def icmp_packetin_handler(self,event,srcMac,dstMac,version_length,tos,flag_offset,ttl,identification,src_ip,dst_ip,
                                        data,in_port,vid = 0):
        
        icmp = icmpPacket.create(data)
        
        if icmp.icmptype == ICMP_ECHO_REQUEST:

            for m in self.packet_out_icmp(event,srcMac,dstMac,
                            version_length,tos,flag_offset,ttl,identification,src_ip,dst_ip,
                            ICMP_ECHO_REPLY,0,icmp.identifier,icmp.seq,icmp.icmp_data,in_port,vid):
                yield m
            """
            # create an icmp reply packet, packet out
            icmpReply = icmpEchoPacket(icmptype = ICMP_ECHO_REPLY,code = 0,icmp_check_sum = 0,
                    identifier = icmp.identifier,seq = icmp.seq,icmp_data = icmp.icmp_data)
            sum = checksum(icmpReply._tobytes())
            icmpReply.icmp_check_sum = sum
            
            # create ip packet
            ip_total_len = (version_length & 0xf) * 4 + icmpReply._realsize()
            ip = ipPacket(version_length = version_length,tos = tos,total_len = ip_total_len,
                    flag_offset = flag_offset,ttl = ttl,proto = IPPROTO_ICMP,identification = identification,
                    checksum = 0,srcaddr = dst_ip,dstaddr = src_ip)
            sum = checksum(ip._tobytes())
            ip.checksum = sum

            ip.data = icmpReply._tobytes()
            
            if vid == 0:
               eth = ethernetPacket_8021Q_OTHER(srcMac = dstMac,dstMac = srcMac,type=ETHERTYPE_IP)
            else:
               eth = ethernetPacket_8021Q(srcMac = dstMac,dstMac = srcMac,type = ETHERTYPE_8021Q,type2 = ETHERTYPE_IP)
            
            eth._setextra(ip._tobytes())

            #self.logger.info(" icmp packet = %r",common.dump(eth))
           
            # ip packet , resubmit to POSTROUTERING_TABLE_ID
            action1 = self.ofp_parser.nx_action_reg_load(ofs_nbits = (0 << 16) | (32 - 1),dst = self.ofp_parser.NXM_NX_REG0,value = in_port)
            action2 = self.ofp_parser.nx_action_resubmit(in_port = self.ofp_parser.OFPP_IN_PORT & 0xffff,table = POSTROUTERING_TABLE_ID)
           
            for m in self.packet_out(event,self.ofp_parser.OFPP_CONTROLLER,self.ofp_parser.OFP_NO_BUFFER,eth._tobytes(),[action1,action2]):
                yield m
            """


    def packet_out_icmp(self,event,srcMac,dstMac,
                      version_length,tos,flag_offset,ttl,identification,src_ip,dst_ip,
                      icmptype,code,identifier,seq,data,in_port,vid = 0):
        
        # create an icmp reply packet, packet out
        icmpReply = icmpEchoPacket(icmptype = icmptype,code = code,icmp_check_sum = 0,
                identifier = identifier,seq = seq,icmp_data = data)
        sum = checksum(icmpReply._tobytes())
        icmpReply.icmp_check_sum = sum
        
        # create ip packet
        ip_total_len = (version_length & 0xf) * 4 + icmpReply._realsize()
        ip = ipPacket(version_length = version_length,tos = tos,total_len = ip_total_len,
                flag_offset = flag_offset,ttl = ttl,proto = IPPROTO_ICMP,identification = identification,
                checksum = 0,srcaddr = dst_ip,dstaddr = src_ip)
        sum = checksum(ip._tobytes())
        ip.checksum = sum

        ip.data = icmpReply._tobytes()
        
        if vid == 0:
           eth = ethernetPacket_8021Q_OTHER(srcMac = dstMac,dstMac = srcMac,type=ETHERTYPE_IP)
        else:
           eth = ethernetPacket_8021Q(srcMac = dstMac,dstMac = srcMac,vlan = vid,type = ETHERTYPE_8021Q,type2 = ETHERTYPE_IP)
        
        eth._setextra(ip._tobytes())

        #self.logger.info(" icmp packet = %r",common.dump(eth))
       
        # ip packet , resubmit to POSTROUTERING_TABLE_ID
        action1 = self.ofp_parser.nx_action_reg_load(ofs_nbits = (0 << 16) | (32 - 1),dst = self.ofp_parser.NXM_NX_REG0,value = in_port)
        action2 = self.ofp_parser.nx_action_resubmit(in_port = self.ofp_parser.OFPP_IN_PORT & 0xffff,table = POSTROUTERING_TABLE_ID)
       
        for m in self.packet_out(event,self.ofp_parser.OFPP_CONTROLLER,self.ofp_parser.OFP_NO_BUFFER,eth._tobytes(),[action1,action2]):
            yield m


        """ 
        icmpEcho= icmpEchoPacket(icmptype = icmptype,code = 0,icmp_check_sum = 0,identifier = identifier,seq = seq,icmp_data = data)
        cs = checksum(icmpEcho._tobytes())

        self.logger.info(" icmp check sum = %r",cs)

        icmpEcho.icmp_check_sum = cs
        
        self.logger.info(" icmp check sum = %r",icmpEcho.icmp_check_sum)
        ip_total_len = (version_length & 0xf)*4 + icmpEcho._realsize()

        self.logger.info(" ip totoal  %r = %r + %r , datalen = %r",ip_total_len,version_length & 0xf,icmpEcho._realsize(),len(data))

        ip = ipPacket(version_length = version_length,tos = tos,total_len = ip_total_len ,flag_offset = flag_offset,ttl = ttl,proto = IPPROTO_ICMP,
                identification = identification,checksum = 0,srcaddr = dst_ip,dstaddr = src_ip)
        cs = checksum(ip._tobytes())
        ip.checksum = cs
        
        self.logger.info(" ip check sum = %r",cs)
        #ip._setextra(icmpEcho._tobytes())
        icmpEcho.version_length = version_length
        icmpEcho.tos = tos
        icmpEcho.total_len = icmpEcho._realsize()
        icmpEcho.flag_offset = flag_offset
        icmpEcho.ttl = ttl
        icmpEcho.proto = IPPROTO_ICMP
        icmpEcho.identification = identification
        icmpEcho.checksum = cs
        icmpEcho.srcaddr = dst_ip
        icmpEcho.dstaddr = src_ip

        if vid == 0:
            eth = ethernetPacket_8021Q_OTHER(srcMac = dstMac,dstMac=srcMac,type=ETHERTYPE_IP)
        else:
            eth = ethernetPacket_8021Q(srcMac = dstMac,dstMac = srcMac,vlan = vid,type = ETHERTYPE_8021Q,type2 = ETHERTYPE_IP)

        eth._setextra(icmpEcho._tobytes())
        

        self.logger.info("eth = %r",common.dump(ethernetPacket.create(eth._tobytes())))

        # ip packet , resubmit to POSTROUTERING_TABLE_ID
        action1 = self.ofp_parser.nx_action_reg_load(ofs_nbits = (0 << 16) | (32 - 1),dst = self.ofp_parser.NXM_NX_REG0,value = in_port)
        action2 = self.ofp_parser.nx_action_resubmit(in_port = self.ofp_parser.OFPP_IN_PORT & 0xffff,table = POSTROUTERING_TABLE_ID)

        for m in self.packet_out(event,self.ofp_parser.OFPP_CONTROLLER,self.ofp_parser.OFP_NO_BUFFER,eth._tobytes(),[action1,action2]):
            yield m
        """

    def mac_learn(self,vlan_id,smac,in_port):
 
        self.logger.info(" add mac learn flow ")
        
        #
        #  public network is l3 ,, do not learn mac
        #
        if self.ports[in_port].public_flag == 1:
            raise StopIteration

        if vlan_id == 0:
            match = self.ofp_parser.ofp_match_oxm()
            oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_ETH_DST,netaddr.EUI(format_mac_to_string(smac)).packed)
            match.oxm_fields.append(oxm)
            
            action = self.ofp_parser.nx_action_reg_load(ofs_nbits = (0 << 16) | (32 - 1),dst = self.ofp_parser.NXM_NX_REG0,value = in_port)
            ins = self.ofp_parser.ofp_instruction_actions(type = self.ofp_parser.OFPIT_APPLY_ACTIONS)
            ins.actions.append(action)
             
            for m in self.add_flow(table_id = MAC_TABLE_ID,match = match,ins = [ins],priority = 150,
                    idle_time = 60,buffer_id = self.ofp_parser.OFP_NO_BUFFER):
                yield m

        else :
            match = self.ofp_parser.ofp_match_oxm()
            oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_VLAN_VID, vlan_id | self.ofp_parser.OFPVID_PRESENT)
            match.oxm_fields.append(oxm)

            oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_ETH_DST,netaddr.EUI(format_mac_to_string(smac)).packed)
            match.oxm_fields.append(oxm)
            
            action = self.ofp_parser.nx_action_reg_load(ofs_nbits = (0 << 16) | (32 - 1),dst = self.ofp_parser.NXM_NX_REG0,value = in_port)
            ins = self.ofp_parser.ofp_instruction_actions(type= self.ofp_parser.OFPIT_APPLY_ACTIONS)
            ins.actions.append(action)

            for m in self.add_flow(table_id = MAC_TABLE_ID,match = match ,ins = [ins],priority = 100,
                                        idle_time = 60,buffer_id = self.ofp_parser.OFP_NO_BUFFER):
                yield m
    
    def host_learn(self,vid,smac,sip,in_port):
        
        self.logger.info("\n\n host learn -- %r \n\n",netaddr.IPAddress(sip))

        #if True == is_gateway(netaddr.IPAddress(sip),self.address):
        #    raise StopIteration

        port_mac = self.ports[in_port].port_hw 

        match = self.ofp_parser.ofp_match_oxm()
        oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_ETH_TYPE,ETHERTYPE_IP)
        match.oxm_fields.append(oxm)
        
        oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_IPV4_DST,sip)
        match.oxm_fields.append(oxm)

        oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_VLAN_VID_W,self.ofp_parser.OFPVID_PRESENT,self.ofp_parser.OFPVID_PRESENT)
        match.oxm_fields.append(oxm)
        

        ins = self.ofp_parser.ofp_instruction_actions(type = self.ofp_parser.OFPIT_APPLY_ACTIONS)
        
        set_vid_field = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_VLAN_VID, vid | self.ofp_parser.OFPVID_PRESENT)
        action = self.ofp_parser.ofp_action_set_field(field = set_vid_field)
        ins.actions.append(action)
        
        set_smac_field = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_ETH_SRC,port_mac)
        action = self.ofp_parser.ofp_action_set_field(field = set_smac_field)
        ins.actions.append(action)

        set_dmac_field = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_ETH_DST,smac)
        action = self.ofp_parser.ofp_action_set_field(field = set_dmac_field)
        ins.actions.append(action)
                        
        action = self.ofp_parser.nx_action_reg_load(ofs_nbits = (0 << 16) | (32 -1 ),dst = self.ofp_parser.NXM_NX_REG0,value = in_port)
        ins.actions.append(action)
        
        action = self.ofp_parser.nx_action_resubmit(in_port = self.ofp_parser.OFPP_IN_PORT & 0xffff,table = POSTROUTERING_TABLE_ID)
        ins.actions.append(action)
 
        for m in self.add_flow(table_id = IP_TABLE_ID,match = match,ins=[ins],priority = 100,
                        idle_time = 60,buffer_id = self.ofp_parser.OFP_NO_BUFFER):
            yield m

    def add_router_flow(self,gatewayMac,gatewayIP):

        #
        # add dst_mac == gatewayMac , IP , resubmit IP_TABLE_ID 
        #

        match = self.ofp_parser.ofp_match_oxm()
        oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_ETH_DST,netaddr.EUI(format_mac_to_string(gatewayMac)).packed)
        match.oxm_fields.append(oxm)

        oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_ETH_TYPE,ETHERTYPE_IP)
        match.oxm_fields.append(oxm)

        action = self.ofp_parser.nx_action_resubmit(in_port = self.ofp_parser.OFPP_IN_PORT & 0xffff,table=IP_TABLE_ID)
        ins = self.ofp_parser.ofp_instruction_actions(type = self.ofp_parser.OFPIT_APPLY_ACTIONS)

        ins.actions.append(action)

        for m in self.add_flow(table_id = ROUTER_TABLE_ID,match = match,ins = [ins],priority = 1000,
                buffer_id = self.ofp_parser.OFP_NO_BUFFER):
            yield m

        
        #
        # dst_ip == gateway , to controller --
        # 
        # default flow do it
        """
        match = self.ofp_parser.ofp_match_oxm()

        oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_ETH_TYPE,ETHERTYPE_IP)
        match.oxm_fields.append(oxm)
        oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_IPV4_DST,gatewayIP)
        match.oxm_fields.append(oxm)

        action = self.ofp_parser.ofp_action_output(port = self.ofp_parser.OFPP_CONTROLLER,
                                max_len = self.ofp_parser.OFPCML_NO_BUFFER)
        ins = self.ofp_parser.ofp_instruction_actions(type = self.ofp_parser.OFPIT_APPLY_ACTIONS)
        ins.actions.append(action)
        
        for m in self.add_flow(table_id = IP_TABLE_ID,match = match,ins = [ins],priority = 100,
                    idle_time = 600,buffer_id = self.ofp_parser.OFP_NO_BUFFER):
            yield m
        """
    def ip_packetIn_handler(self,event):
        self.logger.info(" ip_handlerIn_handler")
        
        if True == is_gateway(netaddr.IPAddress(dst_ip),self.address):
            # ip request to port gateway
            pass
        else:
            pass 
    
    def waitReplyGatewayArp(self,event,smac,sip,tip,in_port):
        while True:
            packetInMatch = OpenflowAsyncMessageEvent.createMatcher(datapathid = self.dpid,type = self.ofp_parser.OFPT_PACKET_IN)
            
            for m in self.waitWithTimeout(5,packetInMatch):
                yield m


            if self.timeout:
                break
            else:
                eth = ethernetPacket.create(self.event.message.data)

                if eth.type == ETHERTYPE_8021Q and eth.type2 == ETHERTYPE_ARP and eth.arp_op == ARPOP_REPLY and eth.arp_sip == tip:
                    
                    self.logger.info("\n\n reply %r \n\n",common.dump(eth))
                    self.logger.info(" eth.arp_tip = %r, %r",netaddr.IPAddress(eth.arp_tip),tip)
                    self.logger.info(" eth.arp_sip = %r",netaddr.IPAddress(eth.arp_sip))
                    self.logger.info(" eth.srcMac = %r",eth.srcMac)


                    ethernet = ethernetPacket_8021Q((ethernetinner,arpPacket),dstMac=smac,srcMac=eth.srcMac,
                            type=ETHERTYPE_8021Q,type2=ETHERTYPE_ARP,vlan=eth.vlan,arp_hwtype=1,arp_proto=ETHERTYPE_IP,
                            hw_len=6,proto_len=4,arp_op=ARPOP_REPLY,arp_smac=eth.arp_smac,arp_tmac=smac,
                            arp_sip = eth.arp_sip,arp_tip = sip)
                    action1 = self.ofp_parser.nx_action_reg_load(ofs_nbits = (0 << 16) | (32 - 1),dst = self.ofp_parser.NXM_NX_REG0,value = in_port)
                    action2 = self.ofp_parser.nx_action_resubmit(in_port = self.ofp_parser.OFPP_IN_PORT & 0xffff,table = POSTROUTERING_TABLE_ID)

                    self.logger.info("\n\n wait reply %r \n\n",common.dump(ethernet))

                    for m in self.packet_out(event,self.ofp_parser.OFPP_CONTROLLER,self.ofp_parser.OFP_NO_BUFFER,ethernet._tobytes(),[action1,action2]):
                        yield m
                    break


    def arp_packetIn_handler(self,event,smac,dmac,sip,tip,op,in_port,vid = 0):
        
        self.logger.info('smac = %r',smac)
        self.logger.info('dmac = %r',dmac)
        self.logger.info('sip = %r,%r',sip,netaddr.IPAddress(sip).format())
        self.logger.info('tip = %r,%r',tip,netaddr.IPAddress(tip).format())
        self.logger.info('op = %r',op)
        self.logger.info('in_port = %d',in_port)

        buffer_id = event.message.buffer_id
        data = event.message.data
        
        #self.logger.info(" message = %r",common.dump(event.message))
        
        #
        # check this host is our network
        #
        if False == check_address_in_network(netaddr.IPAddress(sip),self.address):
            raise StopIteration

        if False == check_address_in_network(netaddr.IPAddress(tip),self.address):
            raise StopIteration
        

        vlan_id = self.get_vlan_id(in_port)
        if vlan_id == None:
            return
    
        if vlan_id == 0 and True == is_gateway(netaddr.IPAddress(sip),self.address):
            
            self.logger.info(" ----- we start subroutine ,,  run wait reply ---\n")

            self.subroutine(self.waitReplyGatewayArp(event,smac,sip,tip,in_port))
            for port_no in self.ports:
                if self.ports[port_no].public_flag != 1 and self.ports[port_no].vlan_id == vid:
                    smac = self.ports[port_no].port_hw
                    dmac = [255,255,255,255,255,255]
                    ethernet = ethernetPacket_8021Q((ethernetinner,arpPacket),dstMac=dmac,srcMac=smac,
                            type=ETHERTYPE_8021Q,type2=ETHERTYPE_ARP,vlan=vid,arp_hwtype=1,arp_proto=ETHERTYPE_IP,
                            hw_len=6,proto_len=4,arp_op=ARPOP_REQUEST,arp_smac=smac,
                            arp_sip = sip,arp_tip = tip)
                    action1 = self.ofp_parser.nx_action_reg_load(ofs_nbits = (0 << 16) | (32 - 1),dst = self.ofp_parser.NXM_NX_REG0,value = port_no)
                    action2 = self.ofp_parser.nx_action_resubmit(in_port = self.ofp_parser.OFPP_IN_PORT & 0xffff,table = POSTROUTERING_TABLE_ID)
                    for m in self.packet_out(event,self.ofp_parser.OFPP_CONTROLLER,self.ofp_parser.OFP_NO_BUFFER,ethernet._tobytes(),[action1,action2]):
                        yield m

            raise StopIteration

        #
        # we learn mac info to table 
        # tag + dstmac, action load portno into reg0 
        #
        for m in self.mac_learn(vid,smac,in_port):
            yield m
        
        # host learn
        for m in self.host_learn(vid,smac,sip,in_port):
            yield m
        
        in_port_mac = self.ports[in_port].port_hw

        # if request gateway arp
        # add flow mac + gateway ==>> controller , packet out gateway mac (in_port mac)
        
        if (in_port_mac == dmac or dmac == [255,255,255,255,255,255]) and True == is_gateway(netaddr.IPAddress(tip),self.address):
            self.logger.info("arp to gateway %s",netaddr.IPAddress(tip))
            if op == ARPOP_REQUEST:
                
                targetMac = self.get_port_mac(in_port)
                
                #
                # this two method to create packet
                #
                """
                arp = arpPacket(arp_hwtype=1,arp_proto=ETHERTYPE_IP,hw_len=6,
                                    proto_len=4,arp_op=ARPOP_REPLY,arp_smac=targetMac,arp_tmac=smac,
                                    arp_sip = tip, arp_tip = sip)
                eth = ethernetPacket(dstMac = smac,srcMac = targetMac,type = ETHERTYPE_ARP)
                eth._setextra(arp._tobytes())

                if vid != 0:
                    self.logger.info("vid = %r",vid)
                    eth = ethernetPacket_8021Q(dstMac = smac,srcMac = targetMac,type = ETHERTYPE_8021Q,vlan = vid,type2 = ETHERTYPE_ARP) 
                else:
                    eth = ethernetPacket_8021Q_OTHER(dstMac = smac,srcMac = targetMac,type = ETHERTYPE_ARP)
                
                eth._setextra(arp._tobytes())
                """
                if vid != 0:
                    eth = ethernetPacket_8021Q((ethernetinner,arpPacket),dstMac=smac,srcMac=targetMac,
                                        type=ETHERTYPE_8021Q,type2=ETHERTYPE_ARP,vlan=vid,arp_hwtype=1,arp_proto=ETHERTYPE_IP,
                                            hw_len=6,proto_len=4,arp_op=ARPOP_REPLY,arp_smac=targetMac,arp_tmac=smac,
                                            arp_sip = tip,arp_tip = sip)
                else:
                    
                    eth = ethernetPacket_8021Q_OTHER((ethernetinner,arpPacket),dstMac=smac,srcMac=targetMac,
                                        type=ETHERTYPE_ARP,arp_hwtype=1,arp_proto=ETHERTYPE_IP,hw_len=6,
                                        proto_len=4,arp_op=ARPOP_REPLY,arp_smac=targetMac,arp_tmac=smac,arp_sip=tip,arp_tip = sip)
               
                #self.logger.info("eth = %r",common.dump(ethernetPacket.create(eth._tobytes())))
                
                #
                # we have arp reply to host with in_port_mac ,
                # add flow handle dst_mac == in_port_mac ,,, it is l3 packet
                # this flow has idle_timeout ,, port del,address del ,, delete it 
                #   
                # add it before arp reply 
                #
                """
                for m in self.add_router_flow(targetMac,tip):
                    yield m
                """
                #
                # we konw packet out port , so load port to reg0, output iti,resubmit it to untag table
                #
                action1 = self.ofp_parser.nx_action_reg_load(ofs_nbits = (0 << 16) | (32 -1 ),dst = self.ofp_parser.NXM_NX_REG0,value = in_port) 
                action2 = self.ofp_parser.nx_action_resubmit(in_port = self.ofp_parser.OFPP_IN_PORT & 0xffff,table = UNTAG_TABLE_ID)
                
                #
                # we want packet out to in_port ,,,  so action must port = OFPP_IN_PORT, other the packet will ingore
                # but if we load OFPP_IN_PORT in reg0 ,  it will miss in untag table,  wrong , so load port must be logic port
                # we set packet out in_port property , OFPP_CONTROLLER
                #
                for m in self.packet_out(event,self.ofp_parser.OFPP_CONTROLLER,self.ofp_parser.OFP_NO_BUFFER,eth._tobytes(),[action1,action2]):
                    yield m
            
            elif op == ARPOP_REPLY:

                self.logger.info("arp reply from %r to port gateway %r",netaddr.IPAddress(sip),netaddr.IPAddress(tip))
                
                #
                # if sip is our router gateway 
                # update gateway mac, add router flow 
                #

                if vlan_id == 0:
                    #
                    # it means gatway is in trunk port, use vid from packet
                    #
                    for m in self.update_router_flow(vid,smac,sip,in_port):
                        yield m
                else:
                    for m in self.update_router_flow(vlan_id,smac,sip,in_port):
                        yield m

                # 
                # receive arp reply , we should packet out to it cache data
                #

                # now do nothing
        else:
            # packout packet to switch, do nothing to arp packet, action resubmit --> UNTAG_TABLE_ID (l2 arp broadcast)
            action = self.ofp_parser.nx_action_resubmit(in_port = self.ofp_parser.OFPP_IN_PORT & 0xffff,table = UNTAG_TABLE_ID)
            for m in self.packet_out(event,in_port,buffer_id,data,[action]):
                yield m

    def packet_out(self,event,in_port,buffer_id,data,actions = None):
        
        packetout = self.ofp_parser.ofp_packet_out()
        packetout.buffer_id = buffer_id
        packetout.in_port = in_port

        buffer_data = b''
        if buffer_id == self.ofp_parser.OFP_NO_BUFFER:
            buffer_data = data

        packetout.data = buffer_data
        packetout.actions.extend(actions)
        self.logger.info("packet out data = %r",buffer_data)
        for m  in self.ofp_proto.batch([packetout],self.connection,self):
            yield m

        for reply in self.openflow_reply:
            self.logger.info('packet_out error %s',reply)

    def get_vlan_id(self,port_no):

        for port_no in self.ports:
            return self.ports[port_no].vlan_id            

    def portStatus_handler(self,event):
        if None:
            yield
    def add_port(self,port):

        self.ports.setdefault(port.port_no,port)
       
        """
        vlan = get_port_vlan(self.dpid,port_no)
        if vlan != None && vlan <= 4095 :
            self.set_port_vlan(port,vlan)
        """
        for m in self.set_port_vlan(port,port.vlan_id):
            yield m
    
    def get_port_mac(self,port):
        return self.ports[port].port_hw

    def set_port_vlan(self,port,vlan_id):
        
        self.logger.info('set port vlan %r',vlan_id)
        # it means trunk port
        if vlan_id == 0:
            # add flow allow it pass , ingore vlan_id , priority 100
            match = self.ofp_parser.ofp_match_oxm()
            oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_IN_PORT,port.port_no)
            match.oxm_fields.append(oxm)
            action = self.ofp_parser.nx_action_resubmit(in_port = self.ofp_parser.OFPP_IN_PORT & 0xffff,table= PREROUTERING_TABLE_ID)
            ins = self.ofp_parser.ofp_instruction_actions(type=self.ofp_parser.OFPIT_APPLY_ACTIONS)
            ins.actions.append(action) 
            
            for m in self.add_flow(table_id = TAG_TABLE_ID,match = match ,ins = [ins],priority = 100,buffer_id = self.ofp_parser.OFP_NO_BUFFER):
                yield m

            # add flow output operate, trunk , output port

            match = self.ofp_parser.ofp_match_oxm()
            oxm = self.ofp_parser.create_oxm(self.ofp_parser.NXM_NX_REG0,port.port_no)
            match.oxm_fields.append(oxm)
            
            action = self.ofp_parser.ofp_action_output(port = port.port_no)
            ins = self.ofp_parser.ofp_instruction_actions(type=self.ofp_parser.OFPIT_APPLY_ACTIONS)
            ins.actions.append(action)

            for m in self.add_flow(table_id = UNTAG_TABLE_ID,match = match, ins = [ins],priority = 100,buffer_id = self.ofp_parser.OFP_NO_BUFFER):
                yield m

        else :
            # it is access port , allow no vlan tag pass , action add tag
            match = self.ofp_parser.ofp_match_oxm()
            oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_VLAN_VID,self.ofp_parser.OFP_VLAN_NONE)
            match.oxm_fields.append(oxm)
            oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_IN_PORT,port.port_no)
            match.oxm_fields.append(oxm)
            
            ins = self.ofp_parser.ofp_instruction_actions(type=self.ofp_parser.OFPIT_APPLY_ACTIONS)
            action = self.ofp_parser.ofp_action_push(ethertype=ETHERTYPE_8021Q)
            ins.actions.append(action)
            
            set_field_oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_VLAN_VID, vlan_id | self.ofp_parser.OFPVID_PRESENT)
            action = self.ofp_parser.ofp_action_set_field(field = set_field_oxm)
            ins.actions.append(action)
            
            # here actions list apply in order install,so resubmit, goto actions should install last 
            action = self.ofp_parser.nx_action_resubmit(in_port = self.ofp_parser.OFPP_IN_PORT & 0xffff,table = PREROUTERING_TABLE_ID)
            ins.actions.append(action)
           
            for m in self.add_flow(table_id = TAG_TABLE_ID,match = match, ins = [ins],priority = 100,buffer_id = self.ofp_parser.OFP_NO_BUFFER):
                yield m

            # add flow output operate , access,untag,output port ,
            match = self.ofp_parser.ofp_match_oxm()
            oxm = self.ofp_parser.create_oxm(self.ofp_parser.NXM_NX_REG0,port.port_no)
            match.oxm_fields.append(oxm)
            
            #
            # match vlan exist with any value 
            #
            oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_VLAN_VID_W,self.ofp_parser.OFPVID_PRESENT,self.ofp_parser.OFPVID_PRESENT)
            match.oxm_fields.append(oxm)
            
            ins = self.ofp_parser.ofp_instruction_actions(type=self.ofp_parser.OFPIT_APPLY_ACTIONS)
            action = self.ofp_parser.ofp_action(type=self.ofp_parser.OFPAT_POP_VLAN)
            ins.actions.append(action)
 
            action = self.ofp_parser.ofp_action_output(port = port.port_no)
            ins.actions.append(action)

           
            for m in self.add_flow(table_id = UNTAG_TABLE_ID,match = match,ins = [ins],priority = 100,buffer_id = self.ofp_parser.OFP_NO_BUFFER):
                yield m

        # check all port ,, set flood flow
        # 1. delete all reg0 = 0, 
        # 2. reg0 = 0, vlan_id , pop vlan, output vlan port + trunk
        vlan_group = self.group_port_by_vlan_id()

        """ 
        match = self.ofp_parser.ofp_match_oxm()
        oxm = self.ofp_parser.create_oxm(self.ofp_parser.NXM_NX_REG0,0)
        match.oxm_fields.append(oxm)
        
        ins = self.ofp_parser.ofp_instruction_actions(type=self.ofp_parser.OFPIT_APPLY_ACTIONS)
        for vlan in vlan_group:
            # this is trunk , only flood to all trunk
            if vlan == 0:
                for port in vlan_group[vlan]:
                    action = self.ofp_parser.ofp_action_output(port = port)
                    ins.actions.append(action)
            # access , match : vlan + reg0 flood to vlan + trunk , port vlan 
            else:
                oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_VLAN_VID,vlan | self.ofp_parser.OFPVID_PRESENT)
                match.oxm_fields.append(oxm)
                
                action = self.ofp_parser.ofp_action(type=self.ofp_parser.OFPAT_POP_VLAN)
                ins.actions.append(action)
                 
                try:
                    p = vlan_group[vlan].union(vlan_group[0])
                except KeyError,e:
                    p = vlan_group[vlan]
                  
                for port in p:
                    action = self.ofp_parser.ofp_action_output(port = port)
                    ins.actions.append(action)
                 
        
        for m in self.add_flow(table_id = UNTAG_TABLE_ID,match = match ,ins = [ins],priority = 100,
                                buffer_id = self.ofp_parser.OFP_NO_BUFFER):
            yield m
        """

        for vlan in vlan_group:
            if vlan == 0:
                match = self.ofp_parser.ofp_match_oxm()
                oxm = self.ofp_parser.create_oxm(self.ofp_parser.NXM_NX_REG0,0)
                match.oxm_fields.append(oxm)

                ins = self.ofp_parser.ofp_instruction_actions(type=self.ofp_parser.OFPIT_APPLY_ACTIONS)
                
                for port in vlan_group[vlan]:
                    action = self.ofp_parser.ofp_action_output(port = port)
                    ins.actions.append(action)
                for m in self.add_flow(table_id = UNTAG_TABLE_ID,match = match,ins = [ins],priority = 90,
                        buffer_id = self.ofp_parser.OFP_NO_BUFFER):
                    yield m
            else:
                match = self.ofp_parser.ofp_match_oxm()
                oxm = self.ofp_parser.create_oxm(self.ofp_parser.NXM_NX_REG0,0)
                match.oxm_fields.append(oxm)
                
                oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_VLAN_VID,vlan | self.ofp_parser.OFPVID_PRESENT)
                match.oxm_fields.append(oxm)
                ins = self.ofp_parser.ofp_instruction_actions(type=self.ofp_parser.OFPIT_APPLY_ACTIONS)
                
                try:
                    trunk_ports = vlan_group[0]
                except KeyError,e:
                    trunk_ports = ()

                for port in trunk_ports:
                    action = self.ofp_parser.ofp_action_output(port = port)
                    ins.actions.append(action)

                action = self.ofp_parser.ofp_action(type=self.ofp_parser.OFPAT_POP_VLAN)
                ins.actions.append(action)

                for port in vlan_group[vlan]:
                    action = self.ofp_parser.ofp_action_output(port = port)
                    ins.actions.append(action)

                for m in self.add_flow(table_id = UNTAG_TABLE_ID,match = match ,ins = [ins],priority = 100,
                        buffer_id = self.ofp_parser.OFP_NO_BUFFER):
                    yield m
        #
        #  every packet from public network ,, will broadcast to inner network
        #   
        #  so we add flow discard in_port == public port, reg0 == 0 
        #
        
        for port in self.ports:
            if self.ports[port].public_flag == 1:
                match = self.ofp_parser.ofp_match_oxm()
                oxm = self.ofp_parser.create_oxm(self.ofp_parser.NXM_NX_REG0,0)
                match.oxm_fields.append(oxm)

                oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_IN_PORT,port)
                match.oxm_fields.append(oxm)

                ins = self.ofp_parser.ofp_instruction_actions(type = self.ofp_parser.OFPIT_CLEAR_ACTIONS)

                for m in self.add_flow(table_id = UNTAG_TABLE_ID,match = match,ins = [ins],priority = 110,
                        buffer_id = self.ofp_parser.OFP_NO_BUFFER):
                    yield m
    def group_port_by_vlan_id(self):
        group = {}
        vlan_id = set()

        for port_no in self.ports:
            vlan_id.add(self.ports[port_no].vlan_id)
            group.setdefault(self.ports[port_no].vlan_id,set())

        for vid in vlan_id:
            for port_no in self.ports:
                if self.ports[port_no].vlan_id == vid:
                    if self.ports[port_no].public_flag != 1:    # public network , we do not brocast packet
                        group[vid].add(port_no)

        self.logger.info("group by vlan_id = %r",group)
        return group


    def add_default_flow(self):

        # tag table ,default drop
        match = self.ofp_parser.ofp_match_oxm()
        instruction = self.ofp_parser.ofp_instruction_actions(type = self.ofp_parser.OFPIT_CLEAR_ACTIONS)

        for m in self.add_flow(table_id = TAG_TABLE_ID,match = match,ins = [instruction],buffer_id = self.ofp_parser.OFP_NO_BUFFER):
            yield m
        self.logger.info('add %s tag table default flow',dpid_to_str(self.dpid))
        
        # prerouting table default table
        match = self.ofp_parser.ofp_match_oxm()
        ins = self.ofp_parser.ofp_instruction_goto_table(table_id = ROUTER_TABLE_ID)

        for m in self.add_flow(table_id = PREROUTERING_TABLE_ID,match = match,ins = [ins],buffer_id = self.ofp_parser.OFP_NO_BUFFER):
            yield m
        self.logger.info('add %s preroutering table default flow',dpid_to_str(self.dpid))
        
        # add func router table  ARP --->> ARP_TABLE_ID , priority 1000 (highest)
        match = self.ofp_parser.ofp_match_oxm()
        oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_ETH_TYPE,ETHERTYPE_ARP)
        match.oxm_fields.append(oxm)
        #action = self.ofp_parser.ofp_action_output(port=self.ofp_parser.OFPP_CONTROLLER,
                            #max_len = self.ofp_parser.OFPCML_NO_BUFFER)
        ins = self.ofp_parser.ofp_instruction_goto_table(table_id = ARP_TABLE_ID)

        for m in self.add_flow(table_id = ROUTER_TABLE_ID,match = match ,ins = [ins],priority = 1000,buffer_id = self.ofp_parser.OFP_NO_BUFFER):
            yield m
        self.logger.info('add %s router table ARP default flow',dpid_to_str(self.dpid))

        # router table  gateway  IP + MAC will send to controller ,
        # flow will add when arp gateway,here don't do it 
        
        # add func router table default flow  l2 --- >> MAC_TABLE_ID
        match = self.ofp_parser.ofp_match_oxm()
        action = self.ofp_parser.nx_action_resubmit(in_port = self.ofp_parser.OFPP_IN_PORT & 0xffff,table = MAC_TABLE_ID)
        ins = self.ofp_parser.ofp_instruction_actions(type=self.ofp_parser.OFPIT_APPLY_ACTIONS)
        ins.actions.append(action)
        action = self.ofp_parser.nx_action_resubmit(in_port = self.ofp_parser.OFPP_IN_PORT & 0xffff,table = POSTROUTERING_TABLE_ID)
        ins.actions.append(action)
        for m in self.add_flow(table_id = ROUTER_TABLE_ID,match = match ,ins = [ins],buffer_id = self.ofp_parser.OFP_NO_BUFFER):
            yield m
        self.logger.info('add %s router table l2 default flow',dpid_to_str(self.dpid))
        
        # add arp table default flow ---->> controller
        match = self.ofp_parser.ofp_match_oxm()
        action = self.ofp_parser.ofp_action_output(port = self.ofp_parser.OFPP_CONTROLLER,
                                    max_len = self.ofp_parser.OFPCML_NO_BUFFER)
        ins = self.ofp_parser.ofp_instruction_actions(type=self.ofp_parser.OFPIT_APPLY_ACTIONS)
        ins.actions.append(action)
        for m in self.add_flow(table_id = ARP_TABLE_ID,match = match,ins = [ins],buffer_id = self.ofp_parser.OFP_NO_BUFFER):
            yield m
        self.logger.info('add %s arp table  default flow',dpid_to_str(self.dpid))
        
        # add ip table default flow ---->> drop
        match = self.ofp_parser.ofp_match_oxm()
        ins = self.ofp_parser.ofp_instruction_actions(type=self.ofp_parser.OFPIT_CLEAR_ACTIONS)

        for m in self.add_flow(table_id = IP_TABLE_ID, match = match,ins = [ins],buffer_id = self.ofp_parser.OFP_NO_BUFFER):
            yield m
        self.logger.info('add %s ip table default flow',dpid_to_str(self.dpid))

        # add postroutering table id default flow --->> UNTAG_TABLE_ID
        match = self.ofp_parser.ofp_match_oxm()
        ins = self.ofp_parser.ofp_instruction_goto_table(table_id = UNTAG_TABLE_ID)
        for m in self.add_flow(table_id = POSTROUTERING_TABLE_ID,match = match ,ins = [ins],buffer_id = self.ofp_parser.OFP_NO_BUFFER):
            yield m
        self.logger.info('add %s postroutering table  default flow',dpid_to_str(self.dpid))
        
        # add untag table default flow --- drop
        match = self.ofp_parser.ofp_match_oxm()
        ins = self.ofp_parser.ofp_instruction_actions(type=self.ofp_parser.OFPIT_CLEAR_ACTIONS)
        for m in self.add_flow(table_id = UNTAG_TABLE_ID,match = match,ins = [ins],buffer_id = self.ofp_parser.OFP_NO_BUFFER):
            yield m
        self.logger.info('add %s untag table  default flow',dpid_to_str(self.dpid))
        
    def add_flow(self,cookie = 0,cookie_mask = 0,table_id = 0,priority = 0,idle_time = 0,
                    hard_time = 0,match = None,ins = None,buffer_id = None):
        
        assert match != None
        assert ins != None
        assert buffer_id != None

        flowRequest = self.ofp_parser.ofp_flow_mod()
        flowRequest.table_id = table_id
        flowRequest.command = self.ofp_parser.OFPFC_ADD
        flowRequest.priority = priority
        flowRequest.cookie = cookie
        flowRequest.cookie_mask = cookie_mask
        flowRequest.idle_timeout = idle_time
        flowRequest.hard_timeout = hard_time
        flowRequest.buffer_id = buffer_id
        flowRequest.flags = self.ofp_parser.OFPFF_SEND_FLOW_REM
        flowRequest.match = match
        for instu in ins:
            flowRequest.instructions.append(instu)

        for m in self.ofp_proto.batch([flowRequest],self.connection,self):
            yield m

        for reply in self.openflow_reply:
            #if reply['type'] == common.OFPET_BAD_REQUEST:
            self.logger.info(' set flow error info %r',common.dump(self.openflow_reply))

class portinfo():
    def __init__(self,port_no,port_name,port_hw,vlan_id = 0,flag = 0):
        self.port_no = port_no
        self.port_name = port_name
        self.port_hw = port_hw
        self.vlan_id = vlan_id
        self.public_flag = flag

class routerItem():
    def __init__(self,network,gateway):
        self.network = network
        self.gateway = gateway

def dpid_to_str(dpid):
        return '%016x' % dpid

def get_portno_from_event(event):

    for oxm in event.message.match.oxm_fields:
        if oxm.header == event.connection.openflowdef.OXM_OF_IN_PORT:
            str_port = ''.join('%d' % n for n in bytearray(oxm.value))
            return int(str_port)

def format_mac_to_string(mac):
        
    return ':'.join('%02x' % n for n in mac)

def check_address_in_network(address,networkList):
    
    for network in networkList:
        if address in network:
            return True
    return False

def get_gateway_in_network(address,networkList):
    for network in networkList:
        if address in network:
            return network.ip

def is_gateway(address,networkList):

    for network in networkList:
        if address == network.ip:
            return True

    return False

def checksum(data):
    if len(data) % 2:
        data += b'\x00'

    s = sum(array.array('H',data))
    s = (s & 0xffff) + (s >> 16)
    s += (s >> 16)
    
    return socket.ntohs(~s & 0xffff)
if __name__ == '__main__':
    manager['module.OpenflowServer.urls'] = ['ltcp://127.0.0.1:6653']
    #manager['server.debugging'] = True
    manager['server.loggingconfig'] = ['log.conf']
    main(None,())

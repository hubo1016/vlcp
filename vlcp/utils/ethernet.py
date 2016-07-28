'''
Created on 2015/7/30

:author: hubo
'''

from namedstruct import *
from namedstruct.namedstruct import rawtype as _rawtype
import array
import socket

ethertype = enum('ethertype', globals(), uint16,
        ETHERTYPE_LOOP      = 0x0060,         #  Ethernet Loopback packet     
        ETHERTYPE_PUP       = 0x0200,         #  Xerox PUP packet             
        ETHERTYPE_PUPAT     = 0x0201,         #  Xerox PUP Addr Trans packet  
        ETHERTYPE_IP        = 0x0800,         #  Internet Protocol packet     
        ETHERTYPE_X25       = 0x0805,         #  CCITT X.25                   
        ETHERTYPE_ARP       = 0x0806,         #  Address Resolution packet    
        ETHERTYPE_BPQ       = 0x08FF,         #  G8BPQ AX.25 Ethernet Packet  [ NOT AN OFFICIALLY REGISTERED ID ] 
        ETHERTYPE_IEEEPUP   = 0x0a00,         #  Xerox IEEE802.3 PUP packet 
        ETHERTYPE_IEEEPUPAT = 0x0a01,         #  Xerox IEEE802.3 PUP Addr Trans packet 
        ETHERTYPE_DEC       = 0x6000,         #  DEC Assigned proto           
        ETHERTYPE_DNA_DL    = 0x6001,         #  DEC DNA Dump/Load            
        ETHERTYPE_DNA_RC    = 0x6002,         #  DEC DNA Remote Console       
        ETHERTYPE_DNA_RT    = 0x6003,         #  DEC DNA Routing              
        ETHERTYPE_LAT       = 0x6004,         #  DEC LAT                      
        ETHERTYPE_DIAG      = 0x6005,         #  DEC Diagnostics              
        ETHERTYPE_CUST      = 0x6006,         #  DEC Customer use             
        ETHERTYPE_SCA       = 0x6007,         #  DEC Systems Comms Arch       
        ETHERTYPE_TEB       = 0x6558,         #  Trans Ether Bridging         
        ETHERTYPE_RARP      = 0x8035,         #  Reverse Addr Res packet      
        ETHERTYPE_ATALK     = 0x809B,         #  Appletalk DDP                
        ETHERTYPE_AARP      = 0x80F3,         #  Appletalk AARP               
        ETHERTYPE_8021Q     = 0x8100,         #  802.1Q VLAN Extended Header  
        ETHERTYPE_IPX       = 0x8137,         #  IPX over DIX                 
        ETHERTYPE_IPV6      = 0x86DD,         #  IPv6 over bluebook           
        ETHERTYPE_PAUSE     = 0x8808,         #  IEEE Pause frames. See 802.3 31B 
        ETHERTYPE_SLOW      = 0x8809,         #  Slow Protocol. See 802.3ad 43B 
        ETHERTYPE_WCCP      = 0x883E,         #  Web-cache coordination protocol defined in draft-wilson-wrec-wccp-v2-00.txt 
        ETHERTYPE_PPP_DISC  = 0x8863,         #  PPPoE discovery messages     
        ETHERTYPE_PPP_SES   = 0x8864,         #  PPPoE session messages       
        ETHERTYPE_MPLS_UC   = 0x8847,         #  MPLS Unicast traffic         
        ETHERTYPE_MPLS_MC   = 0x8848,         #  MPLS Multicast traffic       
        ETHERTYPE_ATMMPOA   = 0x884c,         #  MultiProtocol Over ATM       
        ETHERTYPE_ATMFATE   = 0x8884,         #  Frame-based ATM Transport over Ethernet 
        ETHERTYPE_PAE       = 0x888E,         #  Port Access Entity (IEEE 802.1X) 
        ETHERTYPE_AOE       = 0x88A2,         #  ATA over Ethernet            
        ETHERTYPE_8021QS    = 0x88A8,         #  8021.Q Server VLAN Extended Header 
        ETHERTYPE_TIPC      = 0x88CA,         #  TIPC                         
        ETHERTYPE_1588      = 0x88F7,         #  IEEE 1588 Timesync 
        ETHERTYPE_FCOE      = 0x8906,         #  Fibre Channel over Ethernet  
        ETHERTYPE_TDLS      = 0x890D,         #  TDLS 
        ETHERTYPE_FIP       = 0x8914,         #  FCoE Initialization Protocol 
        ETHERTYPE_EDSA      = 0xDADA,         #  Ethertype DSA [ NOT AN OFFICIALLY REGISTERED ID ] 
        ETHERTYPE_AF_IUCV   = 0xFBFB          #  IBM af_iucv [ NOT AN OFFICIALLY REGISTERED ID ] 
)

arp_op_code = enum('arp_op_code', globals(), uint16,
        ARPOP_REQUEST   = 1,               #  ARP request.  
        ARPOP_REPLY     = 2,               #  ARP reply.  
        ARPOP_RREQUEST  = 3,               #  RARP request.  
        ARPOP_RREPLY    = 4,               #  RARP reply.  
        ARPOP_InREQUEST = 8,               #  InARP request.  
        ARPOP_InREPLY   = 9,               #  InARP reply.  
        ARPOP_NAK       = 10               #  (ATM)ARP NAK.  
)

ip_protocol = enum('ip_protocol', globals(), uint8, False,
    IPPROTO_IP = 0,             #  Dummy protocol for TCP               
    IPPROTO_ICMP = 1,           #  Internet Control Message Protocol    
    IPPROTO_IGMP = 2,           #  Internet Group Management Protocol   
    IPPROTO_IPIP = 4,           #  IPIP tunnels (older KA9Q tunnels use 94) 
    IPPROTO_TCP = 6,            #  Transmission Control Protocol        
    IPPROTO_EGP = 8,            #  Exterior Gateway Protocol            
    IPPROTO_PUP = 12,           #  PUP protocol                         
    IPPROTO_UDP = 17,           #  User Datagram Protocol               
    IPPROTO_IDP = 22,           #  XNS IDP protocol                     
    IPPROTO_DCCP = 33,          #  Datagram Congestion Control Protocol 
    IPPROTO_RSVP = 46,          #  RSVP protocol                        
    IPPROTO_GRE = 47,           #  Cisco GRE tunnels (rfc 1701,1702)    
    IPPROTO_IPV6   = 41,        #  IPv6-in-IPv4 tunnelling              
    IPPROTO_ESP = 50,          #  Encapsulation Security Payload protocol 
    IPPROTO_AH = 51,           #  Authentication Header protocol       
    IPPROTO_BEETPH = 94,       #  IP option pseudo header for BEET 
    IPPROTO_PIM    = 103,       #  Protocol Independent Multicast       
    IPPROTO_COMP   = 108,              #  Compression Header protocol 
    IPPROTO_SCTP   = 132,       #  Stream Control Transport Protocol    
    IPPROTO_UDPLITE = 136,      #  UDP-Lite (RFC 3828)                  
    IPPROTO_RAW    = 255        #  Raw IP packets                       
)


ETH_ALEN = 6

mac_addr = uint8[ETH_ALEN]

mac_addr.formatter = lambda x: ':'.join('%02x' % (n,) for n in x)
def _create_mac_addr(addr = None):
    if addr is None:
        return [0] * ETH_ALEN
    else:
        mac_array = [int(v,16) for v in addr.split(':')]
        return (mac_array + [0] * (ETH_ALEN - len(mac_array)))[:ETH_ALEN]

mac_addr.new = _create_mac_addr

mac_addr_bytes = prim(str(ETH_ALEN) + 's', 'mac_addr_bytes')

mac_addr_bytes.formatter = lambda x: ':'.join('%02x' % (c,) for c in bytearray(x.ljust(6, b'\x00')))
def _create_mac_addr_bytes(addr = None):
    if addr is None:
        return b'\x00' * ETH_ALEN
    else:
        return mac_addr.tobytes(mac_addr(addr))

mac_addr_bytes.new = _create_mac_addr_bytes

ip4_addr = prim('I', 'ip4_addr')

import socket as _socket

ip4_addr.formatter = lambda x: _socket.inet_ntoa(ip4_addr.tobytes(x))
def _ip4_addr_new(ip_addr = None):
    if ip_addr is None:
        return 0
    else:
        return ip4_addr.create(_socket.inet_aton(ip_addr))
ip4_addr.new = _ip4_addr_new

ip4_addr_bytes = prim('4s', 'ip4_addr_bytes')
ip4_addr_bytes.formatter = lambda x: _socket.inet_ntoa(x.ljust(4, b'\x00'))
def _ip4_addr_bytes_new(ip_addr = None):
    if ip_addr is None:
        return b'\x00\x00\x00\x00'
    else:
        return _socket.inet_aton(ip_addr)
ip4_addr_bytes.new = _ip4_addr_bytes_new

ip6_addr = uint8[16]
if hasattr(_socket, 'inet_ntop'):
    ip6_addr.formatter = lambda x: _socket.inet_ntop(_socket.AF_INET6, ip6_addr.tobytes(x.ljust(16, b'\x00')))
if hasattr(_socket, 'inet_pton'):
    def _ip6_addr_new(ip_addr = None):
        if ip_addr is None:
            return [0] * 16
        else:
            return ip6_addr.create(_socket.inet_pton(_socket.AF_INET6, ip_addr))
else:
    def _ip6_addr_new(ip_addr = None):
        if ip_addr is None:
            return [0] * 16
        else:
            fp, sep, lp = ip_addr.partition('::')
            if not sep:
                return [int(v,16) for v in fp.split(':')]
            else:
                fp_l = [int(v,16) for v in fp.split(':')]
                lp_l = [int(v,16) for v in lp.split(':')]
                if len(fp_l) + len(lp_l) > 8:
                    raise ValueError('Illegal IPv6 address')
                else:
                    return fp_l + [0] * (8 - len(fp_l) - len(lp_l)) + lp_l
ip6_addr.new = _ip6_addr_new
    
ip6_addr_bytes = prim('16s', 'ip6_addr')

if hasattr(_socket, 'inet_ntop'):
    ip6_addr.formatter = lambda x: _socket.inet_ntop(_socket.AF_INET6, x)
if hasattr(_socket, 'inet_pton'):
    def _ip6_addr_bytes_new(ip_addr = None):
        if ip_addr is None:
            return b'\x00' * 16
        else:
            return _socket.inet_pton(_socket.AF_INET6, ip_addr)
else:
    def _ip6_addr_bytes_new(ip_addr = None):
        if ip_addr is None:
            return b'\x00' * 16
        else:
            return ip6_addr.tobytes(_ip6_addr_new(ip_addr))
ip6_addr_bytes.new = _ip6_addr_bytes_new

ethernet_l2 = nstruct((mac_addr,'dl_dst'),
                         (mac_addr,'dl_src'),
                         (ethertype,'dl_type'),
                         name='ethernet_l2',
                         padding = 1,
                         size = lambda x: 18 if x.dl_type == ETHERTYPE_8021Q else
                                          22 if x.dl_type == ETHERTYPE_8021QS else
                                          14
                )

ethernet_l2_8021q = nstruct((uint16, 'vlan_tci'),
                                (ethertype, 'dl_type2'),
                                base = ethernet_l2,
                                name = "ethernet_l2_8021q",
                                criteria = lambda x: x.dl_type == ETHERTYPE_8021Q,
                                init = packvalue(ETHERTYPE_8021Q, 'dl_type')
                                )

ethernet_l2_8021ad = nstruct((uint16, 'svlan_tci'),
                             (ethertype, 'dl_type3'),
                             (uint16, 'vlan_tci'),
                             (ethertype, 'dl_type2'),
                             base = ethernet_l2,
                             name = 'ethernet_l2_8021ad',
                             criteria = lambda x: x.dl_type == ETHERTYPE_8021QS,
                             init = lambda x: (packvalue(ETHERTYPE_8021QS, 'dl_type')(x),
                                             packvalue(ETHERTYPE_8021Q, 'dl_type3')(x))
                             )

def dl_type(packet):
    if packet.dl_type == ETHERTYPE_8021Q:
        return packet.dl_type2
    else:
        return packet.dl_type

def vlan_vid(packet):
    if packet.dl_type == ETHERTYPE_8021Q:
        return (packet.vlan_tci & 0xfff) | 0x1000
    else:
        return 0

def vlan_pcp(packet):
    if packet.dl_type == ETHERTYPE_8021Q:
        return (packet.vlan_tci >> 13)
    else:
        return 0

def create_vlan_tci(vid, pcp = 0):
    return ((pcp & 0x7) << 13) | 0x1000 | (vid & 0xfff)


ethernet_l3 = nstruct((ethernet_l2,),
                      name = "ethernet_l3",
                      padding = 1,
                      classifier = lambda x: x.dl_type2 if x.dl_type == ETHERTYPE_8021Q or x.dl_type == ETHERTYPE_8021QS else x.dl_type)

arp_payload = nstruct(
        (uint16,'arp_hwtype'),
        (uint16,'arp_proto'),
        (uint8,'hw_len'),
        (uint8,'proto_len'),
        (arp_op_code,'arp_op'),
        (mac_addr,'arp_sha'),
        (ip4_addr,'arp_spa'),
        (mac_addr,'arp_tha'),
        (ip4_addr,'arp_tpa'),
        name = 'arp_payload',
        padding = 1,
        init = lambda x: (packvalue(1, 'arp_hwtype')(x),
                packvalue(ETHERTYPE_IP, 'arp_proto')(x),
                packvalue(6, 'hw_len')(x),
                packvalue(4, 'proto_len')(x))
        )

def create_packet(packettype, vid = None, pcp = 0, svid = None, spcp = 0, *args, **kwargs):
    if svid is not None:
        dl_type = packettype().dl_type
        return packettype((ethernet_l2, ethernet_l2_8021ad), *args,
                          dl_type2 = dl_type, vlan_tci = create_vlan_tci(vid, pcp),
                          svlan_tci = create_vlan_tci(svid, spcp),
                          **kwargs)
    elif vid is not None:
        dl_type = packettype().dl_type
        return packettype((ethernet_l2, ethernet_l2_8021q), *args,
                          dl_type2 = dl_type, vlan_tci = create_vlan_tci(vid, pcp),
                          **kwargs)
    else:
        return packettype(*args, **kwargs)
        
arp_packet = nstruct((arp_payload,),
                     base = ethernet_l3,
                     classifyby = (ETHERTYPE_ARP,),
                     name = 'arp_packet',
                     init = packvalue(ETHERTYPE_ARP, 'dl_type')
                     )

def checksum(data):
    if len(data) % 2:
        data += b'\x00'
    s = sum(array.array('H',data))
    s = (s & 0xffff) + (s >> 16)
    s += (s >> 16)
    return _socket.ntohs(~s & 0xffff)

def checksum2(*checksums):
    s = sum(checksums)
    s = (s & 0xffff) + (s >> 16)
    s += (s >> 16)
    return (s & 0xffff)

def ip4_checksum(x):
    data = x._tobytes(True)
    return checksum(data)

def _prepack_ip4_header(x):
    size = len(x)
    if size > 60:
        raise ValueError('IP header is too large')
    x.version_length = 0x40 | (size // 4)

ip_tos = enum('ip_tos', globals(), uint8, True,
                IPTOS_ECN_ECT1 = 0x01,
                IPTOS_ECN_ECT0 = 0x02,
                IPTOS_ECN_CE = 0x03,
                IPTOS_DSCP_AF11 =       0x28,
                IPTOS_DSCP_AF12 =       0x30,
                IPTOS_DSCP_AF13 =       0x38,
                IPTOS_DSCP_AF21 =       0x48,
                IPTOS_DSCP_AF22 =       0x50,
                IPTOS_DSCP_AF23 =       0x58,
                IPTOS_DSCP_AF31 =       0x68,
                IPTOS_DSCP_AF32 =       0x70,
                IPTOS_DSCP_AF33 =       0x78,
                IPTOS_DSCP_AF41 =       0x88,
                IPTOS_DSCP_AF42 =       0x90,
                IPTOS_DSCP_AF43 =       0x98,
                IPTOS_DSCP_EF   =       0xb8,
                IPTOS_CLASS_CS1 =       0x20,
                IPTOS_CLASS_CS2 =       0x40,
                IPTOS_CLASS_CS3 =       0x60,
                IPTOS_CLASS_CS4 =       0x80,
                IPTOS_CLASS_CS5 =       0xa0,
                IPTOS_CLASS_CS6 =       0xc0,
                IPTOS_CLASS_CS7 =       0xe0
              )

IPTOS_ECN_NOT_ECT = 0x00
IPTOS_ECN_MASK = 0x03
IPTOS_DSCP_MASK = 0xfc
IPTOS_CLASS_MASK = 0xe0
IPTOS_CLASS_CS0 = 0x00

ip_frag_offset = enum("ip_frag_offset", globals(), uint16, True,
                         IP_RF = 0x8000,
                         IP_DF = 0x4000,
                         IP_MF = 0x2000
                         )
IP_OFFMASK = 0x1fff

IP_FRAG_ANY   = (1 << 0) #  Is this a fragment? 
IP_FRAG_LATER = (1 << 1) #  Is this a fragment with nonzero offset? 

def ip_frag(packet):
    '''
    Not fragments:
        ip_frag(packet) == 0
        not ip_frag(packet)
        
    First packet of fragments:
        ip_frag(packet) == IP_FRAG_ANY
    
    Not first packet of fragments:
        ip_frag(packet) & IP_FRAG_LATER
    
    All fragments:
        ip_frag(packet) & IP_FRAG_ANY

    '''
    return ((packet.frag_off & IP_OFFMASK) and IP_FRAG_LATER) | ((packet.frag_off & (IP_OFFMASK | IP_MF)) and IP_FRAG_ANY)

ip4_header = nstruct(
        # 4bit version + 4bit length
        (uint8,'version_length'), 
        (ip_tos,'tos'),
        (uint16,'total_len'),
        (uint16,'identifier'),
        # 3bit frag + 13bit offset
        (ip_frag_offset,'frag_off'),
        (uint8,'ttl'),
        (ip_protocol,'proto'),
        (uint16,'checksum'),
        (ip4_addr,'ip_src'),
        (ip4_addr,'ip_dst'),
        (raw, 'options'),
        padding = 4,
        name = 'ip4_header',
        size = lambda x: (x.version_length & 0xf) * 4,
        prepack = _prepack_ip4_header,
        init = packvalue(0x45, 'version_length')
        )

def calcuate_ip4_checksum(x):
    x.checksum = 0
    x.checksum = ip4_checksum(x._get_embedded(ip4_header)) or 0xffff

def _prepack_ip4_payload(x):
    x.total_len = x._realsize()
    calcuate_ip4_checksum(x)

ip4_packet = nstruct((ip4_header,),
                     (raw, 'payload'),
                     base = ethernet_l3,
                     classifyby = (ETHERTYPE_IP,),
                     name = 'ip4_packet',
                     init = packvalue(ETHERTYPE_IP, 'dl_type'),
                     prepack = calcuate_ip4_checksum
                     )

# IP Packet without an Ethernet header
ip4_payload = nstruct((ip4_header,),
                      name = "ip4_payload",
                      padding = 1,
                      size = lambda x: x.total_len,
                      prepack = _prepack_ip4_payload
                      )

ip4_partial_payload = nstruct((ip4_header,),
                      name = "ip4_partial_payload",
                      padding = 1,
                      prepack = calcuate_ip4_checksum
                      )

ip4_payload_fragment = nstruct((raw, 'payload'),
                               base = ip4_payload,
                               criteria = lambda x: ip_frag(x) & IP_FRAG_LATER,
                               name = "ip4_payload_fragment"
                               )

ip4_partial_payload_fragment = nstruct((raw, 'payload'),
                               base = ip4_partial_payload,
                               criteria = lambda x: ip_frag(x) & IP_FRAG_LATER,
                               name = "ip4_partial_payload_fragment"
                               )

tcp_header_min = nstruct((uint16, 'sport'),
                          (uint16, 'dport'),
                          (uint32, 'seq'),
                          name = 'tcp_header_min',
                          padding = 1
                          )

tcp_flags = enum('tcp_flags', globals(), uint8, True,
                    TH_FIN        = 0x01,
                    TH_SYN        = 0x02,
                    TH_RST        = 0x04,
                    TH_PUSH       = 0x08,
                    TH_ACK        = 0x10,
                    TH_URG        = 0x20
                   )

def _pack_tcp_header_size(x):
    x.data_offset = ((len(x) // 4) << 4)

tcp_option_kind = enum('tcp_option_kind', globals(), uint8,
                       TCPOPT_EOL = 0,
                       TCPOPT_NOP = 1,
                       TCPOPT_MAXSEG = 2,
                       TCPOPT_WINDOW = 3,
                       TCPOPT_SACK_PERMITTED = 4,
                       TCPOPT_SACK = 5,
                       TCPOPT_TIMESTAMP = 8
                       )

_tcp_option_header = nstruct((tcp_option_kind, 'kind'),
                            name = "tcp_option_header",
                            padding = 1,
                            size = lambda x: 1 if (x.kind == TCPOPT_EOL or x.kind == TCPOPT_NOP) else 2
                            )

_tcp_option_header_length = nstruct((uint8, 'length'),
                                    name = "tcp_option_header_length",
                                    base = _tcp_option_header,
                                    criteria = lambda x: x.kind != TCPOPT_EOL and x.kind != TCPOPT_NOP
                                    )

def _prepack_tcp_option(x):
    s = x._realsize()
    if s > 1:
        x.length = s

tcp_option = nstruct((_tcp_option_header,),
                     name = "tcp_option",
                     padding = 1,
                     size = lambda x: 1 if (x.kind == TCPOPT_EOL or x.kind == TCPOPT_NOP) else x.length,
                     prepack = _prepack_tcp_option
                     )

def _init_tcp_option(kind):
    def _init_func(x):
        x._replace_embedded_type(_tcp_option_header, _tcp_option_header_length)
        x.kind = kind
    return _init_func

tcp_option_maxseg = nstruct((uint16, 'mss'),
                            name = 'tcp_option_maxseg',
                            base = tcp_option,
                            criteria = lambda x: x.kind == TCPOPT_MAXSEG,
                            init = _init_tcp_option(TCPOPT_MAXSEG)
                            )

tcp_option_window = nstruct((uint8, 'shift_cnt'),
                            name = 'tcp_option_window',
                            base = tcp_option,
                            criteria = lambda x: x.kind == TCPOPT_WINDOW,
                            init = _init_tcp_option(TCPOPT_WINDOW)
                            )

tcp_option_sack = nstruct((uint32[2][0], 'edges'),
                          name = 'tcp_option_sack',
                          base = tcp_option,
                          criteria = lambda x: x.kind == TCPOPT_SACK,
                          init = _init_tcp_option(TCPOPT_SACK)
                          )

tcp_option_timestamp = nstruct((uint32, 'tsval'),
                               (uint32, 'tsecr'),
                               name = 'tcp_option_timestamp',
                               base = tcp_option,
                               criteria = lambda x: x.kind == TCPOPT_TIMESTAMP,
                               init = _init_tcp_option(TCPOPT_TIMESTAMP))

def _format_tcp_options(x):
    # Parse tcp options
    options = tcp_option[0].create(x)
    result = []
    for o in options:
        od = dump(o, dumpextra = True, typeinfo = DUMPTYPE_NONE)
        anyvalue = False
        for k,v in od.items():
            if k not in ('kind', 'length') and not k.startswith('_'):
                result.append(k + ': ' + repr(v))
                anyvalue = True
        if '_extra' in od:
            result.append(str(od['kind']) + ': ' + repr(od['_extra']))
            anyvalue = True
        if not anyvalue and o.kind != TCPOPT_EOL and o.kind != TCPOPT_NOP:
            result.append(od['kind'])
    return ', '.join(result)

_tcp_option_raw = _rawtype()
_tcp_option_raw.formatter = _format_tcp_options

tcp_header = nstruct((tcp_header_min,),
                      (uint32, 'ack'),
                      (uint8, 'data_offset'),
                      (tcp_flags, 'tcp_flags'),
                      (uint16, 'tcp_win'),
                      (uint16, 'tcp_sum'),
                      (uint16, 'tcp_urp'),
                      (_tcp_option_raw, 'tcp_options'),
                      name = 'tcp_header',
                      padding = 4,
                      size = lambda x: (x.data_offset >> 4) * 4,
                      prepack = _pack_tcp_header_size
                      )

tcp_payload = nstruct((tcp_header,),
                      (raw, 'data'),
                      name = "tcp_payload",
                      padding = 1
                      )

ip4_pseudo_header = nstruct((ip4_addr, 'ip_src'),
                            (ip4_addr, 'ip_dst'),
                            (uint8,),
                            (ip_protocol, 'protocol'),
                            (uint16, 'length'),
                            name = "ip4_pseudo_header",
                            padding = 1
                            )

def tp4_checksum(payload, src, dst, protocol = IPPROTO_TCP):
    if hasattr(payload, '_tobytes'):
        data = payload._tobytes(True)
    else:
        data = payload
    return checksum2(checksum(ip4_pseudo_header(ip_src = src, ip_dst = dst, length = len(data), protocol = protocol)._tobytes()), checksum(data))

def _prepack_tcp4_payload(x):
    x.tcp_sum = 0
    x.tcp_sum = tp4_checksum(x, x.ip_src, x.ip_dst, IPPROTO_TCP) or 0xffff
    
_tcp_payload_ip4 = nstruct((tcp_payload,),
                           name = "_tcp_payload_ip4",
                           prepack = _prepack_tcp4_payload,
                           padding = 1,
                           lastextra = True
                           )

ip4_tcp_payload = nstruct((_tcp_payload_ip4,),
                            name = "ip4_tcp_payload",
                            base = ip4_payload,
                            criteria = lambda x: not ip_frag(x) and x.proto == IPPROTO_TCP,
                            lastextra = True,
                            init = packvalue(IPPROTO_TCP, 'proto'),
                          )

tcp_bestparse = nstruct((tcp_header_min,),
                       name = "tcp_bestparse",
                       padding = 1)

tcp_bestparse_min = nstruct((raw, 'otherpayload'),
                            base = tcp_bestparse,
                            name = "tcp_bestparse_min",
                            criteria = lambda x: x._realsize() < 20
                            )

tcp_bestparse_header = nstruct((uint32, 'ack'),
                      (uint8, 'data_offset'),
                      (tcp_flags, 'tcp_flags'),
                      (uint16, 'tcp_win'),
                      (uint16, 'tcp_sum'),
                      (uint16, 'tcp_urp'),
                      base = tcp_bestparse,
                      name = "tcp_bestparse_header",
                      criteria = lambda x: x._realsize() >= 20,
                      init = packvalue(5 << 4, 'data_offset')
                      )

tcp_bestparse_partial = nstruct((raw, 'otherpayload'),
                                base = tcp_bestparse_header,
                                name = "tcp_bestparse_partial",
                                criteria = lambda x: x._realsize() < (x.data_offset >> 4) * 4
                                )

_tcp_bestparse_full_options = nstruct((_tcp_option_raw, 'tcp_options'),
                                      name = "_tcp_bestparse_full_options",
                                      size = lambda x: max((x.data_offset >> 4) * 4 - 20, 0),
                                      padding = 4,
                                      prepack = packexpr(lambda x: ((len(x) // 4) << 4) + 5, 'data_offset')
                                      )

tcp_bestparse_full = nstruct((_tcp_bestparse_full_options,),
                             (raw, 'data'),
                                base = tcp_bestparse_header,
                                name = "tcp_bestparse_full",
                                criteria = lambda x: x._realsize() >= (x.data_offset >> 4) * 4
                                )

ip4_tcp_fragment_payload = nstruct((tcp_bestparse,),
                                   name = "ip4_tcp_fragment_payload",
                                   base = ip4_payload,
                                   criteria = lambda x: ip_frag(x) == IP_FRAG_ANY and x.proto == IPPROTO_TCP,
                                   init = packvalue(IPPROTO_TCP, 'proto'),
                                   lastextra = True
                                   )

ip4_tcp_partial_payload = nstruct((tcp_bestparse,),
                                   name = "ip4_tcp_partial_payload",
                                   base = ip4_partial_payload,
                                   criteria = lambda x: not (ip_frag(x) & IP_FRAG_LATER) and x.proto == IPPROTO_TCP,
                                   init = packvalue(IPPROTO_TCP, 'proto'),
                                   lastextra = True
                                   )

udp_header = nstruct((uint16, 'sport'),
                     (uint16, 'dport'),
                     (uint16, 'udp_len'),
                     (uint16, 'udp_sum'),
                     name = "udp_header",
                     padding = 1
                     )

udp_payload = nstruct((udp_header,),
                      (raw, 'data'),
                      name = "udp_payload",
                      padding = 1,
                      prepack = packrealsize('udp_len')
                      )

def _prepack_udp4_payload(x):
    x.udp_sum = 0
    x.udp_sum = tp4_checksum(x, x.ip_src, x.ip_dst, IPPROTO_UDP) or 0xffff

_udp_payload_ip4 = nstruct((udp_payload,),
                           name = "_udp_payload_ip4",
                           prepack = _prepack_udp4_payload,
                           padding = 1,
                           lastextra = True
                           )

ip4_udp_payload = nstruct((_udp_payload_ip4,),
                            name = "ip4_udp_payload",
                            base = ip4_payload,
                            criteria = lambda x: not ip_frag(x) and x.proto == IPPROTO_UDP,
                            lastextra = True,
                            init = packvalue(IPPROTO_UDP, 'proto')
                          )

ip4_udp_fragment_payload = nstruct((udp_header,),
                                   (raw, 'data'),
                                   name = "ip4_udp_fragment_payload",
                                   base = ip4_payload,
                                   criteria = lambda x: ip_frag(x) == IP_FRAG_ANY and x.proto == IPPROTO_UDP,
                                   init = packvalue(IPPROTO_UDP, 'proto')
                                   )

ip4_udp_partial_payload = nstruct((udp_header,),
                                   (raw, 'data'),
                                   name = "ip4_udp_partial_payload",
                                   base = ip4_partial_payload,
                                   criteria = lambda x: not (ip_frag(x) & IP_FRAG_LATER) and x.proto == IPPROTO_UDP,
                                   init = packvalue(IPPROTO_UDP, 'proto')
                                   )

icmp_type = enum('icmp_type', globals(), uint8,
                    ICMP_ECHOREPLY          = 0,       #  Echo Reply                   
                    ICMP_DEST_UNREACH       = 3,       #  Destination Unreachable      
                    ICMP_SOURCE_QUENCH      = 4,       #  Source Quench                
                    ICMP_REDIRECT           = 5,       #  Redirect (change route)      
                    ICMP_ECHO               = 8,       #  Echo Request                 
                    ICMP_TIME_EXCEEDED      = 11,      #  Time Exceeded                
                    ICMP_PARAMETERPROB      = 12,      #  Parameter Problem            
                    ICMP_TIMESTAMP          = 13,      #  Timestamp Request            
                    ICMP_TIMESTAMPREPLY     = 14,      #  Timestamp Reply              
                    ICMP_INFO_REQUEST       = 15,      #  Information Request          
                    ICMP_INFO_REPLY         = 16,      #  Information Reply            
                    ICMP_ADDRESS            = 17,      #  Address Mask Request         
                    ICMP_ADDRESSREPLY       = 18       #  Address Mask Reply           
                )

icmp_code_unreach = enum('icmp_code_unreach', globals(), uint8,
                    ICMP_NET_UNREACH        = 0,       #  Network Unreachable          
                    ICMP_HOST_UNREACH       = 1,       #  Host Unreachable             
                    ICMP_PROT_UNREACH       = 2,       #  Protocol Unreachable         
                    ICMP_PORT_UNREACH       = 3,       #  Port Unreachable             
                    ICMP_FRAG_NEEDED        = 4,       #  Fragmentation Needed/DF set  
                    ICMP_SR_FAILED          = 5,       #  Source Route failed          
                    ICMP_NET_UNKNOWN        = 6,
                    ICMP_HOST_UNKNOWN       = 7,
                    ICMP_HOST_ISOLATED      = 8,
                    ICMP_NET_ANO            = 9,
                    ICMP_HOST_ANO           = 10,
                    ICMP_NET_UNR_TOS        = 11,
                    ICMP_HOST_UNR_TOS       = 12,
                    ICMP_PKT_FILTERED       = 13,      #  Packet filtered 
                    ICMP_PREC_VIOLATION     = 14,      #  Precedence violation 
                    ICMP_PREC_CUTOFF        = 15       #  Precedence cut off 
                    )

icmp_code_redirect = enum('icmp_code_redirect', globals(), uint8,
                    ICMP_REDIR_NET          = 0,       #  Redirect Net                 
                    ICMP_REDIR_HOST         = 1,       #  Redirect Host                
                    ICMP_REDIR_NETTOS       = 2,       #  Redirect Net for TOS         
                    ICMP_REDIR_HOSTTOS      = 3        #  Redirect Host for TOS        
                    )

icmp_code_te = enum('icmp_code_te', globals(), uint8,
                    ICMP_EXC_TTL            = 0,       # TTL count exceeded
                    ICMP_EXC_FRAGTIME       = 1        # Fragment Reass time exceeded
                    )

icmp_header = nstruct((icmp_type, 'icmp_type'),
                      (uint8, 'icmp_code'),
                      (uint16, 'icmp_sum'),
                      name = "icmp_header",
                      padding = 1
                      )

icmp_bestparse = nstruct((icmp_header,),
                       name = "icmp_bestparse",
                       classifier = lambda x: x.icmp_type,
                       padding = 1
                       )

icmp_unreach = nstruct((uint32,),
                       (raw, 'data'),
                        classifyby = (ICMP_DEST_UNREACH,),
                        name = "icmp_unreach",
                        base = icmp_bestparse,
                        init = packvalue(ICMP_DEST_UNREACH, 'icmp_type'),
                        extend = {"icmp_code": icmp_code_unreach}
                    )

icmp_te = nstruct((uint32,),
                (raw, 'data'),
                classifyby = (ICMP_TIME_EXCEEDED,),
                name = "icmp_te",
                base = icmp_bestparse,
                init = packvalue(ICMP_TIME_EXCEEDED, 'icmp_type'),
                extend = {"icmp_code": icmp_code_te}
            )



icmp_parameter_prob = nstruct((uint8, 'icmp_pointer'),
                             (uint8[3],),
                             (raw, 'data'),
                             classifyby = (ICMP_PARAMETERPROB,),
                             name = 'icmp_parameter_prob',
                             base = icmp_bestparse,
                             init = packvalue(ICMP_PARAMETERPROB, 'icmp_type')
                        )

icmp_source_quench = nstruct((uint32,),
                      (raw, 'data'),
                      classifyby = (ICMP_SOURCE_QUENCH,),
                      name = 'icmp_source_quench',
                      base = icmp_bestparse,
                      init = packvalue(ICMP_SOURCE_QUENCH, 'icmp_type')
                      )

icmp_redirect = nstruct((uint32, 'gateway'),
                        (raw, 'data'),
                        classifyby = (ICMP_REDIRECT,),
                        name = 'icmp_redirect',
                        base = icmp_bestparse,
                        init = packvalue(ICMP_REDIRECT, 'icmp_type'),
                        extend = {"icmp_code": icmp_code_redirect}
                        )

icmp_echo = nstruct((uint16, 'icmp_id'),
                   (uint16, 'icmp_seq'),
                   (raw, 'data'),
                   classifyby = (ICMP_ECHO, ICMP_ECHOREPLY),
                   name = "icmp_echo",
                   base = icmp_bestparse,
                   init = packvalue(ICMP_ECHO, 'icmp_type')
                   )

icmp_timestamp = nstruct((uint16, 'icmp_id'),
                         (uint16, 'icmp_seq'),
                         classifyby = (ICMP_TIMESTAMP, ICMP_TIMESTAMPREPLY),
                         name = "icmp_timestamp",
                         base = icmp_bestparse,
                         init = packvalue(ICMP_TIMESTAMP, 'icmp_type')
                         )

icmp_timestamp_partial = nstruct((raw, 'otherpayload'),
                         name = "icmp_timestamp_partial",
                         base = icmp_timestamp,
                         criteria = lambda x: x._realsize() < 20
                         )

icmp_timestamp_full = nstruct((uint32, 'orig_timestamp'),
                              (uint32, 'recv_timestamp'),
                              (uint32, 'trans_timestamp'),
                              name = "icmp_timestamp_full",
                              base = icmp_timestamp,
                              criteria = lambda x: x._realsize() >= 20
                              )

icmp_info = nstruct((uint16, 'icmp_id'),
                     (uint16, 'icmp_seq'),
                     classifyby = (ICMP_INFO_REQUEST, ICMP_INFO_REPLY),
                     name = "icmp_info",
                     base = icmp_bestparse,
                     init = packvalue(ICMP_INFO_REQUEST, 'icmp_type')
                     )

def _prepack_icmp_payload(x):
    x.icmp_sum = 0
    x.icmp_sum = checksum(x._tobytes(True)) or 0xffff

_icmp_payload_ip4 = nstruct((icmp_bestparse,),
                           name = "_icmp_payload_ip4",
                           prepack = _prepack_icmp_payload,
                           padding = 1,
                           lastextra = True
                           )

ip4_icmp_payload = nstruct((_icmp_payload_ip4,),
                           name = "ip4_icmp_payload",
                            base = ip4_payload,
                            criteria = lambda x: not ip_frag(x) and x.proto == IPPROTO_ICMP,
                            lastextra = True,
                            init = packvalue(IPPROTO_ICMP, 'proto')
                          )

ip4_icmp_fragment_payload = nstruct((icmp_bestparse,),
                                    name = "ip4_icmp_fragment_payload",
                                   base = ip4_payload,
                                   criteria = lambda x: ip_frag(x) == IP_FRAG_ANY and x.proto == IPPROTO_ICMP,
                                   lastextra = True,
                                   init = packvalue(IPPROTO_ICMP, 'proto')
                                   )

ip4_icmp_partial_payload = nstruct((icmp_bestparse,),
                                   name = "ip4_icmp_partial_payload",
                                   base = ip4_partial_payload,
                                   criteria = lambda x: not (ip_frag(x) & IP_FRAG_LATER) and x.proto == IPPROTO_ICMP,
                                   lastextra = True,
                                   init = packvalue(IPPROTO_ICMP, 'proto')
                                   )

ethernet_l4 = nstruct((ethernet_l2,),
                      name = "ethernet_l4",
                      padding = 1,
                      classifier = lambda x: x.dl_type2 if x.dl_type == ETHERTYPE_8021Q or x.dl_type == ETHERTYPE_8021QS else x.dl_type)

ethernet_l7 = nstruct((ethernet_l2,),
                      name = "ethernet_l7",
                      padding = 1,
                      classifier = lambda x: x.dl_type2 if x.dl_type == ETHERTYPE_8021Q or x.dl_type == ETHERTYPE_8021QS else x.dl_type)

arp_packet_l4 = nstruct((arp_payload,),
                     base = ethernet_l4,
                     classifyby = (ETHERTYPE_ARP,),
                     name = 'arp_packet_l4',
                     init = packvalue(ETHERTYPE_ARP, 'dl_type'),
                     lastextra = True
                     )

arp_packet_l7 = nstruct((arp_payload,),
                     base = ethernet_l7,
                     classifyby = (ETHERTYPE_ARP,),
                     name = 'arp_packet_l7',
                     init = packvalue(ETHERTYPE_ARP, 'dl_type'),
                     lastextra = True
                     )

ip4_packet_l4 = nstruct((ip4_partial_payload,),
                     base = ethernet_l4,
                     classifyby = (ETHERTYPE_IP,),
                     name = 'ip4_packet_l4',
                     init = packvalue(ETHERTYPE_IP, 'dl_type'),
                     lastextra = True
                     )

ip4_packet_l7 = nstruct((ip4_payload,),
                     base = ethernet_l7,
                     classifyby = (ETHERTYPE_IP,),
                     name = 'ip4_packet_l7',
                     init = packvalue(ETHERTYPE_IP, 'dl_type'),
                     lastextra = True
                     )

def create_fragments_ip4(payload, mtu = 1500, options = '', fragment_options = '', proto = IPPROTO_UDP, *args, **kwargs):
    if hasattr(payload, '_tobytes'):
        payload = payload._tobytes()
    if len(payload) > 65536:
        raise ValueError('Payload is too large')
    ip4_header_size = len(ip4_header(options = options))
    ip4_header_frag_size = len(ip4_header(options = fragment_options))
    if len(payload) + ip4_header_size <= mtu:
        # No fragmentation
        return [ip4_payload_fragment(payload = payload,
                                    options = options,
                                    frag_off = 0,
                                    proto = proto,
                                    *args,
                                    **kwargs)]
    else:
        if mtu < ip4_header_size + 8 or mtu < ip4_header_frag_size + 8:
            raise ValueError('MTU is too small, at least %d is required, got %d' %
                             (max(ip4_header_size + 8, ip4_header_frag_size + 8), mtu))
        frags = []
        next_len = (mtu - ip4_header_size) // 8
        offset = 0
        totalsize = len(payload)
        frags.append(ip4_payload_fragment(payload = payload[offset * 8: (offset + next_len) * 8],
                                          options = options,
                                          frag_off = IP_MF,
                                          proto = proto,
                                          *args,
                                          **kwargs))
        while True:
            offset += next_len
            if totalsize - offset * 8 + ip4_header_frag_size <= mtu:
                frags.append(ip4_payload_fragment(payload = payload[offset * 8:],
                                                  options = fragment_options,
                                                  frag_off = offset,
                                                  proto = proto,
                                                  *args,
                                                  **kwargs))
                break
            else:
                next_len = (mtu - ip4_header_frag_size) // 8
                frags.append(ip4_payload_fragment(payload = payload[offset * 8: (offset + next_len) * 8],
                                                  options = fragment_options,
                                                  frag_off = IP_MF | offset,
                                                  proto = proto,
                                                  *args,
                                                  **kwargs))
        return frags

def create_fragments_ip4_packet(payload, mtu = 1500, options = '', fragment_options = '', proto = IPPROTO_UDP, **kwargs):
    if hasattr(payload, '_tobytes'):
        payload = payload._tobytes()
    if len(payload) > 65536:
        raise ValueError('Payload is too large')
    ip4_header_size = len(ip4_header(options = options))
    ip4_header_frag_size = len(ip4_header(options = fragment_options))
    if len(payload) + ip4_header_size <= mtu:
        # No fragmentation
        return [create_packet(ip4_packet, payload = payload,
                                    options = options,
                                    frag_off = 0,
                                    total_len = len(payload) + ip4_header_size,
                                    proto = proto,
                                    **kwargs)]
    else:
        if mtu < ip4_header_size + 8 or mtu < ip4_header_frag_size + 8:
            raise ValueError('MTU is too small, at least %d is required, got %d' %
                             (max(ip4_header_size + 8, ip4_header_frag_size + 8), mtu))
        frags = []
        next_len = (mtu - ip4_header_size) // 8
        offset = 0
        totalsize = len(payload)
        frags.append(create_packet(ip4_packet, payload = payload[offset * 8: (offset + next_len) * 8],
                                          options = options,
                                          frag_off = IP_MF,
                                          total_len = next_len * 8 + ip4_header_size,
                                          proto = proto,
                                          **kwargs))
        while True:
            offset += next_len
            if totalsize - offset * 8 + ip4_header_frag_size <= mtu:
                frags.append(create_packet(ip4_packet, payload = payload[offset * 8:],
                                                  options = fragment_options,
                                                  frag_off = offset,
                                                  total_len = totalsize - offset * 8 + ip4_header_frag_size,
                                                  proto = proto,
                                                  **kwargs))
                break
            else:
                next_len = (mtu - ip4_header_frag_size) // 8
                frags.append(create_packet(ip4_packet, payload = payload[offset * 8: (offset + next_len) * 8],
                                                  options = fragment_options,
                                                  frag_off = IP_MF | offset,
                                                  total_len = next_len * 8 + ip4_header_frag_size,
                                                  proto = proto,
                                                  **kwargs))
        return frags

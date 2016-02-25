'''
Created on 2015/7/30

:author: hubo
'''

from namedstruct import *

ethertype = enum('ethertype', globals(), uint16,
        ETHERTYPE_LOOP      = 0x0060,         # /* Ethernet Loopback packet     */
        ETHERTYPE_PUP       = 0x0200,         # /* Xerox PUP packet             */
        ETHERTYPE_PUPAT     = 0x0201,         # /* Xerox PUP Addr Trans packet  */
        ETHERTYPE_IP        = 0x0800,         # /* Internet Protocol packet     */
        ETHERTYPE_X25       = 0x0805,         # /* CCITT X.25                   */
        ETHERTYPE_ARP       = 0x0806,         # /* Address Resolution packet    */
        ETHERTYPE_BPQ       = 0x08FF,         # /* G8BPQ AX.25 Ethernet Packet  [ NOT AN OFFICIALLY REGISTERED ID ] */
        ETHERTYPE_IEEEPUP   = 0x0a00,         # /* Xerox IEEE802.3 PUP packet */
        ETHERTYPE_IEEEPUPAT = 0x0a01,         # /* Xerox IEEE802.3 PUP Addr Trans packet */
        ETHERTYPE_DEC       = 0x6000,         # /* DEC Assigned proto           */
        ETHERTYPE_DNA_DL    = 0x6001,         # /* DEC DNA Dump/Load            */
        ETHERTYPE_DNA_RC    = 0x6002,         # /* DEC DNA Remote Console       */
        ETHERTYPE_DNA_RT    = 0x6003,         # /* DEC DNA Routing              */
        ETHERTYPE_LAT       = 0x6004,         # /* DEC LAT                      */
        ETHERTYPE_DIAG      = 0x6005,         # /* DEC Diagnostics              */
        ETHERTYPE_CUST      = 0x6006,         # /* DEC Customer use             */
        ETHERTYPE_SCA       = 0x6007,         # /* DEC Systems Comms Arch       */
        ETHERTYPE_TEB       = 0x6558,         # /* Trans Ether Bridging         */
        ETHERTYPE_RARP      = 0x8035,         # /* Reverse Addr Res packet      */
        ETHERTYPE_ATALK     = 0x809B,         # /* Appletalk DDP                */
        ETHERTYPE_AARP      = 0x80F3,         # /* Appletalk AARP               */
        ETHERTYPE_8021Q     = 0x8100,         # /* 802.1Q VLAN Extended Header  */
        ETHERTYPE_IPX       = 0x8137,         # /* IPX over DIX                 */
        ETHERTYPE_IPV6      = 0x86DD,         # /* IPv6 over bluebook           */
        ETHERTYPE_PAUSE     = 0x8808,         # /* IEEE Pause frames. See 802.3 31B */
        ETHERTYPE_SLOW      = 0x8809,         # /* Slow Protocol. See 802.3ad 43B */
        ETHERTYPE_WCCP      = 0x883E,         # /* Web-cache coordination protocol defined in draft-wilson-wrec-wccp-v2-00.txt */
        ETHERTYPE_PPP_DISC  = 0x8863,         # /* PPPoE discovery messages     */
        ETHERTYPE_PPP_SES   = 0x8864,         # /* PPPoE session messages       */
        ETHERTYPE_MPLS_UC   = 0x8847,         # /* MPLS Unicast traffic         */
        ETHERTYPE_MPLS_MC   = 0x8848,         # /* MPLS Multicast traffic       */
        ETHERTYPE_ATMMPOA   = 0x884c,         # /* MultiProtocol Over ATM       */
        ETHERTYPE_ATMFATE   = 0x8884,         # /* Frame-based ATM Transport over Ethernet */
        ETHERTYPE_PAE       = 0x888E,         # /* Port Access Entity (IEEE 802.1X) */
        ETHERTYPE_AOE       = 0x88A2,         # /* ATA over Ethernet            */
        ETHERTYPE_8021QS    = 0x88A8,         # /* 8021.Q Server VLAN Extended Header */
        ETHERTYPE_TIPC      = 0x88CA,         # /* TIPC                         */
        ETHERTYPE_1588      = 0x88F7,         # /* IEEE 1588 Timesync */
        ETHERTYPE_FCOE      = 0x8906,         # /* Fibre Channel over Ethernet  */
        ETHERTYPE_TDLS      = 0x890D,         # /* TDLS */
        ETHERTYPE_FIP       = 0x8914,         # /* FCoE Initialization Protocol */
        ETHERTYPE_EDSA      = 0xDADA,         # /* Ethertype DSA [ NOT AN OFFICIALLY REGISTERED ID ] */
        ETHERTYPE_AF_IUCV   = 0xFBFB          # /* IBM af_iucv [ NOT AN OFFICIALLY REGISTERED ID ] */
)

arp_op_code = enum('arp_op_code', globals(), uint16,
        ARPOP_REQUEST   = 1,               # /* ARP request.  */
        ARPOP_REPLY     = 2,               # /* ARP reply.  */
        ARPOP_RREQUEST  = 3,               # /* RARP request.  */
        ARPOP_RREPLY    = 4,               # /* RARP reply.  */
        ARPOP_InREQUEST = 8,               # /* InARP request.  */
        ARPOP_InREPLY   = 9,               # /* InARP reply.  */
        ARPOP_NAK       = 10               # /* (ATM)ARP NAK.  */
)

ip_protocol = enum('ip_protocol', globals(), uint8, False,
    IPPROTO_IP = 0,             # /* Dummy protocol for TCP               */
    IPPROTO_ICMP = 1,           # /* Internet Control Message Protocol    */
    IPPROTO_IGMP = 2,           # /* Internet Group Management Protocol   */
    IPPROTO_IPIP = 4,           # /* IPIP tunnels (older KA9Q tunnels use 94) */
    IPPROTO_TCP = 6,            # /* Transmission Control Protocol        */
    IPPROTO_EGP = 8,            # /* Exterior Gateway Protocol            */
    IPPROTO_PUP = 12,           # /* PUP protocol                         */
    IPPROTO_UDP = 17,           # /* User Datagram Protocol               */
    IPPROTO_IDP = 22,           # /* XNS IDP protocol                     */
    IPPROTO_DCCP = 33,          # /* Datagram Congestion Control Protocol */
    IPPROTO_RSVP = 46,          # /* RSVP protocol                        */
    IPPROTO_GRE = 47,           # /* Cisco GRE tunnels (rfc 1701,1702)    */
    IPPROTO_IPV6   = 41,        # /* IPv6-in-IPv4 tunnelling              */
    IPPROTO_ESP = 50,          # /* Encapsulation Security Payload protocol */
    IPPROTO_AH = 51,           # /* Authentication Header protocol       */
    IPPROTO_BEETPH = 94,       # /* IP option pseudo header for BEET */
    IPPROTO_PIM    = 103,       # /* Protocol Independent Multicast       */
    IPPROTO_COMP   = 108,              # /* Compression Header protocol */
    IPPROTO_SCTP   = 132,       # /* Stream Control Transport Protocol    */
    IPPROTO_UDPLITE = 136,      # /* UDP-Lite (RFC 3828)                  */
    IPPROTO_RAW    = 255        # /* Raw IP packets                       */
)

icmp_type = enum('icmp_protocol',globals(),uint8,False,
    ICMP_ECHO_REPLY = 0,
    ICMP_ECHO_REQUEST = 8
)

ETH_ALEN = 6

mac_addr = uint8[ETH_ALEN]

mac_addr.formatter = lambda x: ':'.join('%02X' % (n,) for n in x)

mac_addr_bytes = prim(str(ETH_ALEN) + 's', 'mac_addr_bytes')

mac_addr_bytes.formatter = lambda x: ':'.join('%02X' % (c,) for c in bytearray(x))


ip4_addr = prim('I', 'ip4_addr')

import socket as _socket

ip4_addr.formatter = lambda x: _socket.inet_ntoa(ip4_addr.tobytes(x))

ip4_addr_bytes = prim('4s', 'ip4_addr_bytes')
ip4_addr_bytes.formatter = lambda x: _socket.inet_ntoa(x) 

ip6_addr = uint8[16]
if hasattr(_socket, 'inet_ntop'):
    ip6_addr.formatter = lambda x: _socket.inet_ntop(_socket.AF_INET6, ip6_addr.tobytes(x))

ip6_addr_bytes = prim('16s', 'ip6_addr')

if hasattr(_socket, 'inet_ntop'):
    ip6_addr.formatter = lambda x: _socket.inet_ntop(_socket.AF_INET6, x)


ethernetPacket = nstruct((mac_addr,'dstMac'),(mac_addr,'srcMac'),(ethertype,'type'),name='ethernetPacket',padding = 1)

ethernetinner = nstruct(inline=False,classifier=lambda x: x.type2 if x.type == ETHERTYPE_8021Q else x.type, padding = 1,name='ethernetinner')
ethernetPacket_8021Q = nstruct(
        (uint16,'vlan'),
        (ethertype,'type2'),
        (ethernetinner,),
        criteria = lambda x: x.type == ETHERTYPE_8021Q, 
        base = ethernetPacket,
        name='ethernetPacket_8021Q',
        lastextra=True
        ) 

ethernetPacket_8021Q_OTHER = nstruct(
        (ethernetinner,),
        criteria = lambda x: x.type != ETHERTYPE_8021Q,
        base = ethernetPacket,
        name = 'ethernetPacket_8021Q_other',
        lastextra=True
        )
"""
    ============== ==================== =====================
       Attribute      Description          Example
    ============== ==================== =====================
        hwtype         arphrd
        proto          arppro
        hlen           arphln
        plen           arppln
        opcode         arpop
        src_mac        arpsha               '08:60:6e:7f:74:e7'
        src_ip         arpspa               '192.0.2.1'
        dst_mac        arptha               '00:00:00:00:00:00'
        dst_ip         arptpa               '192.0.2.2'
        ============== ==================== =====================
"""
arpPacket = nstruct(
        (uint16,'arp_hwtype'),
        (uint16,'arp_proto'),
        (uint8,'hw_len'),
        (uint8,'proto_len'),
        (arp_op_code,'arp_op'),
        (mac_addr,'arp_smac'),
        (ip4_addr,'arp_sip'),
        (mac_addr,'arp_tmac'),
        (ip4_addr,'arp_tip'),
        base = ethernetinner,
        #criteria = lambda x: x.type == ETHERTYPE_ARP,
        classifyby = (ETHERTYPE_ARP,), 
        name = 'arpPacket'
        )
'''
    ============== ======================================== ==================
        Attribute      Description                              Example
    ============== ======================================== ==================
        version        Version
        header_length  IHL
        tos            Type of Service
        total_length   Total Length
                         (0 means automatically-calculate when encoding)
        identification Identification
        flags          Flags
        offset         Fragment Offset
        ttl            Time to Live
        proto          Protocol
        csum           Header Checksum
                         (Ignored and automatically-calculated when encoding)
        src            Source Address                           '192.0.2.1'
        dst            Destination Address                      '192.0.2.2'
        option         A bytearray which contains the entire 
                            Options, or None for  no Options
    ============== ======================================== ==================
'''
ipPacket = nstruct(
        # 4bit version + 4bit length
        (uint8,'version_length'), 
        (uint8,'tos'),
        (uint16,'total_len'),
        (uint16,'identification'),
        # 3bit flag + 13bit offset
        (uint16,'flag_offset'),
        (uint8,'ttl'),
        (uint8,'proto'),
        (uint16,'checksum'),
        (ip4_addr,'srcaddr'),
        (ip4_addr,'dstaddr'),
        (raw,'data'),
        base = ethernetinner,
        classifyby = (ETHERTYPE_IP,),
        name = 'ipPacket'
        )

icmpPacket = nstruct(
        (uint8,'icmptype'),
        (uint8,'code'),
        (uint16,'icmp_check_sum'),
        #criteria = lambda x: x.proto == IPPROTO_ICMP, 
        name = 'icmpPacket'
        )

icmpEchoPacket = nstruct(
        (uint16,'identifier'),
        (uint16,'seq'),
        (raw,'icmp_data'),
        base = icmpPacket,
        criteria = lambda x:x.icmptype == ICMP_ECHO_REQUEST,
        name = 'icmpEchoPacket'
        )

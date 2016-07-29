'''
/* Copyright (c) 2008 The Board of Trustees of The Leland Stanford
* Junior University
* Copyright (c) 2011, 2012 Open Networking Foundation
*
* We are making the OpenFlow specification and associated documentation
* (Software) available for public use and benefit with the expectation
* that others will use, modify and enhance the Software and contribute
* those enhancements back to the community. However, since we would
* like to make the Software available for broadest use, with as few
* restrictions as possible permission is hereby granted, free of
* charge, to any person obtaining a copy of this Software to deal in
* the Software under the copyrights without restriction, including
* without limitation the rights to use, copy, modify, merge, publish,
* distribute, sublicense, and/or sell copies of the Software, and to
* permit persons to whom the Software is furnished to do so, subject to
* the following conditions:
*
* The above copyright notice and this permission notice shall be
* included in all copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
* EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
* MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
* NONINFRINGEMENT.  IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
* BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
* ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
* CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
* SOFTWARE.
*
* The name and trademarks of copyright holder(s) may NOT be used in
* advertising or publicity pertaining to the Software or any
* derivatives without specific, written prior permission.
*/
Created on 2015/7/14

:author: hubo
'''
from .common import *
from . import common
from namedstruct.namedstruct import rawtype as _rawtype
from namedstruct.namedstruct import StructDefWarning
import warnings as _warnings

with _warnings.catch_warnings():
    _warnings.filterwarnings('ignore', '^padding', StructDefWarning)
    
    ofp_port_no = enum('ofp_port_no',
                       globals(),
                       uint32,
                       OFPP_MAX = 0xffffff00,
                       OFPP_IN_PORT = 0xfffffff8,
                       OFPP_TABLE = 0xfffffff9,
                       OFPP_NORMAL = 0xfffffffa,
                       OFPP_FLOOD = 0xfffffffb,
                       OFPP_ALL = 0xfffffffc,
                       OFPP_CONTROLLER = 0xfffffffd,
                       OFPP_LOCAL = 0xfffffffe,
                       OFPP_ANY = 0xffffffff)
    
    ofp_error_type = ofp_error_type.extend(globals(),
        OFPET_BAD_INSTRUCTION      = 3,  #/* Error in instruction list. */
        OFPET_BAD_MATCH            = 4,  #/* Error in match. */
        OFPET_FLOW_MOD_FAILED      = 5,  #/* Problem modifying flow entry. */
        OFPET_GROUP_MOD_FAILED     = 6,  #/* Problem modifying group entry. */
        OFPET_PORT_MOD_FAILED      = 7,  #/* Port mod request failed. */
        OFPET_TABLE_MOD_FAILED     = 8,  #/* Table mod request failed. */
        OFPET_QUEUE_OP_FAILED      = 9,  #/* Queue operation failed. */
        OFPET_SWITCH_CONFIG_FAILED = 10, #/* Switch config request failed. */
        OFPET_ROLE_REQUEST_FAILED  = 11, #/* Controller Role request failed. */
        OFPET_METER_MOD_FAILED     = 12, #/* Error in meter. */
        OFPET_TABLE_FEATURES_FAILED = 13,# /* Setting table features failed. */
        OFPET_EXPERIMENTER = 0xffff      #/* Experimenter error messages. */
    )
    
    ofp_type = ofp_type.extend(globals(),
        OFPT_EXPERIMENTER       = 4,  #/* Symmetric message */
    
    #    /* Switch configuration messages. */
        OFPT_FEATURES_REQUEST   = 5,  #/* Controller/switch message */
        OFPT_FEATURES_REPLY     = 6,  #/* Controller/switch message */
        OFPT_GET_CONFIG_REQUEST = 7,  #/* Controller/switch message */
        OFPT_GET_CONFIG_REPLY   = 8,  #/* Controller/switch message */
        OFPT_SET_CONFIG         = 9,  #/* Controller/switch message */
    
    #    /* Asynchronous messages. */
        OFPT_PACKET_IN          = 10, #/* Async message */
        OFPT_FLOW_REMOVED       = 11, #/* Async message */
        OFPT_PORT_STATUS        = 12, #/* Async message */
    
    #    /* Controller command messages. */
        OFPT_PACKET_OUT         = 13, #/* Controller/switch message */
        OFPT_FLOW_MOD           = 14, #/* Controller/switch message */
        OFPT_GROUP_MOD          = 15, #/* Controller/switch message */
        OFPT_PORT_MOD           = 16, #/* Controller/switch message */
        OFPT_TABLE_MOD          = 17, #/* Controller/switch message */
    
    #    /* Multipart messages. */
        OFPT_MULTIPART_REQUEST      = 18, #/* Controller/switch message */
        OFPT_MULTIPART_REPLY        = 19, #/* Controller/switch message */
    
    #    /* Barrier messages. */
        OFPT_BARRIER_REQUEST    = 20, #/* Controller/switch message */
        OFPT_BARRIER_REPLY      = 21, #/* Controller/switch message */
    
    #    /* Queue Configuration messages. */
        OFPT_QUEUE_GET_CONFIG_REQUEST = 22,  #/* Controller/switch message */
        OFPT_QUEUE_GET_CONFIG_REPLY   = 23,  #/* Controller/switch message */
    
    #    /* Controller role change request messages. */
        OFPT_ROLE_REQUEST       = 24, #/* Controller/switch message */
        OFPT_ROLE_REPLY         = 25, #/* Controller/switch message */
    
    #    /* Asynchronous message configuration. */
        OFPT_GET_ASYNC_REQUEST  = 26, #/* Controller/switch message */
        OFPT_GET_ASYNC_REPLY    = 27, #/* Controller/switch message */
        OFPT_SET_ASYNC          = 28, #/* Controller/switch message */
    
    #    /* Meters and rate limiters configuration messages. */
        OFPT_METER_MOD          = 29, #/* Controller/switch message */
    )
    
    ofp_type_reply_set = set([OFPT_ECHO_REPLY, OFPT_FEATURES_REPLY, OFPT_GET_CONFIG_REPLY, OFPT_MULTIPART_REPLY, OFPT_BARRIER_REPLY, OFPT_QUEUE_GET_CONFIG_REPLY, OFPT_ROLE_REPLY, OFPT_GET_ASYNC_REPLY])
    
    ofp_type_asyncmessage_set = set([OFPT_PACKET_IN, OFPT_FLOW_REMOVED, OFPT_PORT_STATUS])
    
    OFP_VERSION = OFP13_VERSION
    
    ofp_msg = nstruct(name = 'ofp_msg',
                      base = common.ofp_msg_mutable,
                      criteria = lambda x: x.header.version == OFP_VERSION,
                      init = packvalue(OFP_VERSION, 'header', 'version'),
                      classifyby = (OFP_VERSION,),
                      classifier = lambda x: x.header.type,
                      extend = {('header', 'type') : ofp_type})
    
    '''
    /* Switch configuration. */
    '''
    ofp_switch_config = nstruct((ofp_config_flags, 'flags'),
                                (uint16, 'miss_send_len'),
                                name = 'ofp_switch_config',
                                base = ofp_msg,
                                criteria = lambda x: x.header.type == OFPT_GET_CONFIG_REPLY or x.header.type == OFPT_SET_CONFIG,
                                classifyby = (OFPT_SET_CONFIG, OFPT_GET_CONFIG_REPLY),
                                init = packvalue(OFPT_SET_CONFIG, 'header','type'))
    
    '''
    /* Configure/Modify behavior of a flow table */
    '''
    ofp_table_mod = nstruct(
        (ofp_table, 'table_id'),     #  /* ID of the table, OFPTT_ALL indicates all tables */
        (uint8[3],),        # /* Pad to 32 bits */
        (ofp_table_config, 'config'), #      /* Bitmap of OFPTC_* flags */
        name = 'ofp_table_mod',
        base = ofp_msg,
        criteria = lambda x: x.header.type == OFPT_TABLE_MOD,
        classifyby = (OFPT_TABLE_MOD,),
        init = packvalue(OFPT_TABLE_MOD, 'header', 'type')
    )
    
    
    '''
    /* Capabilities supported by the datapath. */
    '''
    ofp_capabilities = ofp_capabilities.extend(globals(),
        OFPC_GROUP_STATS    = 1 << 3,  # /* Group statistics. */
        OFPC_PORT_BLOCKED   = 1 << 8   # /* Switch will block looping ports. */
    )
    
    '''
    /* Current state of the physical port.  These are not configurable from
     * the controller.
    */
    '''
    ofp_port_state = ofp_port_state.extend(globals(),
        OFPPS_BLOCKED      = 1 << 1,  # /* Port is blocked */
        OFPPS_LIVE         = 1 << 2,  # /* Live for Fast Failover Group. */
    )
    
    '''
    /* Features of ports available in a datapath. */
    '''
    ofp_port_features = ofp_port_features.extend(
        OFPPF_40GB_FD    = 1 << 7,  #/* 40 Gb full-duplex rate support. */
        OFPPF_100GB_FD   = 1 << 8,  #/* 100 Gb full-duplex rate support. */
        OFPPF_1TB_FD     = 1 << 9,  #/* 1 Tb full-duplex rate support. */
        OFPPF_OTHER      = 1 << 10, #/* Other rate, not in the list. */
    
        OFPPF_COPPER     = 1 << 11, #/* Copper medium. */
        OFPPF_FIBER      = 1 << 12, #/* Fiber medium. */
        OFPPF_AUTONEG    = 1 << 13, #/* Auto-negotiation. */
        OFPPF_PAUSE      = 1 << 14, #/* Pause. */
        OFPPF_PAUSE_ASYM = 1 << 15  #/* Asymmetric pause. */
    )
    
    '''
    /* Description of a port */
    '''
    ofp_port = nstruct(
        (ofp_port_no, 'port_no'),
        (uint8[4],),
        (mac_addr, 'hw_addr'),
        (uint8[2],),                #  /* Align to 64 bits. */
        (char[OFP_MAX_PORT_NAME_LEN], 'name'), # /* Null-terminated */
    
        (ofp_port_config, 'config'),     #   /* Bitmap of OFPPC_* flags. */
        (ofp_port_state, 'state'),      #   /* Bitmap of OFPPS_* flags. */
    
    #    /* Bitmaps of OFPPF_* that describe features.  All bits zeroed if
    #     * unsupported or unavailable. */
        (ofp_port_features, 'curr'),       #   /* Current features. */
        (ofp_port_features, 'advertised'), #   /* Features being advertised by the port. */
        (ofp_port_features, 'supported'),  #   /* Features supported by the port. */
        (ofp_port_features, 'peer'),       #   /* Features advertised by peer. */
    
        (uint32, 'curr_speed'), #   /* Current port bitrate in kbps. */
        (uint32, 'max_speed'),  #   /* Max port bitrate in kbps */
        name = 'ofp_port'
    )
    
    '''
    /* Switch features. */
    '''
    
    ofp_switch_features = nstruct((uint64, 'datapath_id'),
                                  (uint32, 'n_buffers'),
                                  (uint8, 'n_tables'),
                                  (uint8, 'auxiliary_id'),
                                  (uint8[2],),
                                  (ofp_capabilities, 'capabilities'),
                                  (uint32,),
                                  name = 'ofp_switch_features',
                                  base = ofp_msg,
                                  criteria = lambda x: x.header.type == OFPT_FEATURES_REPLY,
                                  classifyby = (OFPT_FEATURES_REPLY,),
                                  init = packvalue(OFPT_FEATURES_REPLY, 'header', 'type'))
    
    '''
    /* A physical port has changed in the datapath */
    '''
    ofp_port_status = nstruct(
        (ofp_port_reason, 'reason'),
        (uint8[7],),
        (ofp_port, 'desc'),
        name= 'ofp_port_status',
        base = ofp_msg,
        criteria = lambda x: x.header.type == OFPT_PORT_STATUS,
        classifyby = (OFPT_PORT_STATUS,),
        init = packvalue(OFPT_PORT_STATUS, 'header', 'type')
    )
    
    '''
    /* Modify behavior of the physical port */
    '''
    ofp_port_mod = nstruct(
        (uint32, 'port_no'),
        (uint8[4],),
        (mac_addr, 'hw_addr'), 
        (uint8[2],),
        (ofp_port_config, 'config'),     #   /* Bitmap of OFPPC_* flags. */
        (ofp_port_config, 'mask'),       #   /* Bitmap of OFPPC_* flags to be changed. */
    
        (ofp_port_features, 'advertise'),  #   /* Bitmap of "ofp_port_features"s.  Zero all bits to prevent any action taking place. */
        (uint8[4],),            #   /* Pad to 64-bits. */
        name = 'ofp_port_mod',
        base = ofp_msg,
        criteria = lambda x: x.header.type == OFPT_PORT_MOD,
        classifyby = (OFPT_PORT_MOD,),
        init = packvalue(OFPT_PORT_MOD, 'header', 'type')
    )
    
    '''
    /* ## -------------------------- ## */
    /* ## OpenFlow Extensible Match. ## */
    /* ## -------------------------- ## */
    
    /* The match type indicates the match structure (set of fields that compose the
     * match) in use. The match type is placed in the type field at the beginning
     * of all match structures. The "OpenFlow Extensible Match" type corresponds
     * to OXM TLV format described below and must be supported by all OpenFlow
     * switches. Extensions that define other match types may be published on the
     * ONF wiki. Support for extensions is optional.
    */
    
    /* Fields to match against flows */
    '''
    ofp_match = nstruct(
        (ofp_match_type, 'type'),      #       /* One of OFPMT_* */
        (uint16, 'length'),    #       /* Length of ofp_match (excluding padding) */
    #    /* Followed by:
    #     *   - Exactly (length - 4) (possibly 0) bytes containing OXM TLVs, then
    #     *   - Exactly ((length + 7)/8*8 - length) (between 0 and 7) bytes of
    #     *     all-zero bytes
    #     * In summary, ofp_match is padded as needed, to make its overall size
    #     * a multiple of 8, to preserve alignment in structures using it.
    #     */
        name = 'ofp_match',
        size = sizefromlen(4096, 'length'),
        prepack = packrealsize('length')
    )
    
    '''
    /* Components of a OXM TLV header.
     * Those macros are not valid for the experimenter class, macros for the
     * experimenter class will depend on the experimenter header used. */
    '''
    def OXM_HEADER__(CLASS, FIELD, HASMASK, LENGTH):
        return (((CLASS) << 16) | ((FIELD) << 9) | ((HASMASK) << 8) | (LENGTH))
    def OXM_HEADER(CLASS, FIELD, LENGTH):
        return OXM_HEADER__(CLASS, FIELD, 0, LENGTH)
    def OXM_HEADER_W(CLASS, FIELD, LENGTH):
        return OXM_HEADER__(CLASS, FIELD, 1, (LENGTH) * 2)
    def OXM_CLASS(HEADER):
        return ((HEADER) >> 16)
    def OXM_FIELD(HEADER):
        return (((HEADER) >> 9) & 0x7f)
    def OXM_TYPE(HEADER):
        return (((HEADER) >> 9) & 0x7fffff)
    def OXM_HASMASK(HEADER):
        return (((HEADER) >> 8) & 1)
    def OXM_LENGTH(HEADER):
        return ((HEADER) & 0xff)
    
    def OXM_MAKE_WILD_HEADER(HEADER):
        return OXM_HEADER_W(OXM_CLASS(HEADER), OXM_FIELD(HEADER), OXM_LENGTH(HEADER))
    
    '''
    /* OXM Class IDs.
     * The high order bit differentiate reserved classes from member classes.
     * Classes 0x0000 to 0x7FFF are member classes, allocated by ONF.
     * Classes 0x8000 to 0xFFFE are reserved classes, reserved for standardisation.
    */
    '''
    ofp_oxm_class = enum('ofp_oxm_class', globals(), uint16,
        OFPXMC_NXM_0          = 0x0000,  #  /* Backward compatibility with NXM */
        OFPXMC_NXM_1          = 0x0001,  #  /* Backward compatibility with NXM */
        OFPXMC_OPENFLOW_BASIC = 0x8000,  #  /* Basic class for OpenFlow */
        OFPXMC_EXPERIMENTER   = 0xFFFF,  #  /* Experimenter class */
    )
    
    '''
    /* OXM Flow match field types for OpenFlow basic class. */
    '''
    oxm_ofb_match_fields = enum('oxm_ofb_match_fields', globals(), uint8,
        OFPXMT_OFB_IN_PORT        = 0, # /* Switch input port. */
        OFPXMT_OFB_IN_PHY_PORT    = 1, # /* Switch physical input port. */
        OFPXMT_OFB_METADATA       = 2, # /* Metadata passed between tables. */
        OFPXMT_OFB_ETH_DST        = 3, # /* Ethernet destination address. */
        OFPXMT_OFB_ETH_SRC        = 4, # /* Ethernet source address. */
        OFPXMT_OFB_ETH_TYPE       = 5, # /* Ethernet frame type. */
        OFPXMT_OFB_VLAN_VID       = 6, # /* VLAN id. */
        OFPXMT_OFB_VLAN_PCP       = 7, # /* VLAN priority. */
        OFPXMT_OFB_IP_DSCP        = 8, # /* IP DSCP (6 bits in ToS field). */
        OFPXMT_OFB_IP_ECN         = 9, # /* IP ECN (2 bits in ToS field). */
        OFPXMT_OFB_IP_PROTO       = 10,# /* IP protocol. */
        OFPXMT_OFB_IPV4_SRC       = 11,# /* IPv4 source address. */
        OFPXMT_OFB_IPV4_DST       = 12,# /* IPv4 destination address. */
        OFPXMT_OFB_TCP_SRC        = 13,# /* TCP source port. */
        OFPXMT_OFB_TCP_DST        = 14,# /* TCP destination port. */
        OFPXMT_OFB_UDP_SRC        = 15,# /* UDP source port. */
        OFPXMT_OFB_UDP_DST        = 16,# /* UDP destination port. */
        OFPXMT_OFB_SCTP_SRC       = 17,# /* SCTP source port. */
        OFPXMT_OFB_SCTP_DST       = 18,# /* SCTP destination port. */
        OFPXMT_OFB_ICMPV4_TYPE    = 19,# /* ICMP type. */
        OFPXMT_OFB_ICMPV4_CODE    = 20,# /* ICMP code. */
        OFPXMT_OFB_ARP_OP         = 21,# /* ARP opcode. */
        OFPXMT_OFB_ARP_SPA        = 22,# /* ARP source IPv4 address. */
        OFPXMT_OFB_ARP_TPA        = 23,# /* ARP target IPv4 address. */
        OFPXMT_OFB_ARP_SHA        = 24,# /* ARP source hardware address. */
        OFPXMT_OFB_ARP_THA        = 25,# /* ARP target hardware address. */
        OFPXMT_OFB_IPV6_SRC       = 26,# /* IPv6 source address. */
        OFPXMT_OFB_IPV6_DST       = 27,# /* IPv6 destination address. */
        OFPXMT_OFB_IPV6_FLABEL    = 28,# /* IPv6 Flow Label */
        OFPXMT_OFB_ICMPV6_TYPE    = 29,# /* ICMPv6 type. */
        OFPXMT_OFB_ICMPV6_CODE    = 30,# /* ICMPv6 code. */
        OFPXMT_OFB_IPV6_ND_TARGET = 31,# /* Target address for ND. */
        OFPXMT_OFB_IPV6_ND_SLL    = 32,# /* Source link-layer for ND. */
        OFPXMT_OFB_IPV6_ND_TLL    = 33,# /* Target link-layer for ND. */
        OFPXMT_OFB_MPLS_LABEL     = 34,# /* MPLS label. */
        OFPXMT_OFB_MPLS_TC        = 35,# /* MPLS TC. */
        OFPXMT_OFB_MPLS_BOS       = 36,# /* MPLS BoS bit. */
        OFPXMT_OFB_PBB_ISID       = 37,# /* PBB I-SID. */
        OFPXMT_OFB_TUNNEL_ID      = 38,# /* Logical Port Metadata. */
        OFPXMT_OFB_IPV6_EXTHDR    = 39,# /* IPv6 Extension Header pseudo-field */
    )
    
    OFPXMT_OFB_ALL = ((1 << 40) - 1)
    
    '''
    /* The VLAN id is 12-bits, so we can use the entire 16 bits to indicate
     * special conditions.
    */
    '''
    ofp_vlan_id = enum('ofp_vlan_id', globals(),
        OFPVID_PRESENT = 0x1000, #/* Bit that indicate that a VLAN id is set */
        OFPVID_NONE    = 0x0000, #/* No VLAN id was set. */
    )
    '''
    /* Define for compatibility */
    '''
    OFP_VLAN_NONE = OFPVID_NONE
    
    
    ofp_oxm_header = enum('ofp_oxm_header', globals(), uint32,
    #===============================================================================
    # /* OpenFlow port on which the packet was received.
    #  * May be a physical port, a logical port, or the reserved port OFPP_LOCAL
    #  *
    #  * Prereqs: None.
    #  *
    #  * Format: 32-bit integer in network byte order.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    OXM_OF_IN_PORT = OXM_HEADER(0x8000, OFPXMT_OFB_IN_PORT, 4),
    
    #===============================================================================
    # /* Physical port on which the packet was received.
    #  *
    #  * Consider a packet received on a tunnel interface defined over a link
    #  * aggregation group (LAG) with two physical port members.  If the tunnel
    #  * interface is the logical port bound to OpenFlow.  In this case,
    #  * OFPXMT_OF_IN_PORT is the tunnel's port number and OFPXMT_OF_IN_PHY_PORT is
    #  * the physical port number of the LAG on which the tunnel is configured.
    #  *
    #  * When a packet is received directly on a physical port and not processed by a
    #  * logical port, OFPXMT_OF_IN_PORT and OFPXMT_OF_IN_PHY_PORT have the same
    #  * value.
    #  *
    #  * This field is usually not available in a regular match and only available
    #  * in ofp_packet_in messages when it's different from OXM_OF_IN_PORT.
    #  *
    #  * Prereqs: OXM_OF_IN_PORT must be present.
    #  *
    #  * Format: 32-bit integer in network byte order.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    OXM_OF_IN_PHY_PORT = OXM_HEADER(0x8000, OFPXMT_OFB_IN_PHY_PORT, 4),
    #===============================================================================
    # /* Table metadata.
    #  *
    #  * Prereqs: None.
    #  *
    #  * Format: 64-bit integer in network byte order.
    #  *
    #  * Masking: Arbitrary masks.
    # */
    #===============================================================================
    OXM_OF_METADATA = OXM_HEADER(0x8000, OFPXMT_OFB_METADATA, 8),
    OXM_OF_METADATA_W = OXM_HEADER_W(0x8000, OFPXMT_OFB_METADATA, 8),
    
    #===============================================================================
    # /* Source or destination address in Ethernet header.
    #  *
    #  * Prereqs: None.
    #  *
    #  * Format: 48-bit Ethernet MAC address.
    #  *
    #  * Masking: Arbitrary masks. */
    #===============================================================================
    OXM_OF_ETH_DST =    OXM_HEADER  (0x8000, OFPXMT_OFB_ETH_DST, 6),
    OXM_OF_ETH_DST_W = OXM_HEADER_W(0x8000, OFPXMT_OFB_ETH_DST, 6),
    OXM_OF_ETH_SRC =   OXM_HEADER  (0x8000, OFPXMT_OFB_ETH_SRC, 6),
    OXM_OF_ETH_SRC_W =  OXM_HEADER_W(0x8000, OFPXMT_OFB_ETH_SRC, 6),
    
    #===============================================================================
    # /* Packet's Ethernet type.
    #  *
    #  * Prereqs: None.
    #  *
    #  * Format: 16-bit integer in network byte order.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    OXM_OF_ETH_TYPE = OXM_HEADER  (0x8000, OFPXMT_OFB_ETH_TYPE, 2),
    
    
    #===============================================================================
    # /* 802.1Q VID.
    #  *
    #  * For a packet with an 802.1Q header, this is the VLAN-ID (VID) from the
    #  * outermost tag, with the CFI bit forced to 1. For a packet with no 802.1Q
    #  * header, this has value OFPVID_NONE.
    #  *
    #  * Prereqs: None.
    #  *
    #  * Format: 16-bit integer in network byte order with bit 13 indicating
    #  * presence of VLAN header and 3 most-significant bits forced to 0.
    #  * Only the lower 13 bits have meaning.
    #  *
    #  * Masking: Arbitrary masks.
    #  *
    #  * This field can be used in various ways:
    #  *
    #  *   - If it is not constrained at all, the nx_match matches packets without
    #  *     an 802.1Q header or with an 802.1Q header that has any VID value.
    #  *
    #  *   - Testing for an exact match with 0x0 matches only packets without
    #  *     an 802.1Q header.
    #  *
    #  *   - Testing for an exact match with a VID value with CFI=1 matches packets
    #  *     that have an 802.1Q header with a specified VID.
    #  *
    #  *   - Testing for an exact match with a nonzero VID value with CFI=0 does
    #  *     not make sense.  The switch may reject this combination.
    #  *
    #  *   - Testing with nxm_value=0, nxm_mask=0x0fff matches packets with no 802.1Q
    #  *     header or with an 802.1Q header with a VID of 0.
    #  *
    #  *   - Testing with nxm_value=0x1000, nxm_mask=0x1000 matches packets with
    #  *     an 802.1Q header that has any VID value.
    # */
    #===============================================================================
    OXM_OF_VLAN_VID = OXM_HEADER  (0x8000, OFPXMT_OFB_VLAN_VID, 2),
    OXM_OF_VLAN_VID_W = OXM_HEADER_W(0x8000, OFPXMT_OFB_VLAN_VID, 2),
    
    
    #===============================================================================
    # /* 802.1Q PCP.
    #  *
    #  * For a packet with an 802.1Q header, this is the VLAN-PCP from the
    #  * outermost tag.  For a packet with no 802.1Q header, this has value
    #  * 0.
    #  *
    #  * Prereqs: OXM_OF_VLAN_VID must be different from OFPVID_NONE.
    #  *
    #  * Format: 8-bit integer with 5 most-significant bits forced to 0.
    #  * Only the lower 3 bits have meaning.
    #  *
    #  * Masking: Not maskable.
    # */
    #===============================================================================
    OXM_OF_VLAN_PCP = OXM_HEADER  (0x8000, OFPXMT_OFB_VLAN_PCP, 1),
    
    #===============================================================================
    # /* The Diff Serv Code Point (DSCP) bits of the IP header.
    #  * Part of the IPv4 ToS field or the IPv6 Traffic Class field.
    #  *
    #  * Prereqs: OXM_OF_ETH_TYPE must be either 0x0800 or 0x86dd.
    #  *
    #  * Format: 8-bit integer with 2 most-significant bits forced to 0.
    #  * Only the lower 6 bits have meaning.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    OXM_OF_IP_DSCP = OXM_HEADER  (0x8000, OFPXMT_OFB_IP_DSCP, 1),
    
    #===============================================================================
    # /* The ECN bits of the IP header.
    #  * Part of the IPv4 ToS field or the IPv6 Traffic Class field.
    #  *
    #  * Prereqs: OXM_OF_ETH_TYPE must be either 0x0800 or 0x86dd.
    #  *
    #  * Format: 8-bit integer with 6 most-significant bits forced to 0.
    #  * Only the lower 2 bits have meaning.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    OXM_OF_IP_ECN = OXM_HEADER  (0x8000, OFPXMT_OFB_IP_ECN, 1),
    
    #===============================================================================
    # /* The "protocol" byte in the IP header.
    #  *
    #  * Prereqs: OXM_OF_ETH_TYPE must be either 0x0800 or 0x86dd.
    #  *
    #  * Format: 8-bit integer.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    OXM_OF_IP_PROTO = OXM_HEADER  (0x8000, OFPXMT_OFB_IP_PROTO, 1),
    
    #===============================================================================
    # /* The source or destination address in the IP header.
    #  *
    #  * Prereqs: OXM_OF_ETH_TYPE must match 0x0800 exactly.
    #  *
    #  * Format: 32-bit integer in network byte order.
    #  *
    #  * Masking: Arbitrary masks.
    # */
    #===============================================================================
    OXM_OF_IPV4_SRC = OXM_HEADER  (0x8000, OFPXMT_OFB_IPV4_SRC, 4),
    OXM_OF_IPV4_SRC_W = OXM_HEADER_W(0x8000, OFPXMT_OFB_IPV4_SRC, 4),
    OXM_OF_IPV4_DST = OXM_HEADER  (0x8000, OFPXMT_OFB_IPV4_DST, 4),
    OXM_OF_IPV4_DST_W = OXM_HEADER_W(0x8000, OFPXMT_OFB_IPV4_DST, 4),
    
    
    #===============================================================================
    # /* The source or destination port in the TCP header.
    #  *
    #  * Prereqs:
    #  *   OXM_OF_ETH_TYPE must be either 0x0800 or 0x86dd.
    #  *   OXM_OF_IP_PROTO must match 6 exactly.
    #  *
    #  * Format: 16-bit integer in network byte order.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    OXM_OF_TCP_SRC = OXM_HEADER  (0x8000, OFPXMT_OFB_TCP_SRC, 2),
    OXM_OF_TCP_DST = OXM_HEADER  (0x8000, OFPXMT_OFB_TCP_DST, 2),
    
    #===============================================================================
    # /* The source or destination port in the UDP header.
    #  *
    #  * Prereqs:
    #  *   OXM_OF_ETH_TYPE must match either 0x0800 or 0x86dd.
    #  *   OXM_OF_IP_PROTO must match 17 exactly.
    #  *
    #  * Format: 16-bit integer in network byte order.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    OXM_OF_UDP_SRC = OXM_HEADER  (0x8000, OFPXMT_OFB_UDP_SRC, 2),
    OXM_OF_UDP_DST = OXM_HEADER  (0x8000, OFPXMT_OFB_UDP_DST, 2),
    
    #===============================================================================
    # /* The source or destination port in the SCTP header.
    #  *
    #  * Prereqs:
    #  *   OXM_OF_ETH_TYPE must match either 0x0800 or 0x86dd.
    #  *   OXM_OF_IP_PROTO must match 132 exactly.
    #  *
    #  * Format: 16-bit integer in network byte order.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    OXM_OF_SCTP_SRC = OXM_HEADER  (0x8000, OFPXMT_OFB_SCTP_SRC, 2),
    OXM_OF_SCTP_DST = OXM_HEADER  (0x8000, OFPXMT_OFB_SCTP_DST, 2),
    
    #===============================================================================
    # /* The type or code in the ICMP header.
    #  *
    #  * Prereqs:
    #  *   OXM_OF_ETH_TYPE must match 0x0800 exactly.
    #  *   OXM_OF_IP_PROTO must match 1 exactly.
    #  *
    #  * Format: 8-bit integer.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    OXM_OF_ICMPV4_TYPE = OXM_HEADER  (0x8000, OFPXMT_OFB_ICMPV4_TYPE, 1),
    OXM_OF_ICMPV4_CODE = OXM_HEADER  (0x8000, OFPXMT_OFB_ICMPV4_CODE, 1),
    
    #===============================================================================
    # /* ARP opcode.
    #  *
    #  * For an Ethernet+IP ARP packet, the opcode in the ARP header.  Always 0
    #  * otherwise.
    #  *
    #  * Prereqs: OXM_OF_ETH_TYPE must match 0x0806 exactly.
    #  *
    #  * Format: 16-bit integer in network byte order.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    OXM_OF_ARP_OP = OXM_HEADER  (0x8000, OFPXMT_OFB_ARP_OP, 2),
    
    #===============================================================================
    # /* For an Ethernet+IP ARP packet, the source or target protocol address
    #  * in the ARP header.  Always 0 otherwise.
    #  *
    #  * Prereqs: OXM_OF_ETH_TYPE must match 0x0806 exactly.
    #  *
    #  * Format: 32-bit integer in network byte order.
    #  *
    #  * Masking: Arbitrary masks.
    # */
    #===============================================================================
    OXM_OF_ARP_SPA = OXM_HEADER  (0x8000, OFPXMT_OFB_ARP_SPA, 4),
    OXM_OF_ARP_SPA_W = OXM_HEADER_W(0x8000, OFPXMT_OFB_ARP_SPA, 4),
    OXM_OF_ARP_TPA = OXM_HEADER  (0x8000, OFPXMT_OFB_ARP_TPA, 4),
    OXM_OF_ARP_TPA_W = OXM_HEADER_W(0x8000, OFPXMT_OFB_ARP_TPA, 4),
    
    #===============================================================================
    # /* For an Ethernet+IP ARP packet, the source or target hardware address
    #  * in the ARP header.  Always 0 otherwise.
    #  *
    #  * Prereqs: OXM_OF_ETH_TYPE must match 0x0806 exactly.
    #  *
    #  * Format: 48-bit Ethernet MAC address.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    OXM_OF_ARP_SHA = OXM_HEADER  (0x8000, OFPXMT_OFB_ARP_SHA, 6),
    OXM_OF_ARP_THA = OXM_HEADER  (0x8000, OFPXMT_OFB_ARP_THA, 6),
    
    #===============================================================================
    # /* The source or destination address in the IPv6 header.
    #  *
    #  * Prereqs: OXM_OF_ETH_TYPE must match 0x86dd exactly.
    #  *
    #  * Format: 128-bit IPv6 address.
    #  *
    #  * Masking: Arbitrary masks.
    # */
    #===============================================================================
    OXM_OF_IPV6_SRC = OXM_HEADER  (0x8000, OFPXMT_OFB_IPV6_SRC, 16),
    OXM_OF_IPV6_SRC_W = OXM_HEADER_W(0x8000, OFPXMT_OFB_IPV6_SRC, 16),
    OXM_OF_IPV6_DST = OXM_HEADER  (0x8000, OFPXMT_OFB_IPV6_DST, 16),
    OXM_OF_IPV6_DST_W = OXM_HEADER_W(0x8000, OFPXMT_OFB_IPV6_DST, 16),
    
    #===============================================================================
    # /* The IPv6 Flow Label
    #  *
    #  * Prereqs:
    #  *   OXM_OF_ETH_TYPE must match 0x86dd exactly
    #  *
    #  * Format: 32-bit integer with 12 most-significant bits forced to 0.
    #  * Only the lower 20 bits have meaning.
    #  *
    #  * Masking: Arbitrary masks.
    # */
    #===============================================================================
    OXM_OF_IPV6_FLABEL = OXM_HEADER  (0x8000, OFPXMT_OFB_IPV6_FLABEL, 4),
    OXM_OF_IPV6_FLABEL_W = OXM_HEADER_W(0x8000, OFPXMT_OFB_IPV6_FLABEL, 4),
    
    #===============================================================================
    # /* The type or code in the ICMPv6 header.
    #  *
    #  * Prereqs:
    #  *   OXM_OF_ETH_TYPE must match 0x86dd exactly.
    #  *   OXM_OF_IP_PROTO must match 58 exactly.
    #  *
    #  * Format: 8-bit integer.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    OXM_OF_ICMPV6_TYPE = OXM_HEADER  (0x8000, OFPXMT_OFB_ICMPV6_TYPE, 1),
    OXM_OF_ICMPV6_CODE = OXM_HEADER  (0x8000, OFPXMT_OFB_ICMPV6_CODE, 1),
    
    #===============================================================================
    # /* The target address in an IPv6 Neighbor Discovery message.
    #  *
    #  * Prereqs:
    #  *   OXM_OF_ETH_TYPE must match 0x86dd exactly.
    #  *   OXM_OF_IP_PROTO must match 58 exactly.
    #  *   OXM_OF_ICMPV6_TYPE must be either 135 or 136.
    #  *
    #  * Format: 128-bit IPv6 address.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    OXM_OF_IPV6_ND_TARGET = OXM_HEADER (0x8000, OFPXMT_OFB_IPV6_ND_TARGET, 16),
    
    #===============================================================================
    # /* The source link-layer address option in an IPv6 Neighbor Discovery
    #  * message.
    #  *
    #  * Prereqs:
    #  *   OXM_OF_ETH_TYPE must match 0x86dd exactly.
    #  *   OXM_OF_IP_PROTO must match 58 exactly.
    #  *   OXM_OF_ICMPV6_TYPE must be exactly 135.
    #  *
    #  * Format: 48-bit Ethernet MAC address.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    OXM_OF_IPV6_ND_SLL = OXM_HEADER  (0x8000, OFPXMT_OFB_IPV6_ND_SLL, 6),
    
    #===============================================================================
    # /* The target link-layer address option in an IPv6 Neighbor Discovery
    #  * message.
    #  *
    #  * Prereqs:
    #  *   OXM_OF_ETH_TYPE must match 0x86dd exactly.
    #  *   OXM_OF_IP_PROTO must match 58 exactly.
    #  *   OXM_OF_ICMPV6_TYPE must be exactly 136.
    #  *
    #  * Format: 48-bit Ethernet MAC address.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    OXM_OF_IPV6_ND_TLL = OXM_HEADER  (0x8000, OFPXMT_OFB_IPV6_ND_TLL, 6),
    
    #===============================================================================
    # /* The LABEL in the first MPLS shim header.
    #  *
    #  * Prereqs:
    #  *   OXM_OF_ETH_TYPE must match 0x8847 or 0x8848 exactly.
    #  *
    #  * Format: 32-bit integer in network byte order with 12 most-significant
    #  * bits forced to 0. Only the lower 20 bits have meaning.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    OXM_OF_MPLS_LABEL = OXM_HEADER  (0x8000, OFPXMT_OFB_MPLS_LABEL, 4),
    
    #===============================================================================
    # /* The TC in the first MPLS shim header.
    #  *
    #  * Prereqs:
    #  *   OXM_OF_ETH_TYPE must match 0x8847 or 0x8848 exactly.
    #  *
    #  * Format: 8-bit integer with 5 most-significant bits forced to 0.
    #  * Only the lower 3 bits have meaning.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    OXM_OF_MPLS_TC = OXM_HEADER  (0x8000, OFPXMT_OFB_MPLS_TC, 1),
    
    #===============================================================================
    # /* The BoS bit in the first MPLS shim header.
    #  *
    #  * Prereqs:
    #  *   OXM_OF_ETH_TYPE must match 0x8847 or 0x8848 exactly.
    #  *
    #  * Format: 8-bit integer with 7 most-significant bits forced to 0.
    #  * Only the lowest bit have a meaning.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    OXM_OF_MPLS_BOS = OXM_HEADER  (0x8000, OFPXMT_OFB_MPLS_BOS, 1),
    
    #===============================================================================
    # /* IEEE 802.1ah I-SID.
    #  *
    #  * For a packet with a PBB header, this is the I-SID from the
    #  * outermost service tag.
    #  *
    #  * Prereqs:
    #  *   OXM_OF_ETH_TYPE must match 0x88E7 exactly.
    #  *
    #  * Format: 24-bit integer in network byte order.
    #  *
    #  * Masking: Arbitrary masks. */
    #===============================================================================
    OXM_OF_PBB_ISID = OXM_HEADER  (0x8000, OFPXMT_OFB_PBB_ISID, 3),
    OXM_OF_PBB_ISID_W = OXM_HEADER_W(0x8000, OFPXMT_OFB_PBB_ISID, 3),
    
    #===============================================================================
    # /* Logical Port Metadata.
    #  *
    #  * Metadata associated with a logical port.
    #  * If the logical port performs encapsulation and decapsulation, this
    #  * is the demultiplexing field from the encapsulation header.
    #  * For example, for a packet received via GRE tunnel including a (32-bit) key,
    #  * the key is stored in the low 32-bits and the high bits are zeroed.
    #  * For a MPLS logical port, the low 20 bits represent the MPLS Label.
    #  * For a VxLAN logical port, the low 24 bits represent the VNI.
    #  * If the packet is not received through a logical port, the value is 0.
    #  *
    #  * Prereqs: None.
    #  *
    #  * Format: 64-bit integer in network byte order.
    #  *
    #  * Masking: Arbitrary masks. */
    #===============================================================================
    OXM_OF_TUNNEL_ID = OXM_HEADER  (0x8000, OFPXMT_OFB_TUNNEL_ID, 8),
    OXM_OF_TUNNEL_ID_W = OXM_HEADER_W(0x8000, OFPXMT_OFB_TUNNEL_ID, 8),
    
    #===============================================================================
    # /* The IPv6 Extension Header pseudo-field.
    #  *
    #  * Prereqs:
    #  *   OXM_OF_ETH_TYPE must match 0x86dd exactly
    #  *
    #  * Format: 16-bit integer with 7 most-significant bits forced to 0.
    #  * Only the lower 9 bits have meaning.
    #  *
    #  * Masking: Maskable. */
    #===============================================================================
    OXM_OF_IPV6_EXTHDR = OXM_HEADER  (0x8000, OFPXMT_OFB_IPV6_EXTHDR, 2),
    OXM_OF_IPV6_EXTHDR_W = OXM_HEADER_W(0x8000, OFPXMT_OFB_IPV6_EXTHDR, 2)
    )
    
    '''
    /* Bit definitions for IPv6 Extension Header pseudo-field. */
    '''
    ofp_ipv6exthdr_flags = enum('ofp_ipv6exthdr_flags', globals(), uint16, True,
        OFPIEH_NONEXT = 1 << 0,   #  /* "No next header" encountered. */
        OFPIEH_ESP    = 1 << 1,   #  /* Encrypted Sec Payload header present. */
        OFPIEH_AUTH   = 1 << 2,   #  /* Authentication header present. */
        OFPIEH_DEST   = 1 << 3,   #  /* 1 or 2 dest headers present. */
        OFPIEH_FRAG   = 1 << 4,   #  /* Fragment header present. */
        OFPIEH_ROUTER = 1 << 5,   #  /* Router header present. */
        OFPIEH_HOP    = 1 << 6,   #  /* Hop-by-hop header present. */
        OFPIEH_UNREP  = 1 << 7,   #  /* Unexpected repeats encountered. */
        OFPIEH_UNSEQ  = 1 << 8    #  /* Unexpected sequencing encountered. */
    )
    
    ofp_oxm = nstruct(
        (ofp_oxm_header, 'header'),
        name = 'ofp_oxm',
        padding = 1,
        size = lambda x: OXM_LENGTH(x.header) + 4
    )
    
    '''
    /* Header for OXM experimenter match fields.
     * The experimenter class should not use OXM_HEADER() macros for defining
     * fields due to this extra header. */
    '''
    ofp_oxm_experimenter = nstruct(
        (uint32, 'experimenter'),   #   /* Experimenter ID which takes the same form as in struct ofp_experimenter_header. */
        base = ofp_oxm,
        name = 'ofp_oxm_experimenter',
        criteria = lambda x: OXM_CLASS(x.header) == OFPXMC_EXPERIMENTER,
        init = packvalue(OXM_HEADER(OFPXMC_EXPERIMENTER, 0, 4), 'header')
    )
    
    ofp_oxm_nomask = nstruct(
        (raw, 'value'),
        base = ofp_oxm,
        criteria = lambda x: not OXM_HASMASK(x.header),
        init = packvalue(OXM_OF_IN_PORT, 'header'),
        name = 'ofp_oxm_nomask'
    )
    
    _ofp_oxm_mask_value = nstruct(
        (raw, 'value'),
        name = 'ofp_oxm_mask_value',
        size = lambda x: OXM_LENGTH(x.header) // 2,
        padding = 1
    )
    
    ofp_oxm_mask = nstruct(
        (_ofp_oxm_mask_value,),
        (raw, 'mask'),
        base = ofp_oxm,
        criteria = lambda x: OXM_HASMASK(x.header),
        init = packvalue(OXM_OF_METADATA_W, 'header'),
        name = 'ofp_oxm_mask',
    )
    
    
    def create_oxm(header, value = None, mask = None):
        if OXM_HASMASK(header):
            oxm = ofp_oxm_mask.new()
            size = OXM_LENGTH(header) // 2
        else:
            oxm = ofp_oxm_nomask.new()
            size = OXM_LENGTH(header)
        oxm.header = header
        oxm.value = create_binary(value, size)
        if OXM_HASMASK(header):
            oxm.mask = create_binary(mask, size)
        oxm._pack()
        oxm._autosubclass()
        return oxm
    
    ofp_match_oxm = nstruct(
        (ofp_oxm[0], 'oxm_fields'),
        base = ofp_match,
        criteria = lambda x: x.type == OFPMT_OXM,
        init = packvalue(OFPMT_OXM, 'type'),
        name = 'ofp_match_oxm'
    )
    
    ofp_oxm_mask_ipv4 = nstruct(name = 'ofp_oxm_mask_ipv4',
                                base = ofp_oxm_mask,
                                criteria = lambda x: x.header in (OXM_OF_IPV4_SRC_W, OXM_OF_IPV4_DST_W, OXM_OF_ARP_SPA_W, OXM_OF_ARP_TPA_W),
                                init = packvalue(OXM_OF_IPV4_SRC_W, 'header'),
                                extend = {'value' : ip4_addr_bytes, 'mask' : ip4_addr_bytes}
                                )
    
    ofp_oxm_nomask_ipv4 = nstruct(name = 'ofp_oxm_nomask_ipv4',
                                base = ofp_oxm_nomask,
                                criteria = lambda x: x.header in (OXM_OF_IPV4_SRC, OXM_OF_IPV4_DST, OXM_OF_ARP_SPA, OXM_OF_ARP_TPA),
                                init = packvalue(OXM_OF_IPV4_SRC, 'header'),
                                extend = {'value' : ip4_addr_bytes}
                                )
    
    ofp_oxm_mask_eth = nstruct(name = 'ofp_oxm_mask_eth',
                               base = ofp_oxm_mask,
                               criteria = lambda x: x.header in (OXM_OF_ETH_SRC_W, OXM_OF_ETH_DST_W),
                                init = packvalue(OXM_OF_ETH_SRC_W, 'header'),
                               extend = {'value' : mac_addr_bytes, 'mask' : mac_addr_bytes})
    
    ofp_oxm_nomask_eth = nstruct(name = 'ofp_oxm_nomask_eth',
                               base = ofp_oxm_nomask,
                               criteria = lambda x: x.header in (OXM_OF_ETH_SRC, OXM_OF_ETH_DST, OXM_OF_IPV6_ND_SLL, OXM_OF_IPV6_ND_TLL, OXM_OF_ARP_SHA, OXM_OF_ARP_THA),
                                init = packvalue(OXM_OF_ETH_SRC, 'header'),
                               extend = {'value' : mac_addr_bytes})
    
    ofp_port_no_raw = _rawtype()
    
    ofp_port_no_raw.formatter = lambda x: ofp_port_no.formatter(ofp_port_no.parse(x)[0])
    
    ofp_oxm_nomask_port = nstruct(name = 'ofp_oxm_nomask_port',
                                    base = ofp_oxm_nomask,
                                    criteria = lambda x: x.header == OXM_OF_IN_PORT,
                                    init = packvalue(OXM_OF_IN_PORT, 'header'),
                                    extend = {'value': ofp_port_no_raw}
                                    )
    
    ofp_ipv6exthdr_flags_raw = _rawtype()
    
    ofp_ipv6exthdr_flags_raw.formatter = lambda x: ofp_ipv6exthdr_flags.formatter(ofp_ipv6exthdr_flags.parse(x)[0])
    
    ofp_oxm_nomask_exthdr = nstruct(name = 'ofp_oxm_nomask_exthdr',
                                    base = ofp_oxm_nomask,
                                    criteria = lambda x: x.header == OXM_OF_IPV6_EXTHDR,
                                    init = packvalue(OXM_OF_IPV6_EXTHDR, 'header'),
                                    extend = {'value': ofp_ipv6exthdr_flags_raw})
    
    ofp_oxm_mask_exthdr = nstruct(name = 'ofp_oxm_mask_exthdr',
                                    base = ofp_oxm_mask,
                                    criteria = lambda x: x.header == OXM_OF_IPV6_EXTHDR_W,
                                    init = packvalue(OXM_OF_IPV6_EXTHDR_W, 'header'),
                                    extend = {'value': ofp_ipv6exthdr_flags_raw, 'mask': ofp_ipv6exthdr_flags_raw})
    
    ethtype_raw = _rawtype()
    ethtype_raw.formatter = lambda x: ethertype.formatter(ethertype.parse(x)[0])
    
    ofp_oxm_nomask_ethertype = nstruct(name = 'ofp_oxm_nomask_ethertype',
                                       base = ofp_oxm_nomask,
                                       criteria = lambda x: x.header == OXM_OF_ETH_TYPE,
                                       init = packvalue(OXM_OF_ETH_TYPE, 'header'),
                                       extend = {'value': ethtype_raw})
    
    arpop_raw = _rawtype()
    
    arpop_raw.formatter = lambda x: arp_op_code.formatter(arp_op_code.parse(x)[0])
    
    ofp_oxm_nomask_arpopcode = nstruct(name = 'ofp_oxm_nomask_arpopcode',
                                       base = ofp_oxm_nomask,
                                       criteria = lambda x: x.header == OXM_OF_ARP_OP,
                                       init = packvalue(OXM_OF_ARP_OP, 'header'),
                                       extend = {'value': arpop_raw})
    
    ip_protocol_raw = _rawtype()
    
    ip_protocol_raw.formatter = lambda x: ip_protocol.formatter(ip_protocol.parse(x)[0])
    
    ofp_oxm_nomask_ip_protocol = nstruct(name = 'ofp_oxm_nomask_ip_protocol',
                                         base = ofp_oxm_nomask,
                                         criteria = lambda x: x.header == OXM_OF_IP_PROTO,
                                         init = packvalue(OXM_OF_IP_PROTO, 'header'),
                                         extend = {'value': ip_protocol_raw})
    
    ofp_oxm_nomask_ipv6 = nstruct(name = 'ofp_oxm_nomask_ipv6',
                                  base = ofp_oxm_nomask,
                                  criteria = lambda x: x.header in (OXM_OF_IPV6_SRC, OXM_OF_IPV6_DST, OXM_OF_IPV6_ND_TARGET),
                                  init = packvalue(OXM_OF_IPV6_SRC, 'header'),
                                  extend = {'value': ip6_addr_bytes})
    ofp_oxm_mask_ipv6 = nstruct(name = 'ofp_oxm_mask_ipv6',
                                  base = ofp_oxm_mask,
                                  criteria = lambda x: x.header in (OXM_OF_IPV6_SRC_W, OXM_OF_IPV6_DST_W),
                                  init = packvalue(OXM_OF_IPV6_SRC, 'header'),
                                  extend = {'value': ip6_addr_bytes, 'mask': ip6_addr_bytes})
    
    '''
    /* ## ----------------- ## */
    /* ## OpenFlow Actions. ## */
    /* ## ----------------- ## */
    '''
    
    ofp_action_type = enum('ofp_action_type', globals(), uint16,
        OFPAT_OUTPUT       = 0,  #/* Output to switch port. */
        OFPAT_COPY_TTL_OUT = 11, #/* Copy TTL "outwards" -- from next-to-outermost to outermost */
        OFPAT_COPY_TTL_IN  = 12, #/* Copy TTL "inwards" -- from outermost to next-to-outermost */
        OFPAT_SET_MPLS_TTL = 15, #/* MPLS TTL */
        OFPAT_DEC_MPLS_TTL = 16, #/* Decrement MPLS TTL */
    
        OFPAT_PUSH_VLAN    = 17, #/* Push a new VLAN tag */
        OFPAT_POP_VLAN     = 18, #/* Pop the outer VLAN tag */
        OFPAT_PUSH_MPLS    = 19, #/* Push a new MPLS tag */
        OFPAT_POP_MPLS     = 20, #/* Pop the outer MPLS tag */
        OFPAT_SET_QUEUE    = 21, #/* Set queue id when outputting to a port */
        OFPAT_GROUP        = 22, #/* Apply group. */
        OFPAT_SET_NW_TTL   = 23, #/* IP TTL. */
        OFPAT_DEC_NW_TTL   = 24, #/* Decrement IP TTL. */
        OFPAT_SET_FIELD    = 25, #/* Set a header field using OXM TLV format. */
        OFPAT_PUSH_PBB     = 26, #/* Push a new PBB service tag (I-TAG) */
        OFPAT_POP_PBB      = 27, #/* Pop the outer PBB service tag (I-TAG) */
        OFPAT_EXPERIMENTER = 0xffff
    )
    
    '''
    /* Action header that is common to all actions.  The length includes the
     * header and any padding used to make the action 64-bit aligned.
     * NB: The length of an action *must* always be a multiple of eight. */
    '''
    ofp_action = nstruct((ofp_action_type, 'type'),
                        (uint16, 'len'),
                        name = 'ofp_action',
                        size = sizefromlen(512, 'len'),
                        prepack = packsize('len'),
                        classifier = lambda x: x.type
                        )
    
    ofp_controller_max_len = enum('ofp_controller_max_len', globals(), uint16,
        OFPCML_MAX       = 0xffe5, #/* maximum max_len value which can be used to request a specific byte length. */
        OFPCML_NO_BUFFER = 0xffff  #/* indicates that no buffering should be applied and the whole packet is to be sent to the controller. */
    )
    
    
    '''
    /* Action structure for OFPAT_OUTPUT, which sends packets out 'port'.
     * When the 'port' is the OFPP_CONTROLLER, 'max_len' indicates the max
     * number of bytes to send.  A 'max_len' of zero means no bytes of the
     * packet should be sent. A 'max_len' of OFPCML_NO_BUFFER means that
     * the packet is not buffered and the complete packet is to be sent to
     * the controller. */
    '''
    ofp_action_output = nstruct((ofp_port_no, 'port'),
                                (ofp_controller_max_len, 'max_len'),
                                (uint8[6],),
                                name = 'ofp_action_output',
                                base = ofp_action,
                                criteria = lambda x: x.type == OFPAT_OUTPUT,
                                classifyby = (OFPAT_OUTPUT,),
                                init = packvalue(OFPAT_OUTPUT, 'type'))
    
    '''
    /* Action structure for OFPAT_SET_MPLS_TTL. */
    '''
    ofp_action_mpls_ttl = nstruct(
        (uint8, 'mpls_ttl'),             #  /* MPLS TTL */
        (uint8[3],),
        name = 'ofp_action_mpls_ttl',
        base = ofp_action,
        criteria = lambda x: x.type == OFPAT_SET_MPLS_TTL,
        classifyby = (OFPAT_SET_MPLS_TTL,),
        init = packvalue(OFPAT_SET_MPLS_TTL, 'type')
    )
    
    '''
    /* Action structure for OFPAT_PUSH_VLAN/MPLS/PBB. */
    '''
    ofp_action_push = nstruct(
        (ethertype, 'ethertype'),          #   /* Ethertype */
        (uint8[2],),
        name = 'ofp_action_push',
        base = ofp_action,
        criteria = lambda x: x.type == OFPAT_PUSH_VLAN or x.type == OFPAT_PUSH_MPLS or x.type == OFPAT_PUSH_PBB,
        classifyby = (OFPAT_PUSH_VLAN, OFPAT_PUSH_MPLS, OFPAT_PUSH_PBB),
        init = packvalue(OFPAT_PUSH_VLAN, 'type')
    )
    
    '''
    /* Action structure for OFPAT_POP_MPLS. */
    '''
    ofp_action_pop_mpls = nstruct(
        (ethertype, 'ethertype'),          #   /* Ethertype */
        (uint8[2],),
        name = 'ofp_action_pop_mpls',
        base = ofp_action,
        criteria = lambda x: x.type == OFPAT_POP_MPLS,
        classifyby = (OFPAT_POP_MPLS,),
        init = packvalue(OFPAT_POP_MPLS, 'type')
    )
    
    '''
    /* Action structure for OFPAT_GROUP. */
    '''
    ofp_action_group = nstruct(
        (uint32, 'group_id'),           #   /* Group identifier. */
        name = 'ofp_action_group',
        base = ofp_action,
        criteria = lambda x: x.type == OFPAT_GROUP,
        classifyby = (OFPAT_GROUP,),
        init = packvalue(OFPAT_GROUP, 'type')
    )
    
    '''
    /* Action structure for OFPAT_SET_NW_TTL. */
    '''
    ofp_action_nw_ttl = nstruct(
        (uint8, 'nw_ttl'),              #   /* IP TTL */
        (uint8[3],),
        base = ofp_action,
        criteria = lambda x: x.type == OFPAT_SET_NW_TTL,
        classifyby = (OFPAT_SET_NW_TTL,),
        init = packvalue(OFPAT_SET_NW_TTL, 'type'),
        name = 'ofp_action_nw_ttl'
    )
    
    '''
    /* Action structure for OFPAT_SET_FIELD. */
    '''
    ofp_action_set_field = nstruct(
        (ofp_oxm, 'field'),
        base = ofp_action,
        criteria = lambda x: x.type == OFPAT_SET_FIELD,
        classifyby = (OFPAT_SET_FIELD,),
        init = packvalue(OFPAT_SET_FIELD, 'type'),
        name = 'ofp_action_set_field'
    )
    
    '''
    /* Action header for OFPAT_EXPERIMENTER.
     * The rest of the body is experimenter-defined. */
    '''
    ofp_action_experimenter = nstruct(
        (uint32, 'experimenter'),     #  /* Experimenter ID which takes the same
    #                                       form as in struct
    #                                       ofp_experimenter_header. */
        base = ofp_action,
        criteria = lambda x: x.type == OFPAT_EXPERIMENTER,
        classifyby = (OFPAT_EXPERIMENTER,),
        init = packvalue(OFPAT_EXPERIMENTER, 'type'),
        name = 'ofp_action_experimenter'
    )
    
    '''
    /* ## ---------------------- ## */
    /* ## OpenFlow Instructions. ## */
    /* ## ---------------------- ## */
    '''
    ofp_instruction_type = enum('ofp_instruction_type', globals(), uint16,
        OFPIT_GOTO_TABLE = 1,      # /* Setup the next table in the lookup pipeline */
        OFPIT_WRITE_METADATA = 2,  # /* Setup the metadata field for use later in pipeline */
        OFPIT_WRITE_ACTIONS = 3,   # /* Write the action(s) onto the datapath action set */
        OFPIT_APPLY_ACTIONS = 4,   # /* Applies the action(s) immediately */
        OFPIT_CLEAR_ACTIONS = 5,   # /* Clears all actions from the datapath action set */
        OFPIT_METER = 6,           # /* Apply meter (rate limiter) */
    
        OFPIT_EXPERIMENTER = 0xFFFF # /* Experimenter instruction */
    )
    
    '''
    /* Instruction header that is common to all instructions.  The length includes
     * the header and any padding used to make the instruction 64-bit aligned.
     * NB: The length of an instruction *must* always be a multiple of eight. */
    '''
    
    ofp_instruction = nstruct(
        (ofp_instruction_type, 'type'),           #     /* Instruction type */
        (uint16, 'len'),            #     /* Length of this struct in bytes. */
        name = 'ofp_instruction',
        size = sizefromlen(65536, 'len'),
        prepack = packsize('len'),
        classifier = lambda x: x.type
    )
    
    '''
    /* Instruction structure for OFPIT_GOTO_TABLE */
    '''
    ofp_instruction_goto_table = nstruct(
        (uint8, 'table_id'),        #     /* Set next table in the lookup pipeline */
        (uint8[3],),                   #     /* Pad to 64 bits. */
        base = ofp_instruction,
        name = 'ofp_instruction_goto_table',
        criteria = lambda x: x.type == OFPIT_GOTO_TABLE,
        classifyby = (OFPIT_GOTO_TABLE,),
        init = packvalue(OFPIT_GOTO_TABLE, 'type')
    )
    
    '''
    /* Instruction structure for OFPIT_WRITE_METADATA */
    '''
    ofp_instruction_write_metadata = nstruct(
        (uint8[4],),                  # /* Align to 64-bits */
        (uint64, 'metadata'),         # /* Metadata value to write */
        (uint64, 'metadata_mask'),    # /* Metadata write bitmask */
        base = ofp_instruction,
        name = 'ofp_instruction_write_metadata',
        criteria = lambda x: x.type == OFPIT_WRITE_METADATA,
        classifyby = (OFPIT_WRITE_METADATA,),
        init = packvalue(OFPIT_WRITE_METADATA, 'type')
    )
    
    '''
    /* Instruction structure for OFPIT_WRITE/APPLY/CLEAR_ACTIONS */
    '''
    ofp_instruction_actions = nstruct(
        (uint8[4],),                  # /* Align to 64-bits */
        (ofp_action[0], 'actions'),   # /* 0 or more actions associated with OFPIT_WRITE_ACTIONS and OFPIT_APPLY_ACTIONS */
        base = ofp_instruction,
        name = 'ofp_instruction_actions',
        criteria = lambda x: x.type == OFPIT_WRITE_ACTIONS or x.type == OFPIT_APPLY_ACTIONS or x.type == OFPIT_CLEAR_ACTIONS,
        classifyby = (OFPIT_WRITE_ACTIONS, OFPIT_APPLY_ACTIONS, OFPIT_CLEAR_ACTIONS),
        init = packvalue(OFPIT_APPLY_ACTIONS, 'type')
    )
    
    '''
    /* Instruction structure for OFPIT_METER */
    '''
    ofp_instruction_meter = nstruct(
        (uint32, 'meter_id'),          # /* Meter instance. */
        base = ofp_instruction,
        name = 'ofp_instruction_meter',
        criteria = lambda x: x.type == OFPIT_METER,
        classifyby = (OFPIT_METER,),
        init = packvalue(OFPIT_METER, 'type')
    )
    
    '''
    /* Instruction structure for experimental instructions */
    '''
    ofp_instruction_experimenter = nstruct(
        (uint32, 'experimenter'),  # /* Experimenter ID which takes the same form as in struct ofp_experimenter_header. */
    #    /* Experimenter-defined arbitrary additional data. */
        base = ofp_instruction,
        name = 'ofp_instruction_experimenter',
        criteria = lambda x: x.type == OFPIT_EXPERIMENTER,
        classifyby = (OFPIT_EXPERIMENTER,),
        init = packvalue(OFPIT_EXPERIMENTER, 'type')
    )
    
    '''
    /* Value used in "idle_timeout" and "hard_timeout" to indicate that the entry
     * is permanent. */
    '''
    OFP_FLOW_PERMANENT = 0
    
    '''
    /* By default, choose a priority in the middle. */
    '''
    OFP_DEFAULT_PRIORITY = 0x8000
    
    ofp_flow_mod_flags = ofp_flow_mod_flags.extend(globals(),
        OFPFF_RESET_COUNTS  = 1 << 2, # /* Reset flow packet and byte counts. */
        OFPFF_NO_PKT_COUNTS = 1 << 3, # /* Don't keep track of packet count. */
        OFPFF_NO_BYT_COUNTS = 1 << 4, # /* Don't keep track of byte count. */
    )
    
    
    '''
    /* Special buffer-id to indicate 'no buffer' */
    '''
    ofp_buffer_id = enum('ofp_buffer_id', globals(), uint32,
                         OFP_NO_BUFFER = 0xffffffff
    )
    
    '''
    /* Flow setup and teardown (controller -> datapath). */
    '''
    ofp_flow_mod = nstruct(
        (uint64, 'cookie'),            #  /* Opaque controller-issued identifier. */
    # /* Mask used to restrict the cookie bits
    # that must match when the command is
    # OFPFC_MODIFY* or OFPFC_DELETE*. A value
    # of 0 indicates no restriction. */
        (uint64, 'cookie_mask'),
    # /* ID of the table to put the flow in.
    # For OFPFC_DELETE_* commands, OFPTT_ALL
    # can also be used to delete matching
    # flows from all tables. */
        (ofp_table, 'table_id'),
        (ofp_flow_mod_command.astype(uint8), 'command'),           #  /* One of OFPFC_*. */
        (uint16, 'idle_timeout'),     #  /* Idle time before discarding (seconds). */
        (uint16, 'hard_timeout'),     #  /* Max time before discarding (seconds). */
        (uint16, 'priority'),         #  /* Priority level of flow entry. */
    # /* Buffered packet to apply to, or
    # OFP_NO_BUFFER.
    # Not meaningful for OFPFC_DELETE*. */
        (ofp_buffer_id, 'buffer_id'),
    # /* For OFPFC_DELETE* commands, require
    # matching entries to include this as an
    # output port.  A value of OFPP_ANY
    # indicates no restriction. */
        (ofp_port_no, 'out_port'),
    # /* For OFPFC_DELETE* commands, require
    # matching entries to include this as an
    # output group.  A value of OFPG_ANY
    # indicates no restriction. */
        (ofp_group, 'out_group'),
        (ofp_flow_mod_flags, 'flags'),            #  /* Bitmap of OFPFF_* flags. */
        (uint8[2],),
        (ofp_match, 'match'),         #  /* Fields to match. Variable size. */
    #    /* The variable size and padded match is always followed by instructions. */
    # /* Instruction set - 0 or more.
    # The length of the instruction
    # set is inferred from the
    # length field in the header. */
        (ofp_instruction[0], 'instructions'),
        base = ofp_msg,
        name = 'ofp_flow_mod',
        criteria = lambda x: x.header.type == OFPT_FLOW_MOD,
        classifyby = (OFPT_FLOW_MOD,),
        init = packvalue(OFPT_FLOW_MOD, 'header', 'type')
    )
    
    
    '''
    /* Group commands */
    '''
    ofp_group_mod_command = enum('ofp_group_mod_command', globals(),uint16,
        OFPGC_ADD    = 0,       # /* New group. */
        OFPGC_MODIFY = 1,       # /* Modify all matching groups. */
        OFPGC_DELETE = 2,       # /* Delete all matching groups. */
    )
    
    '''
    /* Bucket for use in groups. */
    '''
    ofp_bucket = nstruct(
        (uint16, 'len'),
    #                                    /* Length of the bucket in bytes, including
    #                                       this header and any padding to make it
    #                                       64-bit aligned. */
        (uint16, 'weight'),               
    #                                    /* Relative weight of bucket.  Only
    #                                       defined for select groups. */
        (ofp_port_no, 'watch_port'),
    #                                    /* Port whose state affects whether this
    #                                       bucket is live.  Only required for fast
    #                                       failover groups. */
        (ofp_group, 'watch_group'),
    #                                    /* Group whose state affects whether this
    #                                       bucket is live.  Only required for fast
    #                                       failover groups. */
        (uint8[4],),
        (ofp_action[0], 'actions'),
    #                                    /* 0 or more actions associated with
    #                                            the bucket - The action list length
    #                                            is inferred from the length
    #                                            of the bucket. */
        size = sizefromlen(65536, 'len'),
        prepack = packsize('len'),
        init = lambda x: (packvalue(OFPP_ANY, 'watch_port')(x), packvalue(OFPG_ANY, 'watch_group')(x)),
        name = 'ofp_bucket'
    )
    
    '''
    /* Group types.  Values in the range [128, 255] are reserved for experimental
     * use. */
    '''
    ofp_group_type = enum('ofp_group_type', globals(),uint8,
        OFPGT_ALL      = 0, # /* All (multicast/broadcast) group.  */
        OFPGT_SELECT   = 1, # /* Select group. */
        OFPGT_INDIRECT = 2, # /* Indirect group. */
        OFPGT_FF       = 3, # /* Fast failover group. */
    )
    
    '''
    /* Group setup and teardown (controller -> datapath). */
    '''
    ofp_group_mod = nstruct(
        (ofp_group_mod_command, 'command'),     #        /* One of OFPGC_*. */
        (ofp_group_type, 'type'),         #        /* One of OFPGT_*. */
        (uint8,),                #        /* Pad to 64 bits. */
        (ofp_group, 'group_id'),    #        /* Group identifier. */
        (ofp_bucket[0], 'buckets'),    #  /* The length of the bucket array is inferred from the length field in the header. */
        base = ofp_msg,
        criteria = lambda x: x.header.type == OFPT_GROUP_MOD,
        classifyby = (OFPT_GROUP_MOD,),
        init = packvalue(OFPT_GROUP_MOD, 'header', 'type'),
        name = 'ofp_group_mod'
    )
    
    '''
    /* Send packet (controller -> datapath). */
    '''
    
    def _ofp_packet_out_actions_packsize(x):
        x.actions_len = x._realsize() - 8
    ofp_packet_out_actions = nstruct(
        (uint16, 'actions_len'),
        (uint8[6],),
        (ofp_action[0], 'actions'),
        name = 'ofp_packet_out_actions',
        size = lambda x: x.actions_len + 8,
        prepack = _ofp_packet_out_actions_packsize,
        padding = 1)
    
    ofp_packet_out = nstruct(
        (ofp_buffer_id, 'buffer_id'),    #    /* ID assigned by datapath (OFP_NO_BUFFER if none). */
        (ofp_port_no, 'in_port'),      #    /* Packet's input port or OFPP_CONTROLLER. */
        (ofp_packet_out_actions,),
        (raw, 'data'),
        base = ofp_msg,
        criteria = lambda x: x.header.type == OFPT_PACKET_OUT,
        classifyby = (OFPT_PACKET_OUT,),
        init = packvalue(OFPT_PACKET_OUT, 'header', 'type'),
        name = 'ofp_packet_out'
    )
    
    '''
    /* Packet received on port (datapath -> controller). */
    '''
    ofp_packet_in = nstruct(
        (ofp_buffer_id, 'buffer_id'),  #   /* ID assigned by datapath. */
        (uint16, 'total_len'),  #   /* Full length of frame. */
        (ofp_packet_in_reason, 'reason'),      #   /* Reason packet is being sent (one of OFPR_*) */
        (uint8, 'table_id'),    #   /* ID of the table that was looked up */
        (uint64, 'cookie'),     #   /* Cookie of the flow entry that was looked up. */
        (ofp_match, 'match'),     #   /* Packet metadata. Variable size. */
    #    /* The variable size and padded match is always followed by:
    #   - Exactly 2 all-zero padding bytes, then
    #   - An Ethernet frame whose length is inferred from header.length.
    #     The padding bytes preceding the Ethernet frame ensure that the IP
    #     header (if any) following the Ethernet header is 32-bit aligned.
    #*/
        (uint8[2],),            #   /* Align to 64 bit + 16 bit */
        (raw, 'data'),          #   /* Ethernet frame */
        base = ofp_msg,
        criteria = lambda x: x.header.type == OFPT_PACKET_IN,
        classifyby = (OFPT_PACKET_IN,),
        init = packvalue(OFPT_PACKET_IN, 'header', 'type'),
        name = 'ofp_packet_in'
    )

    def get_oxm(fields, header):
        v = [o.value for o in fields if o.header == header]
        if v:
            return v[0]
        else:
            return create_binary(0, OXM_LENGTH(header))
    
    '''
    /* Flow removed (datapath -> controller). */
    '''
    ofp_flow_removed = nstruct(
        (uint64, 'cookie'),     #     /* Opaque controller-issued identifier. */
    
        (uint16, 'priority'),   #     /* Priority level of flow entry. */
        (ofp_flow_removed_reason, 'reason'),      #     /* One of OFPRR_*. */
        (uint8, 'table_id'),    #     /* ID of the table */
    
        (uint32, 'duration_sec'), #   /* Time flow was alive in seconds. */
        (uint32, 'duration_nsec'),#   /* Time flow was alive in nanoseconds beyond duration_sec. */
        (uint16, 'idle_timeout'), #   /* Idle timeout from original flow mod. */
        (uint16, 'hard_timeout'), #   /* Hard timeout from original flow mod. */
        (uint64, 'packet_count'),
        (uint64, 'byte_count'),
        (ofp_match, 'match'),     #   /* Description of fields. Variable size. */
        base = ofp_msg,
        criteria = lambda x: x.header.type == OFPT_FLOW_REMOVED,
        classifyby = (OFPT_FLOW_REMOVED,),
        init = packvalue(OFPT_FLOW_REMOVED, 'header', 'type'),
        name = 'ofp_flow_removed'
    )
    
    '''
    /* Meter numbering. Flow meters can use any number up to OFPM_MAX. */
    '''
    ofp_meter = enum('ofp_meter', globals(),uint32,
    
    #    /* Virtual meters. */
        OFPM_SLOWPATH   = 0xfffffffd,  # /* Meter for slow datapath. */
        OFPM_CONTROLLER = 0xfffffffe,  # /* Meter for controller connection. */
        OFPM_ALL        = 0xffffffff,  # /* Represents all meters for stat requests commands. */
    )
    
    #    /* Last usable meter. */
    OFPM_MAX        = 0xffff0000
    
    
    '''
    /* Meter band types */
    '''
    ofp_meter_band_type = enum('ofp_meter_band_type', globals(), uint16,
        OFPMBT_DROP            = 1,     # /* Drop packet. */
        OFPMBT_DSCP_REMARK     = 2,     # /* Remark DSCP in the IP header. */
        OFPMBT_EXPERIMENTER    = 0xFFFF # /* Experimenter meter band. */
    )
    
    '''
    /* Common header for all meter bands */
    '''
    ofp_meter_band = nstruct(
        (ofp_meter_band_type, 'type'),   # /* One of OFPMBT_*. */
        (uint16, 'len'),    # /* Length in bytes of this band. */
        (uint32, 'rate'),   # /* Rate for this band. */
        (uint32, 'burst_size'), # /* Size of bursts. */
        size = sizefromlen(4096, 'len'),
        prepack = packsize('len'),
        name = 'ofp_meter_band',
        classifier = lambda x: x.type
    )
    
    '''
    /* OFPMBT_DROP band - drop packets */
    '''
    ofp_meter_band_drop = nstruct(
        (uint8[4],),
        base = ofp_meter_band,
        criteria = lambda x: x.type == OFPMBT_DROP,
        classifyby = (OFPMBT_DROP,),
        init = packvalue(OFPMBT_DROP, 'type'),
        name = 'ofp_meter_band_drop'
    )
    
    '''
    /* OFPMBT_DSCP_REMARK band - Remark DSCP in the IP header */
    '''
    ofp_meter_band_dscp_remark = nstruct(
        (uint8, 'prec_level'), # /* Number of drop precedence level to add. */
        (uint8[3],),
        base = ofp_meter_band,
        criteria = lambda x: x.type == OFPMBT_DSCP_REMARK,
        classifyby = (OFPMBT_DSCP_REMARK,),
        init = packvalue(OFPMBT_DSCP_REMARK, 'type'),
        name = 'ofp_meter_band_dscp_remark'
    )
    
    
    '''
    /* OFPMBT_EXPERIMENTER band - Experimenter type.
     * The rest of the band is experimenter-defined. */
    '''
    ofp_meter_band_experimenter = nstruct(
    #/* Experimenter ID which takes the same
    # form as in struct
    # ofp_experimenter_header. */
        (uint32, 'experimenter'),
        base = ofp_meter_band,
        criteria = lambda x: x.type == OFPMBT_EXPERIMENTER,
        classifyby = (OFPMBT_EXPERIMENTER,),
        init = packvalue(OFPMBT_EXPERIMENTER, 'type'),
        name = 'ofp_meter_band_experimenter'
    )
    
    '''
    /* Meter commands */
    '''
    ofp_meter_mod_command = enum('ofp_meter_mod_command', globals(),uint16,
        OFPMC_ADD = 0,            #  /* New meter. */
        OFPMC_MODIFY = 1,         #  /* Modify specified meter. */
        OFPMC_DELETE = 2,         #  /* Delete specified meter. */
    )
    
    '''
    /* Meter configuration flags */
    '''
    ofp_meter_flags = enum('ofp_meter_flags', globals(),uint16,
        OFPMF_KBPS    = 1 << 0,   #  /* Rate value in kb/s (kilo-bit per second). */
        OFPMF_PKTPS   = 1 << 1,   #  /* Rate value in packet/sec. */
        OFPMF_BURST   = 1 << 2,   #  /* Do burst size. */
        OFPMF_STATS   = 1 << 3,   #  /* Collect statistics. */
    )
    
    '''
    /* Meter configuration. OFPT_METER_MOD. */
    '''
    ofp_meter_mod = nstruct(
        (ofp_meter_mod_command, 'command'),      #  /* One of OFPMC_*. */
        (ofp_meter_flags, 'flags'),        #  /* Bitmap of OFPMF_* flags. */
        (ofp_meter, 'meter_id'),     #  /* Meter instance. */
        (ofp_meter_band[0], 'bands'),
    #/* The band list length is inferred from the length field in the header. */
        base = ofp_msg,
        criteria = lambda x: x.header.type == OFPT_METER_MOD,
        classifyby = (OFPT_METER_MOD,),
        init = packvalue(OFPT_METER_MOD, 'header', 'type'),
        name = 'ofp_meter_mod'
    )
    
    
    '''
    /* ofp_error_msg 'code' values for OFPET_BAD_INSTRUCTION.  'data' contains at least
     * the first 64 bytes of the failed request. */
    '''
    ofp_bad_instruction_code = enum('ofp_bad_instruction_code', globals(), uint16,
        OFPBIC_UNKNOWN_INST     = 0, #/* Unknown instruction. */
        OFPBIC_UNSUP_INST       = 1, #/* Switch or table does not support the
    #                                    instruction. */
        OFPBIC_BAD_TABLE_ID     = 2, #/* Invalid Table-ID specified. */
        OFPBIC_UNSUP_METADATA   = 3, #/* Metadata value unsupported by datapath. */
        OFPBIC_UNSUP_METADATA_MASK = 4, #/* Metadata mask value unsupported by
    #                                       datapath. */
        OFPBIC_BAD_EXPERIMENTER = 5, #/* Unknown experimenter id specified. */
        OFPBIC_BAD_EXP_TYPE     = 6, #/* Unknown instruction for experimenter id. */
        OFPBIC_BAD_LEN          = 7, #/* Length problem in instructions. */
        OFPBIC_EPERM            = 8, #/* Permissions error. */
    )
    
    '''
    /* ofp_error_msg 'code' values for OFPET_BAD_MATCH.  'data' contains at least
     * the first 64 bytes of the failed request. */
    '''
    ofp_bad_match_code = enum('ofp_bad_match_code', globals(), uint16,
        OFPBMC_BAD_TYPE         = 0, # /* Unsupported match type specified by the
    #                                     match */
        OFPBMC_BAD_LEN          = 1, # /* Length problem in match. */
        OFPBMC_BAD_TAG          = 2, # /* Match uses an unsupported tag/encap. */
        OFPBMC_BAD_DL_ADDR_MASK = 3, # /* Unsupported datalink addr mask - switch
    #                                     does not support arbitrary datalink
    #                                     address mask. */
        OFPBMC_BAD_NW_ADDR_MASK = 4, # /* Unsupported network addr mask - switch
    #                                     does not support arbitrary network
    #                                     address mask. */
        OFPBMC_BAD_WILDCARDS    = 5, # /* Unsupported combination of fields masked
    #                                     or omitted in the match. */
        OFPBMC_BAD_FIELD        = 6, # /* Unsupported field type in the match. */
        OFPBMC_BAD_VALUE        = 7, # /* Unsupported value in a match field. */
        OFPBMC_BAD_MASK         = 8, # /* Unsupported mask specified in the match,
    #                                     field is not dl-address or nw-address. */
        OFPBMC_BAD_PREREQ       = 9, # /* A prerequisite was not met. */
        OFPBMC_DUP_FIELD        = 10,# /* A field type was duplicated. */
        OFPBMC_EPERM            = 11,# /* Permissions error. */
    )
    
    '''
    /* ofp_error_msg 'code' values for OFPET_FLOW_MOD_FAILED.  'data' contains
     * at least the first 64 bytes of the failed request. */
    '''
    ofp_flow_mod_failed_code = enum('ofp_flow_mod_failed_code', globals(), uint16,
        OFPFMFC_UNKNOWN      = 0,  # /* Unspecified error. */
        OFPFMFC_TABLE_FULL   = 1,  # /* Flow not added because table was full. */
        OFPFMFC_BAD_TABLE_ID = 2,  # /* Table does not exist */
        OFPFMFC_OVERLAP      = 3,  # /* Attempted to add overlapping flow with
    #                                   CHECK_OVERLAP flag set. */
        OFPFMFC_EPERM        = 4,  # /* Permissions error. */
        OFPFMFC_BAD_TIMEOUT  = 5,  # /* Flow not added because of unsupported
    #                                   idle/hard timeout. */
        OFPFMFC_BAD_COMMAND  = 6,  # /* Unsupported or unknown command. */
        OFPFMFC_BAD_FLAGS    = 7,  # /* Unsupported or unknown flags. */
    )
    
    '''
    /* ofp_error_msg 'code' values for OFPET_GROUP_MOD_FAILED.  'data' contains
     * at least the first 64 bytes of the failed request. */
    '''
    ofp_group_mod_failed_code = enum('ofp_group_mod_failed_code', globals(), uint16,
        OFPGMFC_GROUP_EXISTS         = 0, # /* Group not added because a group ADD
    #                                          attempted to replace an
    #                                          already-present group. */
        OFPGMFC_INVALID_GROUP        = 1, # /* Group not added because Group
    #                                          specified is invalid. */
        OFPGMFC_WEIGHT_UNSUPPORTED   = 2, # /* Switch does not support unequal load
    #                                          sharing with select groups. */
        OFPGMFC_OUT_OF_GROUPS        = 3, # /* The group table is full. */
        OFPGMFC_OUT_OF_BUCKETS       = 4, # /* The maximum number of action buckets
    #                                          for a group has been exceeded. */
        OFPGMFC_CHAINING_UNSUPPORTED = 5, # /* Switch does not support groups that
    #                                          forward to groups. */
        OFPGMFC_WATCH_UNSUPPORTED    = 6, # /* This group cannot watch the watch_port
    #                                          or watch_group specified. */
        OFPGMFC_LOOP                 = 7, # /* Group entry would cause a loop. */
        OFPGMFC_UNKNOWN_GROUP        = 8, # /* Group not modified because a group
    #                                          MODIFY attempted to modify a
    #                                          non-existent group. */
        OFPGMFC_CHAINED_GROUP        = 9, # /* Group not deleted because another
    #                                          group is forwarding to it. */
        OFPGMFC_BAD_TYPE             = 10,# /* Unsupported or unknown group type. */
        OFPGMFC_BAD_COMMAND          = 11,# /* Unsupported or unknown command. */
        OFPGMFC_BAD_BUCKET           = 12,# /* Error in bucket. */
        OFPGMFC_BAD_WATCH            = 13,# /* Error in watch port/group. */
        OFPGMFC_EPERM                = 14,# /* Permissions error. */
    )
    
    '''
    /* ofp_error_msg 'code' values for OFPET_PORT_MOD_FAILED.  'data' contains
     * at least the first 64 bytes of the failed request. */
    '''
    ofp_port_mod_failed_code = enum('ofp_port_mod_failed_code', globals(), uint16,
        OFPPMFC_BAD_PORT      = 0,  # /* Specified port number does not exist. */
        OFPPMFC_BAD_HW_ADDR   = 1,  # /* Specified hardware address does not
    #                                  * match the port number. */
        OFPPMFC_BAD_CONFIG    = 2,  # /* Specified config is invalid. */
        OFPPMFC_BAD_ADVERTISE = 3,  # /* Specified advertise is invalid. */
        OFPPMFC_EPERM         = 4,  # /* Permissions error. */
    )
    
    '''
    /* ofp_error_msg 'code' values for OFPET_TABLE_MOD_FAILED.  'data' contains
     * at least the first 64 bytes of the failed request. */
    '''
    ofp_table_mod_failed_code = enum('ofp_table_mod_failed_code', globals(), uint16,
        OFPTMFC_BAD_TABLE  = 0,     # /* Specified table does not exist. */
        OFPTMFC_BAD_CONFIG = 1,     # /* Specified config is invalid. */
        OFPTMFC_EPERM      = 2,     # /* Permissions error. */
    )
    
    '''
    /* ofp_error msg 'code' values for OFPET_QUEUE_OP_FAILED. 'data' contains
     * at least the first 64 bytes of the failed request */
    '''
    ofp_queue_op_failed_code = enum('ofp_queue_op_failed_code', globals(), uint16,
        OFPQOFC_BAD_PORT   = 0,    # /* Invalid port (or port does not exist). */
        OFPQOFC_BAD_QUEUE  = 1,    # /* Queue does not exist. */
        OFPQOFC_EPERM      = 2,    # /* Permissions error. */
    )
    
    '''
    /* ofp_error_msg 'code' values for OFPET_SWITCH_CONFIG_FAILED. 'data' contains
     * at least the first 64 bytes of the failed request. */
    '''
    ofp_switch_config_failed_code = enum('ofp_switch_config_failed_code', globals(), uint16,
        OFPSCFC_BAD_FLAGS  = 0,     # /* Specified flags is invalid. */
        OFPSCFC_BAD_LEN    = 1,     # /* Specified len is invalid. */
        OFPSCFC_EPERM      = 2,     # /* Permissions error. */
    )
    
    '''
    /* ofp_error_msg 'code' values for OFPET_ROLE_REQUEST_FAILED. 'data' contains
     * at least the first 64 bytes of the failed request. */
    '''
    ofp_role_request_failed_code = enum('ofp_role_request_failed_code', globals(), uint16,
        OFPRRFC_STALE      = 0,     # /* Stale Message: old generation_id. */
        OFPRRFC_UNSUP      = 1,     # /* Controller role change unsupported. */
        OFPRRFC_BAD_ROLE   = 2,     # /* Invalid role. */
    )
    
    '''
    /* ofp_error_msg 'code' values for OFPET_METER_MOD_FAILED.  'data' contains
     * at least the first 64 bytes of the failed request. */
    '''
    ofp_meter_mod_failed_code = enum('ofp_meter_mod_failed_code', globals(), uint16,
        OFPMMFC_UNKNOWN       = 0, # /* Unspecified error. */
        OFPMMFC_METER_EXISTS  = 1, # /* Meter not added because a Meter ADD
    #                                 * attempted to replace an existing Meter. */
        OFPMMFC_INVALID_METER = 2, # /* Meter not added because Meter specified
    #                                 * is invalid,
    #                                 * or invalid meter in meter action. */
        OFPMMFC_UNKNOWN_METER = 3, # /* Meter not modified because a Meter MODIFY
    #                                 * attempted to modify a non-existent Meter,
    #                                 * or bad meter in meter action. */
        OFPMMFC_BAD_COMMAND   = 4, # /* Unsupported or unknown command. */
        OFPMMFC_BAD_FLAGS     = 5, # /* Flag configuration unsupported. */
        OFPMMFC_BAD_RATE      = 6, # /* Rate unsupported. */
        OFPMMFC_BAD_BURST     = 7, # /* Burst size unsupported. */
        OFPMMFC_BAD_BAND      = 8, # /* Band unsupported. */
        OFPMMFC_BAD_BAND_VALUE = 9,# /* Band value unsupported. */
        OFPMMFC_OUT_OF_METERS = 10,# /* No more meters available. */
        OFPMMFC_OUT_OF_BANDS  = 11,# /* The maximum number of properties
    #                                 * for a meter has been exceeded. */
    )
    
    '''
    /* ofp_error_msg 'code' values for OFPET_TABLE_FEATURES_FAILED. 'data' contains
     * at least the first 64 bytes of the failed request. */
    '''
    ofp_table_features_failed_code = enum('ofp_table_features_failed_code', globals(), uint16,
        OFPTFFC_BAD_TABLE    = 0,     # /* Specified table does not exist. */
        OFPTFFC_BAD_METADATA = 1,     # /* Invalid metadata mask. */
        OFPTFFC_BAD_TYPE     = 2,     # /* Unknown property type. */
        OFPTFFC_BAD_LEN      = 3,     # /* Length problem in properties. */
        OFPTFFC_BAD_ARGUMENT = 4,     # /* Unsupported property value. */
        OFPTFFC_EPERM        = 5,     # /* Permissions error. */
    )
    
    ofp_multipart_type = enum('ofp_multipart_type', globals(), uint16,
    #    /* Description of this OpenFlow switch.
    #     * The request body is empty.
    #     * The reply body is struct ofp_desc. */
        OFPMP_DESC = 0,
    
    #    /* Individual flow statistics.
    #     * The request body is struct ofp_flow_stats_request.
    #     * The reply body is an array of struct ofp_flow_stats. */
        OFPMP_FLOW = 1,
    
    #    /* Aggregate flow statistics.
    #     * The request body is struct ofp_aggregate_stats_request.
    #     * The reply body is struct ofp_aggregate_stats_reply. */
        OFPMP_AGGREGATE = 2,
    
    #    /* Flow table statistics.
    #     * The request body is empty.
    #     * The reply body is an array of struct ofp_table_stats. */
        OFPMP_TABLE = 3,
    
    #    /* Port statistics.
    #     * The request body is struct ofp_port_stats_request.
    #     * The reply body is an array of struct ofp_port_stats. */
        OFPMP_PORT_STATS = 4,
    
    #    /* Queue statistics for a port
    #     * The request body is struct ofp_queue_stats_request.
    #     * The reply body is an array of struct ofp_queue_stats */
        OFPMP_QUEUE = 5,
    
    #    /* Group counter statistics.
    #     * The request body is struct ofp_group_stats_request.
    #     * The reply is an array of struct ofp_group_stats. */
        OFPMP_GROUP = 6,
    
    #    /* Group description.
    #     * The request body is empty.
    #     * The reply body is an array of struct ofp_group_desc. */
        OFPMP_GROUP_DESC = 7,
    
    #    /* Group features.
    #     * The request body is empty.
    #     * The reply body is struct ofp_group_features. */
        OFPMP_GROUP_FEATURES = 8,
    
    #    /* Meter statistics.
    #     * The request body is struct ofp_meter_multipart_requests.
    #     * The reply body is an array of struct ofp_meter_stats. */
        OFPMP_METER = 9,
    
    #    /* Meter configuration.
    #     * The request body is struct ofp_meter_multipart_requests.
    #     * The reply body is an array of struct ofp_meter_config. */
        OFPMP_METER_CONFIG = 10,
    
    #    /* Meter features.
    #     * The request body is empty.
    #     * The reply body is struct ofp_meter_features. */
        OFPMP_METER_FEATURES = 11,
    
    #    /* Table features.
    #     * The request body is either empty or contains an array of
    #     * struct ofp_table_features containing the controller's
    #     * desired view of the switch. If the switch is unable to
    #     * set the specified view an error is returned.
    #     * The reply body is an array of struct ofp_table_features. */
        OFPMP_TABLE_FEATURES = 12,
    
    #    /* Port description.
    #     * The request body is empty.
    #     * The reply body is an array of struct ofp_port. */
        OFPMP_PORT_DESC = 13,
    
    #    /* Experimenter extension.
    #     * The request and reply bodies begin with
    #     * struct ofp_experimenter_multipart_header.
    #     * The request and reply bodies are otherwise experimenter-defined. */
        OFPMP_EXPERIMENTER = 0xffff
    )
    
    '''
    /* Backward compatibility with 1.3.1 - avoid breaking the API. */
    '''
    ofp_multipart_types = ofp_multipart_type
    
    ofp_multipart_request_flags = enum('ofp_multipart_request_flags', globals(), uint16, True,
        OFPMPF_REQ_MORE  = 1 << 0 # /* More requests to follow. */
    )
    
    ofp_multipart_request = nstruct(
        (ofp_multipart_type, 'type'),         #     /* One of the OFPMP_* constants. */
        (ofp_multipart_request_flags, 'flags'),        #     /* OFPMPF_REQ_* flags. */
        (uint8[4],),
        base = ofp_msg,
        criteria = lambda x: x.header.type == OFPT_MULTIPART_REQUEST,
        classifyby = (OFPT_MULTIPART_REQUEST,),
        init = packvalue(OFPT_MULTIPART_REQUEST, 'header', 'type'),
        classifier = lambda x: x.type,
        name = 'ofp_multipart_request'
    )
    
    ofp_multipart_reply_flags = enum('ofp_multipart_reply_flags', globals(), uint16, True,
        OFPMPF_REPLY_MORE  = 1 << 0 # /* More replies to follow. */
    )
    
    ofp_multipart_reply = nstruct(
        (ofp_multipart_type, 'type'),         #     /* One of the OFPMP_* constants. */
        (ofp_multipart_reply_flags, 'flags'),        #     /* OFPMPF_REPLY_* flags. */
        (uint8[4],),
        base = ofp_msg,
        criteria = lambda x: x.header.type == OFPT_MULTIPART_REPLY,
        classifyby = (OFPT_MULTIPART_REPLY,),
        init = packvalue(OFPT_MULTIPART_REPLY, 'header', 'type'),
        classifier = lambda x: x.type,
        name = 'ofp_multipart_reply'
    )
    
    DESC_STR_LEN = 256
    SERIAL_NUM_LEN = 32
    '''
    /* Body of reply to OFPMP_DESC request.  Each entry is a NULL-terminated
     * ASCII string. */
    '''
    ofp_desc = nstruct(
        (char[DESC_STR_LEN], 'mfr_desc'),    #   /* Manufacturer description. */
        (char[DESC_STR_LEN], 'hw_desc'),     #   /* Hardware description. */
        (char[DESC_STR_LEN], 'sw_desc'),     #   /* Software description. */
        (char[SERIAL_NUM_LEN], 'serial_num'),#   /* Serial number. */
        (char[DESC_STR_LEN], 'dp_desc'),     #   /* Human readable description of datapath. */
        name = 'ofp_desc'
    )
    
    ofp_desc_reply = nstruct(
        (ofp_desc,),
        base = ofp_multipart_reply,
        criteria = lambda x: x.type == OFPMP_DESC,
        classifyby = (OFPMP_DESC,),
        init = packvalue(OFPMP_DESC, 'type'),
        name = 'ofp_desc_reply'
    )
    '''
    /* Body for ofp_multipart_request of type OFPMP_FLOW. */
    '''
    ofp_flow_stats_request = nstruct(
        (ofp_table, 'table_id'),    #  /* ID of table to read (from ofp_table_stats),
    #                                 OFPTT_ALL for all tables. */
        (uint8[3],),            #  /* Align to 32 bits. */
        (ofp_port_no, 'out_port'),   #  /* Require matching entries to include this
    #                                 as an output port.  A value of OFPP_ANY
    #                                 indicates no restriction. */
        (ofp_group, 'out_group'),  #  /* Require matching entries to include this
    #                                 as an output group.  A value of OFPG_ANY
    #                                 indicates no restriction. */
        (uint8[4],),            #  /* Align to 64 bits. */
        (uint64, 'cookie'),     #  /* Require matching entries to contain this
    #                                 cookie value */
        (uint64, 'cookie_mask'),#  /* Mask used to restrict the cookie bits that
    #                                 must match. A value of 0 indicates
    #                                 no restriction. */
        (ofp_match, 'match'),   #  /* Fields to match. Variable size. */
        base = ofp_multipart_request,
        criteria = lambda x: x.type == OFPMP_FLOW,
        classifyby = (OFPMP_FLOW,),
        init = packvalue(OFPMP_FLOW, 'type'),
        name = 'ofp_flow_stats_request'
    )
    
    '''
    /* Body of reply to OFPMP_FLOW request. */
    '''
    ofp_flow_stats = nstruct(
        (uint16, 'length'),         # /* Length of this entry. */
        (uint8, 'table_id'),        # /* ID of table flow came from. */
        (uint8,),
        (uint32, 'duration_sec'),   # /* Time flow has been alive in seconds. */
        (uint32, 'duration_nsec'),  # /* Time flow has been alive in nanoseconds beyond
    #                                 duration_sec. */
        (uint16, 'priority'),       # /* Priority of the entry. */
        (uint16, 'idle_timeout'),   # /* Number of seconds idle before expiration. */
        (uint16, 'hard_timeout'),   # /* Number of seconds before expiration. */
        (ofp_flow_mod_flags, 'flags'),          # /* Bitmap of OFPFF_* flags. */
        (uint8[4],),                # /* Align to 64-bits. */
        (uint64, 'cookie'),         # /* Opaque controller-issued identifier. */
        (uint64, 'packet_count'),   # /* Number of packets in flow. */
        (uint64, 'byte_count'),     # /* Number of bytes in flow. */
        (ofp_match, 'match'),       # /* Description of fields. Variable size. */
    #    /* The variable size and padded match is always followed by instructions. */
        (ofp_instruction[0], 'instructions'), # /* Instruction set - 0 or more. */
        name = 'ofp_flow_stats',
        size = sizefromlen(65536, 'length'),
        prepack = packsize('length')
    )
    
    ofp_flow_stats_reply = nstruct(
        (ofp_flow_stats[0], 'stats'),
        base = ofp_multipart_reply,
        criteria = lambda x: x.type == OFPMP_FLOW,
        classifyby = (OFPMP_FLOW,),
        init = packvalue(OFPMP_FLOW, 'type'),
        name = 'ofp_flow_stats_reply'
    )
    
    '''
    /* Body for ofp_multipart_request of type OFPMP_AGGREGATE. */
    '''
    ofp_aggregate_stats_request = nstruct(
        (ofp_table, 'table_id'),      #     /* ID of table to read (from ofp_table_stats)
    #                                 OFPTT_ALL for all tables. */
        (uint8[3],),              #     /* Align to 32 bits. */
        (ofp_port_no, 'out_port'),     #     /* Require matching entries to include this
    #                                 as an output port.  A value of OFPP_ANY
    #                                 indicates no restriction. */
        (ofp_group, 'out_group'),    #     /* Require matching entries to include this
    #                                 as an output group.  A value of OFPG_ANY
    #                                 indicates no restriction. */
        (uint8[4],),              #     /* Align to 64 bits. */
        (uint64, 'cookie'),       #     /* Require matching entries to contain this
    #                                 cookie value */
        (uint64, 'cookie_mask'),  #     /* Mask used to restrict the cookie bits that
    #                                 must match. A value of 0 indicates
    #                                 no restriction. */
        (ofp_match, 'match'),     #     /* Fields to match. Variable size. */
        base = ofp_multipart_request,
        criteria = lambda x: x.type == OFPMP_AGGREGATE,
        classifyby = (OFPMP_AGGREGATE,),
        init = packvalue(OFPMP_AGGREGATE, 'type'),
        name = 'ofp_aggregate_stats_request'
    )
    
    '''
    /* Body of reply to OFPMP_AGGREGATE request. */
    '''
    ofp_aggregate_stats_reply = nstruct(
        (uint64, 'packet_count'),     #  /* Number of packets in flows. */
        (uint64, 'byte_count'),       #  /* Number of bytes in flows. */
        (uint32, 'flow_count'),       #  /* Number of flows. */
        (uint8[4],),                  #  /* Align to 64 bits. */
        base = ofp_multipart_reply,
        criteria = lambda x: x.type == OFPMP_AGGREGATE,
        classifyby = (OFPMP_AGGREGATE,),
        init = packvalue(OFPMP_AGGREGATE, 'type'),
        name = 'ofp_aggregate_stats_reply'
    )
    
    
    '''
    /* Table Feature property types.
     * Low order bit cleared indicates a property for a regular Flow Entry.
     * Low order bit set indicates a property for the Table-Miss Flow Entry.
    */
    '''
    ofp_table_feature_prop_type = enum('ofp_table_feature_prop_type', globals(), uint16,
        OFPTFPT_INSTRUCTIONS           = 0, # /* Instructions property. */
        OFPTFPT_INSTRUCTIONS_MISS      = 1, # /* Instructions for table-miss. */
        OFPTFPT_NEXT_TABLES            = 2, # /* Next Table property. */
        OFPTFPT_NEXT_TABLES_MISS       = 3, # /* Next Table for table-miss. */
        OFPTFPT_WRITE_ACTIONS          = 4, # /* Write Actions property. */
        OFPTFPT_WRITE_ACTIONS_MISS     = 5, # /* Write Actions for table-miss. */
        OFPTFPT_APPLY_ACTIONS          = 6, # /* Apply Actions property. */
        OFPTFPT_APPLY_ACTIONS_MISS     = 7, # /* Apply Actions for table-miss. */
        OFPTFPT_MATCH                  = 8, # /* Match property. */
        OFPTFPT_WILDCARDS              = 10,# /* Wildcards property. */
        OFPTFPT_WRITE_SETFIELD         = 12,# /* Write Set-Field property. */
        OFPTFPT_WRITE_SETFIELD_MISS    = 13,# /* Write Set-Field for table-miss. */
        OFPTFPT_APPLY_SETFIELD         = 14,# /* Apply Set-Field property. */
        OFPTFPT_APPLY_SETFIELD_MISS    = 15,# /* Apply Set-Field for table-miss. */
        OFPTFPT_EXPERIMENTER           = 0xFFFE,# /* Experimenter property. */
        OFPTFPT_EXPERIMENTER_MISS      = 0xFFFF,# /* Experimenter for table-miss. */
    )
    
    '''
    /* Common header for all Table Feature Properties */
    '''
    ofp_table_feature_prop = nstruct(
        (ofp_table_feature_prop_type, 'type'),                  # /* One of OFPTFPT_*. */
        (uint16, 'length'),                # /* Length in bytes of this property. */
        name = 'ofp_table_feature_prop',
        size = sizefromlen(4096, 'length'),
        prepack = packrealsize('length'),
        classifier = lambda x: x.type
    )
    
    ofp_instruction_feature = nstruct(
        (ofp_instruction_type, 'type'),           #     /* Instruction type */
        (uint16, 'len'),            #     /* Length of this struct in bytes. */
        name = 'ofp_instruction_feature',
        size = sizefromlen(65536, 'len'),
        prepack = packsize('len'),
        padding = 1
    )
    
    ofp_instruction_experimenter_feature = nstruct(
        (uint32, 'experimenter'),  # /* Experimenter ID which takes the same form as in struct ofp_experimenter_header. */
    #    /* Experimenter-defined arbitrary additional data. */
        base = ofp_instruction_feature,
        name = 'ofp_instruction_experimenter_feature',
        criteria = lambda x: x.type == OFPIT_EXPERIMENTER,
        init = packvalue(OFPIT_EXPERIMENTER, 'type')
    )
    
    
    '''
    /* Instructions property */
    '''
    ofp_table_feature_prop_instructions = nstruct(
        (ofp_instruction_feature[0], 'instruction_ids'),  # /* List of instructions */
        name = 'ofp_table_feature_prop_instructions',
        base = ofp_table_feature_prop,
        criteria = lambda x: x.type == OFPTFPT_INSTRUCTIONS or x.type == OFPTFPT_INSTRUCTIONS_MISS,
        classifyby = (OFPTFPT_INSTRUCTIONS, OFPTFPT_INSTRUCTIONS_MISS),
        init = packvalue(OFPTFPT_INSTRUCTIONS, 'type')
    )
    
    '''
    /* Next Tables property */
    '''
    ofp_table_feature_prop_next_tables = nstruct(
        (uint8[0], 'next_table_ids'),       # /* List of table ids. */
        base = ofp_table_feature_prop,
        name = 'ofp_table_feature_prop_next_tables',
        criteria = lambda x: x.type == OFPTFPT_NEXT_TABLES or x.type == OFPTFPT_NEXT_TABLES_MISS,
        classifyby = (OFPTFPT_NEXT_TABLES, OFPTFPT_NEXT_TABLES_MISS),
        init = packvalue(OFPTFPT_NEXT_TABLES, 'type')
    )
    
    ofp_action_desc = nstruct((ofp_action_type, 'type'),
                        (uint16, 'len'),
                        name = 'ofp_action_desc',
                        size = sizefromlen(512, 'len'),
                        prepack = packsize('len'),
                        padding = 1
                        )
    
    
    ofp_action_experimenter_desc = nstruct(
        (uint32, 'experimenter'),     #  /* Experimenter ID which takes the same
    #                                       form as in struct
    #                                       ofp_experimenter_header. */
        base = ofp_action_desc,
        criteria = lambda x: x.type == OFPAT_EXPERIMENTER,
        init = packvalue(OFPAT_EXPERIMENTER, 'type'),
        name = 'ofp_action_experimenter_desc'
    )
    
    '''
    /* Actions property */
    '''
    ofp_table_feature_prop_actions = nstruct(
    #    /* Followed by:
    #     *   - Exactly (length - 4) bytes containing the action_ids, then
    #     *   - Exactly (length + 7)/8*8 - (length) (between 0 and 7)
    #     *     bytes of all-zero bytes */
        (ofp_action_desc[0], 'action_ids'),    #  /* List of actions */
        base = ofp_table_feature_prop,
        criteria = lambda x: x.type in (OFPTFPT_WRITE_ACTIONS, OFPTFPT_WRITE_ACTIONS_MISS, OFPTFPT_APPLY_ACTIONS, OFPTFPT_APPLY_ACTIONS_MISS),
        classifyby = (OFPTFPT_WRITE_ACTIONS, OFPTFPT_WRITE_ACTIONS_MISS, OFPTFPT_APPLY_ACTIONS, OFPTFPT_APPLY_ACTIONS_MISS),
        init = packvalue(OFPTFPT_APPLY_ACTIONS, 'type'),
        name = 'ofp_table_feature_prop_actions'
    )
    
    '''
    /* Match, Wildcard or Set-Field property */
    '''
    ofp_table_feature_prop_oxm  = nstruct(
    #    uint16_t         type;    /* One of OFPTFPT_MATCH,
    #                                 OFPTFPT_WILDCARDS,
    #                                 OFPTFPT_WRITE_SETFIELD,
    #                                 OFPTFPT_WRITE_SETFIELD_MISS,
    #                                 OFPTFPT_APPLY_SETFIELD,
    #                                 OFPTFPT_APPLY_SETFIELD_MISS. */
    #    uint16_t         length;  /* Length in bytes of this property. */
    #    /* Followed by:
    #     *   - Exactly (length - 4) bytes containing the oxm_ids, then
    #     *   - Exactly (length + 7)/8*8 - (length) (between 0 and 7)
    #     *     bytes of all-zero bytes */
        (ofp_oxm_header[0], 'oxm_ids'),    # /* Array of OXM headers */
        name = 'ofp_table_feature_prop_oxm',
        base = ofp_table_feature_prop,
        criteria = lambda x: x.type in (OFPTFPT_MATCH, OFPTFPT_WILDCARDS, OFPTFPT_WRITE_SETFIELD, OFPTFPT_WRITE_SETFIELD_MISS, OFPTFPT_APPLY_SETFIELD, OFPTFPT_APPLY_SETFIELD_MISS),
        classifyby = (OFPTFPT_MATCH, OFPTFPT_WILDCARDS, OFPTFPT_WRITE_SETFIELD, OFPTFPT_WRITE_SETFIELD_MISS, OFPTFPT_APPLY_SETFIELD, OFPTFPT_APPLY_SETFIELD_MISS),
        init = packvalue(OFPTFPT_MATCH, 'type')
    )
    
    '''
    /* Experimenter table feature property */
    '''
    ofp_table_feature_prop_experimenter = nstruct(
    #    uint16_t         type;    /* One of OFPTFPT_EXPERIMENTER,
    #                                 OFPTFPT_EXPERIMENTER_MISS. */
    #    uint16_t         length;  /* Length in bytes of this property. */
        (uint32, 'experimenter'),   #  /* Experimenter ID which takes the same
    #                                       form as in struct
    #                                       ofp_experimenter_header. */
        (uint32, 'exp_type'),       #  /* Experimenter defined. */
    #    /* Followed by:
    #     *   - Exactly (length - 12) bytes containing the experimenter data, then
    #     *   - Exactly (length + 7)/8*8 - (length) (between 0 and 7)
    #     *     bytes of all-zero bytes */
        (uint32[0], 'experimenter_data'),
        name = 'ofp_table_feature_prop_experimenter',
        base = ofp_table_feature_prop,
        criteria = lambda x: x.type == OFPTFPT_EXPERIMENTER or x.type == OFPTFPT_EXPERIMENTER_MISS,
        classifyby = (OFPTFPT_EXPERIMENTER, OFPTFPT_EXPERIMENTER_MISS),
        init = packvalue(OFPTFPT_EXPERIMENTER, 'type')
    )
    
    '''
    /* Body for ofp_multipart_request of type OFPMP_TABLE_FEATURES./
     * Body of reply to OFPMP_TABLE_FEATURES request. */
    '''
    ofp_table_features = nstruct(
        (uint16, 'length'),       #  /* Length is padded to 64 bits. */
        (uint8, 'table_id'),      #  /* Identifier of table.  Lower numbered tables
    #                                are consulted first. */
        (uint8[5],),              #  /* Align to 64-bits. */
        (char[OFP_MAX_TABLE_NAME_LEN], 'name'),
        (uint64, 'metadata_match'), #/* Bits of metadata table can match. */
        (uint64, 'metadata_write'), #/* Bits of metadata table can write. */
        (ofp_table_config, 'config'),         #/* Bitmap of OFPTC_* values */
        (uint32, 'max_entries'),    #/* Max number of entries supported. */
    
    #    /* Table Feature Property list */
        (ofp_table_feature_prop[0], 'properties'),  # /* List of properties */
        name = 'ofp_table_features',
        size = sizefromlen(65536, 'length'),
        prepack = packrealsize('length')
    )
    
    ofp_table_features_request = nstruct(
        (ofp_table_features[0], 'features'),
        base = ofp_multipart_request,
        criteria = lambda x: x.type == OFPMP_TABLE_FEATURES,
        classifyby = (OFPMP_TABLE_FEATURES,),
        init = packvalue(OFPMP_TABLE_FEATURES, 'type'),
        name = 'ofp_table_features_request'
    )
    
    ofp_table_features_reply = nstruct(
        (ofp_table_features[0], 'features'),
        base = ofp_multipart_reply,
        criteria = lambda x: x.type == OFPMP_TABLE_FEATURES,
        classifyby = (OFPMP_TABLE_FEATURES,),
        init = packvalue(OFPMP_TABLE_FEATURES, 'type'),
        name = 'ofp_table_features_reply'
    )
    
    
    '''
    /* Body of reply to OFPMP_TABLE request. */
    '''
    ofp_table_stats = nstruct(
        (uint8, 'table_id'),     #   /* Identifier of table.  Lower numbered tables
    #                                are consulted first. */
        (uint8[3],),             #   /* Align to 32-bits. */
        (uint32, 'active_count'),   #   /* Number of active entries. */
        (uint64, 'lookup_count'),   #   /* Number of packets looked up in table. */
        (uint64, 'matched_count'),  #   /* Number of packets that hit table. */
        name = 'ofp_table_stats'
    )
    
    ofp_table_stats_reply = nstruct(
        (ofp_table_stats[0], 'stats'),
        base = ofp_multipart_reply,
        criteria = lambda x: x.type == OFPMP_TABLE,
        classifyby = (OFPMP_TABLE,),
        init = packvalue(OFPMP_TABLE, 'type'),
        name = 'ofp_table_stats_reply'
    )
    
    '''
    /* Body for ofp_multipart_request of type OFPMP_PORT. */
    '''
    ofp_port_stats_request = nstruct(
        (ofp_port_no, 'port_no'),    #    /* OFPMP_PORT message must request statistics
    #                              * either for a single port (specified in
    #                              * port_no) or for all ports (if port_no ==
    #                              * OFPP_ANY). */
        (uint8[4],),
        base = ofp_multipart_request,
        criteria = lambda x: x.type == OFPMP_PORT_STATS,
        classifyby = (OFPMP_PORT_STATS,),
        init = packvalue(OFPMP_PORT_STATS, 'type'),
        name = 'ofp_port_stats_request'
    )
    
    '''
    /* Body of reply to OFPMP_PORT request. If a counter is unsupported, set
     * the field to all ones. */
    '''
    ofp_port_stats = nstruct(
        (ofp_port_no, 'port_no'),
        (uint8[4],),                # /* Align to 64-bits. */
        (uint64, 'rx_packets'),     # /* Number of received packets. */
        (uint64, 'tx_packets'),     # /* Number of transmitted packets. */
        (uint64, 'rx_bytes'),       # /* Number of received bytes. */
        (uint64, 'tx_bytes'),       # /* Number of transmitted bytes. */
        (uint64, 'rx_dropped'),     # /* Number of packets dropped by RX. */
        (uint64, 'tx_dropped'),     # /* Number of packets dropped by TX. */
        (uint64, 'rx_errors'),      # /* Number of receive errors.  This is a super-set
    #                                of more specific receive errors and should be
    #                                greater than or equal to the sum of all
    #                                rx_*_err values. */
        (uint64, 'tx_errors'),      # /* Number of transmit errors.  This is a super-set
    #                                of more specific transmit errors and should be
    #                                greater than or equal to the sum of all
    #                                tx_*_err values (none currently defined.) */
        (uint64, 'rx_frame_err'),   # /* Number of frame alignment errors. */
        (uint64, 'rx_over_err'),    # /* Number of packets with RX overrun. */
        (uint64, 'rx_crc_err'),     # /* Number of CRC errors. */
        (uint64, 'collisions'),     # /* Number of collisions. */
        (uint32, 'duration_sec'),   # /* Time port has been alive in seconds. */
        (uint32, 'duration_nsec'),  # /* Time port has been alive in nanoseconds beyond
    #                                duration_sec. */
        name = 'ofp_port_stats'
    )
    
    ofp_port_stats_reply = nstruct(
        (ofp_port_stats[0], 'stats'),
        base = ofp_multipart_reply,
        criteria = lambda x: x.type == OFPMP_PORT_STATS,
        classifyby = (OFPMP_PORT_STATS,),
        init = packvalue(OFPMP_PORT_STATS, 'type'),
        name = 'ofp_port_stats_reply'
    )
    
    ofp_port_desc_reply = nstruct(
        (ofp_port[0], 'ports'),
        base = ofp_multipart_reply, 
        criteria = lambda x: x.type == OFPMP_PORT_DESC,
        classifyby = (OFPMP_PORT_DESC,),
        init = packvalue(OFPMP_PORT_DESC, 'type'),
        name = 'ofp_port_desc_reply'
    )
    
    '''
    /* Body of OFPMP_GROUP request. */
    '''
    ofp_group_stats_request = nstruct(
        (ofp_group, 'group_id'),       #  /* All groups if OFPG_ALL. */
        (uint8[4],),                #  /* Align to 64 bits. */
        base = ofp_multipart_request,
        criteria = lambda x: x.type == OFPMP_GROUP,
        classifyby = (OFPMP_GROUP,),
        init = packvalue(OFPMP_GROUP, 'type'),
        name = 'ofp_group_stats_request'
    )
    
    '''
    /* Used in group stats replies. */
    '''
    ofp_bucket_counter = nstruct(
        (uint64, 'packet_count'),   #   /* Number of packets processed by bucket. */
        (uint64, 'byte_count'),     #   /* Number of bytes processed by bucket. */
        name = 'ofp_bucket_counter'
    )
    
    '''
    /* Body of reply to OFPMP_GROUP request. */
    '''
    ofp_group_stats = nstruct(
        (uint16, 'length'),        # /* Length of this entry. */
        (uint8[2],),               # /* Align to 64 bits. */
        (uint32, 'group_id'),      # /* Group identifier. */
        (uint32, 'ref_count'),     # /* Number of flows or groups that directly forward
    #                                to this group. */
        (uint8[4],),               # /* Align to 64 bits. */
        (uint64, 'packet_count'),  # /* Number of packets processed by group. */
        (uint64, 'byte_count'),    # /* Number of bytes processed by group. */
        (uint32, 'duration_sec'),  # /* Time group has been alive in seconds. */
        (uint32, 'duration_nsec'), # /* Time group has been alive in nanoseconds beyond
    #                                duration_sec. */
        (ofp_bucket_counter[0], 'bucket_stats'),    # /* One counter set per bucket. */
        name = 'ofp_group_stats',
        size = sizefromlen(65536, 'length'),
        prepack = packrealsize('length')
    )
    
    ofp_group_stats_reply = nstruct(
        (ofp_group_stats[0], 'stats'),
        base = ofp_multipart_reply,
        criteria = lambda x: x.type == OFPMP_GROUP,
        classifyby = (OFPMP_GROUP,),
        init = packvalue(OFPMP_GROUP, 'type'),
        name = 'ofp_group_stats_reply'
    )
    
    '''
    /* Body of reply to OFPMP_GROUP_DESC request. */
    '''
    ofp_group_desc = nstruct(
        (uint16, 'length'),         # /* Length of this entry. */
        (ofp_group_type, 'type'),            # /* One of OFPGT_*. */
        (uint8,),                   # /* Pad to 64 bits. */
        (uint32, 'group_id'),       # /* Group identifier. */
        (ofp_bucket[0], 'buckets'), # /* List of buckets - 0 or more. */
        size = sizefromlen(65536, 'length'),
        prepack = packrealsize('length'),
        name = 'ofp_group_desc'
    )
    
    ofp_group_desc_reply = nstruct(
        (ofp_group_desc[0], 'stats'),
        base = ofp_multipart_reply,
        criteria = lambda x: x.type == OFPMP_GROUP_DESC,
        classifyby = (OFPMP_GROUP_DESC,),
        init = packvalue(OFPMP_GROUP_DESC, 'type'),
        name = 'ofp_group_desc_reply'
    )
    
    '''
    /* Backward compatibility with 1.3.1 - avoid breaking the API. */
    '''
    ofp_group_desc_stats = ofp_group_desc
    
    '''
    /* Group configuration flags */
    '''
    ofp_group_capabilities = enum('ofp_group_capabilities', globals(), uint32,
        OFPGFC_SELECT_WEIGHT   = 1 << 0, # /* Support weight for select groups */
        OFPGFC_SELECT_LIVENESS = 1 << 1, # /* Support liveness for select groups */
        OFPGFC_CHAINING        = 1 << 2, # /* Support chaining groups */
        OFPGFC_CHAINING_CHECKS = 1 << 3, # /* Check chaining for loops and delete */
    )
    
    ofp_group_type_bitwise = enum('ofp_group_type_bitwise', None, uint32, True,
                                  **dict((k, 1<<v) for (k,v) in ofp_group_type.getDict().items()))
    
    ofp_action_type_bitwise = enum('ofp_action_type_bitwise', None, uint32, True,
                                   **dict((k, 1<<v) for (k,v) in ofp_action_type.getDict().items() if v < 32))
    
    '''
    /* Body of reply to OFPMP_GROUP_FEATURES request. Group features. */
    '''
    ofp_group_features = nstruct(
        (ofp_group_type_bitwise, 'types'),          # /* Bitmap of (1 << OFPGT_*) values supported. */
        (ofp_group_capabilities, 'capabilities'),   # /* Bitmap of OFPGFC_* capability supported. */
        (uint32[4], 'max_groups'),  # /* Maximum number of groups for each type. */
        (ofp_action_type_bitwise[4], 'actions'),     # /* Bitmaps of (1 << OFPAT_*) values supported. */
        name = 'ofp_group_features'
    )
    
    ofp_group_features_reply = nstruct(
        (ofp_group_features,),
        base = ofp_multipart_reply,
        criteria = lambda x: x.type == OFPMP_GROUP_FEATURES,
        classifyby = (OFPMP_GROUP_FEATURES,),
        init = packvalue(OFPMP_GROUP_FEATURES, 'type'),
        name = 'ofp_group_features_reply'
    )
    
    '''
    /* Body of OFPMP_METER and OFPMP_METER_CONFIG requests. */
    '''
    ofp_meter_multipart_request = nstruct(
        (uint32, 'meter_id'),             #    /* Meter instance, or OFPM_ALL. */
        (uint8[4],),                      #    /* Align to 64 bits. */
        base = ofp_multipart_request,
        criteria = lambda x: x.type == OFPMP_METER or x.type == OFPMP_METER_CONFIG,
        classifyby = (OFPMP_METER, OFPMP_METER_CONFIG),
        init = packvalue(OFPMP_METER, 'type'),
        name = 'ofp_meter_multipart_request'
    )
    
    '''
    /* Statistics for each meter band */
    '''
    ofp_meter_band_stats = nstruct(
        (uint64, 'packet_band_count'),      #   /* Number of packets in band. */
        (uint64, 'byte_band_count'),        #   /* Number of bytes in band. */
        name = 'ofp_meter_band_stats'
    )
    
    '''
    /* Body of reply to OFPMP_METER request. Meter statistics. */
    '''
    ofp_meter_stats = nstruct(
        (uint32, 'meter_id'),           #     /* Meter instance. */
        (uint16, 'len'),                #     /* Length in bytes of this stats. */
        (uint8[6],),
        (uint32, 'flow_count'),         #     /* Number of flows bound to meter. */
        (uint64, 'packet_in_count'),    #     /* Number of packets in input. */
        (uint64, 'byte_in_count'),      #     /* Number of bytes in input. */
        (uint32, 'duration_sec'),       #     /* Time meter has been alive in seconds. */
        (uint32, 'duration_nsec'),      #     /* Time meter has been alive in nanoseconds beyond
    #                                 duration_sec. */
        (ofp_meter_band_stats[0], 'band_stats'),  # /* The band_stats length is
    #                                         inferred from the length field. */
        size = sizefromlen(65536, 'len'),
        prepack = packrealsize('len'),
        name = 'ofp_meter_stats'
    )
    
    ofp_meter_stats_reply = nstruct(
        (ofp_meter_stats[0], 'stats'),
        name = 'ofp_meter_stats_reply',
        base = ofp_multipart_reply,
        criteria = lambda x: x.type == OFPMP_METER,
        classifyby = (OFPMP_METER,),
        init = packvalue(OFPMP_METER, 'type')
    )
    
    '''
    /* Body of reply to OFPMP_METER_CONFIG request. Meter configuration. */
    '''
    ofp_meter_config = nstruct(
        (uint16, 'length'),             # /* Length of this entry. */
        (ofp_meter_flags, 'flags'),              # /* All OFPMF_* that apply. */
        (uint32, 'meter_id'),           # /* Meter instance. */
        (ofp_meter_band[0], 'bands'),   # /* The bands length is
    #                                         inferred from the length field. */
        base = ofp_multipart_reply,
        criteria = lambda x: x.type == OFPMP_METER_CONFIG,
        classifyby = (OFPMP_METER_CONFIG,),
        init = packvalue(OFPMP_METER_CONFIG, 'type'),
        name = 'ofp_meter_config'
    )
    
    ofp_meter_band_type_bitwise = enum('ofp_meter_band_type_bitwise', None, uint32, True,
                                       **dict((k,1<<v) for k,v in ofp_meter_band_type.getDict().items()))
    
    '''
    /* Body of reply to OFPMP_METER_FEATURES request. Meter features. */
    '''
    ofp_meter_features = nstruct(
        (uint32, 'max_meter'),   # /* Maximum number of meters. */
        (ofp_meter_band_type_bitwise, 'band_types'),  # /* Bitmaps of (1 << OFPMBT_*) values supported. */
        (ofp_meter_flags, 'capabilities'),# /* Bitmaps of "ofp_meter_flags". */
        (uint8, 'max_bands'),    # /* Maximum bands per meters */
        (uint8, 'max_color'),    # /* Maximum color value */
        (uint8[2],),
        name = 'ofp_meter_features'
    )
    
    ofp_meter_features_reply = nstruct(
        (ofp_meter_features,),
        name = 'ofp_meter_features_reply',
        base = ofp_multipart_reply,
        criteria = lambda x: x.type == OFPMP_METER_FEATURES,
        classifyby = (OFPMP_METER_FEATURES,),
        init = packvalue(OFPMP_METER_FEATURES, 'type'),
    )
    
    '''
    /* Body for ofp_multipart_request/reply of type OFPMP_EXPERIMENTER. */
    '''
    ofp_experimenter_multipart_header = nstruct(
        (uint32, 'experimenter'),  # /* Experimenter ID which takes the same form
    #                                 as in struct ofp_experimenter_header. */
        (uint32, 'exp_type'),      # /* Experimenter defined. */
    #    /* Experimenter-defined arbitrary additional data. */
        name = 'ofp_experimenter_multipart_header',
    )
    
    ofp_experimenter_multipart_request = nstruct(
        (ofp_experimenter_multipart_header,),
        base = ofp_multipart_request,
        criteria = lambda x: x.type == OFPMP_EXPERIMENTER,
        init = packvalue(OFPMP_EXPERIMENTER, 'type'),
        classifyby = (OFPMP_EXPERIMENTER,),
        name = 'ofp_experimenter_multipart_request',
    )
    
    ofp_experimenter_multipart_reply = nstruct(
        (ofp_experimenter_multipart_header,),
        base = ofp_multipart_reply,
        criteria = lambda x: x.type == OFPMP_EXPERIMENTER,
        init = packvalue(OFPMP_EXPERIMENTER, 'type'),
        classifyby = (OFPMP_EXPERIMENTER,),
        name = 'ofp_experimenter_multipart_reply',
    )
    
    
    '''
    /* Experimenter extension. */
    '''
    ofp_experimenter = nstruct(
        (uint32, 'experimenter'),   #      /* Experimenter ID:
    #                                       * - MSB 0: low-order bytes are IEEE OUI.
    #                                       * - MSB != 0: defined by ONF. */
        (uint32, 'exp_type'),       #      /* Experimenter defined. */
        base = ofp_msg,
        criteria = lambda x: x.header.type == OFPT_EXPERIMENTER,
        classifyby = (OFPT_EXPERIMENTER,),
        init = packvalue(OFPT_EXPERIMENTER, 'header', 'type'),
        name = 'ofp_experimenter'
    )
    
    '''
    /* All ones is used to indicate all queues in a port (for stats retrieval). */
    '''
    ofp_queue = enum('ofp_queue', globals(), uint32,
    OFPQ_ALL = 0xffffffff)
    
    '''
    /* Min rate > 1000 means not configured. */
    '''
    OFPQ_MIN_RATE_UNCFG = 0xffff
    
    '''
    /* Max rate > 1000 means not configured. */
    '''
    OFPQ_MAX_RATE_UNCFG = 0xffff
    
    '''
    /* Common description for a queue. */
    '''
    ofp_queue_prop_header = nstruct((uint16, 'property'),
                                    (uint16, 'len'),
                                    (uint8[4],),
                                    name = 'ofp_queue_prop_header')
    
    ofp_queue_prop = nstruct((ofp_queue_prop_header, 'prop_header'),
                             name = 'ofp_queue_prop',
                             size = sizefromlen(256, 'prop_header', 'len'),
                             prepack = packrealsize('prop_header', 'len'),
                             classifier = lambda x: x.prop_header.property
                             )
    '''
    /* Min-Rate queue property description. */
    '''
    ofp_queue_prop_min_rate = nstruct((uint16, 'rate'),
                                  (uint8[6],),
                                  base = ofp_queue_prop,
                                  criteria = lambda x: x.prop_header.property == OFPQT_MIN_RATE,
                                  classifyby = (OFPQT_MIN_RATE,),
                                  init = packvalue(OFPQT_MIN_RATE, 'prop_header', 'property'),
                                  name = 'ofp_queue_prop_min_rate')
    
    '''
    /* Max-Rate queue property description. */
    '''
    ofp_queue_prop_max_rate = nstruct((uint16, 'rate'),
                                  (uint8[6],),
                                  base = ofp_queue_prop,
                                  criteria = lambda x: x.prop_header.property == OFPQT_MAX_RATE,
                                  classifyby = (OFPQT_MAX_RATE,),
                                  init = packvalue(OFPQT_MAX_RATE, 'prop_header', 'property'),
                                  name = 'ofp_queue_prop_max_rate')
    
    '''
    /* Experimenter queue property description. */
    '''
    ofp_queue_prop_experimenter = nstruct((uint32, 'experimenter'),
                                          (uint8[4],),
                                          (raw, 'data'),
                                          name = 'ofp_queue_prop_experimenter',
                                          base = ofp_queue_prop,
                                          criteria = lambda x: x.prop_header.property == OFPQT_EXPERIMENTER,
                                          classifyby = (OFPQT_EXPERIMENTER,),
                                          init = packvalue(OFPQT_EXPERIMENTER, 'prop_header', 'property'))
    
    '''
    /* Full description for a queue. */
    '''
    ofp_packet_queue = nstruct(
        (uint32, 'queue_id'),  # /* id for the specific queue. */
        (uint32, 'port'),      # /* Port this queue is attached to. */
        (uint16, 'len'),       # /* Length in bytes of this queue desc. */
        (uint8[6],),           # /* 64-bit alignment. */
        (ofp_queue_prop_header[0], 'properties'),   # /* List of properties. */
        name = 'ofp_packet_queue',
        size = sizefromlen(4096, 'len'),
        prepack = packsize('len')
    )
    
    '''
    /* Query for port queue configuration. */
    '''
    ofp_queue_get_config_request = nstruct(
        (ofp_port_no, 'port'),      #  /* Port to be queried. Should refer
    #                              to a valid physical port (i.e. <= OFPP_MAX),
    #                              or OFPP_ANY to request all configured
    #                              queues.*/
        (uint8[4],),
        name = 'ofp_queue_get_config_request',
        base = ofp_msg,
        criteria = lambda x: x.header.type == OFPT_QUEUE_GET_CONFIG_REQUEST,
        classifyby = (OFPT_QUEUE_GET_CONFIG_REQUEST,),
        init = packvalue(OFPT_QUEUE_GET_CONFIG_REQUEST, 'header', 'type')
    )
    
    '''
    /* Queue configuration for a given port. */
    '''
    ofp_queue_get_config_reply = nstruct(
        (ofp_port_no, 'port'),
        (uint8[4],),
        (ofp_packet_queue[0], 'queues'),        # /* List of configured queues. */
        base = ofp_msg,
        criteria = lambda x: x.header.type == OFPT_QUEUE_GET_CONFIG_REPLY,
        classifyby = (OFPT_QUEUE_GET_CONFIG_REPLY,),
        init = packvalue(OFPT_QUEUE_GET_CONFIG_REPLY, 'header', 'type'),
        name = 'ofp_queue_get_config_reply'
    )
    
    
    '''
    /* OFPAT_SET_QUEUE action struct: send packets to given queue on port. */
    '''
    ofp_action_set_queue = nstruct(
        (uint32, 'queue_id'),      # /* Queue id for the packets. */
        base = ofp_action,
        criteria = lambda x: x.type == OFPAT_SET_QUEUE,
        classifyby = (OFPAT_SET_QUEUE,),
        init = packvalue(OFPAT_SET_QUEUE, 'type'),
        name = 'ofp_action_set_queue'
    )
    
    ofp_queue_stats_request = nstruct(
        (ofp_port_no, 'port_no'),          # /* All ports if OFPP_ANY. */
        (ofp_queue, 'queue_id'),         # /* All queues if OFPQ_ALL. */
        base = ofp_multipart_request,
        criteria = lambda x: x.type == OFPMP_QUEUE,
        classifyby = (OFPMP_QUEUE,),
        init = packvalue(OFPMP_QUEUE, 'type'),
        name = 'ofp_queue_stats_request'
    )
    
    ofp_queue_stats = nstruct(
        (uint32, 'port_no'),
        (uint32, 'queue_id'),   #       /* Queue i.d */
        (uint64, 'tx_bytes'),   #       /* Number of transmitted bytes. */
        (uint64, 'tx_packets'), #       /* Number of transmitted packets. */
        (uint64, 'tx_errors'),  #       /* Number of packets dropped due to overrun. */
        (uint32, 'duration_sec'),   #   /* Time queue has been alive in seconds. */
        (uint32, 'duration_nsec'),  #   /* Time queue has been alive in nanoseconds beyond
    #                                duration_sec. */
        name = 'ofp_queue_stats'
    )
    
    ofp_queue_stats_reply = nstruct(
        (ofp_queue_stats, 'stats'),
        base = ofp_multipart_reply,
        criteria = lambda x: x.type == OFPMP_QUEUE,
        classifyby = (OFPMP_QUEUE,),
        init = packvalue(OFPMP_QUEUE, 'type'),
        name = 'ofp_queue_stats_reply'
    )
    
    '''
    /* Configures the "role" of the sending controller.  The default role is:
     *
     *    - Equal (OFPCR_ROLE_EQUAL), which allows the controller access to all
     *      OpenFlow features. All controllers have equal responsibility.
     *
     * The other possible roles are a related pair:
     *
     *    - Master (OFPCR_ROLE_MASTER) is equivalent to Equal, except that there
     *      may be at most one Master controller at a time: when a controller
     *      configures itself as Master, any existing Master is demoted to the
     *      Slave role.
     *
     *    - Slave (OFPCR_ROLE_SLAVE) allows the controller read-only access to
     *      OpenFlow features.  In particular attempts to modify the flow table
     *      will be rejected with an OFPBRC_EPERM error.
     *
     *      Slave controllers do not receive OFPT_PACKET_IN or OFPT_FLOW_REMOVED
     *      messages, but they do receive OFPT_PORT_STATUS messages.
    */
    '''
    
    '''
    /* Controller roles. */
    '''
    ofp_controller_role = enum('ofp_controller_role', globals(), uint32,
        OFPCR_ROLE_NOCHANGE = 0,   # /* Don't change current role. */
        OFPCR_ROLE_EQUAL    = 1,   # /* Default role, full access. */
        OFPCR_ROLE_MASTER   = 2,   # /* Full access, at most one master. */
        OFPCR_ROLE_SLAVE    = 3,   # /* Read-only access. */
    )
    
    '''
    /* Role request and reply message. */
    '''
    ofp_role_request = nstruct(
        (ofp_controller_role, 'role'),      #    /* One of OFPCR_ROLE_*. */
        (uint8[4],),           #    /* Align to 64 bits. */
        (uint64, 'generation_id'), #/* Master Election Generation Id */
        base = ofp_msg,
        criteria = lambda x: x.header.type == OFPT_ROLE_REQUEST or x.header.type == OFPT_ROLE_REPLY,
        classifyby = (OFPT_ROLE_REQUEST, OFPT_ROLE_REPLY),
        init = packvalue(OFPT_ROLE_REQUEST, 'header', 'type'),
        name = 'ofp_role_request'
    )
    
    ofp_packet_in_reason_bitwise = enum('ofp_packet_in_reason_bitwise', None, uint32, True,
                                        **dict((k, 1<<v) for k,v in ofp_packet_in_reason.getDict().items()))
    
    ofp_port_reason_bitwise = enum('ofp_port_reason_bitwise', None, uint32, True,
                                        **dict((k, 1<<v) for k,v in ofp_port_reason.getDict().items()))
    
    ofp_flow_removed_reason_bitwise = enum('ofp_flow_removed_reason_bitwise', None, uint32, True,
                                        **dict((k, 1<<v) for k,v in ofp_flow_removed_reason.getDict().items()))
    
    '''
    /* Asynchronous message configuration. */
    '''
    ofp_async_config = nstruct(
        (ofp_packet_in_reason_bitwise[2], 'packet_in_mask'),      #   /* Bitmasks of OFPR_* values. */
        (ofp_port_reason_bitwise[2], 'port_status_mask'),    #   /* Bitmasks of OFPPR_* values. */
        (ofp_flow_removed_reason_bitwise[2], 'flow_removed_mask'),   #   /* Bitmasks of OFPRR_* values. */
        base = ofp_msg,
        criteria = lambda x: x.header.type == OFPT_GET_ASYNC_REPLY or x.header.type == OFPT_SET_ASYNC,
        classifyby = (OFPT_GET_ASYNC_REPLY, OFPT_SET_ASYNC),
        init = packvalue(OFPT_SET_ASYNC, 'header', 'type'),
        name = 'ofp_async_config'
    )
    
    
    ofp_error_types = dict(ofp_error_types)
    
    ofp_error_types.update({
        OFPET_BAD_INSTRUCTION : ofp_error_typedef(OFPET_BAD_INSTRUCTION, ofp_bad_instruction_code, OFP_VERSION, ofp_error_type),
        OFPET_BAD_MATCH : ofp_error_typedef(OFPET_BAD_MATCH, ofp_bad_match_code, OFP_VERSION, ofp_error_type),
        OFPET_FLOW_MOD_FAILED : ofp_error_typedef(OFPET_FLOW_MOD_FAILED, ofp_flow_mod_failed_code, OFP_VERSION, ofp_error_type),
        OFPET_GROUP_MOD_FAILED : ofp_error_typedef(OFPET_GROUP_MOD_FAILED, ofp_group_mod_failed_code, OFP_VERSION, ofp_error_type),
        OFPET_PORT_MOD_FAILED : ofp_error_typedef(OFPET_PORT_MOD_FAILED, ofp_port_mod_failed_code, OFP_VERSION, ofp_error_type),
        OFPET_TABLE_MOD_FAILED : ofp_error_typedef(OFPET_TABLE_MOD_FAILED, ofp_table_mod_failed_code, OFP_VERSION, ofp_error_type),
        OFPET_QUEUE_OP_FAILED : ofp_error_typedef(OFPET_QUEUE_OP_FAILED, ofp_queue_op_failed_code, OFP_VERSION, ofp_error_type),
        OFPET_SWITCH_CONFIG_FAILED : ofp_error_typedef(OFPET_SWITCH_CONFIG_FAILED, ofp_switch_config_failed_code, OFP_VERSION, ofp_error_type),
        OFPET_ROLE_REQUEST_FAILED : ofp_error_typedef(OFPET_ROLE_REQUEST_FAILED, ofp_role_request_failed_code, OFP_VERSION, ofp_error_type),
        OFPET_METER_MOD_FAILED : ofp_error_typedef(OFPET_METER_MOD_FAILED, ofp_meter_mod_failed_code, OFP_VERSION, ofp_error_type),
        OFPET_TABLE_FEATURES_FAILED : ofp_error_typedef(OFPET_TABLE_FEATURES_FAILED, ofp_table_features_failed_code, OFP_VERSION, ofp_error_type)
    })
    
    ofp_vendor_vendorid = 'experimenter'
    ofp_vendor_subtype = 'exp_type'
    
    ofp_action_vendor_vendorid = 'experimenter'
    ofp_action_vendor_subtype = 'exp_type'
    
    ofp_stats_vendor_vendorid = 'experimenter'
    ofp_stats_vendor_subtype = 'exp_type'
    
    from .nicira_ext import *
    
    '''
    /* Header for Nicira vendor requests and replies. */
    '''
    nicira_header = nstruct(
        base = ofp_experimenter,
        criteria = lambda x: x.experimenter == NX_VENDOR_ID,
        init = packvalue(NX_VENDOR_ID, 'experimenter'),
        name = 'nicira_header',
        classifier = lambda x: x.exp_type,
        extend = {'exp_type': nxt_subtype}
    )
    
    '''
    /* Header for Nicira-defined actions. */
    '''
    nx_action = nstruct(
        (nx_action_subtype, 'exp_type'),
        base = ofp_action_experimenter,
        criteria = lambda x: x.experimenter == NX_VENDOR_ID,
        init = packvalue(NX_VENDOR_ID, 'experimenter'),
        name = 'nx_action',
        classifier = lambda x: x.exp_type
    )
    
    nx_stats_request = nstruct(
        base = ofp_experimenter_multipart_request,
        criteria = lambda x: x.experimenter == NX_VENDOR_ID,
        init = packvalue(NX_VENDOR_ID, 'experimenter'),
        name = 'nx_stats_request',
        classifier = lambda x: x.exp_type,
        extend = {'exp_type': nx_stats_subtype}
    )
    
    nx_stats_reply = nstruct(
        base = ofp_experimenter_multipart_reply,
        criteria = lambda x: x.experimenter == NX_VENDOR_ID,
        init = packvalue(NX_VENDOR_ID, 'experimenter'),
        name = 'nx_stats_reply',
        classifier = lambda x: x.exp_type,
        extend = {'exp_type': nx_stats_subtype}
    )
    
    create_extension(globals(), nicira_header, nx_action, nx_stats_request, nx_stats_reply, ofp_vendor_subtype, ofp_action_vendor_subtype, ofp_stats_vendor_subtype)

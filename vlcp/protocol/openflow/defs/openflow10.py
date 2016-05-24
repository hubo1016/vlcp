'''
/*
 * Copyright (c) 2008, 2009, 2010, 2011, 2012, 2013 Nicira, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* OpenFlow: protocol between controller and datapath. */
Created on 2015/7/13

:author: hubo
'''
from .common import *
from . import common
from namedstruct.namedstruct import StructDefWarning
import warnings as _warnings

with _warnings.catch_warnings():
    _warnings.filterwarnings('ignore', '^padding', StructDefWarning)
    
    '''
    /* Port number(s)   meaning
     * ---------------  --------------------------------------
     * 0x0000           not assigned a meaning by OpenFlow 1.0
     * 0x0001...0xfeff  "physical" ports
     * 0xff00...0xfff7  "reserved" but not assigned a meaning by OpenFlow 1.0
     * 0xfff8...0xffff  "reserved" OFPP_* ports with assigned meanings
     */
    
    /* Ranges. */
    '''
    ofp_port = enum('ofp_port',
                       globals(),
                       uint16,
                        OFPP_MAX = 0xff00, # /* Max # of switch ports. */
                        
                        # /* Reserved output "ports". */
                        OFPP_IN_PORT = 0xfff8, # /* Where the packet came in. */
                        OFPP_TABLE = 0xfff9, # /* Perform actions in flow table. */
                        OFPP_NORMAL = 0xfffa, # /* Process with normal L2/L3. */
                        OFPP_FLOOD = 0xfffb, # /* All ports except input port and
                                            #                        * ports disabled by STP. */
                        OFPP_ALL = 0xfffc, # /* All ports except input port. */
                        OFPP_CONTROLLER = 0xfffd, # /* Send to controller. */
                        OFPP_LOCAL = 0xfffe, # /* Local openflow "port". */
                        OFPP_NONE = 0xffff # /* Not associated with any port. */
    )
    
    ofp_port_no = ofp_port
    
    OFPP_FIRST_RESV = 0xfff8, # /* First assigned reserved port. */
    OFPP_LAST_RESV = 0xffff, # /* Last assigned reserved port. */
    
    
    ofp_type = ofp_type.extend(globals(),
        OFPT_VENDOR             = 4,
        OFPT_FEATURES_REQUEST   = 5,  #/* Controller/switch message */
        OFPT_FEATURES_REPLY     = 6,  #/* Controller/switch message */
        OFPT_GET_CONFIG_REQUEST = 7,  #/* Controller/switch message */
        OFPT_GET_CONFIG_REPLY   = 8,  #/* Controller/switch message */
        OFPT_SET_CONFIG         = 9,  #/* Controller/switch message */
    
        OFPT_PACKET_IN          = 10, #/* Async message */
        OFPT_FLOW_REMOVED       = 11, #/* Async message */
        OFPT_PORT_STATUS        = 12, #/* Async message */
    
        OFPT_PACKET_OUT         = 13, #/* Controller/switch message */
        OFPT_FLOW_MOD           = 14, #/* Controller/switch message */
        OFPT_PORT_MOD           = 15, #/* Controller/switch message */
        OFPT_STATS_REQUEST      = 16, #/* Controller/switch message */
        OFPT_STATS_REPLY        = 17, #/* Controller/switch message */
    
        OFPT_BARRIER_REQUEST    = 18, #/* Controller/switch message */
        OFPT_BARRIER_REPLY      = 19, #/* Controller/switch message */
    
        OFPT_QUEUE_GET_CONFIG_REQUEST    = 20, #/* Controller/switch message */
        OFPT_QUEUE_GET_CONFIG_REPLY      = 21  #/* Controller/switch message */
    )
    
    ofp_type_reply_set = set([OFPT_ECHO_REPLY, OFPT_FEATURES_REPLY, OFPT_GET_CONFIG_REPLY, OFPT_STATS_REPLY, OFPT_BARRIER_REPLY, OFPT_QUEUE_GET_CONFIG_REPLY])
    
    ofp_type_asyncmessage_set = set([OFPT_PACKET_IN, OFPT_FLOW_REMOVED, OFPT_PORT_STATUS])
    
    OFP_VERSION = OFP10_VERSION
    
    ofp_msg = nstruct(name = 'ofp_msg',
                      base = common.ofp_msg_mutable,
                      criteria = lambda x: x.header.version == OFP_VERSION,
                      init = packvalue(OFP_VERSION, 'header', 'version'),
                      classifyby = (OFP_VERSION,),
                      classifier = lambda x: x.header.type,
                      extend = {('header','type') : ofp_type})
    
    ofp_vendor = nstruct((uint32, 'vendor'),
                         name = 'ofp_vendor',
                         base = ofp_msg,
                         criteria = lambda x: x.header.type == OFPT_VENDOR,
                         classifyby = (OFPT_VENDOR,),
                         init = packvalue(OFPT_VENDOR, 'header', 'type')
                         )
    
    ofp_error_type = ofp_error_type.extend(globals(),
                                           OFPET_FLOW_MOD_FAILED = 3,
                                           OFPET_PORT_MOD_FAILED = 4,
                                           OFPET_QUEUE_OP_FAILED = 5)
    
    '''
    /* ofp_error_msg 'code' values for OFPET_FLOW_MOD_FAILED. 'data' contains
    * at least the first 64 bytes of the failed request. */
    '''
    ofp_flow_mod_failed_code = enum('ofp_flow_mod_failed_code', globals(),
        OFPFMFC_ALL_TABLES_FULL = 0,        # /* Flow not added because of full tables. */
        OFPFMFC_OVERLAP = 1,                # /* Attempted to add overlapping flow with
    #                                          * CHECK_OVERLAP flag set. */
        OFPFMFC_EPERM = 2,                  # /* Permissions error. */
        OFPFMFC_BAD_EMERG_TIMEOUT = 3,      # /* Flow not added because of non-zero idle/hard
    #                                          * timeout. */
        OFPFMFC_BAD_COMMAND = 4,            # /* Unknown command. */
        OFPFMFC_UNSUPPORTED = 5,            # /* Unsupported action list - cannot process in
    #                                          * the order specified. */
    )
    
    '''
    /* ofp_error_msg 'code' values for OFPET_PORT_MOD_FAILED. 'data' contains
    * at least the first 64 bytes of the failed request. */
    '''
    ofp_port_mod_failed_code = enum('ofp_port_mod_failed_code', globals(),
        OFPPMFC_BAD_PORT = 0,       #/* Specified port does not exist. */
        OFPPMFC_BAD_HW_ADDR = 1,    #/* Specified hardware address is wrong. */
    )
    
    '''
    /* ofp_error msg 'code' values for OFPET_QUEUE_OP_FAILED. 'data' contains
    * at least the first 64 bytes of the failed request */
    '''
    ofp_queue_op_failed_code = enum('ofp_queue_op_failed_code', globals(),
        OFPQOFC_BAD_PORT = 0,   # /* Invalid port (or port does not exist). */
        OFPQOFC_BAD_QUEUE = 1,  # /* Queue does not exist. */
        OFPQOFC_EPERM = 2       # /* Permissions error. */
    )
    
    ofp_error_types = dict(ofp_error_types)
    
    ofp_error_types.update({OFPET_FLOW_MOD_FAILED : ofp_error_typedef(OFPET_FLOW_MOD_FAILED, ofp_flow_mod_failed_code, OFP_VERSION, ofp_error_type),
                            OFPET_PORT_MOD_FAILED : ofp_error_typedef(OFPET_PORT_MOD_FAILED, ofp_port_mod_failed_code, OFP_VERSION, ofp_error_type),
                            OFPET_QUEUE_OP_FAILED : ofp_error_typedef(OFPET_QUEUE_OP_FAILED, ofp_queue_op_failed_code, OFP_VERSION, ofp_error_type)})
    
    ofp_switch_config = nstruct((ofp_config_flags, 'flags'),
                                (uint16, 'miss_send_len'),
                                name = 'ofp_switch_config',
                                base = ofp_msg,
                                criteria = lambda x: x.header.type == OFPT_GET_CONFIG_REPLY or x.header.type == OFPT_SET_CONFIG,
                                classifyby = (OFPT_GET_CONFIG_REPLY, OFPT_SET_CONFIG),
                                init = packvalue(OFPT_SET_CONFIG, 'header','type'))
    
    
    
    '''
    /* OpenFlow 1.0 specific capabilities supported by the datapath (struct
     * ofp_switch_features, member capabilities). */
     '''
    ofp_capabilities  = ofp_capabilities.extend(
                               globals(),
                               OFPC_STP            = 1 << 3,  #/* 802.1d spanning tree. */
                               OFPC_RESERVED       = 1 << 4)  #/* Reserved, must not be set. */
    
    '''
    /* OpenFlow 1.0 specific current state of the physical port.  These are not
     * configurable from the controller.
     */
    /* The OFPPS10_STP_* bits have no effect on switch operation.  The
     * controller must adjust OFPPC_NO_RECV, OFPPC_NO_FWD, and
     * OFPPC_NO_PACKET_IN appropriately to fully implement an 802.1D spanning
     * tree. */
    '''
    ofp_port_state = ofp_port_state.extend(globals(),
        OFPPS_STP_LISTEN  = 0 << 8, # /* Not learning or relaying frames. */
        OFPPS_STP_LEARN   = 1 << 8, # /* Learning but not relaying frames. */
        OFPPS_STP_FORWARD = 2 << 8, # /* Learning and relaying frames. */
        OFPPS_STP_BLOCK   = 3 << 8  # /* Not part of spanning tree. */
        ) # /* Bit mask for OFPPS10_STP_* values. */
    
    OFPPS_STP_MASK    = 3 << 8
    
    OFPPS_ALL = OFPPS_LINK_DOWN | OFPPS_STP_MASK
    
    ofp_action_type = enum('ofp_action_type', globals(),
        uint16,
        OFPAT_OUTPUT = 0,             #/* Output to switch port. */
        OFPAT_SET_VLAN_VID = 1,       #/* Set the 802.1q VLAN id. */
        OFPAT_SET_VLAN_PCP = 2,       #/* Set the 802.1q priority. */
        OFPAT_STRIP_VLAN = 3,         #/* Strip the 802.1q header. */
        OFPAT_SET_DL_SRC = 4,         #/* Ethernet source address. */
        OFPAT_SET_DL_DST = 5,         #/* Ethernet destination address. */
        OFPAT_SET_NW_SRC = 6,         #/* IP source address. */
        OFPAT_SET_NW_DST = 7,         #/* IP destination address. */
        OFPAT_SET_NW_TOS = 8,         #/* IP ToS (DSCP field, 6 bits). */
        OFPAT_SET_TP_SRC = 9,         #/* TCP/UDP source port. */
        OFPAT_SET_TP_DST = 10,        #/* TCP/UDP destination port. */
        OFPAT_ENQUEUE = 11,           #/* Output to queue. */
        OFPAT_VENDOR = 0xffff)
    
    ofp_action = nstruct((ofp_action_type, 'type'),
                        (uint16, 'len'),
                        name = 'ofp_action',
                        size = sizefromlen(512, 'len'),
                        prepack = packsize('len'),
                        classifier = lambda x: x.type
                        )
    
    ofp_action_vendor = nstruct((uint32, 'vendor'),
                                       name = 'ofp_action_vendor',
                                       base = ofp_action,
                                       criteria = lambda x: x.type == OFPAT_VENDOR,
                                       classifyby = (OFPAT_VENDOR,),
                                       init = packvalue(OFPAT_VENDOR, 'type')
                                       )
    
    '''
    /* Action structure for OFPAT10_OUTPUT, which sends packets out 'port'.
     * When the 'port' is the OFPP_CONTROLLER, 'max_len' indicates the max
     * number of bytes to send.  A 'max_len' of zero means no bytes of the
     * packet should be sent. */
    '''
    ofp_action_output = nstruct((ofp_port, 'port'),
                                (uint16, 'max_len'),
                                name = 'ofp_action_output',
                                base = ofp_action,
                                criteria = lambda x: x.type == OFPAT_OUTPUT,
                                classifyby = (OFPAT_OUTPUT,),
                                init = packvalue(OFPAT_OUTPUT, 'type'))
    
    '''
    /* Action structure for OFPAT10_SET_VLAN_VID and OFPAT11_SET_VLAN_VID. */
    '''
    ofp_action_vlan_vid = nstruct(
        (uint16, 'vlan_vid'),             # /* VLAN id. */
        (uint8[2],),
        name = 'ofp_action_vlan_vid',
        base = ofp_action,
        criteria = lambda x: x.type == OFPAT_SET_VLAN_VID,
        classifyby = (OFPAT_SET_VLAN_VID,),
        init = packvalue(OFPAT_SET_VLAN_VID, 'type'))
    
    '''
    /* Action structure for OFPAT10_SET_VLAN_PCP and OFPAT11_SET_VLAN_PCP. */
    '''
    ofp_action_vlan_pcp = nstruct(
        (uint8, 'vlan_pcp'),            #  /* VLAN priority. */
        (uint8[3],),
        name = 'ofp_action_vlan_pcp',
        base = ofp_action,
        criteria = lambda x: x.type == OFPAT_SET_VLAN_PCP,
        classifyby = (OFPAT_SET_VLAN_PCP,),
        init = packvalue(OFPAT_SET_VLAN_PCP, 'type'))
    
    '''
    /* Action structure for OFPAT10_SET_DL_SRC/DST and OFPAT11_SET_DL_SRC/DST. */
    '''
    ofp_action_dl_addr = nstruct(
        (mac_addr, 'dl_addr'),  #  /* Ethernet address. */
        (uint8[6],),
        name = 'ofp_action_dl_addr',
        base = ofp_action,
        criteria = lambda x: x.type == OFPAT_SET_DL_SRC or x.type == OFPAT_SET_DL_DST,
        classifyby = (OFPAT_SET_DL_SRC, OFPAT_SET_DL_DST),
        init = packvalue(OFPAT_SET_DL_SRC, 'type'))
    
    '''
    /* Action structure for OFPAT10_SET_NW_SRC/DST and OFPAT11_SET_NW_SRC/DST. */
    '''
    ofp_action_nw_addr = nstruct(
        (ip4_addr, 'nw_addr'),               # /* IP address. */
        name = 'ofp_action_nw_addr',
        base = ofp_action,
        criteria = lambda x: x.type == OFPAT_SET_NW_SRC or x.type == OFPAT_SET_NW_DST,
        classifyby = (OFPAT_SET_NW_SRC, OFPAT_SET_NW_DST),
        init = packvalue(OFPAT_SET_NW_SRC, 'type'))
    
    '''
    /* Action structure for OFPAT10_SET_NW_TOS and OFPAT11_SET_NW_TOS. */
    '''
    ofp_action_nw_tos = nstruct(
        (uint8, 'nw_tos'),                 # /* DSCP in high 6 bits, rest ignored. */
        (uint8[3],),
        name = 'ofp_action_nw_tos',
        base = ofp_action,
        criteria = lambda x: x.type == OFPAT_SET_NW_TOS,
        classifyby = (OFPAT_SET_NW_TOS,),
        init = packvalue(OFPAT_SET_NW_TOS, 'type'))
    
    '''
    /* Action structure for OFPAT10_SET_TP_SRC/DST and OFPAT11_SET_TP_SRC/DST. */
    '''
    ofp_action_tp_port = nstruct(
        (uint16, 'tp_port'),               # /* TCP/UDP port. */
        (uint8[2],),
        name = 'ofp_action_tp_port',
        base = ofp_action,
        criteria = lambda x: x.type == OFPAT_SET_TP_SRC or x.type == OFPAT_SET_TP_DST,
        classifyby = (OFPAT_SET_TP_SRC, OFPAT_SET_TP_DST),
        init = packvalue(OFPAT_SET_TP_SRC, 'type'))
    
    
    '''
    /* OpenFlow 1.0 specific features of physical ports available in a datapath. */
    '''
    ofp_port_features = ofp_port_features.extend(globals(),
        OFPPF_COPPER     = 1 << 7,  #/* Copper medium. */
        OFPPF_FIBER      = 1 << 8,  #/* Fiber medium. */
        OFPPF_AUTONEG    = 1 << 9,  #/* Auto-negotiation. */
        OFPPF_PAUSE      = 1 << 10, #/* Pause. */
        OFPPF_PAUSE_ASYM = 1 << 11  #/* Asymmetric pause. */
    )
    
    '''
    /* Description of a physical port */
    '''
    ofp_phy_port = nstruct(
        (ofp_port, 'port_no'),
        (mac_addr, 'hw_addr'),
        (char[OFP_MAX_PORT_NAME_LEN], 'name'), #/* Null-terminated */
    
        (ofp_port_config, 'config'),     #   /* Bitmap of OFPPC_* and OFPPC10_* flags. */
        (ofp_port_state, 'state'),      #   /* Bitmap of OFPPS_* and OFPPS10_* flags. */
    
        #/* Bitmaps of OFPPF_* and OFPPF10_* that describe features.  All bits
        # * zeroed if unsupported or unavailable. */
        (ofp_port_features, 'curr'),       #   /* Current features. */
        (ofp_port_features, 'advertised'), #   /* Features being advertised by the port. */
        (ofp_port_features, 'supported'),  #   /* Features supported by the port. */
        (ofp_port_features, 'peer'),       #   /* Features advertised by peer. */
        name = 'ofp_phy_port'
    )
    
    ofp_action_type_bitwise = enum('ofp_action_type_bitwise', None, uint32, True,
                                   **dict((k, 1<<v) for (k,v) in ofp_action_type.getDict().items() if v < 32))
    
    ofp_switch_features = nstruct((uint64, 'datapath_id'),
                                  (uint32, 'n_buffers'),
                                  (uint8, 'n_tables'),
                                  (uint8[3],),
                                  (ofp_capabilities, 'capabilities'),
                                  (ofp_action_type_bitwise, 'actions'),
                                  (ofp_phy_port[0], 'ports'),
                                  name = 'ofp_switch_features',
                                  base = ofp_msg,
                                  criteria = lambda x: x.header.type == OFPT_FEATURES_REPLY,
                                  classifyby = (OFPT_FEATURES_REPLY,),
                                  init = packvalue(OFPT_FEATURES_REPLY, 'header', 'type'))
    
    
    '''
    /* Modify behavior of the physical port */
    '''
    ofp_port_mod = nstruct(
        (ofp_port, 'port_no'),
        (mac_addr, 'hw_addr'), 
    
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
    
    
    ofp_queue_prop_header = nstruct((ofp_queue_properties, 'property'),
                                    (uint16, 'len'),
                                    (uint8[4],),
                                    name = 'ofp_queue_prop_header')
    
    ofp_queue_prop = nstruct((ofp_queue_prop_header, 'prop_header'),
                             name = 'ofp_queue_prop',
                             size = sizefromlen(256, 'prop_header', 'len'),
                             prepack = packrealsize('prop_header', 'len'),
                             classifier = lambda x: x.prop_header.property
                             )
    
    ofp_queue_prop_min_rate = nstruct((uint16, 'rate'),
                                  (uint8[6],),
                                  base = ofp_queue_prop,
                                  criteria = lambda x: x.prop_header.property == OFPQT_MIN_RATE,
                                  classifyby = (OFPQT_MIN_RATE,),
                                  init = packvalue(OFPQT_MIN_RATE, 'prop_header', 'property'),
                                  name = 'ofp_queue_prop_min_rate')
    
    
    ofp_packet_queue = nstruct(
        (uint32, 'queue_id'),       #   /* id for the specific queue. */
        (uint16, 'len'),            #   /* Length in bytes of this queue desc. */
        (uint8[2],),                #   /* 64-bit alignment. */
        (ofp_queue_prop[0], 'properties'),
        name = 'ofp_packet_queue',
        size = sizefromlen(4096, 'len'),
        prepack = packsize('len')
        )
    '''
    /* Query for port queue configuration. */
    '''
    ofp_queue_get_config_request = nstruct(
        (uint16, 'port'),       #   /* Port to be queried. Should refer to a valid physical port (i.e. < OFPP_MAX) */
        (uint8[2],),
        name = 'ofp_queue_get_config_request',
        base = ofp_msg,
        criteria = lambda x: x.header.type == OFPT_QUEUE_GET_CONFIG_REQUEST,
        classifyby = (OFPT_QUEUE_GET_CONFIG_REQUEST,),
        init = packvalue(OFPT_QUEUE_GET_CONFIG_REQUEST, 'header', 'type')
    )
    
    '''
    /* Queue configuration for a given port. */
    '''
    ofp_queue_get_config_reply  = nstruct(
        (uint16, 'port'),
        (uint8[6],),
        (ofp_packet_queue[0], 'queues'),  # /* List of configured queues. */
        base = ofp_msg,
        name = 'ofp_queue_get_config_reply',
        criteria = lambda x: x.header.type == OFPT_QUEUE_GET_CONFIG_REPLY,
        classifyby = (OFPT_QUEUE_GET_CONFIG_REPLY,),
        init = packvalue(OFPT_QUEUE_GET_CONFIG_REPLY, 'header', 'type')
    )
    
    '''
    /* Packet received on port (datapath -> controller). */
    '''
    ofp_packet_in = nstruct(
        (uint32, 'buffer_id'),  #     /* ID assigned by datapath. */
        (uint16, 'total_len'),  #     /* Full length of frame. */
        (ofp_port, 'in_port'),    #     /* Port on which frame was received. */
        (ofp_packet_in_reason, 'reason'),      #     /* Reason packet is being sent (one of OFPR_*) */
        (uint8,),
        (raw, 'data'),          
        base = ofp_msg,
        name = 'ofp_packet_in',
        criteria = lambda x: x.header.type == OFPT_PACKET_IN,
        classifyby = (OFPT_PACKET_IN,),
        init = packvalue(OFPT_PACKET_IN, 'header', 'type')
    )
    
    '''
    /* OFPAT10_ENQUEUE action struct: send packets to given queue on port. */
    '''
    ofp_action_enqueue = nstruct(
        (uint16, 'port'),       #     /* Port that queue belongs. Should
        (uint8[6],),            #     /* Pad for 64-bit alignment. */
        (uint32, 'queue_id'),    #     /* Where to enqueue the packets. */
        name ='ofp_action_enqueue',
        base = ofp_action,
        criteria = lambda x: x.type == OFPAT_ENQUEUE,
        classifyby = (OFPAT_ENQUEUE,),
        init = packvalue(OFPAT_ENQUEUE, 'type')
    )
    
    '''
    /* Send packet (controller -> datapath). */
    '''
    
    def _ofp_packet_out_actions_packsize(x):
        x.actions_len = x._realsize() - 2
    ofp_packet_out_actions = nstruct(
        (uint16, 'actions_len'),
        (ofp_action[0], 'actions'),
        name = 'ofp_packet_out_actions',
        size = lambda x: x.actions_len + 2,
        prepack = _ofp_packet_out_actions_packsize,
        padding = 1)
    
    ofp_packet_out = nstruct(
        (uint32, 'buffer_id'),        #   /* ID assigned by datapath or UINT32_MAX. */
        (ofp_port, 'in_port'),          #   /* Packet's input port (OFPP_NONE if none). */
        (ofp_packet_out_actions,),
        (raw, 'data'),
        name = 'ofp_packet_out',
        base = ofp_msg,
        criteria = lambda x: x.header.type == OFPT_PACKET_OUT,
        classifyby = (OFPT_PACKET_OUT,),
        init = packvalue(OFPT_PACKET_OUT, 'header', 'type')
    )
    
    '''
    /* Flow wildcards. */
    '''
    
    OFPFW_NW_SRC_SHIFT = 8
    OFPFW_NW_SRC_BITS = 6
    
    OFPFW_NW_DST_SHIFT = 14
    OFPFW_NW_DST_BITS = 6
    
    
    ofp_flow_wildcards = enum('ofp_flow_wildcards', globals(),uint32,True,
        OFPFW_IN_PORT    = 1 << 0,  #/* Switch input port. */
        OFPFW_DL_VLAN    = 1 << 1,  #/* VLAN vid. */
        OFPFW_DL_SRC     = 1 << 2,  #/* Ethernet source address. */
        OFPFW_DL_DST     = 1 << 3,  #/* Ethernet destination address. */
        OFPFW_DL_TYPE    = 1 << 4,  #/* Ethernet frame type. */
        OFPFW_NW_PROTO   = 1 << 5,  #/* IP protocol. */
        OFPFW_TP_SRC     = 1 << 6,  #/* TCP/UDP source port. */
        OFPFW_TP_DST     = 1 << 7,  #/* TCP/UDP destination port. */
    #/* IP source address wildcard bit count.  0 is exact match, 1 ignores the
    #* LSB, 2 ignores the 2 least-significant bits, ..., 32 and higher wildcard
    #* the entire field.  This is the *opposite* of the usual convention where
    #* e.g. /24 indicates that 8 bits (not 24 bits) are wildcarded. */
        OFPFW_NW_SRC_MASK = (((1 << OFPFW_NW_SRC_BITS) - 1)
                               << OFPFW_NW_SRC_SHIFT),
        OFPFW_NW_SRC_ALL = 32 << OFPFW_NW_SRC_SHIFT,
    
    #    /* IP destination address wildcard bit count.  Same format as source. */
        OFPFW_NW_DST_MASK = (((1 << OFPFW_NW_DST_BITS) - 1)
                               << OFPFW_NW_DST_SHIFT),
        OFPFW_NW_DST_ALL = 32 << OFPFW_NW_DST_SHIFT,
    
        OFPFW_DL_VLAN_PCP = 1 << 20, # /* VLAN priority. */
        OFPFW_NW_TOS = 1 << 21, # /* IP ToS (DSCP field, 6 bits). */
    
    #    /* Wildcard all fields. */
        OFPFW_ALL = ((1 << 22) - 1)
    )
    
    
    
    #/* The wildcards for ICMP type and code fields use the transport source
    # * and destination port fields, respectively. */
    OFPFW_ICMP_TYPE = OFPFW_TP_SRC
    OFPFW_ICMP_CODE = OFPFW_TP_DST
    
    #/* The VLAN id is 12-bits, so we can use the entire 16 bits to indicate
    # * special conditions.  All ones indicates that 802.1Q header is not present.
    # */
    OFP_VLAN_NONE = 0xffff
    
    '''
    /* Fields to match against flows */
    '''
    ofp_match = nstruct(
        (ofp_flow_wildcards, 'wildcards'),    #    /* Wildcard fields. */
        (ofp_port, 'in_port'),      #    /* Input switch port. */
        (mac_addr, 'dl_src'),  #    /* Ethernet source address. */
        (mac_addr, 'dl_dst'), #    /* Ethernet destination address. */
        (uint16, 'dl_vlan'),      #    /* Input VLAN. */
        (uint8, 'dl_vlan_pcp'),   #    /* Input VLAN priority. */
        (uint8[1],),              #    /* Align to 64-bits. */
        (ethertype, 'dl_type'),      #    /* Ethernet frame type. */
        (uint8, 'nw_tos'),        #    /* IP ToS (DSCP field, 6 bits). */
        (uint8, 'nw_proto'),      #    /* IP protocol or lower 8 bits of ARP opcode. */
        (uint8[2],),              #    /* Align to 64-bits. */
        (ip4_addr, 'nw_src'),       #    /* IP source address. */
        (ip4_addr, 'nw_dst'),       #    /* IP destination address. */
        (uint16, 'tp_src'),       #    /* TCP/UDP source port. */
        (uint16, 'tp_dst'),       #    /* TCP/UDP destination port. */
        name = 'ofp_match'
    )
    
    ofp_flow_mod_flags = ofp_flow_mod_flags.extend(globals(),
        OFPFF_EMERG       = 1 << 2 #/* Part of "emergency flow cache". */
    )
    
    '''
    /* Flow setup and teardown (controller -> datapath). */
    '''
    ofp_flow_mod = nstruct(
        (ofp_match, 'match'),   #    /* Fields to match */
        (uint64, 'cookie'),     #    /* Opaque controller-issued identifier. */
    
    #    /* Flow actions. */
        (ofp_flow_mod_command, 'command'),    #         /* One of OFPFC_*. */
        (uint16, 'idle_timeout'),  #      /* Idle time before discarding (seconds). */
        (uint16, 'hard_timeout'),  #      /* Max time before discarding (seconds). */
        (uint16, 'priority'),      #      /* Priority level of flow entry. */
        (uint32, 'buffer_id'),     #      /* Buffered packet to apply to (or -1). Not meaningful for OFPFC_DELETE*. */
    #/* For OFPFC_DELETE* commands, require matching entries to include this as an
    # output port.  A value of OFPP_NONE indicates no restriction. */
        (ofp_port, 'out_port'),      
        (ofp_flow_mod_flags, 'flags'),         #      /* One of OFPFF_*. */
        (ofp_action[0], 'actions'),  #      /* The action length is inferred from the length field in the header. */
        base = ofp_msg,
        criteria = lambda x: x.header.type == OFPT_FLOW_MOD,
        classifyby = (OFPT_FLOW_MOD,),
        init = packvalue(OFPT_FLOW_MOD, 'header', 'type'),
        name = 'ofp_flow_mod'
    )
    
    '''
    /* Flow removed (datapath -> controller). */
    '''
    ofp_flow_removed = nstruct(
        (ofp_match, 'match'),   # /* Description of fields. */
        (uint64, 'cookie'),     # /* Opaque controller-issued identifier. */
    
        (uint16, 'priority'),   # /* Priority level of flow entry. */
        (ofp_flow_removed_reason, 'reason'),      # /* One of OFPRR_*. */
        (uint8[1],),            # /* Align to 32-bits. */
    
        (uint32, 'duration_sec'), #    /* Time flow was alive in seconds. */
        (uint32, 'duration_nsec'),#    /* Time flow was alive in nanoseconds beyond duration_sec. */
        (uint16, 'idle_timeout'), #    /* Idle timeout from original flow mod. */
        (uint8[2],),               #    /* Align to 64-bits. */
        (uint64, 'packet_count'),
        (uint64, 'byte_count'),
        name = 'ofp_flow_removed',
        base = ofp_msg,
        criteria = lambda x: x.header.type == OFPT_FLOW_REMOVED,
        classifyby = (OFPT_FLOW_REMOVED,),
        init = packvalue(OFPT_FLOW_REMOVED, 'header', 'type')
    )
    
    ofp_port_status = nstruct(
        (ofp_port_reason, 'reason'),
        (uint8[7],),
        (ofp_phy_port, 'desc'),
        name= 'ofp_port_status',
        base = ofp_msg,
        criteria = lambda x: x.header.type == OFPT_PORT_STATUS,
        classifyby = (OFPT_PORT_STATUS,),
        init = packvalue(OFPT_PORT_STATUS, 'header', 'type')
    )
    
    '''
    /* Statistics request or reply message. */
    '''
    
    ofp_stats_types = enum('ofp_stats_types', globals(),uint16,
    #/* Description of this OpenFlow switch.
    #* The request body is empty.
    #* The reply body is struct ofp_desc_stats. */
            OFPST_DESC = 0,
    #/* Individual flow statistics.
    #* The request body is struct ofp_flow_stats_request.
    #* The reply body is an array of struct ofp_flow_stats. */
            OFPST_FLOW = 1,
    #/* Aggregate flow statistics.
    #* The request body is struct ofp_aggregate_stats_request.
    #* The reply body is struct ofp_aggregate_stats_reply. */
            OFPST_AGGREGATE = 2,
    #/* Flow table statistics.
    #* The request body is empty.
    #* The reply body is an array of struct ofp_table_stats. */
            OFPST_TABLE = 3,
    #/* Physical port statistics.
    #* The request body is struct ofp_port_stats_request.
    #* The reply body is an array of struct ofp_port_stats. */
            OFPST_PORT = 4,
    #/* Queue statistics for a port
    #* The request body defines the port
    #* The reply body is an array of struct ofp_queue_stats */
            OFPST_QUEUE = 5,
    #/* Vendor extension.
    #* The request and reply bodies begin with a 32-bit vendor ID, which takes
    #* the same form as in "struct ofp_vendor_header". The request and reply
    #* bodies are otherwise vendor-defined. */
            OFPST_VENDOR = 0xffff
    )
    
    
    ofp_stats_msg = nstruct(
        (ofp_stats_types, 'type'),       #       /* One of the OFPST_* constants. */
        (ofp_stats_reply_flags, 'flags'),      #       /* Requests: always 0.
    #                                 * Replies: 0 or OFPSF_REPLY_MORE. */
        name = 'ofp_stats_msg',
        base = ofp_msg,
        criteria = lambda x: x.header.type == OFPT_STATS_REQUEST or x.header.type == OFPT_STATS_REPLY,
        classifyby = (OFPT_STATS_REQUEST, OFPT_STATS_REPLY),
        init = packvalue(OFPT_STATS_REQUEST, 'header', 'type')
    )
    
    ofp_stats_request = nstruct(
        name = 'ofp_stats_request',
        base = ofp_stats_msg,
        criteria = lambda x: x.header.type == OFPT_STATS_REQUEST,
        classifier = lambda x: x.type,
        init = packvalue(OFPT_STATS_REQUEST, 'header', 'type')
    )
    
    ofp_stats_reply = nstruct(
        name = 'ofp_stats_request',
        base = ofp_stats_msg,
        criteria = lambda x: x.header.type == OFPT_STATS_REPLY,
        classifier = lambda x: x.type,
        init = packvalue(OFPT_STATS_REPLY, 'header', 'type')
    )
    
    
    
    DESC_STR_LEN = 256
    SERIAL_NUM_LEN = 32
    
    ofp_desc_stats = nstruct((char[DESC_STR_LEN], 'mfr_desc'),
                       (char[DESC_STR_LEN], 'hw_desc'),
                       (char[DESC_STR_LEN], 'sw_desc'),
                       (char[SERIAL_NUM_LEN], 'serial_num'),
                       (char[DESC_STR_LEN], 'dp_desc'),
                       name = 'ofp_desc_stats')
    
    ofp_desc_stats_reply = nstruct(
                        (ofp_desc_stats,),
                        name = 'ofp_desc_stats_reply',
                        base = ofp_stats_reply,
                        criteria = lambda x: x.type == OFPST_DESC,
                        classifyby = (OFPST_DESC,),
                        init = packvalue(OFPST_DESC, 'type')
                                   )
    
    '''
    /* Stats request of type OFPST_AGGREGATE or OFPST_FLOW. */
    '''
    ofp_flow_stats_request = nstruct(
        (ofp_match, 'match'),   # /* Fields to match. */
        (ofp_table, 'table_id'),    # /* ID of table to read (from ofp_table_stats) or 0xff for all tables. */
        (uint8,),               # /* Align to 32 bits. */
        (ofp_port, 'out_port'),    # /* Require matching entries to include this as an output port.  A value of OFPP_NONE indicates no restriction. */
        name = 'ofp_flow_stats_request',
        base = ofp_stats_request,
        criteria = lambda x: x.type == OFPST_FLOW or x.type == OFPST_AGGREGATE,
        classifyby = (OFPST_FLOW, OFPST_AGGREGATE),
        init = packvalue(OFPST_FLOW, 'type')
    )
    
    '''
    /* Body of reply to OFPST_FLOW request. */
    '''
    ofp_flow_stats = nstruct(
        (uint16, 'length'),        #/* Length of this entry. */
        (uint8, 'table_id'),       #/* ID of table flow came from. */
        (uint8,),
        (ofp_match, 'match'),       #/* Description of fields. */
        (uint32, 'duration_sec'),  #/* Time flow has been alive in seconds. */
        (uint32, 'duration_nsec'), #/* Time flow has been alive in nanoseconds beyond duration_sec. */
        (uint16, 'priority'),      #/* Priority of the entry. Only meaningful when this is not an exact-match entry. */
        (uint16, 'idle_timeout'),  #/* Number of seconds idle before expiration. */
        (uint16, 'hard_timeout'),  #/* Number of seconds before expiration. */
        (uint8[6],),               #/* Align to 64 bits. */
        (uint64, 'cookie'),        #/* Opaque controller-issued identifier. */
        (uint64, 'packet_count'),  #/* Number of packets in flow. */
        (uint64, 'byte_count'),    #/* Number of bytes in flow. */
        (ofp_action[0], 'actions'),#/* Actions. */
        name = 'ofp_flow_stats',
        size = sizefromlen(4096, 'length'),
        prepack = packsize('length')
    )
    
    ofp_flow_stats_reply = nstruct(
        (ofp_flow_stats[0], 'stats'),
        name = 'ofp_flow_stats_reply',
        base = ofp_stats_reply,
        criteria = lambda x: x.type == OFPST_FLOW,
        classifyby = (OFPST_FLOW,),
        init = packvalue(OFPST_FLOW, 'type')
    )
    
    ofp_table = enum('ofp_table',
                     globals(),
                     uint8,
                     OFPTT_ALL = 0xff)
    
    '''
    /* Body for ofp_stats_request of type OFPST_AGGREGATE. */
    '''
    ofp_aggregate_stats_request = nstruct(
        (ofp_match, 'match'),             # /* Fields to match. */
        (ofp_table, 'table_id'),          # /* ID of table to read (from ofp_table_stats)
                                          #  0xff for all tables or 0xfe for emergency. */
        (uint8,),                         # /* Align to 32 bits. */
        (ofp_port, 'out_port'),           # /* Require matching entries to include this
                                          # as an output port. A value of OFPP_NONE
                                          # indicates no restriction. */
        base = ofp_stats_request,
        criteria = lambda x: x.type == OFPST_AGGREGATE,
        classifyby = (OFPST_AGGREGATE,),
        init = packvalue(OFPST_AGGREGATE, 'type'),
        name = 'ofp_aggregate_stats_request'
    )
    
    
    '''
    /* Body of reply to OFPST_AGGREGATE request. */
    '''
    ofp_aggregate_stats_reply = nstruct(
        (uint64, 'packet_count'),           # /* Number of packets in flows. */
        (uint64, 'byte_count'),             # /* Number of bytes in flows. */
        (uint32, 'flow_count'),             # /* Number of flows. */
        (uint8[4],),
        base = ofp_stats_reply,
        criteria = lambda x: x.type == OFPST_AGGREGATE,
        classifyby = (OFPST_AGGREGATE,),
        init = packvalue(OFPST_AGGREGATE, 'type'),
        name = 'ofp_aggregate_stats_reply'
    )
    
    
    '''
    /* Body of reply to OFPST_TABLE request. */
    '''
    ofp_table_stats = nstruct(
        (uint8, 'table_id'),        # /* Identifier of table.  Lower numbered tables are consulted first. */
        (uint8[3],),                # /* Align to 32-bits. */
        (char[OFP_MAX_TABLE_NAME_LEN], 'name'),
        (ofp_flow_wildcards, 'wildcards'),      # /* Bitmap of OFPFW_* wildcards that are supported by the table. */
        (uint32, 'max_entries'),    # /* Max number of entries supported. */
        (uint32, 'active_count'),   # /* Number of active entries. */
        (uint64, 'lookup_count'),   # /* # of packets looked up in table. */
        (uint64, 'matched_count'),   # /* Number of packets that hit table. */
        name = 'ofp_table_stats'
    )
    
    ofp_table_stats_reply = nstruct(
        (ofp_table_stats[0], 'stats'),
        name = 'ofp_table_stats_reply',
        base = ofp_stats_reply,
        criteria = lambda x: x.type == OFPST_TABLE,
        classifyby = (OFPST_TABLE,),
        init = packvalue(OFPST_TABLE, 'type')
    )
    
    '''
    /* Stats request of type OFPST_PORT. */
    '''
    ofp_port_stats_request = nstruct(
        (ofp_port, 'port_no'),
    #/* OFPST_PORT message may request statistics for a single port (specified with port_no)
    # or for all ports (port_no == OFPP_NONE). */
        (uint8[6],),
        name = 'ofp_port_stats_request',
        base = ofp_stats_request,
        criteria = lambda x: x.type == OFPST_PORT,
        classifyby = (OFPST_PORT,),
        init = packvalue(OFPST_PORT, 'type')
    )
    
    '''
    /* Body of reply to OFPST_PORT request. If a counter is unsupported, set
     * the field to all ones. */
     '''
    ofp_port_stats = nstruct(
        (uint16, 'port_no'),
        (uint8[6],),
        (uint64, 'rx_packets'),    # /* Number of received packets. */
        (uint64, 'tx_packets'),    # /* Number of transmitted packets. */
        (uint64, 'rx_bytes'),      # /* Number of received bytes. */
        (uint64, 'tx_bytes'),      # /* Number of transmitted bytes. */
        (uint64, 'rx_dropped'),    # /* Number of packets dropped by RX. */
        (uint64, 'tx_dropped'),    # /* Number of packets dropped by TX. */
        (uint64, 'rx_errors'),     # /* Number of receive errors.  This is a
    #super-set of receive errors and should be
    #great than or equal to the sum of all
    #rx_*_err values. */
        (uint64, 'tx_errors'),     # /* Number of transmit errors.  This is a super-set of transmit errors. */
        (uint64, 'rx_frame_err'),  # /* Number of frame alignment errors. */
        (uint64, 'rx_over_err'),   # /* Number of packets with RX overrun. */
        (uint64, 'rx_crc_err'),    # /* Number of CRC errors. */
        (uint64, 'collisions'),    # /* Number of collisions. */
        name = 'ofp_port_stats'
    )
    
    ofp_port_stats_reply = nstruct(
        (ofp_port_stats[0], 'stats'),
        name = 'ofp_port_stats_reply',
        base = ofp_stats_reply,
        criteria = lambda x: x.type == OFPST_PORT,
        classifyby = (OFPST_PORT,),
        init = packvalue(OFPST_PORT, 'type')
    )
    
    '''
    /* All ones is used to indicate all queues in a port (for stats retrieval). */
    '''
    ofp_queue = enum('ofp_queue', globals(), uint32,
    OFPQ_ALL = 0xffffffff)
    
    '''
    /* Body for stats request of type OFPST_QUEUE. */
    '''
    ofp_queue_stats_request = nstruct(
        (ofp_port, 'port_no'),     #   /* All ports if OFPP_ALL. */
        (uint8[2],),             #   /* Align to 32-bits. */
        (ofp_queue, 'queue_id'),    #   /* All queues if OFPQ_ALL. */
        name = 'ofp_queue_stats_request',
        base = ofp_stats_request,
        criteria = lambda x: x.type == OFPST_QUEUE,
        classifyby = (OFPST_QUEUE,),
        init = packvalue(OFPST_QUEUE, 'type')
    )
    
    '''
    /* Body for stats reply of type OFPST_QUEUE consists of an array of this
     * structure type. */
     '''
     
    ofp_queue_stats = nstruct(
        (uint16, 'port_no'),
        (uint8[2],),                # /* Align to 32-bits. */
        (uint32, 'queue_id'),       # /* Queue id. */
        (uint64, 'tx_bytes'),       # /* Number of transmitted bytes. */
        (uint64, 'tx_packets'),     # /* Number of transmitted packets. */
        (uint64, 'tx_errors'),      # /* # of packets dropped due to overrun. */
        name = 'ofp_queue_stats'
    )
    
    ofp_queue_stats_reply = nstruct(
        (ofp_queue_stats[0], 'stats'),
        name = 'ofp_queue_stats_reply',
        base = ofp_stats_reply,
        criteria = lambda x: x.type == OFPST_QUEUE,
        classifyby = (OFPST_QUEUE,),
        init = packvalue(OFPST_QUEUE, 'type')
    )
    
    '''
    /* Vendor extension stats message. */
    '''
    ofp_vendor_stats_request = nstruct(
        (uint32, 'vendor'),
        name = 'ofp_vendor_stats_request',
        base = ofp_stats_request,
        criteria = lambda x: x.type == OFPST_VENDOR,
        classifyby = (OFPST_VENDOR,),
        init = packvalue(OFPST_VENDOR, 'type')
    #    /* Followed by vendor-defined arbitrary additional data. */
    )
    
    ofp_vendor_stats_reply = nstruct(
        (uint32, 'vendor'),
        name = 'ofp_vendor_stats_reply',
        base = ofp_stats_reply,
        criteria = lambda x: x.type == OFPST_VENDOR,
        classifyby = (OFPST_VENDOR,),
        init = packvalue(OFPST_VENDOR, 'type')
    #    /* Followed by vendor-defined arbitrary additional data. */
    )
    
    ofp_vendor_vendorid = 'vendor'
    ofp_vendor_subtype = 'subtype'
    
    ofp_action_vendor_vendorid = 'vendor'
    ofp_action_vendor_subtype = 'subtype'
    
    ofp_stats_vendor_vendorid = 'vendor'
    ofp_stats_vendor_subtype = 'subtype'
    
    from .nicira_ext import *
    
    '''
    /* Header for Nicira vendor requests and replies. */
    '''
    nicira_header = nstruct(
        (nxt_subtype, 'subtype'),
        name = 'nicira_header',
        base = ofp_vendor,
        criteria = lambda x: x.vendor == NX_VENDOR_ID,
        init = packvalue(NX_VENDOR_ID, 'vendor'),
        classifier = lambda x: x.subtype
    )
    
    '''
    /* Header for Nicira-defined actions. */
    '''
    nx_action = nstruct(
        (nx_action_subtype, 'subtype'),     #               /* NXAST_*. */
        name = 'nx_action',
        base = ofp_action_vendor,
        criteria = lambda x: x.vendor == NX_VENDOR_ID,
        init = packvalue(NX_VENDOR_ID, 'vendor'),
        classifier = lambda x: x.subtype
    )
    
    nx_stats_request = nstruct(
        (nx_stats_subtype, 'subtype'),
        (uint8[4],),
        base = ofp_vendor_stats_request,
        criteria = lambda x: x.vendor == NX_VENDOR_ID,
        init = packvalue(NX_VENDOR_ID, 'vendor'),
        name = 'nx_stats_request',
        classifier = lambda x: getattr(x, 'subtype')
    )
    
    nx_stats_reply = nstruct(
        (nx_stats_subtype, 'subtype'),
        (uint8[4],),
        base = ofp_vendor_stats_reply,
        criteria = lambda x: x.vendor == NX_VENDOR_ID,
        init = packvalue(NX_VENDOR_ID, 'vendor'),
        name = 'nx_stats_reply',
        classifier = lambda x: getattr(x, 'subtype')
    )
    
    create_extension(globals(), nicira_header, nx_action, nx_stats_request, nx_stats_reply, ofp_vendor_subtype, ofp_action_vendor_subtype, ofp_stats_vendor_subtype)

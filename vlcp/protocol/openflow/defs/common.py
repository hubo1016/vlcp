'''
/* Copyright (c) 2008, 2011, 2012, 2013, 2014 The Board of Trustees of The Leland Stanford
 * Junior University
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

/*
 * Copyright (c) 2008-2014 Nicira, Inc.
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
Created on 2015/7/13

:author: hubo
'''
from namedstruct import *
from namedstruct.namedstruct import NamedStruct, StructDefWarning
from vlcp.utils.ethernet import *
import warnings as _warnings

with _warnings.catch_warnings():
    _warnings.filterwarnings('ignore', '^padding', StructDefWarning)
    ofp_version =  enum('ofp_version',
                        globals(),
                        uint8,
                        OFP10_VERSION = 0x01,
                        OFP11_VERSION = 0x02,
                        OFP12_VERSION = 0x03,
                        OFP13_VERSION = 0x04,
                        OFP14_VERSION = 0x05,
                        OFP15_VERSION = 0x06)
    
    ofp_version_bitwise = enum('ofp_version_bitwise', None, uint32, True,
                               **dict((k, 1<<v) for k,v in ofp_version.getDict().items()))
    
    OF_VENDOR_ID = 0
    NX_VENDOR_ID = 0x00002320
    ONF_VENDOR_ID = 0x4f4e4600
    
    OFP_MAX_TABLE_NAME_LEN = 32
    OFP_MAX_PORT_NAME_LEN = 16
    
    OFP_PORT = 6653
    OFP_TCP_PORT = 6653
    OFP_SSL_PORT = 6653
    
    OFP_ETH_ALEN = 6
    
    ofp_type = enum('ofp_type', globals(), uint8,
        OFPT_HELLO              = 0,  #/* Symmetric message */
        OFPT_ERROR              = 1,  #/* Symmetric message */
        OFPT_ECHO_REQUEST       = 2,  #/* Symmetric message */
        OFPT_ECHO_REPLY         = 3,  #/* Symmetric message */
    )
    
    OFP_DEFAULT_MISS_SEND_LEN = 128
    
    OFP_DL_TYPE_ETH2_CUTOFF = 0x0600
    
    OFP_DL_TYPE_NOT_ETH_TYPE = 0x05ff
    
    OFP_FLOW_PERMANENT = 0
    
    OFP_DEFAULT_PRIORITY = 0x8000
    
    ofp_header = nstruct((ofp_version, 'version'),
                         (ofp_type, 'type'),
                         (uint16, 'length'),
                         (uint32, 'xid'),
                         name = 'ofp_header')
    
    ofp_msg = nstruct((ofp_header, 'header'),
                      name = 'ofp_msg',
                      padding = 1,
                      size = sizefromlen(65536, 'header', 'length'),
                      prepack = packrealsize('header', 'length'))
    
    ofp_msg_mutable = nstruct(
        name = 'ofp_msg_mutable',
        base = ofp_msg,
        criteria = lambda x: x.header.type > OFPT_ECHO_REPLY,
        classifier = lambda x: x.header.version
    )
    
    NamedStruct._registerPickleType('protocol.openflow.defs.common.ofp_msg', ofp_msg)
    
    ofp_error_type = enum('ofp_error_type', globals(), uint16,
        OFPET_HELLO_FAILED         = 0,  #/* Hello protocol failed. */
        OFPET_BAD_REQUEST          = 1,  #/* Request was not understood. */
        OFPET_BAD_ACTION           = 2,  #/* Error in action description. */
        NXET_VENDOR = 0xb0c2,
        OFPET_EXPERIMENTER = 0xffff
    )
    
    ofp_error_msg_base = nstruct((ofp_error_type, 'type'),
                            name = 'ofp_error_msg_base',
                            base = ofp_msg,
                            criteria = lambda x: x.header.type == OFPT_ERROR,
                            init = packvalue(OFPT_ERROR, 'header', 'type'))
    
    ofp_error_msg = nstruct((uint16, 'code'),
                            (raw, 'data'),
                            name = 'ofp_error_msg',
                            base = ofp_error_msg_base,
                            criteria = lambda x: x.type != OFPET_EXPERIMENTER or x.header.length < 14)
    
    ofp_error_experimenter_msg = nstruct(
        (uint16, 'exp_type'),
        (uint32, 'experimenter'),  #  /* Experimenter ID which takes the same form as in struct ofp_experimenter_header. */
    #    (raw, 'data'),          # /* Variable-length data.  Interpreted based on the type and code.  No padding. */
        name = 'ofp_error_experimenter_msg',
        base = ofp_error_msg_base,
        criteria = lambda x: x.header.length >= 14
    )
    
    '''
    /* ofp_error msg 'code' values for NXET_VENDOR. */
    '''
    nx_vendor_code = enum('nx_vendor_code', globals(), uint16,
        NXVC_VENDOR_ERROR = 0,         #  /* 'data' contains struct nx_vendor_error. */
    )
    
    '''
    /* ofp_error_msg 'code' values for OFPET_HELLO_FAILED.  'data' contains an
     * ASCII text string that may give failure details. */
    '''
    ofp_hello_failed_code = enum('ofp_hello_failed_code', globals(),
        OFPHFC_INCOMPATIBLE = 0, #   /* No compatible version. */
        OFPHFC_EPERM        = 1, #   /* Permissions error. */
    )
    
    def ofp_error_typedef(subtype, codeenum, version = None, extendtype = None):
        if version is None:
            return nstruct(name = 'ofp_error_msg',
                           base = ofp_error_msg,
                           criteria = lambda x: x.type == subtype,
                           init = packvalue(subtype, 'type'),
                           extend = {'code' : codeenum}
                           )
        else:
            ext = {'code' : codeenum}
            if extendtype is not None:
                ext['type'] = extendtype
            return nstruct(name = 'ofp_error_msg',
                           base = ofp_error_msg,
                           criteria = lambda x: x.type == subtype and x.header.version == version,
                           init = packvalue(subtype, 'type'),
                           extend = ext
                           )
    
    '''
    /* ofp_error_msg 'code' values for OFPET_BAD_REQUEST.  'data' contains at least
     * the first 64 bytes of the failed request. */
    '''
    ofp_bad_request_code = enum('ofp_bad_request_code', globals(),
        OFPBRC_BAD_VERSION      = 0, # /* ofp_header.version not supported. */
        OFPBRC_BAD_TYPE         = 1, # /* ofp_header.type not supported. */
        OFPBRC_BAD_MULTIPART    = 2, # /* ofp_multipart_request.type not supported. */
        OFPBRC_BAD_EXPERIMENTER = 3, # /* Experimenter id not supported
    #                                   * (in ofp_experimenter_header or
    #                                   * ofp_multipart_request or
    #                                   * ofp_multipart_reply). */
        OFPBRC_BAD_EXP_TYPE     = 4, # /* Experimenter type not supported. */
        OFPBRC_EPERM            = 5, # /* Permissions error. */
        OFPBRC_BAD_LEN          = 6, # /* Wrong request length for type. */
        OFPBRC_BUFFER_EMPTY     = 7, # /* Specified buffer has already been used. */
        OFPBRC_BUFFER_UNKNOWN   = 8, # /* Specified buffer does not exist. */
        OFPBRC_BAD_TABLE_ID     = 9, # /* Specified table-id invalid or does not
    #                                   * exist. */
        OFPBRC_IS_SLAVE         = 10,# /* Denied because controller is slave. */
        OFPBRC_BAD_PORT         = 11,# /* Invalid port. */
        OFPBRC_BAD_PACKET       = 12,# /* Invalid packet in packet-out. */
        OFPBRC_MULTIPART_BUFFER_OVERFLOW    = 13, #/* ofp_multipart_request
    #                                     overflowed the assigned buffer. */
    )
    OFPBRC_BAD_STAT = OFPBRC_BAD_MULTIPART
    OFPBRC_BAD_VENDOR = OFPBRC_BAD_EXPERIMENTER
    OFPBRC_BAD_SUBTYPE = OFPBRC_BAD_EXP_TYPE
    
    
    '''
    /* ofp_error_msg 'code' values for OFPET_BAD_ACTION.  'data' contains at least
     * the first 64 bytes of the failed request. */
    '''
    ofp_bad_action_code = enum('ofp_bad_action_code', globals(),
        OFPBAC_BAD_TYPE           = 0, # /* Unknown or unsupported action type. */
        OFPBAC_BAD_LEN            = 1, # /* Length problem in actions. */
        OFPBAC_BAD_EXPERIMENTER   = 2, # /* Unknown experimenter id specified. */
        OFPBAC_BAD_EXP_TYPE       = 3, # /* Unknown action for experimenter id. */
        OFPBAC_BAD_OUT_PORT       = 4, # /* Problem validating output port. */
        OFPBAC_BAD_ARGUMENT       = 5, # /* Bad action argument. */
        OFPBAC_EPERM              = 6, # /* Permissions error. */
        OFPBAC_TOO_MANY           = 7, # /* Can't handle this many actions. */
        OFPBAC_BAD_QUEUE          = 8, # /* Problem validating output queue. */
        OFPBAC_BAD_OUT_GROUP      = 9, # /* Invalid group id in forward action. */
        OFPBAC_MATCH_INCONSISTENT = 10,# /* Action can't apply for this match,
    #                                       or Set-Field missing prerequisite. */
        OFPBAC_UNSUPPORTED_ORDER  = 11,# /* Action order is unsupported for the
    #                                 action list in an Apply-Actions instruction */
        OFPBAC_BAD_TAG            = 12,# /* Actions uses an unsupported
                                       #    tag/encap. */
        OFPBAC_BAD_SET_TYPE       = 13,# /* Unsupported type in SET_FIELD action. */
        OFPBAC_BAD_SET_LEN        = 14,# /* Length problem in SET_FIELD action. */
        OFPBAC_BAD_SET_ARGUMENT   = 15,# /* Bad argument in SET_FIELD action. */
    )
    
    ofp_error_types = {OFPET_HELLO_FAILED: ofp_error_typedef(OFPET_HELLO_FAILED, ofp_hello_failed_code),
                       OFPET_BAD_REQUEST: ofp_error_typedef(OFPET_BAD_REQUEST, ofp_bad_request_code),
                       OFPET_BAD_ACTION: ofp_error_typedef(OFPET_BAD_ACTION, ofp_bad_action_code),
                       NXET_VENDOR: ofp_error_typedef(NXET_VENDOR, nx_vendor_code)
                       }
    
    ofp_config_flags = enum('ofp_config_flags',
                            globals(),
                            uint16,
                            True,
                            OFPC_FRAG_NORMAL = 0,
                            OFPC_FRAG_DROP = 1,
                            OFPC_FRAG_REASM = 2,
                            OFPC_FRAG_NX_MATCH = 3,
                            OFPC_INVALID_TTL_TO_CONTROLLER = 1 << 2)
    
    OFPC_FRAG_MASK = 3,
    
    
    ofp_port_config = enum('ofp_port_config',
                           globals(),
                           uint32,
                           True,
                           OFPPC_PORT_DOWN = 1<<0,
                           OFPPC_NO_STP = 1<<1,
                           OFPPC_NO_RECV = 1<<2,
                           OFPPC_NO_RECV_STP = 1<<3,
                           OFPPC_NO_FLOOD = 1<<4,
                           OFPPC_NO_FWD = 1<<5,
                           OFPPC_NO_PACKET_IN = 1<<6)
    
    ofp_port_state = enum('ofp_port_state',
                          globals(),
                          uint32,
                          True,
                          OFPPS_LINK_DOWN = 1<<0)
    
    ofp_port_features = enum('ofp_port_features',
                             globals(),
                             uint32,
                             True,
                             OFPPF_10MB_HD = 1<<0,
                             OFPPF_10MB_FD = 1<<1,
                             OFPPF_100MB_HD = 1<<2,
                             OFPPF_100MB_FD = 1<<3,
                             OFPPF_1GB_HD = 1<<4,
                             OFPPF_1GB_FD = 1<<5,
                             OFPPF_10GB_FD = 1<<6)
    
    ofp_queue_properties = enum('ofp_queue_properties',
                                globals(),
                                uint16,
                                OFPQT_MIN_RATE = 1,
                                OFPQT_MAX_RATE = 2,
                                OFPQT_EXPERIMENTER = 0xffff)
    
    
    ofp_capabilities = enum('ofp_capabilities',
                            globals(),
                            uint32,
                            True,
                            OFPC_FLOW_STATS = 1<<0,
                            OFPC_TABLE_STATS = 1<<1,
                            OFPC_PORT_STATS = 1<<2,
                            OFPC_IP_REASM = 1<<5,
                            OFPC_QUEUE_STATS = 1<<6,
                            OFPC_ARP_MATCH_IP = 1<<7)
    
    ofp_packet_in_reason = enum('ofp_packet_in_reason',
                                globals(),
                                uint8,
                                OFPR_NO_MATCH = 0,
                                OFPR_ACTION = 1,
                                OFPR_INVALID_TTL = 2
                                )
    OFPR_N_REASONS = 3
    
    ofp_flow_mod_command = enum('ofp_flow_mod_command',
                                globals(),
                                uint16,
                                OFPFC_ADD = 0,
                                OFPFC_MODIFY = 1,
                                OFPFC_MODIFY_STRICT = 2,
                                OFPFC_DELETE = 3,
                                OFPFC_DELETE_STRICT = 4)
    
    ofp_flow_mod_flags = enum('ofp_flow_mod_flags',
                              globals(),
                              uint16,
                              True,
                              OFPFF_SEND_FLOW_REM = 1 << 0,
                              OFPFF_CHECK_OVERLAP = 1 << 1                          
                              )
    
    ofp_flow_removed_reason = enum('ofp_flow_removed_reason',
                                   globals(),
                                   uint8,
                                   OFPRR_IDLE_TIMEOUT = 0,
                                   OFPRR_HARD_TIMEOUT = 1,
                                   OFPRR_DELETE = 2,
                                   OFPRR_GROUP_DELETE = 3,
                                   OFPRR_METER_DELETE = 4,
                                   OFPRR_EVICTION = 5)
    
    ofp_port_reason = enum('ofp_port_reason',
                           globals(),
                           uint8,
                           OFPPR_ADD = 0,
                           OFPPR_DELETE = 1,
                           OFPPR_MODIFY = 2)
    
    ofp_stats_reply_flags = enum('ofp_stats_reply_flags',
                                 globals(),
                                 uint16,
                                 OFPSF_REPLY_MORE = 1<<0)
    
    
    ofp_aggregate_stats_reply = nstruct((uint64, 'packet_count'),
                                        (uint64, 'byte_count'),
                                        (uint32, 'flow_count'),
                                        (uint8[4],),
                                        name = 'ofp_aggregate_stats_reply')
    
    ofp_match_type = enum('ofp_match_type',
                          globals(),
                          uint16,
                          OFPMT_STANDARD = 0,
                          OFPMT_OXM = 1)
    
    ofp_group = enum('ofp_group',
                     globals(),
                     uint32,
                     OFPG_MAX = 0xffffff00,
                     OFPG_ALL = 0xfffffffc,
                     OFPG_ANY = 0xffffffff)
    
    ofp_group_capabilities = enum('ofp_group_capabilities',
                                  globals(),
                                  OFPGFC_SELECT_WEIGHT = 1 << 0,
                                  OFPGFC_SELECT_LIVENESS = 1 << 1,
                                  OFPGFC_CHAINING = 1 << 2,
                                  OFPGFC_CHAINING_CHECKS = 1 << 3)
    
    ofp_hello_elem_type = enum('ofp_hello_elem_type',
                               globals(),
                               uint16,
                               OFPHET_VERSIONBITMAP = 1) 
    
    ofp_hello_elem = nstruct((ofp_hello_elem_type, 'type'),
                             (uint16, 'length'),
                             name = 'ofp_hello_elem',
                             size = sizefromlen(32, 'length'),
                             prepack = packrealsize('length'),
                             padding = 8
                             )
    
    ofp_versionbitmap = uint32[0]
    
    ofp_versionbitmap.formatter = lambda x: ofp_version_bitwise.tostr(sum(x[i] << (i * 32) for i in range(0, len(x))))
    
    ofp_hello_elem_versionbitmap = nstruct(
                            (ofp_versionbitmap, 'bitmaps'),
                            name = 'ofp_hello_elem_versionbitmap',
                            base = ofp_hello_elem,
                            criteria = lambda x: x.type == OFPHET_VERSIONBITMAP,
                            init = packvalue(OFPHET_VERSIONBITMAP, 'type')
                                           )
    
    ofp_hello = nstruct(
                    (ofp_hello_elem[0], 'elements'),
                    name = 'ofp_hello',
                    base = ofp_msg,
                    criteria = lambda x: x.header.type == OFPT_HELLO,
                    init = packvalue(OFPT_HELLO, 'header', 'type')
                        )
    
    ofp_echo = nstruct(
                       (raw, 'data'),
                       name = 'ofp_echo',
                       base = ofp_msg,
                       criteria = lambda x: x.header.type == OFPT_ECHO_REQUEST or x.header.type == OFPT_ECHO_REPLY,
                       init = packvalue(OFPT_ECHO_REQUEST, 'header', 'type')
                       )
    
    
    
    ofp_table = enum('ofp_table',
                     globals(),
                     uint8,
                     OFPTT_ALL = 0xff)
    
    OFPTT_MAX = 0xfe
    
    
    ofp_table_config = enum('ofp_table_config',
                            globals(),
                            uint32,
                            True,
                            OFPTC_TABLE_MISS_CONTROLLER = 0 << 0, #/* Send to controller. */
                            OFPTC_TABLE_MISS_CONTINUE   = 1 << 0, #/* Go to next table, like OF1.0. */
                            OFPTC_TABLE_MISS_DROP       = 2 << 0, #/* Drop the packet. */
                            OFPTC_TABLE_MISS_MASK       = 3 << 0,
                        
                            #/* OpenFlow 1.4. */
                            OFPTC_EVICTION              = 1 << 2, #/* Allow table to evict flows. */
                            OFPTC_VACANCY_EVENTS        = 1 << 3) #/* Enable vacancy events. */

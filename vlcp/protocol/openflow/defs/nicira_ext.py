'''
Created on 2015/8/3

:author: hubo
'''
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
'''
from . import common
from namedstruct import *
from vlcp.utils.ethernet import mac_addr, ip4_addr, ethertype, ip4_addr_bytes, mac_addr_bytes, \
                                ip6_addr, ip6_addr_bytes, vxlan_group_policy_flags, \
                                nsh_md_types, nsh_md_class
import vlcp.utils.ethernet as _ethernet
from namedstruct.namedstruct import StructDefWarning
import warnings as _warnings

with _warnings.catch_warnings():
    _warnings.filterwarnings('ignore', '^padding', StructDefWarning)
    
    '''
    /* The following vendor extensions, proposed by Nicira, are not yet
     * standardized, so they are not included in openflow.h.  Some of them may be
     * suitable for standardization; others we never expect to standardize. */
    '''
    '''
    /* This is a random number to avoid accidental collision with any other
     * vendor's extension. */
    '''
    
    nxt_subtype = enum('nxt_subtype', globals(), uint32,
        NXT_ROLE_REQUEST = 10,
        NXT_ROLE_REPLY = 11,
        NXT_SET_FLOW_FORMAT = 12,
        NXT_FLOW_MOD = 13,
        NXT_FLOW_REMOVED = 14,
        NXT_FLOW_MOD_TABLE_ID = 15,
        NXT_SET_PACKET_IN_FORMAT = 16,
        NXT_PACKET_IN = 17,
        NXT_FLOW_AGE = 18,
        NXT_SET_ASYNC_CONFIG = 19,
        NXT_SET_CONTROLLER_ID = 20,
        NXT_FLOW_MONITOR_CANCEL = 21,
        NXT_FLOW_MONITOR_PAUSED = 22,
        NXT_FLOW_MONITOR_RESUMED = 23,
        NXT_TLV_TABLE_MOD = 24,
        NXT_TLV_TABLE_REQUEST = 25,
        NXT_TLV_TABLE_REPLY = 26,
        NXT_SET_ASYNC_CONFIG2 = 27,
        NXT_RESUME = 28,
        NXT_CT_FLUSH_ZONE = 29,
        NXT_PACKET_IN2 = 30,
    )
    
    #===========================================================================
    # /* Nicira vendor-specific error messages extension.
    #  *
    #  * OpenFlow 1.0 has a set of predefined error types (OFPET_*) and codes (which
    #  * are specific to each type).  It does not have any provision for
    #  * vendor-specific error codes, and it does not even provide "generic" error
    #  * codes that can apply to problems not anticipated by the OpenFlow
    #  * specification authors.
    #  *
    #  * This extension attempts to address the problem by adding a generic "error
    #  * vendor extension".  The extension works as follows: use NXET_VENDOR as type
    #  * and NXVC_VENDOR_ERROR as code, followed by struct nx_vendor_error with
    #  * vendor-specific details, followed by at least 64 bytes of the failed
    #  * request.
    #  *
    #  * It would be better to have a type-specific vendor extension, e.g. so that
    #  * OFPET_BAD_ACTION could be used with vendor-specific code values.  But
    #  * OFPET_BAD_ACTION and most other standardized types already specify that
    #  * their 'data' values are (the start of) the OpenFlow message being replied
    #  * to, so there is no room to insert a vendor ID.
    #  *
    #  * Currently this extension is only implemented by Open vSwitch, but it seems
    #  * like a reasonable candidate for future standardization.
    #  */
    #===========================================================================
    nx10_error_type = \
        enum(
            'nx10_error_type',
            None,
            uint16,
            OFPET_HELLO_FAILED         = 0,  #/* Hello protocol failed. */
            OFPET_BAD_REQUEST          = 1,  #/* Request was not understood. */
            OFPET_BAD_ACTION           = 2,  #/* Error in action description. */
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
            OFPET_BAD_PROPERTY         = 14, # /* Some property is invalid. */
            OFPET_ASYNC_CONFIG_FAILED  = 15, # /* Asynchronous config request failed. */
            OFPET_FLOW_MONITOR_FAILED  = 16, # /* Setting flow monitor failed. */
            OFPET_BUNDLE_FAILED        = 17, # /* Bundle operation failed. */            
        )
    
    # type = 0
    nx10_error_type0_code = \
        enum(
            'nx10_error_type0_code',
            None,
            uint16,
            OFPBMC_BAD_FIELD = 263
        )
    
    # type = 1
    nx10_error_bad_request_code = \
        enum(
            'nx10_error_type',
            None,
            uint16,
            NXBRC_NXM_INVALID = 256,
            NXBRC_NXM_BAD_TYPE = 257,
            OFPBMC_BAD_VALUE = 258,
            OFPBMC_BAD_MASK = 259,
            OFPBMC_BAD_PREREQ = 260,
            OFPBMC_DUP_FIELD = 261,
            OFPBMC_BAD_WILDCARDS = 262,
            NXBMC_CT_DATAPATH_SUPPORT = 264,
            OFPBRC_BAD_TABLE_ID = 512,
            OFPRRFC_BAD_ROLE = 513,
            OFPBRC_BAD_PORT = 514,
            NXBRC_MUST_BE_ZERO = 515,
            NXBRC_BAD_REASON = 516,
            OFPMOFC_MONITOR_EXISTS = 517,
            OFPMOFC_BAD_FLAGS = 518,
            OFPMOFC_UNKNOWN_MONITOR = 519,
            NXBRC_FM_BAD_EVENT = 520,
            NXBRC_UNENCODABLE_ERROR = 521,
            OFPBAC_MATCH_INCONSISTENT = 522,
            OFPBAC_BAD_SET_TYPE = 523,
            OFPBAC_BAD_SET_LEN = 524,
            OFPBAC_BAD_SET_ARGUMENT = 525,
            NXTTMFC_BAD_COMMAND = 527,
            NXTTMFC_BAD_OPT_LEN = 528,
            NXTTMFC_BAD_FIELD_IDX = 529,
            NXTTMFC_TABLE_FULL = 530,
            NXTTMFC_ALREADY_MAPPED = 531,
            NXTTMFC_DUP_ENTRY = 532,
            NXR_NOT_SUPPORTED = 533,
            NXR_STALE = 534,
            NXST_NOT_CONFIGURED = 535,
            NXFMFC_INVALID_TLV_FIELD = 536,
            NXTTMFC_INVALID_TLV_DEL = 537
        )
    
    # type = 2
    nx10_error_bad_action_code = \
        enum(
            'nx10_error_bad_action_code',
            None,
            uint16,
            NXBAC_MUST_BE_ZERO = 256,
            OFPBIC_UNSUP_INST = 257,
            NXBAC_BAD_CONJUNCTION = 526
        )
    
    # type = 3
    nx10_error_bad_instruction_code = \
        enum(
            'nx10_error_bad_instruction_code',
            None,
            uint16,
            OFPBIC_DUP_INST = 256,
            OFPFMFC_BAD_FLAGS = 258
        )
    
    # type = 5
    nx10_error_flow_mod_failed_code = \
        enum(
            'nx10_error_flow_mod_failed_code',
            None,
            uint16,
            NXFMFC_HARDWARE = 256,
            NXFMFC_BAD_TABLE_ID = 257,
            OFPFMFC_BAD_FLAGS = 258
        )
    
    # type = 13
    nx10_error_table_features_failed_code = \
        enum(
            'nx10_error_table_features_failed_code',
            None,
            uint16,
            OFPBPC_BAD_TYPE = 2,
            OFPBPC_BAD_LEN = 3,
            OFPBPC_BAD_VALUE = 4
        )
    
    # type = 14
    nx10_error_bad_property_code = \
        enum(
            'nx10_error_bad_property_code',
            None,
            uint16,
            OFPBPC_TOO_MANY = 3,
            OFPBPC_DUP_TYPE = 4,
            OFPBPC_BAD_EXPERIMENTER = 5,
            OFPBPC_BAD_EXP_TYPE = 6,
            OFPBPC_BAD_EXP_VALUE = 7,
            OFPBPC_EPERM = 8,
        )
    
    nx_error_type = \
        enum(
            'nx_error_type',
            globals(),
            uint16,
            NXBRC_NXM_INVALID = 2,
            NXBRC_NXM_BAD_TYPE = 3,
            NXBRC_MUST_BE_ZERO = 4,
            NXBRC_BAD_REASON = 5,
            NX_OFPMOFC_MONITOR_EXISTS = 6,
            NX_OFPMOFC_BAD_FLAGS = 7,
            NX_OFPMOFC_UNKNOWN_MONITOR = 8,
            NXBRC_FM_BAD_EVENT = 9,
            NXBRC_UNENCODABLE_ERROR = 10,
            NXBAC_MUST_BE_ZERO = 11,
            NXFMFC_HARDWARE = 12,
            NXFMFC_BAD_TABLE_ID = 13,
            NXBAC_BAD_CONJUNCTION = 15,
            NXTTMFC_BAD_COMMAND = 16,
            NXTTMFC_BAD_OPT_LEN = 17,
            NXTTMFC_BAD_FIELD_IDX = 18,
            NXTTMFC_TABLE_FULL = 19,
            NXTTMFC_ALREADY_MAPPED = 20,
            NXTTMFC_DUP_ENTRY = 21,
            NX_OFPBFC_BAD_VERSION = 22,
            NXQOFC_QUEUE_ERROR = 23,
            NX_OFPBPC_BAD_TYPE = 25,
            NX_OFPBPC_BAD_LEN = 26,
            NX_OFPBPC_BAD_VALUE = 27,
            NX_OFPBPC_TOO_MANY = 28,
            NX_OFPBPC_DUP_TYPE = 29,
            NX_OFPBPC_BAD_EXPERIMENTER = 30,
            NX_OFPBPC_BAD_EXP_TYPE = 31,
            NX_OFPBPC_BAD_EXP_VALUE = 32,
            NX_OFPBPC_EPERM = 33,
            NXR_NOT_SUPPORTED = 34,
            NXR_STALE = 35,
            NXST_NOT_CONFIGURED = 36,
            NXFMFC_INVALID_TLV_FIELD = 37,
            NXTTMFC_INVALID_TLV_DEL = 38,
            NXBAC_BAD_HEADER_TYPE = 39,
            NXBAC_UNKNOWN_ED_PROP = 40,
            NXBAC_BAD_ED_PROP = 41,
            NXBAC_CT_DATAPATH_SUPPORT = 42,
            NXBMC_CT_DATAPATH_SUPPORT = 43,
        )
    
    nx_error_msg = \
        nstruct(
            (raw, 'data'),
            name = 'nx_error_msg',
            base=common.ofp_error_experimenter_msg,
            criteria = lambda x: x.experimenter == common.NX_VENDOR_ID,
            init = packvalue(common.NX_VENDOR_ID, 'experimenter'),
            extend = {'exp_type': nx_error_type}
        )
    
    nx_vendor_error = \
        nstruct(
            (common.nx_vendor_code, 'code'),
            (common.experimenter_ids, 'vendor'),
            (uint16, 'nx_type'),
            (uint16, 'nx_code'),
            name = 'nx_vendor_error',
            base = common.ofp_error_msg_base,
            criteria = lambda x: x.type == common.NXET_VENDOR,
            init = packvalue(common.NXET_VENDOR, 'type')
        )
    
    nx_vendor_error_nx = \
        nstruct(
            name = 'nx_vendor_error_nx',
            base = nx_vendor_error,
            criteria = lambda x: x.vendor == common.NXET_VENDOR,
            init = packvalue(common.NXET_VENDOR, 'vendor'),
            extend = {'nx_type': nx_error_type},
            classifier = lambda x: x.nx_type
        )
    
    nx_vendor_error_types = \
        {k: nstruct(name = 'nx_vendor_error_nx',
                    base = nx_vendor_error_nx,
                    criteria = lambda x: x.nx_type == k,
                    classifyby = (k,),
                    init = packvalue(k, 'nx_type'),
                    extend = {'nx_code' : codeenum}
                   )
         for k, codeenum in ((nx10_error_type.OFPET_HELLO_FAILED, nx10_error_type0_code),
                             (nx10_error_type.OFPET_BAD_REQUEST, nx10_error_bad_request_code),
                             (nx10_error_type.OFPET_BAD_ACTION, nx10_error_bad_action_code),
                             (nx10_error_type.OFPET_BAD_INSTRUCTION, nx10_error_bad_instruction_code),
                             (nx10_error_type.OFPET_FLOW_MOD_FAILED, nx10_error_flow_mod_failed_code),
                             (nx10_error_type.OFPET_TABLE_FEATURES_FAILED, nx10_error_table_features_failed_code),
                             (nx10_error_type.OFPET_BAD_PROPERTY, nx10_error_bad_property_code))}

    
    '''
    /* Fields to use when hashing flows. */
    '''
    nx_hash_fields = enum('nx_hash_fields', globals(), uint16,
    #    /* Ethernet source address (NXM_OF_ETH_SRC) only. */
        NX_HASH_FIELDS_ETH_SRC = 0,
    
        #===========================================================================
        # /* L2 through L4, symmetric across src/dst.  Specifically, each of the
        #  * following fields, if present, is hashed (slashes separate symmetric
        #  * pairs):
        #  *
        #  *  - NXM_OF_ETH_DST / NXM_OF_ETH_SRC
        #  *  - NXM_OF_ETH_TYPE
        #  *  - The VID bits from NXM_OF_VLAN_TCI, ignoring PCP and CFI.
        #  *  - NXM_OF_IP_PROTO
        #  *  - NXM_OF_IP_SRC / NXM_OF_IP_DST
        #  *  - NXM_OF_TCP_SRC / NXM_OF_TCP_DST
        #  */
        #===========================================================================
        NX_HASH_FIELDS_SYMMETRIC_L4 = 1,
        #=======================================================================
        # /* L3+L4 only, including the following fields:
        #  *
        #  *  - NXM_OF_IP_PROTO
        #  *  - NXM_OF_IP_SRC / NXM_OF_IP_DST
        #  *  - NXM_OF_SCTP_SRC / NXM_OF_SCTP_DST
        #  *  - NXM_OF_TCP_SRC / NXM_OF_TCP_DST
        #  */
        #=======================================================================
        NX_HASH_FIELDS_SYMMETRIC_L3L4 = 2,
    
        #=======================================================================
        # /* L3+L4 only with UDP ports, including the following fields:
        #  *
        #  *  - NXM_OF_IP_PROTO
        #  *  - NXM_OF_IP_SRC / NXM_OF_IP_DST
        #  *  - NXM_OF_SCTP_SRC / NXM_OF_SCTP_DST
        #  *  - NXM_OF_TCP_SRC / NXM_OF_TCP_DST
        #  *  - NXM_OF_UDP_SRC / NXM_OF_UDP_DST
        #  */
        #=======================================================================
        NX_HASH_FIELDS_SYMMETRIC_L3L4_UDP = 3,
    
        # /* Network source address (NXM_OF_IP_SRC) only. */
        NX_HASH_FIELDS_NW_SRC = 4,
    
        # /* Network destination address (NXM_OF_IP_DST) only. */
        NX_HASH_FIELDS_NW_DST = 5
    )
    
    nx_packet_in_format = enum('nx_packet_in_format', globals(), uint32,
        NXPIF_STANDARD = 0,         # /* OFPT_PACKET_IN for this OpenFlow version. */
        NXPIF_NXT_PACKET_IN = 1,    # /* NXT_PACKET_IN (since OVS v1.1). */
        NXPIF_NXT_PACKET_IN2 = 2,   # /* NXT_PACKET_IN2 (since OVS v2.6). */
    )

    NXPIF_OPENFLOW10 = NXPIF_STANDARD       # /* Standard OpenFlow 1.0 compatible. */
    NXPIF_NXM = NXPIF_NXT_PACKET_IN         # /* Nicira Extended. */
    
    nx_packet_in2_prop_type = \
        enum(
            'nx_packet_in2_prop_type',
            globals(),
            uint16,
            # /* Packet. */
            NXPINT_PACKET = 0,              # /* Raw packet data. */
            NXPINT_FULL_LEN = 1,            # /* ovs_be32: Full packet len, if truncated. */
            NXPINT_BUFFER_ID = 2,           # /* ovs_be32: Buffer ID, if buffered. */
        
            # /* Information about the flow that triggered the packet-in. */
            NXPINT_TABLE_ID = 3,            # /* uint8_t: Table ID. */
            NXPINT_COOKIE = 4,              # /* ovs_be64: Flow cookie. */
        
            # /* Other. */
            NXPINT_REASON = 5,              # /* uint8_t, one of OFPR_*. */
            NXPINT_METADATA = 6,            # /* NXM or OXM for metadata fields. */
            NXPINT_USERDATA = 7,            # /* From NXAST_CONTROLLER2 userdata. */
            NXPINT_CONTINUATION = 8,        # /* Private data for continuing processing. */
        )

    nx_role = enum('nx_role', globals(), uint32,
        NX_ROLE_OTHER = 0,           #   /* Default role, full access. */
        NX_ROLE_MASTER = 1,          #   /* Full access, at most one. */
        NX_ROLE_SLAVE = 2            #   /* Read-only access. */
    )
    '''
    /* Flexible flow specifications (aka NXM = Nicira Extended Match).
     *
     * OpenFlow 1.0 has "struct ofp10_match" for specifying flow matches.  This
     * structure is fixed-length and hence difficult to extend.  This section
     * describes a more flexible, variable-length flow match, called "nx_match" for
     * short, that is also supported by Open vSwitch.  This section also defines a
     * replacement for each OpenFlow message that includes struct ofp10_match.
     *
     *
     * Format
     * ======
     *
     * An nx_match is a sequence of zero or more "nxm_entry"s, which are
     * type-length-value (TLV) entries, each 5 to 259 (inclusive) bytes long.
     * "nxm_entry"s are not aligned on or padded to any multibyte boundary.  The
     * first 4 bytes of an nxm_entry are its "header", followed by the entry's
     * "body".
     *
     * An nxm_entry's header is interpreted as a 32-bit word in network byte order:
     *
     * |<-------------------- nxm_type ------------------>|
     * |                                                  |
     * |31                              16 15            9| 8 7                0
     * +----------------------------------+---------------+--+------------------+
     * |            nxm_vendor            |   nxm_field   |hm|    nxm_length    |
     * +----------------------------------+---------------+--+------------------+
     *
     * The most-significant 23 bits of the header are collectively "nxm_type".
     * Bits 16...31 are "nxm_vendor", one of the NXM_VENDOR_* values below.  Bits
     * 9...15 are "nxm_field", which is a vendor-specific value.  nxm_type normally
     * designates a protocol header, such as the Ethernet type, but it can also
     * refer to packet metadata, such as the switch port on which a packet arrived.
     *
     * Bit 8 is "nxm_hasmask" (labeled "hm" above for space reasons).  The meaning
     * of this bit is explained later.
     *
     * The least-significant 8 bits are "nxm_length", a positive integer.  The
     * length of the nxm_entry, including the header, is exactly 4 + nxm_length
     * bytes.
     *
     * For a given nxm_vendor, nxm_field, and nxm_hasmask value, nxm_length is a
     * constant.  It is included only to allow software to minimally parse
     * "nxm_entry"s of unknown types.  (Similarly, for a given nxm_vendor,
     * nxm_field, and nxm_length, nxm_hasmask is a constant.)
     *
     *
     * Semantics
     * =========
     *
     * A zero-length nx_match (one with no "nxm_entry"s) matches every packet.
     *
     * An nxm_entry places a constraint on the packets matched by the nx_match:
     *
     *   - If nxm_hasmask is 0, the nxm_entry's body contains a value for the
     *     field, called "nxm_value".  The nx_match matches only packets in which
     *     the field equals nxm_value.
     *
     *   - If nxm_hasmask is 1, then the nxm_entry's body contains a value for the
     *     field (nxm_value), followed by a bitmask of the same length as the
     *     value, called "nxm_mask".  For each 1-bit in position J in nxm_mask, the
     *     nx_match matches only packets for which bit J in the given field's value
     *     matches bit J in nxm_value.  A 0-bit in nxm_mask causes the
     *     corresponding bit in nxm_value is ignored (it should be 0; Open vSwitch
     *     may enforce this someday), as is the corresponding bit in the field's
     *     value.  (The sense of the nxm_mask bits is the opposite of that used by
     *     the "wildcards" member of struct ofp10_match.)
     *
     *     When nxm_hasmask is 1, nxm_length is always even.
     *
     *     An all-zero-bits nxm_mask is equivalent to omitting the nxm_entry
     *     entirely.  An all-one-bits nxm_mask is equivalent to specifying 0 for
     *     nxm_hasmask.
     *
     * When there are multiple "nxm_entry"s, all of the constraints must be met.
     *
     *
     * Mask Restrictions
     * =================
     *
     * Masks may be restricted:
     *
     *   - Some nxm_types may not support masked wildcards, that is, nxm_hasmask
     *     must always be 0 when these fields are specified.  For example, the
     *     field that identifies the port on which a packet was received may not be
     *     masked.
     *
     *   - Some nxm_types that do support masked wildcards may only support certain
     *     nxm_mask patterns.  For example, fields that have IPv4 address values
     *     may be restricted to CIDR masks.
     *
     * These restrictions should be noted in specifications for individual fields.
     * A switch may accept an nxm_hasmask or nxm_mask value that the specification
     * disallows, if the switch correctly implements support for that nxm_hasmask
     * or nxm_mask value.  A switch must reject an attempt to set up a flow that
     * contains a nxm_hasmask or nxm_mask value that it does not support.
     *
     *
     * Prerequisite Restrictions
     * =========================
     *
     * The presence of an nxm_entry with a given nxm_type may be restricted based
     * on the presence of or values of other "nxm_entry"s.  For example:
     *
     *   - An nxm_entry for nxm_type=NXM_OF_IP_TOS is allowed only if it is
     *     preceded by another entry with nxm_type=NXM_OF_ETH_TYPE, nxm_hasmask=0,
     *     and nxm_value=0x0800.  That is, matching on the IP source address is
     *     allowed only if the Ethernet type is explicitly set to IP.
     *
     *   - An nxm_entry for nxm_type=NXM_OF_TCP_SRC is allowed only if it is
     *     preceded by an entry with nxm_type=NXM_OF_ETH_TYPE, nxm_hasmask=0, and
     *     nxm_value either 0x0800 or 0x86dd, and another with
     *     nxm_type=NXM_OF_IP_PROTO, nxm_hasmask=0, nxm_value=6, in that order.
     *     That is, matching on the TCP source port is allowed only if the Ethernet
     *     type is IP or IPv6 and the IP protocol is TCP.
     *
     * These restrictions should be noted in specifications for individual fields.
     * A switch may implement relaxed versions of these restrictions.  A switch
     * must reject an attempt to set up a flow that violates its restrictions.
     *
     *
     * Ordering Restrictions
     * =====================
     *
     * An nxm_entry that has prerequisite restrictions must appear after the
     * "nxm_entry"s for its prerequisites.  Ordering of "nxm_entry"s within an
     * nx_match is not otherwise constrained.
     *
     * Any given nxm_type may appear in an nx_match at most once.
     *
     *
     * nxm_entry Examples
     * ==================
     *
     * These examples show the format of a single nxm_entry with particular
     * nxm_hasmask and nxm_length values.  The diagrams are labeled with field
     * numbers and byte indexes.
     *
     *
     * 8-bit nxm_value, nxm_hasmask=1, nxm_length=2:
     *
     *  0          3  4   5
     * +------------+---+---+
     * |   header   | v | m |
     * +------------+---+---+
     *
     *
     * 16-bit nxm_value, nxm_hasmask=0, nxm_length=2:
     *
     *  0          3 4    5
     * +------------+------+
     * |   header   | value|
     * +------------+------+
     *
     *
     * 32-bit nxm_value, nxm_hasmask=0, nxm_length=4:
     *
     *  0          3 4           7
     * +------------+-------------+
     * |   header   |  nxm_value  |
     * +------------+-------------+
     *
     *
     * 48-bit nxm_value, nxm_hasmask=0, nxm_length=6:
     *
     *  0          3 4                9
     * +------------+------------------+
     * |   header   |     nxm_value    |
     * +------------+------------------+
     *
     *
     * 48-bit nxm_value, nxm_hasmask=1, nxm_length=12:
     *
     *  0          3 4                9 10              15
     * +------------+------------------+------------------+
     * |   header   |     nxm_value    |      nxm_mask    |
     * +------------+------------------+------------------+
     *
     *
     * Error Reporting
     * ===============
     *
     * A switch should report an error in an nx_match using error type
     * OFPET_BAD_REQUEST and one of the NXBRC_NXM_* codes.  Ideally the switch
     * should report a specific error code, if one is assigned for the particular
     * problem, but NXBRC_NXM_INVALID is also available to report a generic
     * nx_match error.
     */
    '''
    def NXM_HEADER__(VENDOR, FIELD, HASMASK, LENGTH):
        return (((VENDOR) << 16) | ((FIELD) << 9) | ((HASMASK) << 8) | (LENGTH))
    def NXM_HEADER(VENDOR, FIELD, LENGTH):
        return NXM_HEADER__(VENDOR, FIELD, 0, LENGTH)
    def NXM_HEADER_W(VENDOR, FIELD, LENGTH):
        return NXM_HEADER__(VENDOR, FIELD, 1, (LENGTH) * 2)
    def NXM_VENDOR(HEADER):
        return ((HEADER) >> 16)
    def NXM_FIELD(HEADER):
        return (((HEADER) >> 9) & 0x7f)
    def NXM_TYPE(HEADER):
        return (((HEADER) >> 9) & 0x7fffff)
    def NXM_HASMASK(HEADER):
        return (((HEADER) >> 8) & 1)
    def NXM_LENGTH(HEADER):
        return ((HEADER) & 0xff)
    
    def NXM_MAKE_WILD_HEADER(HEADER):
        return NXM_HEADER_W(NXM_VENDOR(HEADER), NXM_FIELD(HEADER), NXM_LENGTH(HEADER))
    
    nx_ip_frag = enum('nx_ip_frag', globals(), uint8, True,
    NX_IP_FRAG_ANY   = (1 << 0), # /* Is this a fragment? */
    NX_IP_FRAG_LATER = (1 << 1), # /* Is this a fragment with nonzero offset? */
    )
    
    NXM_NX_MAX_REGS = 16
    
    nxm_header = enum('nxm_header', globals(), uint32,
    #===============================================================================
    # /* ## ------------------------------- ## */
    # /* ## OpenFlow 1.0-compatible fields. ## */
    # /* ## ------------------------------- ## */
    # 
    # /* Physical or virtual port on which the packet was received.
    #  *
    #  * Prereqs: None.
    #  *
    #  * Format: 16-bit integer in network byte order.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    
    NXM_OF_IN_PORT =  NXM_HEADER  (0x0000,  0, 2),
    
    #===============================================================================
    # /* Source or destination address in Ethernet header.
    #  *
    #  * Prereqs: None.
    #  *
    #  * Format: 48-bit Ethernet MAC address.
    #  *
    #  * Masking: Fully maskable, in versions 1.8 and later. Earlier versions only
    #  *   supported the following masks for NXM_OF_ETH_DST_W: 00:00:00:00:00:00,
    #  *   fe:ff:ff:ff:ff:ff, 01:00:00:00:00:00, ff:ff:ff:ff:ff:ff. */
    #===============================================================================
    NXM_OF_ETH_DST = NXM_HEADER  (0x0000,  1, 6),
    NXM_OF_ETH_DST_W = NXM_HEADER_W(0x0000,  1, 6),
    NXM_OF_ETH_SRC = NXM_HEADER  (0x0000,  2, 6),
    NXM_OF_ETH_SRC_W = NXM_HEADER_W(0x0000,  2, 6),
    
    #===============================================================================
    # /* Packet's Ethernet type.
    #  *
    #  * For an Ethernet II packet this is taken from the Ethernet header.  For an
    #  * 802.2 LLC+SNAP header with OUI 00-00-00 this is taken from the SNAP header.
    #  * A packet that has neither format has value 0x05ff
    #  * (OFP_DL_TYPE_NOT_ETH_TYPE).
    #  *
    #  * For a packet with an 802.1Q header, this is the type of the encapsulated
    #  * frame.
    #  *
    #  * Prereqs: None.
    #  *
    #  * Format: 16-bit integer in network byte order.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    NXM_OF_ETH_TYPE = NXM_HEADER  (0x0000,  3, 2),
    
    #===============================================================================
    # /* 802.1Q TCI.
    #  *
    #  * For a packet with an 802.1Q header, this is the Tag Control Information
    #  * (TCI) field, with the CFI bit forced to 1.  For a packet with no 802.1Q
    #  * header, this has value 0.
    #  *
    #  * Prereqs: None.
    #  *
    #  * Format: 16-bit integer in network byte order.
    #  *
    #  * Masking: Arbitrary masks.
    #  *
    #  * This field can be used in various ways:
    #  *
    #  *   - If it is not constrained at all, the nx_match matches packets without
    #  *     an 802.1Q header or with an 802.1Q header that has any TCI value.
    #  *
    #  *   - Testing for an exact match with 0 matches only packets without an
    #  *     802.1Q header.
    #  *
    #  *   - Testing for an exact match with a TCI value with CFI=1 matches packets
    #  *     that have an 802.1Q header with a specified VID and PCP.
    #  *
    #  *   - Testing for an exact match with a nonzero TCI value with CFI=0 does
    #  *     not make sense.  The switch may reject this combination.
    #  *
    #  *   - Testing with a specific VID and CFI=1, with nxm_mask=0x1fff, matches
    #  *     packets that have an 802.1Q header with that VID (and any PCP).
    #  *
    #  *   - Testing with a specific PCP and CFI=1, with nxm_mask=0xf000, matches
    #  *     packets that have an 802.1Q header with that PCP (and any VID).
    #  *
    #  *   - Testing with nxm_value=0, nxm_mask=0x0fff matches packets with no 802.1Q
    #  *     header or with an 802.1Q header with a VID of 0.
    #  *
    #  *   - Testing with nxm_value=0, nxm_mask=0xe000 matches packets with no 802.1Q
    #  *     header or with an 802.1Q header with a PCP of 0.
    #  *
    #  *   - Testing with nxm_value=0, nxm_mask=0xefff matches packets with no 802.1Q
    #  *     header or with an 802.1Q header with both VID and PCP of 0.
    #  */
    #===============================================================================
    NXM_OF_VLAN_TCI = NXM_HEADER  (0x0000,  4, 2),
    NXM_OF_VLAN_TCI_W = NXM_HEADER_W(0x0000,  4, 2),
    
    #===============================================================================
    # /* The "type of service" byte of the IP header, with the ECN bits forced to 0.
    #  *
    #  * Prereqs: NXM_OF_ETH_TYPE must be either 0x0800 or 0x86dd.
    #  *
    #  * Format: 8-bit integer with 2 least-significant bits forced to 0.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    NXM_OF_IP_TOS = NXM_HEADER  (0x0000,  5, 1),
    
    #===============================================================================
    # /* The "protocol" byte in the IP header.
    #  *
    #  * Prereqs: NXM_OF_ETH_TYPE must be either 0x0800 or 0x86dd.
    #  *
    #  * Format: 8-bit integer.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    NXM_OF_IP_PROTO = NXM_HEADER  (0x0000,  6, 1),
    
    #===============================================================================
    # /* The source or destination address in the IP header.
    #  *
    #  * Prereqs: NXM_OF_ETH_TYPE must match 0x0800 exactly.
    #  *
    #  * Format: 32-bit integer in network byte order.
    #  *
    #  * Masking: Fully maskable, in Open vSwitch 1.8 and later.  In earlier
    #  *   versions, only CIDR masks are allowed, that is, masks that consist of N
    #  *   high-order bits set to 1 and the other 32-N bits set to 0. */
    #===============================================================================
    NXM_OF_IP_SRC = NXM_HEADER  (0x0000,  7, 4),
    NXM_OF_IP_SRC_W = NXM_HEADER_W(0x0000,  7, 4),
    NXM_OF_IP_DST = NXM_HEADER  (0x0000,  8, 4),
    NXM_OF_IP_DST_W = NXM_HEADER_W(0x0000,  8, 4),
    
    #===============================================================================
    # /* The source or destination port in the TCP header.
    #  *
    #  * Prereqs:
    #  *   NXM_OF_ETH_TYPE must be either 0x0800 or 0x86dd.
    #  *   NXM_OF_IP_PROTO must match 6 exactly.
    #  *
    #  * Format: 16-bit integer in network byte order.
    #  *
    #  * Masking: Fully maskable, in Open vSwitch 1.6 and later.  Not maskable, in
    #  *   earlier versions. */
    #===============================================================================
    NXM_OF_TCP_SRC = NXM_HEADER  (0x0000,  9, 2),
    NXM_OF_TCP_SRC_W = NXM_HEADER_W(0x0000,  9, 2),
    NXM_OF_TCP_DST = NXM_HEADER  (0x0000, 10, 2),
    NXM_OF_TCP_DST_W = NXM_HEADER_W(0x0000, 10, 2),
    
    #===============================================================================
    # /* The source or destination port in the UDP header.
    #  *
    #  * Prereqs:
    #  *   NXM_OF_ETH_TYPE must match either 0x0800 or 0x86dd.
    #  *   NXM_OF_IP_PROTO must match 17 exactly.
    #  *
    #  * Format: 16-bit integer in network byte order.
    #  *
    #  * Masking: Fully maskable, in Open vSwitch 1.6 and later.  Not maskable, in
    #  *   earlier versions. */
    #===============================================================================
    NXM_OF_UDP_SRC = NXM_HEADER  (0x0000, 11, 2),
    NXM_OF_UDP_SRC_W = NXM_HEADER_W(0x0000, 11, 2),
    NXM_OF_UDP_DST = NXM_HEADER  (0x0000, 12, 2),
    NXM_OF_UDP_DST_W = NXM_HEADER_W(0x0000, 12, 2),
    
    #===============================================================================
    # /* The type or code in the ICMP header.
    #  *
    #  * Prereqs:
    #  *   NXM_OF_ETH_TYPE must match 0x0800 exactly.
    #  *   NXM_OF_IP_PROTO must match 1 exactly.
    #  *
    #  * Format: 8-bit integer.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    NXM_OF_ICMP_TYPE = NXM_HEADER  (0x0000, 13, 1),
    NXM_OF_ICMP_CODE = NXM_HEADER  (0x0000, 14, 1),
    
    #===============================================================================
    # /* ARP opcode.
    #  *
    #  * For an Ethernet+IP ARP packet, the opcode in the ARP header.  Always 0
    #  * otherwise.  Only ARP opcodes between 1 and 255 should be specified for
    #  * matching.
    #  *
    #  * Prereqs: NXM_OF_ETH_TYPE must match either 0x0806 or 0x8035.
    #  *
    #  * Format: 16-bit integer in network byte order.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    NXM_OF_ARP_OP = NXM_HEADER  (0x0000, 15, 2),
    
    #===============================================================================
    # /* For an Ethernet+IP ARP packet, the source or target protocol address
    #  * in the ARP header.  Always 0 otherwise.
    #  *
    #  * Prereqs: NXM_OF_ETH_TYPE must match either 0x0806 or 0x8035.
    #  *
    #  * Format: 32-bit integer in network byte order.
    #  *
    #  * Masking: Fully maskable, in Open vSwitch 1.8 and later.  In earlier
    #  *   versions, only CIDR masks are allowed, that is, masks that consist of N
    #  *   high-order bits set to 1 and the other 32-N bits set to 0. */
    #===============================================================================
    NXM_OF_ARP_SPA = NXM_HEADER  (0x0000, 16, 4),
    NXM_OF_ARP_SPA_W = NXM_HEADER_W(0x0000, 16, 4),
    NXM_OF_ARP_TPA = NXM_HEADER  (0x0000, 17, 4),
    NXM_OF_ARP_TPA_W = NXM_HEADER_W(0x0000, 17, 4),
    
    #===============================================================================
    # /* ## ------------------------ ## */
    # /* ## Nicira match extensions. ## */
    # /* ## ------------------------ ## */
    # 
    # /* Metadata registers.
    #  *
    #  * Registers initially have value 0.  Actions allow register values to be
    #  * manipulated.
    #  *
    #  * Prereqs: None.
    #  *
    #  * Format: Array of 32-bit integer registers.  Space is reserved for up to
    #  *   NXM_NX_MAX_REGS registers, but switches may implement fewer.
    #  *
    #  * Masking: Arbitrary masks. */
    #===============================================================================
    **{
        'NXM_NX_REG' + str(i): NXM_HEADER (0x0001, i, 4)
        for i in range(NXM_NX_MAX_REGS)
    },
    **{
        'NXM_NX_REG' + str(i) + '_W': NXM_HEADER_W (0x0001, i, 4)
        for i in range(NXM_NX_MAX_REGS)
    },
    #===============================================================================
    # /* Tunnel ID.
    #  *
    #  * For a packet received via a GRE, VXLAN or LISP tunnel including a (32-bit)
    #  * key, the key is stored in the low 32-bits and the high bits are zeroed.  For
    #  * other packets, the value is 0.
    #  *
    #  * All zero bits, for packets not received via a keyed tunnel.
    #  *
    #  * Prereqs: None.
    #  *
    #  * Format: 64-bit integer in network byte order.
    #  *
    #  * Masking: Arbitrary masks. */
    #===============================================================================
    NXM_NX_TUN_ID = NXM_HEADER  (0x0001, 16, 8),
    NXM_NX_TUN_ID_W = NXM_HEADER_W(0x0001, 16, 8),
    
    #===============================================================================
    # /* For an Ethernet+IP ARP packet, the source or target hardware address
    #  * in the ARP header.  Always 0 otherwise.
    #  *
    #  * Prereqs: NXM_OF_ETH_TYPE must match either 0x0806 or 0x8035.
    #  *
    #  * Format: 48-bit Ethernet MAC address.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    NXM_NX_ARP_SHA = NXM_HEADER  (0x0001, 17, 6),
    NXM_NX_ARP_THA = NXM_HEADER  (0x0001, 18, 6),
    
    #===============================================================================
    # /* The source or destination address in the IPv6 header.
    #  *
    #  * Prereqs: NXM_OF_ETH_TYPE must match 0x86dd exactly.
    #  *
    #  * Format: 128-bit IPv6 address.
    #  *
    #  * Masking: Fully maskable, in Open vSwitch 1.8 and later.  In previous
    #  *   versions, only CIDR masks are allowed, that is, masks that consist of N
    #  *   high-order bits set to 1 and the other 128-N bits set to 0. */
    #===============================================================================
    NXM_NX_IPV6_SRC = NXM_HEADER  (0x0001, 19, 16),
    NXM_NX_IPV6_SRC_W = NXM_HEADER_W(0x0001, 19, 16),
    NXM_NX_IPV6_DST = NXM_HEADER  (0x0001, 20, 16),
    NXM_NX_IPV6_DST_W = NXM_HEADER_W(0x0001, 20, 16),
    
    #===============================================================================
    # /* The type or code in the ICMPv6 header.
    #  *
    #  * Prereqs:
    #  *   NXM_OF_ETH_TYPE must match 0x86dd exactly.
    #  *   NXM_OF_IP_PROTO must match 58 exactly.
    #  *
    #  * Format: 8-bit integer.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    NXM_NX_ICMPV6_TYPE = NXM_HEADER  (0x0001, 21, 1),
    NXM_NX_ICMPV6_CODE = NXM_HEADER  (0x0001, 22, 1),
    
    #===============================================================================
    # /* The target address in an IPv6 Neighbor Discovery message.
    #  *
    #  * Prereqs:
    #  *   NXM_OF_ETH_TYPE must match 0x86dd exactly.
    #  *   NXM_OF_IP_PROTO must match 58 exactly.
    #  *   NXM_OF_ICMPV6_TYPE must be either 135 or 136.
    #  *
    #  * Format: 128-bit IPv6 address.
    #  *
    #  * Masking: Fully maskable, in Open vSwitch 1.8 and later.  In previous
    #  *   versions, only CIDR masks are allowed, that is, masks that consist of N
    #  *   high-order bits set to 1 and the other 128-N bits set to 0. */
    #===============================================================================
    NXM_NX_ND_TARGET = NXM_HEADER    (0x0001, 23, 16),
    NXM_NX_ND_TARGET_W = NXM_HEADER_W  (0x0001, 23, 16),
    
    #===============================================================================
    # /* The source link-layer address option in an IPv6 Neighbor Discovery
    #  * message.
    #  *
    #  * Prereqs:
    #  *   NXM_OF_ETH_TYPE must match 0x86dd exactly.
    #  *   NXM_OF_IP_PROTO must match 58 exactly.
    #  *   NXM_OF_ICMPV6_TYPE must be exactly 135.
    #  *
    #  * Format: 48-bit Ethernet MAC address.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    NXM_NX_ND_SLL = NXM_HEADER  (0x0001, 24, 6),
    
    #===============================================================================
    # /* The target link-layer address option in an IPv6 Neighbor Discovery
    #  * message.
    #  *
    #  * Prereqs:
    #  *   NXM_OF_ETH_TYPE must match 0x86dd exactly.
    #  *   NXM_OF_IP_PROTO must match 58 exactly.
    #  *   NXM_OF_ICMPV6_TYPE must be exactly 136.
    #  *
    #  * Format: 48-bit Ethernet MAC address.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    NXM_NX_ND_TLL = NXM_HEADER  (0x0001, 25, 6),
    
    #===============================================================================
    # /* IP fragment information.
    #  *
    #  * Prereqs:
    #  *   NXM_OF_ETH_TYPE must be either 0x0800 or 0x86dd.
    #  *
    #  * Format: 8-bit value with one of the values 0, 1, or 3, as described below.
    #  *
    #  * Masking: Fully maskable.
    #  *
    #  * This field has three possible values:
    #  *
    #  *   - A packet that is not an IP fragment has value 0.
    #  *
    #  *   - A packet that is an IP fragment with offset 0 (the first fragment) has
    #  *     bit 0 set and thus value 1.
    #  *
    #  *   - A packet that is an IP fragment with nonzero offset has bits 0 and 1 set
    #  *     and thus value 3.
    #  *
    #  * NX_IP_FRAG_ANY and NX_IP_FRAG_LATER are declared to symbolically represent
    #  * the meanings of bits 0 and 1.
    #  *
    #  * The switch may reject matches against values that can never appear.
    #  *
    #  * It is important to understand how this field interacts with the OpenFlow IP
    #  * fragment handling mode:
    #  *
    #  *   - In OFPC_FRAG_DROP mode, the OpenFlow switch drops all IP fragments
    #  *     before they reach the flow table, so every packet that is available for
    #  *     matching will have value 0 in this field.
    #  *
    #  *   - Open vSwitch does not implement OFPC_FRAG_REASM mode, but if it did then
    #  *     IP fragments would be reassembled before they reached the flow table and
    #  *     again every packet available for matching would always have value 0.
    #  *
    #  *   - In OFPC_FRAG_NORMAL mode, all three values are possible, but OpenFlow
    #  *     1.0 says that fragments' transport ports are always 0, even for the
    #  *     first fragment, so this does not provide much extra information.
    #  *
    #  *   - In OFPC_FRAG_NX_MATCH mode, all three values are possible.  For
    #  *     fragments with offset 0, Open vSwitch makes L4 header information
    #  *     available.
    #  */
    #===============================================================================
    NXM_NX_IP_FRAG = NXM_HEADER  (0x0001, 26, 1),
    NXM_NX_IP_FRAG_W = NXM_HEADER_W(0x0001, 26, 1),
    
    #===============================================================================
    # /* Bits in the value of NXM_NX_IP_FRAG. */
    #===============================================================================
    
    #===============================================================================
    # /* The flow label in the IPv6 header.
    #  *
    #  * Prereqs: NXM_OF_ETH_TYPE must match 0x86dd exactly.
    #  *
    #  * Format: 20-bit IPv6 flow label in least-significant bits.
    #  *
    #  * Masking: Fully maskable. */
    #===============================================================================
    NXM_NX_IPV6_LABEL = NXM_HEADER  (0x0001, 27, 4),
    NXM_NX_IPV6_LABEL_W = NXM_HEADER_W(0x0001, 27, 4),
    
    #===============================================================================
    # /* The ECN of the IP header.
    #  *
    #  * Prereqs: NXM_OF_ETH_TYPE must be either 0x0800 or 0x86dd.
    #  *
    #  * Format: ECN in the low-order 2 bits.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    NXM_NX_IP_ECN = NXM_HEADER  (0x0001, 28, 1),
    
    #===============================================================================
    # /* The time-to-live/hop limit of the IP header.
    #  *
    #  * Prereqs: NXM_OF_ETH_TYPE must be either 0x0800 or 0x86dd.
    #  *
    #  * Format: 8-bit integer.
    #  *
    #  * Masking: Not maskable. */
    #===============================================================================
    NXM_NX_IP_TTL = NXM_HEADER  (0x0001, 29, 1),
    
    #===============================================================================
    # /* Flow cookie.
    #  *
    #  * This may be used to gain the OpenFlow 1.1-like ability to restrict
    #  * certain NXM-based Flow Mod and Flow Stats Request messages to flows
    #  * with specific cookies.  See the "nx_flow_mod" and "nx_flow_stats_request"
    #  * structure definitions for more details.  This match is otherwise not
    #  * allowed.
    #  *
    #  * Prereqs: None.
    #  *
    #  * Format: 64-bit integer in network byte order.
    #  *
    #  * Masking: Arbitrary masks. */
    #===============================================================================
    NXM_NX_COOKIE = NXM_HEADER  (0x0001, 30, 8),
    NXM_NX_COOKIE_W = NXM_HEADER_W(0x0001, 30, 8),
    
    #===============================================================================
    # /* The source or destination address in the outer IP header of a tunneled
    #  * packet.
    #  *
    #  * For non-tunneled packets, the value is 0.
    #  *
    #  * Prereqs: None.
    #  *
    #  * Format: 32-bit integer in network byte order.
    #  *
    #  * Masking: Fully maskable. */
    #===============================================================================
    NXM_NX_TUN_IPV4_SRC = NXM_HEADER  (0x0001, 31, 4),
    NXM_NX_TUN_IPV4_SRC_W = NXM_HEADER_W(0x0001, 31, 4),
    NXM_NX_TUN_IPV4_DST = NXM_HEADER  (0x0001, 32, 4),
    NXM_NX_TUN_IPV4_DST_W = NXM_HEADER_W(0x0001, 32, 4),
    
    #===============================================================================
    # /* Metadata marked onto the packet in a system-dependent manner.
    #  *
    #  * The packet mark may be used to carry contextual information
    #  * to other parts of the system outside of Open vSwitch. As a
    #  * result, the semantics depend on system in use.
    #  *
    #  * Prereqs: None.
    #  *
    #  * Format: 32-bit integer in network byte order.
    #  *
    #  * Masking: Fully maskable. */
    #===============================================================================
    NXM_NX_PKT_MARK = NXM_HEADER  (0x0001, 33, 4),
    NXM_NX_PKT_MARK_W = NXM_HEADER_W(0x0001, 33, 4),
    
    #===============================================================================
    # /* The flags in the TCP header.
    # *
    # * Prereqs:
    # *   NXM_OF_ETH_TYPE must be either 0x0800 or 0x86dd.
    # *   NXM_OF_IP_PROTO must match 6 exactly.
    # *
    # * Format: 16-bit integer with 4 most-significant bits forced to 0.
    # *
    # * Masking: Bits 0-11 fully maskable. */
    #===============================================================================
    NXM_NX_TCP_FLAGS = NXM_HEADER  (0x0001, 34, 2),
    NXM_NX_TCP_FLAGS_W = NXM_HEADER_W(0x0001, 34, 2),
    
    #===============================================================================
    # /* Metadata dp_hash.
    #  *
    #  * Internal use only, not programable from controller.
    #  *
    #  * The dp_hash is used to carry the flow hash computed in the
    #  * datapath.
    #  *
    #  * Prereqs: None.
    #  *
    #  * Format: 32-bit integer in network byte order.
    #  *
    #  * Masking: Fully maskable. */
    #===============================================================================
    NXM_NX_DP_HASH = NXM_HEADER  (0x0001, 35, 4),
    NXM_NX_DP_HASH_W = NXM_HEADER_W(0x0001, 35, 4),
    
    #===============================================================================
    # /* Metadata recirc_id.
    #  *
    #  * Internal use only, not programable from controller.
    #  *
    #  * The recirc_id used for recirculation. 0 is reserved
    #  * for initially received packet.
    #  *
    #  * Prereqs: None.
    #  *
    #  * Format: 32-bit integer in network byte order.
    #  *
    #  * Masking: not maskable. */
    #===============================================================================
    NXM_NX_RECIRC_ID = NXM_HEADER  (0x0001, 36, 4),
    #===========================================================================
    # /* "conj_id".
    #  *
    #  * ID for "conjunction" actions.  Please refer to ovs-ofctl(8)
    #  * documentation of "conjunction" for details.
    #  *
    #  * Type: be32.
    #  * Maskable: no.
    #  * Formatting: decimal.
    #  * Prerequisites: none.
    #  * Access: read-only.
    #  * NXM: NXM_NX_CONJ_ID(37) since v2.4.
    #  * OXM: none. */
    #===========================================================================
    NXM_NX_CONJ_ID = NXM_HEADER  (0x0001, 37, 4),
    #===========================================================================
    # /* "tun_ipv6_src".
    #  *
    #  * The IPv6 source address in the outer IP header of a tunneled packet.
    #  *
    #  * For non-tunneled packets, the value is 0.
    #  *
    #  * Type: be128.
    #  * Maskable: bitwise.
    #  * Formatting: IPv6.
    #  * Prerequisites: none.
    #  * Access: read/write.
    #  * NXM: NXM_NX_TUN_IPV6_SRC(109) since v2.5.
    #  * OXM: none.
    #  * Prefix lookup member: tunnel.ipv6_src.
    #  */
    #===========================================================================
    NXM_NX_TUN_IPV6_SRC = NXM_HEADER  (0x0001, 109, 16),
    NXM_NX_TUN_IPV6_SRC_W = NXM_HEADER_W  (0x0001, 109, 16),
    #===========================================================================
    # /* "tun_ipv6_dst".
    #  *
    #  * The IPv6 destination address in the outer IP header of a tunneled
    #  * packet.
    #  *
    #  * For non-tunneled packets, the value is 0.
    #  *
    #  * Type: be128.
    #  * Maskable: bitwise.
    #  * Formatting: IPv6.
    #  * Prerequisites: none.
    #  * Access: read/write.
    #  * NXM: NXM_NX_TUN_IPV6_DST(110) since v2.5.
    #  * OXM: none.
    #  * Prefix lookup member: tunnel.ipv6_dst.
    #  */
    #===========================================================================
    NXM_NX_TUN_IPV6_DST = NXM_HEADER  (0x0001, 110, 16),
    NXM_NX_TUN_IPV6_DST_W = NXM_HEADER_W  (0x0001, 110, 16),
    #===========================================================================
    # /* "tun_flags".
    #  *
    #  * Flags representing aspects of tunnel behavior.
    #  *
    #  * For non-tunneled packets, the value is 0.
    #  *
    #  * Type: be16 (low 1 bits).
    #  * Maskable: bitwise.
    #  * Formatting: tunnel flags.
    #  * Prerequisites: none.
    #  * Access: read/write.
    #  * NXM: NXM_NX_TUN_FLAGS(104) since v2.5.
    #  * OXM: none.
    #  */
    #===========================================================================
    NXM_NX_TUN_FLAGS = NXM_HEADER  (0x0001, 104, 2),
    NXM_NX_TUN_FLAGS_W = NXM_HEADER_W  (0x0001, 104, 2),
    #===========================================================================
    # /* "tun_gbp_id".
    #  *
    #  * VXLAN Group Policy ID
    #  *
    #  * Type: be16.
    #  * Maskable: bitwise.
    #  * Formatting: decimal.
    #  * Prerequisites: none.
    #  * Access: read/write.
    #  * NXM: NXM_NX_TUN_GBP_ID(38) since v2.4.
    #  * OXM: none.
    #  */
    #===========================================================================
    NXM_NX_TUN_GBP_ID = NXM_HEADER  (0x0001, 38, 2),
    NXM_NX_TUN_GBP_ID_W = NXM_HEADER_W  (0x0001, 38, 2),
    #===========================================================================
    # /* "tun_gbp_flags".
    #  *
    #  * VXLAN Group Policy flags
    #  *
    #  * Type: u8.
    #  * Maskable: bitwise.
    #  * Formatting: hexadecimal.
    #  * Prerequisites: none.
    #  * Access: read/write.
    #  * NXM: NXM_NX_TUN_GBP_FLAGS(39) since v2.4.
    #  * OXM: none.
    #  */
    #===========================================================================
    NXM_NX_TUN_GBP_FLAGS = NXM_HEADER (0x0001, 39, 1),
    NXM_NX_TUN_GBP_FLAGS_W = NXM_HEADER_W (0x0001, 39, 1),
    #===========================================================================
    # /* "ct_state".
    #  *
    #  * Connection tracking state.  The field is populated by the NXAST_CT
    #  * action.
    #  *
    #  * Type: be32.
    #  * Maskable: bitwise.
    #  * Formatting: ct state.
    #  * Prerequisites: none.
    #  * Access: read-only.
    #  * NXM: NXM_NX_CT_STATE(105) since v2.5.
    #  * OXM: none.
    #  */
    #===========================================================================
    NXM_NX_CT_STATE = NXM_HEADER (0x0001, 105, 4),
    NXM_NX_CT_STATE_W = NXM_HEADER_W (0x0001, 105, 4),
    #===========================================================================
    # /* "ct_zone".
    #  *
    #  * Connection tracking zone.  The field is populated by the
    #  * NXAST_CT action.
    #  *
    #  * Type: be16.
    #  * Maskable: no.
    #  * Formatting: hexadecimal.
    #  * Prerequisites: none.
    #  * Access: read-only.
    #  * NXM: NXM_NX_CT_ZONE(106) since v2.5.
    #  * OXM: none.
    #  */
    #===========================================================================
    NXM_NX_CT_ZONE = NXM_HEADER (0x0001, 106, 2),
    #===========================================================================
    # /* "ct_mark".
    #  *
    #  * Connection tracking mark.  The mark is carried with the
    #  * connection tracking state.  On Linux this corresponds to the
    #  * nf_conn's "mark" member but the exact implementation is
    #  * platform-dependent.
    #  *
    #  * Writable only from nested actions within the NXAST_CT action.
    #  *
    #  * Type: be32.
    #  * Maskable: bitwise.
    #  * Formatting: hexadecimal.
    #  * Prerequisites: none.
    #  * Access: read/write.
    #  * NXM: NXM_NX_CT_MARK(107) since v2.5.
    #  * OXM: none.
    #  */
    #===========================================================================
    NXM_NX_CT_MARK = NXM_HEADER (0x0001, 107, 4),
    NXM_NX_CT_MARK_W = NXM_HEADER_W (0x0001, 107, 4),
    #===========================================================================
    # /* "ct_label".
    #  *
    #  * Connection tracking label.  The label is carried with the
    #  * connection tracking state.  On Linux this is held in the
    #  * conntrack label extension but the exact implementation is
    #  * platform-dependent.
    #  *
    #  * Writable only from nested actions within the NXAST_CT action.
    #  *
    #  * Type: be128.
    #  * Maskable: bitwise.
    #  * Formatting: hexadecimal.
    #  * Prerequisites: none.
    #  * Access: read/write.
    #  * NXM: NXM_NX_CT_LABEL(108) since v2.5.
    #  * OXM: none.
    #  */
    #===========================================================================
    NXM_NX_CT_LABEL = NXM_HEADER (0x0001, 108, 32),
    NXM_NX_CT_LABEL_W = NXM_HEADER_W (0x0001, 108, 32),
    #===========================================================================
    # /* "ct_nw_proto".
    #  *
    #  * The "protocol" byte in the IPv4 or IPv6 header for the original
    #  * direction conntrack tuple, or of the master conntrack entry, if the
    #  * current connection is a related connection.
    #  *
    #  * The value is initially zero and populated by the CT action.  The value
    #  * remains zero after the CT action only if the packet can not be
    #  * associated with a valid connection, in which case the prerequisites
    #  * for matching this field ("CT") are not met.
    #  *
    #  * Type: u8.
    #  * Maskable: no.
    #  * Formatting: decimal.
    #  * Prerequisites: CT.
    #  * Access: read-only.
    #  * NXM: NXM_NX_CT_NW_PROTO(119) since v2.8.
    #  * OXM: none.
    #  */
    #===========================================================================
    NXM_NX_CT_NW_PROTO = NXM_HEADER (0x0001, 119, 1),
    #===========================================================================
    # /* "ct_nw_src".
    #  *
    #  * IPv4 source address of the original direction tuple of the conntrack
    #  * entry, or of the master conntrack entry, if the current connection is a
    #  * related connection.
    #  *
    #  * The value is populated by the CT action.
    #  *
    #  * Type: be32.
    #  * Maskable: bitwise.
    #  * Formatting: IPv4.
    #  * Prerequisites: CT.
    #  * Access: read-only.
    #  * NXM: NXM_NX_CT_NW_SRC(120) since v2.8.
    #  * OXM: none.
    #  * Prefix lookup member: ct_nw_src.
    #  */
    #===========================================================================
    NXM_NX_CT_NW_SRC = NXM_HEADER (0x0001, 120, 4),
    NXM_NX_CT_NW_SRC_W = NXM_HEADER_W (0x0001, 120, 4),

    #===========================================================================
    # /* "ct_nw_dst".
    #  *
    #  * IPv4 destination address of the original direction tuple of the
    #  * conntrack entry, or of the master conntrack entry, if the current
    #  * connection is a related connection.
    #  *
    #  * The value is populated by the CT action.
    #  *
    #  * Type: be32.
    #  * Maskable: bitwise.
    #  * Formatting: IPv4.
    #  * Prerequisites: CT.
    #  * Access: read-only.
    #  * NXM: NXM_NX_CT_NW_DST(121) since v2.8.
    #  * OXM: none.
    #  * Prefix lookup member: ct_nw_dst.
    #  */
    #===========================================================================
    NXM_NX_CT_NW_DST = NXM_HEADER (0x0001, 121, 4),
    NXM_NX_CT_NW_DST_W = NXM_HEADER_W (0x0001, 121, 4),

    #===========================================================================
    # /* "ct_ipv6_src".
    #  *
    #  * IPv6 source address of the original direction tuple of the conntrack
    #  * entry, or of the master conntrack entry, if the current connection is a
    #  * related connection.
    #  *
    #  * The value is populated by the CT action.
    #  *
    #  * Type: be128.
    #  * Maskable: bitwise.
    #  * Formatting: IPv6.
    #  * Prerequisites: CT.
    #  * Access: read-only.
    #  * NXM: NXM_NX_CT_IPV6_SRC(122) since v2.8.
    #  * OXM: none.
    #  * Prefix lookup member: ct_ipv6_src.
    #  */
    #===========================================================================
    NXM_NX_CT_IPV6_SRC = NXM_HEADER (0x0001, 122, 16),
    NXM_NX_CT_IPV6_SRC_W = NXM_HEADER_W (0x0001, 122, 16),

    #===========================================================================
    # /* "ct_ipv6_dst".
    #  *
    #  * IPv6 destination address of the original direction tuple of the
    #  * conntrack entry, or of the master conntrack entry, if the current
    #  * connection is a related connection.
    #  *
    #  * The value is populated by the CT action.
    #  *
    #  * Type: be128.
    #  * Maskable: bitwise.
    #  * Formatting: IPv6.
    #  * Prerequisites: CT.
    #  * Access: read-only.
    #  * NXM: NXM_NX_CT_IPV6_DST(123) since v2.8.
    #  * OXM: none.
    #  * Prefix lookup member: ct_ipv6_dst.
    #  */
    #===========================================================================
    NXM_NX_CT_IPV6_DST = NXM_HEADER (0x0001, 123, 16),
    NXM_NX_CT_IPV6_DST_W = NXM_HEADER_W (0x0001, 123, 16),

    #===========================================================================
    # /* "ct_tp_src".
    #  *
    #  * Transport layer source port of the original direction tuple of the
    #  * conntrack entry, or of the master conntrack entry, if the current
    #  * connection is a related connection.
    #  *
    #  * The value is populated by the CT action.
    #  *
    #  * Type: be16.
    #  * Maskable: bitwise.
    #  * Formatting: decimal.
    #  * Prerequisites: CT.
    #  * Access: read-only.
    #  * NXM: NXM_NX_CT_TP_SRC(124) since v2.8.
    #  * OXM: none.
    #  */
    #===========================================================================
    NXM_NX_CT_TP_SRC = NXM_HEADER (0x0001, 124, 2),
    NXM_NX_CT_TP_SRC_W = NXM_HEADER_W (0x0001, 124, 2),

    #===========================================================================
    # /* "ct_tp_dst".
    #  *
    #  * Transport layer destination port of the original direction tuple of the
    #  * conntrack entry, or of the master conntrack entry, if the current
    #  * connection is a related connection.
    #  *
    #  * The value is populated by the CT action.
    #  *
    #  * Type: be16.
    #  * Maskable: bitwise.
    #  * Formatting: decimal.
    #  * Prerequisites: CT.
    #  * Access: read-only.
    #  * NXM: NXM_NX_CT_TP_DST(125) since v2.8.
    #  * OXM: none.
    #  */
    #===========================================================================
    NXM_NX_CT_TP_DST = NXM_HEADER (0x0001, 125, 2),
    NXM_NX_CT_TP_DST_W = NXM_HEADER (0x0001, 125, 2),
    #===========================================================================
    # /* "xxreg<N>".
    #  *
    #  * ``extended-extended register".
    #  *
    #  * Type: be128.
    #  * Maskable: bitwise.
    #  * Formatting: hexadecimal.
    #  * Prerequisites: none.
    #  * Access: read/write.
    #  * NXM: NXM_NX_XXREG0(111) since v2.6.              <0>
    #  * NXM: NXM_NX_XXREG1(112) since v2.6.              <1>
    #  * NXM: NXM_NX_XXREG2(113) since v2.6.              <2>
    #  * NXM: NXM_NX_XXREG3(114) since v2.6.              <3>
    #  * NXM: NXM_NX_XXREG4(115) since vX.Y.              <4>
    #  * NXM: NXM_NX_XXREG5(116) since vX.Y.              <5>
    #  * NXM: NXM_NX_XXREG6(117) since vX.Y.              <6>
    #  * NXM: NXM_NX_XXREG7(118) since vX.Y.              <7>
    #  * OXM: none.
    #  */
    #===========================================================================
    **{
        'NXM_NX_XXREG' + str(i): NXM_HEADER(0x0001, 111 + i, 16)
        for i in range(NXM_NX_MAX_REGS // 4)
      },
    **{
        'NXM_NX_XXREG' + str(i) + '_W': NXM_HEADER_W(0x0001, 111 + i, 16)
        for i in range(NXM_NX_MAX_REGS // 4)
      },
    #===========================================================================
    # /* "mpls_ttl".
    #  *
    #  * The outermost MPLS label's time-to-live (TTL) field, or 0 if no MPLS
    #  * labels are present.
    #  *
    #  * Type: u8.
    #  * Maskable: no.
    #  * Formatting: decimal.
    #  * Prerequisites: MPLS.
    #  * Access: read/write.
    #  * NXM: NXM_NX_MPLS_TTL(30) since v2.6.
    #  * OXM: none.
    #  */
    #===========================================================================
    NXM_NX_MPLS_TTL = NXM_HEADER (0x0001, 30, 1)
    )
    
    nx_conntrack_state = \
        enum(
            'nx_conntrack_state',
            globals(),
            uint32,
            True,
            CS_NEW              = 1 << 0,
            CS_ESTABLISHED      = 1 << 1,
            CS_RELATED          = 1 << 2,
            CS_REPLY_DIR        = 1 << 3,
            CS_INVALID          = 1 << 4,
            CS_TRACKED          = 1 << 5,
            CS_SRC_NAT          = 1 << 6,
            CS_DST_NAT          = 1 << 7
        )
    
    nx_tunnel_metadata_fields = \
        enum(
            'nx_tunnel_metadata_fields',
            None,
            uint16,
            **{'NXM_NX_TUN_METADATA' + str(i): 40 + i
               for i in range(64)}
        )
    
    NX_TUN_METADATA_NUM_OPTS = 64
    
    def create_nx_tunnel_metadata_header(index, length, mask=False):
        return (NXM_HEADER_W if mask else NXM_HEADER)(0x0001, 40 + index, length)
    
    def _is_nx_tunnel_metadata(x):
        return 0 <= ((x & 0xffff) >> 9) - nx_tunnel_metadata_fields.NXM_NX_TUN_METADATA0 < NX_TUN_METADATA_NUM_OPTS
    
    _nx_tunnel_metadata_header_type = prim('I', '_nx_tunnel_metadata_header_type')
    
    def _nx_tunnel_metadata_formatter(x):
        index = ((x & 0xffff) >> 9) - nx_tunnel_metadata_fields.NXM_NX_TUN_METADATA0
        mask = x & 0x100
        length = x & 0xff
        return "NXM_NX_TUN_METADATA" + str(index) + ("_W" if mask else "") + "[" + str(length) + "]"
    
    _nx_tunnel_metadata_header_type.formatter = _nx_tunnel_metadata_formatter
    
    nx_tunnel_flags = \
        enum(
            'nx_tunnel_flags',
            globals(),
            uint16,
            True,
            NX_FLOW_TNL_F_OAM = (1 << 0),           
            
            # /* Private flags */
            NX_FLOW_TNL_F_DONT_FRAGMENT = (1 << 1),
            NX_FLOW_TNL_F_CSUM = (1 << 2),
            NX_FLOW_TNL_F_KEY = (1 << 3),
        )
    
    def NXM_NX_REG(IDX):
        return NXM_HEADER  (0x0001, IDX, 4),
    def NXM_NX_REG_W(IDX):
        return NXM_HEADER_W(0x0001, IDX, 4),
    def NXM_NX_REG_IDX(HEADER):
        return NXM_FIELD(HEADER),
    def NXM_IS_NX_REG(HEADER):
        return (not ((((HEADER) ^ NXM_NX_REG0)) & 0xffffe1ff))
    def NXM_IS_NX_REG_W(HEADER):
        return (not ((((HEADER) ^ NXM_NX_REG0_W)) & 0xffffe1ff))
    
    nx_action_subtype = enum('nx_action_subtype', globals(), uint16,
        NXAST_SNAT__OBSOLETE = 0,       # /* No longer used. */
        NXAST_RESUBMIT = 1,             # /* struct nx_action_resubmit */
        NXAST_SET_TUNNEL = 2,           # /* struct nx_action_set_tunnel */
        NXAST_DROP_SPOOFED_ARP__OBSOLETE = 3,
        NXAST_SET_QUEUE = 4,            # /* struct nx_action_set_queue */
        NXAST_POP_QUEUE = 5,            # /* struct nx_action_pop_queue */
        NXAST_REG_MOVE = 6,             # /* struct nx_action_reg_move */
        NXAST_REG_LOAD = 7,             # /* struct nx_action_reg_load */
        NXAST_NOTE = 8,                 # /* struct nx_action_note */
        NXAST_SET_TUNNEL64 = 9,         # /* struct nx_action_set_tunnel64 */
        NXAST_MULTIPATH = 10,            # /* struct nx_action_multipath */
        NXAST_AUTOPATH__OBSOLETE = 11,   # /* No longer used. */
        NXAST_BUNDLE = 12,               # /* struct nx_action_bundle */
        NXAST_BUNDLE_LOAD = 13,          # /* struct nx_action_bundle */
        NXAST_RESUBMIT_TABLE = 14,       # /* struct nx_action_resubmit */
        NXAST_OUTPUT_REG = 15,           # /* struct nx_action_output_reg */
        NXAST_LEARN = 16,                # /* struct nx_action_learn */
        NXAST_EXIT = 17,                 # /* struct nx_action_header */
        NXAST_DEC_TTL = 18,              # /* struct nx_action_header */
        NXAST_FIN_TIMEOUT = 19,          # /* struct nx_action_fin_timeout */
        NXAST_CONTROLLER = 20,           # /* struct nx_action_controller */
        NXAST_DEC_TTL_CNT_IDS = 21,      # /* struct nx_action_cnt_ids */
        NXAST_WRITE_METADATA = 22,       # /* struct nx_action_write_metadata */
        NXAST_PUSH_MPLS = 23,            # /* struct nx_action_push_mpls */
        NXAST_POP_MPLS = 24,             # /* struct nx_action_pop_mpls */
        NXAST_SET_MPLS_TTL = 25,         # /* struct nx_action_ttl */
        NXAST_DEC_MPLS_TTL = 26,         # /* struct nx_action_header */
        NXAST_STACK_PUSH = 27,           # /* struct nx_action_stack */
        NXAST_STACK_POP = 28,            # /* struct nx_action_stack */
        NXAST_SAMPLE = 29,               # /* struct nx_action_sample */
        NXAST_SET_MPLS_LABEL = 30,       # /* struct nx_action_ttl */
        NXAST_SET_MPLS_TC = 31,          # /* struct nx_action_ttl */
        NXAST_OUTPUT_REG2 = 32,          # /* struct nx_action_output_reg2 */
        NXAST_REG_LOAD2 = 33,            # /* struct nx_action_reg_load2 */
        NXAST_CONJUNCTION = 34,          # /* struct nx_action_conjunction */
        NXAST_CT = 35,                   # /* struct nx_action_conntrack */
        NXAST_NAT = 36,                  # /* struct nx_action_nat */
        NXAST_CONTROLLER2 = 37,          # /* struct nx_action_controller2 */
        NXAST_SAMPLE2 = 38,              # /* struct nx_action_sample2 */
        NXAST_OUTPUT_TRUNC = 39,         # /* struct nx_action_output_trunc */
        NXAST_SAMPLE3 = 41,              # /* struct nx_action_sample2 */
        NXAST_CLONE = 42,                # /* struct nx_action_clone */
        NXAST_CT_CLEAR = 43,             # /* struct nx_action_ct_clear */
        NXAST_RESUBMIT_TABLE_CT = 44,    # /* struct nx_action_resubmit */
        NXAST_LEARN2 = 45,               # /* struct nx_action_learn2 */
        NXAST_ENCAP = 46,                # /* struct nx_action_encap */
        NXAST_DECAP = 47,                # /* struct nx_action_decap */
        NXAST_DEC_NSH_TTL = 48,          # /* struct nx_action_dec_nsh_ttl */
    )
    
    nx_stats_subtype = enum('nx_stats_subtype', globals(), uint32,
        NXST_FLOW = 0,
        NXST_AGGREGATE = 1,
        NXST_FLOW_MONITOR = 2,
        NXST_IPFIX_BRIDGE = 3,
        NXST_IPFIX_FLOW = 4
    )
    
    def create_ofs_nbits(ofs, n_bits):
        return (ofs << 6) | (n_bits - 1)
    
    ofs_nbits_type = prim('H', 'ofs_nbits_type')
    ofs_nbits_type.formatter = lambda x: '[%d..%d]' % (x >> 6, (x >> 6) + (x & 0x3f))
    
    '''
    /* NXAST_MULTIPATH: Multipath link choice algorithm to apply.
     *
     * In the descriptions below, 'n_links' is max_link + 1. */
    '''
    nx_mp_algorithm = enum('nx_mp_algorithm', globals(), uint16,
        #===========================================================================
        # /* link = hash(flow) % n_links.
        #  *
        #  * Redistributes all traffic when n_links changes.  O(1) performance.  See
        #  * RFC 2992.
        #  *
        #  * Use UINT16_MAX for max_link to get a raw hash value. */
        #===========================================================================
        NX_MP_ALG_MODULO_N = 0,
    
        #===========================================================================
        # /* link = hash(flow) / (MAX_HASH / n_links).
        #  *
        #  * Redistributes between one-quarter and one-half of traffic when n_links
        #  * changes.  O(1) performance.  See RFC 2992.
        #  */
        #===========================================================================
        NX_MP_ALG_HASH_THRESHOLD = 1,
    
        #===========================================================================
        # /* for i in [0,n_links):
        #  *   weights[i] = hash(flow, i)
        #  * link = { i such that weights[i] >= weights[j] for all j != i }
        #  *
        #  * Redistributes 1/n_links of traffic when n_links changes.  O(n_links)
        #  * performance.  If n_links is greater than a threshold (currently 64, but
        #  * subject to change), Open vSwitch will substitute another algorithm
        #  * automatically.  See RFC 2992. */
        #===========================================================================
        NX_MP_ALG_HRW = 2,            #  /* Highest Random Weight. */
    
        #===========================================================================
        # /* i = 0
        #  * repeat:
        #  *     i = i + 1
        #  *     link = hash(flow, i) % arg
        #  * while link > max_link
        #  *
        #  * Redistributes 1/n_links of traffic when n_links changes.  O(1)
        #  * performance when arg/max_link is bounded by a constant.
        #  *
        #  * Redistributes all traffic when arg changes.
        #  *
        #  * arg must be greater than max_link and for best performance should be no
        #  * more than approximately max_link * 2.  If arg is outside the acceptable
        #  * range, Open vSwitch will automatically substitute the least power of 2
        #  * greater than max_link.
        #  *
        #  * This algorithm is specific to Open vSwitch.
        #  */
        #===========================================================================
        NX_MP_ALG_ITER_HASH = 3        # /* Iterative Hash. */
    )
    
    # Isn't it 0x7ff if it has 11 bits?
    NX_LEARN_N_BITS_MASK = 0x3ff
    NX_LEARN_SRC_MASK = (1 << 13)
    NX_LEARN_DST_MASK = (3 << 11)
    NX_LEARN_SRC_FIELD = (0 << 13) # /* Copy from field. */
    NX_LEARN_DST_MATCH = (0 << 11) # /* Add match criterion. */
    
    nx_flow_mod_spec_header = enum('nx_flow_mod_spec_header', globals(), uint16, True,
        NX_LEARN_SRC_IMMEDIATE = (1 << 13), # /* Copy from immediate value. */
        NX_LEARN_DST_LOAD = (1 << 11), # /* Add NXAST_REG_LOAD action. */
        NX_LEARN_DST_OUTPUT = (2 << 11), # /* Add OFPAT_OUTPUT action. */
        NX_LEARN_DST_RESERVED = (3 << 11) # /* Not yet defined. */
    )
    
    def NX_FLOWMODSPEC_SRC(x):
        return x & NX_LEARN_SRC_MASK
    def NX_FLOWMODSPEC_DST(x):
        return x & NX_LEARN_DST_MASK
    def NX_FLOWMODSPEC_NBITS(x):
        return x & NX_LEARN_N_BITS_MASK
    
    
    def _createdesc(descr):
        def formatter(x):
            x['_desc'] = descr(x)
            return x
        return formatter
       
    '''
    /* NXAST_BUNDLE: Bundle slave choice algorithm to apply.
     *
     * In the descriptions below, 'slaves' is the list of possible slaves in the
     * order they appear in the OpenFlow action. */
    '''
    nx_bd_algorithm = enum('nx_bd_algorithm', globals(), uint16,
        #===========================================================================
        # /* Chooses the first live slave listed in the bundle.
        #  *
        #  * O(n_slaves) performance. */
        #===========================================================================
        NX_BD_ALG_ACTIVE_BACKUP = 0,
    
        #===========================================================================
        # /* for i in [0,n_slaves):
        #  *   weights[i] = hash(flow, i)
        #  * slave = { slaves[i] such that weights[i] >= weights[j] for all j != i }
        #  *
        #  * Redistributes 1/n_slaves of traffic when a slave's liveness changes.
        #  * O(n_slaves) performance.
        #  *
        #  * Uses the 'fields' and 'basis' parameters. */
        #===========================================================================
        NX_BD_ALG_HRW = 1 # /* Highest Random Weight. */
    )
    
    nx_flow_format = enum('nx_flow_format', globals(), uint32,
        NXFF_OPENFLOW10 = 0,      #   /* Standard OpenFlow 1.0 compatible. */
        NXFF_NXM = 2              #   /* Nicira extended match. */
    )
    
    '''
    /* 'flags' bits in struct nx_flow_monitor_request. */
    '''
    nx_flow_monitor_flags = enum('nx_flow_monitor_flags', globals(), uint16, True,
    #    /* When to send updates. */
        NXFMF_INITIAL = 1 << 0,     #/* Initially matching flows. */
        NXFMF_ADD = 1 << 1,         #/* New matching flows as they are added. */
        NXFMF_DELETE = 1 << 2,      #/* Old matching flows as they are removed. */
        NXFMF_MODIFY = 1 << 3,      #/* Matching flows as they are changed. */
    
    #    /* What to include in updates. */
        NXFMF_ACTIONS = 1 << 4,     #/* If set, actions are included. */
        NXFMF_OWN = 1 << 5,         #/* If set, include own changes in full. */
    )
    
    '''
    /* 'event' values in struct nx_flow_update_header. */
    '''
    nx_flow_update_event = enum('nx_flow_update_event', globals(), uint16,
    #    /* struct nx_flow_update_full. */
        NXFME_ADDED = 0,            # /* Flow was added. */
        NXFME_DELETED = 1,          # /* Flow was deleted. */
        NXFME_MODIFIED = 2,         # /* Flow (generally its actions) was changed. */
    
    #    /* struct nx_flow_update_abbrev. */
        NXFME_ABBREV = 3,           # /* Abbreviated reply. */
    )


    #===========================================================================
    # /* Bits for 'flags' in struct nx_action_learn.
    #  *
    #  * If NX_LEARN_F_SEND_FLOW_REM is set, then the learned flows will have their
    #  * OFPFF_SEND_FLOW_REM flag set.
    #  *
    #  * If NX_LEARN_F_WRITE_RESULT is set, then the actions will write whether the
    #  * learn operation succeded on a bit.  If the learn is successful the bit will
    #  * be set, otherwise (e.g. if the limit is hit) the bit will be unset.
    #  *
    #  * If NX_LEARN_F_DELETE_LEARNED is set, then removing this action will delete
    #  * all the flows from the learn action's 'table_id' that have the learn
    #  * action's 'cookie'.  Important points:
    #  *
    #  *     - The deleted flows include those created by this action, those created
    #  *       by other learn actions with the same 'table_id' and 'cookie', those
    #  *       created by flow_mod requests by a controller in the specified table
    #  *       with the specified cookie, and those created through any other
    #  *       means.
    #  *
    #  *     - If multiple flows specify "learn" actions with
    #  *       NX_LEARN_F_DELETE_LEARNED with the same 'table_id' and 'cookie', then
    #  *       no deletion occurs until all of those "learn" actions are deleted.
    #  *
    #  *     - Deleting a flow that contains a learn action is the most obvious way
    #  *       to delete a learn action.  Modifying a flow's actions, or replacing it
    #  *       by a new flow, can also delete a learn action.  Finally, replacing a
    #  *       learn action with NX_LEARN_F_DELETE_LEARNED with a learn action
    #  *       without that flag also effectively deletes the learn action and can
    #  *       trigger flow deletion.
    #  *
    #  * NX_LEARN_F_DELETE_LEARNED was added in Open vSwitch 2.4. */
    #===========================================================================
    nx_learn_flags = \
        enum(
            'nx_learn_flags',
            globals(),
            uint16,
            True,
            NX_LEARN_F_SEND_FLOW_REM = 1 << 0,
            NX_LEARN_F_DELETE_LEARNED = 1 << 1,
            NX_LEARN_F_WRITE_RESULT = 1 << 2,
        )


    #===========================================================================
    # /* Properties for NXAST_CONTROLLER2.
    #  *
    #  * For more information on the effect of NXAC2PT_PAUSE, see the large comment
    #  * on NXT_PACKET_IN2 in nicira-ext.h */
    #===========================================================================
    nx_action_controller2_prop_type = \
        enum(
            'nx_action_controller2_prop_type',
            globals(),
            uint16,
            NXAC2PT_MAX_LEN = 0,            # /* ovs_be16 max bytes to send (default all). */
            NXAC2PT_CONTROLLER_ID = 1,      # /* ovs_be16 dest controller ID (default 0). */
            NXAC2PT_REASON = 2,             # /* uint8_t reason (OFPR_*), default 0. */
            NXAC2PT_USERDATA = 3,           # /* Data to copy into NXPINT_USERDATA. */
            NXAC2PT_PAUSE = 4,              # /* Flag to pause pipeline to resume later. */
        )


    # /* Direction of sampled packets. */
    nx_action_sample_direction = \
        enum(
            'nx_action_sample_direction',
            globals(),
            uint8,
            #===================================================================
            # /* OVS will attempt to infer the sample's direction based on whether
            #  * 'sampling_port' is the packet's output port.  This is generally
            #  * effective except when sampling happens as part of an output to a patch
            #  * port, which doesn't involve a datapath output action. */
            #===================================================================
            NX_ACTION_SAMPLE_DEFAULT = 0,
        
            #===================================================================
            # /* Explicit direction.  This is useful for sampling packets coming in from
            #  * or going out of a patch port, where the direction cannot be inferred. */
            #===================================================================
            NX_ACTION_SAMPLE_INGRESS = 1,
            NX_ACTION_SAMPLE_EGRESS = 2
        )


    #=======================================================================
    # /* Bits for 'flags' in struct nx_action_conntrack.
    #  *
    #  * If NX_CT_F_COMMIT is set, then the connection entry is moved from the
    #  * unconfirmed to confirmed list in the tracker.
    #  * If NX_CT_F_FORCE is set, in addition to NX_CT_F_COMMIT, then the conntrack
    #  * entry is replaced with a new one in case the original direction of the
    #  * existing entry is opposite of the current packet direction.
    #  */
    #=======================================================================
    nx_conntrack_flags = \
        enum(
            'nx_conntrack_flags',
            globals(),
            uint16,
            True,
            NX_CT_F_COMMIT = 1 << 0,
            NX_CT_F_FORCE  = 1 << 1,
        )

    NX_CT_RECIRC_NONE = common.OFPTT_ALL
    
    nx_conntrack_alg_port = \
        enum(
            'nx_conntrack_alg_port',
            globals(),
            uint16,
            NX_CT_A_IPPORT_NONE = 0,
            NX_CT_A_IPPORT_FTP = 21,
            NX_CT_A_IPPORT_TFTP = 69,
        )


    # /* Which optional fields are present? */
    nx_nat_range = \
        enum(
            'nx_nat_range',
            globals(),
            uint16,
            True,
            NX_NAT_RANGE_IPV4_MIN  = 1 << 0, # /* ovs_be32 */
            NX_NAT_RANGE_IPV4_MAX  = 1 << 1, # /* ovs_be32 */
            NX_NAT_RANGE_IPV6_MIN  = 1 << 2, # /* struct in6_addr */
            NX_NAT_RANGE_IPV6_MAX  = 1 << 3, # /* struct in6_addr */
            NX_NAT_RANGE_PROTO_MIN = 1 << 4, # /* ovs_be16 */
            NX_NAT_RANGE_PROTO_MAX = 1 << 5, # /* ovs_be16 */
        )


    # /* Bits for 'flags' in struct nx_action_nat.
    #  */
    nx_nat_flags = \
        enum(
            'nx_nat_flags',
            globals(),
            uint16,
            True,
            NX_NAT_F_SRC          = 1 << 0, # /* Mutually exclusive with NX_NAT_F_DST. */
            NX_NAT_F_DST          = 1 << 1,
            NX_NAT_F_PERSISTENT   = 1 << 2,
            NX_NAT_F_PROTO_HASH   = 1 << 3, # /* Mutually exclusive with PROTO_RANDOM. */
            NX_NAT_F_PROTO_RANDOM = 1 << 4,
        )

    NX_NAT_F_MASK = (NX_NAT_F_SRC | NX_NAT_F_DST | NX_NAT_F_PERSISTENT | NX_NAT_F_PROTO_HASH | NX_NAT_F_PROTO_RANDOM)


    def PACKET_TYPE(NS, NS_TYPE):
        return (((NS) << 16 | (NS_TYPE)))
    
    # /* Header and packet type name spaces. */
    ofp_header_type_namespaces = \
        enum(
            'ofp_header_type_namespaces',
            globals(),
            uint16,
            OFPHTN_ONF = 0,             # /* ONF namespace. */
            OFPHTN_ETHERTYPE = 1,       # /* ns_type is an Ethertype. */
            OFPHTN_IP_PROTO = 2,        # /* ns_type is a IP protocol number. */
            OFPHTN_UDP_TCP_PORT = 3,    # /* ns_type is a TCP or UDP port. */
            OFPHTN_IPV4_OPTION = 4,     # /* ns_type is an IPv4 option number. */
        )
    OFPHTN_N_TYPES = 5
    
    # /* Well-known packet_type field values. */
    packet_type = \
        enum(
            'packet_type',
            globals(),
            uint32,
            PT_ETH  = PACKET_TYPE(OFPHTN_ONF, 0x0000),  # /* Default PT: Ethernet */
            PT_USE_NEXT_PROTO = PACKET_TYPE(OFPHTN_ONF, 0xfffe),  # /* Pseudo PT for decap. */
            PT_IPV4 = PACKET_TYPE(OFPHTN_ETHERTYPE, _ethernet.ETHERTYPE_IP),
            PT_IPV6 = PACKET_TYPE(OFPHTN_ETHERTYPE, _ethernet.ETHERTYPE_IPV6),
            PT_MPLS = PACKET_TYPE(OFPHTN_ETHERTYPE, _ethernet.ETHERTYPE_MPLS_UC),
            PT_MPLS_MC = PACKET_TYPE(OFPHTN_ETHERTYPE, _ethernet.ETHERTYPE_MPLS_MC),
            PT_NSH  = PACKET_TYPE(OFPHTN_ETHERTYPE, _ethernet.ETHERTYPE_NSH),
            PT_UNKNOWN = PACKET_TYPE(0xffff, 0xffff),   # /* Unknown packet type. */
        )

    ofp_ed_prop_class = \
        enum(
            'ofp_ed_prop_class',
            globals(),
            uint16,
            OFPPPC_BASIC = 0,            # /* ONF Basic class. */
            OFPPPC_MPLS  = 1,            # /* MPLS property class. */
            OFPPPC_GRE   = 2,            # /* GRE property class. */
            OFPPPC_GTP   = 3,            # /* GTP property class. */
            OFPPPC_NSH   = 4,            # /* NSH property class */
        
            # /* Experimenter property class.
            #  *
            #  * First 32 bits of property data
            #  * is exp id after that is the experimenter property data.
            #  */
            OFPPPC_EXPERIMENTER=0xffff
        )
    
    ofp_ed_nsh_prop_type = \
        enum(
            'ofp_ed_nsh_prop_type',
            globals(),
            uint8,
            OFPPPT_PROP_NSH_NONE = 0,    # /* unused */
            OFPPPT_PROP_NSH_MDTYPE = 1,  # /* property MDTYPE in NSH */
            OFPPPT_PROP_NSH_TLV = 2,     # /* property TLV in NSH */
        )

    # /*
    #  * External representation of encap/decap properties.
    #  * These must be padded to a multiple of 8 bytes.
    #  */
    ofp_ed_prop = \
        nstruct(
            (ofp_ed_prop_class, 'prop_class'),
            (uint8, 'type'),
            (uint8, 'len'),
            name = 'ofp_ed_prop',
            size = lambda x: x.len,
            prepack = packrealsize('len'),
            classifier = lambda x: x.prop_class
        )
    
    ofp_ed_prop_nsh = \
        nstruct(
            name = 'ofp_ed_prop_nsh',
            base = ofp_ed_prop,
            criteria = lambda x: x.prop_class == OFPPPC_NSH,
            classifyby = (OFPPPC_NSH,),
            init = packvalue(OFPPPC_NSH, 'prop_class'),
            extend = {'type': ofp_ed_nsh_prop_type},
            classifier = lambda x: x.type
        )
    
    ofp_ed_prop_nsh_md_type = \
        nstruct(
            (nsh_md_types, 'md_type'),          # /* NSH MD type .*/
            (uint8[3],),                        # /* Padding to 8 bytes. */
            name = 'ofp_ed_prop_nsh_md_type',
            base = ofp_ed_prop_nsh,
            criteria = lambda x: x.type == OFPPPT_PROP_NSH_MDTYPE,
            classifyby = (OFPPPT_PROP_NSH_MDTYPE,),
            init = packvalue(OFPPPT_PROP_NSH_MDTYPE, 'type')
        )
    
    _ofp_ed_prop_nsh_tlv_data = \
        nstruct(
            (raw, 'data'),
            name = '_ofp_ed_prop_nsh_tlv_data',
            size = lambda x: x.tlv_len,
            prepack = packrealsize('tlv_len'),
            padding = 1,
        )
    
    ofp_ed_prop_nsh_tlv = \
        nstruct(
            (nsh_md_class, 'tlv_class'),        # /* Metadata class. */
            (uint8, 'tlv_type'),                # /* Metadata type including C bit. */
            (uint8, 'tlv_len'),                # /* Metadata value length (0-127). */
            (_ofp_ed_prop_nsh_tlv_data,),
            name = 'ofp_ed_prop_nsh_tlv',
            base = ofp_ed_prop_nsh,
            criteria = lambda x: x.type == OFPPPT_PROP_NSH_TLV,
            classifyby = (OFPPPT_PROP_NSH_TLV,),
            init = packvalue(OFPPPT_PROP_NSH_TLV, 'type')
        )


    # /* TLV table commands */
    nx_tlv_table_mod_command = \
        enum(
            'nx_tlv_table_mod_command',
            globals(),
            uint16,
            NXTTMC_ADD = 0,          # /* New mappings (fails if an option is already
                                     #    mapped). */
            NXTTMC_DELETE = 1,       # /* Delete mappings, identified by index
                                     #  * (unmapped options are ignored). */
            NXTTMC_CLEAR = 2,        # /* Clear all mappings. Additional information
                                     #    in this command is ignored. */
        )


def create_extension(namespace, nicira_header, nx_action, nx_stats_request, nx_stats_reply,
                     msg_subtype, action_subtype, stats_subtype):
    with _warnings.catch_warnings():
        _warnings.filterwarnings('ignore', '^padding', StructDefWarning)
        
        #=======================================================================
        # /* This command enables or disables an Open vSwitch extension that allows a
        #  * controller to specify the OpenFlow table to which a flow should be added,
        #  * instead of having the switch decide which table is most appropriate as
        #  * required by OpenFlow 1.0.  Because NXM was designed as an extension to
        #  * OpenFlow 1.0, the extension applies equally to ofp10_flow_mod and
        #  * nx_flow_mod.  By default, the extension is disabled.
        #  *
        #  * When this feature is enabled, Open vSwitch treats struct ofp10_flow_mod's
        #  * and struct nx_flow_mod's 16-bit 'command' member as two separate fields.
        #  * The upper 8 bits are used as the table ID, the lower 8 bits specify the
        #  * command as usual.  A table ID of 0xff is treated like a wildcarded table ID.
        #  *
        #  * The specific treatment of the table ID depends on the type of flow mod:
        #  *
        #  *    - OFPFC_ADD: Given a specific table ID, the flow is always placed in that
        #  *      table.  If an identical flow already exists in that table only, then it
        #  *      is replaced.  If the flow cannot be placed in the specified table,
        #  *      either because the table is full or because the table cannot support
        #  *      flows of the given type, the switch replies with an OFPFMFC_TABLE_FULL
        #  *      error.  (A controller can distinguish these cases by comparing the
        #  *      current and maximum number of entries reported in ofp_table_stats.)
        #  *
        #  *      If the table ID is wildcarded, the switch picks an appropriate table
        #  *      itself.  If an identical flow already exist in the selected flow table,
        #  *      then it is replaced.  The choice of table might depend on the flows
        #  *      that are already in the switch; for example, if one table fills up then
        #  *      the switch might fall back to another one.
        #  *
        #  *    - OFPFC_MODIFY, OFPFC_DELETE: Given a specific table ID, only flows
        #  *      within that table are matched and modified or deleted.  If the table ID
        #  *      is wildcarded, flows within any table may be matched and modified or
        #  *      deleted.
        #  *
        #  *    - OFPFC_MODIFY_STRICT, OFPFC_DELETE_STRICT: Given a specific table ID,
        #  *      only a flow within that table may be matched and modified or deleted.
        #  *      If the table ID is wildcarded and exactly one flow within any table
        #  *      matches, then it is modified or deleted; if flows in more than one
        #  *      table match, then none is modified or deleted.
        #  */
        #=======================================================================
        nx_flow_mod_table_id = nstruct(
            (uint8, 'set'),                 # /* Nonzero to enable, zero to disable. */
            (uint8[7],),
            name = 'nx_flow_mod_table_id',
            base = nicira_header,
            criteria = lambda x: getattr(x, msg_subtype) == NXT_FLOW_MOD_TABLE_ID,
            classifyby = (NXT_FLOW_MOD_TABLE_ID,),
            init = packvalue(NXT_FLOW_MOD_TABLE_ID, msg_subtype)
        )
        namespace['nx_flow_mod_table_id'] = nx_flow_mod_table_id
    
    
        #=======================================================================
        # /* NXT_SET_PACKET_IN_FORMAT request.
        #  *
        #  * For any given OpenFlow version, Open vSwitch supports multiple formats for
        #  * "packet-in" messages.  The default is always the standard format for the
        #  * OpenFlow version in question, but NXT_SET_PACKET_IN_FORMAT can be used to
        #  * set an alternative format.
        #  *
        #  * From OVS v1.1 to OVS v2.5, this request was only honored for OpenFlow 1.0.
        #  * Requests to set format NXPIF_NXT_PACKET_IN were accepted for OF1.1+ but they
        #  * had no effect.  (Requests to set formats other than NXPIF_STANDARD or
        #  * NXPIF_NXT_PACKET_IN were rejected with OFPBRC_EPERM.)
        #  *
        #  * From OVS v2.6 onward, this request is honored for all OpenFlow versions.
        #  */
        #=======================================================================
        nx_set_packet_in_format = nstruct(
            (nx_packet_in_format, 'format'),           # /* One of NXPIF_*. */
            name = 'nx_set_packet_in_format',
            base = nicira_header,
            criteria = lambda x: getattr(x, msg_subtype) == NXT_SET_PACKET_IN_FORMAT,
            classifyby = (NXT_SET_PACKET_IN_FORMAT,),
            init = packvalue(NXT_SET_PACKET_IN_FORMAT, msg_subtype)
        )
        namespace['nx_set_packet_in_format'] = nx_set_packet_in_format
        '''
        /* NXT_PACKET_IN (analogous to OFPT_PACKET_IN).
         *
         * NXT_PACKET_IN is similar to the OpenFlow 1.2 OFPT_PACKET_IN.  The
         * differences are:
         *
         *     - NXT_PACKET_IN includes the cookie of the rule that triggered the
         *       message.  (OpenFlow 1.3 OFPT_PACKET_IN also includes the cookie.)
         *
         *     - The metadata fields use NXM (instead of OXM) field numbers.
         *
         * Open vSwitch 1.9.0 and later omits metadata fields that are zero (as allowed
         * by OpenFlow 1.2).  Earlier versions included all implemented metadata
         * fields.
         *
         * Open vSwitch does not include non-metadata in the nx_match, because by
         * definition that information can be found in the packet itself.  The format
         * and the standards allow this, however, so controllers should be prepared to
         * tolerate future changes.
         *
         * The NXM format is convenient for reporting metadata values, but it is
         * important not to interpret the format as matching against a flow, because it
         * does not.  Nothing is being matched; arbitrary metadata masks would not be
         * meaningful.
         *
         * Whereas in most cases a controller can expect to only get back NXM fields
         * that it set up itself (e.g. flow dumps will ordinarily report only NXM
         * fields from flows that the controller added), NXT_PACKET_IN messages might
         * contain fields that the controller does not understand, because the switch
         * might support fields (new registers, new protocols, etc.) that the
         * controller does not.  The controller must prepared to tolerate these.
         *
         * The 'cookie' field has no meaning when 'reason' is OFPR_NO_MATCH.  In this
         * case it should be UINT64_MAX. */
         '''
        
        if 'ofp_oxm' in namespace:
            nx_match = namespace['ofp_oxm']
            namespace['nx_match'] = nx_match
            nx_match_mask = namespace['ofp_oxm_mask']
            namespace['nx_match_mask'] = nx_match_mask
            nx_match_nomask = namespace['ofp_oxm_nomask']
            namespace['nx_match_nomask'] = nx_match_nomask
            create_nxm = namespace['create_oxm']
            namespace['create_nxm'] = create_nxm
            
            nx_match_nomask_ext = nstruct(
                base = nx_match_nomask,
                criteria = lambda x: NXM_VENDOR(x.header) <= 1,
                extend = {'header': nxm_header},
                name = 'nx_match_nomask_ext'
            )
            namespace['nx_match_nomask_ext'] = nx_match_nomask_ext
            nx_match_mask_ext = nstruct(
                base = nx_match_mask,
                criteria = lambda x: NXM_VENDOR(x.header) <= 1,
                extend = {'header': nxm_header},
                name = 'nx_match_mask_ext'
            )
            namespace['nx_match_mask_ext'] = nx_match_mask_ext
            
            OFPXMC_EXPERIMENTER = namespace['OFPXMC_EXPERIMENTER']
            
            all_nxm_header = nxm_header.merge(namespace['ofp_oxm_header'])
            
            def _create_oxm_header_only_fields(name, criteria = None):
                if criteria is None:
                    return ((all_nxm_header, name),
                            (optional(common.experimenter_ids, name + '_experimenter',
                                      lambda x: NXM_VENDOR(getattr(x, name)) == OFPXMC_EXPERIMENTER),))
                else:
                    return ((optional(all_nxm_header, name, criteria),),
                            (optional(common.experimenter_ids, name + '_experimenter',
                                      lambda x: criteria(x) and NXM_VENDOR(getattr(x, name)) == OFPXMC_EXPERIMENTER),))
        else:
            nx_match = nstruct(
                (nxm_header, 'header'),
                name = 'nx_match',
                padding = 1,
                size = lambda x: NXM_LENGTH(x.header) + 4
            )
            namespace['nx_match'] = nx_match
            nx_match_nomask = nstruct(
                (common.hexraw, 'value'),
                base = nx_match,
                criteria = lambda x: not NXM_HASMASK(x.header),
                init = packvalue(NXM_OF_IN_PORT, 'header'),
                name = 'nx_match_nomask'
            )
            namespace['nx_match_nomask'] = nx_match_nomask
            _nxm_mask_value = nstruct(
                (common.hexraw, 'value'),
                name = 'nxm_mask_value',
                size = lambda x: NXM_LENGTH(x.header) // 2,
                padding = 1
            )
            nx_match_mask = nstruct(
                (_nxm_mask_value,),
                (common.hexraw, 'mask'),
                base = nx_match,
                criteria = lambda x: NXM_HASMASK(x.header),
                init = packvalue(NXM_OF_ETH_SRC_W, 'header'),
                name = 'nx_match_mask',
            )
            namespace['nx_match_mask'] = nx_match_mask        
            def create_nxm(header, value = None, mask = None):
                if NXM_HASMASK(header):
                    nxm = nx_match_mask.new()
                    size = NXM_LENGTH(header) // 2
                else:
                    nxm = nx_match_nomask.new()
                    size = NXM_LENGTH(header)
                nxm.header = header
                nxm.value = common.create_binary(value, size)
                if NXM_HASMASK(header):
                    nxm.mask = common.create_binary(mask, size)
                nxm._pack()
                nxm._autosubclass()
                return nxm
            namespace['create_nxm'] = create_nxm
            nx_match_nomask_ext = nx_match_nomask
            nx_match_mask_ext = nx_match_mask
            namespace['nx_match_nomask_ext'] = nx_match_nomask_ext
            namespace['nx_match_mask_ext'] = nx_match_mask_ext
            all_nxm_header = nxm_header
            def _create_oxm_header_only_fields(name, criteria = None):
                if criteria is None:
                    return ((nxm_header, name),)
                else:
                    return ((optional(nxm_header, name, criteria),),)

        from namedstruct.namedstruct import rawtype as _rawtype
        import socket as _socket
        if 'ip4_addr_bytes' in namespace:
            ip4_addr_bytes = namespace['ip4_addr_bytes']
        else:
            ip4_addr_bytes = prim('4s', 'ip4_addr_bytes')
            ip4_addr_bytes.formatter = lambda x: _socket.inet_ntoa(x)
            namespace['ip4_addr_bytes'] = ip4_addr_bytes
        
        nxm_mask_ipv4 = nstruct(name = 'nxm_mask_ipv4',
                                    base = nx_match_mask_ext,
                                    criteria = lambda x: x.header in (NXM_OF_IP_SRC_W, NXM_OF_IP_DST_W,
                                                                      NXM_OF_ARP_SPA_W, NXM_OF_ARP_TPA_W,
                                                                      NXM_NX_TUN_IPV4_SRC_W, NXM_NX_TUN_IPV4_DST_W,
                                                                      NXM_NX_CT_NW_SRC_W, NXM_NX_CT_NW_DST_W),
                                    init = packvalue(NXM_OF_IP_SRC_W, 'header'),
                                    extend = {'value' : ip4_addr_bytes, 'mask' : ip4_addr_bytes}
                                    )
        namespace['nxm_mask_ipv4'] = nxm_mask_ipv4
        nxm_nomask_ipv4 = nstruct(name = 'nxm_nomask_ipv4',
                                    base = nx_match_nomask_ext,
                                    criteria = lambda x: x.header in (NXM_OF_IP_SRC, NXM_OF_IP_DST,
                                                                      NXM_OF_ARP_SPA, NXM_OF_ARP_TPA,
                                                                      NXM_NX_TUN_IPV4_SRC, NXM_NX_TUN_IPV4_DST,
                                                                      NXM_NX_CT_NW_SRC, NXM_NX_CT_NW_DST),
                                    init = packvalue(NXM_OF_IP_SRC, 'header'),
                                    extend = {'value' : ip4_addr_bytes}
                                    )
        namespace['nxm_nomask_ipv4'] = nxm_nomask_ipv4
        if 'mac_addr_bytes' in namespace:
            mac_addr_bytes = namespace['mac_addr_bytes']
        else:
            mac_addr_bytes = _rawtype()        
            mac_addr_bytes.formatter = lambda x: ':'.join('%02X' % (c,) for c in bytearray(x))
            namespace['mac_addr_bytes'] = mac_addr_bytes
        
        nxm_mask_eth = nstruct(name = 'nxm_mask_eth',
                                   base = nx_match_mask_ext,
                                   criteria = lambda x: x.header in (NXM_OF_ETH_SRC_W, NXM_OF_ETH_DST_W),
                                    init = packvalue(NXM_OF_ETH_SRC_W, 'header'),
                                   extend = {'value' : mac_addr_bytes, 'mask' : mac_addr_bytes})
        namespace['nxm_mask_eth'] = nxm_mask_eth
        
        nxm_nomask_eth = nstruct(name = 'nxm_nomask_eth',
                                   base = nx_match_nomask_ext,
                                   criteria = lambda x: x.header in (NXM_OF_ETH_SRC, NXM_OF_ETH_DST, NXM_NX_ND_SLL, NXM_NX_ND_TLL, NXM_NX_ARP_SHA, NXM_NX_ARP_THA),
                                    init = packvalue(NXM_OF_ETH_SRC, 'header'),
                                   extend = {'value' : mac_addr_bytes})
        namespace['nxm_nomask_eth'] = nxm_nomask_eth
        
        
        ofp_port_no = namespace['ofp_port_no']
        
        nx_port_no = enum('nx_port_no', None, uint16,
                           **dict((k, v & 0xffff) for k,v in ofp_port_no.getDict().items())
                           )
        nxm_port_no_raw = _rawtype()
        nxm_port_no_raw.formatter = lambda x: nx_port_no.formatter(nx_port_no.parse(x)[0])
        namespace['nx_port_no'] = nx_port_no
        namespace['nxm_port_no_raw'] = nxm_port_no_raw
        nxm_nomask_port = nstruct(name = 'nxm_nomask_port',
                                        base = nx_match_nomask_ext,
                                        criteria = lambda x: x.header == NXM_OF_IN_PORT,
                                        init = packvalue(NXM_OF_IN_PORT, 'header'),
                                        extend = {'value': nxm_port_no_raw}
                                        )
        namespace['nxm_nomask_port'] = nxm_nomask_port
        if 'ethtype_raw' in namespace:
            ethtype_raw = namespace['ethtype_raw']
        else:
            ethtype_raw = _rawtype()
            ethtype_raw.formatter = lambda x: ethertype.formatter(ethertype.parse(x)[0])
            namespace['ethtype_raw'] = ethtype_raw
        
        nxm_nomask_ethertype = nstruct(name = 'nxm_nomask_ethertype',
                                           base = nx_match_nomask_ext,
                                           criteria = lambda x: x.header == NXM_OF_ETH_TYPE,
                                           init = packvalue(NXM_OF_ETH_TYPE, 'header'),
                                           extend = {'value': ethtype_raw})
        namespace['nxm_nomask_ethertype'] = nxm_nomask_ethertype
        if 'arpop_raw' in namespace:
            arpop_raw = namespace['arpop_raw']
        else:
            arpop_raw = _rawtype()
            arpop_raw.formatter = lambda x: arp_op_code.formatter(arp_op_code.parse(x)[0])
            namespace['arpop_raw'] = arpop_raw
        
        nxm_nomask_arpopcode = nstruct(name = 'nxm_nomask_arpopcode',
                                           base = nx_match_nomask_ext,
                                           criteria = lambda x: x.header == NXM_OF_ARP_OP,
                                           init = packvalue(NXM_OF_ARP_OP, 'header'),
                                           extend = {'value': arpop_raw})
        namespace['nxm_nomask_arpopcode'] = nxm_nomask_arpopcode
        
        if 'ip_protocol_raw' in namespace:
            ip_protocol_raw = namespace['ip_protocol_raw']
        else:
            ip_protocol_raw = _rawtype()
            ip_protocol_raw.formatter = lambda x: ip_protocol.formatter(ip_protocol.parse(x)[0])
            namespace['ip_protocol_raw'] = ip_protocol_raw
        
        nxm_nomask_ip_protocol = nstruct(name = 'nxm_nomask_ip_protocol',
                                             base = nx_match_nomask_ext,
                                             criteria = lambda x: x.header in (NXM_OF_IP_PROTO, NXM_NX_CT_NW_PROTO),
                                             init = packvalue(NXM_OF_IP_PROTO, 'header'),
                                             extend = {'value': ip_protocol_raw})
        namespace['nxm_nomask_ip_protocol'] = nxm_nomask_ip_protocol
        if 'ip6_addr_bytes' in namespace:
            nxm_nomask_ipv6 = nstruct(name = 'nxm_nomask_ipv6',
                                          base = nx_match_nomask_ext,
                                          criteria = lambda x: x.header in (NXM_NX_IPV6_SRC, NXM_NX_IPV6_DST,
                                                                            NXM_NX_ND_TARGET,
                                                                            NXM_NX_TUN_IPV6_SRC,
                                                                            NXM_NX_TUN_IPV6_DST,
                                                                            NXM_NX_CT_IPV6_SRC,
                                                                            NXM_NX_CT_IPV6_DST),
                                          init = packvalue(NXM_NX_IPV6_SRC, 'header'),
                                          extend = {'value': ip6_addr_bytes})
            namespace['nxm_nomask_ipv6'] = nxm_nomask_ipv6
            nxm_mask_ipv6 = nstruct(name = 'nxm_mask_ipv6',
                                          base = nx_match_mask_ext,
                                          criteria = lambda x: x.header in (NXM_NX_IPV6_SRC_W, NXM_NX_IPV6_DST_W,
                                                                            NXM_NX_TUN_IPV6_SRC_W,
                                                                            NXM_NX_TUN_IPV6_DST_W,
                                                                            NXM_NX_CT_IPV6_SRC_W,
                                                                            NXM_NX_CT_IPV6_DST_W),
                                          init = packvalue(NXM_NX_IPV6_SRC_W, 'header'),
                                          extend = {'value': ip6_addr_bytes, 'mask': ip6_addr_bytes})
            namespace['nxm_mask_ipv6'] = nxm_mask_ipv6
        
        nx_ip_frag_raw = _rawtype()
        nx_ip_frag_raw.formatter = lambda x: nx_ip_frag.formatter(nx_ip_frag.parse(x)[0])
        nxm_nomask_ipfrag = nstruct(name = 'nxm_nomask_ipfrag',
                                    base = nx_match_nomask_ext,
                                    criteria = lambda x: x.header == NXM_NX_IP_FRAG,
                                    init = packvalue(NXM_NX_IP_FRAG, 'header'),
                                    extend = {'value': nx_ip_frag_raw})
        namespace['nxm_nomask_ipfrag'] = nxm_nomask_ipfrag
        nxm_mask_ipfrag = nstruct(name = 'nxm_mask_ipfrag',
                                    base = nx_match_mask_ext,
                                    criteria = lambda x: x.header == NXM_NX_IP_FRAG_W,
                                    init = packvalue(NXM_NX_IP_FRAG_W, 'header'),
                                    extend = {'value': nx_ip_frag_raw, 'mask': nx_ip_frag_raw})
        namespace['nxm_mask_ipfrag'] = nxm_mask_ipfrag
        
        nx_tunnel_flags_raw = _rawtype()
        nx_tunnel_flags_raw.formatter = lambda x: nx_tunnel_flags.formatter(nx_tunnel_flags.parse(x)[0])
        nxm_nomask_tun_flag = nstruct(name = 'nxm_nomask_tun_flag',
                                      base = nx_match_nomask_ext,
                                      criteria = lambda x: x.header == NXM_NX_TUN_FLAGS,
                                      init = packvalue(NXM_NX_TUN_FLAGS, 'header'),
                                      extend = {'value': nx_tunnel_flags_raw})
        namespace['nxm_nomask_tun_flag'] = nxm_nomask_tun_flag
        nxm_mask_tun_flag = nstruct(name = 'nxm_mask_tun_flag',
                                    base = nx_match_mask_ext,
                                    criteria = lambda x: x.header == NXM_NX_TUN_FLAGS_W,
                                    init = packvalue(NXM_NX_TUN_FLAGS_W, 'header'),
                                    extend = {'value': nx_tunnel_flags_raw, 'mask': nx_tunnel_flags_raw})
        namespace['nxm_mask_tun_flag'] = nxm_mask_tun_flag

        nx_tun_gbp_flags_raw = _rawtype()
        nx_tun_gbp_flags_raw.formatter = lambda x: vxlan_group_policy_flags.formatter(vxlan_group_policy_flags.parse(x)[0])
        nxm_nomask_tun_gbp_flags = nstruct(name = 'nxm_nomask_tun_gbp_flags',
                                      base = nx_match_nomask_ext,
                                      criteria = lambda x: x.header == NXM_NX_TUN_GBP_FLAGS,
                                      init = packvalue(NXM_NX_TUN_GBP_FLAGS, 'header'),
                                      extend = {'value': nx_tun_gbp_flags_raw})
        namespace['nxm_nomask_tun_gbp_flags'] = nxm_nomask_tun_gbp_flags
        nxm_mask_tun_gbp_flags = nstruct(name = 'nxm_mask_tun_gbp_flags',
                                    base = nx_match_mask_ext,
                                    criteria = lambda x: x.header == NXM_NX_TUN_GBP_FLAGS_W,
                                    init = packvalue(NXM_NX_TUN_GBP_FLAGS_W, 'header'),
                                    extend = {'value': nx_tun_gbp_flags_raw, 'mask': nx_tun_gbp_flags_raw})
        namespace['nxm_mask_tun_gbp_flags'] = nxm_mask_tun_gbp_flags

        nxm_nomask_tun_metadata = nstruct(name = 'nxm_nomask_tun_metadata',
                                      base = nx_match_nomask_ext,
                                      criteria = lambda x: _is_nx_tunnel_metadata(x.header),
                                      init = packvalue(create_nx_tunnel_metadata_header(0, 1, False), 'header'),
                                      extend = {'header': _nx_tunnel_metadata_header_type})
        namespace['nxm_nomask_tun_metadata'] = nxm_nomask_tun_metadata
        nxm_mask_tun_metadata = nstruct(name = 'nxm_mask_tun_metadata',
                                    base = nx_match_mask_ext,
                                    criteria = lambda x: _is_nx_tunnel_metadata(x.header),
                                    init = packvalue(create_nx_tunnel_metadata_header(0, 1, True), 'header'),
                                    extend = {'header': _nx_tunnel_metadata_header_type})
        namespace['nxm_mask_tun_metadata'] = nxm_mask_tun_metadata

        nx_conntrack_state_raw = _rawtype()
        nx_conntrack_state_raw.formatter = lambda x: nx_conntrack_state.formatter(nx_conntrack_state.parse(x)[0])
        nxm_nomask_conntrack_state = nstruct(name = 'nxm_nomask_conntrack_state',
                                      base = nx_match_nomask_ext,
                                      criteria = lambda x: x.header == NXM_NX_CT_STATE,
                                      init = packvalue(NXM_NX_CT_STATE, 'header'),
                                      extend = {'value': nx_conntrack_state_raw})
        namespace['nxm_nomask_conntrack_state'] = nxm_nomask_conntrack_state
        nxm_mask_conntrack_state = nstruct(name = 'nxm_mask_conntrack_state',
                                    base = nx_match_mask_ext,
                                    criteria = lambda x: x.header == NXM_NX_CT_STATE_W,
                                    init = packvalue(NXM_NX_CT_STATE_W, 'header'),
                                    extend = {'value': nx_conntrack_state_raw, 'mask': nx_conntrack_state_raw})
        namespace['nxm_mask_conntrack_state'] = nxm_mask_conntrack_state

        
        nx_matches = nstruct(
                (nx_match[0], 'matches'),
                name = 'nx_matches',
                size = lambda x: x.match_len,
                prepack = packrealsize('match_len'),
                padding = 8
        )
        
        namespace['nx_matches'] = nx_matches
        
        nx_packet_in = nstruct(
            (uint32, 'buffer_id'),     #  /* ID assigned by datapath. */
            (uint16, 'total_len'),     #  /* Full length of frame. */
            (uint8, 'reason'),         #  /* Reason packet is sent (one of OFPR_*). */
            (uint8, 'table_id'),       #  /* ID of the table that was looked up. */
            (uint64, 'cookie'),        #  /* Cookie of the rule that was looked up. */
            (uint16, 'match_len'),     # /* Size of nx_match. */
            (uint8[6],),               # /* Align to 64-bits. */
            (nx_matches,),
            (uint8[2],),
            (raw, 'data'),
            name = 'nx_packet_in',
            base = nicira_header,
            classifyby = (NXT_PACKET_IN,),
            criteria = lambda x: getattr(x, msg_subtype) == NXT_PACKET_IN,
            init = packvalue(NXT_PACKET_IN, msg_subtype)
        )
        namespace['nx_packet_in'] = nx_packet_in
        
        #========================================================================
        # /* NXT_PACKET_IN2
        #  * ==============
        #  *
        #  * NXT_PACKET_IN2 is conceptually similar to OFPT_PACKET_IN but it is expressed
        #  * as an extensible set of properties instead of using a fixed structure.
        #  *
        #  * Added in Open vSwitch 2.6
        #  *
        #  *
        #  * Continuations
        #  * -------------
        #  *
        #  * When a "controller" action specifies the "pause" flag, the controller action
        #  * freezes the packet's trip through Open vSwitch flow tables and serializes
        #  * that state into the packet-in message as a "continuation".  The controller
        #  * can later send the continuation back to the switch, which will restart the
        #  * packet's traversal from the point where it was interrupted.  This permits an
        #  * OpenFlow controller to interpose on a packet midway through processing in
        #  * Open vSwitch.
        #  *
        #  * Continuations fit into packet processing this way:
        #  *
        #  * 1. A packet ingresses into Open vSwitch, which runs it through the OpenFlow
        #  *    tables.
        #  *
        #  * 2. An OpenFlow flow executes a "controller" action that includes the "pause"
        #  *    flag.  Open vSwitch serializes the packet processing state and sends it,
        #  *    as an NXT_PACKET_IN2 that includes an additional NXPINT_CONTINUATION
        #  *    property (the continuation), to the OpenFlow controller.
        #  *
        #  *    (The controller must use NXAST_CONTROLLER2 to generate the packet-in,
        #  *    because only this form of the "controller" action has a "pause" flag.
        #  *    Similarly, the controller must use NXT_SET_PACKET_IN_FORMAT to select
        #  *    NXT_PACKET_IN2 as the packet-in format, because this is the only format
        #  *    that supports continuation passing.)
        #  *
        #  * 3. The controller receives the NXT_PACKET_IN2 and processes it.  The
        #  *    controller can interpret and, if desired, modify some of the contents of
        #  *    the packet-in, such as the packet and the metadata being processed.
        #  *
        #  * 4. The controller sends the continuation back to the switch, using an
        #  *    NXT_RESUME message.  Packet processing resumes where it left off.
        #  *
        #  * The controller might change the pipeline configuration concurrently with
        #  * steps 2 through 4.  For example, it might add or remove OpenFlow flows.  If
        #  * that happens, then the packet will experience a mix of processing from the
        #  * two configurations, that is, the initial processing (before
        #  * NXAST_CONTROLLER2) uses the initial flow table, and the later processing
        #  * (after NXT_RESUME) uses the later flow table.  This means that the
        #  * controller needs to take care to avoid incompatible pipeline changes while
        #  * processing continuations.
        #  *
        #  * External side effects (e.g. "output") of OpenFlow actions processed before
        #  * NXAST_CONTROLLER2 is encountered might be executed during step 2 or step 4,
        #  * and the details may vary among Open vSwitch features and versions.  Thus, a
        #  * controller that wants to make sure that side effects are executed must pass
        #  * the continuation back to the switch, that is, must not skip step 4.
        #  *
        #  * Architecturally, continuations may be "stateful" or "stateless", that is,
        #  * they may or may not refer to buffered state maintained in Open vSwitch.
        #  * This means that a controller should not attempt to resume a given
        #  * continuations more than once (because the switch might have discarded the
        #  * buffered state after the first use).  For the same reason, continuations
        #  * might become "stale" if the controller takes too long to resume them
        #  * (because the switch might have discarded old buffered state).  Taken
        #  * together with the previous note, this means that a controller should resume
        #  * each continuation exactly once (and promptly).
        #  *
        #  * Without the information in NXPINT_CONTINUATION, the controller can (with
        #  * careful design, and help from the flow cookie) determine where the packet is
        #  * in the pipeline, but in the general case it can't determine what nested
        #  * "resubmit"s that may be in progress, or what data is on the stack maintained
        #  * by NXAST_STACK_PUSH and NXAST_STACK_POP actions, what is in the OpenFlow
        #  * action set, etc.
        #  *
        #  * Continuations are expensive because they require a round trip between the
        #  * switch and the controller.  Thus, they should not be used to implement
        #  * processing that needs to happen at "line rate".
        #  *
        #  * The contents of NXPINT_CONTINUATION are private to the switch, may change
        #  * unpredictably from one version of Open vSwitch to another, and are not
        #  * documented here.  The contents are also tied to a given Open vSwitch process
        #  * and bridge, so that restarting Open vSwitch or deleting and recreating a
        #  * bridge will cause the corresponding NXT_RESUME to be rejected.
        #  *
        #  * In the current implementation, Open vSwitch forks the packet processing
        #  * pipeline across patch ports.  Suppose, for example, that the pipeline for
        #  * br0 outputs to a patch port whose peer belongs to br1, and that the pipeline
        #  * for br1 executes a controller action with the "pause" flag.  This only
        #  * pauses processing within br1, and processing in br0 continues and possibly
        #  * completes with visible side effects, such as outputting to ports, before
        #  * br1's controller receives or processes the continuation.  This
        #  * implementation maintains the independence of separate bridges and, since
        #  * processing in br1 cannot affect the behavior of br0 anyway, should not cause
        #  * visible behavioral changes.
        #  *
        #  * A stateless implementation of continuations may ignore the "controller"
        #  * action max_len, always sending the whole packet, because the full packet is
        #  * required to continue traversal.
        #  */        
        #=======================================================================
        nx_packet_in2_prop = \
            nstruct(
                (nx_packet_in2_prop_type, 'type'),
                (uint16, 'len'),
                name = 'nx_packet_in2_prop',
                size = lambda x: x.len,
                prepack = packrealsize('len'),
                classifier = lambda x: x.type
            )
        
        namespace['nx_packet_in2_prop'] = nx_packet_in2_prop
        
        nx_packet_in2_prop_packet = \
            nstruct(
                (raw, 'value'),
                name = 'nx_packet_in2_prop_packet',
                base = nx_packet_in2_prop,
                criteria = lambda x: x.type == NXPINT_PACKET,
                classifyby = (NXPINT_PACKET,),
                init = packvalue(NXPINT_PACKET, 'type')
            )
        
        namespace['nx_packet_in2_prop_packet'] = nx_packet_in2_prop_packet
        
        nx_packet_in2_prop_full_len = \
            nstruct(
                (uint32, 'value'),
                name = 'nx_packet_in2_prop_full_len',
                base = nx_packet_in2_prop,
                criteria = lambda x: x.type == NXPINT_FULL_LEN,
                classifyby = (NXPINT_FULL_LEN,),
                init = packvalue(NXPINT_FULL_LEN, 'type')
            )
        
        namespace['nx_packet_in2_prop_full_len'] = nx_packet_in2_prop_full_len

        nx_packet_in2_prop_buffer_id = \
            nstruct(
                (uint32, 'value'),
                name = 'nx_packet_in2_prop_buffer_id',
                base = nx_packet_in2_prop,
                criteria = lambda x: x.type == NXPINT_BUFFER_ID,
                classifyby = (NXPINT_BUFFER_ID,),
                init = packvalue(NXPINT_BUFFER_ID, 'type')
            )
        
        namespace['nx_packet_in2_prop_buffer_id'] = nx_packet_in2_prop_buffer_id        
        
        nx_packet_in2_prop_table_id = \
            nstruct(
                (uint8, 'value'),
                name = 'nx_packet_in2_prop_table_id',
                base = nx_packet_in2_prop,
                criteria = lambda x: x.type == NXPINT_TABLE_ID,
                classifyby = (NXPINT_TABLE_ID,),
                init = packvalue(NXPINT_TABLE_ID, 'type')
            )
        
        namespace['nx_packet_in2_prop_table_id'] = nx_packet_in2_prop_table_id        

        nx_packet_in2_prop_cookie = \
            nstruct(
                (uint32,),
                (uint64, 'value'),
                name = 'nx_packet_in2_prop_cookie',
                base = nx_packet_in2_prop,
                criteria = lambda x: x.type == NXPINT_COOKIE,
                classifyby = (NXPINT_COOKIE,),
                init = packvalue(NXPINT_COOKIE, 'type')
            )
        
        namespace['nx_packet_in2_prop_cookie'] = nx_packet_in2_prop_cookie                
        
        if 'ofp_packet_in_reason' in namespace:
            ofp_packet_in_reason = namespace['ofp_packet_in_reason']
        else:
            from .common import ofp_packet_in_reason
        
        nx_packet_in2_prop_reason = \
            nstruct(
                (ofp_packet_in_reason, 'value'),
                name = 'nx_packet_in2_prop_reason',
                base = nx_packet_in2_prop,
                criteria = lambda x: x.type == NXPINT_REASON,
                classifyby = (NXPINT_REASON,),
                init = packvalue(NXPINT_REASON, 'type')
            )
        
        namespace['nx_packet_in2_prop_reason'] = nx_packet_in2_prop_reason
        
        nx_packet_in2_prop_metadata = \
            nstruct(
                (nx_match[0], 'value'),
                name = 'nx_packet_in2_prop_metadata',
                base = nx_packet_in2_prop,
                criteria = lambda x: x.type == NXPINT_METADATA,
                classifyby = (NXPINT_METADATA,),
                init = packvalue(NXPINT_METADATA, 'type')
            )
        
        namespace['nx_packet_in2_prop_metadata'] = nx_packet_in2_prop_metadata                
        
        nx_packet_in2_prop_userdata = \
            nstruct(
                (raw, 'value'),
                name = 'nx_packet_in2_prop_userdata',
                base = nx_packet_in2_prop,
                criteria = lambda x: x.type == NXPINT_USERDATA,
                classifyby = (NXPINT_USERDATA,),
                init = packvalue(NXPINT_USERDATA, 'type')
            )
        
        namespace['nx_packet_in2_prop_userdata'] = nx_packet_in2_prop_userdata

        nx_packet_in2_prop_continuation = \
            nstruct(
                (raw, 'value'),
                name = 'nx_packet_in2_prop_continuation',
                base = nx_packet_in2_prop,
                criteria = lambda x: x.type == NXPINT_CONTINUATION,
                classifyby = (NXPINT_CONTINUATION,),
                init = packvalue(NXPINT_CONTINUATION, 'type')
            )
        
        namespace['nx_packet_in2_prop_continuation'] = nx_packet_in2_prop_continuation
        
        nx_packet_in2 = \
            nstruct(
                (nx_packet_in2_prop[0], 'properties'),
                name = 'nx_packet_in2',
                base = nicira_header,
                criteria = lambda x: getattr(x, msg_subtype) == NXT_PACKET_IN2,
                classifyby = (NXT_PACKET_IN2,),
                init = packvalue(NXT_PACKET_IN2, msg_subtype)
            )
        
        namespace['nx_packet_in2'] = nx_packet_in2
        
        # /* Are the idle_age and hard_age members in struct nx_flow_stats supported?
        #  * If so, the switch does not reply to this message (which consists only of
        #  * a "struct nicira_header").  If not, the switch sends an error reply. */
        nx_flow_age = \
            nstruct(
                name = 'nx_flow_age',
                base = nicira_header,
                criteria = lambda x: getattr(x, msg_subtype) == NXT_FLOW_AGE,
                classifyby = (NXT_FLOW_AGE,),
                init = packvalue(NXT_FLOW_AGE, msg_subtype)
            )
        namespace['nx_flow_age'] = nx_flow_age
        
        '''
        /* Configures the "role" of the sending controller.  The default role is:
         *
         *    - Other (NX_ROLE_OTHER), which allows the controller access to all
         *      OpenFlow features.
         *
         * The other possible roles are a related pair:
         *
         *    - Master (NX_ROLE_MASTER) is equivalent to Other, except that there may
         *      be at most one Master controller at a time: when a controller
         *      configures itself as Master, any existing Master is demoted to the
         *      Slave role.
         *
         *    - Slave (NX_ROLE_SLAVE) allows the controller read-only access to
         *      OpenFlow features.  In particular attempts to modify the flow table
         *      will be rejected with an OFPBRC_EPERM error.
         *
         *      Slave controllers do not receive OFPT_PACKET_IN or OFPT_FLOW_REMOVED
         *      messages, but they do receive OFPT_PORT_STATUS messages.
         */
         '''
        nx_role_request = nstruct(
            (nx_role, 'role'),       # /* One of NX_ROLE_*. */
            name = 'nx_role_request',
            base = nicira_header,
            classifyby = (NXT_ROLE_REQUEST, NXT_ROLE_REPLY),
            criteria = lambda x: getattr(x, msg_subtype) == NXT_ROLE_REQUEST or getattr(x, msg_subtype) == NXT_ROLE_REPLY,
            init = packvalue(NXT_ROLE_REQUEST, msg_subtype)
        )
        namespace['nx_role_request'] = nx_role_request
        '''
        /* NXT_SET_ASYNC_CONFIG.
         *
         * Sent by a controller, this message configures the asynchronous messages that
         * the controller wants to receive.  Element 0 in each array specifies messages
         * of interest when the controller has an "other" or "master" role; element 1,
         * when the controller has a "slave" role.
         *
         * Each array element is a bitmask in which a 0-bit disables receiving a
         * particular message and a 1-bit enables receiving it.  Each bit controls the
         * message whose 'reason' corresponds to the bit index.  For example, the bit
         * with value 1<<2 == 4 in port_status_mask[1] determines whether the
         * controller will receive OFPT_PORT_STATUS messages with reason OFPPR_MODIFY
         * (value 2) when the controller has a "slave" role.
         *
         * As a side effect, for service controllers, this message changes the
         * miss_send_len from default of zero to OFP_DEFAULT_MISS_SEND_LEN (128).
         */
         '''
        if 'ofp_packet_in_reason_bitwise' in namespace:
            ofp_packet_in_reason_bitwise = namespace['ofp_packet_in_reason_bitwise']
        else:
            ofp_packet_in_reason_bitwise = enum('ofp_packet_in_reason_bitwise', None, uint32,
                                            **dict((k, 1<<v) for k,v in ofp_packet_in_reason.getDict().items()))
            namespace['ofp_packet_in_reason_bitwise'] = ofp_packet_in_reason_bitwise
    
        ofp_port_reason = namespace['ofp_port_reason']    
        if 'ofp_port_reason_bitwise' in namespace:
            ofp_port_reason_bitwise = namespace['ofp_port_reason_bitwise']
        else:
            ofp_port_reason_bitwise = enum('ofp_port_reason_bitwise', None, uint32,
                                            **dict((k, 1<<v) for k,v in ofp_port_reason.getDict().items()))
            namespace['ofp_port_reason_bitwise'] = ofp_port_reason_bitwise
    
        ofp_flow_removed_reason = namespace['ofp_flow_removed_reason']
        if 'ofp_flow_removed_reason_bitwise' in namespace:
            ofp_flow_removed_reason_bitwise = namespace['ofp_flow_removed_reason_bitwise']
        else:
            ofp_flow_removed_reason_bitwise = enum('ofp_flow_removed_reason_bitwise', None, uint32,
                                            **dict((k, 1<<v) for k,v in ofp_flow_removed_reason.getDict().items()))
            namespace['ofp_flow_removed_reason_bitwise'] = ofp_flow_removed_reason_bitwise
        
        nx_async_config = nstruct(
            (ofp_packet_in_reason_bitwise[2], 'packet_in_mask'),  #     /* Bitmasks of OFPR_* values. */
            (ofp_port_reason_bitwise[2], 'port_status_mask'),     #     /* Bitmasks of OFPRR_* values. */
            (ofp_flow_removed_reason_bitwise[2], 'flow_removed_mask'), #/* Bitmasks of OFPPR_* values. */
            name = 'nx_async_config',
            base = nicira_header,
            classifyby = (NXT_SET_ASYNC_CONFIG,),
            criteria = lambda x: getattr(x, msg_subtype) == NXT_SET_ASYNC_CONFIG,
            init = packvalue(NXT_SET_ASYNC_CONFIG, msg_subtype)
        )
        namespace['nx_async_config'] = nx_async_config
        
        if 'ofp_async_config_prop' in namespace:
            nx_async_config2_prop = namespace['ofp_async_config_prop']
            namespace['nx_async_config2_prop'] = nx_async_config2_prop
            namespace['nx_async_config2_prop_reasons'] = namespace['ofp_async_config_prop_reasons']
            namespace['nx_async_config2_prop_reasons_packet_in'] = namespace['ofp_async_config_prop_reasons_packet_in']
            namespace['nx_async_config2_prop_reasons_port_status'] = namespace['ofp_async_config_prop_reasons_port_status']
            namespace['nx_async_config2_prop_reasons_flow_removed'] = namespace['ofp_async_config_prop_reasons_flow_removed']
            namespace['nx_async_config2_prop_reasons_role_status'] = namespace['ofp_async_config_prop_reasons_role_status']
            namespace['nx_async_config2_prop_reasons_table'] = namespace['ofp_async_config_prop_reasons_table']
            namespace['nx_async_config2_prop_reasons_requestforward'] = namespace['ofp_async_config_prop_reasons_requestforward']
            namespace['nx_async_config2_prop_experimenter'] = namespace['ofp_async_config_prop_experimenter']
        else:
            # /* Common header for all async config Properties */
            nx_async_config2_prop = \
                nstruct(
                    (common.ofp_async_config_prop_type, 'type'),
                                                        # /* One of OFPACPT_*. */
                    (uint16, 'length'),                 # /* Length in bytes of this property. */
                    name = 'nx_async_config2_prop',
                    size = lambda x: x.length,
                    prepack = packrealsize('length'),
                    classifier = lambda x: x.type
                )
            namespace['nx_async_config2_prop'] = nx_async_config2_prop
            
            # /* Various reason based properties */
            nx_async_config2_prop_reasons = \
                nstruct(
                    (uint32, 'mask'),         # /* Bitmasks of reason values. */
                    name = 'nx_async_config2_prop_reasons',
                    base = nx_async_config2_prop,
                    criteria = lambda x: common.OFPACPT_PACKET_IN_SLAVE <= x.type <= common.OFPACPT_REQUESTFORWARD_MASTER,
                    classifyby = (common.OFPACPT_PACKET_IN_SLAVE,
                                  common.OFPACPT_PACKET_IN_MASTER,
                                  common.OFPACPT_PORT_STATUS_SLAVE,
                                  common.OFPACPT_PORT_STATUS_MASTER,
                                  common.OFPACPT_FLOW_REMOVED_SLAVE,
                                  common.OFPACPT_FLOW_REMOVED_MASTER,
                                  common.OFPACPT_ROLE_STATUS_SLAVE,
                                  common.OFPACPT_ROLE_STATUS_MASTER,
                                  common.OFPACPT_TABLE_STATUS_SLAVE,
                                  common.OFPACPT_TABLE_STATUS_MASTER,
                                  common.OFPACPT_REQUESTFORWARD_SLAVE,
                                  common.OFPACPT_REQUESTFORWARD_MASTER,
                                  common.OFPTFPT_EXPERIMENTER_SLAVE,
                                  common.OFPTFPT_EXPERIMENTER_MASTER,
                                 ),
                    classifier = lambda x: x.type
                )
            namespace['nx_async_config2_prop_reasons'] = nx_async_config2_prop_reasons
            
            ofp_controller_role_reason = namespace['ofp_controller_role_reason']
            if 'ofp_controller_role_reason_bitwise' in namespace:
                ofp_controller_role_reason_bitwise = namespace['ofp_controller_role_reason_bitwise']
            else:
                ofp_controller_role_reason_bitwise = enum('ofp_controller_role_reason_bitwise', None, uint32, True,
                                                          **dict((k, 1<<v) for k,v in ofp_controller_role_reason.getDict().items()))
                namespace['ofp_controller_role_reason_bitwise'] = ofp_controller_role_reason_bitwise
            
            ofp_table_reason = namespace['ofp_table_reason']
            if 'ofp_table_reason_bitwise' in namespace:
                ofp_table_reason_bitwise = namespace['ofp_table_reason_bitwise']
            else:
                ofp_table_reason_bitwise = enum('ofp_table_reason_bitwise', None, uint32, True,
                                                    **dict((k, 1<<v) for k,v in ofp_table_reason.getDict().items()))
                namespace['ofp_table_reason_bitwise'] = ofp_table_reason_bitwise
            
            ofp_requestforward_reason = namespace['ofp_requestforward_reason']
            if 'ofp_requestforward_reason_bitwise' in namespace:
                ofp_requestforward_reason_bitwise = namespace['ofp_requestforward_reason_bitwise']
            else:
                ofp_requestforward_reason_bitwise = enum('ofp_requestforward_reason_bitwise', None, uint32, True,
                                                    **dict((k, 1<<v) for k,v in ofp_requestforward_reason.getDict().items()))
                namespace['ofp_requestforward_reason_bitwise'] = ofp_requestforward_reason_bitwise
            
            nx_async_config2_prop_reasons_packet_in = \
                nstruct(
                    name = 'nx_async_config2_prop_reasons_packet_in',
                    base = nx_async_config2_prop_reasons,
                    criteria = lambda x: x.type in (common.OFPACPT_PACKET_IN_SLAVE, common.OFPACPT_PACKET_IN_MASTER),
                    classifyby = (common.OFPACPT_PACKET_IN_SLAVE, common.OFPACPT_PACKET_IN_MASTER),
                    init = packvalue(common.OFPACPT_PACKET_IN_SLAVE, 'type'),
                    extend = {"mask": ofp_packet_in_reason_bitwise}
                )
            namespace['nx_async_config2_prop_reasons_packet_in'] = nx_async_config2_prop_reasons_packet_in
            
            nx_async_config2_prop_reasons_port_status = \
                nstruct(
                    name = 'nx_async_config2_prop_reasons_port_status',
                    base = nx_async_config2_prop_reasons,
                    criteria = lambda x: x.type in (common.OFPACPT_PORT_STATUS_SLAVE, common.OFPACPT_PORT_STATUS_MASTER),
                    classifyby = (common.OFPACPT_PORT_STATUS_SLAVE, common.OFPACPT_PORT_STATUS_MASTER),
                    init = packvalue(common.OFPACPT_PORT_STATUS_SLAVE, 'type'),
                    extend = {"mask": ofp_port_reason_bitwise}
                )
            namespace['nx_async_config2_prop_reasons_port_status'] = nx_async_config2_prop_reasons_port_status
            
            nx_async_config2_prop_reasons_flow_removed = \
                nstruct(
                    name = 'nx_async_config2_prop_reasons_flow_removed',
                    base = nx_async_config2_prop_reasons,
                    criteria = lambda x: x.type in (common.OFPACPT_FLOW_REMOVED_SLAVE, common.OFPACPT_FLOW_REMOVED_MASTER),
                    classifyby = (common.OFPACPT_FLOW_REMOVED_SLAVE, common.OFPACPT_FLOW_REMOVED_MASTER),
                    init = packvalue(common.OFPACPT_FLOW_REMOVED_SLAVE, 'type'),
                    extend = {"mask": ofp_flow_removed_reason_bitwise}
                )
            namespace['nx_async_config2_prop_reasons_flow_removed'] = nx_async_config2_prop_reasons_flow_removed
            
            nx_async_config2_prop_reasons_role_status = \
                nstruct(
                    name = 'nx_async_config2_prop_reasons_role_status',
                    base = nx_async_config2_prop_reasons,
                    criteria = lambda x: x.type in (common.OFPACPT_ROLE_STATUS_SLAVE, common.OFPACPT_ROLE_STATUS_MASTER),
                    classifyby = (common.OFPACPT_ROLE_STATUS_SLAVE, common.OFPACPT_ROLE_STATUS_MASTER),
                    init = packvalue(common.OFPACPT_ROLE_STATUS_SLAVE, 'type'),
                    extend = {"mask": ofp_controller_role_reason_bitwise}
                )
            namespace['nx_async_config2_prop_reasons_role_status'] = nx_async_config2_prop_reasons_role_status
            
            nx_async_config2_prop_reasons_table = \
                nstruct(
                    name = 'nx_async_config2_prop_reasons_table',
                    base = nx_async_config2_prop_reasons,
                    criteria = lambda x: x.type in (common.OFPACPT_TABLE_STATUS_SLAVE, common.OFPACPT_TABLE_STATUS_MASTER),
                    classifyby = (common.OFPACPT_TABLE_STATUS_SLAVE, common.OFPACPT_TABLE_STATUS_MASTER),
                    init = packvalue(common.OFPACPT_TABLE_STATUS_SLAVE, 'type'),
                    extend = {"mask": ofp_table_reason_bitwise}
                )
            namespace['nx_async_config2_prop_reasons_table'] = nx_async_config2_prop_reasons_table
        
            nx_async_config2_prop_reasons_requestforward = \
                nstruct(
                    name = 'nx_async_config2_prop_reasons_requestforward',
                    base = nx_async_config2_prop_reasons,
                    criteria = lambda x: x.type in (common.OFPACPT_REQUESTFORWARD_SLAVE, common.OFPACPT_REQUESTFORWARD_MASTER),
                    classifyby = (common.OFPACPT_REQUESTFORWARD_SLAVE, common.OFPACPT_REQUESTFORWARD_MASTER),
                    init = packvalue(common.OFPACPT_REQUESTFORWARD_SLAVE, 'type'),
                    extend = {"mask": ofp_requestforward_reason_bitwise}
                )
            namespace['nx_async_config2_prop_reasons_requestforward'] = nx_async_config2_prop_reasons_requestforward
        
            # /* Experimenter async config  property */
            nx_async_config2_prop_experimenter = \
                nstruct(
                    (common.experimenter_ids, 'experimenter'),       # /* Experimenter ID which takes the same
                                                    #    form as in struct
                                                    #    ofp_experimenter_header. */
                    (uint32, 'exp_type'),           # /* Experimenter defined. */
                    # /* Followed by:
                    #  *   - Exactly (length - 12) bytes containing the experimenter data, then
                    #  *   - Exactly (length + 7)/8*8 - (length) (between 0 and 7)
                    #  *     bytes of all-zero bytes */
                    name = 'nx_async_config2_prop_experimenter',
                    base = nx_async_config2_prop,
                    criteria = lambda x: x.type in (common.OFPTFPT_EXPERIMENTER_SLAVE, common.OFPTFPT_EXPERIMENTER_MASTER),
                    classifyby = (common.OFPTFPT_EXPERIMENTER_SLAVE, common.OFPTFPT_EXPERIMENTER_MASTER),
                    init = packvalue(common.OFPTFPT_EXPERIMENTER_SLAVE, 'type')
                )
            namespace['nx_async_config2_prop_experimenter'] = nx_async_config2_prop_experimenter
            
        # /* Asynchronous message configuration. */
        nx_async_config2 = \
            nstruct(
                # /* Async config Property list - 0 or more */
                (nx_async_config2_prop[0], 'properties'),
                name = 'nx_async_config2',
                base = nicira_header,
                classifyby = (NXT_SET_ASYNC_CONFIG2,),
                criteria = lambda x: getattr(x, msg_subtype) == NXT_SET_ASYNC_CONFIG2,
                init = packvalue(NXT_SET_ASYNC_CONFIG2, msg_subtype)
            )
        namespace['nx_async_config2'] = nx_async_config2

        '''
        /* Nicira vendor flow actions. */
        '''
        #=======================================================================
        # /* Action structures for NXAST_RESUBMIT, NXAST_RESUBMIT_TABLE, and
        #  * NXAST_RESUBMIT_TABLE_CT.
        #  *
        #  * These actions search one of the switch's flow tables:
        #  *
        #  *    - For NXAST_RESUBMIT_TABLE and NXAST_RESUBMIT_TABLE_CT, if the
        #  *      'table' member is not 255, then it specifies the table to search.
        #  *
        #  *    - Otherwise (for NXAST_RESUBMIT_TABLE or NXAST_RESUBMIT_TABLE_CT with a
        #  *      'table' of 255, or for NXAST_RESUBMIT regardless of 'table'), it
        #  *      searches the current flow table, that is, the OpenFlow flow table that
        #  *      contains the flow from which this action was obtained.  If this action
        #  *      did not come from a flow table (e.g. it came from an OFPT_PACKET_OUT
        #  *      message), then table 0 is the current table.
        #  *
        #  * The flow table lookup uses a flow that may be slightly modified from the
        #  * original lookup:
        #  *
        #  *    - For NXAST_RESUBMIT, the 'in_port' member of struct nx_action_resubmit
        #  *      is used as the flow's in_port.
        #  *
        #  *    - For NXAST_RESUBMIT_TABLE and NXAST_RESUBMIT_TABLE_CT, if the 'in_port'
        #  *      member is not OFPP_IN_PORT, then its value is used as the flow's
        #  *      in_port.  Otherwise, the original in_port is used.
        #  *
        #  *    - For NXAST_RESUBMIT_TABLE_CT the Conntrack 5-tuple fields are used as
        #  *      the packets IP header fields during the lookup.
        #  *
        #  *    - If actions that modify the flow (e.g. OFPAT_SET_VLAN_VID) precede the
        #  *      resubmit action, then the flow is updated with the new values.
        #  *
        #  * Following the lookup, the original in_port is restored.
        #  *
        #  * If the modified flow matched in the flow table, then the corresponding
        #  * actions are executed.  Afterward, actions following the resubmit in the
        #  * original set of actions, if any, are executed; any changes made to the
        #  * packet (e.g. changes to VLAN) by secondary actions persist when those
        #  * actions are executed, although the original in_port is restored.
        #  *
        #  * Resubmit actions may be used any number of times within a set of actions.
        #  *
        #  * Resubmit actions may nest.  To prevent infinite loops and excessive resource
        #  * use, the implementation may limit nesting depth and the total number of
        #  * resubmits:
        #  *
        #  *    - Open vSwitch 1.0.1 and earlier did not support recursion.
        #  *
        #  *    - Open vSwitch 1.0.2 and 1.0.3 limited recursion to 8 levels.
        #  *
        #  *    - Open vSwitch 1.1 and 1.2 limited recursion to 16 levels.
        #  *
        #  *    - Open vSwitch 1.2 through 1.8 limited recursion to 32 levels.
        #  *
        #  *    - Open vSwitch 1.9 through 2.0 limited recursion to 64 levels.
        #  *
        #  *    - Open vSwitch 2.1 through 2.5 limited recursion to 64 levels and impose
        #  *      a total limit of 4,096 resubmits per flow translation (earlier versions
        #  *      did not impose any total limit).
        #  *
        #  * NXAST_RESUBMIT ignores 'table' and 'pad'.  NXAST_RESUBMIT_TABLE and
        #  * NXAST_RESUBMIT_TABLE_CT require 'pad' to be all-bits-zero.
        #  *
        #  * Open vSwitch 1.0.1 and earlier did not support recursion.  Open vSwitch
        #  * before 1.2.90 did not support NXAST_RESUBMIT_TABLE.  Open vSwitch before
        #  * 2.8.0 did not support NXAST_RESUBMIT_TABLE_CT.
        #  */
        #=======================================================================
        nx_action_resubmit = nstruct(
            (nx_port_no, 'in_port'),        # /* New in_port for checking flow table. */
            (uint8, 'table'),               # /* NXAST_RESUBMIT_TABLE: table to use. */
            (uint8[3],),
            base = nx_action,
            criteria = lambda x: getattr(x, action_subtype) in \
                                    (NXAST_RESUBMIT_TABLE, NXAST_RESUBMIT, NXAST_RESUBMIT_TABLE_CT),
            classifyby = (NXAST_RESUBMIT_TABLE, NXAST_RESUBMIT, NXAST_RESUBMIT_TABLE_CT),
            name = 'nx_action_resubmit',
            init = packvalue(NXAST_RESUBMIT_TABLE, action_subtype)
        )
        namespace['nx_action_resubmit'] = nx_action_resubmit
        '''
        /* Action structure for NXAST_SET_TUNNEL.
         *
         * Sets the encapsulating tunnel ID to a 32-bit value.  The most-significant 32
         * bits of the tunnel ID are set to 0. */
        '''
        nx_action_set_tunnel = nstruct(
            (uint8[2],),
            (uint32, 'tun_id'),         #         /* Tunnel ID. */
            name = 'nx_action_set_tunnel',
            base = nx_action,
            classifyby = (NXAST_SET_TUNNEL,),
            criteria = lambda x: getattr(x, action_subtype) == NXAST_SET_TUNNEL,
            init = packvalue(NXAST_SET_TUNNEL, action_subtype)
        )
        namespace['nx_action_set_tunnel'] = nx_action_set_tunnel
        '''
        /* Action structure for NXAST_SET_TUNNEL64.
         *
         * Sets the encapsulating tunnel ID to a 64-bit value. */
        '''
        nx_action_set_tunnel64 = nstruct(
            (uint8[6],),
            (uint64, 'tun_id'),          #       /* Tunnel ID. */
            name = 'nx_action_set_tunnel64',
            base = nx_action,
            classifyby = (NXAST_SET_TUNNEL64,),
            criteria = lambda x: getattr(x, action_subtype) == NXAST_SET_TUNNEL64,
            init = packvalue(NXAST_SET_TUNNEL64, action_subtype)
        )
        namespace['nx_action_set_tunnel64'] = nx_action_set_tunnel64
        '''
        /* Action structure for NXAST_SET_QUEUE.
         *
         * Set the queue that should be used when packets are output.  This is similar
         * to the OpenFlow OFPAT_ENQUEUE action, but does not take the output port as
         * an argument.  This allows the queue to be defined before the port is
         * known. */
        '''
        nx_action_set_queue = nstruct(
            (uint8[2],),
            (uint32, 'queue_id'),             #    /* Where to enqueue packets. */
            name = 'nx_action_set_queue',
            base = nx_action,
            classifyby = (NXAST_SET_QUEUE,),
            criteria = lambda x: getattr(x, action_subtype) == NXAST_SET_QUEUE,
            init = packvalue(NXAST_SET_QUEUE, action_subtype)
        )
        namespace['nx_action_set_queue'] = nx_action_set_queue
    
        '''
        /* Action structure for NXAST_POP_QUEUE.
         *
         * Restores the queue to the value it was before any NXAST_SET_QUEUE actions
         * were used.  Only the original queue can be restored this way; no stack is
         * maintained. */
        '''
        nx_action_pop_queue = nstruct(
            (uint8[6],),
            name = 'nx_action_pop_queue',
            base = nx_action,
            classifyby = (NXAST_POP_QUEUE,),
            criteria = lambda x: getattr(x, action_subtype) == NXAST_POP_QUEUE,
            init = packvalue(NXAST_POP_QUEUE, action_subtype)
        )
        namespace['nx_action_pop_queue'] = nx_action_pop_queue
    
        '''
        /* Action structure for NXAST_REG_MOVE.
         *
         * Copies src[src_ofs:src_ofs+n_bits] to dst[dst_ofs:dst_ofs+n_bits], where
         * a[b:c] denotes the bits within 'a' numbered 'b' through 'c' (not including
         * bit 'c').  Bit numbering starts at 0 for the least-significant bit, 1 for
         * the next most significant bit, and so on.
         *
         * 'src' and 'dst' are nxm_header values with nxm_hasmask=0.  (It doesn't make
         * sense to use nxm_hasmask=1 because the action does not do any kind of
         * matching; it uses the actual value of a field.)
         *
         * The following nxm_header values are potentially acceptable as 'src':
         *
         *   - NXM_OF_IN_PORT
         *   - NXM_OF_ETH_DST
         *   - NXM_OF_ETH_SRC
         *   - NXM_OF_ETH_TYPE
         *   - NXM_OF_VLAN_TCI
         *   - NXM_OF_IP_TOS
         *   - NXM_OF_IP_PROTO
         *   - NXM_OF_IP_SRC
         *   - NXM_OF_IP_DST
         *   - NXM_OF_TCP_SRC
         *   - NXM_OF_TCP_DST
         *   - NXM_OF_UDP_SRC
         *   - NXM_OF_UDP_DST
         *   - NXM_OF_ICMP_TYPE
         *   - NXM_OF_ICMP_CODE
         *   - NXM_OF_ARP_OP
         *   - NXM_OF_ARP_SPA
         *   - NXM_OF_ARP_TPA
         *   - NXM_NX_TUN_ID
         *   - NXM_NX_ARP_SHA
         *   - NXM_NX_ARP_THA
         *   - NXM_NX_ICMPV6_TYPE
         *   - NXM_NX_ICMPV6_CODE
         *   - NXM_NX_ND_SLL
         *   - NXM_NX_ND_TLL
         *   - NXM_NX_REG(idx) for idx in the switch's accepted range.
         *   - NXM_NX_PKT_MARK
         *   - NXM_NX_TUN_IPV4_SRC
         *   - NXM_NX_TUN_IPV4_DST
         *
         * The following nxm_header values are potentially acceptable as 'dst':
         *
         *   - NXM_OF_ETH_DST
         *   - NXM_OF_ETH_SRC
         *   - NXM_OF_IP_TOS
         *   - NXM_OF_IP_SRC
         *   - NXM_OF_IP_DST
         *   - NXM_OF_TCP_SRC
         *   - NXM_OF_TCP_DST
         *   - NXM_OF_UDP_SRC
         *   - NXM_OF_UDP_DST
         *   - NXM_NX_ARP_SHA
         *   - NXM_NX_ARP_THA
         *   - NXM_OF_ARP_OP
         *   - NXM_OF_ARP_SPA
         *   - NXM_OF_ARP_TPA
         *     Modifying any of the above fields changes the corresponding packet
         *     header.
         *
         *   - NXM_OF_IN_PORT
         *
         *   - NXM_NX_REG(idx) for idx in the switch's accepted range.
         *
         *   - NXM_NX_PKT_MARK
         *
         *   - NXM_OF_VLAN_TCI.  Modifying this field's value has side effects on the
         *     packet's 802.1Q header.  Setting a value with CFI=0 removes the 802.1Q
         *     header (if any), ignoring the other bits.  Setting a value with CFI=1
         *     adds or modifies the 802.1Q header appropriately, setting the TCI field
         *     to the field's new value (with the CFI bit masked out).
         *
         *   - NXM_NX_TUN_ID, NXM_NX_TUN_IPV4_SRC, NXM_NX_TUN_IPV4_DST.  Modifying
         *     any of these values modifies the corresponding tunnel header field used
         *     for the packet's next tunnel encapsulation, if allowed by the
         *     configuration of the output tunnel port.
         *
         * A given nxm_header value may be used as 'src' or 'dst' only on a flow whose
         * nx_match satisfies its prerequisites.  For example, NXM_OF_IP_TOS may be
         * used only if the flow's nx_match includes an nxm_entry that specifies
         * nxm_type=NXM_OF_ETH_TYPE, nxm_hasmask=0, and nxm_value=0x0800.
         *
         * The switch will reject actions for which src_ofs+n_bits is greater than the
         * width of 'src' or dst_ofs+n_bits is greater than the width of 'dst' with
         * error type OFPET_BAD_ACTION, code OFPBAC_BAD_ARGUMENT.
         *
         * This action behaves properly when 'src' overlaps with 'dst', that is, it
         * behaves as if 'src' were copied out to a temporary buffer, then the
         * temporary buffer copied to 'dst'.
         */
         '''
        nx_action_reg_move = nstruct(
            (uint16, 'n_bits'),         #       /* Number of bits. */
            (uint16, 'src_ofs'),        #       /* Starting bit offset in source. */
            (uint16, 'dst_ofs'),        #       /* Starting bit offset in destination. */
            *_create_oxm_header_only_fields('src'),
                                        #       /* OXM/NXM header for source field (4 or 8 bytes) */
            *_create_oxm_header_only_fields('dst'),
                                        #       /* OXM/NXM header for destination field (4 or 8 bytes) */
            name = 'nx_action_reg_move',
            base = nx_action,
            classifyby = (NXAST_REG_MOVE,),
            criteria = lambda x: getattr(x, action_subtype) == NXAST_REG_MOVE,
            init = packvalue(NXAST_REG_MOVE, action_subtype),
            formatter = _createdesc(lambda x:'move:%s[%d..%d]->%s[%d..%d]' % (x['src'], x['src_ofs'], x['src_ofs'] + x['n_bits'] - 1, x['dst'], x['dst_ofs'], x['dst_ofs'] + x['n_bits'] - 1))
        )
        namespace['nx_action_reg_move'] = nx_action_reg_move
        '''
        /* Action structure for NXAST_REG_LOAD.
         *
         * Copies value[0:n_bits] to dst[ofs:ofs+n_bits], where a[b:c] denotes the bits
         * within 'a' numbered 'b' through 'c' (not including bit 'c').  Bit numbering
         * starts at 0 for the least-significant bit, 1 for the next most significant
         * bit, and so on.
         *
         * 'dst' is an nxm_header with nxm_hasmask=0.  See the documentation for
         * NXAST_REG_MOVE, above, for the permitted fields and for the side effects of
         * loading them.
         *
         * The 'ofs' and 'n_bits' fields are combined into a single 'ofs_nbits' field
         * to avoid enlarging the structure by another 8 bytes.  To allow 'n_bits' to
         * take a value between 1 and 64 (inclusive) while taking up only 6 bits, it is
         * also stored as one less than its true value:
         *
         *  15                           6 5                0
         * +------------------------------+------------------+
         * |              ofs             |    n_bits - 1    |
         * +------------------------------+------------------+
         *
         * The switch will reject actions for which ofs+n_bits is greater than the
         * width of 'dst', or in which any bits in 'value' with value 2**n_bits or
         * greater are set to 1, with error type OFPET_BAD_ACTION, code
         * OFPBAC_BAD_ARGUMENT.
         */
        '''
        nx_action_reg_load = nstruct(
            (ofs_nbits_type, 'ofs_nbits'),   #  /* (ofs << 6) | (n_bits - 1). */
            (all_nxm_header, 'dst'),         #  /* Destination register. */
            (uint64, 'value'),               #  /* Immediate value. */
            name = 'nx_action_reg_load',
            base = nx_action,
            classifyby = (NXAST_REG_LOAD,),
            criteria = lambda x: getattr(x, action_subtype) == NXAST_REG_LOAD,
            init = packvalue(NXAST_REG_LOAD, action_subtype),
            formatter = _createdesc(lambda x: 'load:0x%x->%s%s' % (x['value'], x['dst'], x['ofs_nbits']))
        )
        namespace['nx_action_reg_load'] = nx_action_reg_load
        
        # /* The NXAST_REG_LOAD2 action structure is "struct ext_action_header",
        #  * followed by:
        #  *
        #  * - An NXM/OXM header, value, and optionally a mask.
        #  * - Enough 0-bytes to pad out to a multiple of 64 bits.
        #  *
        #  * The "pad" member is the beginning of the above. */
        
        nx_action_reg_load2 = \
            nstruct(
                (uint8[6],),
                (nx_match, 'field'),
                name = 'nx_action_reg_load2',
                base = nx_action,
                classifyby = (NXAST_REG_LOAD2,),
                criteria = lambda x: getattr(x, action_subtype) == NXAST_REG_LOAD2,
                init = packvalue(NXAST_REG_LOAD2, action_subtype)
            )
        namespace['nx_action_reg_load2'] = nx_action_reg_load2
        
        '''
        /* Action structure for NXAST_STACK_PUSH and NXAST_STACK_POP.
         *
         * Pushes (or pops) field[offset: offset + n_bits] to (or from)
         * top of the stack.
         */
        '''
        nx_action_stack = nstruct(
            (uint16, 'offset'),          #      /* Bit offset into the field. */
            *_create_oxm_header_only_fields('field'),
                                         #      /* The field used for push or pop. */
            (uint16, 'n_bits'),          #      /* (n_bits + 1) bits of the field. */
            (uint8[6],),                 #      /* Reserved, must be zero. */
            name = 'nx_action_stack',
            base = nx_action,
            classifyby = (NXAST_STACK_PUSH, NXAST_STACK_POP),
            criteria = lambda x: getattr(x, action_subtype) == NXAST_STACK_PUSH or getattr(x, action_subtype) == NXAST_STACK_POP,
            init = packvalue(NXAST_STACK_PUSH, action_subtype),
            formatter = _createdesc(lambda x: '%s:%s[%d..%d]' % ('push' if x[action_subtype] == 'NXAST_STACK_PUSH' else 'pop', x['field'], x['offset'], (x['offset'] + x['n_bits'] - 1)))
        )
        namespace['nx_action_stack'] = nx_action_stack
        
        '''
        /* Action structure for NXAST_NOTE.
         *
         * This action has no effect.  It is variable length.  The switch does not
         * attempt to interpret the user-defined 'note' data in any way.  A controller
         * can use this action to attach arbitrary metadata to a flow.
         *
         * This action might go away in the future.
         */
        '''
        nx_action_note = nstruct(
            (varchr, 'note'),
            name = 'nx_action_note',
            base = nx_action,
            classifyby = (NXAST_NOTE,),
            criteria = lambda x: getattr(x, action_subtype) == NXAST_NOTE,
            init = packvalue(NXAST_NOTE, action_subtype)
        )
        namespace['nx_action_note'] = nx_action_note
        
        '''
        /* Action structure for NXAST_MULTIPATH.
         *
         * This action performs the following steps in sequence:
         *
         *    1. Hashes the fields designated by 'fields', one of NX_HASH_FIELDS_*.
         *       Refer to the definition of "enum nx_mp_fields" for details.
         *
         *       The 'basis' value is used as a universal hash parameter, that is,
         *       different values of 'basis' yield different hash functions.  The
         *       particular universal hash function used is implementation-defined.
         *
         *       The hashed fields' values are drawn from the current state of the
         *       flow, including all modifications that have been made by actions up to
         *       this point.
         *
         *    2. Applies the multipath link choice algorithm specified by 'algorithm',
         *       one of NX_MP_ALG_*.  Refer to the definition of "enum nx_mp_algorithm"
         *       for details.
         *
         *       The output of the algorithm is 'link', an unsigned integer less than
         *       or equal to 'max_link'.
         *
         *       Some algorithms use 'arg' as an additional argument.
         *
         *    3. Stores 'link' in dst[ofs:ofs+n_bits].  The format and semantics of
         *       'dst' and 'ofs_nbits' are similar to those for the NXAST_REG_LOAD
         *       action.
         *
         * The switch will reject actions that have an unknown 'fields', or an unknown
         * 'algorithm', or in which ofs+n_bits is greater than the width of 'dst', or
         * in which 'max_link' is greater than or equal to 2**n_bits, with error type
         * OFPET_BAD_ACTION, code OFPBAC_BAD_ARGUMENT.
         */
        '''
        nx_action_multipath = nstruct(
        
            #/* What fields to hash and how. */
            (nx_hash_fields, 'fields'),       #     /* One of NX_HASH_FIELDS_*. */
            (uint16, 'basis'),                #     /* Universal hash parameter. */
            (uint16,),
        
            #/* Multipath link choice algorithm to apply to hash value. */
            (nx_mp_algorithm, 'algorithm'),   #     /* One of NX_MP_ALG_*. */
            (uint16, 'max_link'),             #     /* Number of output links, minus 1. */
            (uint32, 'arg'),                  #     /* Algorithm-specific argument. */
            (uint16,),
        
            # /* Where to store the result. */
            (ofs_nbits_type, 'ofs_nbits'),        #     /* (ofs << 6) | (n_bits - 1). */
            (all_nxm_header, 'dst'),              #     /* Destination. */
            name = 'nx_action_multipath',
            base = nx_action,
            classifyby = (NXAST_MULTIPATH,),
            criteria = lambda x: getattr(x, action_subtype) == NXAST_MULTIPATH,
            init = packvalue(NXAST_MULTIPATH, action_subtype),
            formatter = _createdesc(lambda x: 'multipath(%s,%d,%s,%d,%d,%s%s)' % (x['fields'], x['basis'], x['algorithm'],x['max_link'] + 1, x['arg'], x['dst'], x['ofs_nbits']))
        )
        namespace['nx_action_multipath'] = nx_action_multipath
    
        #=======================================================================
        # /* Action structure for NXAST_LEARN and NXAST_LEARN2.
        #  *
        #  * This action adds or modifies a flow in an OpenFlow table, similar to
        #  * OFPT_FLOW_MOD with OFPFC_MODIFY_STRICT as 'command'.  The new flow has the
        #  * specified idle timeout, hard timeout, priority, cookie, and flags.  The new
        #  * flow's match criteria and actions are built by applying each of the series
        #  * of flow_mod_spec elements included as part of the action.
        #  *
        #  * A flow_mod_spec starts with a 16-bit header.  A header that is all-bits-0 is
        #  * a no-op used for padding the action as a whole to a multiple of 8 bytes in
        #  * length.  Otherwise, the flow_mod_spec can be thought of as copying 'n_bits'
        #  * bits from a source to a destination.  In this case, the header contains
        #  * multiple fields:
        #  *
        #  *  15  14  13 12  11 10                              0
        #  * +------+---+------+---------------------------------+
        #  * |   0  |src|  dst |             n_bits              |
        #  * +------+---+------+---------------------------------+
        #  *
        #  * The meaning and format of a flow_mod_spec depends on 'src' and 'dst'.  The
        #  * following table summarizes the meaning of each possible combination.
        #  * Details follow the table:
        #  *
        #  *   src dst  meaning
        #  *   --- ---  ----------------------------------------------------------
        #  *    0   0   Add match criteria based on value in a field.
        #  *    1   0   Add match criteria based on an immediate value.
        #  *    0   1   Add NXAST_REG_LOAD action to copy field into a different field.
        #  *    1   1   Add NXAST_REG_LOAD action to load immediate value into a field.
        #  *    0   2   Add OFPAT_OUTPUT action to output to port from specified field.
        #  *   All other combinations are undefined and not allowed.
        #  *
        #  * The flow_mod_spec header is followed by a source specification and a
        #  * destination specification.  The format and meaning of the source
        #  * specification depends on 'src':
        #  *
        #  *   - If 'src' is 0, the source bits are taken from a field in the flow to
        #  *     which this action is attached.  (This should be a wildcarded field.  If
        #  *     its value is fully specified then the source bits being copied have
        #  *     constant values.)
        #  *
        #  *     The source specification is an ovs_be32 'field' and an ovs_be16 'ofs'.
        #  *     'field' is an nxm_header with nxm_hasmask=0, and 'ofs' the starting bit
        #  *     offset within that field.  The source bits are field[ofs:ofs+n_bits-1].
        #  *     'field' and 'ofs' are subject to the same restrictions as the source
        #  *     field in NXAST_REG_MOVE.
        #  *
        #  *   - If 'src' is 1, the source bits are a constant value.  The source
        #  *     specification is (n_bits+15)/16*2 bytes long.  Taking those bytes as a
        #  *     number in network order, the source bits are the 'n_bits'
        #  *     least-significant bits.  The switch will report an error if other bits
        #  *     in the constant are nonzero.
        #  *
        #  * The flow_mod_spec destination specification, for 'dst' of 0 or 1, is an
        #  * ovs_be32 'field' and an ovs_be16 'ofs'.  'field' is an nxm_header with
        #  * nxm_hasmask=0 and 'ofs' is a starting bit offset within that field.  The
        #  * meaning of the flow_mod_spec depends on 'dst':
        #  *
        #  *   - If 'dst' is 0, the flow_mod_spec specifies match criteria for the new
        #  *     flow.  The new flow matches only if bits field[ofs:ofs+n_bits-1] in a
        #  *     packet equal the source bits.  'field' may be any nxm_header with
        #  *     nxm_hasmask=0 that is allowed in NXT_FLOW_MOD.
        #  *
        #  *     Order is significant.  Earlier flow_mod_specs must satisfy any
        #  *     prerequisites for matching fields specified later, by copying constant
        #  *     values into prerequisite fields.
        #  *
        #  *     The switch will reject flow_mod_specs that do not satisfy NXM masking
        #  *     restrictions.
        #  *
        #  *   - If 'dst' is 1, the flow_mod_spec specifies an NXAST_REG_LOAD action for
        #  *     the new flow.  The new flow copies the source bits into
        #  *     field[ofs:ofs+n_bits-1].  Actions are executed in the same order as the
        #  *     flow_mod_specs.
        #  *
        #  *     A single NXAST_REG_LOAD action writes no more than 64 bits, so n_bits
        #  *     greater than 64 yields multiple NXAST_REG_LOAD actions.
        #  *
        #  * The flow_mod_spec destination spec for 'dst' of 2 (when 'src' is 0) is
        #  * empty.  It has the following meaning:
        #  *
        #  *   - The flow_mod_spec specifies an OFPAT_OUTPUT action for the new flow.
        #  *     The new flow outputs to the OpenFlow port specified by the source field.
        #  *     Of the special output ports with value OFPP_MAX or larger, OFPP_IN_PORT,
        #  *     OFPP_FLOOD, OFPP_LOCAL, and OFPP_ALL are supported.  Other special ports
        #  *     may not be used.
        #  *
        #  * Resource Management
        #  * -------------------
        #  *
        #  * A switch has a finite amount of flow table space available for learning.
        #  * When this space is exhausted, no new learning table entries will be learned
        #  * until some existing flow table entries expire.  The controller should be
        #  * prepared to handle this by flooding (which can be implemented as a
        #  * low-priority flow).
        #  *
        #  * If a learned flow matches a single TCP stream with a relatively long
        #  * timeout, one may make the best of resource constraints by setting
        #  * 'fin_idle_timeout' or 'fin_hard_timeout' (both measured in seconds), or
        #  * both, to shorter timeouts.  When either of these is specified as a nonzero
        #  * value, OVS adds a NXAST_FIN_TIMEOUT action, with the specified timeouts, to
        #  * the learned flow.
        #  *
        #  * Examples
        #  * --------
        #  *
        #  * The following examples give a prose description of the flow_mod_specs along
        #  * with informal notation for how those would be represented and a hex dump of
        #  * the bytes that would be required.
        #  *
        #  * These examples could work with various nx_action_learn parameters.  Typical
        #  * values would be idle_timeout=OFP_FLOW_PERMANENT, hard_timeout=60,
        #  * priority=OFP_DEFAULT_PRIORITY, flags=0, table_id=10.
        #  *
        #  * 1. Learn input port based on the source MAC, with lookup into
        #  *    NXM_NX_REG1[16:31] by resubmit to in_port=99:
        #  *
        #  *    Match on in_port=99:
        #  *       ovs_be16(src=1, dst=0, n_bits=16),               20 10
        #  *       ovs_be16(99),                                    00 63
        #  *       ovs_be32(NXM_OF_IN_PORT), ovs_be16(0)            00 00 00 02 00 00
        #  *
        #  *    Match Ethernet destination on Ethernet source from packet:
        #  *       ovs_be16(src=0, dst=0, n_bits=48),               00 30
        #  *       ovs_be32(NXM_OF_ETH_SRC), ovs_be16(0)            00 00 04 06 00 00
        #  *       ovs_be32(NXM_OF_ETH_DST), ovs_be16(0)            00 00 02 06 00 00
        #  *
        #  *    Set NXM_NX_REG1[16:31] to the packet's input port:
        #  *       ovs_be16(src=0, dst=1, n_bits=16),               08 10
        #  *       ovs_be32(NXM_OF_IN_PORT), ovs_be16(0)            00 00 00 02 00 00
        #  *       ovs_be32(NXM_NX_REG1), ovs_be16(16)              00 01 02 04 00 10
        #  *
        #  *    Given a packet that arrived on port A with Ethernet source address B,
        #  *    this would set up the flow "in_port=99, dl_dst=B,
        #  *    actions=load:A->NXM_NX_REG1[16..31]".
        #  *
        #  *    In syntax accepted by ovs-ofctl, this action is: learn(in_port=99,
        #  *    NXM_OF_ETH_DST[]=NXM_OF_ETH_SRC[],
        #  *    load:NXM_OF_IN_PORT[]->NXM_NX_REG1[16..31])
        #  *
        #  * 2. Output to input port based on the source MAC and VLAN VID, with lookup
        #  *    into NXM_NX_REG1[16:31]:
        #  *
        #  *    Match on same VLAN ID as packet:
        #  *       ovs_be16(src=0, dst=0, n_bits=12),               00 0c
        #  *       ovs_be32(NXM_OF_VLAN_TCI), ovs_be16(0)           00 00 08 02 00 00
        #  *       ovs_be32(NXM_OF_VLAN_TCI), ovs_be16(0)           00 00 08 02 00 00
        #  *
        #  *    Match Ethernet destination on Ethernet source from packet:
        #  *       ovs_be16(src=0, dst=0, n_bits=48),               00 30
        #  *       ovs_be32(NXM_OF_ETH_SRC), ovs_be16(0)            00 00 04 06 00 00
        #  *       ovs_be32(NXM_OF_ETH_DST), ovs_be16(0)            00 00 02 06 00 00
        #  *
        #  *    Output to the packet's input port:
        #  *       ovs_be16(src=0, dst=2, n_bits=16),               10 10
        #  *       ovs_be32(NXM_OF_IN_PORT), ovs_be16(0)            00 00 00 02 00 00
        #  *
        #  *    Given a packet that arrived on port A with Ethernet source address B in
        #  *    VLAN C, this would set up the flow "dl_dst=B, vlan_vid=C,
        #  *    actions=output:A".
        #  *
        #  *    In syntax accepted by ovs-ofctl, this action is:
        #  *    learn(NXM_OF_VLAN_TCI[0..11], NXM_OF_ETH_DST[]=NXM_OF_ETH_SRC[],
        #  *    output:NXM_OF_IN_PORT[])
        #  *
        #  * 3. Here's a recipe for a very simple-minded MAC learning switch.  It uses a
        #  *    10-second MAC expiration time to make it easier to see what's going on
        #  *
        #  *      ovs-vsctl del-controller br0
        #  *      ovs-ofctl del-flows br0
        #  *      ovs-ofctl add-flow br0 "table=0 actions=learn(table=1, \
        #           hard_timeout=10, NXM_OF_VLAN_TCI[0..11],             \
        #           NXM_OF_ETH_DST[]=NXM_OF_ETH_SRC[],                   \
        #           output:NXM_OF_IN_PORT[]), resubmit(,1)"
        #  *      ovs-ofctl add-flow br0 "table=1 priority=0 actions=flood"
        #  *
        #  *    You can then dump the MAC learning table with:
        #  *
        #  *      ovs-ofctl dump-flows br0 table=1
        #  *
        #  * Usage Advice
        #  * ------------
        #  *
        #  * For best performance, segregate learned flows into a table that is not used
        #  * for any other flows except possibly for a lowest-priority "catch-all" flow
        #  * (a flow with no match criteria).  If different learning actions specify
        #  * different match criteria, use different tables for the learned flows.
        #  *
        #  * The meaning of 'hard_timeout' and 'idle_timeout' can be counterintuitive.
        #  * These timeouts apply to the flow that is added, which means that a flow with
        #  * an idle timeout will expire when no traffic has been sent *to* the learned
        #  * address.  This is not usually the intent in MAC learning; instead, we want
        #  * the MAC learn entry to expire when no traffic has been sent *from* the
        #  * learned address.  Use a hard timeout for that.
        #  *
        #  *
        #  * Visibility of Changes
        #  * ---------------------
        #  *
        #  * Prior to Open vSwitch 2.4, any changes made by a "learn" action in a given
        #  * flow translation are visible to flow table lookups made later in the flow
        #  * translation.  This means that, in the example above, a MAC learned by the
        #  * learn action in table 0 would be found in table 1 (if the packet being
        #  * processed had the same source and destination MAC address).
        #  *
        #  * In Open vSwitch 2.4 and later, changes to a flow table (whether to add or
        #  * modify a flow) by a "learn" action are visible only for later flow
        #  * translations, not for later lookups within the same flow translation.  In
        #  * the MAC learning example, a MAC learned by the learn action in table 0 would
        #  * not be found in table 1 if the flow translation would resubmit to table 1
        #  * after the processing of the learn action, meaning that if this MAC had not
        #  * been learned before then the packet would be flooded. */
        #=======================================================================
        _nx_flow_mod_spec_src = nstruct(
            name = '_nx_flow_mod_spec_src',
            padding = 1,
            size = lambda x: (((NX_FLOWMODSPEC_NBITS(x.header) + 15) // 16 * 2) if NX_FLOWMODSPEC_SRC(x.header) else 6)
        )
        
        _nx_flow_mod_spec_dst = nstruct(
            name = '_nx_flow_mod_spec_dst',
            padding = 1,
            size = lambda x: 0 if NX_FLOWMODSPEC_DST(x.header) == NX_LEARN_DST_OUTPUT else 6
        )
        
        _nx_flow_mod_spec_src_value = nstruct(
            (raw, 'value'),
            name = '_nx_flow_mod_spec_src_value',
            base = _nx_flow_mod_spec_src,
            criteria = lambda x: NX_FLOWMODSPEC_SRC(x.header)
        )
        
        _nx_flow_mod_spec_src_field = nstruct(
            (all_nxm_header, 'src'),
            (uint16, 'src_ofs'),
            name = '_nx_flow_mod_spec_src_field',
            base = _nx_flow_mod_spec_src,
            criteria = lambda x: not NX_FLOWMODSPEC_SRC(x.header)
        )
        
        _nx_flow_mod_spec_dst_field = nstruct(
            (all_nxm_header, 'dst'),
            (uint16, 'dst_ofs'),
            name = '_nx_flow_mod_spec_dst_field',
            base = _nx_flow_mod_spec_dst,
            criteria = lambda x: NX_FLOWMODSPEC_DST(x.header) == NX_LEARN_DST_MATCH or NX_FLOWMODSPEC_DST(x.header) == NX_LEARN_DST_LOAD
        )
        
        _nx_flow_mod_spec_dst_output = nstruct(
            name = '_nx_flow_mod_spec_dst_output',
            base = _nx_flow_mod_spec_dst,
            criteria = lambda x: NX_FLOWMODSPEC_DST(x.header) == NX_LEARN_DST_OUTPUT
        )
        
        def _create_field(dst, ofs):
            if NXM_HASMASK(dst):
                raise ValueError('Must specify a nxm_header without mask')
            return _nx_flow_mod_spec_dst_field.new(dst = dst, dst_ofs = ofs)._tobytes()
        
        def _create_header(src, dst, n_bits):
            return uint16.tobytes((src & NX_LEARN_SRC_MASK) | (dst & NX_LEARN_DST_MASK) | (n_bits & NX_LEARN_N_BITS_MASK))


        def _nx_flow_mod_spec_formatter(x):
            if NX_FLOWMODSPEC_SRC(x['header']):
                srcdesc = '0x' + ''.join('%02x' % (c,) for c in bytearray(x['value']))
            else:
                srcdesc = '%s[%d..%d]' % (x['src'], x['src_ofs'], x['src_ofs'] + NX_FLOWMODSPEC_NBITS(x['header']) - 1)
            dstv = NX_FLOWMODSPEC_DST(x['header'])
            if dstv != NX_LEARN_DST_OUTPUT:
                dstdesc = '%s[%d..%d]' % (x['dst'], x['dst_ofs'], x['dst_ofs'] + NX_FLOWMODSPEC_NBITS(x['header']) - 1)
            if dstv == NX_LEARN_DST_MATCH:
                x['_desc'] = '%s=%s' % (dstdesc, srcdesc)
            elif dstv == NX_LEARN_DST_LOAD:
                x['_desc'] = 'load:%s->%s' % (srcdesc, dstdesc)
            elif NX_FLOWMODSPEC_SRC(x['header']):
                x['_desc'] = 'output:%s' % nxm_port_no_raw.formatter(common.create_binary(x['value'], 2))
            else:
                x['_desc'] = 'output:%s' % (srcdesc,)
            x['header'] = nx_flow_mod_spec_header.formatter(x['header'])
            return x
        
        nx_flow_mod_spec = nstruct(
            (uint16, 'header'),
            (_nx_flow_mod_spec_src,),
            (_nx_flow_mod_spec_dst,),
            name = 'nx_flow_mod_spec',
            padding = 1,
            formatter = _nx_flow_mod_spec_formatter,
            lastextra = False
            # if x.header == 0, size is 14, the padding should not be so large so it will not be successfully parsed
        )
        namespace['nx_flow_mod_spec'] = nx_flow_mod_spec
        def create_nxfms_matchfield(src, dst, src_ofs = 0, dst_ofs = 0, n_bits = None):
            if n_bits is None:
                n_bits = min(NXM_LENGTH(dst) * 8 - dst_ofs, NXM_LENGTH(src) * 8 - src_ofs)
            if n_bits <= 0:
                raise ValueError('Cannot create flow mod spec with 0 bits')
            return nx_flow_mod_spec.parse(_create_header(NX_LEARN_SRC_FIELD, NX_LEARN_DST_MATCH, n_bits) + _create_field(src, src_ofs) + _create_field(dst, dst_ofs))[0]
        namespace['create_nxfms_matchfield'] = create_nxfms_matchfield
        def create_nxfms_matchvalue(dst, value, dst_ofs = 0, n_bits = None):
            if n_bits is None:
                n_bits = NXM_LENGTH(dst) * 8 - dst_ofs
            if n_bits <= 0:
                raise ValueError('Cannot create flow mod spec with 0 bits')
            return nx_flow_mod_spec.parse(_create_header(NX_LEARN_SRC_IMMEDIATE, NX_LEARN_DST_MATCH, n_bits) + common.create_binary(value, (n_bits + 15) // 16 * 2) + _create_field(dst, dst_ofs))[0]
        namespace['create_nxfms_matchvalue'] = create_nxfms_matchvalue
        def create_nxfms_loadfield(src, dst, src_ofs = 0, dst_ofs = 0, n_bits = None):
            if n_bits is None:
                n_bits = min(NXM_LENGTH(dst) * 8 - dst_ofs, NXM_LENGTH(src) * 8 - src_ofs)
            if n_bits <= 0:
                raise ValueError('Cannot create flow mod spec with 0 bits')
            return nx_flow_mod_spec.parse(_create_header(NX_LEARN_SRC_FIELD, NX_LEARN_DST_LOAD, n_bits) + _create_field(src, src_ofs) + _create_field(dst, dst_ofs))[0]
        namespace['create_nxfms_loadfield'] = create_nxfms_loadfield
        def create_nxfms_loadvalue(dst, value, dst_ofs = 0, n_bits = None):
            if n_bits is None:
                n_bits = NXM_LENGTH(dst) * 8 - dst_ofs
            if n_bits <= 0:
                raise ValueError('Cannot create flow mod spec with 0 bits')
            return nx_flow_mod_spec.parse(_create_header(NX_LEARN_SRC_IMMEDIATE, NX_LEARN_DST_LOAD, n_bits) + common.create_binary(value, (n_bits + 15) // 16 * 2) + _create_field(dst, dst_ofs))[0]
        namespace['create_nxfms_loadvalue'] = create_nxfms_loadvalue
        def create_nxfms_outputfield(src, src_ofs = 0, n_bits = None):
            if n_bits is None:
                n_bits = NXM_LENGTH(src) * 8 - src_ofs
            if n_bits <= 0:
                raise ValueError('Cannot create flow mod spec with 0 bits')
            return nx_flow_mod_spec.parse(_create_header(NX_LEARN_SRC_FIELD, NX_LEARN_DST_OUTPUT, n_bits) + _create_field(src, src_ofs))[0]
        namespace['create_nxfms_outputfield'] = create_nxfms_outputfield
        def create_nxfms_outputvalue(dst, value):
            return nx_flow_mod_spec.parse(_create_header(NX_LEARN_SRC_IMMEDIATE, NX_LEARN_DST_OUTPUT, 16) + common.create_binary(value, 2))[0]    
        namespace['create_nxfms_outputvalue'] = create_nxfms_outputvalue
        
        ofp_flow_mod_flags = namespace['ofp_flow_mod_flags']
        
        
        
        _nx_action_learn = \
            nstruct(
                (uint16, 'idle_timeout'),    #  /* Idle time before discarding (seconds). */
                (uint16, 'hard_timeout'),    #  /* Max time before discarding (seconds). */
                (uint16, 'priority'),        #  /* Priority level of flow entry. */
                (uint64, 'cookie'),          #  /* Cookie for new flow. */
                (nx_learn_flags, 'flags'),   #  /* NX_LEARN_F_*. */
                (uint8, 'table_id'),         #  /* Table to insert flow entry. */
                (uint8,),                    #  /* Must be zero. */
                (uint16, 'fin_idle_timeout'),#  /* Idle timeout after FIN, if nonzero. */
                (uint16, 'fin_hard_timeout'),#  /* Hard timeout after FIN, if nonzero. */
                name = '_nx_action_learn',
                padding = 1
            )

        nx_action_learn = \
            nstruct(
                (_nx_action_learn,),
                (nx_flow_mod_spec[0], 'specs'),
                base = nx_action,
                name = 'nx_action_learn',
                classifyby = (NXAST_LEARN,),
                criteria = lambda x: getattr(x, action_subtype) == NXAST_LEARN,
                init = packvalue(NXAST_LEARN, action_subtype),
            )
        namespace['nx_action_learn'] = nx_action_learn
        
        def _prepack_nx_action_learn2(x):
            if hasattr(x, 'result_field'):
                x.flags |= NX_LEARN_F_WRITE_RESULT
            else:
                x.flags &= ~NX_LEARN_F_WRITE_RESULT
        
        nx_action_learn2 = \
            nstruct(
                (_nx_action_learn,),
                (nx_flow_mod_spec[0], 'specs'),
                (uint32, 'limit'),                  # /* Maximum number of learned flows.
                                                    #  * 0 indicates unlimited. */
                # /* Where to store the result. */
                (uint16, 'result_dst_ofs'),         # /* Starting bit offset in destination. */
            
                (uint16,),                          # /* Must be zero. */
                *_create_oxm_header_only_fields('result_field', lambda x: x.flags & NX_LEARN_F_WRITE_RESULT),
                (nx_flow_mod_spec[0], 'specs'),
                base = nx_action,
                name = 'nx_action_learn2',
                classifyby = (NXAST_LEARN2,),
                criteria = lambda x: getattr(x, action_subtype) == NXAST_LEARN2,
                init = packvalue(NXAST_LEARN2, action_subtype),
                prepack = _prepack_nx_action_learn2,
                formatter = _createdesc(lambda x: 'result:%s[%d..%d]' % (x['result_field'], x['result_dst_ofs'], x['result_dst_ofs'])
                                                  if 'result_field' in x
                                                  else '(no result)')
            )
        namespace['nx_action_learn2'] = nx_action_learn2
    
        '''
        /* Action structure for NXAST_FIN_TIMEOUT.
         *
         * This action changes the idle timeout or hard timeout, or both, of this
         * OpenFlow rule when the rule matches a TCP packet with the FIN or RST flag.
         * When such a packet is observed, the action reduces the rule's idle timeout
         * to 'fin_idle_timeout' and its hard timeout to 'fin_hard_timeout'.  This
         * action has no effect on an existing timeout that is already shorter than the
         * one that the action specifies.  A 'fin_idle_timeout' or 'fin_hard_timeout'
         * of zero has no effect on the respective timeout.
         *
         * 'fin_idle_timeout' and 'fin_hard_timeout' are measured in seconds.
         * 'fin_hard_timeout' specifies time since the flow's creation, not since the
         * receipt of the FIN or RST.
         *
         * This is useful for quickly discarding learned TCP flows that otherwise will
         * take a long time to expire.
         *
         * This action is intended for use with an OpenFlow rule that matches only a
         * single TCP flow.  If the rule matches multiple TCP flows (e.g. it wildcards
         * all TCP traffic, or all TCP traffic to a particular port), then any FIN or
         * RST in any of those flows will cause the entire OpenFlow rule to expire
         * early, which is not normally desirable.
         */
         '''
        nx_action_fin_timeout = nstruct(
            (uint16, 'fin_idle_timeout'),   #  /* New idle timeout, if nonzero. */
            (uint16, 'fin_hard_timeout'),   #  /* New hard timeout, if nonzero. */
            (uint16,),
            base = nx_action,
            name = 'nx_action_fin_timeout',
            criteria = lambda x: getattr(x, action_subtype) == NXAST_FIN_TIMEOUT,
            classifyby = (NXAST_FIN_TIMEOUT,),
            init = packvalue(NXAST_FIN_TIMEOUT, action_subtype)
        )
        namespace['nx_action_fin_timeout'] = nx_action_fin_timeout
        '''
        /* Action structure for NXAST_BUNDLE and NXAST_BUNDLE_LOAD.
         *
         * The bundle actions choose a slave from a supplied list of options.
         * NXAST_BUNDLE outputs to its selection.  NXAST_BUNDLE_LOAD writes its
         * selection to a register.
         *
         * The list of possible slaves follows the nx_action_bundle structure. The size
         * of each slave is governed by its type as indicated by the 'slave_type'
         * parameter. The list of slaves should be padded at its end with zeros to make
         * the total length of the action a multiple of 8.
         *
         * Switches infer from the 'slave_type' parameter the size of each slave.  All
         * implementations must support the NXM_OF_IN_PORT 'slave_type' which indicates
         * that the slaves are OpenFlow port numbers with NXM_LENGTH(NXM_OF_IN_PORT) ==
         * 2 byte width.  Switches should reject actions which indicate unknown or
         * unsupported slave types.
         *
         * Switches use a strategy dictated by the 'algorithm' parameter to choose a
         * slave.  If the switch does not support the specified 'algorithm' parameter,
         * it should reject the action.
         *
         * Several algorithms take into account liveness when selecting slaves.  The
         * liveness of a slave is implementation defined (with one exception), but will
         * generally take into account things like its carrier status and the results
         * of any link monitoring protocols which happen to be running on it.  In order
         * to give controllers a place-holder value, the OFPP_NONE port is always
         * considered live.
         *
         * Some slave selection strategies require the use of a hash function, in which
         * case the 'fields' and 'basis' parameters should be populated.  The 'fields'
         * parameter (one of NX_HASH_FIELDS_*) designates which parts of the flow to
         * hash.  Refer to the definition of "enum nx_hash_fields" for details.  The
         * 'basis' parameter is used as a universal hash parameter.  Different values
         * of 'basis' yield different hash results.
         *
         * The 'zero' parameter at the end of the action structure is reserved for
         * future use.  Switches are required to reject actions which have nonzero
         * bytes in the 'zero' field.
         *
         * NXAST_BUNDLE actions should have 'ofs_nbits' and 'dst' zeroed.  Switches
         * should reject actions which have nonzero bytes in either of these fields.
         *
         * NXAST_BUNDLE_LOAD stores the OpenFlow port number of the selected slave in
         * dst[ofs:ofs+n_bits].  The format and semantics of 'dst' and 'ofs_nbits' are
         * similar to those for the NXAST_REG_LOAD action. */
        '''
        nx_action_bundle = nstruct(    
    #        /* Slave choice algorithm to apply to hash value. */
            (nx_bd_algorithm, 'algorithm'),  #         /* One of NX_BD_ALG_*. */
        
    #        /* What fields to hash and how. */
            (nx_hash_fields, 'fields'),     #         /* One of NX_HASH_FIELDS_*. */
            (uint16, 'basis'),              #         /* Universal hash parameter. */
        
            (all_nxm_header, 'slave_type'), #         /* NXM_OF_IN_PORT. */
            (uint16, 'n_slaves'),           #         /* Number of slaves. */
        
            (ofs_nbits_type, 'ofs_nbits'),  #         /* (ofs << 6) | (n_bits - 1). */
            (all_nxm_header, 'dst'),        #         /* Destination. */
        
            (uint8[4],),                    #         /* Reserved. Must be zero. */
            name = 'nx_action_bundle',
            base = nx_action,
            criteria = lambda x: getattr(x, action_subtype) == NXAST_BUNDLE or getattr(x, action_subtype) == NXAST_BUNDLE_LOAD,
            classifyby = (NXAST_BUNDLE, NXAST_BUNDLE_LOAD),
            init = packvalue(NXAST_BUNDLE, action_subtype)
        )
        namespace['nx_action_bundle'] = nx_action_bundle
        
        def _nx_slave_ports_prepack(x):
            x.n_slaves = len(x.bundles)
        _nx_slave_ports = nstruct(
            (nx_port_no[0], 'bundles'),
            name = '_nx_slave_ports',
            size = lambda x: x.n_slaves * 2,
            prepack = _nx_slave_ports_prepack,
            padding = 1
        )
    
        nx_action_bundle_port = nstruct(
            (_nx_slave_ports,),
            base = nx_action_bundle,
            name = 'nx_action_bundle_port',
            criteria = lambda x: x.slave_type == NXM_OF_IN_PORT,
            init = packvalue(NXM_OF_IN_PORT, 'slave_type'),
            lastextra = False,
            formatter = _createdesc(lambda x: 'bundle_load(%s,%d,%s,%s,%s%s,slaves:%r)' % \
                (x['fields'], x['basis'], x['algorithm'], x['slave_type'], x['dst'], x['ofs_nbits'], x['bundles']) \
                if x[action_subtype] == 'NXAST_BUNDLE_LOAD' else 'bundle(%s,%d,%s,%s,slaves:%r)' % (x['fields'], x['basis'], x['algorithm'], x['slave_type'], x['bundles']))
        )
        namespace['nx_action_bundle_port'] = nx_action_bundle_port
        
        def _nx_slave_others_prepack(x):
            x.n_slaves = len(x.bundlesraw) // NXM_LENGTH(x.slave_type)
        
        _nx_slave_others = nstruct(
            (raw, 'bundlesraw'),
            name = '_nx_slave_others',
            size = lambda x: x.n_slaves * NXM_LENGTH(x.slave_type),
            prepack = _nx_slave_others_prepack,
            padding = 1
        )
        
        nx_action_bundle_others = nstruct(
            (_nx_slave_others,),
            base = nx_action_bundle,
            name = 'nx_action_bundle_others',
            criteria = lambda x: x.slave_type != NXM_OF_IN_PORT,
            lastextra = False,
            init = packvalue(NXM_OF_ETH_DST, 'slave_type'),
            formatter = _createdesc(lambda x: 'bundle_load(%s,%d,%s,%s,%s%s,slaves:%r)' % \
                (x['fields'], x['basis'], x['algorithm'], x['slave_type'], x['dst'], x['ofs_nbits'], x['bundleraw']) \
                if x[action_subtype] == 'NXAST_BUNDLE_LOAD' else 'bundle(%s,%d,%s,%s,slaves:%r)' % (x['fields'], x['basis'], x['algorithm'], x['slave_type'], x['bundleraw']))
        )
        namespace['nx_action_bundle_others'] = nx_action_bundle_others
        '''
        /* Action structure for NXAST_DEC_TTL_CNT_IDS.
         *
         * If the packet is not IPv4 or IPv6, does nothing.  For IPv4 or IPv6, if the
         * TTL or hop limit is at least 2, decrements it by 1.  Otherwise, if TTL or
         * hop limit is 0 or 1, sends a packet-in to the controllers with each of the
         * 'n_controllers' controller IDs specified in 'cnt_ids'.
         *
         * (This differs from NXAST_DEC_TTL in that for NXAST_DEC_TTL the packet-in is
         * sent only to controllers with id 0.)
         */
         '''
        
        def _nx_action_cnt_ids_ids_prepack(x):
            x.n_controllers = len(x.cnt_ids)
        _nx_action_cnt_ids_ids = nstruct(
            (uint16[0], 'cnt_ids'),
            name = '_nx_action_cnt_ids_ids',
            size = lambda x: 2 * x.n_controllers,
            prepack = _nx_action_cnt_ids_ids_prepack
        )
        
        nx_action_cnt_ids = nstruct(
            (uint16, 'n_controllers'),    # /* Number of controllers. */
            (uint8[4],),                  # /* Must be zero. */
            (_nx_action_cnt_ids_ids,),
            base = nx_action,
            classifyby = (NXAST_DEC_TTL_CNT_IDS,),
            criteria = lambda x: getattr(x, action_subtype) == NXAST_DEC_TTL_CNT_IDS,
            init = packvalue(NXAST_DEC_TTL_CNT_IDS, action_subtype),
            lastextra = False,
            name = 'nx_action_cnt_ids'
        )
        namespace['nx_action_cnt_ids'] = nx_action_cnt_ids
    
        '''
        /* Action structure for NXAST_OUTPUT_REG.
         *
         * Outputs to the OpenFlow port number written to src[ofs:ofs+nbits].
         *
         * The format and semantics of 'src' and 'ofs_nbits' are similar to those for
         * the NXAST_REG_LOAD action.
         *
         * The acceptable nxm_header values for 'src' are the same as the acceptable
         * nxm_header values for the 'src' field of NXAST_REG_MOVE.
         *
         * The 'max_len' field indicates the number of bytes to send when the chosen
         * port is OFPP_CONTROLLER.  Its semantics are equivalent to the 'max_len'
         * field of OFPAT_OUTPUT.
         *
         * The 'zero' field is required to be zeroed for forward compatibility. */
        '''
        nx_action_output_reg = nstruct(
            (ofs_nbits_type, 'ofs_nbits'),      # /* (ofs << 6) | (n_bits - 1). */
            (all_nxm_header, 'src'),            # /* Source. */
            (uint16, 'max_len'),                # /* Max length to send to controller. */
            (uint8[6],),                        # /* Reserved, must be zero. */
            base = nx_action,
            classifyby = (NXAST_OUTPUT_REG,),
            criteria = lambda x: getattr(x, action_subtype) == NXAST_OUTPUT_REG,
            init = packvalue(NXAST_OUTPUT_REG, action_subtype),
            name = 'nx_action_output_reg',
            formatter = _createdesc(lambda x: 'output:%s%s' % (x['src'], x['ofs_nbits']))
        )
        namespace['nx_action_output_reg'] = nx_action_output_reg

        #=======================================================================
        # /* Action structure for NXAST_OUTPUT_REG2.
        #  *
        #  * Like the NXAST_OUTPUT_REG but organized so that there is room for a 64-bit
        #  * experimenter OXM as 'src'.
        #  */
        #=======================================================================
        nx_action_output_reg2 = \
            nstruct(        
                (ofs_nbits_type, 'ofs_nbits'),      # /* (ofs << 6) | (n_bits - 1). */
                (uint16, 'max_len'),                # /* Max length to send to controller. */
            
                *_create_oxm_header_only_fields('src'),
                base = nx_action,
                classifyby = (NXAST_OUTPUT_REG2,),
                criteria = lambda x: getattr(x, action_subtype) == NXAST_OUTPUT_REG2,
                init = packvalue(NXAST_OUTPUT_REG2, action_subtype),
                name = 'nx_action_output_reg2',
                formatter = _createdesc(lambda x: 'output:%s%s' % (x['src'], x['ofs_nbits']))
        )
        namespace['nx_action_output_reg2'] = nx_action_output_reg2

    
        '''
        /* NXAST_EXIT
         *
         * Discontinues action processing.
         *
         * The NXAST_EXIT action causes the switch to immediately halt processing
         * actions for the flow.  Any actions which have already been processed are
         * executed by the switch.  However, any further actions, including those which
         * may be in different tables, or different levels of the NXAST_RESUBMIT
         * hierarchy, will be ignored.
         *
         * Uses the nx_action_header structure. */
        
        /* ## --------------------- ## */
        /* ## Requests and replies. ## */
        /* ## --------------------- ## */
        '''
        
        '''
        /* NXT_SET_FLOW_FORMAT request. */
        '''
        nx_set_flow_format = nstruct(
            (nx_flow_format, 'format'),         #            /* One of NXFF_*. */
            name = 'nx_set_flow_format',
            base = nicira_header,
            criteria = lambda x: getattr(x, msg_subtype) == NXT_SET_FLOW_FORMAT,
            classifyby = (NXT_SET_FLOW_FORMAT,),
            init = packvalue(NXT_SET_FLOW_FORMAT, msg_subtype)
        )
        namespace['nx_set_flow_format'] = nx_set_flow_format
    
        '''
        /* NXT_FLOW_MOD (analogous to OFPT_FLOW_MOD).
         *
         * It is possible to limit flow deletions and modifications to certain
         * cookies by using the NXM_NX_COOKIE(_W) matches.  The "cookie" field
         * is used only to add or modify flow cookies.
         */
         '''
        ofp_flow_mod_command = namespace['ofp_flow_mod_command']
        nx_flow_mod = nstruct(
            (uint64, 'cookie'),                  #     /* Opaque controller-issued identifier. */
            (ofp_flow_mod_command, 'command'),   #     /* OFPFC_* + possibly a table ID (see comment
    #                                       * on struct nx_flow_mod_table_id). */
            (uint16, 'idle_timeout'),            #     /* Idle time before discarding (seconds). */
            (uint16, 'hard_timeout'),            #     /* Max time before discarding (seconds). */
            (uint16, 'priority'),                #     /* Priority level of flow entry. */
            (uint32, 'buffer_id'),               #     /* Buffered packet to apply to (or -1).
    #                                         Not meaningful for OFPFC_DELETE*. */
            (nx_port_no, 'out_port'),            #     /* For OFPFC_DELETE* commands, require
    #                                         matching entries to include this as an
    #                                         output port.  A value of OFPP_NONE
    #                                         indicates no restriction. */
            (ofp_flow_mod_flags, 'flags'),       #     /* One of OFPFF_*. */
            (uint16, 'match_len'),               #     /* Size of nx_match. */
            (uint8[6],),                         #     /* Align to 64-bits. */
            (nx_matches,),
            base = nicira_header,
            criteria = lambda x: getattr(x, msg_subtype) == NXT_FLOW_MOD,
            classifyby = (NXT_FLOW_MOD,),
            init = packvalue(NXT_FLOW_MOD, msg_subtype),
            name = 'nx_flow_mod'
        )
        namespace['nx_flow_mod'] = nx_flow_mod
        
        '''
        /* NXT_FLOW_REMOVED (analogous to OFPT_FLOW_REMOVED).
         *
         * 'table_id' is present only in Open vSwitch 1.11 and later.  In earlier
         * versions of Open vSwitch, this is a padding byte that is always zeroed.
         * Therefore, a 'table_id' value of 0 indicates that the table ID is not known,
         * and other values may be interpreted as one more than the flow's former table
         * ID. */
         '''
        nx_flow_removed = nstruct(
            (uint64, 'cookie'),             # /* Opaque controller-issued identifier. */
            (uint16, 'priority'),           # /* Priority level of flow entry. */
            (ofp_flow_removed_reason, 'reason'),    #    /* One of OFPRR_*. */
            (uint8, 'table_id'),                #    /* Flow's former table ID, plus one. */
            (uint32, 'duration_sec'),           #    /* Time flow was alive in seconds. */
            (uint32, 'duration_nsec'),          #    /* Time flow was alive in nanoseconds beyond
    #                                     duration_sec. */
            (uint16, 'idle_timeout'),           #    /* Idle timeout from original flow mod. */
            (uint16, 'match_len'),              #    /* Size of nx_match. */
            (uint64, 'packet_count'),
            (uint64, 'byte_count'),
            (nx_matches,),
            base = nicira_header,
            criteria = lambda x: getattr(x, msg_subtype) == NXT_FLOW_REMOVED,
            classifyby = (NXT_FLOW_REMOVED,),
            init = packvalue(NXT_FLOW_REMOVED, msg_subtype),
            name = 'nx_flow_removed'        
        )
        namespace['nx_flow_removed'] = nx_flow_removed
    
        '''
        /* Nicira vendor stats request of type NXST_FLOW (analogous to OFPST_FLOW
         * request).
         *
         * It is possible to limit matches to certain cookies by using the
         * NXM_NX_COOKIE and NXM_NX_COOKIE_W matches.
         */
         '''
        nx_flow_stats_request = nstruct(
            (nx_port_no, 'out_port'),       #/* Require matching entries to include this
    #                                     as an output port.  A value of OFPP_NONE
    #                                     indicates no restriction. */
            (uint16, 'match_len'),          #       /* Length of nx_match. */
            (uint8, 'table_id'),            #       /* ID of table to read (from ofp_table_stats)
    #                                     or 0xff for all tables. */
            (uint8[3],),                    #       /* Align to 64 bits. */
            (nx_matches,),
            base = nx_stats_request,
            criteria = lambda x: getattr(x, stats_subtype) == NXST_FLOW,
            classifyby = (NXST_FLOW,),
            init = packvalue(NXST_FLOW, stats_subtype),
            name = 'nx_flow_stats_request'
        )
        namespace['nx_flow_stats_request'] = nx_flow_stats_request
        '''
        /* Body for Nicira vendor stats reply of type NXST_FLOW (analogous to
         * OFPST_FLOW reply).
         *
         * The values of 'idle_age' and 'hard_age' are only meaningful when talking to
         * a switch that implements the NXT_FLOW_AGE extension.  Zero means that the
         * true value is unknown, perhaps because hardware does not track the value.
         * (Zero is also the value that one should ordinarily expect to see talking to
         * a switch that does not implement NXT_FLOW_AGE, since those switches zero the
         * padding bytes that these fields replaced.)  A nonzero value X represents X-1
         * seconds.  A value of 65535 represents 65534 or more seconds.
         *
         * 'idle_age' is the number of seconds that the flow has been idle, that is,
         * the number of seconds since a packet passed through the flow.  'hard_age' is
         * the number of seconds since the flow was last modified (e.g. OFPFC_MODIFY or
         * OFPFC_MODIFY_STRICT).  (The 'duration_*' fields are the elapsed time since
         * the flow was added, regardless of subsequent modifications.)
         *
         * For a flow with an idle or hard timeout, 'idle_age' or 'hard_age',
         * respectively, will ordinarily be smaller than the timeout, but flow
         * expiration times are only approximate and so one must be prepared to
         * tolerate expirations that occur somewhat early or late.
         */
         '''
        ofp_action = namespace['ofp_action']
        
        nx_flow_stats = nstruct(
            (uint16, 'length'),         # /* Length of this entry. */
            (uint8, 'table_id'),        # /* ID of table flow came from. */
            (uint8,),
            (uint32, 'duration_sec'),   # /* Time flow has been alive in seconds. */
            (uint32, 'duration_nsec'),  # /* Time flow has been alive in nanoseconds
    #                                     beyond duration_sec. */
            (uint16, 'priority'),       # /* Priority of the entry. */
            (uint16, 'idle_timeout'),   # /* Number of seconds idle before expiration. */
            (uint16, 'hard_timeout'),   # /* Number of seconds before expiration. */
            (uint16, 'match_len'),      # /* Length of nx_match. */
            (uint16, 'idle_age'),       # /* Seconds since last packet, plus one. */
            (uint16, 'hard_age'),       # /* Seconds since last modification, plus one. */
            (uint64, 'cookie'),         # /* Opaque controller-issued identifier. */
            (uint64, 'packet_count'),   # /* Number of packets, UINT64_MAX if unknown. */
            (uint64, 'byte_count'),     # /* Number of bytes, UINT64_MAX if unknown. */
            #=======================================================================
            # /* Followed by:
            #  *   - Exactly match_len (possibly 0) bytes containing the nx_match, then
            #  *   - Exactly (match_len + 7)/8*8 - match_len (between 0 and 7) bytes of
            #  *     all-zero bytes, then
            #  *   - Actions to fill out the remainder 'length' bytes (always a multiple
            #  *     of 8).
            #  */
            #=======================================================================
            (nx_matches,),
            (ofp_action[0], 'actions'),
            name = 'nx_flow_stats',
            size = lambda x: x.length,
            prepack = packsize('length')        
        )
        namespace['nx_flow_stats'] = nx_flow_stats
        
        nx_flow_stats_reply = nstruct(
            (nx_flow_stats[0], 'stats'),
            base = nx_stats_reply,
            classifyby = (NXST_FLOW,),
            criteria = lambda x: getattr(x, stats_subtype) == NXST_FLOW,
            init = packvalue(NXST_FLOW, stats_subtype),
            name = 'nx_flow_stats_reply'
        )
        namespace['nx_flow_stats_reply'] = nx_flow_stats_reply
    
        '''
        /* Nicira vendor stats request of type NXST_AGGREGATE (analogous to
         * OFPST_AGGREGATE request).
         *
         * The reply format is identical to the reply format for OFPST_AGGREGATE,
         * except for the header. */
        '''
        nx_aggregate_stats_request = nstruct(
            (nx_port_no, 'out_port'),           #       /* Require matching entries to include this
    #                                     as an output port.  A value of OFPP_NONE
    #                                     indicates no restriction. */
            (uint16, 'match_len'),              #       /* Length of nx_match. */
            (uint8, 'table_id'),                #       /* ID of table to read (from ofp_table_stats)
    #                                     or 0xff for all tables. */
            (uint8[3],),                        #       /* Align to 64 bits. */
            #=======================================================================
            # /* Followed by:
            #  *   - Exactly match_len (possibly 0) bytes containing the nx_match, then
            #  *   - Exactly (match_len + 7)/8*8 - match_len (between 0 and 7) bytes of
            #  *     all-zero bytes, which must also exactly fill out the length of the
            #  *     message.
            #  */
            #=======================================================================
            (nx_matches,),
            base = nx_stats_request,
            name = 'nx_aggregate_stats_request',
            criteria = lambda x: getattr(x, stats_subtype) == NXST_AGGREGATE,
            classifyby = (NXST_AGGREGATE,),
            init = packvalue(NXST_AGGREGATE, stats_subtype),
            lastextra = False
        )
        namespace['nx_aggregate_stats_request'] = nx_aggregate_stats_request
    
        nx_aggregate_stats_reply = nstruct(
            (uint64, 'packet_count'),           # /* Number of packets in flows. */
            (uint64, 'byte_count'),             # /* Number of bytes in flows. */
            (uint32, 'flow_count'),             # /* Number of flows. */
            (uint8[4],),
            base = nx_stats_reply,
            name = 'nx_aggregate_stats_reply',
            criteria = lambda x: getattr(x, stats_subtype) == NXST_AGGREGATE,
            classifyby = (NXST_AGGREGATE,),
            init = packvalue(NXST_AGGREGATE, stats_subtype)
        )
        namespace['nx_aggregate_stats_reply'] = nx_aggregate_stats_reply
    
        '''
        /* NXT_SET_CONTROLLER_ID.
         *
         * Each OpenFlow controller connection has a 16-bit identifier that is
         * initially 0.  This message changes the connection's ID to 'id'.
         *
         * Controller connection IDs need not be unique.
         *
         * The NXAST_CONTROLLER action is the only current user of controller
         * connection IDs. */
         '''
        nx_controller_id = nstruct(
            (uint8[6],),                    # /* Must be zero. */
            (uint16, 'controller_id'),      # /* New controller connection ID. */
            base = nicira_header,
            name = 'nx_controller_id',
            criteria = lambda x: getattr(x, msg_subtype) == NXT_SET_CONTROLLER_ID,
            init = packvalue(NXT_SET_CONTROLLER_ID, msg_subtype),
            classifyby = (NXT_SET_CONTROLLER_ID,)
        )
        namespace['nx_controller_id'] = nx_controller_id
    
        '''
        /* Action structure for NXAST_CONTROLLER.
         *
         * This generalizes using OFPAT_OUTPUT to send a packet to OFPP_CONTROLLER.  In
         * addition to the 'max_len' that OFPAT_OUTPUT supports, it also allows
         * specifying:
         *
         *    - 'reason': The reason code to use in the ofp_packet_in or nx_packet_in.
         *
         *    - 'controller_id': The ID of the controller connection to which the
         *      ofp_packet_in should be sent.  The ofp_packet_in or nx_packet_in is
         *      sent only to controllers that have the specified controller connection
         *      ID.  See "struct nx_controller_id" for more information. */
         '''
        nx_action_controller = nstruct(
            (uint16, 'max_len'),                    # /* Maximum length to send to controller. */
            (uint16, 'controller_id'),              # /* Controller ID to send packet-in. */
            (ofp_packet_in_reason, 'reason'),       # /* enum ofp_packet_in_reason (OFPR_*). */
            (uint8,),
            base = nx_action,
            name = 'nx_action_controller',
            criteria = lambda x: getattr(x, action_subtype) == NXAST_CONTROLLER,
            classifyby = (NXAST_CONTROLLER,),
            init = packvalue(NXAST_CONTROLLER, action_subtype)
        )
        namespace['nx_action_controller'] = nx_action_controller
        
        nx_action_controller2_prop = \
            nstruct(
                (nx_action_controller2_prop_type, 'type'),
                (uint16, 'length'),
                name = 'nx_action_controller2_prop',
                size = lambda x: x.length,
                prepack = packrealsize('length'),
                classifier = lambda x: x.type
            )
        namespace['nx_action_controller2_prop'] = nx_action_controller2_prop
        
        nx_action_controller2_prop_max_len = \
            nstruct(
                (uint16, 'value'),
                name = 'nx_action_controller2_prop_max_len',
                base = nx_action_controller2_prop,
                classifyby = (NXAC2PT_MAX_LEN,),
                init = packvalue(NXAC2PT_MAX_LEN, 'type'),
                criteria = lambda x: x.type == NXAC2PT_MAX_LEN
            )
        namespace['nx_action_controller2_prop_max_len'] = nx_action_controller2_prop_max_len

        nx_action_controller2_prop_controller_id = \
            nstruct(
                (uint16, 'value'),
                name = 'nx_action_controller2_prop_controller_id',
                base = nx_action_controller2_prop,
                classifyby = (NXAC2PT_CONTROLLER_ID,),
                init = packvalue(NXAC2PT_CONTROLLER_ID, 'type'),
                criteria = lambda x: x.type == NXAC2PT_CONTROLLER_ID
            )
        namespace['nx_action_controller2_prop_controller_id'] = nx_action_controller2_prop_controller_id
        
        nx_action_controller2_prop_reason = \
            nstruct(
                (ofp_packet_in_reason, 'value'),
                name = 'nx_action_controller2_prop_reason',
                base = nx_action_controller2_prop,
                classifyby = (NXAC2PT_REASON,),
                init = packvalue(NXAC2PT_REASON, 'type'),
                criteria = lambda x: x.type == NXAC2PT_REASON
            )
        namespace['nx_action_controller2_prop_reason'] = nx_action_controller2_prop_reason

        nx_action_controller2_prop_userdata = \
            nstruct(
                (raw, 'value'),
                name = 'nx_action_controller2_prop_userdata',
                base = nx_action_controller2_prop,
                classifyby = (NXAC2PT_USERDATA,),
                init = packvalue(NXAC2PT_USERDATA, 'type'),
                criteria = lambda x: x.type == NXAC2PT_USERDATA
            )
        namespace['nx_action_controller2_prop_userdata'] = nx_action_controller2_prop_userdata

        nx_action_controller2_prop_pause = \
            nstruct(
                name = 'nx_action_controller2_prop_pause',
                base = nx_action_controller2_prop,
                classifyby = (NXAC2PT_PAUSE,),
                init = packvalue(NXAC2PT_PAUSE, 'type'),
                criteria = lambda x: x.type == NXAC2PT_PAUSE
            )
        namespace['nx_action_controller2_prop_pause'] = nx_action_controller2_prop_pause
        
        nx_action_controller2 = \
            nstruct(
                (uint8[6],),
                (nx_action_controller2_prop[0], 'properties'),
                name = 'nx_action_controller2',
                base = nx_action,
                criteria = lambda x: getattr(x, action_subtype) == NXAST_CONTROLLER2,
                classifyby = (NXAST_CONTROLLER2,),
                init = packvalue(NXAST_CONTROLLER2, action_subtype)
            )
        namespace['nx_action_controller2'] = nx_action_controller2
        
        '''
        /* Flow Table Monitoring
         * =====================
         *
         * NXST_FLOW_MONITOR allows a controller to keep track of changes to OpenFlow
         * flow table(s) or subsets of them, with the following workflow:
         *
         * 1. The controller sends an NXST_FLOW_MONITOR request to begin monitoring
         *    flows.  The 'id' in the request must be unique among all monitors that
         *    the controller has started and not yet canceled on this OpenFlow
         *    connection.
         *
         * 2. The switch responds with an NXST_FLOW_MONITOR reply.  If the request's
         *    'flags' included NXFMF_INITIAL, the reply includes all the flows that
         *    matched the request at the time of the request (with event NXFME_ADDED).
         *    If 'flags' did not include NXFMF_INITIAL, the reply is empty.
         *
         *    The reply uses the xid of the request (as do all replies to OpenFlow
         *    requests).
         *
         * 3. Whenever a change to a flow table entry matches some outstanding monitor
         *    request's criteria and flags, the switch sends a notification to the
         *    controller as an additional NXST_FLOW_MONITOR reply with xid 0.
         *
         *    When multiple outstanding monitors match a single change, only a single
         *    notification is sent.  This merged notification includes the information
         *    requested in any of the individual monitors.  That is, if any of the
         *    matching monitors requests actions (NXFMF_ACTIONS), the notification
         *    includes actions, and if any of the monitors request full changes for the
         *    controller's own changes (NXFMF_OWN), the controller's own changes will
         *    be included in full.
         *
         * 4. The controller may cancel a monitor with NXT_FLOW_MONITOR_CANCEL.  No
         *    further notifications will be sent on the basis of the canceled monitor
         *    afterward.
         *
         *
         * Buffer Management
         * =================
         *
         * OpenFlow messages for flow monitor notifications can overflow the buffer
         * space available to the switch, either temporarily (e.g. due to network
         * conditions slowing OpenFlow traffic) or more permanently (e.g. the sustained
         * rate of flow table change exceeds the network bandwidth between switch and
         * controller).
         *
         * When Open vSwitch's notification buffer space reaches a limiting threshold,
         * OVS reacts as follows:
         *
         * 1. OVS sends an NXT_FLOW_MONITOR_PAUSED message to the controller, following
         *    all the already queued notifications.  After it receives this message,
         *    the controller knows that its view of the flow table, as represented by
         *    flow monitor notifications, is incomplete.
         *
         * 2. As long as the notification buffer is not empty:
         *
         *        - NXMFE_ADD and NXFME_MODIFIED notifications will not be sent.
         *
         *        - NXFME_DELETED notifications will still be sent, but only for flows
         *          that existed before OVS sent NXT_FLOW_MONITOR_PAUSED.
         *
         *        - NXFME_ABBREV notifications will not be sent.  They are treated as
         *          the expanded version (and therefore only the NXFME_DELETED
         *          components, if any, are sent).
         *
         * 3. When the notification buffer empties, OVS sends NXFME_ADD notifications
         *    for flows added since the buffer reached its limit and NXFME_MODIFIED
         *    notifications for flows that existed before the limit was reached and
         *    changed after the limit was reached.
         *
         * 4. OVS sends an NXT_FLOW_MONITOR_RESUMED message to the controller.  After
         *    it receives this message, the controller knows that its view of the flow
         *    table, as represented by flow monitor notifications, is again complete.
         *
         * This allows the maximum buffer space requirement for notifications to be
         * bounded by the limit plus the maximum number of supported flows.
         *
         *
         * "Flow Removed" messages
         * =======================
         *
         * The flow monitor mechanism is independent of OFPT_FLOW_REMOVED and
         * NXT_FLOW_REMOVED.  Flow monitor updates for deletion are sent if
         * NXFMF_DELETE is set on a monitor, regardless of whether the
         * OFPFF_SEND_FLOW_REM flag was set when the flow was added. */
        
        /* NXST_FLOW_MONITOR request.
         *
         * The NXST_FLOW_MONITOR request's body consists of an array of zero or more
         * instances of this structure.  The request arranges to monitor the flows
         * that match the specified criteria, which are interpreted in the same way as
         * for NXST_FLOW.
         *
         * 'id' identifies a particular monitor for the purpose of allowing it to be
         * canceled later with NXT_FLOW_MONITOR_CANCEL.  'id' must be unique among
         * existing monitors that have not already been canceled.
         *
         * The reply includes the initial flow matches for monitors that have the
         * NXFMF_INITIAL flag set.  No single flow will be included in the reply more
         * than once, even if more than one requested monitor matches that flow.  The
         * reply will be empty if none of the monitors has NXFMF_INITIAL set or if none
         * of the monitors initially matches any flows.
         *
         * For NXFMF_ADD, an event will be reported if 'out_port' matches against the
         * actions of the flow being added or, for a flow that is replacing an existing
         * flow, if 'out_port' matches against the actions of the flow being replaced.
         * For NXFMF_DELETE, 'out_port' matches against the actions of a flow being
         * deleted.  For NXFMF_MODIFY, an event will be reported if 'out_port' matches
         * either the old or the new actions. */
         '''
        ofp_table = namespace['ofp_table']
        nx_flow_monitor_request = nstruct(
            (uint32, 'id'),                     # /* Controller-assigned ID for this monitor. */
            (nx_flow_monitor_flags, 'flags'),   # /* NXFMF_*. */
            (nx_port_no, 'out_port'),           # /* Required output port, if not OFPP_NONE. */
            (uint16, 'match_len'),              # /* Length of nx_match. */
            (ofp_table, 'table_id'),            # /* One table's ID or 0xff for all tables. */
            (uint8[5],),                        # /* Align to 64 bits (must be zero). */
            (nx_matches,),
            name = 'nx_flow_monitor_request',
            base = nx_stats_request,
            criteria = lambda x: getattr(x, stats_subtype) == NXST_FLOW_MONITOR,
            init = packvalue(NXST_FLOW_MONITOR, stats_subtype),
            classifyby = (NXST_FLOW_MONITOR,)
        )
        namespace['nx_flow_monitor_request'] = nx_flow_monitor_request
        '''
        /* NXST_FLOW_MONITOR reply header.
         *
         * The body of an NXST_FLOW_MONITOR reply is an array of variable-length
         * structures, each of which begins with this header.  The 'length' member may
         * be used to traverse the array, and the 'event' member may be used to
         * determine the particular structure.
         *
         * Every instance is a multiple of 8 bytes long. */
        '''
        nx_flow_update = nstruct(
            (uint16, 'length'),            #/* Length of this entry. */
            (nx_flow_update_event, 'event'),        #             /* One of NXFME_*. */
            name = 'nx_flow_update',
            size = lambda x: x.length,
            prepack = packsize('length')
        )
        namespace['nx_flow_update'] = nx_flow_update
    
        '''
        /* NXST_FLOW_MONITOR reply for NXFME_ADDED, NXFME_DELETED, and
         * NXFME_MODIFIED. */
         '''
        nx_flow_update_full = nstruct(
            (ofp_flow_removed_reason, 'reason'),        #            /* OFPRR_* for NXFME_DELETED, else zero. */
            (uint16, 'priority'),                       #          /* Priority of the entry. */
            (uint16, 'idle_timeout'),                   #      /* Number of seconds idle before expiration. */
            (uint16, 'hard_timeout'),                   #      /* Number of seconds before expiration. */
            (uint16, 'match_len'),                      #         /* Length of nx_match. */
            (uint8, 'table_id'),                        #           /* ID of flow's table. */
            (uint8,),                                   #                /* Reserved, currently zeroed. */
            (uint64, 'cookie'),                         #            /* Opaque controller-issued identifier. */
            #=======================================================================
            # /* Followed by:
            #  *   - Exactly match_len (possibly 0) bytes containing the nx_match, then
            #  *   - Exactly (match_len + 7)/8*8 - match_len (between 0 and 7) bytes of
            #  *     all-zero bytes, then
            #  *   - Actions to fill out the remainder 'length' bytes (always a multiple
            #  *     of 8).  If NXFMF_ACTIONS was not specified, or 'event' is
            #  *     NXFME_DELETED, no actions are included.
            #  */
            #=======================================================================
            (nx_matches,),
            (ofp_action[0], 'actions'),
            name = 'nx_flow_update_full',
            base = nx_flow_update,
            criteria = lambda x: x.event in (NXFME_ADDED, NXFME_DELETED, NXFME_MODIFIED),
            init = packvalue(NXFME_ADDED, 'event')
        )
        namespace['nx_flow_update_full'] = nx_flow_update_full
        '''
        /* NXST_FLOW_MONITOR reply for NXFME_ABBREV.
         *
         * When the controller does not specify NXFMF_OWN in a monitor request, any
         * flow tables changes due to the controller's own requests (on the same
         * OpenFlow channel) will be abbreviated, when possible, to this form, which
         * simply specifies the 'xid' of the OpenFlow request (e.g. an OFPT_FLOW_MOD or
         * NXT_FLOW_MOD) that caused the change.
         *
         * Some changes cannot be abbreviated and will be sent in full:
         *
         *   - Changes that only partially succeed.  This can happen if, for example,
         *     a flow_mod with type OFPFC_MODIFY affects multiple flows, but only some
         *     of those modifications succeed (e.g. due to hardware limitations).
         *
         *     This cannot occur with the current implementation of the Open vSwitch
         *     software datapath.  It could happen with other datapath implementations.
         *
         *   - Changes that race with conflicting changes made by other controllers or
         *     other flow_mods (not separated by barriers) by the same controller.
         *
         *     This cannot occur with the current Open vSwitch implementation
         *     (regardless of datapath) because Open vSwitch internally serializes
         *     potentially conflicting changes.
         *
         * A flow_mod that does not change the flow table will not trigger any
         * notification, even an abbreviated one.  For example, a "modify" or "delete"
         * flow_mod that does not match any flows will not trigger a notification.
         * Whether an "add" or "modify" that specifies all the same parameters that a
         * flow already has triggers a notification is unspecified and subject to
         * change in future versions of Open vSwitch.
         *
         * OVS will always send the notifications for a given flow table change before
         * the reply to a OFPT_BARRIER_REQUEST request that follows the flow table
         * change.  Thus, if the controller does not receive an abbreviated (or
         * unabbreviated) notification for a flow_mod before the next
         * OFPT_BARRIER_REPLY, it will never receive one. */
         '''
        nx_flow_update_abbrev = nstruct(
            (uint32, 'xid'),                    # /* Controller-specified xid from flow_mod. */
            name = 'nx_flow_update_abbrev',
            base = nx_flow_update,
            criteria = lambda x: x.event == NXFME_ABBREV,
            init = packvalue(NXFME_ABBREV, 'event')
        )
        namespace['nx_flow_update_abbrev'] = nx_flow_update_abbrev
    
        nx_flow_monitor_reply = nstruct(
            (nx_flow_update[0], 'stats'),
            base = nx_stats_reply,
            classifyby = (NXST_FLOW_MONITOR,),
            name = 'nx_flow_monitor_reply',
            criteria = lambda x: getattr(x, stats_subtype) == NXST_FLOW_MONITOR,
            init = packvalue(NXST_FLOW_MONITOR, stats_subtype)
        )
        namespace['nx_flow_monitor_reply'] = nx_flow_monitor_reply
        '''
        /* NXT_FLOW_MONITOR_CANCEL.
         *
         * Used by a controller to cancel an outstanding monitor. */
        '''
        nx_flow_monitor_cancel = nstruct(
            (uint32, 'id'),                     # /* 'id' from nx_flow_monitor_request. */
            name = 'nx_flow_monitor_cancel',
            base = nicira_header,
            classifyby = (NXT_FLOW_MONITOR_CANCEL,),
            criteria = lambda x: getattr(x, msg_subtype) == NXT_FLOW_MONITOR_CANCEL,
            init = packvalue(NXT_FLOW_MONITOR_CANCEL, msg_subtype)
        )
        namespace['nx_flow_monitor_cancel'] = nx_flow_monitor_cancel
    
        '''
        /* Action structure for NXAST_WRITE_METADATA.
         *
         * Modifies the 'mask' bits of the metadata value. */
        '''
        nx_action_write_metadata = nstruct(
            (uint8[6],),                        # /* Must be zero. */
            (uint64, 'metadata'),               # /* Metadata register. */
            (uint64, 'mask'),                   # /* Metadata mask. */
            base = nx_action,
            classifyby = (NXAST_WRITE_METADATA,),
            criteria = lambda x: getattr(x, action_subtype) == NXAST_WRITE_METADATA,
            init = packvalue(NXAST_WRITE_METADATA, action_subtype),
            name = 'nx_action_write_metadata'
        )
        namespace['nx_action_write_metadata'] = nx_action_write_metadata
    
        '''
        /* Action structure for NXAST_PUSH_MPLS. */
        '''
        nx_action_push_mpls = nstruct(
            (ethertype, 'ethertype'),           # /* Ethertype */
            (uint8[4],),
            base = nx_action,
            classifyby = (NXAST_PUSH_MPLS,),
            criteria = lambda x: getattr(x, action_subtype) == NXAST_PUSH_MPLS,
            init = packvalue(NXAST_PUSH_MPLS, action_subtype),
            name = 'nx_action_push_mpls'
        )
        namespace['nx_action_push_mpls'] = nx_action_push_mpls
    
        '''
        /* Action structure for NXAST_POP_MPLS. */
        '''
        nx_action_pop_mpls = nstruct(
            (ethertype, 'ethertype'),           # /* Ethertype */
            (uint8[4],),
            base = nx_action,
            classifyby = (NXAST_POP_MPLS,),
            criteria = lambda x: getattr(x, action_subtype) == NXAST_POP_MPLS,
            init = packvalue(NXAST_POP_MPLS, action_subtype),
            name = 'nx_action_pop_mpls'
        )
        namespace['nx_action_pop_mpls'] = nx_action_pop_mpls
    
        '''
        /* Action structure for NXAST_SET_MPLS_LABEL. */
        '''
        nx_action_mpls_label = nstruct(
            (uint8[2],),                    # /* Must be zero. */
            (uint32, 'label'),              # /* LABEL */
            base = nx_action,
            classifyby = (NXAST_SET_MPLS_LABEL,),
            criteria = lambda x: getattr(x, action_subtype) == NXAST_SET_MPLS_LABEL,
            init = packvalue(NXAST_SET_MPLS_LABEL, action_subtype),
            name = 'nx_action_mpls_label'
        )
        namespace['nx_action_mpls_label'] = nx_action_mpls_label
    
        '''
        /* Action structure for NXAST_SET_MPLS_TC. */
        '''
        nx_action_mpls_tc = nstruct(
            (uint8, 'tc'),                      # /* TC */
            (uint8[5],),
            base = nx_action,
            classifyby = (NXAST_SET_MPLS_TC,),
            criteria = lambda x: getattr(x, action_subtype) == NXAST_SET_MPLS_TC,
            init = packvalue(NXAST_SET_MPLS_TC, action_subtype),
            name = 'nx_action_mpls_tc'
        )
        namespace['nx_action_mpls_tc'] = nx_action_mpls_tc
    
        '''
        /* Action structure for NXAST_SET_MPLS_TTL. */
        '''
        nx_action_mpls_ttl = nstruct(
            (uint8,  'ttl'),                    # /* TTL */
            (uint8[5],),
            base = nx_action,
            classifyby = (NXAST_SET_MPLS_TTL,),
            criteria = lambda x: getattr(x, action_subtype) == NXAST_SET_MPLS_TTL,
            init = packvalue(NXAST_SET_MPLS_TTL, action_subtype),
            name = 'nx_action_mpls_ttl'
        )
        namespace['nx_action_mpls_ttl'] = nx_action_mpls_ttl
    
        '''
        /* Action structure for NXAST_SAMPLE.
         *
         * Samples matching packets with the given probability and sends them
         * each to the set of collectors identified with the given ID.  The
         * probability is expressed as a number of packets to be sampled out
         * of USHRT_MAX packets, and must be >0.
         *
         * When sending packet samples to IPFIX collectors, the IPFIX flow
         * record sent for each sampled packet is associated with the given
         * observation domain ID and observation point ID.  Each IPFIX flow
         * record contain the sampled packet's headers when executing this
         * rule.  If a sampled packet's headers are modified by previous
         * actions in the flow, those modified headers are sent. */
        '''
        nx_action_sample = nstruct(
            (uint16, 'probability'),            #           /* Fraction of packets to sample. */
            (uint32, 'collector_set_id'),       #      /* ID of collector set in OVSDB. */
            (uint32, 'obs_domain_id'),          #         /* ID of sampling observation domain. */
            (uint32, 'obs_point_id'),           #          /* ID of sampling observation point. */
            base = nx_action,
            classifyby = (NXAST_SAMPLE,),
            criteria = lambda x: getattr(x, action_subtype) == NXAST_SAMPLE,
            init = packvalue(NXAST_SAMPLE, action_subtype),
            name = 'nx_action_sample'
        )
        namespace['nx_action_sample'] = nx_action_sample
        #=======================================================================
        # /* Action structure for NXAST_SAMPLE2 and NXAST_SAMPLE3.
        #  *
        #  * NXAST_SAMPLE2 was added in Open vSwitch 2.5.90.  Compared to NXAST_SAMPLE,
        #  * it adds support for exporting egress tunnel information.
        #  *
        #  * NXAST_SAMPLE3 was added in Open vSwitch 2.6.90.  Compared to NXAST_SAMPLE2,
        #  * it adds support for the 'direction' field. */
        #=======================================================================
        nx_action_sample2 = \
            nstruct(
                (uint16, 'probability'),          # /* Fraction of packets to sample. */
                (uint32, 'collector_set_id'),     # /* ID of collector set in OVSDB. */
                (uint32, 'obs_domain_id'),        # /* ID of sampling observation domain. */
                (uint32, 'obs_point_id'),         # /* ID of sampling observation point. */
                (nx_port_no, 'sampling_port'),    # /* Sampling port. */
                (nx_action_sample_direction, 'direction'),
                                                  # /* NXAST_SAMPLE3 only. */
                (uint8[5],),                      # /* Pad to a multiple of 8 bytes */
                base = nx_action,
                classifyby = (NXAST_SAMPLE2, NXAST_SAMPLE3),
                criteria = lambda x: getattr(x, action_subtype) in (NXAST_SAMPLE2, NXAST_SAMPLE3),
                init = packvalue(NXAST_SAMPLE2, action_subtype),
                name = 'nx_action_sample2'
            )
        namespace['nx_action_sample2'] = nx_action_sample2
        # /* Action structure for NXAST_CONJUNCTION. */
        nx_action_conjunction = \
            nstruct(
                (uint8, 'clause'),
                (uint8, 'n_clauses'),
                (uint32, 'id'),
                base = nx_action,
                classifyby = (NXAST_CONJUNCTION,),
                criteria = lambda x: getattr(x, action_subtype) == NXAST_CONJUNCTION,
                init = packvalue(NXAST_CONJUNCTION, action_subtype),
                name = 'nx_action_conjunction'
            )
        namespace['nx_action_conjunction'] = nx_action_conjunction
        
        #=======================================================================
        # /* Action structure for NXAST_CT.
        #  *
        #  * Pass traffic to the connection tracker.
        #  *
        #  * There are two important concepts to understanding the connection tracking
        #  * interface: Packet state and Connection state. Packets may be "Untracked" or
        #  * "Tracked". Connections may be "Uncommitted" or "Committed".
        #  *
        #  *   - Packet State:
        #  *
        #  *      Untracked packets have an unknown connection state.  In most
        #  *      cases, packets entering the OpenFlow pipeline will initially be
        #  *      in the untracked state. Untracked packets may become tracked by
        #  *      executing NXAST_CT with a "recirc_table" specified. This makes
        #  *      various aspects about the connection available, in particular
        #  *      the connection state.
        #  *
        #  *      An NXAST_CT action always puts the packet into an untracked
        #  *      state for the current processing path.  If "recirc_table" is
        #  *      set, execution is forked and the packet passes through the
        #  *      connection tracker.  The specified table's processing path is
        #  *      able to match on Connection state until the end of the OpenFlow
        #  *      pipeline or NXAST_CT is called again.
        #  *
        #  *   - Connection State:
        #  *
        #  *      Multiple packets may be associated with a single connection. Initially,
        #  *      all connections are uncommitted. The connection state corresponding to
        #  *      a packet is available in the NXM_NX_CT_STATE field for tracked packets.
        #  *
        #  *      Uncommitted connections have no state stored about them. Uncommitted
        #  *      connections may transition into the committed state by executing
        #  *      NXAST_CT with the NX_CT_F_COMMIT flag.
        #  *
        #  *      Once a connection becomes committed, information may be gathered about
        #  *      the connection by passing subsequent packets through the connection
        #  *      tracker, and the state of the connection will be stored beyond the
        #  *      lifetime of packet processing.
        #  *
        #  *      A committed connection always has the directionality of the packet that
        #  *      caused the connection to be committed in the first place.  This is the
        #  *      "original direction" of the connection, and the opposite direction is
        #  *      the "reply direction".  If a connection is already committed, but it is
        #  *      then decided that the original direction should be the opposite of the
        #  *      existing connection, NX_CT_F_FORCE flag may be used in addition to
        #  *      NX_CT_F_COMMIT flag to in effect terminate the existing connection and
        #  *      start a new one in the current direction.
        #  *
        #  *      Connections may transition back into the uncommitted state due to
        #  *      external timers, or due to the contents of packets that are sent to the
        #  *      connection tracker. This behaviour is outside of the scope of the
        #  *      OpenFlow interface.
        #  *
        #  * The "zone" specifies a context within which the tracking is done:
        #  *
        #  *      The connection tracking zone is a 16-bit number. Each zone is an
        #  *      independent connection tracking context. The connection state for each
        #  *      connection is completely separate for each zone, so if a connection
        #  *      is committed to zone A, then it will remain uncommitted in zone B.
        #  *      If NXAST_CT is executed with the same zone multiple times, later
        #  *      executions have no effect.
        #  *
        #  *      If 'zone_src' is nonzero, this specifies that the zone should be
        #  *      sourced from a field zone_src[ofs:ofs+nbits]. The format and semantics
        #  *      of 'zone_src' and 'zone_ofs_nbits' are similar to those for the
        #  *      NXAST_REG_LOAD action. The acceptable nxm_header values for 'zone_src'
        #  *      are the same as the acceptable nxm_header values for the 'src' field of
        #  *      NXAST_REG_MOVE.
        #  *
        #  *      If 'zone_src' is zero, then the value of 'zone_imm' will be used as the
        #  *      connection tracking zone.
        #  *
        #  * The "recirc_table" allows NXM_NX_CT_* fields to become available:
        #  *
        #  *      If "recirc_table" has a value other than NX_CT_RECIRC_NONE, then the
        #  *      packet will be logically cloned prior to executing this action. One
        #  *      copy will be sent to the connection tracker, then will be re-injected
        #  *      into the OpenFlow pipeline beginning at the OpenFlow table specified in
        #  *      this field. When the packet re-enters the pipeline, the NXM_NX_CT_*
        #  *      fields will be populated. The original instance of the packet will
        #  *      continue the current actions list. This can be thought of as similar to
        #  *      the effect of the "output" action: One copy is sent out (in this case,
        #  *      to the connection tracker), but the current copy continues processing.
        #  *
        #  *      It is strongly recommended that this table is later than the current
        #  *      table, to prevent loops.
        #  *
        #  * The "alg" attaches protocol-specific behaviour to this action:
        #  *
        #  *      The ALG is a 16-bit number which specifies that additional
        #  *      processing should be applied to this traffic.
        #  *
        #  *      Protocol | Value | Meaning
        #  *      --------------------------------------------------------------------
        #  *      None     |     0 | No protocol-specific behaviour.
        #  *      FTP      |    21 | Parse FTP control connections and observe the
        #  *               |       | negotiation of related data connections.
        #  *      Other    | Other | Unsupported protocols.
        #  *
        #  *      By way of example, if FTP control connections have this action applied
        #  *      with the ALG set to FTP (21), then the connection tracker will observe
        #  *      the negotiation of data connections. This allows the connection
        #  *      tracker to identify subsequent data connections as "related" to this
        #  *      existing connection. The "related" flag will be populated in the
        #  *      NXM_NX_CT_STATE field for such connections if the 'recirc_table' is
        #  *      specified.
        #  *
        #  * Zero or more actions may immediately follow this action. These actions will
        #  * be executed within the context of the connection tracker, and they require
        #  * NX_CT_F_COMMIT flag be set.
        #  */
        #=======================================================================
        
        def _nx_action_conntrack_prepack(x):
            if x.zone_src == 0:
                if not hasattr(x, 'zone_imm'):
                    x.zone_imm = 0
                if hasattr(x, 'zone_ofs_nbits'):
                    del x.zone_ofs_nbits
            else:
                if not hasattr(x, 'zone_ofs_nbits'):
                    raise ValueError("Must specify zone_ofs_nbits when zone_src != 0")
                if hasattr(x, 'zone_imm'):
                    del x.zone_imm
        
        _nx_action_conntrack_prepack_union = \
            nstruct(
                name = '_nx_action_conntrack_prepack_union',
                padding = 1,
                init = packvalue(0, 'zone_imm'),
                prepack = _nx_action_conntrack_prepack
            )
        
        nx_action_conntrack = \
            nstruct(
                (nx_conntrack_flags, 'flags'),
                                            # /* Zero or more NX_CT_F_* flags.
                                            #  * Unspecified flag bits must be zero. */
                (all_nxm_header, 'zone_src'),
                                            # /* Connection tracking context. */
                (_nx_action_conntrack_prepack_union,),
                (optional(ofs_nbits_type, 'zone_ofs_nbits',
                          lambda x: x.zone_src != 0),),
                                            # /* Range to use from source field. */
                (optional(uint16, 'zone_imm', lambda x: x.zone_src == 0),),
                                            # /* Immediate value for zone. */
                (ofp_table, 'recirc_table'),# /* Recirculate to a specific table, or
                                            #    NX_CT_RECIRC_NONE for no recirculation. */
                (uint8[3],),                # /* Zeroes */
                (nx_conntrack_alg_port, 'alg'),
                                            # /* Well-known port number for the protocol.
                                            #  * 0 indicates no ALG is required. */
                # /* Followed by a sequence of zero or more OpenFlow actions. The length of
                #  * these is included in 'len'. */
                (namespace['ofp_action'][0], 'sub_actions'),
                name = 'nx_action_conntrack',
                base = nx_action,
                classifyby = (NXAST_CT,),
                criteria = lambda x: getattr(x, action_subtype) == NXAST_CT,
                init = packvalue(NXAST_CT, action_subtype),
            )
        namespace['nx_action_conntrack'] = nx_action_conntrack
        
        def _pack_flag(field, flag, value):
            def _pack_flag_prepack(x):
                if hasattr(x, field):
                    setattr(x, flag, getattr(x, flag) | value)
                else:
                    setattr(x, flag, getattr(x, flag) & (~value))
            return _pack_flag_prepack
        
        def _nat_range(type_, field, value):
            return optional(type_, field, lambda x: x.range_present & value,
                            _pack_flag(field, 'range_present', value))

        # /* Action structure for NXAST_NAT. */
        nx_action_nat = \
            nstruct(
                (uint8[2],),                # /* Must be zero. */
                (nx_nat_flags, 'flags'),    # /* Zero or more NX_NAT_F_* flags.
                                            #  * Unspecified flag bits must be zero. */
                (nx_nat_range, 'range_present'),
                                            # /* NX_NAT_RANGE_* */
                # /* Followed by optional parameters as specified by 'range_present' */
                (_nat_range(ip4_addr, 'ipv4_min', NX_NAT_RANGE_IPV4_MIN),),
                (_nat_range(ip4_addr, 'ipv4_max', NX_NAT_RANGE_IPV4_MAX),),
                (_nat_range(ip6_addr, 'ipv6_min', NX_NAT_RANGE_IPV6_MIN),),
                (_nat_range(ip6_addr, 'ipv6_max', NX_NAT_RANGE_IPV6_MAX),),
                (_nat_range(uint16, 'ipv6_max', NX_NAT_RANGE_IPV6_MAX),),
                name = 'nx_action_nat',
                base = nx_action,
                classifyby = (NXAST_NAT,),
                criteria = lambda x: getattr(x, action_subtype) == NXAST_NAT,
                init = packvalue(NXAST_NAT, action_subtype),
            )
        namespace['nx_action_nat'] = nx_action_nat

        # /* Truncate output action. */
        nx_action_output_trunc = \
            nstruct(
                (nx_port_no, 'port'),             # /* Output port */
                (uint32, 'max_len'),              # /* Truncate packet to size bytes */
                name = 'nx_action_output_trunc',
                base = nx_action,
                classifyby = (NXAST_OUTPUT_TRUNC,),
                criteria = lambda x: getattr(x, action_subtype) == NXAST_OUTPUT_TRUNC,
                init = packvalue(NXAST_OUTPUT_TRUNC, action_subtype),
            )
        namespace['nx_action_output_trunc'] = nx_action_output_trunc
        
        # /* The NXAST_CLONE action is "struct ext_action_header", followed by zero or
        #  * more embedded OpenFlow actions. */
        nx_action_clone = \
            nstruct(
                (uint8[6],),
                (namespace['ofp_action'][0], 'sub_actions'),
                name = 'nx_action_clone',
                base = nx_action,
                classifyby = (NXAST_CLONE,),
                criteria = lambda x: getattr(x, action_subtype) == NXAST_CLONE,
                init = packvalue(NXAST_CLONE, action_subtype),
            )
        namespace['nx_action_clone'] = nx_action_clone
        
        # /* ct_clear action. */
        nx_action_ct_clear = \
            nstruct(
                name = 'nx_action_ct_clear',
                base = nx_action,
                classifyby = (NXAST_CT_CLEAR,),
                criteria = lambda x: getattr(x, action_subtype) == NXAST_CT_CLEAR,
                init = packvalue(NXAST_CT_CLEAR, action_subtype),
            )
        namespace['nx_action_ct_clear'] = nx_action_ct_clear

        # /* Action structure for NXAST_ENCAP */
        nx_action_encap = \
            nstruct(
                (uint16, 'hdr_size'),       # /* Header size in bytes, 0 = 'not specified'.*/
                (packet_type, 'new_pkt_type'),
                                            # /* Header type to add and PACKET_TYPE of result. */
                (ofp_ed_prop[0], 'props'),  # /* Encap TLV properties. */
                name = 'nx_action_encap',
                base = nx_action,
                classifyby = (NXAST_ENCAP,),
                criteria = lambda x: getattr(x, action_subtype) == NXAST_ENCAP,
                init = packvalue(NXAST_ENCAP, action_subtype),
            )
        namespace['nx_action_encap'] = nx_action_encap

        nx_action_decap = \
            nstruct(
                (uint8[2],),                # /* 2 bytes padding */
            
                # /* Packet type or result.
                #  *
                #  * The special value (0,0xFFFE) "Use next proto"
                #  * is used to request OVS to automatically set the new packet type based
                #  * on the decap'ed header's next protocol.
                #  */
                (packet_type, 'new_pkt_type'),
                name = 'nx_action_decap',
                base = nx_action,
                classifyby = (NXAST_DECAP,),
                criteria = lambda x: getattr(x, action_subtype) == NXAST_DECAP,
                init = packvalue(NXAST_DECAP, action_subtype),
            )
        namespace['nx_action_decap'] = nx_action_decap
        
        # /* Action dec_nsh_ttl */
        nx_action_dec_nsh_ttl = \
            nstruct(
                name = 'nx_action_dec_nsh_ttl',
                base = nx_action,
                classifyby = (NXAST_DEC_NSH_TTL,),
                criteria = lambda x: getattr(x, action_subtype) == NXAST_DEC_NSH_TTL,
                init = packvalue(NXAST_DEC_NSH_TTL, action_subtype),
            )
        namespace['nx_action_dec_nsh_ttl'] = nx_action_dec_nsh_ttl
        
        
        #=======================================================================
        # /* Variable-length option TLV table maintenance commands.
        #  *
        #  * The option in Type-Length-Value format is widely used in tunnel options,
        #  * e.g., the base Geneve header is followed by zero or more options in TLV
        #  * format. Each option consists of a four byte option header and a variable
        #  * amount of option data interpreted according to the type. The generic TLV
        #  * format in tunnel options is as following:
        #  *
        #  * 0                   1                   2                   3
        #  * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
        #  * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        #  * |          Option Class         |      Type     |R|R|R| Length  |
        #  * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        #  * |                      Variable Option Data                     |
        #  * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        #  *
        #  * In order to work with this variable-length options in TLV format in
        #  * tunnel options, we need to maintain a mapping table between an option
        #  * TLV (defined by <class, type, length>) and an NXM field that can be
        #  * operated on for the purposes of matches, actions, etc. This mapping
        #  * must be explicitly specified by the user.
        #  *
        #  * There are two primary groups of OpenFlow messages that are introduced
        #  * as Nicira extensions: modification commands (add, delete, clear mappings)
        #  * and table status request/reply to dump the current table along with switch
        #  * information.
        #  *
        #  * Note that mappings should not be changed while they are in active use by
        #  * a flow. The result of doing so is undefined. */
        #=======================================================================
                
        # /* Map between an option TLV and an NXM field. */
        nx_tlv_map = \
            nstruct(
                (uint16, 'option_class'),   # /* TLV class. */
                (uint8,  'option_type'),    # /* TLV type. */
                (uint8,  'option_len'),     # /* TLV length (multiple of 4). */
                (uint16, 'index'),          # /* NXM_NX_TUN_METADATA<n> index */
                (uint8[2],),
                name = 'nx_tlv_map'
            )
        
        namespace['nx_tlv_map'] = nx_tlv_map
        # /* NXT_TLV_TABLE_MOD.
        #  *
        #  * Use to configure a mapping between option TLVs (class, type, length)
        #  * and NXM fields (NXM_NX_TUN_METADATA<n> where 'index' is <n>).
        #  *
        #  * This command is atomic: all operations on different options will
        #  * either succeed or fail. */
        nx_tlv_table_mod = \
            nstruct(
                (nx_tlv_table_mod_command, 'command'),           # /* One of NTTTMC_* */
                (uint8[6],),
                # /* struct nx_tlv_map[0]; Array of maps between indicies and option
                #    TLVs. The number of elements is inferred
                #    from the length field in the header. */
                (nx_tlv_map[0], 'maps'),
                name = 'nx_tlv_table_mod',
                base = nicira_header,
                classifyby = (NXT_TLV_TABLE_MOD,),
                criteria = lambda x: getattr(x, msg_subtype) == NXT_TLV_TABLE_MOD,
                init = packvalue(NXT_TLV_TABLE_MOD, msg_subtype)
            )
        namespace['nx_tlv_table_mod'] = nx_tlv_table_mod
        
        nx_tlv_table_request = \
            nstruct(
                name = 'nx_tlv_table_request',
                base = nicira_header,
                classifyby = (NXT_TLV_TABLE_REQUEST,),
                criteria = lambda x: getattr(x, msg_subtype) == NXT_TLV_TABLE_REQUEST,
                init = packvalue(NXT_TLV_TABLE_REQUEST, msg_subtype)
            )
        namespace['nx_tlv_table_request'] = nx_tlv_table_request
        
        # /* NXT_TLV_TABLE_REPLY.
        #  *
        #  * Issued in reponse to an NXT_TLV_TABLE_REQUEST to give information
        #  * about the current status of the TLV table in the switch. Provides
        #  * both static information about the switch's capabilities as well as
        #  * the configured TLV table. */
        nx_tlv_table_reply = \
            nstruct(
                (uint32, 'max_option_space'),   # /* Maximum total of option sizes supported. */
                (uint16, 'max_fields'),         # /* Maximum number of match fields supported. */
                (uint8[10],),
                # /* struct nx_tlv_map[0]; Array of maps between indicies and option
                #                             TLVs. The number of elements is inferred
                #                             from the length field in the header. */
                (nx_tlv_map[0], 'maps'),
                name = 'nx_tlv_table_reply',
                base = nicira_header,
                classifyby = (NXT_TLV_TABLE_REPLY,),
                criteria = lambda x: getattr(x, msg_subtype) == NXT_TLV_TABLE_REPLY,
                init = packvalue(NXT_TLV_TABLE_REPLY, msg_subtype)
            )
        namespace['nx_tlv_table_reply'] = nx_tlv_table_reply
        
        nx_resume = \
            nstruct(
                (nx_packet_in2_prop[0], 'properties'),
                name = 'nx_resume',
                base = nicira_header,
                criteria = lambda x: getattr(x, msg_subtype) == NXT_RESUME,
                classifyby = (NXT_RESUME,),
                init = packvalue(NXT_RESUME, msg_subtype)
            )
        
        namespace['nx_resume'] = nx_resume
        
        nx_zone_id = \
            nstruct(
                (uint8[6],),                     # /* Must be zero. */
                (uint16, 'zone_id'),             # /* Connection tracking zone. */
                name = 'nx_zone_id',
                base = nicira_header,
                criteria = lambda x: getattr(x, msg_subtype) == NXT_CT_FLUSH_ZONE,
                classifyby = (NXT_CT_FLUSH_ZONE,),
                init = packvalue(NXT_CT_FLUSH_ZONE, msg_subtype)
            )
        
        namespace['nx_zone_id'] = nx_zone_id
        
        nx_ipfix_bridge_stats_request = \
            nstruct(
                base = nx_stats_request,
                name = 'nx_ipfix_bridge_stats_request',
                criteria = lambda x: getattr(x, stats_subtype) == NXST_IPFIX_BRIDGE,
                classifyby = (NXST_IPFIX_BRIDGE,),
                init = packvalue(NXST_IPFIX_BRIDGE, stats_subtype)
            )
        namespace['nx_ipfix_bridge_stats_request'] = nx_ipfix_bridge_stats_request

        nx_ipfix_stats = \
            nstruct(
                (uint64, 'total_flows'),
                (uint64, 'current_flows'),
                (uint64, 'pkts'),
                (uint64, 'ipv4_pkts'),
                (uint64, 'ipv6_pkts'),
                (uint64, 'error_pkts'),
                (uint64, 'ipv4_error_pkts'),
                (uint64, 'ipv6_error_pkts'),
                (uint64, 'tx_pkts'),
                (uint64, 'tx_errors'),
                (uint32, 'collector_set_id'),       # /* Range 0 to 4,294,967,295. */
                (uint8[4],),                        # /* Pad to a multiple of 8 bytes. */
                name = 'nx_ipfix_stats'
            )
        namespace['nx_ipfix_stats'] = nx_ipfix_stats
        
        nx_ipfix_bridge_stats_reply = \
            nstruct(
                (nx_ipfix_stats,),
                base = nx_stats_reply,
                name = 'nx_ipfix_bridge_stats_reply',
                criteria = lambda x: getattr(x, stats_subtype) == NXST_IPFIX_BRIDGE,
                classifyby = (NXST_IPFIX_BRIDGE,),
                init = packvalue(NXST_IPFIX_BRIDGE, stats_subtype)
            )
        namespace['nx_ipfix_bridge_stats_reply'] = nx_ipfix_bridge_stats_reply
        
        nx_ipfix_flow_stats_request = \
            nstruct(
                base = nx_stats_request,
                name = 'nx_ipfix_flow_stats_request',
                criteria = lambda x: getattr(x, stats_subtype) == NXST_IPFIX_FLOW,
                classifyby = (NXST_IPFIX_FLOW,),
                init = packvalue(NXST_IPFIX_FLOW, stats_subtype)
            )
        namespace['nx_ipfix_flow_stats_request'] = nx_ipfix_flow_stats_request

        nx_ipfix_flow_stats_reply = \
            nstruct(
                (nx_ipfix_stats[0], 'stats'),
                base = nx_stats_reply,
                name = 'nx_ipfix_flow_stats_reply',
                criteria = lambda x: getattr(x, stats_subtype) == NXST_IPFIX_FLOW,
                classifyby = (NXST_IPFIX_FLOW,),
                init = packvalue(NXST_IPFIX_FLOW, stats_subtype)
            )
        namespace['nx_ipfix_flow_stats_reply'] = nx_ipfix_flow_stats_reply
        
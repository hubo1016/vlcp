#!/usr/bin/python
#! --*-- utf-8 --*--

import logging

from vlcp.utils.ethernet import *
from vlcp.protocol.openflow import common

TAG_TABLE_ID = 0
ACL_TABLE_ID = 2
PRE_ROUTINE_TABLE_ID = 4
FUN_TABLE_ID = 6
ARP_TABLE_ID = 8
IP_TABLE_ID = 10
MAC_TABLE_ID = 12
MAC_LEARN_TABLE_ID = 20
POS_ROUTINE_TABLE_ID = 14
UNTAG_TABLE_ID = 16

logger = logging.getLogger("ofpctl")

class OfpCtl(object):
    def __init__(self,parser,protocol,connection):
        super(OfpCtl,self).__init__()
        
        self.ofp_parser = parser
        self.ofp_protocol = protocol
        self.conn = connection
    def add_flow(self,container,match,ins,buffer_id,cookie = 0,cookie_mask = 0,
            table_id = 0,priority = 0,idle_time = 0,hard_time = 0,flag = 0):

        assert match != None
        assert ins != None
        assert buffer_id != None
        
        flow = self.ofp_parser.ofp_flow_mod()
        flow.table_id = table_id
        flow.command = self.ofp_parser.OFPFC_ADD
        flow.cookie = cookie
        flow.cookie_mask = cookie_mask
        flow.priority = priority
        flow.idle_timeout = idle_time
        flow.hard_timeout = hard_time
        flow.buffer_id = buffer_id
        flow.flags = flag
        flow.match = match

        for insi in ins:
            flow.instructions.append(insi)
         
        """
        for m in self.ofp_protocol.batch([flow],self.conn,container):
            yield m
        """

        for m in self.conn.write(self.ofp_protocol.formatrequest(flow,self.conn)):
            yield m
    
    def del_flow(self,container,match,table_id = 0,cookie = 0,cookie_mask = 0,
            outport = 0,outgroup = 0):

        assert match != None

        flow = self.ofp_parser.ofp_flow_mod()
        flow.command = self.ofp_parser.OFPFC_DELETE
        flow.match = match
        flow.table_id = table_id
        flow.cookie = cookie
        flow.cookie = cookie_mask
        flow.out_port = outport
        flow.out_group = outgroup
        
        for m in self.conn.write(self.ofp_protocol.formatrequest(flow,self.conn)):
            yield m

class OfpCtlViper(OfpCtl):
    def add_default_flow(self,container):

        # delete every flow first
        match = self.ofp_parser.ofp_match_oxm()
        for m in self.del_flow(container,match,table_id = self.ofp_parser.OFPTT_ALL,
                outport = self.ofp_parser.OFPP_ANY,outgroup = self.ofp_parser.OFPG_ANY):
            yield m
        
        # delete every group table
        for m in self.del_group_flow(networkId = self.ofp_parser.OFPG_ALL):
            yield m
        
        # first table TAG table id default drop
        match = self.ofp_parser.ofp_match_oxm()
        ins = self.ofp_parser.ofp_instruction_actions(type=self.ofp_parser.OFPIT_CLEAR_ACTIONS)
         
        for m in self.add_flow(container,match,[ins,],self.ofp_parser.OFP_NO_BUFFER,
                table_id = TAG_TABLE_ID):
            yield m
        
        # table ACL table id default goto table PRE_ROUTINE_TABLE_ID 
        match = self.ofp_parser.ofp_match_oxm()
        ins = self.ofp_parser.ofp_instruction_goto_table(table_id = PRE_ROUTINE_TABLE_ID)
        
        for m in self.add_flow(container,match,[ins,],self.ofp_parser.OFP_NO_BUFFER,
                table_id = ACL_TABLE_ID):
            yield m

        # table PRE_ROUTINE_TABLE_ID default goto table FUN_TABLE_ID
        match = self.ofp_parser.ofp_match_oxm()
        ins = self.ofp_parser.ofp_instruction_goto_table(table_id = FUN_TABLE_ID)
        
        for m in self.add_flow(container,match,[ins,],self.ofp_parser.OFP_NO_BUFFER,
                table_id = PRE_ROUTINE_TABLE_ID):
            yield m
        
        # table FUN_TABLE_ID default drop
        match = self.ofp_parser.ofp_match_oxm()
        ins = self.ofp_parser.ofp_instruction_actions(type=self.ofp_parser.OFPIT_CLEAR_ACTIONS)
         
        for m in self.add_flow(container,match,[ins,],self.ofp_parser.OFP_NO_BUFFER,
                table_id = FUN_TABLE_ID):
            yield m
        
        # table FUN_TABLE_ID  add ARP packet goto ARP_TABLE_ID
        # (arp table handle same mac packet , lookup mac learn table
        #  goto broadcast group)
        match = self.ofp_parser.ofp_match_oxm()
        oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_ETH_TYPE,ETHERTYPE_ARP)
        match.oxm_fields.append(oxm)
        
        #ins = self.ofp_parser.ofp_instruction_goto_table(table_id = ARP_TABLE_ID)
        
        ins = self.ofp_parser.ofp_instruction_actions(type = self.ofp_parser.OFPIT_APPLY_ACTIONS)
        action1 = self.ofp_parser.nx_action_resubmit(in_port=self.ofp_parser.OFPP_IN_PORT & 0xffff, 
                table = MAC_LEARN_TABLE_ID)
        ins.actions.append(action1)

        action2 = self.ofp_parser.nx_action_resubmit(in_port=self.ofp_parser.OFPP_IN_PORT & 0xffff,
                table = ARP_TABLE_ID)
        ins.actions.append(action2)
        for m in self.add_flow(container,match,[ins,],self.ofp_parser.OFP_NO_BUFFER,
                table_id = FUN_TABLE_ID,priority = 200):
            yield m
        
        """
        ### change it resubmit mac_learn_table_id , resubmit mac_table_id
        # table FUN_TABLE_ID add other packet goto MAC_TABLE_ID
        match = self.ofp_parser.ofp_match_oxm()
        ins = self.ofp_parser.ofp_instruction_goto_table(table_id = MAC_TABLE_ID)
         
        for m in self.add_flow(container,match,[ins,],self.ofp_parser.OFP_NO_BUFFER,
                table_id = FUN_TABLE_ID,priority = 50):
            yield m
        """
        match = self.ofp_parser.ofp_match_oxm()
        ins = self.ofp_parser.ofp_instruction_actions(type=self.ofp_parser.OFPIT_APPLY_ACTIONS)
        action1 = self.ofp_parser.nx_action_resubmit(in_port=self.ofp_parser.OFPP_IN_PORT & 0xffff,
                table = MAC_LEARN_TABLE_ID)
        
        ins.actions.append(action1)
        action2 = self.ofp_parser.nx_action_resubmit(in_port=self.ofp_parser.OFPP_IN_PORT & 0xffff,
                table = MAC_TABLE_ID)
        
        ins.actions.append(action2)
        
        for m in self.add_flow(container,match,[ins,],self.ofp_parser.OFP_NO_BUFFER,
                table_id = FUN_TABLE_ID,priority = 50):
            yield m
        
        # table MAC_TABLE_ID default goto POS_ROUTINE_TABLE_ID
        match = self.ofp_parser.ofp_match_oxm()
        ins = self.ofp_parser.ofp_instruction_goto_table(table_id = POS_ROUTINE_TABLE_ID)
        
        for m in self.add_flow(container,match,[ins,],self.ofp_parser.OFP_NO_BUFFER,
                table_id = MAC_TABLE_ID):
            yield m

        # table POS_ROUTINE_TABLE_ID default goto UNTAG_TABLE_ID 
        match = self.ofp_parser.ofp_match_oxm()
        ins = self.ofp_parser.ofp_instruction_goto_table(table_id = UNTAG_TABLE_ID)
        
        for m in self.add_flow(container,match,[ins,],self.ofp_parser.OFP_NO_BUFFER,
                table_id = POS_ROUTINE_TABLE_ID):
            yield m

    def add_default_acl(self,container,networkId):

        #src mac == broadcast drop
        match = self.ofp_parser.ofp_match_oxm()
        oxm = self.ofp_parser.create_oxm(self.ofp_parser.NXM_NX_REG5,networkId)
        match.oxm_fields.append(oxm)

        oxm = self.ofp_parser.create_oxm(self.ofp_parser.OXM_OF_ETH_SRC,
                [255,255,255,255,255,255])
        match.oxm_fields.append(oxm)

        ins = self.ofp_parser.ofp_instruction_actions(type = self.ofp_parser.OFPIT_CLEAR_ACTIONS)
        
        for m in self.add_flow(container,match,[ins,],self.ofp_parser.OFP_NO_BUFFER,
                table_id = ACL_TABLE_ID,priority = 50):
            yield m

        #  we should drop packet with VID ??
    
    def create_group_flow(self,networkId):
        
        # now we only use type ALL 
        group = self.ofp_parser.ofp_group_mod(command = self.ofp_parser.OFPGC_ADD,
                type = self.ofp_parser.OFPGT_ALL,group_id = networkId)
        
        for m in self.conn.write(self.ofp_protocol.formatrequest(group,self.conn)):
            yield m
    
    def del_group_flow(self,networkId):
        
        group = self.ofp_parser.ofp_group_mod(command = self.ofp_parser.OFPGC_DELETE,
                type = self.ofp_parser.OFPGT_ALL,group_id = networkId)

        for m in self.conn.write(self.ofp_protocol.formatrequest(group,self.conn)):
            yield m
    def mod_group_flow(self,container,networkId,bucket = []):
        group = self.ofp_parser.ofp_group_mod(command = self.ofp_parser.OFPGC_MODIFY,
                type = self.ofp_parser.OFPGT_ALL,group_id = networkId,
                buckets = bucket)

        for m in self.conn.write(self.ofp_protocol.formatrequest(group,self.conn)):
            yield m


    """

    #
    # there is a problem,, many routine call this, 
    # read bucket in group, write new bucket, IT IS NOT TRANSACTION
    # result is not correct!
    #
    def mod_group_flow(self,container,networkId,bucket = []):
        
        buckets = []
        
        logger.info(" ---- mod_group_flow ----")

        # of1.3 have no insert bucket
        # so we must check the old bucket and add the new to buckets

        groupdesc = self.ofp_parser.ofp_multipart_request(type = self.ofp_parser.OFPMP_GROUP_DESC)
        for m in self.ofp_protocol.querymultipart(groupdesc,self.conn,container):
            yield m
        
        for reply in container.openflow_reply:
            logger.info(" ---- reply = %r",common.dump(reply))
            for g in reply.stats:
                if g.group_id == networkId:
                    for b in g.buckets:
                        logger.info(" we check one b")
                        buckets.append(b)
        
        # add new bucket
        buckets.append(*bucket)
        
        group = self.ofp_parser.ofp_group_mod(command = self.ofp_parser.OFPGC_MODIFY,
                type = self.ofp_parser.OFPGT_ALL,
                group_id = networkId,buckets = buckets)

        for m in self.conn.write(self.ofp_protocol.formatrequest(group,self.conn)):
            yield m
        logger.info(" ----- mod_group_flow complete ---")
     """

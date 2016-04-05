#! /usr/bin/python
#! --*-- utf-8 --*--

import logging

from vlcp.config import defaultconfig
from vlcp.server.module import Module,depend,ModuleNotification
from vlcp.event.runnable import RoutineContainer
from vlcp.service.sdn import viperflow,ofpmanager
from vlcp.service.sdn import ofpctl
from vlcp.utils.ethernet import *
from vlcp.service.sdn.ofpctl import ACL_TABLE_ID,TAG_TABLE_ID,MAC_TABLE_ID,POS_ROUTINE_TABLE_ID,UNTAG_TABLE_ID,ARP_TABLE_ID,MAC_LEARN_TABLE_ID,PRE_ROUTINE_TABLE_ID,FUN_TABLE_ID


logger = logging.getLogger('l2')

PHYNET_NUMBER = 0
LOGICNET_NUMBER = 0
L2_APP_SERIAL_NO = 1

@defaultconfig
@depend(viperflow.ViperFlow,ofpmanager.OpenflowManager)
class l2(Module):
    def __init__(self,server):
        super(l2,self).__init__(server)

        self.app_routine = RoutineContainer(self.scheduler)
        self.app_routine.main = self._main
        self.routines.append(self.app_routine)
        
        self.phynetwork = {}
        self.logicnetwork = {}

    def _main(self):
        
        switch_update_matcher = ModuleNotification.createMatcher('openflowmanager','update')

        physicnet_event = ModuleNotification.createMatcher('viperflow','physicnetwork')

        logicnet_event = ModuleNotification.createMatcher('viperflow','logicnetwork')
        
        while True:
 
            yield (physicnet_event,logicnet_event,switch_update_matcher)

            if self.app_routine.matcher is switch_update_matcher:
                
                event = self.app_routine.event
                
                for switch_conn in event.add:
                    self.app_routine.subroutine(self.init_switch(switch_conn))
                
                for switch_disc in event.remove:
                    self.app_routine.subroutine(self.uninit_switch(switch_disc))

            if self.app_routine.matcher is physicnet_event:

                event = self.app_routine.event
                
                #logger.info(' --- physicnet event = %r',dir(event))
                
                connection = event.conn

                for phyport in event.add:
                    logger.info(' -- phynet add = %r --',phyport)
                    self.app_routine.subroutine(self.physicport_add(connection,phyport))
                
                for phynet in event.remove:
                    logger.info(' -- phynet remove = %r --',phyport)
                    self.app_routine.subroutine(self.physicport_remove(connection,phyport))

            if self.app_routine.matcher is logicnet_event:

                event = self.app_routine.event
                connection = event.conn

                for logicport in event.add:
                    self.app_routine.subroutine(self.logicnetport_add(connection,
                        logicport))
                for logicport in event.remove:
                    self.app_routine.subroutine(self.logicnetport_remove(connection,
                        logicport))
    
    def init_switch(self,conn):
        logger.info(" --- init_switch ---")
        
        dpid = conn.openflow_datapathid
        
        ofp_parser = conn.openflowdef
        ofp_proto = conn.protocol 
        ofctl = ofpctl.OfpCtlViper(ofp_parser,ofp_proto,conn)
        

        for m in ofctl.add_default_flow(self.app_routine):
            yield m
        
    def uninit_switch(self,conn):
        logger.info(' --- uninit_switch --')
        
        if None:
            yield 
    def physicport_add(self,conn,phynet):
        logger.info(" --- physicnetwork_update ---") 
         
        #
        # that switch which in conn have an port add phy
        #
        
        dpid = conn.openflow_datapathid
        ofp_parser = conn.openflowdef
        ofp_proto = conn.protocol
        
        if (dpid,phynet['name']) not in self.phynetwork:
            global PHYNET_NUMBER
            PHYNET_NUMBER += 1
            
            phynet['id'] = PHYNET_NUMBER

            self.phynetwork[(dpid,phynet['name'])] = phynet
            # this port must be trunk or vxlan
            
            # cookie 32 -- 24 APP serial no , 24 -- 20 phynet id
            cookie = L2_APP_SERIAL_NO << 24 | phynet['id'] << 20

            if phynet['type'] == 'vlan':
                # this must be trunk ,, every thing allowed in
                
                ofctl = ofpctl.OfpCtlViper(ofp_parser,ofp_proto,conn)

                match = ofp_parser.ofp_match_oxm()
                oxm = ofp_parser.create_oxm(ofp_parser.OXM_OF_IN_PORT,
                        phynet['ports'][0]['portno'])
                match.oxm_fields.append(oxm)
                ins = ofp_parser.ofp_instruction_goto_table(table_id = ACL_TABLE_ID)
                 
                for m in ofctl.add_flow(self.app_routine,match,[ins,],
                        ofp_parser.OFP_NO_BUFFER,table_id = TAG_TABLE_ID,priority = 50,
                        cookie = cookie):
                    yield m
                 
            if phynet['type'] == 'vxlan':
                pass
        else:
            # is a phynetwork will have more phy port ??
            pass
        """
        if phynet['name'] is not in self.phynetwork:
            # we should add phynet
            self.phynetwork[phynet['name']] = phynet
        else:
            
            # update port

            # del every thing about that delete port
            
            # add new port every thing ,

            pass
        """
        if None:
            yield 
    def physicport_remove(self,conn,phynet):
        logger.info(" --- physicnetwork_update ---") 
        
        # is every phynet have only one phynet port
        # remove port will also delete phynet

        if None:
            yield 
    def logicnetport_add(self,conn,portInfo):
        logger.info(" --- logicnetwork_ add_ port ----")
         
        logger.info(" ----logicnet add portInfo %r ----",portInfo)
         
        dpid = conn.openflow_datapathid
        ofp_parser = conn.openflowdef
        ofp_proto = conn.protocol
         
        ofctl = ofpctl.OfpCtlViper(ofp_parser,ofp_proto,conn)

        #
        # here there is an error, the VIF port is discover before
        # phy port, so VIF port flow will stop here, it is an error
        # it should be wait here for physicnet created
        #
        if(dpid,portInfo['physicnet']['name']) not in self.phynetwork:
            raise StopIteration
       
        portInfo['logicnet']['type'] = portInfo['physicnet']['type']
        
        if(dpid,portInfo["logicnet"]['name']) not in self.logicnetwork:
            global LOGICNET_NUMBER
            LOGICNET_NUMBER += 1
            portInfo["logicnet"]['id'] = LOGICNET_NUMBER
            portInfo['logicnet']['ports'] = []
            portInfo['logicnet']['groupBucket'] = []
            self.logicnetwork[(dpid,portInfo['logicnet']['name'])] = portInfo['logicnet']
            
            phynet = self.phynetwork[(dpid,portInfo['physicnet']['name'])]

            # create an network
            # add an group use to broadcast ,  groupid == network_id
            cookie = L2_APP_SERIAL_NO << 24 | phynet['id'] << 20 | portInfo['logicnet']['id']   
            # add flow allowed phy port segment id packet in
            if phynet['type'] == 'vlan':
                
                #
                # allow segment id , phy port in , put networkid to metadata
                #

                #
                # the segment id phy port have been belong to this logicnet,
                # we must allow it in , and add the broadcast group
                #
                match = ofp_parser.ofp_match_oxm()
                oxm = ofp_parser.create_oxm(ofp_parser.OXM_OF_VLAN_VID,
                        portInfo['logicnet']['segment_id']|ofp_parser.OFPVID_PRESENT)
                match.oxm_fields.append(oxm)
                
                oxm = ofp_parser.create_oxm(ofp_parser.OXM_OF_IN_PORT,phynet['ports'][0]['portno'])
                match.oxm_fields.append(oxm) 
                
                """
                #
                # learn action , can not match metadata ,  so 
                # we replace metadata with reg5
                #
                ins1 = ofp_parser.ofp_instruction_write_metadata(metadata = portInfo['logicnet']['id'],
                        metadata_mask = 0xffffffffffffffff)
                ins2 = ofp_parser.ofp_instruction_goto_table(table_id = ACL_TABLE_ID)
                """
                ins1 = ofp_parser.ofp_instruction_actions(type = ofp_parser.OFPIT_APPLY_ACTIONS)
                action = ofp_parser.nx_action_reg_load(ofs_nbits = (0 << 16)|(32 - 1),dst = ofp_parser.NXM_NX_REG5,
                    value = portInfo['logicnet']['id'])
                ins1.actions.append(action)

                action = ofp_parser.ofp_action(type = ofp_parser.OFPAT_POP_VLAN)
                ins1.actions.append(action)
                
                ins2 = ofp_parser.ofp_instruction_goto_table(table_id = ACL_TABLE_ID)
                for m in ofctl.add_flow(self.app_routine,match,[ins1,ins2],
                      ofp_parser.OFP_NO_BUFFER,table_id = TAG_TABLE_ID,priority = 100,
                      cookie = cookie):
                   yield m
                
                # an new logic network ,we should add some acl flow 
                for m in ofctl.add_default_acl(self.app_routine,
                        portInfo['logicnet']['id']):
                    yield m
                
                # add mac learn flow to PRE_ROUTINE_TABLE_ID
                # (I think PRE_ROUTINE_TABLE_ID is not correct)
                match = ofp_parser.ofp_match_oxm()

                # here I don't want to learn broadcast packet

                oxm = ofp_parser.create_oxm(ofp_parser.OXM_OF_ETH_DST_W,[0,0,0,0,0,0],
                        [1,0,0,0,0,0])
                match.oxm_fields.append(oxm)

                ins1 = ofp_parser.ofp_instruction_actions(type = ofp_parser.OFPIT_APPLY_ACTIONS)
                
                spec1 = ofp_parser.create_nxfms_matchfield(src = ofp_parser.NXM_NX_REG5,
                        dst = ofp_parser.NXM_NX_REG5,n_bits = 32) 
                spec2 = ofp_parser.create_nxfms_matchfield(src = ofp_parser.NXM_OF_ETH_DST,
                        dst = ofp_parser.NXM_OF_ETH_SRC,n_bits = 48)
                
                spec3 = ofp_parser.create_nxfms_loadfield(src = ofp_parser.NXM_OF_IN_PORT,
                        dst = ofp_parser.NXM_NX_REG1,n_bits = 16)
                action = ofp_parser.nx_action_learn(idle_timeout = 60,priority = 50,
                        cookie = cookie,table_id = MAC_LEARN_TABLE_ID,
                        specs = [spec1,spec2,spec3])
                ins1.actions.append(action)

                ins2 = ofp_parser.ofp_instruction_goto_table(table_id = FUN_TABLE_ID)

                for m in ofctl.add_flow(self.app_routine,match,[ins1,ins2],
                        ofp_parser.OFP_NO_BUFFER,table_id = PRE_ROUTINE_TABLE_ID,priority = 50):
                    yield m
                   
                # we also filled in function flow into FUN_TABLE_ID
                #
                # 1. ARP packet goto ARP_TABLE_ID (priority 200)
                # 2. is this logicnetwork have router, mac == router mac
                #    should goto IP_TABLE_ID (priority 150)
                #    (logicnet info from DB will have one router or {}
                #       router will have router port,that all we kown
                #       which logicnet connected)
                # 3. other should goto MAC_TABLE_ID (priority 50)
                
                # (1,3 or DHCP, will common for every logic network
                #   so add it in default flow )
                
                # MAC_TABLE_ID every network , will learn , default 
                # will goto network group , broadcast it
                
                # create an group for this logic netwrok
                for m in ofctl.create_group_flow(portInfo['logicnet']['id']):
                    yield m
                
                # we should add this segment id phy port to logicnet 
                # group table , it is part of broadcast group
                # load in_port no to reg1, resubmit to POS_ROUTINE_TABLE_ID
                action1 = ofp_parser.nx_action_reg_load(ofs_nbits = (0 << 16)|(32 - 1),
                        dst = ofp_parser.NXM_NX_REG1,value = phynet['ports'][0]['portno'])
                
                action2 = ofp_parser.nx_action_resubmit(in_port = ofp_parser.OFPP_IN_PORT & 0xffff,
                        table = POS_ROUTINE_TABLE_ID)
                
                bucket1 = ofp_parser.ofp_bucket(actions = [action1,action2],
                        watch_port = ofp_parser.OFPP_ANY,watch_group = ofp_parser.OFPG_ANY)

                # we save bucket
                portInfo['logicnet']['groupBucket'].append({phynet['ports'][0]['portno']:bucket1})

                for m in ofctl.mod_group_flow(self.app_routine,portInfo['logicnet']['id'],
                        bucket = [ b for v in portInfo['logicnet']['groupBucket'] for b in v.values()]):
                    yield m
                
                """
                # mac table add this network default broadcast 
                match = ofp_parser.ofp_match_oxm()
                oxm = ofp_parser.create_oxm(ofp_parser.OXM_OF_METADATA,
                        portInfo['logicnet']['id'])
                match.oxm_fields.append(oxm)
                ins = ofp_parser.ofp_instruction_actions(type = ofp_parser.OFPIT_APPLY_ACTIONS)
                action = ofp_parser.ofp_action_group(group_id = portInfo['logicnet']['id'])
                ins.actions.append(action)

                for m in ofctl.add_flow(self.app_routine,match,[ins,],
                        ofp_parser.OFP_NO_BUFFER,table_id = MAC_TABLE_ID,
                        priority = 50,cookie = cookie):
                    yield m

                #
                #  have a problem ,  learn action can't add complex actions 
                #  lookup mac will resubmit mac learn table
                #  reg1 == 0 , means broadcast (default flow)
                """
                match = ofp_parser.ofp_match_oxm()
                oxm = ofp_parser.create_oxm(ofp_parser.NXM_NX_REG5,
                        portInfo['logicnet']['id'])
                match.oxm_fields.append(oxm)

                oxm = ofp_parser.create_oxm(ofp_parser.NXM_NX_REG1,0)
                match.oxm_fields.append(oxm)
                
                ins = ofp_parser.ofp_instruction_actions(type = ofp_parser.OFPIT_APPLY_ACTIONS)
                action = ofp_parser.ofp_action_group(group_id = portInfo['logicnet']['id'])
                ins.actions.append(action)

                for m in ofctl.add_flow(self.app_routine,match,[ins,],
                        ofp_parser.OFP_NO_BUFFER,table_id = MAC_TABLE_ID,
                        priority = 50,cookie = cookie):
                    yield m
                
                # this phy port vlan ,  output it should add tag 
                match = ofp_parser.ofp_match_oxm()
                oxm = ofp_parser.create_oxm(ofp_parser.NXM_NX_REG5,portInfo['logicnet']['id'])
                match.oxm_fields.append(oxm)
                
                oxm = ofp_parser.create_oxm(ofp_parser.NXM_NX_REG1,phynet['ports'][0]['portno'])
                match.oxm_fields.append(oxm)

                ins = ofp_parser.ofp_instruction_actions(type = ofp_parser.OFPIT_APPLY_ACTIONS)
                action = ofp_parser.ofp_action_push(ethertype=ETHERTYPE_8021Q)
                ins.actions.append(action)

                set_vid_field = ofp_parser.create_oxm(ofp_parser.OXM_OF_VLAN_VID, 
                        portInfo['logicnet']['segment_id']|ofp_parser.OFPVID_PRESENT)
                action = ofp_parser.ofp_action_set_field(field = set_vid_field)
                ins.actions.append(action)
                
                action = ofp_parser.ofp_action_output(port = phynet['ports'][0]['portno'])
                ins.actions.append(action)
                
                for m in ofctl.add_flow(self.app_routine,match,[ins,],ofp_parser.OFP_NO_BUFFER,
                        table_id = UNTAG_TABLE_ID,priority = 50,cookie = cookie):
                    yield m
                # arp table add this network default broadcast
                match = ofp_parser.ofp_match_oxm()
                oxm = ofp_parser.create_oxm(ofp_parser.NXM_NX_REG5,
                        portInfo['logicnet']['id'])
                match.oxm_fields.append(oxm)
                ins = ofp_parser.ofp_instruction_actions(type = ofp_parser.OFPIT_APPLY_ACTIONS)
                action = ofp_parser.ofp_action_group(group_id = portInfo['logicnet']['id'])
                ins.actions.append(action)

                for m in ofctl.add_flow(self.app_routine,match,[ins,],
                        ofp_parser.OFP_NO_BUFFER,table_id = ARP_TABLE_ID,
                        priority = 50,cookie = cookie):
                    yield m
                

                # add mac learn flow for logicnetwork , put it in FUN_TABLE_ID, highest priroity
                # question:  if switch don't have learn action ,, where we should learn ??

            if phynet['type'] == 'vxlan':
                pass

        logicnet = self.logicnetwork[(dpid,portInfo["logicnet"]['name'])]
        logicnet['ports'].append({'name':portInfo['name'],'id':portInfo['id'],
            'portno':portInfo['portno']})

        logger.info('logicnet info %r',self.logicnetwork[(dpid,portInfo["logicnet"]['name'])])
        
        phynet = self.phynetwork[(dpid,portInfo['physicnet']['name'])]
        cookie = L2_APP_SERIAL_NO << 24 | phynet['id'] << 20 | logicnet['id']   
         
        # here we add an port to logicnet 
        if logicnet['type'] == 'vlan':
            # this VIF must be vlan access port
            match = ofp_parser.ofp_match_oxm()
            oxm = ofp_parser.create_oxm(ofp_parser.OXM_OF_VLAN_VID,ofp_parser.OFP_VLAN_NONE)
            match.oxm_fields.append(oxm)
            
            oxm = ofp_parser.create_oxm(ofp_parser.OXM_OF_IN_PORT,portInfo['portno'])
            match.oxm_fields.append(oxm)
            
            #ins1 = ofp_parser.ofp_instruction_write_metadata(metadata = logicnet['id'],
            #        metadata_mask = 0xffffffffffffffff)
            ins2 = ofp_parser.ofp_instruction_goto_table(table_id = ACL_TABLE_ID)
            
            ins1 = ofp_parser.ofp_instruction_actions(type = ofp_parser.OFPIT_APPLY_ACTIONS)
            # record segment id is correct ??
            action = ofp_parser.nx_action_reg_load(ofs_nbits = (0 << 16)|(32 - 1),dst = ofp_parser.NXM_NX_REG0,
                    value = logicnet['segment_id'])
            ins1.actions.append(action)
            action = ofp_parser.nx_action_reg_load(ofs_nbits = (0 << 16)|(32 - 1),dst = ofp_parser.NXM_NX_REG5,
                    value = logicnet["id"])
            
            ins1.actions.append(action)
            
            for m in ofctl.add_flow(self.app_routine,match,[ins1,ins2],
                  ofp_parser.OFP_NO_BUFFER,table_id = TAG_TABLE_ID,priority = 100,
                  cookie = cookie):
               yield m
            
            # add this port to logicnetwork broadcast group
            action1 = ofp_parser.nx_action_reg_load(ofs_nbits = (0 << 16)|(32 - 1),
                    dst = ofp_parser.NXM_NX_REG1,value = portInfo['portno'])
            
            action2 = ofp_parser.nx_action_resubmit(in_port = ofp_parser.OFPP_IN_PORT & 0xffff,
                    table = POS_ROUTINE_TABLE_ID)
            
            bucket1 = ofp_parser.ofp_bucket(actions = [action1,action2],
                    watch_port = ofp_parser.OFPP_ANY,watch_group = ofp_parser.OFPG_ANY)

            logicnet['groupBucket'].append({portInfo['portno']:bucket1})

            logging.info(" groupBucket = %r",logicnet['groupBucket'])
            s = [b for v in logicnet['groupBucket'] for b in v.values()]
            logging.info(" groupBucket SSSS = %r",s)
            
            """
            for m in ofctl.mod_group_flow(self.app_routine,logicnet['id'],
                    buckets = [b for v in logicnet['groupBucket'] for b in v.values()]):
                yield m
            """
            for m in ofctl.mod_group_flow(self.app_routine,logicnet['id'],
                    bucket = s):
                yield m

            
            # here add UNTAG_TABLE_ID , output
            match = ofp_parser.ofp_match_oxm()
            oxm = ofp_parser.create_oxm(ofp_parser.NXM_NX_REG5,logicnet['id'])
            match.oxm_fields.append(oxm)

            oxm = ofp_parser.create_oxm(ofp_parser.NXM_NX_REG1,portInfo['portno'])
            match.oxm_fields.append(oxm)

            ins = ofp_parser.ofp_instruction_actions(type = ofp_parser.OFPIT_APPLY_ACTIONS)
            action = ofp_parser.ofp_action_output(port = portInfo['portno'])
            ins.actions.append(action)

            for m in ofctl.add_flow(self.app_routine,match,[ins,],
                    ofp_parser.OFP_NO_BUFFER,table_id = UNTAG_TABLE_ID,priority = 50,
                    cookie = cookie):
                yield m

            # if this port have an mac 
            # we add this mac info to mac learn
        if logicnet['type'] == 'vxlan':
            pass
        
        if None:
            yield

    def logicnetport_remove(self,conn,portInfo):
        logger.info(" --- logicnetwork_ remove_ port ----")
        
        if None:
            yield 

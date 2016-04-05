#!/usr/bin/python
#! --*-- utf-8 --*--
PHYSICNETWOKRKEY = "viperflow.physicalnetwork" 

import logging
import json
import copy

from vlcp.config import defaultconfig
from vlcp.server.module import Module,depend,callAPI,ModuleNotification
from vlcp.event.runnable import RoutineContainer
from vlcp.service.connection import redisdb

from vlcp.service.sdn import ofpmanager,ofpportmanager,ovsdbmanager,ovsdbportmanager

logger = logging.getLogger("viperflow")


@defaultconfig
@depend(redisdb.RedisDB,ofpmanager.OpenflowManager,ofpportmanager.OpenflowPortManager,\
        ovsdbmanager.OVSDBManager,ovsdbportmanager.OVSDBPortManager)
class ViperFlow(Module):
    def __init__(self,server):
        super(ViperFlow,self).__init__(server)
        
        self.app_routine = RoutineContainer(self.scheduler)
        self.app_routine.main = self._main
        self.routines.append(self.app_routine)

    def _main(self):
        
        switch_update_matcher = ModuleNotification.createMatcher('openflowmanager','update')
        port_update_matcher = ModuleNotification.createMatcher('openflowportmanager','update')        
        
        logger.info(' ViperFlow App running ---')

        while True:

            yield (switch_update_matcher,port_update_matcher)

            if self.app_routine.matcher is port_update_matcher:
                port_event = self.app_routine.event

                for port in port_event.add:
                    self.app_routine.subroutine(self.add_port_handler(port_event.connection,port))

    def add_port_handler(self,conn,port):
        
        #logger.info(" --- add_port_handler --- %r",port)
        # this port is on the switch which datapathid == dpid 
        dpid = conn.openflow_datapathid

        #
        # every switch port will run here,
        # get systemid + brname + portname as key  (it is only phynet port)
        # wait until other info from DB
        #
        
        # if it is not phy net port,  we should try to get extend_id from ovsdb
        # if we can not find extend_id ,, it is phynet port ,  
        # get port info from db, other wait until conf

        # wait until get ovsDB connection  (if we get info from db yet ,we also
        # watch it's status change)

        # filter name same as bridge name
        try:
            for m in callAPI(self.app_routine,'ovsdbmanager','waitconnection',{'datapathid':dpid}):
                yield m
        except:
            raise StopIteration
        
       
        ovsdbconn = self.app_routine.retvalue
        systemid = ovsdbconn.ovsdb_systemid

        for m in callAPI(self.app_routine,'ovsdbmanager','getbridges',{'connection':ovsdbconn}):
            yield m
        
        bridges = self.app_routine.retvalue
        if bridges == None:
            raise StopIteration
        
        brname = None
        for bridge in bridges:
            if bridge[0] == dpid:
                brname = bridge[1]
        
        if brname == None:
            raise StopIteration
        
        # ignore internal port name same with br name
        if brname == port.name:
            raise StopIteration


        for m in callAPI(self.app_routine,'ovsdbportmanager','waitportbyno',{'datapathid':dpid,'portno':port.port_no}):
            yield m
        
        dbPortInfo = self.app_routine.retvalue
        
        logger.info(" dbPortInfo = %r",dbPortInfo)
        

        # first we check it has extend id

        if len(dbPortInfo['external_ids']) != 0:
            interface_id = dbPortInfo['external_ids']['container_id']

            # have interface_id , this VIF must be connect to VM
            # get INFO from DB
            
            key = "viperflow.logicport." + interface_id
            
            for m in callAPI(self.app_routine,'redisdb','get',{'key':key}):
                yield m
            
            #stage1 = json.loads(self.app_routine.retvalue)
            stage1 = self.app_routine.retvalue
            logger.info(" get VIF info stage 1 %r",stage1)

            key2 = "viperflow.logicnetwork." + stage1['logicnet']

            for m in callAPI(self.app_routine,'redisdb','get',{'key':key2}):
                yield m

            #stage2 = json.loads(self.app_routine.retvalue)
            stage2 = self.app_routine.retvalue
            logger.info(" get VIF info stage 2 %r",stage2)

            key3 = "viperflow.physicalnetwork." + stage2['physicnetname']

            for m in callAPI(self.app_routine,'redisdb','get',{'key':key3}):
                yield m

            #stage3 = json.loads(self.app_routine.retvalue)
            stage3 = self.app_routine.retvalue
            logger.info(" get VIF info stage 3 %r",stage3)

            # if we have an error , we must wait until get it from DB
            # after all ,  we must watch everyting change
            
            portInfo = copy.deepcopy(stage1)
            portInfo['logicnet'] = stage2
            portInfo['physicnet'] = stage3
            portInfo['portno'] = port.port_no

            notifyEvent = ModuleNotification(self.getServiceName(),"logicnetwork",
                    add = [portInfo,],remove = [],conn = conn)
            self.scheduler.emergesend(notifyEvent)
            
            # we have find the port in chassis ,  we can update DB
            stage1['system_id'] = systemid
            stage1['brname'] = brname
            stage1['portno'] = port.port_no

            for m in callAPI(self.app_routine,'redisdb','set',
                    {'key':key,'value':stage1}):
                yield m
        else:
            # it is phynet port
            
            # first find unique key info 
            ukey = 'vlcp.port.' + systemid + '.' + brname + '.' + port.name  
            
            logger.info(" --- get ukey %r -- ",ukey)
            
            try:
                for m in callAPI(self.app_routine,'redisdb','get',{'key':ukey}):
                    yield m
                
                if self.app_routine.retvalue != None:
                    value = self.app_routine.retvalue

                    # ukey and value as key ,  value mybe change
                    # we should watch there changes
                    # do it after ...
                    #
                    # we can start a routine to watch key change , send event back 
                    
                    logger.info('-- get ukey value %r --',value)         
                    
                    #value_json = json.loads(value)

                    # find  
                    for m in callAPI(self.app_routine,'redisdb','get',{'key':value['key']}):
                        yield m
                    
                    value1 = self.app_routine.retvalue
                    
                    logger.info('-- get ukey value1 %r --',value)
                    # send event ,  physic network update port
                else:
                    # we can not find ukey value , try wkey
                    wkey = 'vlcp.port.*.*.' + port.name

                    logger.info(" --- get wkey %r -- ",wkey)
                    
                    for m in callAPI(self.app_routine,'redisdb','get',{'key':wkey}):
                        yield m

                    if self.app_routine.retvalue != None:
                        value0 = self.app_routine.retvalue

                        logger.info('-- get wkey value %r --',value0)

                        #value_json = json.loads(value)
                        
                        logger.info(" value json key %r",value0['key'])
                        
                        for m in callAPI(self.app_routine,'redisdb','get',{'key':value0['key']}):
                            yield m

                        value1 = self.app_routine.retvalue
                        
                        phyJson = value1
                        logger.info('-- get wkey value1 %r --',value1)         
                        # send event, physic network update port
                        
                        logger.info('-- phyJson = %r --',phyJson)
                        
                        phyJson['ports'] = [{'name':port.name,'portno':port.port_no,
                            'systemid':'','brname':''}]

                        logger.info('-- phyJson = %r --',phyJson)
                        
                        notifyEvent = ModuleNotification(self.getServiceName(),"physicnetwork",add = [phyJson,],remove = [],conn = conn)
                        self.scheduler.emergesend(notifyEvent)
            except:
                pass


        # last we watch every change
        #if value_json['type'] == 'PN':
            # this port is about physical network , 
            # we should watch port update, physical update
        #    pass

"""
#
# example to serial class
#
class PhysicNetwork(object):
    
    PHYSICNETWOKRKEY = "vlcp.physicalnetwork." 
   
    def __init__(self,name,type,rangeStart,rangeEnd):
        super(PhysicNetwork,self).__init__()
        
        self.name = name
        self.type = type
        self.rangeStart = rangeStart
        self.rangeEnd = rangeEnd

    def getKey(self):

        return self.PHYSICNETWOKRKEY + self.name
    
    def jsonencode(self):
        return self.__dict__
    
    @classmethod
    def jsondecode(cls,obj):
        o = cls.__new__(cls)
        o.__dict__.update(obj)
        return o
"""

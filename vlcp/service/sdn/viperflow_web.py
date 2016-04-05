#!/usr/bin/python
#! --*-- utf-8 --*--

import logging
import json

from vlcp.config import defaultconfig
from vlcp.server.module import Module,api,depend,callAPI
from vlcp.event.runnable import RoutineContainer
from vlcp.service.connection import redisdb 
#from vlcp.service.sdn import viperflow

logger = logging.getLogger('ViperFlowWeb')

PHYSICNETWOKRKEY = "viperflow.physicalnetwork"
LOGICNETWORKKEY = "viperflow.logicnetwork"
LOGICPORTKEY = "viperflow.logicport"

@defaultconfig
@depend(redisdb.RedisDB)
class ViperFlowWeb(Module):
    def __init__(self,server):
        super(ViperFlowWeb,self).__init__(server)
        
        self.app_routine = RoutineContainer(self.scheduler)

        self.app_routine.main = self._main
        self.routines.append(self.app_routine)
        
        #self.redis = redisdb.RedisDB()
        
        self.createAPI(api(self.create_ph_net,self.app_routine))
    def _main(self):
        
        for m in self.create_ph_net("abc"):
            yield m

        for m in self.create_py_port("abc"):
            yield m

        for m in self.create_logic_network("abc"):
            yield m
        
        for m in self.create_logicnet_port("06ccadc3c97f","port1"):
            yield m
        
        for m in self.create_logicnet_port("75df4b1dd386","port2"):
            yield m
    
        if None:
            yield
    
    def create_ph_net(self,data):
        
        "physical net info {name,type,range}"
        
        """
        phy = viperflow.PhysicNetwork("phy1","vlan",1000,2000)
        key = phy.getKey()
        
        print("key = %r",key)
        for m in callAPI(self.app_routine,"redisdb",'getclient'):
            yield m
        
        redisClient = self.app_routine.retvalue
        
        for m in callAPI(self.app_routine,"redisdb","set",{'key':key,"value":phy}):
            yield m
        
        #self.redis.set(key,phy)

        for m in redisClient.execute_command(self.app_routine,'get',key):
            yield m

        print(self.app_routine.retvalue)
        
        
        for m in callAPI(self.app_routine,"redisdb","get",{'key':key}):
            yield m

        print(self.app_routine.retvalue)
        """
        phy = {}
        phy['name'] = 'phy1'
        phy['type'] = 'vlan'
        phy['rangeStart'] = '1000'
        phy['rangeEnd'] = '2000'
        
        # here we need to deal with 'name' without '.' 
        key = PHYSICNETWOKRKEY + '.' + phy['name']
        
        # check this key is exist??
        
        for m in callAPI(self.app_routine,"redisdb",'getclient'):
            yield m
        
        redisClient = self.app_routine.retvalue
        
        # save into redis
        for m in callAPI(self.app_routine,"redisdb","set",{'key':key,"value":json.dumps(phy)}):
            yield m
        
        
        # save index
        for m in callAPI(self.app_routine,"redisdb",'set',{'key':PHYSICNETWOKRKEY,"value":key}):
            yield m
        """
        for m in redisClient.execute_command(self.app_routine,'get',key):
            yield m

        print(self.app_routine.retvalue)
        
        
        for m in callAPI(self.app_routine,"redisdb","get",{'key':key}):
            yield m

        print(self.app_routine.retvalue)
        """
        """
        jsonPhy = json.dumps(phy)

        for m in redisClient.execute_command(self.app_routine,'publish',key,jsonPhy):
            yield m
        """
    def create_py_port(self,data):
        
        
        # data {'physicnetname':'ph1','port':{'sysid','','brname','','portname','eth0'}}
        # if sysid == '' means all
        # if brname == '' means all

        phy = {}
        phy['name'] = 'phy1' 
         
        # get phy info from DB
        phykey = PHYSICNETWOKRKEY + '.' + phy['name']
         
        for m in callAPI(self.app_routine,'redisdb',"get",{'key':phykey}):
            yield m

        if self.app_routine.retvalue == None:
            raise StopIteration

        port = {}
        port['sysid'] = ''
        port['brname'] = ''
        port['portname'] = 'enp0s8'
        
        if port['sysid'] == '':
            port['sysid'] = '*'
        if port['brname'] == '':
            port['brname'] = '*'
        
        # 
        # vlcp.port.sysid.brname.portname: {type:'PN','key':'PNKey'}
        #
        portkey = 'vlcp.port.'+port['sysid'] + '.' + port['brname'] + '.' + port['portname']
        
        portvalue = {'type':'PN','key':phykey}
        for m in callAPI(self.app_routine,'redisdb','set',{'key':portkey,'value':json.dumps(portvalue)}):
            yield m

    def create_logic_network(self,data):
    
        # data {'name':'logic1','physicnetname':'ph1'}

        logicnet = {}
        logicnet['name'] = 'logic1'
        logicnet['physicnetname'] = 'phy1'

        #  we should get avaliable segment_id (vid | vni)
        logicnet['segment_id'] = 1001

        logicnetkey = LOGICNETWORKKEY + '.' + logicnet['name']

        for m in callAPI(self.app_routine,'redisdb','set',{'key':logicnetkey,'value':json.dumps(logicnet)}):
            yield m

    def create_logicnet_port(self,data,name):

        logicnetport = {}
        logicnetport['name'] = name
        logicnetport['logicnet'] = 'logic1'
        logicnetport['mac'] = ''
        
        # this id must be ovs extend_id
        #logicnetport['id'] = '06ccadc3c97f'
        logicnetport['id'] = data

        logicportkey = LOGICPORTKEY + '.' + logicnetport['id']

        for m in callAPI(self.app_routine,'redisdb','set',{'key':logicportkey,'value':json.dumps(logicnetport)}):
            yield m

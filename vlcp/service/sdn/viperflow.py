#!/usr/bin/python
#! --*-- utf-8 --*--

from vlcp.config import defaultconfig
from vlcp.server.module import Module,depend,callAPI,api
from vlcp.event.runnable import RoutineContainer

from vlcp.utils.dataobject import DataObjectSet,updater,\
            set_new,DataObjectUpdateEvent,watch_context,dump,ReferenceObject
import vlcp.service.kvdb.objectdb as objectdb

from vlcp.utils.networkmodel import *

from uuid import uuid1
import logging

logger = logging.getLogger('viperflow')

#logger.setLevel(logging.DEBUG)

@defaultconfig
@depend(objectdb.ObjectDB)
class viperflow(Module):
    def __init__(self,server):
        super(viperflow,self).__init__(server)
        self.app_routine = RoutineContainer(self.scheduler)
        self.app_routine.main = self._main
        self.routines.append(self.app_routine)
        self._reqid = 0
        self.createAPI(api(self.createphysicalnetwork,self.app_routine),
                       api(self.createphysicalnetworks,self.app_routine),
                       api(self.updatephysicalnetwork,self.app_routine),
                       #api(self.updatephysicalnetworks,self.app_routine),
                       api(self.deletephysicalnetwork,self.app_routine),
                       #api(self.deletephysicalnetworks,self.app_routine),
                       api(self.listphysicalnetwork,self.app_routine),
                       api(self.createphysicalport,self.app_routine),
                       api(self.createphysicalports,self.app_routine)
                       ) 
    def _main(self):
        

        #
        #  main func is test , will be deleted laster
        #
        for m in self.createphysicalnetwork(vlanrange = [(1,100)]):
            yield m
        
        networks = [{'type':'vlan','vlanrange':[(1,100)]},
                    {'type':'vlan','vlanrange':[(100,300)]}]
        #networks = [{'type':'vlan'},{'type':'vlan'}]
        for m in self.createphysicalnetworks(networks):
            yield m
        
        logger.info(" ######## create physicalnetwork =  %r ",self.app_routine.retvalue)

        # test update
        updateattr = {'name':'abc','vlanrange':[(50,100)]}
        for m in self.updatephysicalnetwork(self.app_routine.retvalue[0].get('id'),**updateattr):
            yield m

        logger.info(" ######## update physicalnetwork =  %r ",self.app_routine.retvalue)
        
        listid = self.app_routine.retvalue[0].get('id')
        # test delete
        """
        for m in self.deletephysicalnetwork(self.app_routine.retvalue[0].get('id')):
            yield m
        
        logger.info(" ####### delete %r",self.app_routine.retvalue)
        """
        # test list

        logger.info(" listid = %r",listid)
        for m in self.listphysicalnetwork():
            yield m

        logger.info(" ####### list %r",self.app_routine.retvalue)

        for m in self.listphysicalnetwork(listid):
            yield m

        logger.info(" ###### list one %r",self.app_routine.retvalue)

        for m in self.listphysicalnetwork(listid,name = "abc"):
            yield m

        logger.info(" ###### list one %r",self.app_routine.retvalue)
        

        # test create physical port
        logger.info(" listid = %r",listid)
        for m in self.createphysicalport(listid,'enp0s8',rate = '1000'):
            yield m

        logger.info(' ###### create physical port %r',self.app_routine.retvalue)

        # test create physical ports

        ports = [{'phynetid':listid,'name':'eth0'},{'phynetid':listid,'name':'eth1',
            'rate':10000}]
        

        for m in self.createphysicalports(ports):
            yield m

        logger.info(' ##### create physical ports %r',self.app_routine.retvalue)
    def _dumpkeys(self,keys):
        self._reqid += 1
        reqid = ('viperflow',self._reqid)
        
        for m in callAPI(self.app_routine,'objectdb','mget',{'keys':keys,'requestid':reqid}):
            yield m

        retobjs = self.app_routine.retvalue

        with watch_context(keys,retobjs,reqid,self.app_routine):
            self.app_routine.retvalue = [dump(v) for v in retobjs]
    
    def _getkeys(self,keys):
        self._reqid += 1
        reqid = ('viperflow',self._reqid)
        for m in callAPI(self.app_routine,'objectdb','mget',{'keys':keys,'requestid':reqid}):
            yield m
        with watch_context(keys,self.app_routine.retvalue,reqid,self.app_routine):
            pass

    def createphysicalnetwork(self,type = 'vlan',id = None, **kwargs):
        
        # {'type':'vlan' or 'vxlan','id' = None,uuid1(),'vlanrange':[(1,100),(200,300)],kwargs}
        if not id:
            id = str(uuid1())
        try:
            for m in callAPI(self.app_routine,'public',
                    'createphysicalnetwork',{'type':type,'id':id,'args':kwargs},timeout = 1):
                yield m
        except:
            # here means driver is not find .. error
            raise 
        
        updater = self.app_routine.retvalue
        
        try:
            for m in callAPI(self.app_routine,'objectdb','transact',
                    {'keys':[PhysicalNetworkSet.default_key(),
                        PhysicalNetwork.default_key(id),
                        PhysicalNetworkMap.default_key(id)],'updater':updater}):
                yield m
        except:
            # here means updtate DB error,  
            raise 

        for m in self._dumpkeys([PhysicalNetwork.default_key(id),]):
            yield m

        self.app_routine.retvalue = self.app_routine.retvalue[0]    
    
    def createphysicalnetworks(self,networks):
        #networks [{type='vlan' or 'vxlan',id = None or uuid1(),'vlanrange':[(100,200),(400,401)],kwargs}]
        
        typenetworks = dict()
        # first check id is None, allocate for it
        # group by type, do it use type driver
        for network in networks:
            if 'type' not in network:
                raise ValueError("network must have type attr")
            network.setdefault('id',str(uuid1())) 
            if network.get('type') not in typenetworks: 
                typenetworks.setdefault(network.get('type'),{'networks':[network]})
            else:
                typenetworks.get(network.get('type')).get('networks').append(network) 
         
        for k,v in typenetworks.items():
            try:
                for m in callAPI(self.app_routine,'public','createphysicalnetworks',
                        {'networks':v.get('networks'),'type':k},timeout = 1):
                    yield m

                networkskey = [PhysicalNetwork.default_key(network.get('id'))
                                    for network in v.get('networks')]
                networksmapkey = [PhysicalNetworkMap.default_key(network.get('id'))
                                    for network in v.get('networks')]

                updater = self.app_routine.retvalue

                v['networkskey'] = networkskey
                v['networksmapkey'] = networksmapkey
                v['updater'] = updater
            except:
                raise

        
        keys = [PhysicalNetworkSet.default_key()]
        for _,v in typenetworks.items():
            keys.extend(v.get('networkskey'))
            keys.extend(v.get('networksmapkey'))
        
        def updater(keys,values):

            retnetworks = [None]
            retnetworkkeys = [keys[0]]
            for k,v in typenetworks.items():
                # [0] is physet
                start = 1
                physet = values[0]
                typevalues = values[start:start + len(v.get('networkskey'))]
                typekeys = keys[start:start + len(v.get('networkskey'))]
                typemapvalues = values[start + len(v.get('networkskey')):start + len(v.get('networkskey')) + len(v.get('networksmapkey'))]
                typemapkeys = keys[start + len(v.get('networkskey')):start + len(v.get('networkskey')) + len(v.get('networksmapkey'))]

                typeretnetworkkeys, typeretnetworks = v.get('updater')(list(keys[0:1]) + list(typekeys) + list(typemapkeys),[physet]+typevalues+typemapvalues)
                
                retnetworks.extend(typeretnetworks[1:])
                retnetworkkeys.extend(typeretnetworkkeys[1:])
                physet = typeretnetworks[0]
                start = start + len(v.get('networkskey')) + len(v.get('networksmapkey')) 
               
            retnetworks[0] = physet
            return retnetworkkeys,retnetworks
        try:
            for m in callAPI(self.app_routine,'objectdb','transact',
                    {'keys':keys,'updater':updater}):
                yield m
        except:
            raise
        
        for m in self._dumpkeys(keys[1:len(keys)//2 + 1]):
            yield m
    
    def updatephysicalnetwork(self,id,**kwargs):
        """
        update physicalnetwork that id info
        """
        if id is None:
            raise ValueError("update must be special id")
        
        networkkey = PhysicalNetwork.default_key(id)
        # we use networkmap to check vlanrange 
        networkmapkey = PhysicalNetworkMap.default_key(id)
        for m in self._getkeys([networkkey]):
            yield m

        networkobj = self.app_routine.retvalue
        
        # means this id physicalnetwork not exist 
        if len(networkobj) == 0 or networkobj[0] is None:
            raise ValueError("physicalnetwork id error")

        for m in callAPI(self.app_routine,'public','updatephysicalnetwork',
                {'type':networkobj[0].type,'id':id,'args':kwargs},timeout = 1):
            yield m
        updater = self.app_routine.retvalue

        try:
            for m in callAPI(self.app_routine,'objectdb','transact',
                    {'keys':[networkkey,networkmapkey],'updater':updater}):
                yield m

        except:
            raise
        
        for m in self._dumpkeys([networkkey]):
            yield m
    
    def deletephysicalnetwork(self,id):
        if id is None:
            raise ValueError("delete netwrok must special id")

        networkkey = PhysicalNetwork.default_key(id)
        networkmapkey = PhysicalNetworkMap.default_key(id)
        pynetsetkey = PhysicalNetworkSet.default_key()
        
        for m in self._getkeys([networkkey]):
            yield m
        networkobj = self.app_routine.retvalue

        if len(networkobj) == 0 or networkobj[0] is None:
            raise ValueError("physicalnetwork id error")
        
        try:
            for m in callAPI(self.app_routine,'public','deletephysicalnetwork',
                {'type':networkobj[0].type,'id':id},timeout = 1):
                yield m
        except:
            raise
        updater = self.app_routine.retvalue

        try:
            for m in callAPI(self.app_routine,'objectdb','transact',
                    {'keys':[pynetsetkey,networkkey,networkmapkey],'updater':updater}):
                yield m
        except:
            raise
        self.app_routine.retvalue = {"status":'OK'}

    def listphysicalnetwork(self,id = None,**kwargs):
        
        def set_walker(key,set,walk,save):
            for refnetwork in set.dataset():
                networkkey = refnetwork.getkey()
                try:
                    networkobj = walk(networkkey)
                except KeyError:
                    pass
                else:
                    """
                    for k,v in kwargs.items():
                        if getattr(networkobj,k,None) != v:
                            break
                    else:
                        save(networkkey)
                    """
                    if all(getattr(networkobj,k,None) == v for k,v in kwargs.items()):
                        save(networkkey)
        
        def walker_func(set_func):
            def walker(key,obj,walk,save):
                set_walker(key,set_func(obj),walk,save)
            return walker
        # get all phynet
        if not id:
            physetkey = PhysicalNetworkSet.default_key()
            # an unique id used to unwatch
            self._reqid += 1
            reqid = ('viperflow',self._reqid)
            
            for m in callAPI(self.app_routine,'objectdb','walk',{'keys':[physetkey],
                'walkerdict':{physetkey:walker_func(lambda x:x.set)},
                'requestid':reqid}):
                yield m
            keys,values = self.app_routine.retvalue
            # dump will get reference
            with watch_context(keys,values,reqid,self.app_routine):
                self.app_routine.retvalue = [dump(r) for r in values]

        else:
        # get that id phynet info
            phynetkey = PhysicalNetwork.default_key(id)
            for m in self._getkeys([phynetkey]):
                yield m
            retobj = self.app_routine.retvalue

            if all(getattr(retobj,k,None) == v for k,v in kwargs.items()):
                self.app_routine.retvalue = [dump(retobj)]
            else:
                self.app_routine.retvalue = []
    
    def createphysicalport(self,phynetid,name,vhost='',systemid='%',bridge='%',**kwargs):
        #phyports {'phynetid':'phynetid','name':'eth0','vhost':'vhost','systemid':'%','bridge':'%',kwargs} 
        phynetkey = PhysicalNetwork.default_key(phynetid)
        
        logger.info(' create port phynet key %r %r',phynetkey,phynetid)

        for m in self._getkeys([phynetkey]):
            yield m

        phynetobj = self.app_routine.retvalue

        logger.info(' phynetobj = %r',phynetobj[0].type)
        if len(phynetobj) == 0 or phynetobj[0] is None:
            raise ValueError('special phynet id is not existed')
        
        try:
            for m in callAPI(self.app_routine,'public','createphysicalport',
                    {'phynettype':phynetobj[0].type,'name':name,'vhost':vhost,
                        'systemid':systemid,'bridge':bridge,'args':kwargs},
                    timeout = 1):
                yield m
        except:
            raise

        updater = self.app_routine.retvalue
        
        phyportkey = PhysicalPort.default_key(vhost,systemid,bridge,name)
        phynetmapkey = PhysicalNetworkMap.default_key(phynetid)
        phyportsetkey = PhysicalPortSet.default_key()
        keys = [phyportkey,phynetmapkey,phyportsetkey]
        try:
            for m in callAPI(self.app_routine,'objectdb','transact',
                {'keys':keys,'updater':updater}):
                yield m
        except:
            raise
        
        for m in self._dumpkeys([phyportkey]):
            yield m

    def createphysicalports(self,ports):
        # ports [{'phynetid':id,'name':eth0,'vhost':'',systemid:'%'},{.....}]
        
        porttype = dict()
        for port in ports:
            port.setdefault('vhost','')
            port.setdefault('systemid','%')
            port.setdefault('bridge','%')
            
            portkey = PhysicalNetwork.default_key(port.get('phynetid'))

            for m in self._getkeys([portkey]):
                yield m
            portobj = self.app_routine.retvalue 
            if len(portobj) == 0 or portobj[0] is None:
                raise ValueError("port phynet not existed",port.get('name'),portkey)

            #port['type'] = portobj[0].type
            type = portobj[0].type 
            
            if type not in porttype:
                porttype.setdefault(type,{'ports':[port]})
            else:
                porttype.get(type).get('ports').append(port)
        

        for k,v in porttype.items():
            
            try:
                for m in callAPI(self.app_routine,'public','createphysicalports',
                        {'type':k,'ports':v.get('ports')},timeout=1):
                    yield m
            except:
                raise
            
            phymapkeys = [PhysicalNetworkMap.default_key(port.get('phynetid')) 
                        for port in v.get('ports') ]

            phyportkeys = [PhysicalNetwork.default_key(port.get('vhost'),
                    port.get('systemid'),port.get('bridge'),port.get('name'))
                        for port in v.get('ports')]

            phyportkeys = [PhysicalPort.default_key(port.get('vhost'),
                    port.get('systemid'),port.get('bridge'),port.get('name'))
                    for port in v.get('ports')]
            
            updater = self.app_routine.retvalue
            
            v['portkeys'] = phyportkeys
            v['portmapkeys'] = phymapkeys
            v['updater'] = updater
        
        keys = [PhysicalPortSet.default_key()]

        for _,v in porttype.items():
            keys.extend(v.get('portkeys'))
            keys.extend(v.get('portmapkeys'))
         
        def updater(keys,values):

            retkeys = [keys[0]]
            retvalues = [None]

            for k,v in porttype.items():
                start = 1
                physet = values[0]
                typeportkeys = keys[start:start + len(v.get('portkeys'))]
                typeportvalues = values[start:start + len(v.get('portkeys'))]
                typeportmapkeys = keys[start + len(v.get('portkeys')):start +
                        len(v.get('portkeys'))+ len(v.get('portmapkeys'))]
                typeportmapvalues = values[start + len(v.get('portkeys')):start + 
                        len(v.get('portkeys'))+ len(v.get('portmapkeys'))] 
                

                rettypekeys,rettypevalues = v.get('updater')(list(keys[0:1]) + 
                        list(typeportkeys) +list(typeportmapkeys),[physet] + 
                            typeportvalues + typeportmapvalues) 

                retkeys.extend(rettypekeys[1:])
                retvalues.extend(rettypevalues[1:])

                start = start + len(v.get('portkeys')) + len(v.get('portmapkeys'))
                physet = rettypevalues[0]

            retvalues[0] = physet
            return retkeys,retvalues
        try:
            for m in callAPI(self.app_routine,'objectdb','transact',
                    {'keys':keys,'updater':updater}):
                yield m
        except:
            raise

        for m in self._dumpkeys(keys[1:len(keys)//2 + 1]):
            yield m


    # the first run as routine going
    def load(self,container):
        
        # init callback set dataobject as an transaction
        # args [values] order as keys
        @updater
        def init(physet,phyportset,logicset,logicportset):
            if physet is None:
                physet = PhysicalNetworkSet()
            if phyportset is None:
                phyportset = PhysicalPortSet()
            if logicset is None:
                logicset = LogicalNetworkSet()
            if logicportset is None:
                logicportset = LogicalPortSet()
             
            return [physet,phyportset,logicset,logicportset]
        
        # dataobject keys that will be init ,, add it if necessary
        initdataobjectkeys = [PhysicalNetworkSet.default_key(),
                PhysicalPortSet.default_key(),LogicalNetworkSet.default_key(),
                LogicalPortSet.default_key()]
        for m in callAPI(container,'objectdb','transact',
                {'keys':initdataobjectkeys,'updater':init}):
            yield m
        
        # call so main routine will be run
        for m in Module.load(self,container):
            yield m

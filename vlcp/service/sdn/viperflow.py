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
class ViperFlow(Module):
    def __init__(self,server):
        super(ViperFlow,self).__init__(server)
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
                       api(self.createphysicalports,self.app_routine),
                       api(self.updatephysicalport,self.app_routine),
                       api(self.deletephysicalport,self.app_routine),
                       api(self.listphysicalport,self.app_routine),
                       api(self.createlogicalnetwork,self.app_routine),
                       api(self.createlogicalnetworks,self.app_routine),
                       api(self.updatelogicalnetwork,self.app_routine),
                       api(self.deletelogicalnetwork,self.app_routine),
                       api(self.createlogicalport,self.app_routine),
                       api(self.createlogicalports,self.app_routine),
                       api(self.updatelogcialport,self.app_routine),
                       api(self.deletelogcialport,self.app_routine),
                       api(self.listlogicalport,self.app_routine)
                       ) 
    def _main(self):
        

        #
        #  main func is test , will be deleted laster
        #
        for m in self.createphysicalnetwork(vlanrange = [(1,100)]):
            yield m
        
        listid1 = self.app_routine.retvalue[0].get('id')
        logger.info(" ######## create physicalnetwork =  %r ",self.app_routine.retvalue)
        
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

        # test update physical port

        for m in self.updatephysicalport('eth0',rate='999'):
            yield m

        logger.info(' ##### update physical ports %r',self.app_routine.retvalue)

        # test delete physical port
        for m in self.deletephysicalport('eth0'):
            yield m

        logger.info(' ##### delete physical ports %r',self.app_routine.retvalue)
        # test list physical port

        for m in self.listphysicalport('eth1'):
            yield m
        
        logger.info(' ##### list physical ports %r',self.app_routine.retvalue)

        for m in self.listphysicalport():
            yield m

        logger.info(' ##### list physical ports %r',self.app_routine.retvalue)
        
        for m in self.listphysicalport(phynetid = listid):
            yield m

        logger.info(' ##### list physical ports %r',self.app_routine.retvalue)
        
        # test create logicalnetwork
        for m in self.createlogicalnetwork(listid):
            yield m

        logger.info(' ##### list createlogical network %r',self.app_routine.retvalue)
        
        # test create logicalnetwork
        for m in self.createlogicalnetwork(listid):
            yield m

        logger.info(' ##### list createlogical network %r',self.app_routine.retvalue)
        
        # test create logicalnetwork
        for m in self.createlogicalnetwork(listid,vlanid = 52):
            yield m

        logger.info(' ##### list createlogical network %r',self.app_routine.retvalue)
        
        # test create logicalnetwork
        for m in self.createlogicalnetwork(listid):
            yield m

        logger.info(' ##### list createlogical network %r',self.app_routine.retvalue)
        # test create logicalnetworks
        #n = [{'phynetid':listid}]
        n = [{'phynetid':listid},{'phynetid':listid,'vlanid':99}]

        for m in self.createlogicalnetworks(n):
            yield m

        logger.info(' ##### list createlogical network %r',self.app_routine.retvalue)
        
        lgnetid = self.app_routine.retvalue[0]['id'] 
        
        logger.info(' ##### createlogical network lgnetid %r',lgnetid)
        
        # test update lgnetwork
        
        for m in self.updatelogicalnetwork(lgnetid,name = "google"):
            yield m

        logger.info(' ##### list updatelogical network %r',self.app_routine.retvalue)

        for m in self.updatelogicalnetwork(lgnetid,vlanid = 58):
            yield m

        logger.info(' ##### list updatelogical network %r',self.app_routine.retvalue)
        
        # test delete lgnetwork
        """
        for m in self.deletelogicalnetwork(lgnetid):
            yield m

        logger.info(' ##### list del network %r',self.app_routine.retvalue)
        """
        # test list lgnetwork

        for m in self.listlogicalnetwork():
            yield m

        logger.info(' ##### list logical network %r',self.app_routine.retvalue)
 
        for m in self.listlogicalnetwork(name = 'google'):
            yield m

        logger.info(' ##### list logical network %r',self.app_routine.retvalue)
        
        for m in self.listlogicalnetwork(phynetid = listid):
            yield m

        logger.info(' ##### list logical network %r',self.app_routine.retvalue)
        

        # test create logicalport

        for m in self.createlogicalport(lgnetid,name = "ABC"):
            yield m

        logger.info(' ##### create logical port %r',self.app_routine.retvalue)
        
        logicalports = [{"logicnetid":lgnetid},{"logicnetid":lgnetid}]
        for m in self.createlogicalports(logicalports):
            yield m
        logger.info(' ##### create logical ports %r',self.app_routine.retvalue)
        
        logicalportid = self.app_routine.retvalue[0]["id"]
        # test update logicalport

        for m in self.updatelogcialport(logicalportid,name = "eth0"):
            yield m
        logger.info(' ##### update logical port %r',self.app_routine.retvalue)

        """
        # test delete logicalport
        
        for m in self.deletelogcialport(logicalportid):
            yield m
        logger.info(' ##### delete logical port %r',self.app_routine.retvalue)
        """
        # test list logical port

        for m in self.listlogicalport(name='eth0'):
            yield m

        logger.info(' ##### list logical port %r',self.app_routine.retvalue)

        for m in self.listlogicalport(id = logicalportid):
            yield m

        logger.info(' ##### list logical port %r',self.app_routine.retvalue)

        for m in self.listlogicalport(logicnetid = lgnetid):
            yield m

        logger.info(' ##### list logical port %r',self.app_routine.retvalue)

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
        """        
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
        """
        if not id:
            id = str(uuid1())
        
        network = {'type':type,'id':id}
        network.update(kwargs)

        for m in self.createphysicalnetworks([network]):
            yield m
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
            start = 1
            for k,v in typenetworks.items():
                # [0] is physet
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
                self.app_routine.retvalue = dump(retobj)
            else:
                self.app_routine.retvalue = []
    
    def createphysicalport(self,phynetid,name,vhost='',systemid='%',bridge='%',**kwargs):
        
        """
        #phyports {'phynetid':'phynetid','name':'eth0','vhost':'vhost','systemid':'%','bridge':'%',kwargs} 
        phynetkey = PhysicalNetwork.default_key(phynetid)
        
        for m in self._getkeys([phynetkey]):
            yield m

        phynetobj = self.app_routine.retvalue

        if len(phynetobj) == 0 or phynetobj[0] is None:
            raise ValueError('special phynet id is not existed')
        
        try:
            for m in callAPI(self.app_routine,'public','createphysicalport',
                    {'phynettype':phynetobj[0].type,'phynetid':phynetid,'name':name,
                        'vhost':vhost,'systemid':systemid,'bridge':bridge,'args':kwargs},
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
        """

        port = {'phynetid':phynetid,'name':name,'vhost':vhost,'systemid':systemid,'bridge':bridge}
        port.update(kwargs)

        for m in self.createphysicalports([port]):
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
                raise ValueError("port phynet not existed "+port.get('name')+" "+portkey)
            
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

            start = 1
            for k,v in porttype.items():
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
    
    def updatephysicalport(self,name,vhost='',systemid='%',bridge='%',**args):

        portkey = PhysicalPort.default_key(vhost,systemid,bridge,name)

        for m in self._getkeys([portkey]):
            yield m

        portobj = self.app_routine.retvalue
        if len(portobj) == 0 or portobj[0] is None:
            raise ValueError("update port not exist "+name)
        
        try: 
            for m in callAPI(self.app_routine,'public','updatephysicalport',
                {"phynettype":portobj[0].physicalnetwork.type,'vhost':vhost,
                    'systemid':systemid,'bridge':bridge,'name':name,'args':args},
                        timeout = 1):
                yield m
        except:
            raise

        updater = self.app_routine.retvalue
        
        try:
            for m in callAPI(self.app_routine,'objectdb','transact',
                {'keys':[portkey],'updater':updater}):
                yield m
        except:
            raise
        
        for m in self._dumpkeys([portkey]):
            yield m
    
    def deletephysicalport(self,name,vhost='',systemid='%',bridge='%'):

        portkey = PhysicalPort.default_key(vhost,systemid,bridge,name)

        for m in self._getkeys([portkey]):
            yield m

        portobj = self.app_routine.retvalue

        if len(portobj) == 0 or portobj[0] is None:
            raise ValueError('delete port not existed '+name)
        
        keys = [portkey,PhysicalNetworkMap.default_key(portobj[0].physicalnetwork.id),
                PhysicalPortSet.default_key()]
        
        try:
            for m in callAPI(self.app_routine,'public','deletephysicalport',
                    {'phynettype':portobj[0].physicalnetwork.type,'vhost':vhost,
                        'systemid':systemid,'bridge':bridge,'name':name},timeout = 1):
                yield m
        except:
            raise
        
        updater = self.app_routine.retvalue

        try:
            for m in callAPI(self.app_routine,'objectdb','transact',
                    {'keys':keys,'updater':updater}):
                yield m
        except:
            raise

        self.app_routine.retvalue = {"status":'OK'}
    
    def listphysicalport(self,name = None,phynetid = None,vhost='',
            systemid='%',bridge='%',**args):
        
        def set_walker(key,set,walk,save):

            for weakobj in set.dataset():
                phyportkey = weakobj.getkey()

                try:
                    phyport = walk(phyportkey)
                except:
                    pass
                
                if not phynetid:
                    if all(getattr(phyport,k,None) == v for k,v in args.items()):
                        save(phyportkey)
                else:

                    try:
                        phynet = walk(phyport.physicalnetwork.getkey())
                    except:
                        pass
                    else:
                        if phynet.id == phynetid:
                            if all(getattr(phyport,k,None) == v for k,v in args.items()):
                                save(phyportkey)
        
        def walker_func(set_func):

            def walker(key,obj,walk,save):
                set_walker(key,set_func(obj),walk,save)
                
            return walker
        
        if not name:
            # get all physical port
            phyportsetkey = PhysicalPortSet.default_key()
            # an unique id used to unwatch
            self._reqid += 1
            reqid = ('viperflow',self._reqid)
            
            for m in callAPI(self.app_routine,'objectdb','walk',{'keys':[phyportsetkey],
                'walkerdict':{phyportsetkey:walker_func(lambda x:x.set)},
                'requestid':reqid}):
                yield m
            keys,values = self.app_routine.retvalue
            # dump will get reference
            with watch_context(keys,values,reqid,self.app_routine):
                self.app_routine.retvalue = [dump(r) for r in values]

        else:
            phyportkey = PhysicalPort.default_key(vhost,systemid,bridge,name)

            for m in self._getkeys([phyportkey]):
                yield m

            retobj = self.app_routine.retvalue

            if all(getattr(retobj,k,None) == v for k,v in args.items()):
                self.app_routine.retvalue = dump(retobj)
            else:
                self.app_routine.retvalue = []
    
    def createlogicalnetwork(self,phynetid,id = None,**kwargs):
        
        if not id:
            id = str(uuid1())
        """
        phynetkey = PhysicalNetwork.default_key(phynetid)
        phynetmapkey = PhysicalNetworkMap.default_key(phynetid)

        for m in self._getkeys([phynetkey]):
            yield m

        phynetobj = self.app_routine.retvalue

        if len(phynetobj) == 0 or phynetobj[0] is None:
            raise ValueError(' physicalnetwork id not exist',phynetid)
        
        try:
            for m in callAPI(self.app_routine,'public','createlogicalnetwork',
                {'phynettype':phynetobj[0].type,'phynetid':phynetid,'id':id,
                    'args':kwargs},timeout = 1):
                yield m
        except:
            raise

        updater = self.app_routine.retvalue

        keys = [LogicalNetworkSet.default_key(),LogicalNetwork.default_key(id),
                LogicalNetworkMap.default_key(id),phynetkey,phynetmapkey]

        try:
            for m in callAPI(self.app_routine,'objectdb','transact',
                    {'keys':keys,'updater':updater}):
                yield m
        except:
            raise
        
        for m in self._dumpkeys([LogicalNetwork.default_key(id)]):
            yield m
        """

        network = {'phynetid':phynetid,'id':id}
        network.update(kwargs)

        for m in self.createlogicalnetworks([network]):
            yield m
    def createlogicalnetworks(self,networks):
        
        # networks [{'phynetid':'id','id':'id' ...},{'phynetid':'id',...}]
        typenetwork = {}
        for network in networks:
            if 'phynetid' not in network:
                raise ValueError("create logicalnet must special phynetid")

            network.setdefault('id',str(uuid1()))

            phynetkey = PhysicalNetwork.default_key(network.get('phynetid'))

            for m in self._getkeys([phynetkey]):
                yield m

            phynetobj = self.app_routine.retvalue

            if len(phynetobj) == 0 or phynetobj[0] is None:
                raise ValueError("special phynetid not existed")
            
            phynettype = phynetobj[0].type

            if phynettype not in typenetwork:
                typenetwork.setdefault(phynettype,{'networks':[network]})
            else:
                typenetwork.get(phynettype).get('networks').append(network)


        for k, v in typenetwork.items():
            try:
                for m in callAPI(self.app_routine,'public','createlogicalnetworks',
                        {'phynettype':k,'networks':v.get('networks')},timeout = 1):
                    yield m
            except:
                    raise

            updater = self.app_routine.retvalue
            
            lgnetkey = [LogicalNetwork.default_key(n.get('id')) 
                            for n in v.get('networks')] 
            lgnetmapkey = [LogicalNetworkMap.default_key(n.get('id'))
                            for n in v.get('networks')]

            phynetkey = list(set([PhysicalNetwork.default_key(n.get('phynetid'))
                            for n in v.get('networks')]))

            #
            # if we use map default key , to set , it will be disorder with
            # phynetkey,  so we create map key use set(phynetkey)
            #
            phynetmapkey = [PhysicalNetworkMap.default_key(PhysicalNetwork.\
                    _getIndices(n)[1][0]) for n in phynetkey]

            v['lgnetkeys'] = lgnetkey
            v['lgnetmapkeys'] = lgnetmapkey
            # 
            # will have more logicalnetwork create on one phynet,
            # so we should reduce phynetkey , phynetmapkey
            #
            v['phynetkeys'] = phynetkey
            v['phynetmapkeys'] = phynetmapkey
            v['updater'] = updater

        keys = [LogicalNetworkSet.default_key()]
        for _,v in typenetwork.items():
            keys.extend(v.get('lgnetkeys'))
            keys.extend(v.get('lgnetmapkeys'))
            keys.extend(v.get('phynetkeys'))
            keys.extend(v.get('phynetmapkeys'))
        
        def updater(keys,values):
            retkeys = [keys[0]]
            retvalues = [None]
            start = 1
            for k,v in typenetwork.items():
                lgnetset = values[0]
                typelgnetworkkeys = keys[start:start+len(v.get('lgnetkeys'))]
                typelgnetworkvalues = values[start:start+len(v.get('lgnetkeys'))]
                typelgnetworkmapkeys = keys[start+len(v.get('lgnetkeys')):\
                        start+len(v.get('lgnetkeys'))+len(v.get('lgnetmapkeys'))]
                typelgnetworkmapvalues = values[start+len(v.get('lgnetkeys')):\
                        start+len(v.get('lgnetkeys'))+len(v.get('lgnetmapkeys'))]

                typephynetkeys = keys[start+len(v.get('lgnetkeys'))+len(v.get('lgnetmapkeys')):
                        start+len(v.get('lgnetkeys'))+len(v.get('lgnetmapkeys'))+len(v.get('phynetkeys'))]

                typephynetvalues = values[start+len(v.get('lgnetkeys'))+len(v.get('lgnetmapkeys')):
                        start+len(v.get('lgnetkeys'))+len(v.get('lgnetmapkeys'))+len(v.get('phynetkeys'))]
                
                typephynetmapkeys = keys[start+len(v.get('lgnetkeys'))+len(v.get('lgnetmapkeys'))\
                            +len(v.get('phynetkeys')):start + len(v.get('lgnetkeys'))\
                            +len(v.get('lgnetmapkeys'))+len(v.get('phynetkeys'))+len(v.get('phynetmapkeys'))]
                typephynetmapvalues = values[start+len(v.get('lgnetkeys'))+len(v.get('lgnetmapkeys'))\
                        +len(v.get('phynetkeys')):start + len(v.get('lgnetkeys'))\
                        +len(v.get('lgnetmapkeys'))+len(v.get('phynetkeys'))+len(v.get('phynetmapkeys'))]
                
                typeretkeys,typeretvalues = v.get('updater')(list(keys[0:1])+list(typelgnetworkkeys)+\
                            list(typelgnetworkmapkeys)+list(typephynetkeys)+list(typephynetmapkeys),\
                            [values[0]]+typelgnetworkvalues+typelgnetworkmapvalues+typephynetvalues+\
                            typephynetmapvalues)
                
                retkeys.extend(typeretkeys[1:])
                retvalues.extend(typeretvalues[1:])
                lgnetset = typeretvalues[0]
                start = start + len(v.get('lgnetkeys'))+len(v.get('lgnetmapkeys'))+\
                        len(v.get('phynetkeys')) + len(v.get('phynetmapkeys'))
            retvalues[0] = lgnetset
            return retkeys,retvalues
        try:
            for m in callAPI(self.app_routine,'objectdb','transact',
                    {'keys':keys,'updater':updater}):
                yield m

        except:
            raise
        
        dumpkeys = []
        for _,v in typenetwork.items():
            dumpkeys.extend(v.get('lgnetkeys'))
        for m in self._dumpkeys(dumpkeys):
            yield m
    
    def updatelogicalnetwork(self,id,**kwargs):
        
        # update phynetid is disabled 

        lgnetworkkey = LogicalNetwork.default_key(id)
        for m in self._getkeys([lgnetworkkey]):
            yield m
        
        lgnetworkobj = self.app_routine.retvalue
        
        if len(lgnetworkobj) == 0 or lgnetworkobj[0] is None:
            raise ValueError("lgnetwork id not existed "+id)

        try:
            for m in callAPI(self.app_routine,'public','updatelogicalnetwork',
                    {'phynettype':lgnetworkobj[0].physicalnet.type,'id':id,'args':kwargs},timeout=1):
                yield m

        except:
            raise

        updater = self.app_routine.retvalue
        
        phynetkey = lgnetworkobj[0].physicalnet.getkey()
        phynetmapkey = PhysicalNetworkMap.default_key(PhysicalNetwork._getIndices(phynetkey)[1][0])
        keys = [lgnetworkkey,phynetkey,phynetmapkey]

        try:
            for m in callAPI(self.app_routine,'objectdb','transact',
                    {'keys':keys,'updater':updater}):
                yield m
        except:
            raise

        for m in self._dumpkeys([lgnetworkkey]):
            yield m

    def deletelogicalnetwork(self,id):
        
        # update phynetid is disabled 

        lgnetworkkey = LogicalNetwork.default_key(id)
        lgnetworkmapkey = LogicalNetworkMap.default_key(id)
        
        for m in self._getkeys([lgnetworkkey]):
            yield m
        
        lgnetworkobj = self.app_routine.retvalue
        
        if len(lgnetworkobj) == 0 or lgnetworkobj[0] is None:
            raise ValueError("lgnetwork id not existed "+id)

        try:
            for m in callAPI(self.app_routine,'public','deletelogicalnetwork',
                    {'phynettype':lgnetworkobj[0].physicalnet.type,'id':id},timeout=1):
                yield m

        except:
            raise

        updater = self.app_routine.retvalue
        
        phynetkey = lgnetworkobj[0].physicalnet.getkey()
        phynetmapkey = PhysicalNetworkMap.default_key(PhysicalNetwork._getIndices(phynetkey)[1][0])
        
        lgnetworksetkey = LogicalNetworkSet.default_key() 
        keys = [lgnetworksetkey,lgnetworkkey,lgnetworkmapkey,phynetmapkey]

        try:
            for m in callAPI(self.app_routine,'objectdb','transact',
                    {'keys':keys,'updater':updater}):
                yield m
        except:
            raise

        self.app_routine.retvalue = {"status":'OK'}

    def listlogicalnetwork(self,id = None,phynetid = None,**args):
        
        def set_walker(key,set,walk,save):

            for weakobj in set.dataset():
                lgnetkey = weakobj.getkey()

                try:
                    lgnet = walk(lgnetkey)
                except KeyError:
                    pass
                else: 
                    if not phynetid:
                        if all(getattr(lgnet,k,None) == v for k,v in args.items()):
                            save(lgnetkey)
                    else:
                        try:
                            phynet = walk(lgnet.physicalnet.getkey())
                        except KeyError:
                            pass
                        else:
                            if phynet.id == phynetid:
                                if all(getattr(lgnet,k,None) == v for k,v in args.items()):
                                    save(lgnetkey)
            
        def walker_func(set_func):

            def walker(key,obj,walk,save):
                set_walker(key,set_func(obj),walk,save)
                
            return walker
        
        if not id:
            # get all logical network
            lgnetsetkey = LogicalNetworkSet.default_key()
            # an unique id used to unwatch
            self._reqid += 1
            reqid = ('viperflow',self._reqid)
            
            for m in callAPI(self.app_routine,'objectdb','walk',{'keys':[lgnetsetkey],
                'walkerdict':{lgnetsetkey:walker_func(lambda x:x.set)},
                'requestid':reqid}):
                yield m
            keys,values = self.app_routine.retvalue

            # dump will get reference
            with watch_context(keys,values,reqid,self.app_routine):
                self.app_routine.retvalue = [dump(r) for r in values]

        else:
            lgnetkey = LogicalNetwork.default_key(id)
            
            for m in self._getkeys([lgnetkey]):
                yield m

            retobj = self.app_routine.retvalue

            if all(getattr(retobj,k,None) == v for k,v in args.items()):
                self.app_routine.retvalue = dump(retobj)
            else:
                self.app_routine.retvalue = []
    
    """
    def updatelogicalnetworks(self,networks):
        #networks [{'id':id,....},{'id':id,....}]

        typenetwork = {}
        for network in networks:
            lgnetworkkey = LogicalNetwork.default_key(network.get("id"))
            
            for m in self._getkeys([lgnetworkkey]):
                yield m

            lgnetworkobj = self.app_routine.retvalue
            if len(lgnetworkobj) == 0 or lgnetworkobj[0] is None:
                raise ValueError("logicalnetwork key %r is not existd",network.get('id'))

            if lgnetworkobj.physicalnet.type not in typenetwork:
                typenetwork.setdefault(lgnetworkobj.physicalnet.type,{'networks':[network],
                    'phynetkey':[lgnetworkobj.physicalnet.getkey()]})
            else:
                typenetwork.get(lgnetworkobj.physicalnet.type).get('networks').append(network)
                typenetwork.get(lgnetworkobj.physicalnet.type).get('phynetkey').append(lgnetworkobj.physicalnet.getkey())
        
        for k,v in typenetwork.items():
            try:
                for m in callAPI(self.app_routine,'public','updatelogicalnetworks',
                    {'phynettype':k,'networks':v.get('networks')},timeout=1):
                    yield m
            except:
                raise

            updater = self.app_routine.retvalues
            
            lgnetworkkeys = [LogicalNetwork.default_key(n.get('id')) 
                        for n in v.get('networks')]
            
            phynetkeys = list(set([k for k in v.get('phynetkey')])) 

            v['lgnetworkkeys'] = lgnetworkkeys
            v['updater'] = updater
        
        keys = []
        for _,v in typenetwork.items():
            keys.extend(v.get("lgnetworkkeys")) 
        
        def updater(keys,values):
            
            start = 0
            retkeys = []
            retvalues = []
            for k,v in typenetwork.items():
                typelgnetkeys = keys[start:start+len(v.get('lgnetworkkeys'))]    
                typelgnetvalues = values[start:start+len(v.get('lgnetworkkeys'))]   

                typeretkeys,typeretvalues = v.get('updater')(typelgnetkeys,typelgnetvalues)
                
                retkeys.extend(typeretkeys)
                retvalues.extend(typeretvalues)
                start = start + len(v.get('lgnetworkkeys'))

            return retkeys,retvalues
        
        try:
            for m in callAPI(self.app_routine,'objectdb','transact',
                {"keys":keys,'updater':updater}):
                yield m
        except:
            raise

        for m in self._dumpkeys(keys):
            yield m
    """

    def createlogicalport(self,logicnetid,id = None,**args):

        if not id:
            id = str(uuid1())

        port = {'logicnetid':logicnetid,'id':id}
        port.update(args)

        for m in self.createlogicalports([port]):
            yield m

    def createlogicalports(self,ports):
        
        for port in ports:
            lognetkey = LogicalNetwork.default_key(port.get("logicnetid"))

            for m in self._getkeys([lognetkey]):
                yield m

            lognetobj = self.app_routine.retvalue

            if len(lognetobj) == 0 or lognetobj[0] is None:
                raise ValueError("logicalnetwork not existed " +port.get("logicnetid"))
            
            port.setdefault('id',str(uuid1()))
        
        lgportsetkey = LogicalPortSet.default_key()
        lgportkeys = [LogicalPort.default_key(p.get('id')) for p in ports]
        lgports = [self._createlogicalports(**p) for p in ports]
        lgnetkeys = list(set([p.logicalnetwork.getkey() for p in lgports]))
        lgnetmapkeys = [LogicalNetworkMap.default_key(LogicalNetwork._getIndices(k)[1][0]) for k in lgnetkeys]
        
        def updater(keys,values):
            netkeys = keys[1+len(lgportkeys):1+len(lgportkeys)+len(lgnetkeys)]
            netvalues = values[1+len(lgportkeys):1+len(lgportkeys)+len(lgnetkeys)]
            netmapkeys = keys[1+len(lgportkeys)+len(lgnetkeys):]
            netmapvalues = values[1+len(lgportkeys)+len(lgnetkeys):]
            lgnetdict = dict(zip(netkeys,zip(netvalues,netmapvalues)))
            
            for i in range(0,len(ports)):
                values[1+i] = set_new(values[1+i],lgports[i])

                _,netmap = lgnetdict.get(lgports[i].logicalnetwork.getkey())
                netmap.ports.dataset().add(lgports[i].create_weakreference())
                values[0].set.dataset().add(lgports[i].create_weakreference())
            return keys,values
        try:
            for m in callAPI(self.app_routine,'objectdb','transact',
                    {"keys":[lgportsetkey]+lgportkeys+lgnetkeys+lgnetmapkeys,'updater':updater}):
                yield m
        except:
            raise

        for m in self._dumpkeys(lgportkeys):
            yield m
    def _createlogicalports(self,id,logicnetid,**args):

        lgport = LogicalPort.create_instance(id)
        lgport.logicalnetwork = ReferenceObject(LogicalNetwork.default_key(logicnetid))

        for k,v in args.items():
            setattr(lgport,k,v)

        return lgport

    def updatelogcialport(self,id,**kwargs):
        
        lgportkey = LogicalPort.default_key(id)

        for m in self._getkeys([lgportkey]):
            yield m

        lgportobj = self.app_routine.retvalue

        if len(lgportobj) == 0 or lgportobj[0] is None:
            raise ValueError("logical port id not existed "+id)

        @updater
        def update(lgport):
            for k,v in kwargs.items():
                setattr(lgport,k,v)

            return [lgport]


        try:
            for m in callAPI(self.app_routine,'objectdb','transact',
                    {'keys':[lgportkey,],'updater':update}):
                yield m
        except:
            raise

        for m in self._dumpkeys([lgportkey]):
            yield m
    
    def deletelogcialport(self,id):

        lgportkey = LogicalPort.default_key(id)

        for m in self._getkeys([lgportkey]):
            yield m

        lgportobj = self.app_routine.retvalue

        if len(lgportobj) == 0 or lgportobj[0] is None:
            raise ValueError("logical port id not existed "+id)

        lgnetmapkey = LogicalNetworkMap.default_key(lgportobj[0].logicalnetwork.id)

        lgportsetkey = LogicalPortSet.default_key()

        @updater
        def update(portset,lgnetmap,lgport):
            
            for weakobj in portset.set.dataset().copy():
                if weakobj.getkey() == lgport.getkey():
                    portset.set.dataset().remove(weakobj)
            
            for weakobj in lgnetmap.ports.dataset().copy():
                if weakobj.getkey() == lgport.getkey():
                    lgnetmap.ports.dataset().remove(weakobj)
            return [portset,lgnetmap,None]
        
        try:
            for m in callAPI(self.app_routine,'objectdb','transact',
                    {"keys":[lgportsetkey,lgnetmapkey,lgportkey],'updater':update}):
                yield m
        except:
            raise

        self.app_routine.retvalue = {"status":'OK'}

    def listlogicalport(self,id = None,logicnetid = None,**kwargs):
        def set_walker(key,set,walk,save):

            for weakobj in set.dataset():
                lgportkey = weakobj.getkey()

                try:
                    lgport = walk(lgportkey)
                except KeyError:
                    pass
                else: 
                    if not logicnetid:
                        if all(getattr(lgport,k,None) == v for k,v in kwargs.items()):
                            save(lgportkey)
                    else:
                        try:
                            lgnet = walk(lgport.logicalnetwork.getkey())
                        except:
                            pass
                        else:
                            if lgnet.id == logicnetid:
                                if all(getattr(lgport,k,None) == v for k,v in kwargs.items()):
                                    save(lgportkey)
            
        def walker_func(set_func):

            def walker(key,obj,walk,save):
                set_walker(key,set_func(obj),walk,save)
                
            return walker
        
        if not id:
            # get all logical ports
            lgportsetkey = LogicalPortSet.default_key()
            # an unique id used to unwatch
            self._reqid += 1
            reqid = ('viperflow',self._reqid)
            
            for m in callAPI(self.app_routine,'objectdb','walk',{'keys':[lgportsetkey],
                'walkerdict':{lgportsetkey:walker_func(lambda x:x.set)},
                'requestid':reqid}):
                yield m
            keys,values = self.app_routine.retvalue

            # dump will get reference
            with watch_context(keys,values,reqid,self.app_routine):
                self.app_routine.retvalue = [dump(r) for r in values]

        else:
            lgportkey = LogicalPort.default_key(id)
            
            for m in self._getkeys([lgportkey]):
                yield m

            retobj = self.app_routine.retvalue

            if all(getattr(retobj,k,None) == v for k,v in kwargs.items()):
                self.app_routine.retvalue = dump(retobj)
            else:
                self.app_routine.retvalue = []

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

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
import copy
import logging

logger = logging.getLogger('viperflow')

#logger.setLevel(logging.DEBUG)

class UpdateConflictException(Exception):
    def __init__(self,desc="db update conflict"):
        super(UpdateConflictException,self).__init__(desc)


@defaultconfig
@depend(objectdb.ObjectDB)
class ViperFlow(Module):
    def __init__(self,server):
        super(ViperFlow,self).__init__(server)
        self.app_routine = RoutineContainer(self.scheduler)
        self.app_routine.main = self.main
        self.routines.append(self.app_routine)
        self._reqid = 0
        self.createAPI(api(self.createphysicalnetwork,self.app_routine),
                       api(self.createphysicalnetworks,self.app_routine),
                       api(self.updatephysicalnetwork,self.app_routine),
                       api(self.updatephysicalnetworks,self.app_routine),
                       api(self.deletephysicalnetwork,self.app_routine),
                       api(self.deletephysicalnetworks,self.app_routine),
                       api(self.listphysicalnetwork,self.app_routine),
                       api(self.createphysicalport,self.app_routine),
                       api(self.createphysicalports,self.app_routine),
                       api(self.updatephysicalport,self.app_routine),
                       api(self.updatephysicalports,self.app_routine),
                       api(self.deletephysicalport,self.app_routine),
                       api(self.deletephysicalports,self.app_routine),
                       api(self.listphysicalport,self.app_routine),
                       api(self.createlogicalnetwork,self.app_routine),
                       api(self.createlogicalnetworks,self.app_routine),
                       api(self.updatelogicalnetwork,self.app_routine),
                       api(self.updatelogicalnetworks,self.app_routine),
                       api(self.deletelogicalnetwork,self.app_routine),
                       api(self.deletelogicalnetworks,self.app_routine),
                       api(self.listlogicalnetwork,self.app_routine),
                       api(self.createlogicalport,self.app_routine),
                       api(self.createlogicalports,self.app_routine),
                       api(self.updatelogicalport,self.app_routine),
                       api(self.updatelogicalports,self.app_routine),
                       api(self.deletelogicalport,self.app_routine),
                       api(self.deletelogicalports,self.app_routine),
                       api(self.listlogicalport,self.app_routine)
                       ) 
    def main(self):
        if False:
            yield
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
            #
            # deepcopy every networks elements
            # case:[network]*N point to same object will auto create same id
            #        
            network = copy.deepcopy(network)
            network.setdefault('id',str(uuid1()))

            if network['type'] not in typenetworks: 
                typenetworks.setdefault(network['type'],{'networks':[network]})
            else:
                typenetworks.get(network['type'])['networks'].append(network) 
         
        for k,v in typenetworks.items():
            try:
                for m in callAPI(self.app_routine,'public','createphysicalnetworks',
                        {'networks':v['networks'],'type':k},timeout = 1):
                    yield m
                #
                # this keys will have repeat, we don't care it,
                # object will same with keys lasted
                #

                networkskey = [PhysicalNetwork.default_key(network['id'])
                                    for network in v['networks']]
                networksmapkey = [PhysicalNetworkMap.default_key(network['id'])
                                    for network in v['networks']]

                updater = self.app_routine.retvalue

                v['networkskey'] = networkskey
                v['networksmapkey'] = networksmapkey
                v['updater'] = updater
            except:
                raise

        
        keys = [PhysicalNetworkSet.default_key()]
        for _,v in typenetworks.items():
            keys.extend(v['networkskey'])
            keys.extend(v['networksmapkey'])
        
        def updater(keys,values):

            retnetworks = [None]
            retnetworkkeys = [keys[0]]
            start = 1
            physet = values[0]
            for k,v in typenetworks.items():
                # [0] is physet

                """
                typevalues = values[start:start + len(v.get('networkskey'))]
                typekeys = keys[start:start + len(v.get('networkskey'))]
                typemapvalues = values[start + len(v.get('networkskey')):start + len(v.get('networkskey')) + len(v.get('networksmapkey'))]
                typemapkeys = keys[start + len(v.get('networkskey')):start + len(v.get('networkskey')) + len(v.get('networksmapkey'))]

                typeretnetworkkeys, typeretnetworks = v.get('updater')(list(keys[0:1]) + list(typekeys) + list(typemapkeys),[physet]+typevalues+typemapvalues)
                
                retnetworks.extend(typeretnetworks[1:])
                retnetworkkeys.extend(typeretnetworkkeys[1:])
                physet = typeretnetworks[0]
                start = start + len(v.get('networkskey')) + len(v.get('networksmapkey')) 
                """
                typekeylen = len(v["networkskey"]) + len(v["networksmapkey"])

                try:
                    typeretnetworkkeys,typeretnetworks = v['updater'](tuple(keys[0:1])+keys[start:start+typekeylen],
                                                            [physet]+values[start:start+typekeylen])
                except:
                    raise
                else:
                    retnetworks.extend(typeretnetworks[1:])
                    retnetworkkeys.extend(typeretnetworkkeys[1:])
                    physet = typeretnetworks[0]
                    start = start + typekeylen 
                 
            retnetworks[0] = physet

            return retnetworkkeys,retnetworks
        try:
            for m in callAPI(self.app_routine,'objectdb','transact',
                    {'keys':keys,'updater':updater}):
                yield m
        except:
            raise
        
        dumpkeys = []
        for _,v in typenetworks.items():
            dumpkeys.extend(v.get("networkskey"))
        
        for m in self._dumpkeys(dumpkeys):
            yield m
    
    def updatephysicalnetwork(self,id,**kwargs):
        """
        update physicalnetwork that id info
        """
        if id is None:
            raise ValueError("update must be special id")
        
        network = {"id":id}
        network.update(kwargs)

        for m in self.updatephysicalnetworks([network]):
            yield m
        """
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
        """

    def updatephysicalnetworks(self,networks):
        # networks [{"id":phynetid,....}]
        
        phynetkeys = []
        typenetworks = dict()
        for network in networks:
            if 'type' in network:
                raise ValueError("physicalnetwork type can't be change")
            
            phynetkeys.append(PhysicalNetwork.default_key(network["id"])) 
        
        phynetkeys = list(set(phynetkeys))
        
        for m in self._getkeys(phynetkeys):
            yield m

        phynetvalues = self.app_routine.retvalue

        if None in phynetvalues:
            raise ValueError("physicalnetwork key not existed " +\
                    PhysicalNetwork._getIndices(phynetkeys[phynetvalues.index(None)])[1][0])
        
        phynetdict = dict(zip(phynetkeys,phynetvalues))
        
        for network in networks:
            phynetobj = phynetdict[PhysicalNetwork.default_key(network['id'])]
            
            if phynetobj.type not in typenetworks:
                typenetworks.setdefault(phynetobj.type,{"networks":[network]})
            else:
                typenetworks[phynetobj.type]['networks'].append(network)
        
        for k,v in typenetworks.items():
            try:
                for m in callAPI(self.app_routine,'public','updatephysicalnetworks',
                        {'type':k,'networks':v.get('networks')},timeout = 1):
                    yield m
            except:
                raise

            updater = self.app_routine.retvalue

            #
            # when networks have element to update same phynet,
            # len(phynetkey) != len(networks) 
            #
            phynetkey = list(set([PhysicalNetwork.default_key(n.get("id"))
                            for n in v.get('networks')]))
            
            phynetmapkey = [PhysicalNetworkMap.default_key(PhysicalNetwork._getIndices(key)[1][0])
                                for key in phynetkey]

            v['updater'] = updater
            v['phynetkey'] = phynetkey
            v['phynetmapkey'] = phynetmapkey

        keys = []
        for _,v in typenetworks.items():
            keys.extend(v.get("phynetkey"))
            keys.extend(v.get("phynetmapkey"))

        def updater(keys,values):
            start = 0
            retkeys = []
            retvalues = []
            for k,v in typenetworks.items():
                """
                typekeys = keys[start:start + len(v.get('phynetkey'))]
                typevalues = values[start:start + len(v.get('phynetkey'))]
                
                typemapkeys = keys[start+len(v.get('phynetkey')):
                                start+len(v.get('phynetkey'))+len(v.get('phynetmapkey'))]
                typemapvalues = values[start+len(v.get('phynetkey')):
                                start+len(v.get('phynetkey'))+len(v.get('phynetmapkey'))]

                rettypekeys,rettypevalues = v.get('updater')(typekeys+typemapkeys,
                                                typevalues+typemapvalues)
                
                retkeys.extend(rettypekeys)
                retvalues.extend(rettypevalues)
                start = start + len(v.get('phynetkey')) + len(v.get("phynetmapkey"))
                """
                typekeylen = len(v['phynetkey']) + len(v['phynetmapkey'])

                try:
                    rettypekeys,rettypevalues = v['updater'](keys[start:start+typekeylen],
                                values[start:start+typekeylen])
                except:
                    raise
                else:
                    retkeys.extend(rettypekeys)
                    retvalues.extend(rettypevalues)
                    start = start + typekeylen
            return retkeys,retvalues

        try:
            for m in callAPI(self.app_routine,'objectdb','transact',
                    {'keys':keys,'updater':updater}):
                yield m
        except:
            raise
        else:
            dumpkeys = []
            for _,v in typenetworks.items():
                dumpkeys.extend(v.get("phynetkey"))
            for m in self._dumpkeys(dumpkeys):
                yield m
    def deletephysicalnetwork(self,id):
        if id is None:
            raise ValueError("delete netwrok must special id")
        network = {"id":id}

        for m in self.deletephysicalnetworks([network]):
            yield m
        """
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
        """ 
    def deletephysicalnetworks(self,networks):
        # networks [{"id":id},{"id":id}]
        
        
        typenetworks = dict()
        phynetkeys = list(set([PhysicalNetwork.default_key(network['id']) for network in networks]))
        
        for m in self._getkeys(phynetkeys):
            yield m
        
        phynetvalues = self.app_routine.retvalue
        
        if None in phynetvalues:
            raise ValueError("physicalnetwork key not existed " +\
                    PhysicalNetwork._getIndices(phynetkeys[phynetvalues.index(None)])[1][0])
        
        phynetdict = dict(zip(phynetkeys,phynetvalues))
        
        for network in networks:
            phynetobj = phynetdict[PhysicalNetwork.default_key(network["id"])]

            if phynetobj.type not in typenetworks:
                typenetworks.setdefault(phynetobj.type,{"networks":[network]})
            else:
                typenetworks[phynetobj.type]['networks'].append(network)

        for k,v in typenetworks.items():
            
            try:
                for m in callAPI(self.app_routine,'public','deletephysicalnetworks',
                        {'type':k,'networks':v.get('networks')}):
                    yield m
            except:
                raise

            updater = self.app_routine.retvalue
            phynetkeys = list(set([PhysicalNetwork.default_key(n.get("id"))
                            for n in v.get('networks')]))
            
            phynetmapkeys = [PhysicalNetworkMap.default_key(PhysicalNetwork._getIndices(key)[1][0])
                                for key in phynetkeys]
            
            v['updater'] = updater
            v['phynetkeys'] = phynetkeys
            v['phynetmapkeys'] = phynetmapkeys
      
        keys = [PhysicalNetworkSet.default_key()]
        for _,v in typenetworks.items():
            keys.extend(v.get('phynetkeys'))
            keys.extend(v.get('phynetmapkeys'))
        
        def updater(keys,values):
            start = 1
            physet = values[0]
            
            retkeys = [keys[0]]
            retvalues = [None]
            for k,v in typenetworks.items():
                
                """
                typekeys = keys[start:start + len(v.get('phynetkeys'))]
                typevalues = values[start:start + len(v.get('phynetkeys'))]

                typemapkeys = keys[start+len(v.get('phynetkeys')):
                                    start + len(v.get('phynetkeys')) + len(v.get("phynetmapkeys"))]
                typemapvalues = values[start+len(v.get('phynetkeys')):
                                    start + len(v.get('phynetkeys')) + len(v.get("phynetmapkeys"))]
                
                rettypekeys,rettypevalues = v.get('updater')(keys[0:1]+typekeys+typemapkeys,
                                                [physet]+typevalues + typemapvalues)
                
                retkeys.extend(rettypekeys[1:])
                retvalues.extend(rettypevalues[1:])
                physet = rettypevalues[0]

                start = start + len(v.get('phynetkeys')) + len(v.get("phynetmapkeys"))
                """
                typekeylen = len(v['phynetkeys']) + len(v['phynetmapkeys'])

                try:
                    rettypekeys,rettypevalues = v['updater'](tuple(keys[0:1])+keys[start:start+typekeylen],
                            [physet]+values[start:start+typekeylen])
                except:
                    raise
                else:
                    retkeys.extend(rettypekeys[1:])
                    retvalues.extend(rettypevalues[1:])
                    physet = rettypevalues[0]
                    start = start + typekeylen

            retvalues[0] = physet

            return retkeys,retvalues

        try:
            for m in callAPI(self.app_routine,'objectdb','transact',
                    {'keys':keys,'updater':updater}):
                yield m
        except:
            raise
        else:
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
            if len(retobj) == 0 or retobj[0] is None:
                self.app_routine.retvalue = []
            else:
                if all(getattr(retobj[0],k,None) == v for k,v in kwargs.items()):
                    self.app_routine.retvalue = dump(retobj)
                else:
                    self.app_routine.retvalue = []
    
    def createphysicalport(self,physicalnetwork,name,vhost='',systemid='%',bridge='%',**kwargs):
        
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

        port = {'physicalnetwork':physicalnetwork,'name':name,'vhost':vhost,'systemid':systemid,'bridge':bridge}
        port.update(kwargs)

        for m in self.createphysicalports([port]):
            yield m
    def createphysicalports(self,ports):
        # ports [{'physicalnetwork':id,'name':eth0,'vhost':'',systemid:'%'},{.....}]
        
        #
        # we should use this while , when first get obj, to get type
        # second update , get obj , if fobj type or None conflict with sobj
        # we will try another once
        #
        
        phynetkeys = []
        newports = []
        porttype = dict()

        for port in ports:
            port = copy.deepcopy(port)
            port.setdefault('vhost','')
            port.setdefault('systemid','%')
            port.setdefault('bridge','%')
            phynetkeys.append(PhysicalNetwork.default_key(port.get('physicalnetwork')))
            newports.append(port)
        
        phynetkeys = list(set(phynetkeys))
        
        for m in self._getkeys(phynetkeys):
            yield m
        
        phynetvalues = self.app_routine.retvalue

        if None in phynetvalues:
            raise ValueError("physicalnetwork key not existed " +\
                    PhysicalNetwork._getIndices(phynetkeys[phynetvalues.index(None)])[1][0])

        phynetdict = dict(zip(phynetkeys,phynetvalues))
        
        for port in newports:
            phynetobj = phynetdict[PhysicalNetwork.default_key(port['physicalnetwork'])]
            type = phynetobj.type 
            
            if type not in porttype:
                porttype.setdefault(type,{'ports':[port]})
            else:
                porttype[type]['ports'].append(port)

        for k,v in porttype.items():
            
            try:
                for m in callAPI(self.app_routine,'public','createphysicalports',
                        {'type':k,'ports':v.get('ports')},timeout=1):
                    yield m
            except:
                raise
            
            phynetkeys = list(set([PhysicalNetwork.default_key(port["physicalnetwork"]) for port in v.get("ports")]))
            phynetmapkeys = [PhysicalNetworkMap.default_key(PhysicalNetwork._getIndices(key)[1][0]) 
                                for key in phynetkeys]
            
            """
            phynetkeys = [PhysicalNetwork.default_key(port.get('vhost'),
                    port.get('systemid'),port.get('bridge'),port.get('name'))
                        for port in v.get('ports')]
            """
            phyportkeys = [PhysicalPort.default_key(port.get('vhost'),
                    port.get('systemid'),port.get('bridge'),port.get('name'))
                    for port in v.get('ports')]
            
            updater = self.app_routine.retvalue
            
            v['portkeys'] = phyportkeys
            v['portnetkeys'] = phynetkeys
            v['portmapkeys'] = phynetmapkeys
            v['updater'] = updater

        keys = [PhysicalPortSet.default_key()]
        for _,v in porttype.items():
            keys.extend(v.get('portkeys'))
            keys.extend(v.get('portnetkeys'))
            keys.extend(v.get('portmapkeys'))
        
        def updater(keys,values):

            retkeys = [keys[0]]
            retvalues = [None]
            physet = values[0]
            start = 1
            for k,v in porttype.items():
                """
                typeportkeys = keys[start:start + len(v.get('portkeys'))]
                typeportvalues = values[start:start + len(v.get('portkeys'))]

                typeportnetkeys = keys[start + len(v.get('portkeys')):start + 
                                    len(v['portkeys']) + len(v['portnetkeys'])]
                typeportnetvalues = values[start + len(v.get('portkeys')):start + 
                                    len(v['portkeys']) + len(v['portnetkeys'])]

                typeportmapkeys = keys[start + len(v.get('portkeys')) + len(v.get('portnetkeys')):start +
                        len(v.get('portkeys'))+ len(v.get('portnetkeys')) + len(v.get('portmapkeys'))]
                typeportmapvalues = values[start + len(v.get('portkeys')) + len(v.get('portnetkeys')):start + 
                        len(v.get('portkeys'))+ len(v.get('portnetkeys')) + len(v.get('portmapkeys'))] 
                
                rettypekeys,rettypevalues = v.get('updater')(list(keys[0:1]) + 
                        list(typeportkeys) + list(typeportnetkeys) + list(typeportmapkeys),[physet] + 
                            typeportvalues + typeportnetvalues + typeportmapvalues) 

                retkeys.extend(rettypekeys[1:])
                retvalues.extend(rettypevalues[1:])

                start = start + len(v.get('portkeys')) + len(v.get('portnetkeys')) + len(v.get('portmapkeys'))
                physet = rettypevalues[0]
                """

                typekeylen = len(v['portkeys']) + len(v['portnetkeys']) + len(v['portmapkeys'])
                try:
                    rettypekeys,rettypevalues = v['updater'](keys[0:1]+keys[start:start+typekeylen],
                                [physet]+values[start:start+typekeylen])
                except:
                    raise
                else:
                    retkeys.extend(rettypekeys[1:])
                    retvalues.extend(rettypevalues[1:])
                    physet = rettypevalues[0]
                    start = start + typekeylen
            retvalues[0] = physet
            return retkeys,retvalues
   
        try:
            for m in callAPI(self.app_routine,'objectdb','transact',
                    {'keys':keys,'updater':updater}):
                yield m
        except:
            raise
        else:
            dumpkeys = []
            for _,v in porttype.items():
                dumpkeys.extend(v.get('portkeys'))
            for m in self._dumpkeys(dumpkeys):
                yield m

    def updatephysicalport(self,name,vhost='',systemid='%',bridge='%',**args):
        
        """
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
        """
        if not name:
            raise ValueError("must speclial physicalport name")
        
        port = {'name':name,'vhost':vhost,'systemid':systemid,'bridge':bridge}
        port.update(args)

        for m in self.updatephysicalports([port]):
            yield m
    
    def updatephysicalports(self,ports):
        # ports [{'name':eth0,'vhost':'',systemid:'%'},{.....}]
        
        self._reqid += 1
        reqid = ('viperflow',self._reqid)
        
        max_try = 1
        while True:

            fphysicalportkeys = []
            newports = []
            typeport = dict()
            for port in ports:

                port = copy.deepcopy(port)
                
                if 'name' not in port :
                    raise ValueError("must speclial physicalport name")
                
                if 'physicalnetwork' in port:
                    raise ValueError("physicalnetwork can not be change")

                port.setdefault("vhost","")
                port.setdefault("systemid","%")
                port.setdefault("bridge","%")
                portkey = PhysicalPort.default_key(port.get('vhost'),
                            port.get('systemid'),port.get('bridge'),port.get('name'))

                fphysicalportkeys.append(portkey)
                newports.append(port)
                
            fphysicalportkeys = list(set(fphysicalportkeys))
            for m in callAPI(self.app_routine,'objectdb','mget',{'keys':fphysicalportkeys,'requestid':reqid}):
                yield m
            
            fphysicalportvalues = self.app_routine.retvalue
            
            if None in fphysicalportvalues:
                raise ValueError(" physical ports is not existed "+ fphysicalportkeys[fphysicalportvalues.index(None)])
            
            physicalportdict = dict(zip(fphysicalportkeys,fphysicalportvalues))
            
            for port in newports:
                portobj = physicalportdict[PhysicalPort.default_key(port['vhost'],port['systemid'],
                                    port['bridge'],port['name'])]

                porttype = portobj.physicalnetwork.type

                if porttype not in typeport:
                    typeport.setdefault(porttype,{"ports":[port]})
                else:
                    typeport.get(porttype).get("ports").append(port)
                        
            for k,v in typeport.items():
                try:
                    for m in callAPI(self.app_routine,'public','updatephysicalports',
                            {"phynettype":k,'ports':v.get('ports')},timeout = 1):
                        yield m

                except:
                    raise

                updater = self.app_routine.retvalue
                portkeys = list(set([PhysicalPort.default_key(p.get('vhost'),p.get('systemid'),p.get('bridge'),
                            p.get('name')) for p in v.get('ports')]))
                
                v["updater"] = updater
                v["portkeys"] = portkeys
            
            keys = []
            typesortvalues = []
            for _,v in typeport.items():
                keys.extend(v.get('portkeys'))
                typesortvalues.extend(physicalportdict[key] for key in v.get('portkeys'))

            def update(keys,values):
                start = 0
                sortd_index = 0
                retkeys = []
                retvalues = []
                for k,v in typeport.items():
                    typekeys = keys[start:start + len(v.get("portkeys"))]
                    typevalues = values[start:start + len(v.get("portkeys"))]
                    
                    if [n.physicalnetwork.getkey() if n is not None else None for n in typevalues] !=\
                            [n.physicalnetwork.getkey() for n in typesortvalues[sortd_index:sortd_index + len(v.get('portkeys'))]]:
                        raise UpdateConflictException

                    rettypekeys,rettypevalues = v.get('updater')(typekeys,typevalues)

                    retkeys.extend(rettypekeys)
                    retvalues.extend(rettypevalues)
                    start = start + len(v.get('portkeys'))
                    sortd_index = sortd_index + len(v.get('portkeys'))

                return keys,values
            try:
                for m in callAPI(self.app_routine,'objectdb','transact',
                        {"keys":keys,"updater":update}):
                    yield m
            except UpdateConflictException:
                logger.info(" cause UpdateConflict Exception try once")
                max_try -= 1
                if max_try <= 0:
                    continue
                else:
                    raise
            except:
                raise
            else:
                break
        with watch_context(fphysicalportkeys,fphysicalportvalues,reqid,self.app_routine):
            pass
           
        for m in self._dumpkeys(keys):
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
    
    def deletephysicalports(self,ports):
        
        while True:
            typeport = dict()

            fphysicalportkeys = []
            newports = []
            for port in ports:

                port = copy.deepcopy(port)
                
                port.setdefault('vhost',"")
                port.setdefault('systemid',"%")
                port.setdefault('bridge',"%")
                
                portkey = PhysicalPort.default_key(port.get('vhost'),port.get("systemid"),
                            port.get("bridge"),port.get("name"))
                
                fphysicalportkeys.append(portkey)
                newports.append(port)
                
                #for m in self._getkeys([portkey]):
                #    yield m
                #portobj = self.app_routine.retvalue
                    
                #if len(portobj) == 0 or portobj[0] is None:
                #    continue
                #porttype = portobj[0].physicalnetwork.type
                #phynetid = portobj[0].physicalnetwork.id
                
                #
                # save phynetid in port , for driver find phynet phynetmap
                # 
                
                #port["phynetid"] = phynetid

                #if porttype not in typeport:
                #    typeport.setdefault(porttype,{"ports":[port]})
                #else:
                #    typeport.get(porttype).get("ports").append(port)
            fphysicalportkeys = list(set(fphysicalportkeys))
            self._reqid += 1
            reqid = ('viperflow',self._reqid)
            for m in callAPI(self.app_routine,'objectdb','mget',{'keys':fphysicalportkeys,'requestid':reqid}):
                yield m
                
            fphysicalportvalues = self.app_routine.retvalue
            
            if None in fphysicalportvalues:
                raise ValueError(" physical ports is not existed "+ fphysicalportkeys[fphysicalportvalues.index(None)])
            
            physicalportdict = dict(zip(fphysicalportkeys,fphysicalportvalues))
            
            for port in newports:
                portobj = physicalportdict[PhysicalPort.default_key(port['vhost'],port['systemid'],
                                port['bridge'],port['name'])]
                porttype = portobj.physicalnetwork.type
                phynetid = portobj.physicalnetwork.id
                
                port["phynetid"] = phynetid
                if porttype not in typeport:
                    typeport.setdefault(porttype,{"ports":[port]})
                else:
                    typeport.get(porttype).get("ports").append(port)

            for k,v in typeport.items():
                try:
                    for m in callAPI(self.app_routine,"public","deletephysicalports",
                            {"phynettype":k,"ports":v.get("ports")},timeout = 1):
                        yield m

                except:
                    raise

                updater = self.app_routine.retvalue
                
                portkeys = [PhysicalPort.default_key(p.get('vhost'),p.get('systemid'),
                            p.get('bridge'),p.get('name')) for p in v.get('ports')]
                phynetkeys = list(set([PhysicalNetwork.default_key(p.get("phynetid")) for p in v.get('ports')]))
                phynetmapkeys = list(set([PhysicalNetworkMap.default_key(p.get("phynetid")) for p in v.get('ports')]))
                
                v["updater"] = updater
                v["portkeys"] = portkeys
                v["phynetmapkeys"] = phynetmapkeys

            keys = [PhysicalPortSet.default_key()]
            typesortvalues = []
            for _,v in typeport.items():
                keys.extend(v.get("portkeys"))
                keys.extend(v.get("phynetmapkeys"))
                typesortvalues.extend(physicalportdict[key] for key in v.get('portkeys'))
            
            def update(keys,values):
                start = 1
                sortd_index = 0
                portset = values[0]
                retkeys = [keys[0]]
                retvalues = [None]
                for k,v in typeport.items():
                    typekeys = keys[start:start + len(v.get("portkeys"))]
                    typevalues = values[start:start + len(v.get("portkeys"))]
                    
                    if [n.physicalnetwork.getkey() if n is not None else None for n in typevalues] != [n.physicalnetwork.getkey() for n in typesortvalues[sortd_index:sortd_index + len(v.get('portkeys'))]]:
                        raise UpdateConflictException
                        
                    typemapkeys = keys[start + len(v.get("portkeys")):
                                    start + len(v.get("portkeys")) + len(v.get("phynetmapkeys"))]
                    typemapvalues = values[start + len(v.get("portkeys")):
                                    start + len(v.get("portkeys")) + len(v.get("phynetmapkeys"))]

                    typeretkeys,typeretvalues = v.get("updater")(keys[0:1]+typekeys+typemapkeys,
                                [portset]+typevalues+typemapvalues)
                    
                    retkeys.extend(typeretkeys[1:])
                    retvalues.extend(typeretvalues[1:])
                    portset = typeretvalues[0]
                    start = start + len(v.get('portkeys')) + len(v.get('phynetmapkeys'))
                
                retvalues[0] = portset

                return retkeys,retvalues
            try:
                for m in callAPI(self.app_routine,"objectdb","transact",
                        {"keys":keys,"updater":update}):
                    yield m
            except UpdateConflictException:
                logger.info(" cause UpdateConflict Exception try once")
                continue
            except:
                raise
            else:
                with watch_context(fphysicalportkeys,fphysicalportvalues,reqid,self.app_routine):
                    pass
                break

        self.app_routine.retvalue = {"status":'OK'}
    def listphysicalport(self,name = None,phynetwork = None,vhost='',
            systemid='%',bridge='%',**args):
        
        def set_walker(key,set,walk,save):

            for weakobj in set.dataset():
                phyportkey = weakobj.getkey()

                try:
                    phyport = walk(phyportkey)
                except:
                    pass
                
                if not phynetwork:
                    if all(getattr(phyport,k,None) == v for k,v in args.items()):
                        save(phyportkey)
                else:

                    try:
                        phynet = walk(phyport.physicalnetwork.getkey())
                    except:
                        pass
                    else:
                        if phynet.id == phynetwork:
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

            if len(retobj) == 0 or retobj[0] is None:
                self.app_routine.retvalue = []
            else:
                if all(getattr(retobj,k,None) == v for k,v in args.items()):
                    self.app_routine.retvalue = dump(retobj)
                else:
                    self.app_routine.retvalue = []
    
    def createlogicalnetwork(self,phynetwork,id = None,**kwargs):
        
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

        network = {'phynetwork':phynetwork,'id':id}
        network.update(kwargs)

        for m in self.createlogicalnetworks([network]):
            yield m
    def createlogicalnetworks(self,networks):
        
        # networks [{'phynetwork':'id','id':'id' ...},{'phynetwork':'id',...}]
        while True:

            fphynetkeys = []
            newnetworks = []
            for network in networks:
                network = copy.deepcopy(network)
                 
                if 'phynetwork' not in network:
                    raise ValueError("create logicalnet must special phynetwork id")

                network.setdefault('id',str(uuid1()))
                
                fphynetkeys.append(PhysicalNetwork.default_key(network.get('phynetwork')))
                newnetworks.append(network)

            self._reqid += 1
            reqid = ('viperflow',self._reqid)
            for m in callAPI(self.app_routine,'objectdb','mget',{'keys':list(set(fphynetkeys)),'requestid':reqid}):
                yield m
                 
            fphynetvalues = self.app_routine.retvalue
                
            if None in fphynetvalues:
                raise ValueError("physical net id " + PhysicalNetwork._getIndices(fphynetkeys[fphynetvalues.index(None)])[1][0] + " not existed")

            phynetdict = dict(zip(list(set(fphynetkeys)),fphynetvalues))

           
            typenetwork = {}
            for network in newnetworks:
                
                #network = copy.deepcopy(network)

                #if 'phynetwork' not in network:
                #    raise ValueError("create logicalnet must special phynetwork id")

                #network.setdefault('id',str(uuid1()))

                #phynetkey = PhysicalNetwork.default_key(network.get('phynetwork'))

                #for m in self._getkeys([phynetkey]):
                #    yield m

                phynetobj = phynetdict[PhysicalNetwork.default_key(network.get('phynetwork'))]

                #if len(phynetobj) == 0 or phynetobj[0] is None:
                #    raise ValueError("special phynetwork id not existed")
                
                phynettype = phynetobj.type

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

                phynetkey = list(set([PhysicalNetwork.default_key(n.get('phynetwork'))
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
            typesortvalues = []
            for _,v in typenetwork.items():
                keys.extend(v.get('lgnetkeys'))
                keys.extend(v.get('lgnetmapkeys'))
                keys.extend(v.get('phynetkeys'))
                keys.extend(v.get('phynetmapkeys'))

                typesortvalues.extend(phynetdict[key] for key in v['phynetkeys'])
            
            def updater(keys,values):
                retkeys = [keys[0]]
                retvalues = [None]
                lgnetset = values[0]
                start = 1
                sortindex = 0 
                for k,v in typenetwork.items():
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
                    
                    if [n.id if n is not None else None for n in typephynetvalues] != [n.id for n in typesortvalues[sortindex:sortindex + len(v.get('phynetkeys'))]]:
                        raise UpdateConflictException

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

                    sortindex = sortindex + len(v.get("phynetkeys"))
                
                retvalues[0] = lgnetset
                return retkeys,retvalues
            try:
                for m in callAPI(self.app_routine,'objectdb','transact',
                        {'keys':keys,'updater':updater}):
                    yield m
            except UpdateConflictException:
                logger.info(" cause UpdateConflict Exception try once")
                continue
            except:
                raise
            else:
                with watch_context(fphynetkeys,fphynetvalues,reqid,self.app_routine):
                    pass
                break
        
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
                    {'phynettype':lgnetworkobj[0].physicalnetwork.type,'id':id,'args':kwargs},timeout=1):
                yield m

        except:
            raise

        updater = self.app_routine.retvalue
        
        phynetkey = lgnetworkobj[0].physicalnetwork.getkey()
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
    
    def updatelogicalnetworks(self,networks):
        #networks [{'id':id,....},{'id':id,....}]
        while True:

            flgnetworkkeys = [] 
            newnetworks = [] 
            for network in networks:
                network = copy.deepcopy(network)
                
                flgnetworkkeys.append(LogicalNetwork.default_key(network["id"]))
                newnetworks.append(network)
                
            self._reqid += 1
            reqid = ('viperflow',self._reqid)
            for m in callAPI(self.app_routine,'objectdb','mget',{'keys':list(set(flgnetworkkeys)),'requestid':reqid}):
                yield m
            
            flgnetworkvalues = self.app_routine.retvalue
            

            if None in flgnetworkvalues:
                raise ValueError ("logical net id " + LogicalNetwork._getIndices(flgnetworkkeys[flgnetworkvalues.index(None)])[1][0] + " not existed")
            
            lgnetworkdict = dict(zip(flgnetworkkeys,flgnetworkvalues))

            typenetwork = {}
            for network in newnetworks:

                #network = copy.deepcopy(network)

                #lgnetworkkey = LogicalNetwork.default_key(network.get("id"))
                
                #for m in self._getkeys([lgnetworkkey]):
                #    yield m

                #lgnetworkobj = self.app_routine.retvalue
                #if len(lgnetworkobj) == 0 or lgnetworkobj[0] is None:
                #    raise ValueError("logicalnetwork key %r is not existd",network.get('id'))
                
                # we save phynetid in update object, used to find phynet,phymap
                # will del this attr when update in driver

                lgnetworkobj = lgnetworkdict[LogicalNetwork.default_key(network["id"])] 
                
                network["phynetid"] = lgnetworkobj.physicalnetwork.id
                
                if lgnetworkobj.physicalnetwork.type not in typenetwork:
                    typenetwork.setdefault(lgnetworkobj.physicalnetwork.type,{'networks':[network],
                        'phynetkey':[lgnetworkobj.physicalnetwork.getkey()]})
                else:
                    typenetwork.get(lgnetworkobj.physicalnetwork.type).get('networks').append(network)
                    typenetwork.get(lgnetworkobj.physicalnetwork.type).get('phynetkey').append(lgnetworkobj.physicalnetwork.getkey())
            
            for k,v in typenetwork.items():
                try:
                    for m in callAPI(self.app_routine,'public','updatelogicalnetworks',
                        {'phynettype':k,'networks':v.get('networks')},timeout=1):
                        yield m
                except:
                    raise

                updater = self.app_routine.retvalue
                
                lgnetworkkeys = [LogicalNetwork.default_key(n.get('id')) 
                            for n in v.get('networks')]
                
                phynetkeys = list(set([k for k in v.get('phynetkey')])) 
                
                phynetmapkeys = [PhysicalNetworkMap.default_key(PhysicalNetwork._getIndices(key)[1][0])
                                    for key in phynetkeys]
                v['lgnetworkkeys'] = lgnetworkkeys
                v['updater'] = updater
                v['phynetkeys'] = phynetkeys
                v['phynetmapkeys'] = phynetmapkeys
            
            keys = []
            typesortvalues = []
            for _,v in typenetwork.items():
                keys.extend(v.get("lgnetworkkeys")) 
                keys.extend(v.get("phynetkeys"))
                keys.extend(v.get("phynetmapkeys"))
                typesortvalues.extend(lgnetworkdict[key] for key in v.get("lgnetworkkeys"))
            
            def updater(keys,values):
                
                start = 0
                sortd_index = 0 
                retkeys = []
                retvalues = []
                for k,v in typenetwork.items():
                    typelgnetkeys = keys[start:start+len(v.get('lgnetworkkeys'))]    
                    typelgnetvalues = values[start:start+len(v.get('lgnetworkkeys'))]   
                    
                    if [n.physicalnetwork.getkey() if n is not None else None for n in typelgnetvalues] != [n.physicalnetwork.getkey() for n in typesortvalues[sortd_index:sortd_index + len(v.get("lgnetworkkeys"))]]:
                        raise UpdateConflictException
                    
                    phynetkeys = keys[start + len(v.get('lgnetworkkeys')):
                                        start + len(v.get('lgnetworkkeys'))+len(v.get("phynetkeys"))]
                    phynetvalues = values[start + len(v.get('lgnetworkkeys')):
                                        start + len(v.get('lgnetworkkeys'))+len(v.get("phynetkeys"))]
                   
                    phynetmapkeys = keys[start+len(v.get("lgnetworkkeys"))+len(v.get('phynetkeys')):
                                        start+len(v.get("lgnetworkkeys"))+len(v.get('phynetkeys'))+
                                        len(v.get("phynetmapkeys"))] 
                    phynetmapvalues = values[start+len(v.get("lgnetworkkeys"))+len(v.get('phynetkeys')):
                                        start+len(v.get("lgnetworkkeys"))+len(v.get('phynetkeys'))+
                                        len(v.get("phynetmapkeys"))] 
                    typeretkeys,typeretvalues = v.get('updater')(typelgnetkeys+phynetkeys+phynetmapkeys,
                                        typelgnetvalues+phynetvalues+phynetmapvalues)
                    
                    retkeys.extend(typeretkeys)
                    retvalues.extend(typeretvalues)
                    start = start + len(v.get('lgnetworkkeys'))+len(v.get("phynetkeys")) + \
                                len(v.get("phynetmapkeys"))
                    sortd_index = sortd_index + len(v.get("lgnetworkkeys"))
                return retkeys,retvalues


            try:
                for m in callAPI(self.app_routine,"objectdb",'transact',
                        {"keys":keys,'updater':updater}):
                    yield m
            except UpdateConflictException:
                logger.info(" cause UpdateConflict Exception try once")
                continue 
            except:
                raise
            else:
                with watch_context(flgnetworkkeys,flgnetworkvalues,reqid,self.app_routine):
                    pass
                break

        dumpkeys = []
        for _,v in typenetwork.items():
            dumpkeys.extend(v.get("lgnetworkkeys"))

        for m in self._dumpkeys(dumpkeys):
            yield m
        

    def deletelogicalnetwork(self,id):
        
        lgnetworkkey = LogicalNetwork.default_key(id)
        lgnetworkmapkey = LogicalNetworkMap.default_key(id)
        
        for m in self._getkeys([lgnetworkkey]):
            yield m
        
        lgnetworkobj = self.app_routine.retvalue
        
        if len(lgnetworkobj) == 0 or lgnetworkobj[0] is None:
            raise ValueError("lgnetwork id not existed "+id)

        try:
            for m in callAPI(self.app_routine,'public','deletelogicalnetwork',
                    {'phynettype':lgnetworkobj[0].physicalnetwork.type,'id':id},timeout=1):
                yield m

        except:
            raise

        updater = self.app_routine.retvalue
        
        phynetkey = lgnetworkobj[0].physicalnetwork.getkey()
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
    
    def deletelogicalnetworks(self,networks):
        # networks [{"id":id},{"id":id}]
        while True:
            flgnetworkkeys = []
            newnetworks = []
            for network in networks:
                network = copy.deepcopy(network)
                flgnetworkkeys.append(LogicalNetwork.default_key(network["id"]))
                newnetworks.append(network)
                
            self._reqid += 1
            reqid = ('viperflow',self._reqid)
            for m in callAPI(self.app_routine,'objectdb','mget',{'keys':list(set(flgnetworkkeys)),'requestid':reqid}):
                yield m
               
            flgnetworkvalues = self.app_routine.retvalue
            
            lgnetworkdict = dict(zip(flgnetworkkeys,flgnetworkvalues))

            typenetwork = {}
            for network in networks:

                #network = copy.deepcopy(network)

                #lgnetworkkey = LogicalNetwork.default_key(network.get("id"))
                
                #for m in self._getkeys([lgnetworkkey]):
                #    yield m

                #lgnetworkobj = self.app_routine.retvalue
                #if len(lgnetworkobj) == 0 or lgnetworkobj[0] is None:
                #    continue 

                lgnetworkobj = lgnetworkdict[LogicalNetwork.default_key(network["id"])]
                
                # we save phynetid in update object, used to find phynet,phymap
                # will del this attr when update in driver
                network["phynetid"] = lgnetworkobj.physicalnetwork.id
                
                if lgnetworkobj.physicalnetwork.type not in typenetwork:
                    typenetwork.setdefault(lgnetworkobj.physicalnetwork.type,{'networks':[network],
                        'phynetkey':[lgnetworkobj.physicalnetwork.getkey()]})
                else:
                    typenetwork.get(lgnetworkobj.physicalnetwork.type).get('networks').append(network)
                    typenetwork.get(lgnetworkobj.physicalnetwork.type).get('phynetkey').append(lgnetworkobj.physicalnetwork.getkey())
            
            for k,v in typenetwork.items():
                try:
                    for m in callAPI(self.app_routine,'public','deletelogicalnetworks',
                        {'phynettype':k,'networks':v.get('networks')},timeout=1):
                        yield m
                except:
                    raise

                updater = self.app_routine.retvalue
                
                lgnetworkkeys = [LogicalNetwork.default_key(n.get('id')) 
                            for n in v.get('networks')]
                
                lgnetworkmapkeys = [LogicalNetworkMap.default_key(n.get('id')) 
                            for n in v.get('networks')]
                
                phynetkeys = list(set([k for k in v.get('phynetkey')])) 
                
                phynetmapkeys = [PhysicalNetworkMap.default_key(PhysicalNetwork._getIndices(key)[1][0])
                                    for key in phynetkeys]
                v['lgnetworkkeys'] = lgnetworkkeys
                v['updater'] = updater
                v['lgnetworkmapkeys'] = lgnetworkmapkeys
                v['phynetmapkeys'] = phynetmapkeys
            
            keys = [LogicalNetworkSet.default_key()]
            typesortvalues = []
            for _,v in typenetwork.items():
                keys.extend(v.get("lgnetworkkeys")) 
                keys.extend(v.get("lgnetworkmapkeys"))
                keys.extend(v.get("phynetmapkeys"))
                typesortvalues.extend(lgnetworkdict[key] for key in v.get("lgnetworkkeys"))
            
            def updater(keys,values):
                
                start = 1
                sortd_index = 0
                retkeys = [keys[0]]
                retvalues = [None]
                lgnetset = values[0]
                for k,v in typenetwork.items():
                    typelgnetkeys = keys[start:start+len(v.get('lgnetworkkeys'))]    
                    typelgnetvalues = values[start:start+len(v.get('lgnetworkkeys'))]   
                    
                    if [n.physicalnetwork.getkey() if n is not None else None for n in typelgnetvalues] != [n.physicalnetwork.getkey() if n is not None else None for n in typesortvalues[sortd_index:sortd_index + len(v.get('lgnetworkkeys'))]]:
                        raise UpdateConflictException

                    phynetkeys = keys[start + len(v.get('lgnetworkkeys')):
                                        start + len(v.get('lgnetworkkeys'))+len(v.get("lgnetworkmapkeys"))]
                    phynetvalues = values[start + len(v.get('lgnetworkkeys')):
                                        start + len(v.get('lgnetworkkeys'))+len(v.get("lgnetworkmapkeys"))]
                   
                    phynetmapkeys = keys[start+len(v.get("lgnetworkkeys"))+len(v.get('lgnetworkmapkeys')):
                                        start+len(v.get("lgnetworkkeys"))+len(v.get('lgnetworkmapkeys'))+
                                        len(v.get("phynetmapkeys"))] 
                    phynetmapvalues = values[start+len(v.get("lgnetworkkeys"))+len(v.get('lgnetworkmapkeys')):
                                        start+len(v.get("lgnetworkkeys"))+len(v.get('lgnetworkmapkeys'))+
                                        len(v.get("phynetmapkeys"))] 
                    typeretkeys,typeretvalues = v.get('updater')(keys[0:1]+typelgnetkeys+phynetkeys+phynetmapkeys,
                                        [lgnetset]+typelgnetvalues+phynetvalues+phynetmapvalues)
                    
                    retkeys.extend(typeretkeys[1:])
                    retvalues.extend(typeretvalues[1:])
                    lgnetset = typeretvalues[0]
                    start = start + len(v.get('lgnetworkkeys'))+len(v.get("lgnetworkmapkeys")) + \
                                len(v.get("phynetmapkeys"))
                    sortd_index = sortd_index + len(v.get('lgnetworkkeys'))
                retvalues[0] = lgnetset
                return retkeys,retvalues


            try:
                for m in callAPI(self.app_routine,"objectdb",'transact',
                        {"keys":keys,'updater':updater}):
                    yield m

            except UpdateConflictException:
                logger.info(" cause UpdateConflict Exception try once")
                continue 
            except:
                raise
            else:
                with watch_context(flgnetworkkeys,flgnetworkvalues,reqid,self.app_routine):
                    pass
                break

        self.app_routine.retvalue = {"status":'OK'}
 
    def listlogicalnetwork(self,id = None,phynetwork = None,**args):
        
        def set_walker(key,set,walk,save):

            for weakobj in set.dataset():
                lgnetkey = weakobj.getkey()

                try:
                    lgnet = walk(lgnetkey)
                except KeyError:
                    pass
                else: 
                    if not phynetwork:
                        if all(getattr(lgnet,k,None) == v for k,v in args.items()):
                            save(lgnetkey)
                    else:
                        try:
                            phynet = walk(lgnet.physicalnetwork.getkey())
                        except KeyError:
                            pass
                        else:
                            if phynet.id == phynetwork:
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
    

    def createlogicalport(self,logicalnetwork,id = None,**args):

        if not id:
            id = str(uuid1())

        port = {'logicalnetwork':logicalnetwork,'id':id}
        port.update(args)

        for m in self.createlogicalports([port]):
            yield m

    def createlogicalports(self,ports):
        
        flgnetworkkeys = []
        newports = []
        for port in ports:
            port = copy.deepcopy(port)
            
            #lgnetworkkeys.append(LogicalNetwork.default_key(port["logicalnetwork"]))
            port.setdefault('id',str(uuid1()))
            
            newports.append(port)
        
        """
        self._reqid += 1
        reqid = ('viperflow',self._reqid)
        for m in callAPI(self.app_routine,'objectdb','mget',{'keys':list(set(flgnetworkkeys)),'requestid':reqid}):
            yield m
       
        flgnetworkvalues = self.app_routine.retvalue
        
        
        if None in flgnetworkvalues:
            raise ValueError ("logical net id " + LogicalNetwork._getIndices(flgnetworkkeys[flgnetworkvalues.index(None)])[1][0] + " not existed")
        
        lgnetworkdict = dict(zip(flgnetworkkeys,flgnetworkvalues))
        #for port in new:ports:

            #port = copy.deepcopy(port)

            #lognetkey = LogicalNetwork.default_key(port.get("logicalnetwork"))

            #for m in self._getkeys([lognetkey]):
            #    yield m

            #lognetobj = self.app_routine.retvalue

            #if len(lognetobj) == 0 or lognetobj[0] is None:
            #    raise ValueError("logicalnetwork not existed " +port.get("logicalnetwork"))
            
            #port.setdefault('id',str(uuid1()))
        """
        lgportsetkey = LogicalPortSet.default_key()
        lgportkeys = [LogicalPort.default_key(p.get('id')) for p in newports]
        lgports = [self._createlogicalports(**p) for p in newports]
        lgnetkeys = list(set([p.network.getkey() for p in lgports]))
        lgnetmapkeys = [LogicalNetworkMap.default_key(LogicalNetwork._getIndices(k)[1][0]) for k in lgnetkeys]
        
        def updater(keys,values):
            netkeys = keys[1+len(lgportkeys):1+len(lgportkeys)+len(lgnetkeys)]
            netvalues = values[1+len(lgportkeys):1+len(lgportkeys)+len(lgnetkeys)]
       
       
            netmapkeys = keys[1+len(lgportkeys)+len(lgnetkeys):]
            netmapvalues = values[1+len(lgportkeys)+len(lgnetkeys):]
            lgnetdict = dict(zip(netkeys,zip(netvalues,netmapvalues)))
            
            for i in range(0,len(newports)):
                values[1+i] = set_new(values[1+i],lgports[i])

                _,netmap = lgnetdict.get(lgports[i].network.getkey())

                if netmap:
                    netmap.ports.dataset().add(lgports[i].create_weakreference())
                    values[0].set.dataset().add(lgports[i].create_weakreference())
                else:
                    raise ValueError("lgnetworkkey not existed "+lgports[i].network.getkey())
            return keys,values
        try:
            for m in callAPI(self.app_routine,'objectdb','transact',
                    {"keys":[lgportsetkey]+lgportkeys+lgnetkeys+lgnetmapkeys,'updater':updater}):
                yield m
        except:
            raise

        for m in self._dumpkeys(lgportkeys):
            yield m
    def _createlogicalports(self,id,logicalnetwork,**args):

        lgport = LogicalPort.create_instance(id)
        lgport.network = ReferenceObject(LogicalNetwork.default_key(logicalnetwork))

        for k,v in args.items():
            setattr(lgport,k,v)

        return lgport

    def updatelogicalport(self,id,**kwargs):
        
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
    
    def updatelogicalports(self,ports):
        
        # ports [{"id":id,...},{...}]
        """
        for port in ports:
            
            port = copy.deepcopy(port)

            lgportkey = LogicalPort.default_key(port.get("id"))
            
            for m in self._getkeys([lgportkey]):
                yield m

            lgportobj = self.app_routine.retvalue
            if len(lgportobj) == 0 or lgportobj[0] is None:
                raise ValueError("logical port id not existed "+ port.get("id"))
        """
        while True:
            lgportkeys = list(set([LogicalPort.default_key(port.get("id")) 
                            for port in ports]))
            self._reqid += 1
            reqid = ('viperflow',self._reqid)
            for m in callAPI(self.app_routine,'objectdb','mget',{'keys':lgportkeys,'requestid':reqid}):
                yield m
            
            lgportvalues = self.app_routine.retvalue
            if None in lgportvalues:
                raise ValueError("logical port id not existed "+LogicalPort._getIndices(lgportkeys[lgportvalues.index(None)])[1][0])
            
            def update(keys,values):

                if [v.id if v is not None else None for v in values] != [v.id for v in lgportvalues]:
                    raise UpdateConflictException
                
                lgportdict = dict(zip(keys,values))
                for port in ports:
                    lgport = lgportdict.get(LogicalPort.default_key(port.get("id")))

                    for k,v in port.items():
                        setattr(lgport,k,v)

                return keys,values
            try:
                for m in callAPI(self.app_routine,"objectdb","transact",
                        {"keys":lgportkeys,"updater":update}):
                    yield m
            except UpdateConflictException:
                logger.info(" cause UpdateConflict Exception try once")
                continue
            except:
                raise
            else:
                with watch_context(lgportkeys,lgportvalues,reqid,self.app_routine):
                    pass
                break

        for m in self._dumpkeys(lgportkeys):
            yield m
    def deletelogicalport(self,id):

        lgportkey = LogicalPort.default_key(id)

        for m in self._getkeys([lgportkey]):
            yield m

        lgportobj = self.app_routine.retvalue

        if len(lgportobj) == 0 or lgportobj[0] is None:
            raise ValueError("logical port id not existed "+id)

        lgnetmapkey = LogicalNetworkMap.default_key(lgportobj[0].network.id)

        lgportsetkey = LogicalPortSet.default_key()

        @updater
        def update(portset,lgnetmap,lgport):
            
            portset.set.dataset().discard(lgport.create_weakreference())
           
            lgnetmap.ports.dataset().discard(lgport.create_weakreference())
           
            return [portset,lgnetmap,None]
        
        try:
            for m in callAPI(self.app_routine,'objectdb','transact',
                    {"keys":[lgportsetkey,lgnetmapkey,lgportkey],'updater':update}):
                yield m
        except:
            raise

        self.app_routine.retvalue = {"status":'OK'}
    
    def deletelogicalports(self,ports):
        
        """
        p = []
        for port in ports:

            port = copy.deepcopy(port)
            
            lgportkey = LogicalPort.default_key(port.get("id"))
            for m in self._getkeys([lgportkey]):
                yield m
            lgportobj = self.app_routine.retvalue

            if len(lgportobj) == 0 or lgportobj[0] is None:
                continue

            port['lgnetid'] = lgportobj[0].network.id
            p.append(port)
        """
        while True:

            lgportkeys = [LogicalPort.default_key(port.get("id"))
                            for port in ports]
            self._reqid += 1
            reqid = ('viperflow',self._reqid)
            for m in callAPI(self.app_routine,'objectdb','mget',{'keys':lgportkeys,'requestid':reqid}):
                yield m
                
            flgportvalues = self.app_routine.retvalue

            lgportdict = dict(zip(lgportkeys,flgportvalues))
            
            p = []
            for port in ports:
                port = copy.deepcopy(port)

                portobj = lgportdict[LogicalPort.default_key(port["id"])]
                if portobj:
                    port['lgnetid'] = portobj.network.id
                    p.append(port)


            lgnetmapkeys = list(set([LogicalNetworkMap.default_key(port.get("lgnetid"))
                            for port in p]))
            
            keys = [LogicalPortSet.default_key()] + lgportkeys + lgnetmapkeys
            
            def update(keys,values):
                lgportkeys = keys[1:1+len(ports)]
                lgportvalues = values[1:1+len(ports)]
                
                if [v.network.getkey() if v is not None else None for v in flgportvalues] != [v.network.getkey() if v is not None else None for v in lgportvalues]:
                    raise UpdateConflictException
                
                lgnetmapkeys = keys[1+len(ports):]
                lgnetmapvalues = values[1+len(ports):]
                
                lgportdict = dict(zip(lgportkeys,lgportvalues))
                lgnetmapdict = dict(zip(lgnetmapkeys,lgnetmapvalues))

                for port in p:
                    lgport = lgportdict.get(LogicalPort.default_key(port.get("id")))
                    lgnetmap = lgnetmapdict.get(LogicalNetworkMap.default_key(port.get("lgnetid")))
                    
                    lgnetmap.ports.dataset().discard(lgport.create_weakreference())
                   
                    values[0].set.dataset().discard(lgport.create_weakreference())
                return keys,[values[0]]+[None]*len(ports)+lgnetmapvalues
            try:
                for m in callAPI(self.app_routine,"objectdb","transact",
                    {"keys":keys,"updater":update}):
                    yield m
            except UpdateConflictException:
                logger.info(" cause UpdateConflict Exception try once")
                continue
            except:
                raise
            else:
                with watch_context(lgportkeys,flgportvalues,reqid,self.app_routine):
                    pass
                break
        self.app_routine.retvalue = {"status":'OK'}
    
    def listlogicalport(self,id = None,logicalnetwork = None,**kwargs):
        def set_walker(key,set,walk,save):

            for weakobj in set.dataset():
                lgportkey = weakobj.getkey()

                try:
                    lgport = walk(lgportkey)
                except KeyError:
                    pass
                else: 
                    if not logicalnetwork:
                        if all(getattr(lgport,k,None) == v for k,v in kwargs.items()):
                            save(lgportkey)
                    else:
                        try:
                            lgnet = walk(lgport.network.getkey())
                        except:
                            pass
                        else:
                            if lgnet.id == logicalnetwork:
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


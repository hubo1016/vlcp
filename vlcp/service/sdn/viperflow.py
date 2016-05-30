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
                       api(self.listphysicalnetworks,self.app_routine),
                       api(self.createphysicalport,self.app_routine),
                       api(self.createphysicalports,self.app_routine),
                       api(self.updatephysicalport,self.app_routine),
                       api(self.updatephysicalports,self.app_routine),
                       api(self.deletephysicalport,self.app_routine),
                       api(self.deletephysicalports,self.app_routine),
                       api(self.listphysicalports,self.app_routine),
                       api(self.createlogicalnetwork,self.app_routine),
                       api(self.createlogicalnetworks,self.app_routine),
                       api(self.updatelogicalnetwork,self.app_routine),
                       api(self.updatelogicalnetworks,self.app_routine),
                       api(self.deletelogicalnetwork,self.app_routine),
                       api(self.deletelogicalnetworks,self.app_routine),
                       api(self.listlogicalnetworks,self.app_routine),
                       api(self.createlogicalport,self.app_routine),
                       api(self.createlogicalports,self.app_routine),
                       api(self.updatelogicalport,self.app_routine),
                       api(self.updatelogicalports,self.app_routine),
                       api(self.deletelogicalport,self.app_routine),
                       api(self.deletelogicalports,self.app_routine),
                       api(self.listlogicalports,self.app_routine)
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
        "create physicalnetwork , return created info"        
        if not id:
            id = str(uuid1())
        
        network = {'type':type,'id':id}
        network.update(kwargs)

        for m in self.createphysicalnetworks([network]):
            yield m
    def createphysicalnetworks(self,networks):
        "create multi physicalnetworks, return created infos"
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
        "update physicalnetwork that id info,return updated info"
        
        if id is None:
            raise ValueError("update must be special id")
        
        network = {"id":id}
        network.update(kwargs)

        for m in self.updatephysicalnetworks([network]):
            yield m

    def updatephysicalnetworks(self,networks):
        "update multi physicalnetworks that id info,return updated infos"
        
        # networks [{"id":phynetid,....}]
        
        phynetkeys = set()
        typenetworks = dict()
        for network in networks:
            if 'type' in network:
                raise ValueError("physicalnetwork type can't be change")
            
            if 'id' not in network:
                raise ValueError("must special id")
            
            phynetkey = PhysicalNetwork.default_key(network['id'])
            if phynetkey not in phynetkeys:
                phynetkeys.add(phynetkey)
            else:
                raise ValueError("key repeat "+network['id'])
        
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
        "delete physicalnetwork that id,return status OK"
        if id is None:
            raise ValueError("delete netwrok must special id")
        network = {"id":id}

        for m in self.deletephysicalnetworks([network]):
            yield m
    def deletephysicalnetworks(self,networks):
        "delete physicalnetworks that ids,return status OK"
        # networks [{"id":id},{"id":id}]
        
        typenetworks = dict()
        phynetkeys = set()
        for network in networks:
            if 'id' not in network:
                raise ValueError("must special id")

            phynetkey = PhysicalNetwork.default_key(network['id'])
            if phynetkey not in phynetkeys:
                phynetkeys.add(phynetkey)
            else:
                raise ValueError("key repeate "+network['id'])
        
        phynetkeys = list(phynetkeys)

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

    def listphysicalnetworks(self,id = None,**kwargs):
        "list physcialnetwork infos" 
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
        "create physicalport,return created info" 
        port = {'physicalnetwork':physicalnetwork,'name':name,'vhost':vhost,'systemid':systemid,'bridge':bridge}
        port.update(kwargs)

        for m in self.createphysicalports([port]):
            yield m
    def createphysicalports(self,ports):
        "create multi physicalport, return created infos"
        # ports [{'physicalnetwork':id,'name':eth0,'vhost':'',systemid:'%'},{.....}]
        
        phynetkeys = []
        newports = []
        porttype = dict()
        idset = set()

        for port in ports:
            port = copy.deepcopy(port)
            if 'name' not in port:
                raise ValueError("must special name")

            if 'physicalnetwork' not in port:
                raise ValueError("must special physicalnetwork")
            
            port.setdefault('vhost','')
            port.setdefault('systemid','%')
            port.setdefault('bridge','%')

            key = '.'.join([port['vhost'],port['systemid'],port['bridge'],port['name']])
            if key not in idset:
                idset.add(key)
            else:
                raise ValueError("key repeat "+ key)

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
        "update physicalport info that id, return updated info" 
        if not name:
            raise ValueError("must speclial physicalport name")
        
        port = {'name':name,'vhost':vhost,'systemid':systemid,'bridge':bridge}
        port.update(args)

        for m in self.updatephysicalports([port]):
            yield m
    
    def updatephysicalports(self,ports):
        "update multi physicalport info that ids, return updated infos" 
        # ports [{'name':eth0,'vhost':'',systemid:'%'},{.....}]
        
        self._reqid += 1
        reqid = ('viperflow',self._reqid)
        max_try = 1
        
        fphysicalportkeys = set()
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
            
            if portkey not in fphysicalportkeys:
                fphysicalportkeys.add(portkey)
            else:
                raise ValueError("key repeat "+ portkey)
            
            newports.append(port)
            
        fphysicalportkeys = list(fphysicalportkeys)
        for m in callAPI(self.app_routine,'objectdb','mget',{'keys':fphysicalportkeys,'requestid':reqid}):
            yield m
        
        fphysicalportvalues = self.app_routine.retvalue
        
        if None in fphysicalportvalues:
            with watch_context(fphysicalportkeys,fphysicalportvalues,reqid,self.app_routine):
                pass
            raise ValueError(" physical ports is not existed "+ fphysicalportkeys[fphysicalportvalues.index(None)])
        
        physicalportdict = dict(zip(fphysicalportkeys,fphysicalportvalues))

        try:
            while True:            
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
                    index = 0
                    retkeys = []
                    retvalues = []
                    for k,v in typeport.items():
                        typekeys = keys[start:start + len(v.get("portkeys"))]
                        typevalues = values[start:start + len(v.get("portkeys"))]
                        
                        if [n.physicalnetwork.getkey() if n is not None else None for n in typevalues] !=\
                                [n.physicalnetwork.getkey() for n in typesortvalues[index:index + len(v.get('portkeys'))]]:
                            raise UpdateConflictException

                        rettypekeys,rettypevalues = v.get('updater')(typekeys,typevalues)

                        retkeys.extend(rettypekeys)
                        retvalues.extend(rettypevalues)
                        start = start + len(v.get('portkeys'))
                        index = index + len(v.get('portkeys'))

                    return keys,values

                try:
                    for m in callAPI(self.app_routine,'objectdb','transact',
                            {"keys":keys,"updater":update}):
                        yield m
                except UpdateConflictException:
                    max_try -= 1
                    if max_try < 0:
                        raise
                    else:
                        logger.info(" cause UpdateConflict Exception try once")
                        continue
                except:
                    raise
                else:
                    break

        except:
            raise
        else:
            for m in self._dumpkeys(keys):
                yield m
        finally:
            with watch_context(fphysicalportkeys,fphysicalportvalues,reqid,self.app_routine):
                pass
    def deletephysicalport(self,name,vhost='',systemid='%',bridge='%'):
        "delete physicalport that id, return status OK" 
        if not name:
            raise ValueError("must speclial physicalport name")
        port = {'name':name,'vhost':vhost,'systemid':systemid,'bridge':bridge}

        for m in self.deletephysicalports([port]):
            yield m
    def deletephysicalports(self,ports):
        "delete physicalports that ids, return status OK" 
        self._reqid += 1
        reqid = ('viperflow',self._reqid)
        max_try = 1

        typeport = dict()
        fphysicalportkeys = set()
        newports = []
        for port in ports:

            port = copy.deepcopy(port)
             
            if 'name' not in port :
                raise ValueError("must speclial physicalport name")
            
            port.setdefault('vhost',"")
            port.setdefault('systemid',"%")
            port.setdefault('bridge',"%")
            
            portkey = PhysicalPort.default_key(port.get('vhost'),port.get("systemid"),
                        port.get("bridge"),port.get("name"))
            
            if portkey not in fphysicalportkeys:
                fphysicalportkeys.add(portkey)
            else:
                raise ValueError("key repeat "+portkey)
            
            newports.append(port)
            
        fphysicalportkeys = list(fphysicalportkeys)
        for m in callAPI(self.app_routine,'objectdb','mget',{'keys':fphysicalportkeys,'requestid':reqid}):
            yield m
            
        fphysicalportvalues = self.app_routine.retvalue
        
        if None in fphysicalportvalues:
            with watch_context(fphysicalportkeys,fphysicalportvalues,reqid,self.app_routine):
                pass
            raise ValueError(" physical ports is not existed "+ fphysicalportkeys[fphysicalportvalues.index(None)])
        
        physicalportdict = dict(zip(fphysicalportkeys,fphysicalportvalues))

        try:
            while True:
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
                        with watch_context(fphysicalportkeys,fphysicalportvalues,reqid,self.app_routine):
                            pass
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
                    index = 0
                    portset = values[0]
                    retkeys = [keys[0]]
                    retvalues = [None]
                    for k,v in typeport.items():
                        typekeylen = len(v['portkeys']) + len(v['phynetmapkeys'])
                        sortkeylen = len(v['portkeys'])
                        
                        if [n.physicalnetwork.getkey() if n is not None else None 
                                for n in values[start:start + sortkeylen]]!=\
                           [n.physicalnetwork.getkey() for n in typesortvalues[index:index+sortkeylen]]:
                            raise UpdateConflictException
                        
                        try:
                            typeretkeys,typeretvalues = v['updater'](keys[0:1]+keys[start:start+typekeylen],
                                    [portset]+values[start:start+typekeylen])
                        except:
                            raise
                        else:
                            retkeys.extend(typeretkeys[1:])
                            retvalues.extend(typeretvalues[1:])
                            portset = typeretvalues[0]
                            start = start + typekeylen
                            index = index + sortkeylen
                    retvalues[0] = portset

                    return retkeys,retvalues


                try:
                    for m in callAPI(self.app_routine,"objectdb","transact",
                            {"keys":keys,"updater":update}):
                        yield m
                except UpdateConflictException:
                    maxtry -= 1
                    if maxtry < 0:
                        raise
                    else:
                        logger.info(" cause UpdateConflict Exception try once")
                        continue
                except:
                    raise
                else:
                    break

        except:
            raise
        else:
            self.app_routine.retvalue = {"status":'OK'}
        finally:
            with watch_context(fphysicalportkeys,fphysicalportvalues,reqid,self.app_routine):
                pass
        
    def listphysicalports(self,name = None,physicalnetwork = None,vhost='',
            systemid='%',bridge='%',**args):
        "list physicalports info" 
        def set_walker(key,set,walk,save):

            for weakobj in set.dataset():
                phyportkey = weakobj.getkey()

                try:
                    phyport = walk(phyportkey)
                except:
                    pass
                
                if not physicalnetwork:
                    if all(getattr(phyport,k,None) == v for k,v in args.items()):
                        save(phyportkey)
                else:

                    try:
                        phynet = walk(phyport.physicalnetwork.getkey())
                    except:
                        pass
                    else:
                        if phynet.id == physicalnetwork:
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
    
    def createlogicalnetwork(self,physicalnetwork,id = None,**kwargs):
        "create logicalnetwork info,return creared info" 
        if not id:
            id = str(uuid1())
        network = {'physicalnetwork':physicalnetwork,'id':id}
        network.update(kwargs)

        for m in self.createlogicalnetworks([network]):
            yield m
    def createlogicalnetworks(self,networks):
        "create logicalnetworks info,return creared infos" 
        # networks [{'physicalnetwork':'id','id':'id' ...},{'physicalnetwork':'id',...}]
        
        idset = set()
        phynetkeys = []
        newnetworks = []

        for network in networks:
            network = copy.deepcopy(network)
            if 'physicalnetwork' not in network:
                raise ValueError("create logicalnet must special physicalnetwork id")
            
            if 'id' in network:
                if network['id'] not in idset:
                    idset.add(network['id'])
                else:
                    raise ValueError("key repeat "+network['id'])
            else:
                network.setdefault('id',str(uuid1()))
                
            phynetkeys.append(PhysicalNetwork.default_key(network.get('physicalnetwork')))
            newnetworks.append(network)

        phynetkeys = list(set(phynetkeys))
        for m in self._getkeys(phynetkeys):
            yield m

        phynetvalues = self.app_routine.retvalue
        
        if None in phynetvalues:
            raise ValueError("physicalnetwork key not existed " +\
                    PhysicalNetwork._getIndices(phynetkeys[phynetvalues.index(None)])[1][0])
        
        phynetdict = dict(zip(phynetkeys,phynetvalues)) 
        
        typenetwork = dict()
        for network in newnetworks:
            phynetobj = phynetdict[PhysicalNetwork.default_key(network.get('physicalnetwork'))]
            phynettype = phynetobj.type

            if phynettype not in typenetwork:
                typenetwork.setdefault(phynettype,{'networks':[network]})
            else:
                typenetwork[phynettype]['networks'].append(network)
        
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

            phynetkey = list(set([PhysicalNetwork.default_key(n.get('physicalnetwork'))
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
            lgnetset = values[0]
            start = 1
            for k,v in typenetwork.items():

                typekeylen = len(v['lgnetkeys']) + len(v['lgnetmapkeys']) +\
                        len(v['phynetkeys'])+len(v['phynetmapkeys'])

                try:
                    typeretkeys,typeretvalues = v['updater'](keys[0:1]+keys[start:start+typekeylen],
                            [lgnetset]+values[start:start+typekeylen])
                except:
                    raise
                else:
                    retkeys.extend(typeretkeys[1:])
                    retvalues.extend(typeretvalues[1:])
                    lgnetset = typeretvalues[0]
                    start = start + typekeylen

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
        "update logicalnetwork info that id, return updated info" 
        # update phynetid is disabled 

        if not id:
            raise ValueError("must special logicalnetwork id")

        network = {'id':id}
        network.update(kwargs)

        for m in self.updatelogicalnetworks([network]):
            yield m
    def updatelogicalnetworks(self,networks):
        "update logicalnetworks info that ids, return updated infos" 
        #networks [{'id':id,....},{'id':id,....}]

        self._reqid += 1
        reqid = ('viperflow',self._reqid)
        maxtry = 1
        
        flgnetworkkeys = set()
        newnetworks = [] 
        for network in networks:
            network = copy.deepcopy(network)
            key = LogicalNetwork.default_key(network['id'])
            if key not in flgnetworkkeys:
                flgnetworkkeys.add(key)
            else:
                raise ValueError("key repeate "+key)
            newnetworks.append(network)
        
        flgnetworkkeys = list(flgnetworkkeys)
        
        for m in callAPI(self.app_routine,'objectdb','mget',{'keys':flgnetworkkeys,'requestid':reqid}):
            yield m
            
        flgnetworkvalues = self.app_routine.retvalue
        if None in flgnetworkvalues:
            raise ValueError ("logical net id " + LogicalNetwork._getIndices(flgnetworkkeys[flgnetworkvalues.index(None)])[1][0] + " not existed")
        
        lgnetworkdict = dict(zip(flgnetworkkeys,flgnetworkvalues))
        
        try:

            while True:
                typenetwork = dict()
                for network in newnetworks:
                    lgnetworkobj = lgnetworkdict[LogicalNetwork.default_key(network["id"])] 
                    
                    network["phynetid"] = lgnetworkobj.physicalnetwork.id
                    
                    if lgnetworkobj.physicalnetwork.type not in typenetwork:
                        typenetwork.setdefault(lgnetworkobj.physicalnetwork.type,{'networks':[network],
                            'phynetkey':[lgnetworkobj.physicalnetwork.getkey()]})
                    else:
                        typenetwork[lgnetworkobj.physicalnetwork.type]['networks'].append(network)
                        typenetwork[lgnetworkobj.physicalnetwork.type]['phynetkey'].append(lgnetworkobj.physicalnetwork.getkey())

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
                    index = 0 
                    retkeys = []
                    retvalues = []
                    for k,v in typenetwork.items():
                        typekeylen = len(v['lgnetworkkeys']) + len(v['phynetkeys']) + len(v['phynetmapkeys'])
                        objlen = len(v['lgnetworkkeys'])
                        
                        if [n.physicalnetwork.getkey() if n is not None else None for n in values[start:start+objlen]] !=\
                                [n.physicalnetwork.getkey() for n in typesortvalues[index:index + objlen]]:
                            raise UpdateConflictException
                        try:
                            typeretkeys,typeretvalues = v['updater'](keys[start:start+typekeylen],
                                    values[start:start+typekeylen])
                        except:
                            raise
                        else:
                            retkeys.extend(typeretkeys)
                            retvalues.extend(typeretvalues)
                            start = start + typekeylen
                            index = index + objlen
                    return retkeys,retvalues


                try:
                    for m in callAPI(self.app_routine,"objectdb",'transact',
                            {"keys":keys,'updater':updater}):
                        yield m
                except UpdateConflictException:
                    maxtry -= 1
                    if maxtry < 0:
                        raise
                    else:
                        logger.info(" cause UpdateConflict Exception try once")
                        continue 
                except:
                    raise
                else:
                    break

        except:
            raise
        else:
            dumpkeys = []
            for _,v in typenetwork.items():
                dumpkeys.extend(v.get("lgnetworkkeys"))

            for m in self._dumpkeys(dumpkeys):
                yield m

        finally:
            with watch_context(flgnetworkkeys,flgnetworkvalues,reqid,self.app_routine):
                pass

    def deletelogicalnetwork(self,id):
        "delete logicalnetwork that id,return status OK" 
        if not id:
            raise ValueError("must special id")

        network = {'id':id}
        for m in self.deletelogicalnetworks([network]):
            yield m

    def deletelogicalnetworks(self,networks):
        "delete logicalnetworks that ids,return status OK" 
        # networks [{"id":id},{"id":id}]

        self._reqid += 1
        reqid = ('viperflow',self._reqid)
        maxtry = 1

        flgnetworkkeys = set()
        newnetworks = []

        for network in networks:
            network = copy.deepcopy(network)
            key = LogicalNetwork.default_key(network["id"])
            if key not in flgnetworkkeys:
                flgnetworkkeys.add(key)
            else:
                raise ValueError("key repeate "+key)
            newnetworks.append(network)
        
        flgnetworkkeys = list(flgnetworkkeys)

        for m in callAPI(self.app_routine,'objectdb','mget',{'keys':flgnetworkkeys,'requestid':reqid}):
            yield m
            
        flgnetworkvalues = self.app_routine.retvalue
        if None in flgnetworkvalues:
            raise ValueError ("logical net id " + LogicalNetwork._getIndices(flgnetworkkeys[flgnetworkvalues.index(None)])[1][0] + " not existed")
        
        lgnetworkdict = dict(zip(flgnetworkkeys,flgnetworkvalues))
        
        try:
            while True:
                typenetwork = dict()
                for network in newnetworks:
                    lgnetworkobj = lgnetworkdict[LogicalNetwork.default_key(network["id"])]
                    
                    # we save phynetid in update object, used to find phynet,phymap
                    # will del this attr when update in driver
                    network["phynetid"] = lgnetworkobj.physicalnetwork.id
                    
                    if lgnetworkobj.physicalnetwork.type not in typenetwork:
                        typenetwork.setdefault(lgnetworkobj.physicalnetwork.type,{'networks':[network],
                            'phynetkey':[lgnetworkobj.physicalnetwork.getkey()]})
                    else:
                        typenetwork[lgnetworkobj.physicalnetwork.type]['networks'].append(network)
                        typenetwork[lgnetworkobj.physicalnetwork.type]['phynetkey'].append(lgnetworkobj.physicalnetwork.getkey())
                        
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
                    index = 0
                    retkeys = [keys[0]]
                    retvalues = [None]
                    lgnetset = values[0]
                    for k,v in typenetwork.items():
                        typekeylen = len(v['lgnetworkkeys']) + len(v['lgnetworkmapkeys']) + len(v['phynetmapkeys'])
                        objlen = len(v['lgnetworkkeys'])
                        
                        if [n.physicalnetwork.getkey() if n is not None else None for n in values[start:start+objlen]] !=\
                                [n.physicalnetwork.getkey() for n in typesortvalues[index:index+objlen]]:
                            raise UpdateConflictException
                        try:
                            typeretkeys,typeretvalues = v['updater'](keys[0:1]+keys[start:start+typekeylen],
                                    [lgnetset]+values[start:start+typekeylen])
                        except:
                            raise
                        else:
                            retkeys.extend(typeretkeys[1:])
                            retvalues.extend(typeretvalues[1:])
                            lgnetset = typeretvalues[0]
                            start = start + typekeylen
                            index = index + objlen
                    retvalues[0] = lgnetset
                    return retkeys,retvalues

                try:
                    for m in callAPI(self.app_routine,"objectdb",'transact',
                            {"keys":keys,'updater':updater}):
                        yield m

                except UpdateConflictException:
                    maxtry -= 1
                    if maxtry < 0:
                        raise
                    else:
                        logger.info(" cause UpdateConflict Exception try once")
                        continue 
                except:
                    raise
                else:
                    break

        except:
            raise
        else:
            self.app_routine.retvalue = {"status":'OK'}
        finally:
             with watch_context(flgnetworkkeys,flgnetworkvalues,reqid,self.app_routine):
                pass
        
    def listlogicalnetworks(self,id = None,physicalnetwork = None,**args):
        "list logcialnetworks infos" 
        def set_walker(key,set,walk,save):

            for weakobj in set.dataset():
                lgnetkey = weakobj.getkey()

                try:
                    lgnet = walk(lgnetkey)
                except KeyError:
                    pass
                else: 
                    if not physicalnetwork:
                        if all(getattr(lgnet,k,None) == v for k,v in args.items()):
                            save(lgnetkey)
                    else:
                        try:
                            phynet = walk(lgnet.physicalnetwork.getkey())
                        except KeyError:
                            pass
                        else:
                            if phynet.id == physicalnetwork:
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
        "create logicalport info,return created info"
        if not id:
            id = str(uuid1())

        port = {'logicalnetwork':logicalnetwork,'id':id}
        port.update(args)

        for m in self.createlogicalports([port]):
            yield m

    def createlogicalports(self,ports):
        
        "create multi logicalport info,return created infos"
        
        idset = set()
        newports = []
        for port in ports:
            port = copy.deepcopy(port)
            if 'id' in ports:
                if ports['id'] not in idset():
                    idset.add(ports['id'])
                else:
                    raise ValueError("id repeat "+ id)
            else:
                port.setdefault('id',str(uuid1()))

            newports.append(port)

        lgportsetkey = LogicalPortSet.default_key()
        lgportkeys = [LogicalPort.default_key(p['id']) for p in newports]
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
        else:
            for m in self._dumpkeys(lgportkeys):
                yield m
    def _createlogicalports(self,id,logicalnetwork,**args):

        lgport = LogicalPort.create_instance(id)
        lgport.network = ReferenceObject(LogicalNetwork.default_key(logicalnetwork))

        for k,v in args.items():
            setattr(lgport,k,v)

        return lgport

    def updatelogicalport(self,id,**kwargs):
        "update logicalport that id,return updated info" 
        if not id :
            raise ValueError("must special id")

        port = {"id":id}
        port.update(kwargs)
        
        for m in self.updatelogicalports([port]):
            yield m
    def updatelogicalports(self,ports):
        "update logicalports that ids,return updated info" 
        # ports [{"id":id,...},{...}]
        lgportkeys = set()
        for port in ports:
            if 'id' in port:
                if port['id'] not in lgportkeys:
                    lgportkeys.add(port['id'])
                else:
                    raise ValueError("key repeat "+ port['id'])
            else:
                raise ValueError("must special id")
        
        lgportkeys = [LogicalPort.default_key(key) for key in lgportkeys]
        def update(keys,values):

            lgportdict = dict(zip(keys,values))
            for port in ports:

                lgport = lgportdict.get(LogicalPort.default_key(port["id"]))
                
                if not lgport:
                    raise ValueError("key object not existed "+ port['id'])
                
                for k,v in port.items():
                    setattr(lgport,k,v)

            return keys,values

        try:
            for m in callAPI(self.app_routine,"objectdb","transact",
                    {"keys":lgportkeys,"updater":update}):
                yield m
        except:
            raise
        else:
            for m in self._dumpkeys(lgportkeys):
                yield m

    def deletelogicalport(self,id):
        "delete logcialport that id, return status OK" 
        if not id:
            raise ValueError("must special id")
        p = {"id":id}
        for m in self.deletelogicalports([p]):
            yield m
    def deletelogicalports(self,ports):
        "delete logcialports that ids, return status OK" 
        self._reqid += 1
        reqid = ('viperflow',self._reqid)
        maxtry = 1
        lgportkeys = set()
        for port in ports:
            if 'id' in port:
                if port['id'] not in lgportkeys:
                    lgportkeys.add(port['id'])
                else:
                    raise ValueError("key repeat "+ port['id'])
            else:
                raise ValueError("must special id")


        lgportkeys = [LogicalPort.default_key(key) for key in lgportkeys]
        
        for m in callAPI(self.app_routine,'objectdb','mget',{'keys':lgportkeys,'requestid':reqid}):
            yield m
        
        lgportvalues = self.app_routine.retvalue

        if None in lgportvalues:
            raise ValueError("logicalport is not existed "+\
                    LogicalPort._getIndices(lgportkeys[lgportvalues.index(None)])[1][0])

        lgportdict = dict(zip(lgportkeys,lgportvalues))
        
        try:
            while True:
                newports = [] 
                for port in ports:
                    port = copy.deepcopy(port)
                    key = LogicalPort.default_key(port["id"])
                    portobj = lgportdict[key]
                    
                    # fake attr for delete
                    port['lgnetid'] = portobj.network.id
                    
                    newports.append(port)

                lgnetmapkeys = list(set([LogicalNetworkMap.default_key(p['lgnetid'])
                            for p in newports]))

                keys = [LogicalPortSet.default_key()] + lgportkeys + lgnetmapkeys
                
                def update(keys,values):
                    lgportkeys = keys[1:1+len(ports)]
                    lgportvalues = values[1:1+len(ports)]
                    
                    if [v.network.getkey() if v is not None else None for v in lgportvalues] !=\
                            [v.network.getkey() for v in lgportvalues]:
                        raise UpdateConflictException
                    
                    lgnetmapkeys = keys[1+len(ports):]
                    lgnetmapvalues = values[1+len(ports):]
                    
                    lgportdict = dict(zip(lgportkeys,lgportvalues))
                    lgnetmapdict = dict(zip(lgnetmapkeys,lgnetmapvalues))

                    for port in newports:
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
                    maxtry -= 1
                    if maxtry < 0:
                        raise
                    else:
                        logger.info(" cause UpdateConflict Exception try once")
                        continue
                except:
                    raise
                else:
                    break
        except:
            raise
        else:
            self.app_routine.retvalue = {"status":'OK'}
        finally:
            with watch_context(lgportkeys,lgportvalues,reqid,self.app_routine):
                pass

    def listlogicalports(self,id = None,logicalnetwork = None,**kwargs):
        "list logicalports infos"
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


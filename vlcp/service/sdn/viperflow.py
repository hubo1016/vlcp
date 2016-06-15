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
import itertools
import socket

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
                       api(self.listlogicalports,self.app_routine),
                       api(self.createsubnet,self.app_routine),
                       api(self.createsubnets,self.app_routine),
                       api(self.updatesubnet,self.app_routine),
                       api(self.updatesubnets,self.app_routine),
                       api(self.deletesubnet,self.app_routine),
                       api(self.deletesubnets,self.app_routine),
                       api(self.listsubnets,self.app_routine)
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
            if set is None:
                return
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
                if obj is None:
                    return
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
            self.app_routine.retvalue = {"status":'OK'}
        finally:
            with watch_context(fphysicalportkeys,fphysicalportvalues,reqid,self.app_routine):
                pass

    def listphysicalports(self,name = None,physicalnetwork = None,vhost='',
            systemid='%',bridge='%',**args):
        "list physicalports info"
        def set_walker(key,set,walk,save):
            if set is None:
                return
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
                if obj is None:
                    return
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
            if set is None:
                return
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
                if obj is None:
                    return
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
                if ports['id'] not in idset:
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

        flgportvalues = self.app_routine.retvalue

        if None in flgportvalues:
            raise ValueError("logicalport is not existed "+\
                    LogicalPort._getIndices(lgportkeys[flgportvalues.index(None)])[1][0])

        lgportdict = dict(zip(lgportkeys,flgportvalues))

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
                            [v.network.getkey() for v in flgportvalues]:
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
            with watch_context(lgportkeys,flgportvalues,reqid,self.app_routine):
                pass

    def listlogicalports(self,id = None,logicalnetwork = None,**kwargs):
        "list logicalports infos"
        def set_walker(key,set,walk,save):
            if set is None:
                return
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
                if obj is None:
                    return
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

    def createsubnet(self,logicalnetwork,id=None,**kwargs):
        "create subnet info"
        if not id:
            id = str(uuid1())
        subnet = {'id':id,'logicalnetwork':logicalnetwork}
        subnet.update(kwargs)

        for m in self.createsubnets([subnet]):
            yield m

    def createsubnets(self,subnets):
        "create subnets info"
        idset = set()
        newsubnets = list()
        for subnet in subnets:
            subnet = copy.deepcopy(subnet)
            if 'id' in subnet:
                if subnet['id'] not in idset:
                    idset.add(subnet['id'])
                else:
                    raise ValueError('id repeate' + id)
            else:
                subnet['id'] = str(uuid1())

            if 'logicalnetwork' not in subnet:
                raise ValueError('create subnet must special logicalnetwork')

            if 'cidr' not in subnet:
                raise ValueError('create subnet must special cidr')
            else:
                try:
                    cidr,prefix = parse_ip4_network(subnet['cidr'])
                except:
                    raise
                else:
                    if 'gateway' in subnet:
                        try:
                            gateway = parse_ip4_address(subnet['gateway'])
                            assert ip_in_network(gateway,cidr,prefix)
                        except:
                            raise

                        if 'allocated_start' not in subnet:
                            if 'allocated_end' not in subnet:
                                # we assume gateway is smallest in a allocate pool
                                start = gateway + 1
                                end = network_last(cidr,prefix)
                                if start == end:
                                    raise ValueError('special ' + subnet['gateway'] + " as small,\
                                     allocated is none in cird")

                                subnet['allocated_start'] = int_to_str(start)
                                subnet['allocated_end'] = int_to_str(end)
                            else:
                                try:
                                    end = parse_ip4_address(subnet['allocated_end'])
                                    assert ip_in_network(end,cidr,prefix)
                                    assert end != gateway
                                    assert end > network_first(cidr,prefix)
                                    if end > gateway:
                                        start = gateway + 1
                                    else:
                                        start = network_first(cidr,prefix)

                                    if start == end:
                                        raise ValueError("allocated pool is none")
                                    subnet["allocated_start"] = int_to_str(start)
                                except:
                                    raise
                        else:
                            if 'allocated_end' not in subnet:
                                try:
                                    start = parse_ip4_address(subnet['allocated_start'])
                                    assert ip_in_network(start,cidr,prefix)
                                    assert start != gateway
                                    assert start < network_last(cidr,prefix)
                                    if start > gateway:
                                        end = network_last(cidr,prefix)
                                    else:
                                        end = gateway - 1

                                    if start == end:
                                        raise ValueError("allocated pool is None")
                                    subnet['allocated_end'] = int_to_str(end)
                                except:
                                    raise
                            else:
                                try:
                                    start = parse_ip4_address(subnet['allocated_start'])
                                    end = parse_ip4_address(subnet['allocated_end'])
                                    assert start < end
                                    assert ip_in_network(start,cidr,prefix)
                                    assert ip_in_network(end,cidr,prefix)

                                    if start <= gateway <= end:
                                        raise ValueError('gateway must out of allocated pool')

                                except:
                                    raise

                    else:
                        if 'allocated_start' not in subnet:
                            if 'allocated_end' not in subnet:
                                subnet['allocated_start'] = int_to_str(network_first(cidr,prefix))
                                subnet['allocated_end'] = int_to_str(network_last(cidr,prefix))
                            else:
                                try:
                                    end = parse_ip4_address(subnet['allocated_end'])
                                    assert ip_in_network(end,cidr,prefix)
                                    assert end > network_first(cidr,prefix)
                                    subnet['allocated_start'] = int_to_str(network_first(cidr,prefix))
                                except:
                                    raise
                        else:
                            if 'allocated_end' not in subnet:
                                try:
                                    start = parse_ip4_address(subnet['allocated_start'])
                                    assert ip_in_network(start,cidr,prefix)
                                    assert start < network_last(cidr,prefix)
                                    subnet['allocated_end'] = int_to_str(network_last(cidr,prefix))
                                except:
                                    raise
                            else:
                                try:
                                    start = parse_ip4_address(subnet['allocated_start'])
                                    end = parse_ip4_address(subnet['allocated_end'])
                                    assert start < end
                                    assert ip_in_network(start,cidr,prefix)
                                    assert ip_in_network(end,cidr,prefix)
                                except:
                                    raise

            newsubnets.append(subnet)

        subnetobjs = [self._createsubnet(**sn) for sn in newsubnets]
        lgnetmapkeys = list(set([LogicalNetworkMap.default_key(sn['logicalnetwork']) for sn in newsubnets]))
        subnetobjkeys = [subnetobj[0].getkey() for subnetobj in subnetobjs]
        subnetmapobjkeys = [subnetobj[1].getkey() for subnetobj in subnetobjs]
        subnetsetkey = SubNetSet.default_key()

        keys = itertools.chain([subnetsetkey],subnetobjkeys,subnetmapobjkeys,lgnetmapkeys)

        def subnetupdate(keys, values):
            subnetobjlen = len(subnetobjs)
            setobj = values[0]
            lgnetmaps = values[1 + subnetobjlen * 2:]
            lgnetmkeys = [nm.getkey() for nm in lgnetmaps]
            lgnetmapdict = dict(zip(lgnetmkeys,lgnetmaps))

            for index,(subnet,subnetmap) in enumerate(subnetobjs):
                values[index + 1] = set_new(values[index + 1],subnet)
                values[index + 1 + subnetobjlen] = set_new(values[index + 1 + subnetobjlen],subnetmap)

                mk = LogicalNetworkMap.default_key(LogicalNetwork._getIndices(subnet.network.getkey())[1][0])
                lm = lgnetmapdict.get(mk)
                if lm:
                    lm.subnets.dataset().add(subnet.create_weakreference())
                else:
                    raise ValueError("logicalnetwork " + subnet.network.getkey() + " not existed")

                setobj.set.dataset().add(subnet.create_weakreference())

            return keys,values
        try:
            for m in callAPI(self.app_routine,'objectdb','transact',{"keys":keys,'updater':subnetupdate}):
                yield m
        except:
            raise
        else:
            for m in self._dumpkeys(subnetobjkeys):
                yield m

    def _createsubnet(self,id,logicalnetwork,**kwargs):
        subnetobj = SubNet.create_instance(id)
        subnetobj.network = ReferenceObject(LogicalNetwork.default_key(logicalnetwork))
        subnetmapobj = SubNetMap.create_instance(id)

        for k,v in kwargs.items():
            setattr(subnetobj,k,v)
        return subnetobj,subnetmapobj

    def updatesubnet(self,id,**kwargs):
        "update subnet info"
        if not id:
            raise ValueError("must special subnet id when updatesubnet")
        subnet = {"id":id}
        subnet.update(kwargs)

        for m in self.updatesubnets([subnet]):
            yield m

    def updatesubnets(self,subnets):
        "update subnets info"
        idset = set()
        for subnet in subnets:
            if 'cidr' in subnet:
                raise ValueError("subnet can not update cidr")
            if 'id' not in subnet:
                raise ValueError("update subnet must special id")
            else:
                if subnet['id'] not in idset:
                    idset.add(subnet['id'])
                else:
                    raise ValueError("id repeat error")
        subnetkeys = [SubNet.default_key(sn['id']) for sn in subnets]
        subnetmapkeys = [SubNetMap.default_key(sn['id']) for sn in subnets]

        keys = itertools.chain(subnetkeys,subnetmapkeys)
        def subnetupdate(keys, values):
            snkeys = keys[0:len(subnets)]
            subnetobj = values[0:len(subnets)]
            subnetmapobj = values[len(subnets):]

            subnetdict = dict(zip(snkeys,zip(subnetobj,subnetmapobj)))
            for sn in subnets:
                snkey = SubNet.default_key(sn['id'])
                snet,smap = subnetdict.get(snkey)
                if not snet or not smap:
                    raise ValueError(" update object "+ sn['id'] + "not existed" )

                # check ipaddress
                if 'gateway' in sn:
                    try:
                        parse_ip4_address(sn['gateway'])
                    except:
                        raise

                if 'allocated_start' in sn:
                    try:
                        parse_ip4_address(sn['allocated_start'])
                    except:
                        raise

                if 'allocated_end' in sn:
                    try:
                        parse_ip4_address(sn['allocated_end'])
                    except:
                        raise

                for k,v in sn.items():
                    setattr(snet,k,v)

                try:
                    check_ip_pool(gateway=getattr(snet,'gateway',None),start=snet.allocated_start,end=snet.allocated_end,
                                  allocated=smap.allocated_ips.keys(),cidr=snet.cidr)
                except:
                    raise ValueError("ip pool conflict gateway " + getattr(snet,'gateway','None') + " start " +\
                                     snet.allocated_start + " end " + snet.allocated_end + " cidr " + snet.cidr)
            return snkeys,subnetobj
        try:
            for m in callAPI(self.app_routine,'objectdb','transact',{'keys':keys,'updater':subnetupdate}):
                yield m
        except:
            raise
        else:
            for m in self._dumpkeys(subnetkeys):
                yield m

    def deletesubnet(self,id):
        "delete subnet info"
        if not id:
            raise ValueError("must special id")

        subnet = {"id":id}

        for m in self.deletesubnets([subnet]):
            yield m

    def deletesubnets(self,subnets):
        "delete subnets info"
        self._reqid += 1
        reqid = ('viperflow',self._reqid)
        maxtry = 1
        idset = set()
        for subnet in subnets:
            if 'id' not in subnet:
                raise ValueError("must special id")
            if subnet["id"] not in idset:
                idset.add(subnet["id"])
            else:
                raise ValueError("id repeat " + subnet["id"])

        subnetkeys = [SubNet.default_key(sn['id']) for sn in subnets]

        for m in callAPI(self.app_routine, "objectdb", "mget", {'keys': subnetkeys, 'requestid': reqid}):
            yield m

        fsubnetobjs = self.app_routine.retvalue
        if None in fsubnetobjs:
            raise ValueError(" subnet not existed " + SubNet._getIndices(subnetkeys[fsubnetobjs.index(None)])[1][0])

        subnetdict = dict(zip(subnetkeys,fsubnetobjs))

        try:
            while True:
                newsubnets = []
                for subnet in subnets:
                    subnet = copy.deepcopy(subnet)
                    subnetobj = subnetdict.get(SubNet.default_key(subnet['id']))
                    subnet['logicalnetwork'] = subnetobj.network.id

                    newsubnets.append(subnet)

                subnetmapkeys = [SubNetMap.default_key(sn['id']) for sn in newsubnets]

                lognetkeys = list(set([LogicalNetwork.default_key(sn['logicalnetwork']) for sn in newsubnets]))

                lognetmapkeys = [LogicalNetworkMap.default_key(LogicalNetwork._getIndices(key)[1][0])
                                 for key in lognetkeys]

                keys = itertools.chain([SubNetSet.default_key()],subnetkeys,subnetmapkeys,lognetkeys,lognetmapkeys)
                
                def subnetupdate(keys,values):
                    subset = values[0]
                    subnetlen = len(subnetkeys)
                    lognetlen = len(lognetkeys)

                    sk = keys[1:1 + subnetlen]
                    subnetobjs = values[1:1 + subnetlen]

                    smk = keys[1+subnetlen:1 + subnetlen + subnetlen]
                    subnetmapobjs = values[1+subnetlen:1 + subnetlen + subnetlen]
                    
                    lgnetkeys = keys[1 + subnetlen * 2:1 + subnetlen *2 + lognetlen]
                    lgnetobjs = values[1 + subnetlen * 2:1 + subnetlen *2 + lognetlen]

                    lgnetmapkeys = keys[1 + subnetlen *2 + lognetlen:]
                    lgnetmapobjs = values[1 + subnetlen *2 + lognetlen:]
                    
                    if [v.network.getkey() if v is not None else None for v in subnetobjs] !=\
                            [v.network.getkey() for v in fsubnetobjs]:
                        raise UpdateConflictException

                    subnetsdict = dict(zip(sk,zip(subnetobjs,subnetmapobjs)))
                    lgnetdict = dict(zip(lgnetkeys,zip(lgnetobjs,lgnetmapobjs)))
                    
                    for subnet in newsubnets:
                        k = SubNet.default_key(subnet['id'])
                        nk = LogicalNetwork.default_key(subnet['logicalnetwork'])

                        snobj,snmobj = subnetsdict.get(k)
                        if snmobj.allocated_ips:
                            raise ValueError("there logicalport in " + k + " delete it before")
                        _,lgnetmap = lgnetdict.get(nk)
                        lgnetmap.subnets.dataset().discard(snobj.create_weakreference())
                        subset.set.dataset().discard(snobj.create_weakreference())
                    
                    return itertools.chain([keys[0]],sk,smk,lgnetmapkeys),\
                            itertools.chain([subset],[None]*subnetlen,[None]*subnetlen,lgnetmapobjs)

                try:
                    for m in callAPI(self.app_routine,'objectdb','transact',
                                     {'keys':keys,'updater':subnetupdate}):
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
            with watch_context(subnetkeys,fsubnetobjs,reqid,self.app_routine):
                pass

    def listsubnets(self,id = None,logicalnetwork=None,**kwargs):
        "list subnets infos"
        def set_walker(key,set,walk,save):

            for weakobj in set.dataset():
                subnetkey = weakobj.getkey()

                try:
                    subnet = walk(subnetkey)
                except KeyError:
                    pass
                else:
                    if not logicalnetwork:
                        if all(getattr(subnet,k,None) == v for k,v in kwargs.items()):
                            save(subnetkey)
                    else:
                        try:
                            lgnet = walk(subnet.network.getkey())
                        except:
                            pass
                        else:
                            if lgnet.id == logicalnetwork:
                                if all(getattr(subnet,k,None) == v for k,v in kwargs.items()):
                                    save(subnetkey)

        def walker_func(set_func):

            def walker(key,obj,walk,save):
                set_walker(key,set_func(obj),walk,save)

            return walker

        if not id:
            # get all subnets
            subnetsetkey = SubNetSet.default_key()
            # an unique id used to unwatch
            self._reqid += 1
            reqid = ('viperflow',self._reqid)

            for m in callAPI(self.app_routine,'objectdb','walk',{'keys':[subnetsetkey],
                'walkerdict':{subnetsetkey:walker_func(lambda x:x.set)},
                'requestid':reqid}):
                yield m
            keys,values = self.app_routine.retvalue

            # dump will get reference
            with watch_context(keys,values,reqid,self.app_routine):
                self.app_routine.retvalue = [dump(r) for r in values]

        else:
            subnetkey = SubNet.default_key(id)

            for m in self._getkeys([subnetkey]):
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
        def init(physet,phyportset,logicset,logicportset,subnetset):
            if physet is None:
                physet = PhysicalNetworkSet()
            if phyportset is None:
                phyportset = PhysicalPortSet()
            if logicset is None:
                logicset = LogicalNetworkSet()
            if logicportset is None:
                logicportset = LogicalPortSet()
            if subnetset is None:
                subnetset = SubNetSet()

            return [physet,phyportset,logicset,logicportset,subnetset]

        # dataobject keys that will be init ,, add it if necessary
        initdataobjectkeys = [PhysicalNetworkSet.default_key(),
                PhysicalPortSet.default_key(),LogicalNetworkSet.default_key(),
                LogicalPortSet.default_key(),SubNetSet.default_key()]
        for m in callAPI(container,'objectdb','transact',
                {'keys':initdataobjectkeys,'updater':init}):
            yield m

        # call so main routine will be run
        for m in Module.load(self,container):
            yield m



def check_ip_pool(gateway, start, end, allocated, cidr):


    nstart = parse_ip4_address(start)
    nend = parse_ip4_address(end)
    ncidr,prefix = parse_ip4_network(cidr)
    if gateway:
        try:
            ngateway = parse_ip4_address(gateway)
            assert ip_in_network(ngateway,ncidr,prefix)
            assert ip_in_network(nstart,ncidr,prefix)
            assert ip_in_network(nend,ncidr,prefix)
            assert nstart < nend
            assert ngateway < nstart or ngateway > nend

            for ip in allocated:
                nip = parse_ip4_address(ip)
                assert nstart < nip < nend
        except:
            raise
    else:
        try:
            assert ip_in_network(nstart,ncidr,prefix)
            assert ip_in_network(nend,ncidr,prefix)
            assert nstart < nend

            for ip in allocated:
                nip = parse_ip4_address(ip)
                assert nstart < nip < nend
        except:
            raise

def parse_ip4_network( network ):

    if '/' not in network:
        raise ValueError("invalid cidr " + network)
    ip,prefix = network.rsplit('/',1)

    if not 0 < int(prefix) < 2 ** 32 - 1:
        raise ValueError("invalid prefix " + prefix)

    netmask = (2 ** 32 - 1) >> (32 - int(prefix)) << (32 - int(prefix))

    try:
        sip = socket.inet_pton(socket.AF_INET,ip)
        value = int.from_bytes(sip,byteorder='big')
    except:
        raise
    else:
        return value & netmask,int(prefix)

def parse_ip4_address(address):
    try:
        ip = socket.inet_pton(socket.AF_INET,address)
    except:
        raise
    else:
        return int.from_bytes(ip,byteorder='big')

def ip_in_network(ip,network,prefix):
    shift = 32 - prefix
    return (ip >> shift) == (network >> shift)

def network_first(network,prefix):
    return network + 1

def network_last(network,prefix):
    hostmask = (1 << (32 - prefix)) - 1
    return network | hostmask

def int_to_str(int_address):
    if 0 < int_address < 2**32 - 1:
        return '%d.%d.%d.%d'%(
                int_address >> 24,
                int_address >> 16 & 0xff,
                int_address >> 8 & 0xff,
                int_address & 0xff
        )
    else:
        raise ValueError("invaild address " + str(int_address))

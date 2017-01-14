#!/usr/bin/python
#! --*-- utf-8 --*--

from vlcp.config import defaultconfig
from vlcp.server.module import Module,depend,callAPI,api
from vlcp.event.runnable import RoutineContainer
from vlcp.utils.ethernet import ip4_addr,mac_addr
from vlcp.utils.dataobject import DataObjectSet,updater,\
            set_new,DataObjectUpdateEvent,watch_context,dump,ReferenceObject
import vlcp.service.kvdb.objectdb as objectdb

from vlcp.utils.networkmodel import *
from vlcp.utils.netutils import ip_in_network, network_first, network_last,\
                        parse_ip4_address, parse_ip4_network

from uuid import uuid1
import copy
import logging
import itertools

logger = logging.getLogger('viperflow')

#logger.setLevel(logging.DEBUG)

class UpdateConflictException(Exception):
    def __init__(self,desc="db update conflict"):
        super(UpdateConflictException,self).__init__(desc)


@defaultconfig
@depend(objectdb.ObjectDB)
class ViperFlow(Module):
    """
    Standard network model for L2 SDN
    """
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


    def _dumpone(self,key,filter):
        for m in self._getkeys([key]):
            yield m
        retobjs = self.app_routine.retvalue
        if len(retobjs) == 0 or retobjs[0] is None:
            self.app_routine.retvalue = []
        else:
            if all(getattr(retobjs[0], k, None) == v for k, v in filter.items()):
                self.app_routine.retvalue = dump(retobjs)
            else:
                self.app_routine.retvalue = []

    def _getkeys(self,keys):
        self._reqid += 1
        reqid = ('viperflow',self._reqid)
        for m in callAPI(self.app_routine,'objectdb','mget',{'keys':keys,'requestid':reqid}):
            yield m
        with watch_context(keys,self.app_routine.retvalue,reqid,self.app_routine):
            pass

    def createphysicalnetwork(self,type = 'vlan',id = None, **kwargs):
        """
        Create physical network.
        
        :param type: Network type, usually one of *vlan*, *vxlan*, *local*, *native*
        
        :param id: Specify the created physical network ID. If omitted or None, an UUID is
                   generated.
        
        :param \*\*kwargs: extended creation parameters. Look for the document of the corresponding
                           driver. Common options include:
                           
                           vnirange
                              list of ``[start,end]`` ranges like ``[[1000,2000]]``. Both *start* and
                              *end* are included. It specifies the usable VNI ranges for VXLAN network.
                              
                           vlanrange
                              list of ``[start,end]`` ranges like ``[[1000,2000]]``. Both *start* and
                              *end* are included. It specifies the usable VLAN tag ranges for VLAN network.
        
        :return: A dictionary of information of the created physical network.
        """
        if not id:
            id = str(uuid1())

        network = {'type':type,'id':id}
        network.update(kwargs)

        for m in self.createphysicalnetworks([network]):
            yield m
    def createphysicalnetworks(self,networks):
        """
        Create multiple physical networks in a transaction.
        
        :param networks: each should be a dictionary contains all the parameters in ``createphysicalnetwork``
        
        :return: A list of dictionaries of information of the created physical networks.
        """
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
        """
        Update physical network with the specified ID.
        
        :param id: physical network ID
        
        :param \*\*kwargs: attributes to be updated, usually the same attributes for creating.
        
        :return: A dictionary of information of the updated physical network.        
        """

        if id is None:
            raise ValueError("update must be special id")

        network = {"id":id}
        network.update(kwargs)

        for m in self.updatephysicalnetworks([network]):
            yield m

    def updatephysicalnetworks(self,networks):
        """
        Update multiple physical networks in a transaction
        
        :param networks: a list of dictionaries, each contains parameters of ``updatephysicalnetwork``
        
        :return: A list of dictionaries of information of the updated physical network.
        """

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
        """
        Delete physical network with specified ID
        
        :param id: Physical network ID
        
        :return: ``{"status": "OK"}``
        """
        if id is None:
            raise ValueError("delete netwrok must special id")
        network = {"id":id}

        for m in self.deletephysicalnetworks([network]):
            yield m
    def deletephysicalnetworks(self,networks):
        """
        Delete multiple physical networks with a transaction
        
        :param networks: a list of ``{"id": <id>}`` dictionaries.
        
        :return: ``{"status": "OK"}``
        """
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
        """
        Query physical network information
        
        :param id: If specified, only return the physical network with the specified ID.
        
        :param \*\*kwargs: customized filters, only return a physical network if
                           the attribute value of this physical network matches
                           the specified value.
        
        :return: A list of dictionaries each stands for a matched physical network
        """
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
            for m in self._dumpone(phynetkey,kwargs):
                yield m


    def createphysicalport(self,physicalnetwork,name,vhost='',systemid='%',bridge='%',**kwargs):
        """
        Create physical port
        
        :param physicalnetwork: physical network this port is in.
        
        :param name: port name of the physical port, should match the name in OVSDB
        
        :param vhost: only match ports for the specified vHost
        
        :param systemid: only match ports on this systemid; or '%' to match all systemids.
        
        :param bridge: only match ports on bridges with this name; or '%' to match all bridges.
        
        :param \*\*kwargs: customized creation options, check the driver document
        
        :return: A dictionary containing information of the created physical port.
        """
        port = {'physicalnetwork':physicalnetwork,'name':name,'vhost':vhost,'systemid':systemid,'bridge':bridge}
        port.update(kwargs)

        for m in self.createphysicalports([port]):
            yield m
    def createphysicalports(self,ports):
        """
        Create multiple physical ports in a transaction
        
        :param ports: A list of dictionaries, each contains all parameters for ``createphysicalport``
        
        :return: A list of dictionaries of information of the created physical ports
        """
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
        """
        Update physical port
        
        :param name: Update physical port with this name.
        
        :param vhost: Update physical port with this vHost.
        
        :param systemid: Update physical port with this systemid.
        
        :param bridge: Update physical port with this bridge name.
        
        :param \*\*kwargs: Attributes to be updated
        
        :return: Updated result as a dictionary.
        """
        if not name:
            raise ValueError("must speclial physicalport name")

        port = {'name':name,'vhost':vhost,'systemid':systemid,'bridge':bridge}
        port.update(args)

        for m in self.updatephysicalports([port]):
            yield m

    def updatephysicalports(self,ports):
        """
        Update multiple physical ports with a transaction
        
        :param ports: a list of ``updatephysicalport`` parameters
                
        :return: Updated result as a list of dictionaries.
        """
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

        with watch_context(fphysicalportkeys,fphysicalportvalues,reqid,self.app_routine):
            if None in fphysicalportvalues:
                raise ValueError(" physical ports is not existed "+ fphysicalportkeys[fphysicalportvalues.index(None)])
    
            physicalportdict = dict(zip(fphysicalportkeys,fphysicalportvalues))
    
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
                else:
                    break    
            for m in self._dumpkeys(keys):
                yield m

    def deletephysicalport(self,name,vhost='',systemid='%',bridge='%'):
        """
        Delete a physical port
        
        :param name: physical port name.
        
        :param vhost: physical port vHost.
        
        :param systemid: physical port systemid.
        
        :param bridge: physcial port bridge.
        
        :return: ``{"status": "OK"}``
        """
        if not name:
            raise ValueError("must speclial physicalport name")
        port = {'name':name,'vhost':vhost,'systemid':systemid,'bridge':bridge}

        for m in self.deletephysicalports([port]):
            yield m
    def deletephysicalports(self,ports):
        """
        Delete multiple physical ports in a transaction
        
                Delete a physical port
        
        :param ports: a list of ``deletephysicalport`` parameters
        
        :return: ``{"status": "OK"}``
        """
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

        with watch_context(fphysicalportkeys, fphysicalportvalues, reqid, self.app_routine):
            if None in fphysicalportvalues:
                raise ValueError(" physical ports is not existed "+ fphysicalportkeys[fphysicalportvalues.index(None)])

            physicalportdict = dict(zip(fphysicalportkeys,fphysicalportvalues))

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
                    for m in callAPI(self.app_routine,"public","deletephysicalports",
                            {"phynettype":k,"ports":v.get("ports")},timeout = 1):
                        yield m
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
                else:
                    break
            self.app_routine.retvalue = {"status":'OK'}

    def listphysicalports(self,name = None,physicalnetwork = None,vhost='',
            systemid='%',bridge='%',**kwargs):
        """
        Query physical port information
        
        :param name: If specified, only return the physical port with the specified name.
        
        :param physicalnetwork: If specified, only return physical ports in that physical network
        
        :param vhost: If specified, only return physical ports for that vHost.
        
        :param systemid: If specified, only return physical ports for that systemid.
        
        :param bridge: If specified, only return physical ports for that bridge. 
        
        :param \*\*kwargs: customized filters, only return a physical network if
                           the attribute value of this physical network matches
                           the specified value.
        
        :return: A list of dictionaries each stands for a matched physical network
        """

        if name:
            phyportkey = PhysicalPort.default_key(vhost,systemid,bridge,name)

            for m in self._dumpone(phyportkey,kwargs):
                yield m
        else:
            if physicalnetwork:
                # special physicalnetwork , find it in map , filter it ..
                physical_map_key = PhysicalNetworkMap.default_key(physicalnetwork)

                self._reqid += 1
                reqid = ('viperflow',self._reqid)

                def walk_map(key,value,walk,save):
                    if value is None:
                        return

                    for weakobj in value.ports.dataset():
                        phyport_key = weakobj.getkey()

                        try:
                            phyport_obj = walk(phyport_key)
                        except KeyError:
                            pass
                        else:
                            if all(getattr(phyport_obj,k,None) == v for k ,v in kwargs.items()):
                                save(phyport_key)

                for m in callAPI(self.app_routine,'objectdb','walk',{"keys":[physical_map_key],
                                        "walkerdict":{physical_map_key:walk_map},
                                        "requestid":reqid}):
                    yield m

                keys,values = self.app_routine.retvalue

                with watch_context(keys,values,reqid,self.app_routine):
                    self.app_routine.retvalue = [dump(r) for r in values]

            else:
                # find it in all, , filter it ,,
                phyport_set_key = PhysicalPortSet.default_key()

                self._reqid += 1
                reqid = ('viperflow',self._reqid)

                def walk_set(key,value,walk,save):
                    if value is None:
                        return

                    for weakobj in value.set.dataset():
                        phyport_key = weakobj.getkey()

                        try:
                            phyport_obj = walk(phyport_key)
                        except KeyError:
                            pass
                        else:
                            if all(getattr(phyport_obj,k,None) == v for k,v in kwargs.items()):
                                save(phyport_key)

                for m in callAPI(self.app_routine,'objectdb','walk',{"keys":[phyport_set_key],
                                        "walkerdict":{phyport_set_key:walk_set},
                                        "requestid":reqid}):
                    yield m

                keys, values = self.app_routine.retvalue

                with watch_context(keys,values,reqid,self.app_routine):
                    self.app_routine.retvalue = [dump(r) for r in values]

    def createlogicalnetwork(self,physicalnetwork,id = None,**kwargs):
        """
        Create logical network
        
        :param physicalnetwork: physical network ID that contains this logical network
        
        :param id: logical network ID. If ommited an UUID is generated.
        
        :param \*\*kwargs: customized options for logical network creation.
                           Common options include:
                           
                           vni/vxlan
                              Specify VNI / VLAN tag for VXLAN / VLAN network. If omitted,
                              an unused VNI / VLAN tag is picked automatically.
                           
                           mtu
                              MTU value for this network. You can use 1450 for VXLAN networks.
        
        :return: A dictionary of information of the created logical port
        """
        if not id:
            id = str(uuid1())
        network = {'physicalnetwork':physicalnetwork,'id':id}
        network.update(kwargs)

        for m in self.createlogicalnetworks([network]):
            yield m
    def createlogicalnetworks(self,networks):
        """
        Create multiple logical networks in a transaction.
        
        :param networks: a list of ``createlogicalnetwork`` parameters.
        
        :return: a list of dictionaries for the created logical networks.
        """
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
        """
        Update logical network attributes of the ID
        """
        # update phynetid is disabled

        if not id:
            raise ValueError("must special logicalnetwork id")

        network = {'id':id}
        network.update(kwargs)

        for m in self.updatelogicalnetworks([network]):
            yield m
    def updatelogicalnetworks(self,networks):
        """
        Update multiple logical networks in a transaction
        """
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

        with watch_context(flgnetworkkeys, flgnetworkvalues, reqid, self.app_routine):
            if None in flgnetworkvalues:
                raise ValueError ("logical net id " + LogicalNetwork._getIndices(flgnetworkkeys[flgnetworkvalues.index(None)])[1][0] + " not existed")

            lgnetworkdict = dict(zip(flgnetworkkeys,flgnetworkvalues))

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
                    for m in callAPI(self.app_routine,'public','updatelogicalnetworks',
                        {'phynettype':k,'networks':v.get('networks')},timeout=1):
                        yield m

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

                        typeretkeys,typeretvalues = v['updater'](keys[start:start+typekeylen],
                                values[start:start+typekeylen])

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
                else:
                    break

            dumpkeys = []
            for _,v in typenetwork.items():
                dumpkeys.extend(v.get("lgnetworkkeys"))

            for m in self._dumpkeys(dumpkeys):
                yield m

    def deletelogicalnetwork(self,id):
        """
        Delete logical network
        """
        if not id:
            raise ValueError("must special id")

        network = {'id':id}
        for m in self.deletelogicalnetworks([network]):
            yield m

    def deletelogicalnetworks(self,networks):
        """
        Delete logical networks
        
        :param networks: a list of ``{"id":id}``
        
        :return: ``{"status": "OK"}``
        """
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

        with watch_context(flgnetworkkeys, flgnetworkvalues, reqid, self.app_routine):
            if None in flgnetworkvalues:
                raise ValueError ("logical net id " + LogicalNetwork._getIndices(flgnetworkkeys[flgnetworkvalues.index(None)])[1][0] + " not existed")

            lgnetworkdict = dict(zip(flgnetworkkeys,flgnetworkvalues))

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

                    for m in callAPI(self.app_routine,'public','deletelogicalnetworks',
                        {'phynettype':k,'networks':v.get('networks')},timeout=1):
                        yield m

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
                        lognetmaps = values[start + len(v['lgnetworkkeys']) : start + len(v['lgnetworkkeys'])
                                                                              + len(v['lgnetworkmapkeys'])]
                        # Must check if there are some ref object existed
                        for lognetmap in lognetmaps:

                            if not lognetmap:
                                raise ValueError(" logical network mybe not existed %r" % (lognetmap.id,))

                            if lognetmap.ports.dataset():
                                raise ValueError('There are still ports in logical network %r' % (lognetmap.id,))

                            if lognetmap.subnets.dataset():
                                raise ValueError('There are still subnets in logical network %r' % (lognetmap.id,))


                        typekeylen = len(v['lgnetworkkeys']) + len(v['lgnetworkmapkeys']) + len(v['phynetmapkeys'])
                        objlen = len(v['lgnetworkkeys'])

                        if [n.physicalnetwork.getkey() if n is not None else None for n in values[start:start+objlen]] !=\
                                [n.physicalnetwork.getkey() for n in typesortvalues[index:index+objlen]]:
                            raise UpdateConflictException

                        typeretkeys,typeretvalues = v['updater'](keys[0:1]+keys[start:start+typekeylen],
                                [lgnetset]+values[start:start+typekeylen])

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
                else:
                    break

            self.app_routine.retvalue = {"status":'OK'}

    def listlogicalnetworks(self,id = None,physicalnetwork = None,**kwargs):
        """
        Query logical network information
        
        :param id: If specified, only return the logical network with the specified ID.
        
        :param physicalnetwork: If specified, only return logical networks in this physical network.
        
        :param \*\*kwargs: customized filters, only return a logical network if
                           the attribute value of this logical network matches
                           the specified value.
        
        :return: A list of dictionaries each stands for a matched logical network
        """

        if id:
            # special id ,, find it,, filter it
            lgnetkey = LogicalNetwork.default_key(id)

            for m in self._dumpone(lgnetkey,kwargs):
                yield m

        else:
            if physicalnetwork:
                # special physicalnetwork , find it in physicalnetwork,  filter it
                self._reqid += 1
                reqid = ('viperflow',self._reqid)

                physicalnetwork_map_key = PhysicalNetworkMap.default_key(physicalnetwork)

                def walk_map(key,value,walk,save):
                    if value is None:
                        return

                    for weakobj in value.logicnetworks.dataset():
                        lgnet_key = weakobj.getkey()

                        try:
                            lgnet_obj = walk(lgnet_key)
                        except KeyError:
                            pass
                        else:
                            if all(getattr(lgnet_obj,k,None) == v for k ,v in kwargs.items()):
                                save(lgnet_key)

                for m in callAPI(self.app_routine,'objectdb','walk',{'keys':[physicalnetwork_map_key],
                                        'walkerdict':{physicalnetwork_map_key:walk_map},
                                        'requestid':reqid}):
                    yield m

                keys,values = self.app_routine.retvalue

                with watch_context(keys,values,reqid,self.app_routine):
                    self.app_routine.retvalue = [dump(r) for r in values]
            else:
                # find it in all set , filter it
                self._reqid += 1
                reqid = ('viperflow',self._reqid)
                lgnet_set_key = LogicalNetworkSet.default_key()

                def walk_set(key,value,walk,save):
                    if value is None:
                        return

                    for weakobj in value.set.dataset():
                        lgnet_key = weakobj.getkey()

                        try:
                            lgnet_obj = walk(lgnet_key)
                        except KeyError:
                            pass
                        else:
                            if all(getattr(lgnet_obj,k,None) == v for k,v in kwargs.items()):
                                save(lgnet_key)

                for m in callAPI(self.app_routine,"objectdb","walk",{'keys':[lgnet_set_key],
                                    "walkerdict":{lgnet_set_key:walk_set},"requestid":reqid}):
                    yield m

                keys,values = self.app_routine.retvalue

                with watch_context(keys,values,reqid,self.app_routine):
                    self.app_routine.retvalue = [dump(r) for r in values]

    def createlogicalport(self,logicalnetwork,id=None,subnet=None,**args):
        """
        Create logical port
        
        :param logicalnetwork: logical network containing this port
        
        :param id: logical port id. If omitted an UUID is created.
        
        :param subnet: subnet containing this port
        
        :param \*\*kwargs: customized options for creating logical ports.
                           Common options are:
                           
                           mac_address
                              port MAC address
                           
                           ip_address
                              port IP address
        
        :return: a dictionary for the logical port
        """
        if not id:
            id = str(uuid1())
        
        if subnet:
            port = {'logicalnetwork':logicalnetwork,'id':id,'subnet':subnet}
        else:
            port = {'logicalnetwork':logicalnetwork,'id':id}
        
        port.update(args)

        for m in self.createlogicalports([port]):
            yield m

    def createlogicalports(self,ports):
        """
        Create multiple logical ports in a transaction
        """
        idset = set()
        newports = []
        subnetids = []
        for port in ports:
            port = copy.deepcopy(port)
            if 'id' in port:
                if port['id'] not in idset:
                    idset.add(port['id'])
                else:
                    raise ValueError("id repeat "+ id)
            else:
                port.setdefault('id',str(uuid1()))

            if 'mac_address' in port:
                try:
                    port['mac_address'] = mac_addr.formatter(mac_addr(port['mac_address']))
                except Exception:
                    raise ValueError(" invalid mac address " + port['mac_address'])
            if 'subnet' in port:
                subnetids.append(port['subnet'])

            newports.append(port)

        lgportsetkey = LogicalPortSet.default_key()
        lgportkeys = [LogicalPort.default_key(p['id']) for p in newports]
        lgports = [self._createlogicalports(**p) for p in newports]
        lgnetkeys = list(set([p.network.getkey() for p in lgports]))
        lgnetmapkeys = [LogicalNetworkMap.default_key(LogicalNetwork._getIndices(k)[1][0]) for k in lgnetkeys]
        subnetkeys = list(set([SubNet.default_key(id) for id in subnetids]))
        subnetmapkeys = [SubNetMap.default_key(SubNet._getIndices(key)[1][0]) for key in subnetkeys]

        keys = [lgportsetkey] + lgportkeys + lgnetkeys + lgnetmapkeys + subnetkeys + subnetmapkeys
        def updater(keys,values):
            netkeys = keys[1+len(lgportkeys):1+len(lgportkeys)+len(lgnetkeys)]
            netvalues = values[1+len(lgportkeys):1+len(lgportkeys)+len(lgnetkeys)]

            netmapkeys = keys[1+len(lgportkeys)+len(lgnetkeys):1+len(lgportkeys)+len(lgnetkeys)*2]
            netmapvalues = values[1+len(lgportkeys)+len(lgnetkeys):1+len(lgportkeys)+len(lgnetkeys)*2]

            snkeys = keys[1+len(lgportkeys)+len(lgnetkeys)*2:1+len(lgportkeys)+len(lgnetkeys)*2 + len(subnetkeys)]
            snobj =  values[1+len(lgportkeys)+len(lgnetkeys)*2:1+len(lgportkeys)+len(lgnetkeys)*2 + len(subnetkeys)]

            snmkeys = keys[1+len(lgportkeys)+len(lgnetkeys)*2 + len(subnetkeys):]
            snmobjs = values[1+len(lgportkeys)+len(lgnetkeys)*2 + len(subnetkeys):]
            subnetdict = dict(zip(snkeys,zip(snobj,snmobjs)))
            lgnetdict = dict(zip(netkeys,zip(netvalues,netmapvalues)))

            for i in range(0,len(newports)):

                values[1+i] = set_new(values[1+i],lgports[i])
                
                _,netmap = lgnetdict.get(values[1+i].network.getkey())

                if not netmap:
                    raise ValueError("lgnetworkkey not existed "+values[1+i].network.getkey())
                
                if hasattr(values[1+i],'subnet'):
                    sk = SubNet.default_key(values[1+i].subnet)
                    sn,smn = subnetdict.get(sk)
                    if not sn or not smn:
                        raise ValueError("special subnet " + values[1+i].subnet + " not existed")

                    if sn.create_weakreference() not in netmap.subnets.dataset():
                        raise ValueError("special subnet " + sn.id + " not in logicalnetwork " + smn.id)
                    else:
                        # we should allocated one ip_address for this lgport
                        if hasattr(values[1+i],'ip_address'):
                            try:
                                ip_address = parse_ip4_address(values[1+i].ip_address)
                            except:
                                raise ValueError("special ip_address" + values[1+i].ip_address + " invailed")
                            else:
                                # check ip_address in cidr
                                start = parse_ip4_address(sn.allocated_start)
                                end = parse_ip4_address(sn.allocated_end)
                                try:
                                    assert start <= ip_address <= end
                                    if hasattr(sn,'gateway'):
                                        assert ip_address != parse_ip4_address(sn.gateway)
                                except:
                                    raise ValueError("special ipaddress " + values[1+i].ip_address + " invaild")

                                if str(ip_address) not in smn.allocated_ips:
                                    smn.allocated_ips[str(ip_address)] = values[1+i].create_weakreference()
                                    #overlay subnet attr to subnet weakref
                                    setattr(values[1+i],'subnet',ReferenceObject(sn.getkey()))
                                else:
                                    raise ValueError("ipaddress " + values[1+i].ip_address + " have been used")
                        else:
                            # allocated ip_address from cidr
                            start = parse_ip4_address(sn.allocated_start)
                            end = parse_ip4_address(sn.allocated_end)
                            gateway = None
                            if hasattr(sn,"gateway"):
                                gateway = parse_ip4_address(sn.gateway)

                            for ip_address in range(start,end):
                                if str(ip_address) not in smn.allocated_ips and ip_address != gateway:
                                    setattr(values[1+i],'ip_address',ip4_addr.formatter(ip_address))
                                    smn.allocated_ips[str(ip_address)] = values[1+i].create_weakreference()
                                    #overlay subnet attr to subnet weakref
                                    setattr(values[1+i],'subnet',ReferenceObject(sn.getkey()))
                                    break
                            else:
                                raise ValueError("can not find avaliable ipaddress from pool")


                if netmap:
                    netmap.ports.dataset().add(values[1+i].create_weakreference())
                    values[0].set.dataset().add(values[1+i].create_weakreference())
                else:
                    raise ValueError("lgnetworkkey not existed "+lgports[i].network.getkey())

            return keys,values
        try:
            for m in callAPI(self.app_routine,'objectdb','transact',
                    {"keys":keys,'updater':updater}):
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
        "Update attributes of the specified logical port"
        if not id :
            raise ValueError("must special id")

        port = {"id":id}
        port.update(kwargs)

        for m in self.updatelogicalports([port]):
            yield m
    def updatelogicalports(self,ports):
        "Update multiple logcial ports"
        # ports [{"id":id,...},{...}]
        lgportkeys = set()
        updatesubnets = set()
        for port in ports:
            if 'id' in port:
                if port['id'] not in lgportkeys:
                    lgportkeys.add(port['id'])
                else:
                    raise ValueError("key repeat "+ port['id'])
            else:
                raise ValueError("must special id")

            if 'logicalnetwork' in port:
                raise ValueError("can not update logicalnetwork id")
            if 'subnet' in port:
                raise ValueError("can not update subnet id")

            if 'ip_address' in port:
                updatesubnets.add(port['id'])

        updatesubnetportkeys = [LogicalPort.default_key(key) for key in updatesubnets]
        for m in self._getkeys(updatesubnetportkeys):
            yield m
        updatesubnetportobjs = self.app_routine.retvalue

        if None in updatesubnetportobjs:
            raise ValueError(" key logicalport not existed " +updatesubnets[updatesubnetportobjs.index(None)])

        subnetmap = dict(zip(updatesubnets,[lgport.subnet.id for lgport in updatesubnetportobjs
                                            if hasattr(lgport,'subnet')]))
        subnetkeys = list(set([lgport.subnet.getkey() for lgport in updatesubnetportobjs 
                                            if hasattr(lgport,'subnet')]))
        subnetmapkeys = [SubNetMap.default_key(SubNet._getIndices(key)[1][0]) for key in subnetkeys]
        lgportkeys = [LogicalPort.default_key(key) for key in lgportkeys]
        def update(keys,values):

            portkeys = keys[0:len(lgportkeys)]
            portobj = values[0:len(lgportkeys)]
            lgportdict = dict(zip(portkeys,portobj))

            sk = keys[len(lgportkeys):len(lgportkeys) + len(subnetkeys)]
            skobj = values[len(lgportkeys):len(lgportkeys) + len(subnetkeys)]
            smk = keys[len(lgportkeys) + len(subnetkeys):]
            smkobj = values[len(lgportkeys) + len(subnetkeys):]
           
            subnetdict = dict(zip(sk,zip(skobj,smkobj)))
            for port in ports:
                lgport = lgportdict.get(LogicalPort.default_key(port["id"]))
                if not lgport:
                    raise ValueError("key object not existed "+ port['id'])
                if 'ip_address' in port:
                    if getattr(lgport,'ip_address',None):
                        subnetid = subnetmap.get(port['id'])
                        subnetobj,subnetmapobj = subnetdict.get(SubNet.default_key(subnetid))
                        if subnetobj and subnetmapobj:
                            del subnetmapobj.allocated_ips[str(parse_ip4_address(lgport.ip_address))]
                        else:
                            raise ValueError("special subnetid " + subnetid + " not existed")
            for port in ports:

                lgport = lgportdict.get(LogicalPort.default_key(port["id"]))

                if not lgport:
                    raise ValueError("key object not existed "+ port['id'])

                if 'ip_address' in port:
                    subnetid = subnetmap.get(port['id'])
                    subnetobj,subnetmapobj = subnetdict.get(SubNet.default_key(subnetid))
                    if subnetobj and subnetmapobj:
                        ip_address = parse_ip4_address(port['ip_address'])
                        start = parse_ip4_address(subnetobj.allocated_start)
                        end = parse_ip4_address(subnetobj.allocated_end)
                        try:
                            assert start <= ip_address <= end
                            if hasattr(subnetobj,"gateway"):
                                assert ip_address != parse_ip4_address(subnetobj.gateway)
                        except:
                            raise ValueError("special ipaddress " + port['ip_address'] + " invaild")

                        if str(ip_address) not in subnetmapobj.allocated_ips:
                            subnetmapobj.allocated_ips[str(ip_address)] = lgport.create_weakreference()
                        else:
                            raise ValueError("ipaddress " + port['ip_address'] + " have been used")
                    else:
                        raise ValueError("special subnetid " + subnetid + " not existed")

                if 'mac_address' in port:
                    # check and format mac address
                    try:
                        mac = mac_addr(port['mac_address'])
                    except:
                        raise ValueError("mac address invalid %r",port['mac_address'])
                    else:
                        # format
                        port['mac_address'] = mac_addr.formatter(mac)

                for k,v in port.items():
                    setattr(lgport,k,v)

            return keys,values

        try:
            for m in callAPI(self.app_routine,"objectdb","transact",
                    {"keys":lgportkeys + subnetkeys + subnetmapkeys,"updater":update}):
                yield m
        except:
            raise
        else:
            for m in self._dumpkeys(lgportkeys):
                yield m

    def deletelogicalport(self,id):
        "Delete logical port"
        if not id:
            raise ValueError("must special id")
        p = {"id":id}
        for m in self.deletelogicalports([p]):
            yield m
    def deletelogicalports(self,ports):
        "Delete multiple logical ports"
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

        with watch_context(lgportkeys, flgportvalues, reqid, self.app_routine):
            if None in flgportvalues:
                raise ValueError("logicalport is not existed "+\
                        LogicalPort._getIndices(lgportkeys[flgportvalues.index(None)])[1][0])

            lgportdict = dict(zip(lgportkeys,flgportvalues))

            while True:
                newports = []
                for port in ports:
                    port = copy.deepcopy(port)
                    key = LogicalPort.default_key(port["id"])
                    portobj = lgportdict[key]

                    # fake attr for delete
                    port['lgnetid'] = portobj.network.id

                    if hasattr(portobj,'subnet'):
                        #fake attr for delete
                        port['subnetid'] = portobj.subnet.id

                    newports.append(port)

                lgnetmapkeys = list(set([LogicalNetworkMap.default_key(p['lgnetid'])
                            for p in newports]))

                subnetmapkeys = list(set([SubNetMap.default_key(p['subnetid'])
                                 for p in newports if p.get('subnetid') ]))
                keys = [LogicalPortSet.default_key()] + lgportkeys + lgnetmapkeys + subnetmapkeys

                def update(keys,values):
                    lgportkeys = keys[1:1+len(ports)]
                    lgportvalues = values[1:1+len(ports)]

                    if [v.network.getkey() if v is not None else None for v in lgportvalues] !=\
                            [v.network.getkey() for v in flgportvalues]:
                        raise UpdateConflictException

                    lgmapkeys = keys[1+len(ports):1+len(ports)+len(lgnetmapkeys)]
                    lgmapvalues = values[1+len(ports):1 + len(ports) + len(lgnetmapkeys)]

                    snmapkeys = keys[1 + len(ports) + len(lgnetmapkeys):]
                    snmapvalues = values[1 + len(ports) + len(lgnetmapkeys):]
                    lgportdict = dict(zip(lgportkeys,lgportvalues))
                    lgnetmapdict = dict(zip(lgmapkeys,lgmapvalues))
                    subnetdict = dict(zip(snmapkeys,snmapvalues))

                    for port in newports:
                        lgport = lgportdict.get(LogicalPort.default_key(port.get("id")))
                        if 'subnetid' in port:
                            smk = SubNetMap.default_key(port['subnetid'])
                            smobj = subnetdict.get(smk)
                            if smobj:
                                del smobj.allocated_ips[str(parse_ip4_address(lgport.ip_address))]
                            else:
                                logging.warning("subnet obj" + smk + "not existed")
                        lgnetmap = lgnetmapdict.get(LogicalNetworkMap.default_key(port.get("lgnetid")))

                        lgnetmap.ports.dataset().discard(lgport.create_weakreference())

                        values[0].set.dataset().discard(lgport.create_weakreference())

                    return keys,[values[0]]+[None]*len(ports)+lgmapvalues+snmapvalues

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
                else:
                    break

            self.app_routine.retvalue = {"status":'OK'}

    def listlogicalports(self,id = None,logicalnetwork = None,**kwargs):
        """
        Query logical port
        
        :param id: If specified, returns only logical port with this ID.
        
        :param logicalnetwork: If specified, returns only logical ports in this network.
        
        :param \*\*kwargs: customzied filters
        
        :return: return matched logical ports
        """
        if id:
            # special id ,  find it ,, filter it
            lgportkey = LogicalPort.default_key(id)

            for m in self._dumpone(lgportkey,kwargs):
                yield m
        else:
            if logicalnetwork:
                # special logicalnetwork , find in logicalnetwork map , filter it
                lgnet_map_key = LogicalNetworkMap.default_key(logicalnetwork)

                self._reqid += 1
                reqid = ('viperflow',self._reqid)

                def walk_map(key,value,walk,save):
                    if value is None:
                        return

                    for weakobj in value.ports.dataset():
                        lgportkey = weakobj.getkey()
                        try:
                            lgport_obj = walk(lgportkey)
                        except KeyError:
                            pass
                        else:
                            if all(getattr(lgport_obj,k,None) == v for k,v in kwargs.items()):
                                save(lgportkey)

                for m in callAPI(self.app_routine,'objectdb','walk',{'keys':[lgnet_map_key],
                                    'walkerdict':{lgnet_map_key:walk_map},
                                    'requestid':reqid}):
                    yield m

                keys,values = self.app_routine.retvalue

                with watch_context(keys,values,reqid,self.app_routine):
                    self.app_routine.retvalue = [dump(r) for r in values]

            else:
                logport_set_key = LogicalPortSet.default_key()

                self._reqid += 1
                reqid = ('viperflow',self._reqid)

                def walk_set(key,value,walk,save):
                    if value is None:
                        return

                    for weakobj in value.set.dataset():
                        lgportkey = weakobj.getkey()
                        try:
                            lgport_obj = walk(lgportkey)
                        except KeyError:
                            pass
                        else:
                            if all(getattr(lgport_obj,k,None) == v for k,v in kwargs.items()):
                                save(lgportkey)

                for m in callAPI(self.app_routine,"objectdb","walk",{'keys':[logport_set_key],
                                                "walkerdict":{logport_set_key:walk_set},
                                                "requestid":reqid}):
                    yield m

                keys,values = self.app_routine.retvalue

                with watch_context(keys,values,reqid,self.app_routine):
                    self.app_routine.retvalue = [dump(r) for r in values]

    def createsubnet(self,logicalnetwork,cidr,id=None,**kwargs):
        """
        Create a subnet for the logical network.
        
        :param logicalnetwork: The logical network is subnet is in.
        
        :param cidr: CIDR of this subnet like ``"10.0.1.0/24"``
        
        :param id: subnet ID. If omitted, an UUID is generated.
        
        :param \*\*kwargs: customized creating options. Common options are:
                           
                           gateway
                              Gateway address for this subnet
                           
                           allocated_start
                              First IP of the allowed IP range.
                           
                           allocated_end
                              Last IP of the allowed IP range.
                           
                           host_routes
                              A list of ``[dest_cidr, via]`` like
                              ``[["192.168.1.0/24", "192.168.2.3"],["192.168.3.0/24","192.168.2.4"]]``.
                              This creates static routes on the subnet.
        
        :return: A dictionary of information of the subnet.
        """
        if not id:
            id = str(uuid1())
        subnet = {'id':id,'logicalnetwork':logicalnetwork,'cidr':cidr}
        subnet.update(kwargs)

        for m in self.createsubnets([subnet]):
            yield m

    def createsubnets(self,subnets):
        """
        Create multiple subnets in a transaction.
        """
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
                        gateway = parse_ip4_address(subnet['gateway'])
                        if not ip_in_network(gateway,cidr,prefix):
                            raise ValueError(" gateway ip not in cidr")
                            # format ipaddr to same in value store
                        subnet['gateway'] = ip4_addr.formatter(gateway)

                    if "allocated_start" not in subnet:
                        if "allocated_end" not in subnet:
                            start = network_first(cidr,prefix)
                            end = network_last(cidr,prefix)

                            subnet['allocated_start'] = ip4_addr.formatter(start)
                            subnet['allocated_end'] = ip4_addr.formatter(end)
                        else:
                            start = network_first(cidr,prefix)
                            end = parse_ip4_address(subnet['allocated_end'])

                            if start >= end:
                                raise ValueError(" allocated ip pool is None")

                            subnet['allocated_start'] = ip4_addr.formatter(start)
                            subnet['allocated_end'] = ip4_addr.formatter(end)
                    else:
                        if "allocated_end" not in subnet:
                            start = parse_ip4_address(subnet['allocated_start'])
                            end = network_last(cidr,prefix)

                            if start >= end:
                                raise ValueError(" allocated ip pool is None")

                            subnet['allocated_start'] = ip4_addr.formatter(start)
                            subnet['allocated_end'] = ip4_addr.formatter(end)
                        else:
                            start = parse_ip4_address(subnet['allocated_start'])
                            end = parse_ip4_address(subnet['allocated_end'])

                            if start >= end:
                                raise ValueError(" allocated ip pool is None")

                            subnet['allocated_start'] = ip4_addr.formatter(start)
                            subnet['allocated_end'] = ip4_addr.formatter(end)
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
        """
        Update subnet attributes
        """
        if not id:
            raise ValueError("must special subnet id when updatesubnet")
        subnet = {"id":id}
        subnet.update(kwargs)

        for m in self.updatesubnets([subnet]):
            yield m

    def updatesubnets(self,subnets):
        """
        Update multiple subnets
        """
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
                    except Exception:
                        raise ValueError('invalid gateway ' + sn['gateway'])

                if 'allocated_start' in sn:
                    try:
                        parse_ip4_address(sn['allocated_start'])
                    except Exception:
                        raise ValueError('invalid allocated start ' + sn['allocated_start'])

                if 'allocated_end' in sn:
                    try:
                        parse_ip4_address(sn['allocated_end'])
                    except Exception:
                        raise ValueError('invalid allocated end ' + sn['allocated_end'])

                for k,v in sn.items():
                    setattr(snet,k,v)

                try:
                    check_ip_pool(gateway=getattr(snet,'gateway',None),start=snet.allocated_start,end=snet.allocated_end,
                                  allocated=smap.allocated_ips.keys(),cidr=snet.cidr)
                except Exception:
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
        """
        Delete subnet
        """
        if not id:
            raise ValueError("must special id")

        subnet = {"id":id}

        for m in self.deletesubnets([subnet]):
            yield m

    def deletesubnets(self,subnets):
        """
        Delete multiple subnets
        """
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

        with watch_context(subnetkeys, fsubnetobjs, reqid, self.app_routine):
            if None in fsubnetobjs:
                raise ValueError(" subnet not existed " + SubNet._getIndices(subnetkeys[fsubnetobjs.index(None)])[1][0])

            subnetdict = dict(zip(subnetkeys,fsubnetobjs))

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

                        if hasattr(snobj,"router"):
                            raise ValueError("there router interface use subnet " + k + " delete it before")

                        _,lgnetmap = lgnetdict.get(nk)
                        lgnetmap.subnets.dataset().discard(snobj.create_weakreference())
                        subset.set.dataset().discard(snobj.create_weakreference())

                    return tuple(itertools.chain([keys[0]],sk,smk,lgnetmapkeys)),\
                            tuple(itertools.chain([subset],[None]*subnetlen,[None]*subnetlen,lgnetmapobjs))

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
                else:
                    break
            self.app_routine.retvalue = {"status":'OK'}

    def listsubnets(self,id = None,logicalnetwork=None,**kwargs):
        """
        Query subnets
        
        :param id: if specified, only return subnet with this ID
        
        :param logicalnetwork: if specified, only return subnet in the network
        
        :param \*\*kwargs: customized filters
        
        :return: A list of dictionaries each stands for a matched subnet.
        """
        if id:
            # special id , find it ,, filter it
            subnet_key = SubNet.default_key(id)

            for m in self._dumpone(subnet_key,kwargs):
                yield m

        else:
            if logicalnetwork:
                # special logicalnetwork ,, find in logicalnetwork ,, filter it

                def walk_map(key,value,walk,save):
                    if value is None:
                        return

                    for weakobj in value.subnets.dataset():
                        subnet_key = weakobj.getkey()
                        try:
                            subnet_obj = walk(subnet_key)
                        except KeyError:
                            pass
                        else:
                            if all(getattr(subnet_obj,k,None) == v for k,v in kwargs.items()):
                                save(subnet_key)

                lgnetmap_key = LogicalNetworkMap.default_key(logicalnetwork)

                self._reqid += 1
                reqid = ("viperflow",self._reqid)

                for m in callAPI(self.app_routine,"objectdb","walk",{'keys':[lgnetmap_key],
                                        'walkerdict':{lgnetmap_key:walk_map},"requestid":reqid}):
                    yield m

                keys, values = self.app_routine.retvalue

                with watch_context(keys,values,reqid,self.app_routine):
                    self.app_routine.retvalue = [dump(r) for r in values]
            else:
                # find in all set ,, filter it
                subnet_set_key = SubNetSet.default_key()

                self._reqid += 1
                reqid = ('viperflow',self._reqid)

                def walk_set(key,value,walk,save):
                    if value is None:
                        return

                    for weakobj in value.set.dataset():
                        subnet_key = weakobj.getkey()

                        try:
                            subnet_obj = walk(subnet_key)
                        except KeyError:
                            pass
                        else:
                            if all(getattr(subnet_obj,k,None) == v for k,v in kwargs.items()):
                                save(subnet_key)

                for m in callAPI(self.app_routine,"objectdb","walk",{'keys':[subnet_set_key],
                                    'walkerdict':{subnet_set_key:walk_set},'requestid':reqid}):
                    yield m

                keys,values = self.app_routine.retvalue

                with watch_context(keys,values,reqid,self.app_routine):
                    self.app_routine.retvalue = [dump(r) for r in values]

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
        ngateway = parse_ip4_address(gateway)
        assert ip_in_network(nstart,ncidr,prefix)
        assert ip_in_network(nend,ncidr,prefix)
        assert nstart <= nend

        for ip in allocated:
            nip = parse_ip4_address(ip)
            assert ip != ngateway
            assert nstart <= nip <= nend
    else:
        assert ip_in_network(nstart,ncidr,prefix)
        assert ip_in_network(nend,ncidr,prefix)
        assert nstart <= nend

        for ip in allocated:
            nip = parse_ip4_address(ip)
            assert nstart <= nip <= nend



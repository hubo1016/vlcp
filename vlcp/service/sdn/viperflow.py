#!/usr/bin/python
#! --*-- utf-8 --*--

from vlcp.config import defaultconfig
from vlcp.server.module import Module,depend,call_api,api
from vlcp.event.runnable import RoutineContainer
from vlcp.utils.ethernet import ip4_addr,mac_addr
from vlcp.utils.dataobject import DataObjectSet,updater,\
            set_new,DataObjectUpdateEvent,watch_context,dump,ReferenceObject,\
    request_context
import vlcp.service.kvdb.objectdb as objectdb

from vlcp.utils.networkmodel import *
from vlcp.utils.netutils import ip_in_network, network_first, network_last,\
                        parse_ip4_address, parse_ip4_network,\
    format_network_cidr, format_ip_address, check_ip_address

from uuid import uuid1
import copy
import logging
import itertools
from vlcp.utils.exceptions import AsyncTransactionLockException, WalkKeyNotRetrieved,\
    APIRejectedException
from collections import OrderedDict
from contextlib import suppress
from vlcp.utils.walkerlib import ensure_keys
from pychecktype.checked import checked
from vlcp.utils.typelib import ip_address_type, cidr_type, autoint, mac_address_type,\
    cidr_nonstrict_type
from pychecktype import tuple_, NoMatch

logger = logging.getLogger('viperflow')

#logger.setLevel(logging.DEBUG)

class UpdateConflictException(Exception):
    def __init__(self,desc="db update conflict"):
        super(UpdateConflictException,self).__init__(desc)


def dispatch_walker(parameter_dict, walker_map, create,
                    get_type):
    def _walker(walk, write, timestamp):
        # Collect type of each item and group into dict
        type_group = {}
        all_collected = True
        for key, parameters in parameter_dict.items():
            try:
                value = walk(key)
            except WalkKeyNotRetrieved:
                all_collected = False
            else:
                if value is None:
                    if not create:
                        raise ValueError(key + " does not exist")
                else:
                    if create:
                        # Raise exception
                        set_new(value, value)
                try:
                    type_, physicalnetwork_id = get_type(key, value, walk, parameters=parameters)
                except WalkKeyNotRetrieved:
                    all_collected = False
                else:
                    if type_ in type_group:
                        type_group[type_][key] = (parameters, physicalnetwork_id)
                    else:
                        type_group[type_] = {key: (parameters, physicalnetwork_id)}
        if not all_collected:
            return
        # Check if there are any types that are not in walker_map
        if any(t not in walker_map for t in type_group):
            raise AsyncTransactionLockException(type_group)
        for t, d in type_group.items():
            walker_map[t](walk, write, timestamp, {k: v[0] for k, v in d.items()})
    return _walker


def create_physicalnetwork_gettype(k, v, walk, parameters):
    return parameters['type'], parameters['id']


def physicalnetwork_gettype(k, v, walk, parameters):
    return v.type, v.id


def create_physicalport_gettype(k, v, walk, parameters):
    phynet = walk(PhysicalNetwork.default_key(parameters['physicalnetwork']))
    if phynet is None:
        raise ValueError("Physical network " + parameters['physicalnetwork'] + ' not exists')
    return phynet.type, phynet.id


def create_physicalport_prekey(key, parameters):
    return (PhysicalNetwork.default_key(parameters['physicalnetwork']),)


def physicalport_gettype(k, v, walk, parameters):
    phynet = walk(v.physicalnetwork.getkey())
    return phynet.type, phynet.id
    

create_logicalnetwork_gettype = create_physicalport_gettype


create_logicalnetwork_prekey = create_physicalport_prekey


logicalnetwork_gettype = physicalport_gettype


def dispatch_async_walker(parameter_dict, create, get_type, publicapi_,
                          direct_get_type = False,
                          pre_keys = None):
    async def _asyncwalker(last_info, container):
        walker_map = {}
        keys = set(parameter_dict)
        if last_info is None and direct_get_type:
            type_group = {}
            for key, parameters in parameter_dict.items():
                type_, physicalnetwork_id = get_type(key, None, None, parameters=parameters)
                if type_ in type_group:
                    type_group[type_][key] = (parameters, physicalnetwork_id)
                else:
                    type_group[type_] = {key: (parameters, physicalnetwork_id)}
            last_info = type_group
        if last_info is not None:            
            for t, d in last_info.items():
                # Collect walker and keys from public API
                try:
                    walker, k_ = await call_api(container, 'public', publicapi_,
                                                {'type': t})
                except APIRejectedException:
                    raise ValueError("Physical network type %r is not supported, or the corresponding network plugin is not loaded" % (t,))
                for _, (parameters, phynet_id) in d.items():
                    # If it is not changed, this network is needed
                    # this reduces an extra retry
                    keys.add(PhysicalNetwork.default_key(phynet_id))
                if k_:
                    for _, (parameters, phynet_id) in d.items():
                        keys.update(k_(phynet_id, parameters))
                walker_map[t] = walker
        else:
            if pre_keys:
                for key, parameters in parameter_dict.items():
                    keys.update(pre_keys(key, parameters))
        return (tuple(keys), dispatch_walker(parameter_dict, walker_map, create, get_type))
    return _asyncwalker


@defaultconfig
@depend(objectdb.ObjectDB)
class ViperFlow(Module):
    """
    Standard network model for L2 SDN
    """
    def __init__(self,server):
        super(ViperFlow,self).__init__(server)
        self.app_routine = RoutineContainer(self.scheduler)
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

    async def _dumpkeys(self, keys, filter=None):
        self._reqid += 1
        reqid = ('viperflow', self._reqid)

        with request_context(reqid, self.app_routine):
            retobjs = await call_api(self.app_routine,'objectdb','mget',{'keys':keys,'requestid':reqid})
            if filter is None:
                return [dump(o) for o in retobjs]
            else:
                return [dump(o) for o in retobjs if o is not None and all(getattr(o, k, None) == v for k, v in filter.items())]

    async def _dumpone(self, key, filter):
        return await self._dumpkeys([key], filter)

    async def _getkeys(self,keys):
        self._reqid += 1
        reqid = ('viperflow', self._reqid)
        with request_context(reqid, self.app_routine):
            return await call_api(self.app_routine,'objectdb','mget',{'keys':keys,'requestid':reqid})
    
    async def createphysicalnetwork(self, type: str = 'vlan', id: (str, None) = None, **kwargs: {'?vnirange': [tuple_((int, int))],
                                                                                                 "?vlanrange": [tuple_((int, int))]}):
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

        return await self.createphysicalnetworks([network])
    
    @checked
    async def createphysicalnetworks(self,networks: [{"?id": str, "type": str,
                                                      '?vnirange': [tuple_((int, int))],
                                                      "?vlanrange": [tuple_((int, int))]}]):
        """
        Create multiple physical networks in a transaction.
        
        :param networks: each should be a dictionary contains all the parameters in ``createphysicalnetwork``
        
        :return: A list of dictionaries of information of the created physical networks.
        """
        #networks [{type='vlan' or 'vxlan',id = None or uuid1(),'vlanrange':[(100,200),(400,401)],kwargs}]
        parameter_dict = OrderedDict()
        # first check id is None, allocate for it
        # group by type, do it use type driver
        for network in networks:
            #
            # deepcopy every networks elements
            # case:[network]*N point to same object will auto create same id
            #
            network = copy.deepcopy(network)
            if 'id' not in network:
                network['id'] = str(uuid1())
            phynetkey = PhysicalNetwork.default_key(network['id'])
            if phynetkey in parameter_dict:
                raise ValueError("Repeated ID: " + network['id'])
            parameter_dict[phynetkey] = network
        await call_api(self.app_routine, 'objectdb', 'asyncwritewalk',
                        {"asyncwalker": dispatch_async_walker(parameter_dict, True, create_physicalnetwork_gettype,
                                                              'createphysicalnetwork',
                                                              direct_get_type=True),
                         "withtime": True})
        return await self._dumpkeys(parameter_dict)
    
    async def updatephysicalnetwork(self, id: str, **kwargs: {'?vnirange': [tuple_((int, int))],
                                                              "?vlanrange": [tuple_((int, int))]}):
        """
        Update physical network with the specified ID.
        
        :param id: physical network ID
        
        :param \*\*kwargs: attributes to be updated, usually the same attributes for creating.
        
        :return: A dictionary of information of the updated physical network.        
        """
        network = {"id":id}
        network.update(kwargs)

        return await self.updatephysicalnetworks([network])
    
    @checked
    async def updatephysicalnetworks(self,networks: [{"id": str,
                                                      '?vnirange': [tuple_((int, int))],
                                                      "?vlanrange": [tuple_((int, int))]}]):
        """
        Update multiple physical networks in a transaction
        
        :param networks: a list of dictionaries, each contains parameters of ``updatephysicalnetwork``
        
        :return: A list of dictionaries of information of the updated physical network.
        """

        # networks [{"id":phynetid,....}]

        parameter_dict = OrderedDict()
        for network in networks:
            if 'type' in network:
                raise ValueError("physical network type can't be changed")

            phynetkey = PhysicalNetwork.default_key(network['id'])
            if phynetkey in parameter_dict:
                raise ValueError("Repeated ID: "+network['id'])
            parameter_dict[phynetkey] = network

        await call_api(self.app_routine, 'objectdb', 'asyncwritewalk',
                        {"asyncwalker": dispatch_async_walker(parameter_dict, False, physicalnetwork_gettype,
                                                              'updatephysicalnetwork'),
                         "withtime": True})
        return await self._dumpkeys(parameter_dict)
    
    async def deletephysicalnetwork(self, id: str):
        """
        Delete physical network with specified ID
        
        :param id: Physical network ID
        
        :return: ``{"status": "OK"}``
        """
        network = {"id":id}
        return await self.deletephysicalnetworks([network])
    
    @checked
    async def deletephysicalnetworks(self,networks: [{"id": str}]):
        """
        Delete multiple physical networks with a transaction
        
        :param networks: a list of ``{"id": <id>}`` dictionaries.
        
        :return: ``{"status": "OK"}``
        """
        # networks [{"id":id},{"id":id}]
        parameter_dict = {}
        for network in networks:
            phynetkey = PhysicalNetwork.default_key(network['id'])
            if phynetkey in parameter_dict:
                raise ValueError("Repeated ID: "+network['id'])
            parameter_dict[phynetkey] = network

        await call_api(self.app_routine, 'objectdb', 'asyncwritewalk',
                {"asyncwalker": dispatch_async_walker(parameter_dict, False, physicalnetwork_gettype,
                                                      'deletephysicalnetwork'),
                 "withtime": True})
        return {"status":'OK'}

    async def listphysicalnetworks(self,id = None,**kwargs):
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
                with suppress(WalkKeyNotRetrieved):
                    networkobj = walk(networkkey)
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
            with request_context(reqid, self.app_routine):
                _, values = await call_api(self.app_routine,'objectdb','walk',{'keys':[physetkey],
                                                'walkerdict':{physetkey:walker_func(lambda x:x.set)},
                                                'requestid':reqid})
                return [dump(r) for r in values]
        else:
            # get that id phynet info
            phynetkey = PhysicalNetwork.default_key(id)
            return await self._dumpone(phynetkey,kwargs)
    
    async def createphysicalport(self,physicalnetwork: str, name: str, vhost: str='',
                                 systemid: str='%',
                                 bridge: str='%',
                                 **kwargs):
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

        return await self.createphysicalports([port])
    
    @checked
    async def createphysicalports(self,ports: [{"physicalnetwork": str,
                                                "name": str,
                                                "?vhost": str,
                                                "?systemid": str,
                                                "?bridge": str}]):
        """
        Create multiple physical ports in a transaction
        
        :param ports: A list of dictionaries, each contains all parameters for ``createphysicalport``
        
        :return: A list of dictionaries of information of the created physical ports
        """
        # ports [{'physicalnetwork':id,'name':eth0,'vhost':'',systemid:'%'},{.....}]

        parameter_dict = OrderedDict()

        for port in ports:
            port = copy.deepcopy(port)
            port.setdefault('vhost','')
            port.setdefault('systemid','%')
            port.setdefault('bridge','%')
            
            key = PhysicalPort.default_key(port['vhost'], port['systemid'], port['bridge'], port['name'])
            if key in parameter_dict:
                raise ValueError("Repeated key: " + '.'.join([port['vhost'],port['systemid'],port['bridge'],port['name']]))
            parameter_dict[key] = port
        
        await call_api(self.app_routine, 'objectdb', 'asyncwritewalk',
                        {"asyncwalker": dispatch_async_walker(parameter_dict, True, create_physicalport_gettype,
                                                              'createphysicalport',
                                                              pre_keys=create_physicalport_prekey),
                         "withtime": True})
        return await self._dumpkeys(parameter_dict)
    
    async def updatephysicalport(self, name: str, vhost : str = '',
                                       systemid: str = '%',
                                       bridge: str = '%',
                                       **args):
        """
        Update physical port
        
        :param name: Update physical port with this name.
        
        :param vhost: Update physical port with this vHost.
        
        :param systemid: Update physical port with this systemid.
        
        :param bridge: Update physical port with this bridge name.
        
        :param \*\*kwargs: Attributes to be updated
        
        :return: Updated result as a dictionary.
        """
        port = {'name':name,'vhost':vhost,'systemid':systemid,'bridge':bridge}
        port.update(args)

        return await self.updatephysicalports([port])
    
    @checked
    async def updatephysicalports(self, ports: [{"name": str,
                                                "?vhost": str,
                                                "?systemid": str,
                                                "?bridge": str}]):
        """
        Update multiple physical ports with a transaction
        
        :param ports: a list of ``updatephysicalport`` parameters
                
        :return: Updated result as a list of dictionaries.
        """
        # ports [{'name':eth0,'vhost':'',systemid:'%'},{.....}]
        parameter_dict = OrderedDict()
        for port in ports:
            if 'physicalnetwork' in port:
                raise ValueError("physical network cannot be changed")
            port = copy.deepcopy(port)
            port.setdefault("vhost","")
            port.setdefault("systemid","%")
            port.setdefault("bridge","%")
            portkey = PhysicalPort.default_key(port['vhost'],
                        port['systemid'], port['bridge'], port['name'])
            
            if portkey in parameter_dict:
                raise ValueError("Repeated key: " + '.'.join([port['vhost'],port['systemid'],port['bridge'],port['name']]))
            parameter_dict[portkey] = port
        await call_api(self.app_routine, 'objectdb', 'asyncwritewalk',
                        {"asyncwalker": dispatch_async_walker(parameter_dict, False, physicalport_gettype,
                                                              'updatephysicalport'),
                         "withtime": True})
        return await self._dumpkeys(parameter_dict)
    
    async def deletephysicalport(self, name: str, vhost : str = '',
                                       systemid: str = '%',
                                       bridge: str = '%'):
        """
        Delete a physical port
        
        :param name: physical port name.
        
        :param vhost: physical port vHost.
        
        :param systemid: physical port systemid.
        
        :param bridge: physcial port bridge.
        
        :return: ``{"status": "OK"}``
        """
        port = {'name':name,'vhost':vhost,'systemid':systemid,'bridge':bridge}
        return await self.deletephysicalports([port])

    @checked
    async def deletephysicalports(self, ports: [{"name": str,
                                                  "?vhost": str,
                                                  "?systemid": str,
                                                  "?bridge": str}]):
        """
        Delete multiple physical ports in a transaction
        
                Delete a physical port
        
        :param ports: a list of ``deletephysicalport`` parameters
        
        :return: ``{"status": "OK"}``
        """

        parameter_dict = {}
        for port in ports:
            port = copy.deepcopy(port)
            port.setdefault("vhost","")
            port.setdefault("systemid","%")
            port.setdefault("bridge","%")
            portkey = PhysicalPort.default_key(port['vhost'],
                        port['systemid'], port['bridge'], port['name'])
            
            if portkey in parameter_dict:
                raise ValueError("Repeated key: " + '.'.join([port['vhost'],port['systemid'],port['bridge'],port['name']]))
            parameter_dict[portkey] = port
        await call_api(self.app_routine, 'objectdb', 'asyncwritewalk',
                        {"asyncwalker": dispatch_async_walker(parameter_dict, False, physicalport_gettype,
                                                              'deletephysicalport'),
                         "withtime": True})
        return {"status":'OK'}

    async def listphysicalports(self,name = None,physicalnetwork = None,vhost='',
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
            return await self._dumpone(phyportkey,kwargs)
        else:
            if physicalnetwork:
                # specify physicalnetwork , find it in map , filter it ..
                physical_map_key = PhysicalNetworkMap.default_key(physicalnetwork)

                self._reqid += 1
                reqid = ('viperflow',self._reqid)
                with request_context(reqid, self.app_routine):
                    def walk_map(key,value,walk,save):
                        if value is None:
                            return
    
                        for weakobj in value.ports.dataset():
                            phyport_key = weakobj.getkey()
    
                            with suppress(WalkKeyNotRetrieved):
                                phyport_obj = walk(phyport_key)
                                if all(getattr(phyport_obj,k,None) == v for k ,v in kwargs.items()):
                                    save(phyport_key)
                    _, values = await call_api(self.app_routine, 'objectdb', 'walk',
                                               {"keys":[physical_map_key],
                                                "walkerdict":{physical_map_key:walk_map},
                                                "requestid":reqid})

                    return [dump(r) for r in values]

            else:
                # find it in all, , filter it ,,
                phyport_set_key = PhysicalPortSet.default_key()

                self._reqid += 1
                reqid = ('viperflow',self._reqid)
                with request_context(reqid, self.app_routine):
                    def walk_set(key,value,walk,save):
                        if value is None:
                            return
    
                        for weakobj in value.set.dataset():
                            phyport_key = weakobj.getkey()
    
                            with suppress(WalkKeyNotRetrieved):
                                phyport_obj = walk(phyport_key)
                                if all(getattr(phyport_obj,k,None) == v for k,v in kwargs.items()):
                                    save(phyport_key)
    
                    _, values = await call_api(self.app_routine,'objectdb','walk',{"keys":[phyport_set_key],
                                                                "walkerdict":{phyport_set_key:walk_set},
                                                                "requestid":reqid})
    
                    return [dump(r) for r in values]
    
    async def createlogicalnetwork(self, physicalnetwork: str,
                                         id: (str, None) = None,
                                         **kwargs: {"?vni": autoint,
                                                    "?vxlan": autoint,
                                                    "?mtu": autoint}):
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

        return await self.createlogicalnetworks([network])
    
    @checked
    async def createlogicalnetworks(self,networks: [{"physicalnetwork": str,
                                                     "?id": str,
                                                     "?vni": autoint,
                                                     "?vxlan": autoint,
                                                     "?mtu": autoint}]):
        """
        Create multiple logical networks in a transaction.
        
        :param networks: a list of ``createlogicalnetwork`` parameters.
        
        :return: a list of dictionaries for the created logical networks.
        """
        # networks [{'physicalnetwork':'id','id':'id' ...},{'physicalnetwork':'id',...}]

        parameter_dict = OrderedDict()

        for network in networks:
            network = copy.deepcopy(network)
            if 'id' not in network:
                network['id'] = str(uuid1())
            key = LogicalNetwork.default_key(network['id'])
            if key in parameter_dict:
                raise ValueError("Repeated ID: " + network['id'])
            parameter_dict[key] = network        
        await call_api(self.app_routine, 'objectdb', 'asyncwritewalk',
                        {"asyncwalker": dispatch_async_walker(parameter_dict, True, create_logicalnetwork_gettype,
                                                              'createlogicalnetwork',
                                                              pre_keys=create_logicalnetwork_prekey),
                         "withtime": True})
        return await self._dumpkeys(parameter_dict)
    
    async def updatelogicalnetwork(self, id: str, **kwargs: {"?vni": autoint,
                                                            "?vxlan": autoint,
                                                            "?mtu": autoint}):
        """
        Update logical network attributes of the ID
        """
        # update phynetid is disabled
        network = {'id':id}
        network.update(kwargs)

        return await self.updatelogicalnetworks([network])
    
    @checked
    async def updatelogicalnetworks(self,networks: [{"id": str,
                                                     "?vni": autoint,
                                                     "?vxlan": autoint,
                                                     "?mtu": autoint}]):
        """
        Update multiple logical networks in a transaction
        """
        #networks [{'id':id,....},{'id':id,....}]

        parameter_dict = OrderedDict()

        for network in networks:
            if 'physicalnetwork' in network:
                raise ValueError("physical network cannot be changed")
            key = LogicalNetwork.default_key(network['id'])
            if key in parameter_dict:
                raise ValueError("Repeated ID: " + network['id'])
            parameter_dict[key] = network
        await call_api(self.app_routine, 'objectdb', 'asyncwritewalk',
                        {"asyncwalker": dispatch_async_walker(parameter_dict, False, logicalnetwork_gettype,
                                                              'updatelogicalnetwork'),
                         "withtime": True})
        return await self._dumpkeys(parameter_dict)
    
    async def deletelogicalnetwork(self, id: str):
        """
        Delete logical network
        """
        network = {'id':id}
        return await self.deletelogicalnetworks([network])
    
    @checked
    async def deletelogicalnetworks(self, networks: [{"id": str}]):
        """
        Delete logical networks
        
        :param networks: a list of ``{"id":id}``
        
        :return: ``{"status": "OK"}``
        """
        # networks [{"id":id},{"id":id}]
        parameter_dict = OrderedDict()

        for network in networks:
            key = LogicalNetwork.default_key(network['id'])
            if key in parameter_dict:
                raise ValueError("Repeated ID: " + network['id'])
            parameter_dict[key] = network
        await call_api(self.app_routine, 'objectdb', 'asyncwritewalk',
                        {"asyncwalker": dispatch_async_walker(parameter_dict, False, logicalnetwork_gettype,
                                                              'deletelogicalnetwork'),
                         "withtime": True})
        return {"status":'OK'}

    async def listlogicalnetworks(self,id = None,physicalnetwork = None,**kwargs):
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
            # specify id ,, find it,, filter it
            lgnetkey = LogicalNetwork.default_key(id)

            return await self._dumpone(lgnetkey,kwargs)

        else:
            if physicalnetwork:
                # specify physicalnetwork , find it in physicalnetwork,  filter it
                self._reqid += 1
                reqid = ('viperflow',self._reqid)

                physicalnetwork_map_key = PhysicalNetworkMap.default_key(physicalnetwork)

                def walk_map(key,value,walk,save):
                    if value is None:
                        return

                    for weakobj in value.logicnetworks.dataset():
                        lgnet_key = weakobj.getkey()

                        with suppress(WalkKeyNotRetrieved):
                            lgnet_obj = walk(lgnet_key)
                            if all(getattr(lgnet_obj,k,None) == v for k ,v in kwargs.items()):
                                save(lgnet_key)
                with request_context(reqid, self.app_routine):
                    _, values = await call_api(self.app_routine,'objectdb','walk',
                                                    {'keys':[physicalnetwork_map_key],
                                                    'walkerdict':{physicalnetwork_map_key:walk_map},
                                                    'requestid':reqid})
                    return [dump(r) for r in values]
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

                        with suppress(WalkKeyNotRetrieved):
                            lgnet_obj = walk(lgnet_key)
                            if all(getattr(lgnet_obj,k,None) == v for k,v in kwargs.items()):
                                save(lgnet_key)
                with request_context(reqid, self.app_routine):
                    _, values = await call_api(self.app_routine,"objectdb","walk",
                                                    {'keys':[lgnet_set_key],
                                                     "walkerdict":{lgnet_set_key:walk_set},"requestid":reqid})
                    return [dump(r) for r in values]
    
    async def createlogicalport(self, logicalnetwork: str,
                                      id: (str, None) = None,
                                      subnet: (str, None) = None,
                                      **kwargs: {"?mac_address": mac_address_type,
                                                 "?ip_address": ip_address_type}):
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
        
        port.update(kwargs)

        return await self.createlogicalports([port])
    
    @checked
    async def createlogicalports(self, ports: [{"?id": str,
                                                "logicalnetwork": str,
                                                "?subnet": str,
                                                "?mac_address": mac_address_type,
                                                "?ip_address": ip_address_type}]):
        """
        Create multiple logical ports in a transaction
        """
        parameter_dict = OrderedDict()
        keys = set()
        for port in ports:
            port = copy.deepcopy(port)
            if 'id' not in port:
                port['id'] = str(uuid1())
            key = LogicalPort.default_key(port['id'])
            if key in parameter_dict:
                raise ValueError("Repeated ID: "+ port['id'])
            if 'logicalnetwork' not in port:
                raise ValueError("must specify logicalnetwork ID")
            keys.add(key)
            keys.add(LogicalNetwork.default_key(port['logicalnetwork']))
            keys.add(LogicalNetworkMap.default_key(port['logicalnetwork']))
            if 'subnet' in port:
                keys.add(SubNet.default_key(port['subnet']))
                keys.add(SubNetMap.default_key(port['subnet']))
            parameter_dict[key] = port
        keys.add(LogicalPortSet.default_key())
        def walker(walk, write):
            # Process logical ports with specified IP address first,
            # so the automatically allocated IPs do not conflict
            # with specified IPs
            for key, parameters in sorted(parameter_dict.items(),
                                          key=lambda x: 'ip_address' in x[1],
                                          reverse=True):
                with suppress(WalkKeyNotRetrieved):
                    value = walk(key)
                    value = set_new(value, LogicalPort.create_instance(parameters['id']))
                    with suppress(WalkKeyNotRetrieved):
                        lognet_id = parameters['logicalnetwork']
                        lognet = walk(LogicalNetwork.default_key(lognet_id))
                        if not lognet:
                            raise ValueError("Logical network " + lognet_id + " not exists")
                        value.network = lognet.create_reference()
                        logmap = walk(LogicalNetworkMap.default_key(lognet_id))
                        logmap.ports.dataset().add(value.create_weakreference())
                        write(logmap.getkey(), logmap)
                        if 'subnet' in parameters:
                            subnet_id = parameters['subnet']
                            subnet = walk(SubNet.default_key(subnet_id))
                            if not subnet:
                                raise ValueError("Subnet " + subnet_id + " not exists")
                            if subnet.create_weakreference() not in logmap.subnets.dataset():
                                raise ValueError("Specified subnet " + subnet_id + " is not in logical network " + lognet_id)
                            subnet_map = walk(SubNetMap.default_key(subnet_id))
                            value.subnet = subnet.create_reference()
                            if 'ip_address' in parameters:
                                ip_address = parse_ip4_address(parameters['ip_address'])
                                value.ip_address = ip4_addr.formatter(ip_address)
                                # check ip_address in cidr
                                start = parse_ip4_address(subnet.allocated_start)
                                end = parse_ip4_address(subnet.allocated_end)
                                try:
                                    assert start <= ip_address <= end
                                    if hasattr(subnet, 'gateway'):
                                        assert ip_address != parse_ip4_address(subnet.gateway)
                                except Exception:
                                    raise ValueError("Specified ip_address " + parameters['ip_address'] + " is not an usable IP address in subnet " + subnet_id)

                                if str(ip_address) not in subnet_map.allocated_ips:
                                    subnet_map.allocated_ips[str(ip_address)] = value.create_weakreference()
                                else:
                                    raise ValueError("IP address " + parameters['ip_address'] + " has been used in subnet " + subnet_id)
                            else:
                                # allocated ip_address from cidr
                                start = parse_ip4_address(subnet.allocated_start)
                                end = parse_ip4_address(subnet.allocated_end)
                                gateway = None
                                if hasattr(subnet, "gateway"):
                                    gateway = parse_ip4_address(subnet.gateway)
    
                                for ip_address in range(start, end + 1):
                                    if str(ip_address) not in subnet_map.allocated_ips and ip_address != gateway:
                                        value.ip_address = ip4_addr.formatter(ip_address)
                                        subnet_map.allocated_ips[str(ip_address)] = value.create_weakreference()
                                        break
                                else:
                                    raise ValueError("Cannot allocate an available IP address from subnet " + subnet_id)
                            write(subnet_map.getkey(), subnet_map)
                        # Process other parameters
                        for k,v in parameters.items():
                            if k not in ('id', 'logicalnetwork', 'subnet', 'ip_address'):
                                setattr(value, k, v)
                        write(key, value)
                    with suppress(WalkKeyNotRetrieved):
                        logport_set = walk(LogicalPortSet.default_key())
                        logport_set.set.dataset().add(value.create_weakreference())
                        write(logport_set.getkey(), logport_set)
        await call_api(self.app_routine, 'objectdb', 'writewalk', {'keys': keys, 'walker': walker})
        return await self._dumpkeys(parameter_dict)

    async def updatelogicalport(self, id: str, **kwargs: {"?mac_address": mac_address_type,
                                                          "?ip_address": ip_address_type}):
        "Update attributes of the specified logical port"
        if not id :
            raise ValueError("must specify id")

        port = {"id":id}
        port.update(kwargs)

        return await self.updatelogicalports([port])
    
    @checked
    async def updatelogicalports(self, ports: [{"id": str,
                                                "?mac_address": mac_address_type,
                                                "?ip_address": ip_address_type}]):
        "Update multiple logcial ports"
        # ports [{"id":id,...},{...}]
        
        parameter_dict = OrderedDict()
        for port in ports:
            port = copy.deepcopy(port)
            key = LogicalPort.default_key(port['id'])
            if key in parameter_dict:
                raise ValueError("Repeated ID: " + port['id'])
            if 'logicalnetwork' in port:
                raise ValueError("logical network cannot be changed")
            if 'network' in port:
                raise ValueError("logical network cannot be changed")
            if 'subnet' in port:
                raise ValueError("subnet cannot be changed")
            parameter_dict[key] = port

        def walker(walk, write):
            # Must deallocate all IP addresses before allocating new
            deallocated_all = True
            for key, parameters in parameter_dict.items():
                try:
                    value = walk(key)
                    if value is None:
                        raise ValueError("Logical port " + parameters['id'] + " not exists")
                    if 'ip_address' in parameters and hasattr(value, 'subnet') and hasattr(value, 'ip_address'):
                        # Subnet is needed when allocating IP address
                        ensure_keys(walk, value.subnet.getkey())
                        subnet_map = walk(SubNetMap._subnet.leftkey(value.subnet))
                        del subnet_map.allocated_ips[str(parse_ip4_address(value.ip_address))]
                        write(subnet_map.getkey(), subnet_map)
                except WalkKeyNotRetrieved:
                    deallocated_all = False
            if not deallocated_all:
                return
            # Update processing
            for key, parameters in parameter_dict.items():
                value = walk(key)
                if 'ip_address' in parameters and hasattr(value, 'subnet'):
                    with suppress(WalkKeyNotRetrieved):
                        ensure_keys(walk, value.subnet.getkey(),
                                          SubNetMap._subnet.leftkey(value.subnet))
                        try:
                            subnet = walk(value.subnet.getkey())
                        except WalkKeyNotRetrieved:
                            # Also retrieve subnet map to prevent another try
                            ensure_keys(walk, SubNetMap._subnet.leftkey(value.subnet))
                            raise
                        subnet_map = walk(SubNetMap._subnet.leftkey(value.subnet))
                        ip_address = parse_ip4_address(parameters['ip_address'])
                        start = parse_ip4_address(subnet.allocated_start)
                        end = parse_ip4_address(subnet.allocated_end)
                        try:
                            assert start <= ip_address <= end
                            if hasattr(subnet,"gateway"):
                                assert ip_address != parse_ip4_address(subnet.gateway)
                        except Exception:
                            raise ValueError("Specified ip_address " + parameters['ip_address'] + " is not an usable IP address in subnet " + subnet.id)

                        if str(ip_address) not in subnet_map.allocated_ips:
                            subnet_map.allocated_ips[str(ip_address)] = value.create_weakreference()
                            write(subnet_map.getkey(), subnet_map)
                        else:
                            raise ValueError("Cannot allocate an available IP address from subnet " + subnet.id)
                for k, v in parameters.items():
                    if k not in ('id',):
                        setattr(value, k, v)
                write(key, value)

        await call_api(self.app_routine, 'objectdb', 'writewalk', {'keys': tuple(parameter_dict.keys()),
                                                                   'walker': walker})
        return await self._dumpkeys(parameter_dict)

    async def deletelogicalport(self, id: str):
        "Delete logical port"
        p = {"id":id}
        return await self.deletelogicalports([p])
    
    @checked
    async def deletelogicalports(self, ports: [{"id": str}]):
        "Delete multiple logical ports"
        parameter_dict = OrderedDict()
        for port in ports:
            key = LogicalPort.default_key(port['id'])
            if key in parameter_dict:
                raise ValueError("Repeated ID: " + port['id'])
            parameter_dict[key] = port
        def walker(walk, write):
            for key, parameters in parameter_dict.items():
                with suppress(WalkKeyNotRetrieved):
                    value = walk(key)
                    if value is None:
                        raise ValueError("Logical port " + parameters['id'] + " not exists")
                    with suppress(WalkKeyNotRetrieved):
                        lognet_map = walk(LogicalNetworkMap._network.leftkey(value.network))
                        lognet_map.ports.dataset().discard(value.create_weakreference())
                        write(lognet_map.getkey(), lognet_map)
                    if hasattr(value, 'subnet'):
                        with suppress(WalkKeyNotRetrieved):
                            subnet_map = walk(SubNetMap._subnet.leftkey(value.subnet))
                            del subnet_map.allocated_ips[str(parse_ip4_address(value.ip_address))]
                            write(subnet_map.getkey(), subnet_map)
                    with suppress(WalkKeyNotRetrieved):
                        logport_set = walk(LogicalPortSet.default_key())
                        logport_set.set.dataset().discard(value.create_weakreference())
                        write(logport_set.getkey(), logport_set)
        await call_api(self.app_routine, 'objectdb', 'writewalk',{'keys': tuple(parameter_dict) + (LogicalPortSet.default_key(),),
                                                                  'walker': walker})
        return {"status":'OK'}

    async def listlogicalports(self,id = None,logicalnetwork = None,**kwargs):
        """
        Query logical port
        
        :param id: If specified, returns only logical port with this ID.
        
        :param logicalnetwork: If specified, returns only logical ports in this network.
        
        :param \*\*kwargs: customzied filters
        
        :return: return matched logical ports
        """
        if id:
            # specify id ,  find it ,, filter it
            lgportkey = LogicalPort.default_key(id)
            return await self._dumpone(lgportkey,kwargs)
        else:
            if logicalnetwork:
                # specify logicalnetwork , find in logicalnetwork map , filter it
                lgnet_map_key = LogicalNetworkMap.default_key(logicalnetwork)

                self._reqid += 1
                reqid = ('viperflow',self._reqid)
                def walk_map(key,value,walk,save):
                    if value is None:
                        return

                    for weakobj in value.ports.dataset():
                        lgportkey = weakobj.getkey()
                        with suppress(WalkKeyNotRetrieved):
                            lgport_obj = walk(lgportkey)
                            if all(getattr(lgport_obj,k,None) == v for k,v in kwargs.items()):
                                save(lgportkey)
                with request_context(reqid, self.app_routine):
                    _, values = await call_api(self.app_routine,'objectdb','walk',
                                               {'keys':[lgnet_map_key],
                                                'walkerdict':{lgnet_map_key:walk_map},
                                                'requestid':reqid})
                    return [dump(r) for r in values]

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
                with request_context(reqid, self.app_routine):
                    _, values = await call_api(self.app_routine,"objectdb","walk",{'keys':[logport_set_key],
                                                                "walkerdict":{logport_set_key:walk_set},
                                                                "requestid":reqid})
                    return [dump(r) for r in values]

    async def createsubnet(self, logicalnetwork: str,
                                 cidr: cidr_type,
                                 id: (str, None) = None,
                                 **kwargs: {"?gateway": ip_address_type,
                                            "?allocated_start": ip_address_type,
                                            "?allocated_end": ip_address_type,
                                            "?host_routes": [tuple_((cidr_nonstrict_type, ip_address_type))],
                                            "?isexternal": bool,
                                            "?pre_host_config": [{"?systemid": str,
                                                                  "?bridge": str,
                                                                  "?vhost": str,
                                                                  "?cidr": cidr_type,
                                                                  "?local_address": ip_address_type,
                                                                  "?gateway": ip_address_type}]}):
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

                           isexternal
                              This subnet can forward packet to external physical network

                           pre_host_config
                              A list of ``[{systemid, bridge, cidr, local_address, gateway, ...}]``
                              Per host configuration, will union with public info when used
        
        :return: A dictionary of information of the subnet.
        """
        if not id:
            id = str(uuid1())
        subnet = {'id':id,'logicalnetwork':logicalnetwork,'cidr':cidr}
        subnet.update(kwargs)

        return await self.createsubnets([subnet])
    
    @checked
    async def createsubnets(self, subnets: [{"?id": str,
                                             "logicalnetwork": str,
                                             "cidr": cidr_type,
                                             "?gateway": ip_address_type,
                                             "?allocated_start": ip_address_type,
                                             "?allocated_end": ip_address_type,
                                             "?host_routes": [tuple_((cidr_nonstrict_type, ip_address_type))],
                                             "?isexternal": bool,
                                             "?pre_host_config": [{"?systemid": str,
                                                                   "?bridge": str,
                                                                   "?vhost": str,
                                                                   "?cidr": cidr_type,
                                                                   "?local_address": ip_address_type,
                                                                   "?gateway": ip_address_type}]}]):
        """
        Create multiple subnets in a transaction.
        """
        parameter_dict = OrderedDict()
        for subnet in subnets:
            subnet = copy.deepcopy(subnet)
            if 'id' not in subnet:
                subnet['id'] = str(uuid1())
            key = SubNet.default_key(subnet['id'])
            if key in parameter_dict:
                raise ValueError("Repeated ID: " + subnet['id'])
            cidr, prefix = parse_ip4_network(subnet['cidr'])
            if 'gateway' in subnet:
                gateway = parse_ip4_address(subnet['gateway'])
                if not ip_in_network(gateway, cidr, prefix):
                    raise ValueError(" %s not in cidr %s" % (subnet['gateway'], subnet["cidr"]))
            if "allocated_start" not in subnet:
                start = network_first(cidr, prefix)
            else:
                start = parse_ip4_address(subnet['allocated_start'])
                if not ip_in_network(start, cidr, prefix):
                    raise ValueError("%s not in cidr %s" % (subnet['allocated_start'], subnet["cidr"]))
        
            if "allocated_end" not in subnet:
                end = network_last(cidr,prefix)
            else:
                end = parse_ip4_address(subnet['allocated_end'])
                if not ip_in_network(end, cidr, prefix):
                    raise ValueError("%s not in cidr %s" % (subnet['allocated_end'], subnet["cidr"]))
            if start > end:
                raise ValueError("IP pool is empty for subnet " + subnet['id'])
            subnet['allocated_start'] = ip4_addr.formatter(start)
            subnet['allocated_end'] = ip4_addr.formatter(end)
            if 'pre_host_config' in subnet:
                for d in subnet['pre_host_config']:
                    if 'systemid' not in d:
                        d['systemid'] = "%"
                    if 'bridge' not in d:
                        d['bridge'] = '%'
                    if 'vhost' not in d:
                        d['vhost'] = ''

                    if 'cidr' in d:
                        local_cidr, local_prefix = parse_ip4_network(d['cidr'])
                        local_cidr_str = d['cidr']
                    else:
                        local_cidr, local_prefix = cidr, prefix
                        local_cidr_str = subnet['cidr']
                    if 'local_address' in d:
                        local = parse_ip4_address(d['local_address'])
                        if not ip_in_network(local, local_cidr, local_prefix):
                            raise ValueError(" %s not in cidr %s" % (d['local_address'], local_cidr_str))
                    if 'gateway' in d:
                        gateway = parse_ip4_address(d['gateway'])
                        if not ip_in_network(gateway, local_cidr, local_prefix):
                            raise ValueError(" %s not in cidr %s" % (d['gateway'], local_cidr_str))
            parameter_dict[key] = subnet
        
        keys = set(parameter_dict)
        keys.update(SubNetMap.default_key(subnet['id']) for subnet in parameter_dict.values())
        keys.update(LogicalNetworkMap.default_key(subnet['logicalnetwork']) for subnet in parameter_dict.values())
        keys.add(SubNetSet.default_key())
        def walker(walk, write):
            for key, parameters in parameter_dict.items():
                with suppress(WalkKeyNotRetrieved):
                    value = walk(key)
                    value = set_new(value, SubNet.create_instance(parameters['id']))
                    subnet_map = SubNetMap.create_instance(parameters['id'])
                    with suppress(WalkKeyNotRetrieved):
                        network_map = walk(LogicalNetworkMap.default_key(parameters['logicalnetwork']))
                        if network_map is None:
                            raise ValueError("Logical network " + parameters['logicalnetwork'] + " not exists")
                        value.network = ReferenceObject(LogicalNetwork.default_key(parameters['logicalnetwork']))
                        network_map.subnets.dataset().add(value.create_weakreference())
                        write(network_map.getkey(), network_map)
                    for k,v in parameters.items():
                        if k not in ('id', 'logicalnetwork', 'network'):
                            setattr(value, k, v)
                    with suppress(WalkKeyNotRetrieved):
                        subnet_set = walk(SubNetSet.default_key())
                        subnet_set.set.dataset().add(value.create_weakreference())
                        write(subnet_set.getkey(), subnet_set)
                    write(key, value)
                    write(subnet_map.getkey(), subnet_map)
        await call_api(self.app_routine,'objectdb','writewalk',{"keys":tuple(keys),
                                                                'walker':walker})
        return await self._dumpkeys(parameter_dict)

    async def updatesubnet(self,id: str, **kwargs: {"?cidr": cidr_type,
                                                    "?gateway": ip_address_type,
                                                    "?allocated_start": ip_address_type,
                                                    "?allocated_end": ip_address_type,
                                                    "?host_routes": [tuple_((cidr_nonstrict_type, ip_address_type))],
                                                    "?isexternal": bool,
                                                    "?pre_host_config": [{"?systemid": str,
                                                                          "?bridge": str,
                                                                          "?vhost": str,
                                                                          "?cidr": cidr_type,
                                                                          "?local_address": ip_address_type,
                                                                          "?gateway": ip_address_type}]}):
        """
        Update subnet attributes
        """
        subnet = {"id":id}
        subnet.update(kwargs)
        return await self.updatesubnets([subnet])
    
    @checked
    async def updatesubnets(self, subnets: [{"id": str,
                                             "?cidr": cidr_type,
                                             "?gateway": ip_address_type,
                                             "?allocated_start": ip_address_type,
                                             "?allocated_end": ip_address_type,
                                             "?host_routes": [tuple_((cidr_nonstrict_type, ip_address_type))],
                                             "?pre_host_config": [{"?systemid": str,
                                                                   "?bridge": str,
                                                                   "?vhost": str,
                                                                   "?cidr": cidr_type,
                                                                   "?local_address": ip_address_type,
                                                                   "?gateway": ip_address_type}]}]):
        """
        Update multiple subnets
        """
        parameter_dict = OrderedDict()
        for subnet in subnets:
            subnet = copy.deepcopy(subnet)
            if 'logicalnetwork' in subnet:
                raise ValueError("logical network cannot be changed")
            if "isexternal" in subnet:
                raise ValueError("isexternal cannot be changed")
            key = SubNet.default_key(subnet['id'])
            if key in parameter_dict:
                raise ValueError("Repeated ID: " + subnet['id'])
            if 'pre_host_config' in subnet:
                for d in subnet['pre_host_config']:
                    if 'systemid' not in d:
                        d['systemid'] = "%"
                    if 'bridge' not in d:
                        d['bridge'] = '%'
                    if 'vhost' not in d:
                        d['vhost'] = ''
                    if 'cidr' in d:
                        d['cidr'] = format_network_cidr(d['cidr'], True)
                        local_cidr, local_prefix = parse_ip4_network(d['cidr'])
                        if 'local_address' in d:
                            local = parse_ip4_address(d['local_address'])
                            if not ip_in_network(local, local_cidr, local_prefix):
                                raise ValueError(" %s not in cidr %s" % (d['local_address'], d['cidr']))
                        if 'gateway' in d:
                            gateway = parse_ip4_address(d['gateway'])
                            if not ip_in_network(gateway, local_cidr, local_prefix):
                                raise ValueError(" %s not in cidr %s" % (d['gateway'], d['cidr']))
            parameter_dict[key] = subnet

        subnetkeys = [SubNet.default_key(sn['id']) for sn in subnets]
        subnetmapkeys = [SubNetMap.default_key(sn['id']) for sn in subnets]

        keys = itertools.chain(subnetkeys,subnetmapkeys)
        
        def walker(walk, write):
            for key, parameters in parameter_dict.items():
                with suppress(WalkKeyNotRetrieved):
                    value = walk(key)
                    if value is None:
                        raise ValueError("Subnet " + parameters['id'] + " not exists")
                    subnet_map = walk(SubNetMap._subnet.leftkey(key))
                    for k, v in parameters.items():
                        if k not in ('id', 'network', 'logicalnetwork'):
                            setattr(value, k, v)
                    # Check consistency
                    try:
                        check_ip_pool(gateway=getattr(value, 'gateway', None),
                                      start=value.allocated_start,
                                      end=value.allocated_end,
                                      allocated=subnet_map.allocated_ips.keys(),
                                      cidr=value.cidr)
                    except Exception:
                        raise ValueError("New configurations conflicts with the old configuration or allocations in subnet "\
                                        + parameters['id'])
                    # Check pre_host_config
                    if hasattr(value, 'pre_host_config'):
                        cidr, prefix = parse_ip4_network(value.cidr)
                        for d in value.pre_host_config:
                            if 'cidr' in d:
                                local_cidr, local_prefix = parse_ip4_network(d['cidr'])
                                local_cidr_str = d['cidr']
                            else:
                                local_cidr, local_prefix = cidr, prefix
                                local_cidr_str = value.cidr
                            if 'local_address' in d:
                                local = parse_ip4_address(d['local_address'])
                                if not ip_in_network(local, local_cidr, local_prefix):
                                    raise ValueError(" %s not in cidr %s" % (d['local_address'], local_cidr_str))
                            if 'gateway' in d:
                                gateway = parse_ip4_address(d['gateway'])
                                if not ip_in_network(gateway, local_cidr, local_prefix):
                                    raise ValueError(" %s not in cidr %s" % (d['gateway'], local_cidr_str))
                    write(key, value)
        await call_api(self.app_routine, 'objectdb', 'writewalk', {'keys':keys,'walker':walker})
        return await self._dumpkeys(parameter_dict)

    async def deletesubnet(self, id: str):
        """
        Delete subnet
        """
        subnet = {"id":id}

        return await self.deletesubnets([subnet])

    @checked
    async def deletesubnets(self, subnets: [{"id": str}]):
        """
        Delete multiple subnets
        """
        parameter_dict = OrderedDict()
        for subnet in subnets:
            key = SubNet.default_key(subnet['id'])
            if key in parameter_dict:
                raise ValueError("Repeated ID: " + subnet['id'])
            parameter_dict[key] = subnet

        subnetkeys = [SubNet.default_key(sn['id']) for sn in subnets]
        subnetmapkeys = [SubNetMap.default_key(sn['id']) for sn in subnets]

        keys = itertools.chain(subnetkeys,subnetmapkeys,[SubNetSet.default_key()])
        def walker(walk, write):
            for key, parameters in parameter_dict.items():
                with suppress(WalkKeyNotRetrieved):
                    value = walk(key)
                    if value is None:
                        raise ValueError("Subnet " + parameters['id'] + " not exists")
                    if hasattr(value, 'router'):
                        raise ValueError("Subnet " + parameters['id'] + " is in router " + value.router.getkey())
                    subnet_map = walk(SubNetMap._subnet.leftkey(key))
                    if subnet_map.allocated_ips:
                        raise ValueError("Subnet " + parameters['id'] + " has logical ports or router ports")
                    with suppress(WalkKeyNotRetrieved):
                        # Remove from logical network map
                        lognet_map = walk(LogicalNetworkMap._network.leftkey(value.network))
                        lognet_map.subnets.dataset().discard(value.create_weakreference())
                        write(lognet_map.getkey(), lognet_map)
                    with suppress(WalkKeyNotRetrieved):
                        subnet_set = walk(SubNetSet.default_key())
                        subnet_set.set.dataset().discard(value.create_weakreference())
                        write(subnet_set.getkey(), subnet_set)
                    write(key, None)
                    write(subnet_map.getkey(), None)
        await call_api(self.app_routine, "objectdb", "writewalk", {'keys': keys, 'walker': walker})
        return {"status":'OK'}

    async def listsubnets(self,id = None,logicalnetwork=None,**kwargs):
        """
        Query subnets
        
        :param id: if specified, only return subnet with this ID
        
        :param logicalnetwork: if specified, only return subnet in the network
        
        :param \*\*kwargs: customized filters
        
        :return: A list of dictionaries each stands for a matched subnet.
        """
        if id:
            # specify id , find it ,, filter it
            subnet_key = SubNet.default_key(id)

            return await self._dumpone(subnet_key,kwargs)

        else:
            if logicalnetwork:
                # specify logicalnetwork ,, find in logicalnetwork ,, filter it

                def walk_map(key,value,walk,save):
                    if value is None:
                        return

                    for weakobj in value.subnets.dataset():
                        subnet_key = weakobj.getkey()
                        with suppress(WalkKeyNotRetrieved):
                            subnet_obj = walk(subnet_key)
                            if all(getattr(subnet_obj,k,None) == v for k,v in kwargs.items()):
                                save(subnet_key)

                lgnetmap_key = LogicalNetworkMap.default_key(logicalnetwork)

                self._reqid += 1
                reqid = ("viperflow",self._reqid)
                with request_context(reqid, self.app_routine):
                    _, values = await call_api(self.app_routine,"objectdb","walk",
                                                {'keys':[lgnetmap_key],
                                                 'walkerdict':{lgnetmap_key:walk_map},
                                                 "requestid":reqid})
                    return [dump(r) for r in values]
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
                with request_context(reqid, self.app_routine):
                    _, values = await call_api(self.app_routine,"objectdb","walk",
                                              {'keys':[subnet_set_key],
                                               'walkerdict':{subnet_set_key:walk_set},
                                               'requestid':reqid})
                    return [dump(r) for r in values]

    # the first run as routine going
    async def load(self,container):

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
        await call_api(container,'objectdb','transact',
                       {'keys':initdataobjectkeys,'updater':init})

        # call so main routine will be run
        return await Module.load(self,container)


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


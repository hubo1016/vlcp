import logging
import copy

from uuid import uuid1
from vlcp.server.module import Module,depend,api,call_api
from vlcp.event.runnable import RoutineContainer
from vlcp.utils.networkmodel import *
from vlcp.utils.dataobject import watch_context,set_new,dump,ReferenceObject,\
    request_context, create_new
from vlcp.utils.ethernet import ip4_addr
from vlcp.utils.netutils import format_network_cidr,check_ip_address, parse_ip4_network, ip_in_network,\
    parse_ip4_address

import vlcp.service.kvdb.objectdb as objectdb
from vlcp.config.config import defaultconfig
from pychecktype.checked import checked
from pychecktype import tuple_
from vlcp.utils.typelib import ip_address_type, cidr_nonstrict_type
from collections import OrderedDict
from contextlib import suppress
from vlcp.utils.exceptions import WalkKeyNotRetrieved


@depend(objectdb.ObjectDB)
@defaultconfig
class VRouterApi(Module):
    """
    Standard network model for L3 SDN
    """
    def __init__(self,server):
        super(VRouterApi,self).__init__(server)

        self.app_routine = RoutineContainer(self.scheduler)
        self._reqid = 0

        self.createAPI(api(self.createvirtualrouter,self.app_routine),
                       api(self.createvirtualrouters,self.app_routine),
                       api(self.updatevirtualrouter,self.app_routine),
                       api(self.updatevirtualrouters,self.app_routine),
                       api(self.deletevirtualrouter,self.app_routine),
                       api(self.deletevirtualrouters,self.app_routine),
                       api(self.listvirtualrouters,self.app_routine),
                       api(self.addrouterinterface,self.app_routine),
                       api(self.addrouterinterfaces,self.app_routine),
                       api(self.removerouterinterface,self.app_routine),
                       api(self.removerouterinterfaces,self.app_routine),
                       api(self.listrouterinterfaces,self.app_routine))

    async def _dumpkeys(self, keys, filter=None):
        self._reqid += 1
        reqid = ('virtualrouter', self._reqid)

        with request_context(reqid, self.app_routine):
            retobjs = await call_api(self.app_routine,'objectdb','mget',{'keys':keys,'requestid':reqid})
            if filter is None:
                return [dump(o) for o in retobjs]
            else:
                return [dump(o) for o in retobjs if o is not None and all(getattr(o, k, None) == v for k, v in filter.items())]

    async def _dumpone(self, key, filter):
        return await self._dumpkeys([key], filter)

    async def createvirtualrouter(self, id: (str, None) = None,
                                        **kwargs: {"?routes": [tuple_((cidr_nonstrict_type, ip_address_type))]}):
        """
        Create a virtual router
        
        :param id: Virtual router id. If omitted, an UUID is generated.
        
        :param \*\*kwargs: extra attributes for creation.
        
        :return: A dictionary of information of the virtual router.
        """
        if not id:
            id = str(uuid1())

        router = {"id":id}
        router.update(kwargs)

        return await self.createvirtualrouters([router])
    
    @checked
    async def createvirtualrouters(self, routers: [{"?id": str,
                                                     "?routes": [tuple_((cidr_nonstrict_type, ip_address_type))]}]):
        """
        Create multiple virtual routers in a transaction
        """
        idset = set()
        newrouters = []
        for router in routers:
            router = copy.deepcopy(router)
            if "id" not in router:
                router["id"] = str(uuid1())
            else:
                if router["id"] in idset:
                    raise ValueError("Repeated ID: " + router['id'])
                else:
                    idset.add(router['id'])

            newrouters.append(router)
        
        routerkeys = [VRouter.default_key(r['id']) for r in newrouters]
        routersetkey = [VRouterSet.default_key()]

        routerobjects = [self._createvirtualrouter(**r) for r in newrouters ]

        def createrouterdb(keys,values):
            routerset = values[0]

            for i in range(0,len(routerkeys)):
                values[i+1] = set_new(values[i+1],routerobjects[i])
                routerset.set.dataset().add(routerobjects[i].create_weakreference())

            return keys,values
        await call_api(self.app_routine,"objectdb","transact",
                         {"keys":routersetkey+routerkeys,"updater":createrouterdb})
        return await self._dumpkeys(routerkeys)

    def _createvirtualrouter(self,id,**kwargs):

        router = VRouter.create_instance(id)

        # [(prefix, nexthop),(prefix, nexthop)]
        if 'routes' in kwargs:
            router.routes.extend(kwargs['routes'])

        for k,v in kwargs.items():
            if k != "routes":
                setattr(router,k,v)

        return router

    async def updatevirtualrouter(self, id: str, **kwargs: {"?routes": [tuple_((cidr_nonstrict_type, ip_address_type))]}):
        """
        Update virtual router
        """
        if not id:
            raise ValueError("must specify id")
        router = {"id":id}
        router.update(kwargs)
        return await self.updatevirtualrouters([router])
    
    @checked
    async def updatevirtualrouters(self, routers: [{"id": str,
                                                    "?routes": [tuple_((cidr_nonstrict_type, ip_address_type))]}]):
        "Update multiple virtual routers"
        idset = set()
        for router in routers:
            if 'id' not in router:
                raise ValueError("must specify id")
            else:
                if router['id'] in idset:
                    raise ValueError("Repeated ID: " + router['id'])
                else:
                    idset.add(router['id'])
        routerkeys = [VRouter.default_key(r['id']) for r in routers]

        def updaterouter(keys,values):

            for i in range(0,len(routers)):
                if values[i]:
                    for k,v in routers[i].items():
                        if k == 'routes':
                            values[i].routes[:] = copy.deepcopy(v)
                        else:
                            setattr(values[i], k, v)
                else:
                    raise ValueError("Virtual router not exists: " + routers[i]['id'])
            
            return keys,values
        await call_api(self.app_routine,'objectdb','transact',
                         {'keys':routerkeys,'updater':updaterouter})
        return await self._dumpkeys(routerkeys)

    async def deletevirtualrouter(self, id: str):
        "Delete virtual router"
        if not id:
            raise ValueError("must specify id")

        router = {"id":id}

        return await self.deletevirtualrouters([router])
    
    @checked
    async def deletevirtualrouters(self, routers: [{"id": str}]):
        "Delete multiple virtual routers"
        idset = set()
        for router in routers:
            if 'id' not in router:
                raise ValueError("must specify id")
            else:
                if router['id'] in idset:
                    raise ValueError("Repeated ID: " + router['id'])
                else:
                    idset.add(router['id'])

        routersetkey = [VRouterSet.default_key()]
        routerkeys = [VRouter.default_key(v['id']) for v in routers]

        def deletevrouter(keys,values):
            routerset = values[0]

            for i in range(0,len(routers)):
                if values[i+1].interfaces.dataset():
                    raise ValueError("there are still interface(s) in this router, delete them first")
                routerset.set.dataset().discard(values[i+1].create_weakreference())

            return keys,[routerset] + [None] * len(routers)
        await call_api(self.app_routine,"objectdb","transact",
                             {"keys":routersetkey + routerkeys,"updater":deletevrouter})
        return {"status":"OK"}

    async def listvirtualrouters(self, id: (str, None) = None, **kwargs):
        """
        Query virtual router
        
        :param id: if specified, only return virtual router with this ID
        
        :param \*\*kwargs: customized filter
        
        :return: a list of dictionaries each stands for a matched virtual router.
        """
        if id:
            return await self._dumpone(VRouter.default_key(id), kwargs)
        else:
            # we get all router from vrouterset index
            routersetkey = VRouterSet.default_key()

            self._reqid += 1
            reqid = ("virtualrouter",self._reqid)

            def set_walker(key,set,walk,save):

                for weakobj in set.dataset():
                    routerkey = weakobj.getkey()

                    try:
                        router = walk(routerkey)
                    except KeyError:
                        pass
                    else:
                        if all(getattr(router,k,None) == v for k,v in kwargs.items()):
                            save(routerkey)

            def walker_func(set_func):
                def walker(key,obj,walk,save):
                    
                    if obj is None:
                        return
                    
                    set_walker(key,set_func(obj),walk,save)

                return walker
            with request_context(reqid, self.app_routine):
                _, values = await call_api(self.app_routine,"objectdb","walk",
                                                 {"keys":[routersetkey],"walkerdict":{routersetkey:walker_func(lambda x:x.set)},
                                                  "requestid":reqid})
                return [dump(r) for r in values]

    async def addrouterinterface(self, router: str, subnet: str, id: (str, None) = None,
                                 **kwargs: {'?ip_address': ip_address_type}):
        """
        Connect virtual router to a subnet
        
        :param router: virtual router ID
        
        :param subnet: subnet ID
        
        :param id: router port ID
        
        :param \*\*kwargs: customized options
        
        :return: A dictionary of information of the created router port
        """
        if not id:
            id = str(uuid1())

        if not router:
            raise ValueError("must specify virtual router ID")

        if not subnet:
            raise ValueError("must specify subnet ID")

        interface = {"id":id,"router":router,"subnet":subnet}

        interface.update(kwargs)
        return await self.addrouterinterfaces([interface])
    
    @checked
    async def addrouterinterfaces(self, interfaces: [{"router": str,
                                                      "subnet": str,
                                                      "?id": str,
                                                      '?ip_address': ip_address_type}]):
        """
        Create multiple router interfaces
        """
        keys = set()
        parameter_dict = OrderedDict()
        for interface in interfaces:
            interface = copy.deepcopy(interface)
            if 'id' not in interface:
                interface['id'] = str(uuid1())
            key = RouterPort.default_key(interface['id'])
            if key in parameter_dict:
                raise ValueError("Repeated ID: "+interface['id'])
            parameter_dict[key] = interface
            keys.add(key)
            keys.add(VRouter.default_key(interface['router']))
            keys.add(SubNet.default_key(interface['subnet']))
            keys.add(SubNetMap.default_key(interface['subnet']))
        
        def walker(walk, write):
            for key, parameters in parameter_dict.items():
                with suppress(WalkKeyNotRetrieved):
                    value = walk(key)
                    value = create_new(RouterPort, value, parameters['id'])
                    subnet = walk(SubNet.default_key(parameters['subnet']))
                    if subnet is None:
                        raise ValueError("Subnet " + parameters['subnet'] + " not exists")
                    subnet_map = walk(SubNetMap.default_key(parameters['subnet']))
                    router = walk(VRouter.default_key(parameters['router']))
                    if router is None:
                        raise ValueError("Virtual router " + parameters['router'] + " not exists")
                    if hasattr(subnet, 'router'):
                        # normal subnet can only have one router
                        _, (rid,) = VRouter._getIndices(subnet.router.getkey())
                        raise ValueError("Subnet %r is already in virtual router %r", parameters['subnet'], rid)
                    if hasattr(subnet_map, 'routers'):
                        if router.create_weakreference() in subnet_map.routers.dataset():
                            raise ValueError("Subnet %r is already in virtual router %r", parameters['subnet'],
                                                                                          parameters['router'])
                    if 'ip_address' in parameters:
                        if getattr(subnet, 'isexternal', False):
                            raise ValueError("Cannot specify IP address when add external subnet to virtual router")
                        # Check IP address in CIDR
                        nip = parse_ip4_address(parameters['ip_address'])
                        cidr, prefix = parse_ip4_network(subnet.cidr)
                        if not ip_in_network(nip, cidr, prefix):
                            raise ValueError("IP address " + parameters['ip_address'] + " not in subnet CIDR")
                        # Check IP not allocated
                        if str(nip) in subnet_map.allocated_ips:
                            raise ValueError("IP address " + parameters['ip_address'] + " has been used")
                        else:
                            # Save to allocated map
                            subnet_map.allocated_ips[str(nip)] = (value.create_weakreference(), router.create_weakreference())
                            write(subnet_map.getkey(), subnet_map)
                    else:
                        # Use gateway
                        if not hasattr(subnet, 'gateway'):
                            raise ValueError("Subnet " + subnet.id + " does not have a gateway, IP address on router port must be specified explicitly")
                    if not hasattr(subnet_map, 'routers'):
                        subnet_map.routers = DataObjectSet()
                    subnet_map.routers.dataset().add(router.create_weakreference())
                    if not hasattr(subnet_map, 'routerports'):
                        subnet_map.routerports = {}
                    subnet_map.routerports[router.id] = value.create_weakreference()
                    write(subnet_map.getkey(), subnet_map)
                    if not getattr(subnet, 'isexternal', False):
                        subnet.router = value.create_weakreference()
                        write(subnet.getkey(), subnet)
                    # Save port to router
                    router.interfaces.dataset().add(value.create_weakreference())
                    write(router.getkey(), router)
                    value.router = router.create_reference()
                    value.subnet = subnet.create_reference()
                    for k, v in parameters.items():
                        if k not in ('router', 'subnet', 'id'):
                            setattr(value, k, v)
                    write(key, value)
        await call_api(self.app_routine,'objectdb','writewalk',
                       {"keys":keys, 'walker':walker})
        return await self._dumpkeys(parameter_dict)

    async def removerouterinterface(self, router: str, subnet: str):
        """
        Remote a subnet from the router
        
        :param router: virtual router ID
        
        :param subnet: subnet ID
        
        :return: ``{"status": "OK"}``
        """
        if not router:
            raise ValueError("must specify router id")

        if not subnet:
            raise ValueError("must specify subnet id")

        interface = {"router": router, "subnet": subnet}

        await self.removerouterinterfaces([interface])
    
    @checked
    async def removerouterinterfaces(self, interfaces: [{"router": str,
                                                   "subnet": str}]):
        """
        Remote multiple subnets from routers
        """
        for interface in interfaces:
            if 'router' not in interface:
                raise ValueError("must specify router ID")
            if "subnet" not in interface:
                raise ValueError("must specify subnet ID")
        
        keys = set()
        keys.update(VRouter.default_key(interface['router']) for interface in interfaces)
        keys.update(SubNet.default_key(interface['subnet']) for interface in interfaces)
        keys.update(SubNetMap.default_key(interface['subnet']) for interface in interfaces)
        
        def walker(walk, write):
            for interface in interfaces:
                with suppress(WalkKeyNotRetrieved):
                    router = walk(VRouter.default_key(interface['router']))
                    if router is None:
                        raise ValueError("Virtual router " + interface['router'] + " not exists")
                    subnet = walk(SubNet.default_key(interface['subnet']))
                    if subnet is None:
                        raise ValueError("Subnet " + interface['subnet'] + " not exists")
                    subnet_map = walk(SubNetMap.default_key(interface['subnet']))
                    if router.create_weakreference() not in subnet_map.routers.dataset():
                        raise ValueError("Subnet %r not in virtual router %r" % (subnet.id, router.id))
                    port = subnet_map.routerports.pop(router.id)
                    subnet_map.routers.dataset().discard(router.create_weakreference())
                    write(subnet_map.getkey(), subnet_map)
                    router.interfaces.dataset().discard(port)
                    write(router.getkey(), router)
                    if hasattr(subnet, 'router') and subnet.router == port:
                        delattr(subnet, 'router')
                        write(subnet.getkey(), subnet)
                    write(port.getkey(), None)

        await call_api(self.app_routine,"objectdb","writewalk",{"keys": keys,
                                                               "walker": walker})
        return {'status': 'OK'}

    async def listrouterinterfaces(self, id: str, **kwargs):
        """
        Query router ports from a virtual router
        
        :param id: virtual router ID
        
        :param \*\*kwargs: customized filters on router interfaces
        
        :return: a list of dictionaries each stands for a matched router interface
        """
        if not id:
            raise ValueError("must specify router id")

        routerkey = VRouter.default_key(id)

        self._reqid += 1
        reqid = ("virtualrouter", self._reqid)
        
        def set_walker(key, interfaces, walk, save):
            for weakobj in interfaces.dataset():
                vpkey = weakobj.getkey()
                try:
                    virtualport = walk(vpkey)
                except KeyError:
                    pass
                else:
                    if all(getattr(virtualport,k,None) == v for k,v in kwargs.items()):
                        save(vpkey)
        
        def walk_func(filter_func):
            def walker(key,obj,walk,save):
                if obj is None:
                    return
                set_walker(key,filter_func(obj),walk,save)
            return walker
        with request_context(reqid, self.app_routine):
            _, values = await call_api(self.app_routine,"objectdb","walk",
                                            {"keys":[routerkey],"walkerdict":{routerkey:walk_func(lambda x:x.interfaces)},
                                                "requestid":reqid})
            return [dump(r) for r in values]

    async def load(self,container):

        initkeys = [VRouterSet.default_key(),DVRouterForwardSet.default_key(),
                    DVRouterExternalAddressInfo.default_key()]

        def init(keys,values):
            
            if values[0] is None:
                values[0] = VRouterSet()

            if values[1] is None:
                values[1] = DVRouterForwardSet()

            if values[2] is None:
                values[2] = DVRouterExternalAddressInfo()
            
            return keys,values

        
        await call_api(container,"objectdb","transact",
                         {"keys":initkeys,"updater":init})
        
        await Module.load(self, container)

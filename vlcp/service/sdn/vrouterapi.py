import logging
import copy

from uuid import uuid1
from vlcp.server.module import Module,depend,api,callAPI
from vlcp.event.runnable import RoutineContainer
from vlcp.utils.networkmodel import *
from vlcp.utils.dataobject import watch_context,set_new,dump,ReferenceObject
from vlcp.utils.ethernet import ip4_addr
from vlcp.utils.netutils import format_network_cidr,check_ip_address, parse_ip4_network, ip_in_network

import vlcp.service.kvdb.objectdb as objectdb
from vlcp.config.config import defaultconfig

logger = logging.getLogger("vrouterapi")


@depend(objectdb.ObjectDB)
@defaultconfig
class VRouterApi(Module):
    """
    Standard network model for L3 SDN
    """
    def __init__(self,server):
        super(VRouterApi,self).__init__(server)

        self.app_routine = RoutineContainer(self.scheduler)
        self.app_routine.main = self.main
        self.routines.append(self.app_routine)
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
    def main(self):
        logger.info(" viperflow VRouterApi running --")
        
        if False:
            yield 
    def _dumpkeys(self,keys):
        self._reqid += 1
        reqid = ('virtualrouter',self._reqid)

        for m in callAPI(self.app_routine,'objectdb','mget',{'keys':keys,'requestid':reqid}):
            yield m

        retobjs = self.app_routine.retvalue

        with watch_context(keys,retobjs,reqid,self.app_routine):
            self.app_routine.retvalue = [dump(v) for v in retobjs]

    def _getkeys(self,keys):
        self._reqid += 1
        reqid = ('virtualrouter',self._reqid)
        for m in callAPI(self.app_routine,'objectdb','mget',{'keys':keys,'requestid':reqid}):
            yield m
        with watch_context(keys,self.app_routine.retvalue,reqid,self.app_routine):
            pass

    def createvirtualrouter(self,id=None,**kwargs):
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

        for m in self.createvirtualrouters([router]):
            yield m

    def createvirtualrouters(self,routers):
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
                    raise ValueError("id repeat " + router['id'])
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
        try:
            for m in callAPI(self.app_routine,"objectdb","transact",
                             {"keys":routersetkey+routerkeys,"updater":createrouterdb}):
                yield m
        except:
            raise
        else:
            for m in self._dumpkeys(routerkeys):
                yield m

    def _createvirtualrouter(self,id,**kwargs):

        router = VRouter.create_instance(id)

        # [(prefix, nexthop),(prefix, nexthop)]
        if 'routes' in kwargs:
            for e in kwargs['routes']:
                ip_prefix = e[0]
                nexthop = e[1]

                if ip_prefix and nexthop:
                    ip_prefix = format_network_cidr(ip_prefix)
                    nexthop = check_ip_address(nexthop)
                    router.routes.append((ip_prefix,nexthop))
                else:
                    raise ValueError(" routes format error " + e)

        for k,v in kwargs.items():
            if k != "routes":
                setattr(router,k,v)

        return router

    def updatevirtualrouter(self,id,**kwargs):
        """
        Update virtual router
        """
        if not id:
            raise ValueError(" must specify id")
        router = {"id":id}
        router.update(kwargs)

        for m in self.updatevirtualrouters([router]):
            yield m

    def updatevirtualrouters(self,routers):
        "Update multiple virtual routers"
        idset = set()
        for router in routers:
            if 'id' not in router:
                raise ValueError(" must specify id")
            else:
                if router['id'] in idset:
                    raise ValueError(" id repeat " + router['id'])
                else:
                    idset.add(router['id'])

            # if routers updating ,, check format
            if 'routes' in router:
                for r in router['routes']:
                    ip_prefix = r[0]
                    nexthop = r[1]

                    if ip_prefix and nexthop:
                        ip_prefix = parse_ip4_network(ip_prefix)
                        nexthop = check_ip_address(nexthop)
                    else:
                        raise ValueError("routes format error " + r)

        routerkeys = [VRouter.default_key(r['id']) for r in routers]

        def updaterouter(keys,values):

            for i in range(0,len(routers)):
                if values[i]:
                    for k,v in routers[i].items():

                        if k == 'routes':
                            # update routers accord new static routes
                            values[i].routes = []
                            for pn in v:
                                values[i].routes.append(pn)
                        else:
                            setattr(values[i],k,v)
                else:
                    raise ValueError("router object not exists " + routers[i]['id'])
            
            return keys,values
        try:
            for m in callAPI(self.app_routine,'objectdb','transact',
                             {'keys':routerkeys,'updater':updaterouter}):
                yield m
        except:
            raise
        else:

            for m in self._dumpkeys(routerkeys):
                yield m

    def deletevirtualrouter(self,id):
        "Delete virtual router"
        if not id:
            raise ValueError("must specify id")

        router = {"id":id}

        for m in self.deletevirtualrouters([router]):
            yield m

    def deletevirtualrouters(self,routers):
        "Delete multiple virtual routers"
        idset = set()
        for router in routers:
            if 'id' not in router:
                raise ValueError(" must specify id")
            else:
                if router['id'] in idset:
                    raise ValueError(" id repeat " + router['id'])
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
        try:
            for m in callAPI(self.app_routine,"objectdb","transact",
                             {"keys":routersetkey + routerkeys,"updater":deletevrouter}):
                yield m
        except:
            raise
        else:
            self.app_routine.retvalue = {"status":"OK"}

    def listvirtualrouters(self,id=None,**kwargs):
        """
        Query virtual router
        
        :param id: if specified, only return virtual router with this ID
        
        :param \*\*kwargs: customized filter
        
        :return: a list of dictionaries each stands for a matched virtual router.
        """
        if id:
            routerkey = [VRouter.default_key(id)]

            for m in self._getkeys(routerkey):
                yield m

            retobj = self.app_routine.retvalue

            if all(getattr(retobj,k,None) == v for k,v in kwargs.items()):
                self.app_routine.retvalue = dump(retobj)
            else:
                self.app_routine.retvalue = []
        else:
            # we get all router from vrouterset index
            routersetkey = VRouterSet.default_key()

            self._reqid = +1
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

            for m in callAPI(self.app_routine,"objectdb","walk",
                             {"keys":[routersetkey],"walkerdict":{routersetkey:walker_func(lambda x:x.set)},
                              "requestid":reqid}):
                yield m

            keys ,values = self.app_routine.retvalue

            with watch_context(keys,values,reqid,self.app_routine):
                self.app_routine.retvalue = [dump(r) for r in values]

    def addrouterinterface(self,router,subnet,id=None,**kwargs):
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
            raise ValueError(" must specify routerid")

        if not subnet:
            raise ValueError(" must specify subnetid")

        interface = {"id":id,"router":router,"subnet":subnet}

        interface.update(kwargs)
        for m in self.addrouterinterfaces([interface]):
            yield m

    def addrouterinterfaces(self,interfaces):
        """
        Create multiple router interfaces
        """
        idset = set()
        newinterfaces = []
        for interface in interfaces:
            interface = copy.deepcopy(interface)
            if 'id' in interface:
                if interface['id'] not in idset:
                    idset.add(interface['id'])
                else:
                    raise ValueError(" id repeat "+interface['id'])
            else:
                interface['id'] = str(uuid1())

            if 'router' not in interface:
                raise ValueError("must specify router id")

            if 'subnet' not in interface:
                raise ValueError("must specify subnet id")

            newinterfaces.append(interface)

        routerportkeys = [RouterPort.default_key(interface['id'])
                           for interface in newinterfaces]
        routerportobjects = [self._createrouterport(**interface) for interface in newinterfaces]

        routerkeys = list(set([routerport.router.getkey() for routerport in routerportobjects]))

        subnetkeys = list(set([routerport.subnet.getkey() for routerport in routerportobjects]))

        subnetmapkeys = [SubNetMap.default_key(SubNet._getIndices(k)[1][0]) for k in subnetkeys]
        keys = routerkeys + subnetkeys + subnetmapkeys + routerportkeys
        newrouterportdict = dict(zip(routerportkeys,routerportobjects))

        def addrouterinterface(keys,values):

            rkeys = keys[0:len(routerkeys)]
            robjs = values[0:len(routerkeys)]

            snkeys = keys[len(routerkeys):len(routerkeys) + len(subnetkeys)]
            snobjs = values[len(routerkeys):len(routerkeys) + len(subnetkeys)]

            snmkeys = keys[len(routerkeys) + len(subnetkeys):len(routerkeys) + len(subnetkeys) + len(subnetmapkeys)]
            snmobjs = values[len(routerkeys) + len(subnetkeys):len(routerkeys) + len(subnetkeys) + len(subnetmapkeys)]

            rpkeys = keys[len(routerkeys) + len(subnetkeys) + len(subnetmapkeys):]
            rpobjs = values[len(routerkeys) + len(subnetkeys) + len(subnetmapkeys):]

            rdict = dict(zip(rkeys,robjs))
            sndict = dict(zip(snkeys,zip(snobjs,snmobjs)))
            rpdict = dict(zip(rpkeys,rpobjs))
            
            for i,interface in enumerate(newinterfaces):
                routerport = interface['id']
                router = interface['router']
                subnet = interface['subnet']

                routerobj = rdict.get(VRouter.default_key(router))
                subnetobj,subnetmapobj = sndict.get(SubNet.default_key(subnet))
                newrouterport = newrouterportdict.get(RouterPort.default_key(routerport))
                routerport = rpdict.get(RouterPort.default_key(routerport))
                
                if routerobj and subnetobj and subnetmapobj:

                    # now subnet only have one router ,,so check it
                    if not hasattr(subnetobj,'router'):

                        # new router port special ip address , we check it in subnetmap
                        if hasattr(newrouterport,"ip_address"):
                            ipaddress = ip4_addr(newrouterport.ip_address)

                            n,p = parse_ip4_network(subnetobj.cidr)

                            if not ip_in_network(ipaddress,n,p):
                                raise ValueError(" special ip address not in subnet cidr")

                            if str(ipaddress) not in subnetmapobj.allocated_ips:
                                subnetmapobj.allocated_ips[str(ipaddress)] = newrouterport.create_weakreference()
                            else:
                                raise ValueError(" ip address have used in subnet "+ newrouterport.ip_address)
                        else:
                            # not have special ip address, special gateway to this only router port
                            # gateway in subnet existed be sure

                            #
                            # when this case , special subnet gateway as ip_address
                            # but we do not set ip_address attr ,  when subnet gateway change ,
                            # we do not need to change router port attr

                            #setattr(newrouterport,"ip_address",subnetobj.gateway)

                            # it may be subnet not have gateway, checkout it
                            if not hasattr(subnetobj,"gateway"):
                                raise ValueError(" interface not special ip_address and subnet has no gateway")

                        #routerport = set_new(routerport,newrouterport)
                        values[len(routerkeys) + len(subnetkeys) + len(subnetmapkeys)+i] = set_new(
                                    values[len(routerkeys) + len(subnetkeys) + len(subnetmapkeys) + i],
                                    routerportobjects[i]
                                )
                        subnetobj.router = newrouterport.create_weakreference()
                        routerobj.interfaces.dataset().add(newrouterport.create_weakreference())
                    else:
                        raise ValueError(" subnet " + subnet + " have router port " + subnetobj.router.getkey())
                else:
                    raise ValueError(" routerobj " + router + "or subnetobj " + subnet + " not existed ")

            return keys,values
        try:
            for m in callAPI(self.app_routine,'objectdb','transact',
                         {"keys":keys,"updater":addrouterinterface}):
                yield m
        except:
            raise
        else:
            for m in self._dumpkeys(routerportkeys):
                yield m

    def _createrouterport(self,id,router,subnet,**kwargs):
        routerport = RouterPort.create_instance(id)
        routerport.router = ReferenceObject(VRouter.default_key(router))
        routerport.subnet = ReferenceObject(SubNet.default_key(subnet))

        for k,v in kwargs.items():
            setattr(routerport,k,v)

        return routerport

    def removerouterinterface(self,router,subnet):
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

        interface = {"router":router,"subnet":subnet}

        for m in self.removerouterinterfaces([interface]):
            yield m

    def removerouterinterfaces(self,interfaces):
        """
        Remote multiple subnets from routers
        """

        # idset use to check repeat subnet!
        idset = set()
        delete_interfaces = []

        for interface in interfaces:
            interface = copy.deepcopy(interface)
            if "router" not in interface:
                raise ValueError(" must specify router=id")
            if "subnet" not in interface:
                raise ValueError(" must specify subnet=id")
            else:
                if interface["subnet"] in idset:
                    raise ValueError("repeated subnet id " + interface["subnet"])
                else:
                    idset.add(interface["subnet"])

            delete_interfaces.append(interface)

        subnetkeys = [SubNet.default_key(interface["subnet"]) for interface in delete_interfaces]
        subnetmapkeys = [SubNetMap.default_key(interface["subnet"]) for interface in delete_interfaces]
        for m in self._getkeys(subnetkeys):
            yield m

        subnetobjs = self.app_routine.retvalue

        if None in subnetobjs:
            raise ValueError(" subnet object not existed " +\
                             SubNet._getIndices(subnetkeys[subnetobjs.index(None)])[1][0])

        sndict = dict(zip(subnetkeys,subnetobjs))

        for interface in delete_interfaces:
            if SubNet.default_key(interface['subnet']) in sndict:
                snobj = sndict[SubNet.default_key(interface["subnet"])]
                if hasattr(snobj,"router"):
                    routerportid = RouterPort._getIndices(snobj.router.getkey())[1][0]
                    interface['routerport'] = routerportid
                else:
                    raise ValueError("subnet " + interface["subnet"] + " is not plugged to router" )


        subnetkeys = list(set(subnetkeys))
        
        routerportkeys = [s.router.getkey() for s in subnetobjs]
        
        routerportkeys = list(set(routerportkeys))
        
        routerkeys = [VRouter.default_key(interface["router"]) for interface in delete_interfaces]

        routerkeys = list(set(routerkeys))
        
        def delete_interface(keys,values):
            rkeys = keys[0:len(routerkeys)]
            robjs = values[0:len(routerkeys)]

            snkeys = keys[len(routerkeys):len(routerkeys) + len(subnetkeys)]
            snobjs = values[len(routerkeys):len(routerkeys) + len(subnetkeys)]

            snmkeys = keys[len(routerkeys) + len(subnetkeys):len(routerkeys) + len(subnetkeys) + len(subnetmapkeys)]
            snmobjs = values[len(routerkeys) + len(subnetkeys):len(routerkeys) + len(subnetkeys) + len(subnetmapkeys)]

            rpkeys = keys[-len(routerportkeys):]
            rpobjs = values[-len(routerportkeys):]

            rdict = dict(zip(rkeys,robjs))
            sndict = dict(zip(snkeys,snobjs))
            snmdict = dict(zip(snmkeys,snmobjs))
            rpdict = dict(zip(rpkeys,rpobjs))

            for interface in delete_interfaces:
                r = rdict.get(VRouter.default_key(interface["router"]),None)
                sn = sndict.get(SubNet.default_key(interface["subnet"]),None)
                snm = snmdict.get(SubNetMap.default_key(interface["subnet"]),None)
                rp = rpdict.get(RouterPort.default_key(interface["routerport"]),None)

                if r and sn and snm and rp:
                    # has not attr ip_address, means router port use subnet gateway as ip_address
                    if hasattr(rp,'ip_address'):
                        # it means have allocated ip address in subnetmap, delete it
                        ipaddress = ip4_addr(rp.ip_address)
                        del snm.allocated_ips[str(ipaddress)]

                    # one subnet only have one router , so del this attr
                    if hasattr(sn,'router'):
                        delattr(sn,'router')

                    if rp.create_weakreference() in r.interfaces.dataset():
                        r.interfaces.dataset().discard(rp.create_weakreference())
                    else:
                        raise ValueError("router " + interface["router"] + " have no router port " +
                                         interface["routerport"])
                else:
                    raise ValueError("route " + interface["router"] + " subnet " + interface["subnet"] +
                                     " routerport " + interface["routerport"] + " not existed!")

            return keys,robjs + snobjs + snmobjs + [None]*len(routerportkeys)

        transact_keys = routerkeys + subnetkeys + subnetmapkeys + routerportkeys

        for m in callAPI(self.app_routine,"objectdb","transact",{"keys":transact_keys,
                                                                 "updater":delete_interface}):
            yield m

        self.app_routine.retvalue = {'status': 'OK'}

    def listrouterinterfaces(self,id,**kwargs):
        """
        Query router ports from a virtual router
        
        :param id: virtual router ID
        
        :param \*\*kwargs: customized filters on router interfaces
        
        :return: a list of dictionaries each stands for a matched router interface
        """
        if not id:
            raise ValueError(" must special router id")

        routerkey = VRouter.default_key(id)

        self._reqid = +1
        reqid = ("virtualrouter",self._reqid)
        
        def set_walker(key,interfaces,walk,save):

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
        for m in callAPI(self.app_routine,"objectdb","walk",
                {"keys":[routerkey],"walkerdict":{routerkey:walk_func(lambda x:x.interfaces)},
                    "requestid":reqid}):
            yield m

        keys,values = self.app_routine.retvalue

        with watch_context(keys,values,reqid,self.app_routine):
            self.app_routine.retvalue = [dump(r) for r in values]

    def load(self,container):

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

        
        for m in callAPI(container,"objectdb","transact",
                         {"keys":initkeys,"updater":init}):
            yield m
        
        for m in Module.load(self,container):
            yield m



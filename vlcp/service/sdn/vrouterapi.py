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

logger = logging.getLogger("vrouterapi")


@depend(objectdb.ObjectDB)
class VRouterApi(Module):
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
        "create virtual router "
        if not id:
            id = str(uuid1())

        router = {"id":id}
        router.update(kwargs)

        for m in self.createvirtualrouters([router]):
            yield m

    def createvirtualrouters(self,routers):
        "create virutal routers"
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
        "update virtual router"
        if not id:
            raise ValueError(" must special id")
        router = {"id":id}
        router.update(kwargs)

        for m in self.updatevirtualrouters([router]):
            yield m

    def updatevirtualrouters(self,routers):
        "update virtual routers"
        idset = set()
        for router in routers:
            if 'id' not in router:
                raise ValueError(" must special id")
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
                    raise ValueError(" router object not existe " + routers[i]['id'])
            
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
        "delete virtual router"
        if not id:
            raise ValueError(" must special id")

        router = {"id":id}

        for m in self.deletevirtualrouters([router]):
            yield m

    def deletevirtualrouters(self,routers):
        "delete virtual routers"
        idset = set()
        for router in routers:
            if 'id' not in router:
                raise ValueError(" must special id")
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
                    raise ValueError("there interface in router, delete it first")
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
        "list virtual routers info"
        if id:
            routerkey = [VRouter.default_key()]

            for m in self._getkeys([routerkey]):
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
        "add interface into router"
        if not id:
            id = str(uuid1())

        if not router:
            raise ValueError(" must special routerid")

        if not subnet:
            raise ValueError(" must special subnetid")

        interface = {"id":id,"router":router,"subnet":subnet}

        interface.update(kwargs)
        for m in self.addrouterinterfaces([interface]):
            yield m

    def addrouterinterfaces(self,interfaces):
        "add interfaces into routers"
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
                raise ValueError(" must special router id")

            if 'subnet' not in interface:
                raise ValueError(" must special subnet id")

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
                        raise ValueError(" subnet " + subnet + " have router port " + subnetobj.router.id)
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

        #routerport.router = WeakReferenceObject(VRouter.default_key(router))
        #routerport.subnet = WeakReferenceObject(SubNet.default_key(subnet))

        for k,v in kwargs.items():
            setattr(routerport,k,v)

        return routerport

    def removerouterinterface(self,id):
        "remvoe interface from router"
        if not id:
            raise ValueError("must special interface id")

        interface = {"id":id}

        for m in self.removerouterinterfaces([interface]):
            yield m

    def removerouterinterfaces(self,interfaces):
        "remvoe interfaces from routers"
        idset = set()
        deleteinterfaces = list()

        for interface in interfaces:
            interface = copy.deepcopy(interface)
            if 'id' not in interface:
                raise ValueError(" must special id")
            else:
                if interface['id'] in idset:
                    raise ValueError(" id repeat " + interface['id'])
                else:
                    idset.add(interface['id'])
            deleteinterfaces.append(interface)

        routerportkeys = [RouterPort.default_key(interface['id']) for interface in deleteinterfaces]

        for m in self._getkeys(routerportkeys):
            yield m

        routerportobjs = self.app_routine.retvalue

        if None in routerportobjs:
            raise ValueError(" interface object not existed " +\
                             RouterPort._getIndices(routerportkeys[routerportobjs.index(None)])[1][0])

        routerportdict = dict(zip(routerportkeys,routerportobjs))

        for interface in deleteinterfaces:
            routerportobj = routerportdict[RouterPort.default_key(interface['id'])]
            interface['router'] = RouterPort._getIndices(routerportobj.router.getkey())[1][0]
            interface['subnet'] = SubNet._getIndices(routerportobj.subnet.getkey())[1][0]

        routerkeys = list(set([VRouter.default_key(interface['router']) for interface in deleteinterfaces]))

        # subnet only have one router (router port), so subnetkey is not repeat, set() it anyway
        subnetkeys = list(set(SubNet.default_key(interface['subnet']) for interface in deleteinterfaces))

        subnetmapkeys = [SubNetMap.default_key(SubNet._getIndices(key)[1][0]) for key in subnetkeys]
        keys = routerkeys + subnetkeys + subnetmapkeys + routerportkeys

        def removerouterinterface(keys,values):
            rkeys = keys[0:len(routerkeys)]
            robjs = values[0:len(routerkeys)]


            snkeys = keys[len(routerkeys):len(routerkeys) + len(subnetkeys)]
            snobjs = values[len(routerkeys):len(routerkeys) + len(subnetkeys)]

            snmkeys = keys[len(routerkeys) + len(subnetkeys):len(routerkeys) + len(subnetkeys) + len(subnetmapkeys)]
            snmobjs = values[len(routerkeys) + len(subnetkeys):len(routerkeys) + len(subnetmapkeys) + len(subnetmapkeys)]

            rportkeys = keys[len(routerkeys) + len(subnetkeys) + len(subnetmapkeys):]
            rportobjs = values[len(routerkeys) + len(subnetkeys) + len(subnetmapkeys):]

            # we get (routerkeys,routerobjs) on begin to get (router , subnet) attr
            # this is second get (routerkeys,routerobjs) , if (router,subnet) is different from last, Nothing can do!
            if [r.router.getkey() for r in routerportobjs] !=\
                    [r.router.getkey() if r is not None else None for r in rportobjs] and\
                [r.subnet.getkey() for r in routerportobjs] !=\
                    [r.subnet.getkey() if r is not None else None for r in  rportobjs]:
                raise ValueError(" conflict error, try again!")
 
            rdict = dict(zip(rkeys,robjs))
            sndict = dict(zip(snkeys,zip(snobjs,snmobjs)))

            rportdict = dict(zip(rportkeys,rportobjs))
            

            for interface in deleteinterfaces:
                routerobj = rdict[VRouter.default_key(interface['router'])]
                subnetobj,subnetmapobj = sndict[SubNet.default_key(interface['subnet'])]
                rportobj = rportdict[RouterPort.default_key(interface['id'])]

                if routerobj and subnetobj and subnetmapobj:

                    # has not attr ip_address, means router port use subnet gateway as ip_address
                    if hasattr(rportobj,'ip_address'):
                        # it means have allocated ip address in subnetmap, delete it
                        ipaddress = ip4_addr(rportobj.ip_address)
                        del subnetmapobj.allocated_ips[str(ipaddress)]

                    # one subnet only have one router , so del this attr
                    if hasattr(subnetobj,'router'):
                        delattr(subnetobj,'router')

                    routerobj.interfaces.dataset().discard(rportobj.create_weakreference())
                else:
                    raise ValueError(" interface router " + interface['router']\
                                     + "or subnet " + interface['subnet'] + "not existed")

            return keys,robjs + snobjs + snmobjs + [None] * len(routerportkeys)
        for m in callAPI(self.app_routine,'objectdb','transact',
                         {'keys':keys,'updater':removerouterinterface}):
            yield m

        self.app_routine.retvalue = {'status':'OK'}

    def listrouterinterfaces(self,id,**kwargs):
        "list interfaces info plugin in router"

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



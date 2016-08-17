import itertools

import vlcp.service.sdn.ioprocessing as iop

from vlcp.event import RoutineContainer
from vlcp.protocol.openflow import OpenflowConnectionStateEvent
from vlcp.service.sdn.flowbase import FlowBase
from vlcp.service.sdn.ofpmanager import FlowInitialize
from vlcp.utils.ethernet import mac_addr_bytes, ip4_addr_bytes, ip4_addr
from vlcp.utils.flowupdater import FlowUpdater
from vlcp.utils.netutils import parse_ip4_network, ip_in_network, get_netmask
from vlcp.utils.networkmodel import VRouter, RouterPort, SubNet


class RouterUpdater(FlowUpdater):

    def __init__(self, connection, parent):
        super(RouterUpdater, self).__init__(connection, (), ("routerupdater", connection), parent._logger)
        self._parent = parent
        self._lastlogicalport = dict()
        self._lastlogicalnet = dict()

        self._lastrouterinfo = dict()
        self._original_keys = ()
    def main(self):
        try:
            self.subroutine(self._update_handler(), True, "updater_handler")

            for m in FlowUpdater.main(self):
                yield m
        finally:

            if hasattr(self, "updater_handler"):
                self.updater_handler.close()

    def _update_handler(self):

        dataobjectchange = iop.DataObjectChanged.createMatcher(None, None, self._connection)

        yield (dataobjectchange,)

        self._lastlogicalport, _ , self._lastlogicalnet, _ = self.event.current

        self._update_walk()

    def _update_walk(self):

        logicalportkeys = [p.getkey() for p, _ in self._lastlogicalport]
        logicalnetkeys = [n.getkey() for n, _ in self._lastlogicalnet]

        self._initialkeys = logicalportkeys + logicalnetkeys
        self._original_keys = logicalportkeys + logicalnetkeys

        self._walkerdict = dict(itertools.chain(((p,self._walk_lgport) for p in logicalportkeys),
                                           ((n,self._walk_lgnet) for n in logicalnetkeys)))

        self.subroutine(self.restart_walk(),False)

    def _walk_lgport(self,key,value,walk,save):

        if value is None:
            return
        save(key)

        # lgport --> subnet --> routerport --> router --> routerport --->> subnet --->> logicalnet
        if hasattr(value, "subnet"):
            try:
                subnetobj = walk(value.subnet.getkey())
            except KeyError:
                pass
            else:
                save(subnetobj.getkey())

                if hasattr(subnetobj, "router"):
                    try:
                        routerport = walk(subnetobj.router.getkey())
                    except KeyError:
                        pass
                    else:
                        save(routerport.getkey())

                        if hasattr(routerport, "router"):
                            try:
                                router = walk(routerport.router.getkey())
                            except KeyError:
                                pass
                            else:
                                save(router.getkey())

                                if router.interfaces.dataset():

                                    for weakobj in router.interfaces.dataset():
                                        routerport_weakkey = weakobj.getkey()

                                        # we walk from this key , so except
                                        if routerport_weakkey != routerport.getkey():
                                            try:
                                                weakrouterport = walk(routerport_weakkey)
                                            except KeyError:
                                                pass
                                            else:
                                                save(routerport_weakkey)

                                                if hasattr(weakrouterport, "subnet"):
                                                    try:
                                                        weaksubnet = walk(weakrouterport.subnet.getkey())
                                                    except KeyError:
                                                        pass
                                                    else:
                                                        save(weaksubnet.getkey())

                                                        if hasattr(weaksubnet, "network"):
                                                            try:
                                                                logicalnetwork = walk(weaksubnet.network.getkey())
                                                            except KeyError:
                                                                pass
                                                            else:
                                                                save(logicalnetwork.getkey())

    def _walk_lgnet(self, key, value, walk, save):

        if value is None:
            return

        save(key)

    def updateflow(self, connection, addvalues, removevalues, updatedvalues):

        try:
            lastrouterinfo = self._lastrouterinfo

            allobjects = set(o for o in self._savedresult if o is not None and not o.isdeleted())

            currentlognetinfo = dict((n,id) for n,id in self._lastlogicalnet if n in allobjects)

            currentsubnetinfo = dict((s,currentlognetinfo[s.network]) for s in allobjects
                                     if s.isinstance(SubNet) and s.network in currentlognetinfo)

            currentrouterportinfo = dict((r,r.subnet) for r in allobjects
                                            if r.isinstance(RouterPort)
                                         )
            #currentrouter = set(n for n in allobjects if n.isinstance(VRouter))

            currentrouterinfo = dict((r,([(r.routes,self._parent.inroutermac,
                                            getattr(interface,"ip_address",interface.subnet.gateway),
                                            interface.subnet.cidr,
                                            currentsubnetinfo[interface.subnet])
                                            for interface in currentrouterportinfo
                                            if interface.router.getkey() == r.getkey() and
                                            hasattr(interface,"subnet") and (hasattr(interface,"ip_address")
                                                                             or hasattr(interface.subnet,"gateway"))
                                            and interface.subnet in currentsubnetinfo
                                         ])
                                      ) for r in allobjects if r.isinstance(VRouter)
                                     )

            self._lastrouterinfo = currentrouterinfo

            ofdef = connection.openflowdef
            vhost = connection.protocol.vhost
            l3input = self._parent._gettableindex("l3input",vhost)
            l3router =  self._parent._gettableindex("l3router",vhost)
            l3output = self._parent._gettableindex("l3output",vhost)
            cmds = []

            if connection.protocol.disablenxext:
                def match_network(nid):
                    return ofdef.create_oxm(ofdef.OXM_OF_METADATA_W, (nid & 0xffff) << 32,
                                            b'\x00\x00\xff\xff\x00\x00\x00\x00')
            else:
                def match_network(nid):
                    return ofdef.create_oxm(ofdef.NXM_NX_REG5, nid)

            def _createinputflow(macaddress,ipaddress,netid):
                return [
                    ofdef.ofp_flow_mod(
                        cookie=0x3,
                        cookie_mask=0xffffffffffffffff,
                        table_id=l3input,
                        command=ofdef.OFPFC_ADD,
                        priority=ofdef.OFP_DEFAULT_PRIORITY,
                        buffer_id=ofdef.OFP_NO_BUFFER,
                        out_port=ofdef.OFPP_ANY,
                        out_group=ofdef.OFPG_ANY,
                        match=ofdef.ofp_match_oxm(
                            oxm_fields=[
                                match_network(netid),
                                ofdef.create_oxm(ofdef.OXM_OF_ETH_DST, mac_addr_bytes(macaddress)),
                                ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE, ofdef.ETHERTYPE_IP),
                                ofdef.create_oxm(ofdef.OXM_OF_IPV4_DST, ip4_addr_bytes(ipaddress))
                            ]
                        ),
                        instructions=[
                            ofdef.ofp_instruction_actions(
                                actions=[
                                    ofdef.ofp_instruction_goto_table(table_id=l3router)
                                ]
                            )
                        ]
                    )
                ]

            def _createrouterflow(routes,nid):
                ret = []

                for cidr,prefix,netid in routes:
                    flow = ofdef.ofp_flow_mod(
                            table_id = l3router,
                            command = ofdef.OFPFC_ADD,
                            priority = ofdef.OFP_DEFAULT_PRIORITY + prefix,
                            buffer_id = ofdef.OFP_NO_BUFFER,
                            out_port = ofdef.OFPP_ANY,
                            out_group = ofdef.OFPG_ANY,
                            match = ofdef.ofp_match_oxm(
                                oxm_fields = [
                                    match_network(nid),
                                    ofdef.create_oxm(ofdef.OXM_OF_ETH_TYPE,ofdef.ETHERTYPE_IP),
                                    ofdef.create_oxm(ofdef.OXM_OF_IPV4_DST_W,
                                                 ip4_addr_bytes(ip4_addr.formatter(cidr)),
                                                 ip4_addr_bytes(ip4_addr.formatter(get_netmask(prefix))))
                                ]
                            ),
                            instrctions=[
                                ofdef.ofp_action_set_field(
                                    field = ofdef.create_oxm(ofdef.NXM_NX_REG5,netid)
                                ),
                                ofdef.ofp_instruction_goto_table(table = l3output)
                            ]
                        )

                    ret.append([flow])

            for obj in addvalues:
                if obj in currentrouterinfo and obj not in lastrouterinfo:
                    link_routes = []
                    static_routes = []
                    add_routes = []

                    for routes,mac,ipaddress,cidr,netid in currentrouterinfo[obj]:

                        # every interface have same routes in routerinfo
                        static_routes = routes

                        network, prefix = parse_ip4_network(cidr)
                        link_routes.append((network,prefix,netid))

                        # add router mac + ipaddress ---->>> l3router
                        cmds.extend(_createinputflow(mac, ipaddress, netid))

                    for network,prefix,netid in link_routes:
                        add_routes.append((network,prefix,netid))
                        for cidr, nethop in static_routes:

                            if ip_in_network(nethop,network,prefix):
                                c, f = parse_ip4_network(cidr)
                                add_routes.append((c,f,netid))

                    for _,_,_,netid in currentrouterportinfo[obj]:
                        cmds.extend(_createrouterflow(add_routes,netid))

            for m in self.execute_commands(connection,cmds):
                yield m



        except Exception:
            self._logger.warning("router update flow exception, ignore it! continue",exc_info=True)


class L3Router(FlowBase):
    _tablerequest = (
        ("l3router", ("l3input",), "router"),
        ("l3output", ("l3router",), "router"),
        ("l2output", ("l3output",), "")
    )

    _default_inroutermac = '1a:23:67:59:63:33'

    def __init__(self, server):
        super(L3Router, self).__init__(server)
        self.app_routine = RoutineContainer(self.scheduler)
        self.app_routine.main = self._main
        self.routines.append(self.app_routine)
        self._flowupdater = dict()

    def _main(self):
        flowinit = FlowInitialize.createMatcher(_ismatch=lambda x: self.vhostbind is None or
                                                x.vhost in self.vhostbind)

        conndown = OpenflowConnectionStateEvent.createMatcher(state=OpenflowConnectionStateEvent.CONNECTION_DOWN,
                                                              _ismatch=lambda x: self.vhostbind is None
                                                              or x.vhost in self.vhostbind)

        while True:
            yield (flowinit, conndown)

            if self.app_routine.matcher is flowinit:
                c = self.app_routine.event.connection
                self.app_routine.subroutine(self._init_conn(c))
            if self.app_routine.matcher is conndown:
                c = self.app_routine.event.connection
                self.app_routine.subroutine(self._uninit_conn(c))

    def _init_conn(self, conn):

        if conn in self.flowupdater:
            updater = self._flowupdater.pop(conn)
            updater.close()

        updater = RouterUpdater(conn, self)

        self._flowupdater[conn] = updater
        updater.start()

        if False:
            yield

    def _uninit_conn(self, conn):

        if conn in self._flowupdater:
            updater = self._flowupdater.pop(conn)
            updater.close()

        if False:
            yield

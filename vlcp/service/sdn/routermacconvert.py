import itertools

from vlcp.event import RoutineContainer
from vlcp.protocol.openflow import OpenflowConnectionStateEvent
from vlcp.server.module import callAPI
from vlcp.service.sdn.flowbase import FlowBase
from vlcp.service.sdn.ofpmanager import FlowInitialize
from vlcp.utils.ethernet import mac_addr, mac_addr_bytes
from vlcp.utils.flowupdater import FlowUpdater
import vlcp.service.sdn.ioprocessing as iop

class RouterMACConvertUpdater(FlowUpdater):
    def __init__(self, connection, parent):
        super(RouterMACConvertUpdater, self).__init__(connection, (),
                                                      ("RouterMACConvertUpdater", connection), parent._logger)
        self._parent = parent
        self._lastphysicalport = {}
        self._lastphyportinfo = {}

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

        while True:
            yield (dataobjectchange,)

            _, self._lastphysicalport, _, _ = self.event.current


    def _update_walk(self):
        physicalportkeys = [p.getkey() for p, _ in self._lastphysicalport]

        self._initialkeys = physicalportkeys
        #self._original_keys = logicalportkeys + logicalnetkeys

        self._walkerdict = dict(itertools.chain(((p, self._walk_phyport) for p in physicalportkeys)))

        self.subroutine(self.restart_walk(), False)

    def _walk_phyport(self, key, value, walk, save):
        if value is None:
            return
        save(key)

    def updateflow(self, connection, addvalues, removevalues, updatedvalues):

        try:
            #allobjects = set(o for o in self._savedresult if o is not None and not o.isdeleted())
            datapath_id = connection.openflow_datapathid
            ofdef = connection.openflowdef
            vhost = connection.protocol.vhost

            # for m in self.executeAll([callAPI(self,"openflowportmanager","waitportbyname",
            #                             {"datapathid":datapath_id,"vhost":vhost,"portno":portno})
            #                             for _,portno in self._lastphysicalport]):
            #     yield m

            currentphyportinfo = {}
            lastphyportinfo = self._lastphyportinfo
            for p,portno in self._lastphysicalport:

                for m in callAPI(self,"openflowportmanager","waitportbyname",
                                 {"datapathid":datapath_id,"vhost":vhost,"portno":portno}):
                    yield m

                portmac = self.retvalue.hw_addr

                # convert physicalport mac as router out mac
                outmac = [s ^ m for s,m in zip(portmac,mac_addr(self._parent.outroutermacmask))]

                currentphyportinfo[p] = (mac_addr.formatter(outmac),portno)

            self._lastphyportinfo = currentphyportinfo

            inmacconverttable = self._parent._gettableindex("inmacconvert", vhost)
            outmacconverttable = self._parent._gettableindex("outmacconvert", vhost)

            cmds = []

            def _add_mac_convert_flow(mac,portno):
                return [
                    ofdef.ofp_flow_mod(
                        table_id=inmacconverttable,
                        command=ofdef.OFPFC_ADD,
                        priority=ofdef.OFP_DEFAULT_PRIORITY,
                        buffer_id=ofdef.OFP_NO_BUFFER,
                        out_port=ofdef.OFPP_ANY,
                        out_group=ofdef.OFPG_ANY,
                        match=ofdef.ofp_match_oxm(
                            oxm_fields=[
                                ofdef.create_oxm(ofdef.OXM_OF_IN_PORT,portno),
                                ofdef.create_oxm(ofdef.OXM_OF_ETH_DST, mac_addr_bytes(mac))
                            ]
                        ),
                        instructions=[
                            ofdef.ofp_instruction_actions(
                                actions = [
                                    ofdef.ofp_action_set_field(
                                        field=ofdef.create_oxm(ofdef.OXM_OF_ETH_DST,
                                                               mac_addr_bytes(self._parent.inroutermac))
                                    )
                                ]
                            )
                        ]
                    ),
                    ofdef.ofp_flow_mod(
                        table_id=outmacconverttable,
                        command=ofdef.OFPFC_ADD,
                        priority=ofdef.OFP_DEFAULT_PRIORITY,
                        buffer_id=ofdef.OFP_NO_BUFFER,
                        out_port=ofdef.OFPP_ANY,
                        out_group=ofdef.OFPG_ANY,
                        match=ofdef.ofp_match_oxm(
                            oxm_fields=[
                                ofdef.create_oxm(ofdef.OXM_OF_IN_PORT,portno),
                                ofdef.create_oxm(ofdef.OXM_OF_ETH_SRC, mac_addr_bytes(self._parent.inroutermac))
                            ]
                        ),
                        instructions=[
                            ofdef.ofp_instruction_actions(
                                actions = [
                                    ofdef.ofp_action_set_field(
                                        field=ofdef.create_oxm(ofdef.OXM_OF_ETH_SRC, mac_addr_bytes(mac))
                                    )
                                ]
                            )
                        ]
                    )
                ]

            def _remove_mac_convert_flow(mac,portno):
                return [
                    ofdef.ofp_flow_mod(
                        table_id=inmacconverttable,
                        command=ofdef.OFPFC_DELETE,
                        priority=ofdef.OFP_DEFAULT_PRIORITY,
                        buffer_id=ofdef.OFP_NO_BUFFER,
                        out_port=ofdef.OFPP_ANY,
                        out_group=ofdef.OFPG_ANY,
                        match=ofdef.ofp_match_oxm(
                            oxm_fields=[
                                ofdef.create_oxm(ofdef.OXM_OF_IN_PORT, portno),
                                ofdef.create_oxm(ofdef.OXM_OF_ETH_DST, mac_addr_bytes(mac))
                            ]
                        )
                    ),
                    ofdef.ofp_flow_mod(
                        table_id=outmacconverttable,
                        command=ofdef.OFPFC_DELETE,
                        priority=ofdef.OFP_DEFAULT_PRIORITY,
                        buffer_id=ofdef.OFP_NO_BUFFER,
                        out_port=ofdef.OFPP_ANY,
                        out_group=ofdef.OFPG_ANY,
                        match=ofdef.ofp_match_oxm(
                            oxm_fields=[
                                ofdef.create_oxm(ofdef.OXM_OF_IN_PORT, portno),
                                ofdef.create_oxm(ofdef.OXM_OF_ETH_SRC, mac_addr_bytes(self._parent.inroutermac))
                            ]
                        )
                    )

                ]
            for obj in removevalues:
                if obj in lastphyportinfo:
                    mac,portno = lastphyportinfo[obj]
                    cmds.extend(_remove_mac_convert_flow(mac,portno))

            for m in self.execute_commands(connection,cmds):
                yield m

            del cmds[:]
            for obj in addvalues:
                if obj in currentphyportinfo:
                    mac,portno = currentphyportinfo[obj]
                    cmds.extend(_add_mac_convert_flow(mac,portno))

            for m in self.execute_commands(connection, cmds):
                yield m
        except Exception:
            self._logger.warning("router convert mac flow exception, ignore it! continue", exc_info=True)

class RouterMACConvert(FlowBase):
    _tablerequest = (
        ("inmacconvert", ("ingress",), ""),
        ("l2input",("inmacconvert",),""),
        ("outmacconvert", ("l2output",), ""),
        ("egress", ("outmacconvert",), "")
    )

    _default_inroutermac = '1a:23:67:59:63:33'
    _default_outroutermacmask = '0a:00:00:00:00:00'

    def __init__(self, server):
        super(RouterMACConvert, self).__init__(server)
        self.app_routine = RoutineContainer(self.scheduler)
        self.app_routine.main = self._main
        self.routines.append(self.app_routine)
        self._flowupdater = dict()

    def _main(self):
        flowinit = FlowInitialize.createMatcher(_ismatch=lambda x: self.vhostbind is None or
                                                                   x.vhost in self.vhostbind)

        conndown = OpenflowConnectionStateEvent.createMatcher(state=OpenflowConnectionStateEvent.CONNECTION_DOWN,
                                                              _ismatch=lambda x: self.vhostbind is None
                                                                                 or x.createby.vhost in self.vhostbind)

        while True:
            yield (flowinit, conndown)

            if self.app_routine.matcher is flowinit:
                c = self.app_routine.event.connection
                self.app_routine.subroutine(self._init_conn(c))
            if self.app_routine.matcher is conndown:
                c = self.app_routine.event.connection
                self.app_routine.subroutine(self._uninit_conn(c))

    def _init_conn(self, conn):
        if conn in self._flowupdater:
            updater = self._flowupdater.pop(conn)
            updater.close()

        updater = RouterMACConvertUpdater(conn, self)

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
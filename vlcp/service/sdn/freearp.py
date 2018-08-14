'''
Created on 2018/8/9

:author: wangqianb
'''

from vlcp.config.config import defaultconfig
from vlcp.server.module import depend, api
from vlcp.service.sdn.flowbase import FlowBase
from vlcp.service.sdn.ofpmanager import FlowInitialize
from vlcp.utils.flowupdater import FlowUpdater
from vlcp.utils.networkmodel import PhysicalPort, LogicalPort, LogicalNetwork,\
    LogicalNetworkMap
import vlcp.service.kvdb.objectdb as objectdb
import vlcp.service.sdn.ofpportmanager as ofpportmanager
from vlcp.event.runnable import RoutineContainer
from vlcp.protocol.openflow.openflow import OpenflowConnectionStateEvent,\
    OpenflowAsyncMessageEvent, OpenflowErrorResultException
from vlcp.utils.ethernet import arp_packet_l4, mac_addr, ethernet_l2, ip4_addr
import vlcp.service.sdn.ioprocessing as iop
import itertools
from vlcp.utils.exceptions import WalkKeyNotRetrieved
from contextlib import suppress
from vlcp.event.event import M_
from vlcp.server.module import call_api


class FreeArpUpdater(FlowUpdater):
    def __init__(self, connection, parent):
        FlowUpdater.__init__(self, connection, (), ('FreeArpUpdater', connection), parent._logger)
        self._parent = parent
        self._lastlognets = ()
        self._lastphyports = ()
        self._lastlogports = ()
        self._lastlogportinfo = {}
        self._lastphyportinfo = {}
        self._lastlognetinfo = {}
        self._last_arps = set()

    async def main(self):
        try:
            self.subroutine(self._update_handler(), True, '_update_handler_routine')
            await FlowUpdater.main(self)
        finally:
            if hasattr(self, '_update_handler_routine'):
                self._update_handler_routine.close()

    async def _update_handler(self):
        dataobjectchanged = iop.DataObjectChanged.createMatcher(None, None, self._connection)
        while True:
            ev = await dataobjectchanged
            self._lastlogports, self._lastphyports, self._lastlognets, _ = ev.current
            self._update_walk()
            self.updateobjects((p for p, _ in self._lastlogports))

    def _walk_logport(self, key, value, walk, save):
        save(key)
        if value is not None:
            return

    def _walk_phyport(self, key, value, walk, save):
        save(key)
        if value is not None:
            return

    def _walk_lognet(self, key, value, walk, save):
        save(key)
        if value is None:
            return


    def _update_walk(self):
        logport_keys = [p.getkey() for p, _ in self._lastlogports]
        phyport_keys = [p.getkey() for p, _ in self._lastphyports]
        lognet_keys = [n.getkey() for n, _ in self._lastlognets]
        lognet_mapkeys = [LogicalNetworkMap.default_key(n.id) for n, _ in self._lastlognets]

        self._initialkeys = logport_keys + lognet_keys + phyport_keys
        self._walkerdict = dict(itertools.chain(((n, self._walk_lognet) for n in lognet_keys),
                                                ((p, self._walk_logport) for p in logport_keys),
                                                ((p, self._walk_phyport) for p in phyport_keys)))
        self.subroutine(self.restart_walk(), False)

    async def updateflow(self, connection, addvalues, removevalues, updatedvalues):
        try:
            allobjs = set(o for o in self._savedresult if o is not None and not o.isdeleted())
            lastlogportinfo = self._lastlogportinfo
            currentlognetinfo = dict((n, (id, n.physicalnetwork)) for n, id in self._lastlognets if n in allobjs)
            currentlogportinfo = dict((p, (id, p.network)) for p, id in self._lastlogports if p in allobjs)
            currentphyportinfo = dict((p, (id, p.physicalnetwork)) for p, id in self._lastphyports if p in allobjs)

            currentphynetinfo = dict((p.physicalnetwork, (id, p)) for p, id in self._lastphyports if p in allobjs)
            self._lastlogportinfo = currentlogportinfo
            self._lastlognetinfo = currentlognetinfo
            self._lastphyportinfo = currentphyportinfo
            cmds = []
            ofdef = connection.openflowdef

            def packet_out_free_arp(logicalport,logicalnet,logicalnetid,phyportid):
                egress = self._parent._gettableindex("egress", self._connection.protocol.vhost)
                arp_request_packet = arp_packet_l4(
                    dl_src=mac_addr(logicalport.mac_address),
                    dl_dst=mac_addr("FF:FF:FF:FF:FF:FF"),
                    arp_op=ofdef.ARPOP_REQUEST,
                    arp_sha=mac_addr(logicalport.mac_address),
                    arp_spa=ip4_addr(logicalport.ip_address),
                    arp_tpa=ip4_addr(logicalport.ip_address)
                )

                return ofdef.ofp_packet_out(buffer_id=ofdef.OFP_NO_BUFFER,
                                                 in_port=ofdef.OFPP_CONTROLLER,
                                                 actions=[
                                                     ofdef.ofp_action_set_field(
                                                         field=ofdef.create_oxm(ofdef.NXM_NX_REG4,
                                                                                logicalnetid)
                                                     ),
                                                     ofdef.ofp_action_set_field(
                                                         field=ofdef.create_oxm(ofdef.NXM_NX_REG5,
                                                                                logicalnetid)
                                                     ),
                                                     ofdef.ofp_action_set_field(
                                                         field=ofdef.create_oxm(ofdef.NXM_NX_REG6,
                                                                                phyportid)
                                                     ),
                                                     ofdef.ofp_action_set_field(
                                                         field=ofdef.create_oxm(ofdef.NXM_NX_REG7,
                                                                                0x4000)
                                                     ),
                                                     ofdef.nx_action_resubmit(
                                                         table=egress
                                                     )
                                                 ],
                                                 data=arp_request_packet._tobytes()
                                                 )

            for p in currentlogportinfo:
                if p not in lastlogportinfo or lastlogportinfo[p] != currentlogportinfo[p]:
                    _, logicalnet = currentlogportinfo[p]
                    logicalnetid, _ = currentlognetinfo[logicalnet]
                    phyportid, _ = currentphynetinfo[logicalnet.physicalnetwork]
                    if phyportid is not None and logicalnet.physicalnetwork.type in self._parent.allowednetworktypes:
                        if await call_api(self, 'ioprocessing', 'flowready', {'connection': connection,
                                                                            'logicalnetworkid': logicalnetid,
                                                                            'physicalportid': phyportid}):
                            cmds.append(packet_out_free_arp(p,logicalnet,logicalnetid,phyportid))
            if cmds is not None:
                await self.execute_commands(connection, cmds)
        except Exception:
            self._logger.warning("Unexpected exception in FreeArpUpdater. Will ignore and continue.", exc_info=True)


@defaultconfig
@depend(ofpportmanager.OpenflowPortManager, objectdb.ObjectDB)
class FreeArp(FlowBase):
    "Send FREE ARP"

    _tablerequest = (("ingress", (), ''),
                     ("egress", ("ingress",), ''))

    _default_allowednetworktypes = ("native", "vlan")

    def __init__(self, server):
        FlowBase.__init__(self, server)
        self.apiroutine = RoutineContainer(self.scheduler)
        self.apiroutine.main = self._main
        self.routines.append(self.apiroutine)
        self._flowupdaters = {}

    async def _main(self):
        flow_init = FlowInitialize.createMatcher(_ismatch = lambda x: self.vhostbind is None or x.vhost in self.vhostbind)
        conn_down = OpenflowConnectionStateEvent.createMatcher(state = OpenflowConnectionStateEvent.CONNECTION_DOWN,
                                                               _ismatch = lambda x: self.vhostbind is None or x.createby.vhost in self.vhostbind)
        while True:
            ev, m = await M_(flow_init, conn_down)
            if m is flow_init:
                c = ev.connection
                self.apiroutine.subroutine(self._init_conn(c))
            else:
                c = ev.connection
                self.apiroutine.subroutine(self._remove_conn(c))

    async def _init_conn(self, conn):
        # Default
        if conn in self._flowupdaters:
            updater = self._flowupdaters.pop(conn)
            updater.close()
        updater = FreeArpUpdater(conn, self)
        self._flowupdaters[conn] = updater
        updater.start()

    async def _remove_conn(self, conn):
        # Do not need to modify flows
        if conn in self._flowupdaters:
            self._flowupdaters.pop(conn).close()
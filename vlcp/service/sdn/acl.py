'''
Created on 2018/8/9

:author: wangqianb
'''

from vlcp.config.config import defaultconfig
from vlcp.server.module import depend
from vlcp.service.sdn.flowbase import FlowBase
from vlcp.service.sdn.ofpmanager import FlowInitialize
from vlcp.utils.flowupdater import FlowUpdater
from vlcp.utils.networkmodel import SubNet, LogicalPort
from vlcp.utils.netutils import ip4_parser, mac_parser, protocol_parser, icmp_parser, protocol_dport_parser, protocol_sport_parser
import vlcp.service.kvdb.objectdb as objectdb
import vlcp.service.sdn.ofpportmanager as ofpportmanager
from vlcp.event.runnable import RoutineContainer
from vlcp.protocol.openflow.openflow import OpenflowConnectionStateEvent
import vlcp.service.sdn.ioprocessing as iop
import itertools
from vlcp.event.event import M_

acl_parser = {
                "src_mac": ('OXM_OF_ETH_SRC', mac_parser),
                "dst_mac": ('OXM_OF_ETH_DST', mac_parser),
                "src_ip": ('OXM_OF_IPV4_SRC', ip4_parser),
                "dst_ip": ('OXM_OF_IPV4_DST', ip4_parser),
                "protocol": ('OXM_OF_IP_PROTO', protocol_parser),
                "icmp_type": ('OXM_OF_ICMPV4_TYPE', icmp_parser),
                "icmp_code": ('OXM_OF_ICMPV4_CODE', icmp_parser)
            }
acl_port_parser = {
                "sport": protocol_sport_parser,
                "dport": protocol_dport_parser
           }
ip_type_key = ["src_ip", "dst_ip", "protocol", "icmp_type", "icmp_code", "sport", "dport"]

class ACLUpdater(FlowUpdater):
    def __init__(self, connection, parent):
        FlowUpdater.__init__(self, connection, (), ('ACLUpdater', connection), parent._logger)
        self._parent = parent
        self._lastlognets = ()
        self._lastphyports = ()
        self._lastlogports = ()
        self._lastlogportinfo = {}
        self._lastsubnetsinfo = {}
        self._lastlognetinfo = {}
        self._orig_initialkeys = ()

    async def main(self):
        try:
            if self._connection.protocol.disablenxext:
                self._logger.warning("ACL fuc disabled on connection %r because Nicira extension is not enabled", self._connection)
                return
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
        if hasattr(value, 'subnet'):
            try:
                subnet = walk(value.subnet.getkey())
            except KeyError:
                pass
            else:
                save(subnet.getkey())

    def _walk_phyport(self, key, value, walk, save):
        save(key)

    def _walk_lognet(self, key, value, walk, save):
        save(key)

    def _update_walk(self):
        logport_keys = [p.getkey() for p, _ in self._lastlogports]
        phyport_keys = [p.getkey() for p, _ in self._lastphyports]
        lognet_keys = [n.getkey() for n, _ in self._lastlognets]

        self._initialkeys = logport_keys + lognet_keys + phyport_keys
       # self._orig_initialkeys = logport_keys + lognet_keys + phyport_keys
        self._walkerdict = dict(itertools.chain(((n, self._walk_lognet) for n in lognet_keys),
                                                ((p, self._walk_logport) for p in logport_keys),
                                                ((p, self._walk_phyport) for p in phyport_keys)))
        self.subroutine(self.restart_walk(), False)

    async def updateflow(self, connection, addvalues, removevalues, updatedvalues):
        try:
            allobjs = set(o for o in self._savedresult if o is not None and not o.isdeleted())
            lastlogportinfo = self._lastlogportinfo
            lastlognetinfo = self._lastlognetinfo
            lastsubnetinfo = self._lastsubnetsinfo
            currentlognetinfo = dict((n, (id, n.acl)) for n, id in self._lastlognets if n in allobjs and hasattr(n, "acl"))
            currentlogportinfo = dict((p, (p.egress_acl, p.ingress_acl)) for p, id in self._lastlogports if p in allobjs
                                      and hasattr(p, 'ingress_acl') and hasattr(p, 'egress_acl'))
            currentsubnetinfo = dict((s, s.acl) for s in allobjs if s is not None and not s.isdeleted()
                                     and s.isinstance(SubNet) and hasattr(s, 'acl'))

            self._lastsubnetsinfo = currentsubnetinfo
            self._lastlogportinfo = currentlogportinfo
            self._lastlognetinfo = currentlognetinfo
            cmds = []
            ofdef = connection.openflowdef

            tab_egress = self._parent._gettableindex('egress_acl', self._connection.protocol.vhost)
            tab_ingress = self._parent._gettableindex('ingress_acl', self._connection.protocol.vhost)
            tab_acl = self._parent._gettableindex('acl', self._connection.protocol.vhost)
            table_next = {
                tab_egress: self._parent._getnexttable('', 'egress_acl', vhost=self._connection.protocol.vhost),
                tab_ingress: self._parent._getnexttable('', 'ingress_acl', vhost=self._connection.protocol.vhost),
                tab_acl: self._parent._getnexttable('', 'acl', vhost=self._connection.protocol.vhost)
            }

            def add_flow(oxm_fields, act, pri, table):
                ins = []
                if act is False:
                    ins.append(ofdef.ofp_instruction_actions(type=ofdef.OFPIT_CLEAR_ACTIONS))
                else:
                    ins.append(ofdef.ofp_instruction_goto_table(table_id=table_next[table]))
                return ofdef.ofp_flow_mod(
                        cookie=0x2,
                        cookie_mask=0xffffffffffffffff,
                        table_id=table,
                        command=ofdef.OFPFC_ADD,
                        priority=pri,
                        out_port=ofdef.OFPP_ANY,
                        out_group=ofdef.OFPG_ANY,
                        buffer_id=ofdef.OFP_NO_BUFFER,
                        match=ofdef.ofp_match_oxm(
                            oxm_fields=oxm_fields
                        ),
                        instructions=ins
                )

            def del_flow(oxm_fields, pri, table):
                return ofdef.ofp_flow_mod(
                        cookie=0x2,
                        cookie_mask=0xffffffffffffffff,
                        table_id=table,
                        command=ofdef.OFPFC_DELETE,
                        priority=pri,
                        out_port=ofdef.OFPP_ANY,
                        out_group=ofdef.OFPG_ANY,
                        buffer_id=ofdef.OFP_NO_BUFFER,
                        match=ofdef.ofp_match_oxm(
                            oxm_fields=oxm_fields
                        )
                )

            def add_acl(cmds, acls, table):
                for acl in acls[::-1]:
                    oxm_fields = []
                    pri = ofdef.OFP_DEFAULT_PRIORITY
                    keys = set(k for k in acl)
                    if len(keys.intersection(set(ip_type_key))):
                        oxm_fields.append(ofdef.create_oxm(getattr(ofdef, "OXM_OF_ETH_TYPE"), ofdef.ETHERTYPE_IP))
                    for k in acl:
                        if k == 'accept':
                            act = acl[k]
                        elif k == 'priority':
                            pri = acl[k]
                        elif k in acl_port_parser:
                            oxm_parser = acl_port_parser[k]
                            proc, _ = protocol_parser(acl['protocol'])
                            oxm_def = oxm_parser(proc)
                            oxm_fields.append(ofdef.create_oxm(getattr(ofdef, oxm_def), acl[k]))
                        elif k in acl_parser:
                            oxm_def, parser = acl_parser[k]
                            value, mask = parser(acl[k])
                            if mask is None:
                                oxm_fields.append(ofdef.create_oxm(getattr(ofdef, oxm_def), value))
                            else:
                                oxm_fields.append(ofdef.create_oxm(getattr(ofdef, oxm_def + '_W'), value, mask))
                    if oxm_fields:
                        cmds.append(add_flow(oxm_fields, act, pri, table))

            def del_acl(cmds, acls, table):
                for acl in acls[::-1]:
                    oxm_fields = []
                    pri = ofdef.OFP_DEFAULT_PRIORITY
                    keys = set(k for k in acl)
                    if len(keys.intersection(set(ip_type_key))):
                        oxm_fields.append(ofdef.create_oxm(getattr(ofdef, "OXM_OF_ETH_TYPE"), ofdef.ETHERTYPE_IP))
                    for k in acl:
                        if k == 'priority':
                            pri = acl[k]
                        elif k in acl_port_parser:
                            oxm_parser = acl_port_parser[k]
                            proc, _ = protocol_parser(acl['protocol'])
                            oxm_def = oxm_parser(proc)
                            oxm_fields.append(ofdef.create_oxm(getattr(ofdef, oxm_def), acl[k]))
                        elif k in acl_parser:
                            oxm_def, parser = acl_parser[k]
                            value, mask = parser(acl[k])
                            if mask is None:
                                oxm_fields.append(ofdef.create_oxm(getattr(ofdef, oxm_def), value))
                            else:
                                oxm_fields.append(ofdef.create_oxm(getattr(ofdef, oxm_def + '_W'), value, mask))
                    if oxm_fields:
                        cmds.append(del_flow(oxm_fields, pri, table))

            # logicalnet acl process
            for n in currentlognetinfo:
                if n not in lastlognetinfo:
                    _, acl = currentlognetinfo[n]
                    add_acl(cmds, acl, tab_acl)
                elif currentlognetinfo[n] != lastlognetinfo[n]:
                    _, currentacl = currentlognetinfo[n]
                    _, lastacl = lastlognetinfo[n]
                    if currentacl != lastacl:
                        for acl in lastacl:
                            if acl not in currentacl:
                                del_acl(cmds, [acl], tab_acl)
                        for acl in currentacl:
                            if acl not in lastacl:
                                add_acl(cmds, [acl], tab_acl)
            if cmds:
                await self.execute_commands(connection, cmds)
                del cmds[:]

            # logicalport acl process
            for p in currentlogportinfo:
                if p not in lastlogportinfo:
                    _, ingress_acl = currentlogportinfo[p]
                    egress_acl, _ = currentlogportinfo[p]
                    add_acl(cmds, egress_acl, tab_egress)
                    add_acl(cmds, ingress_acl, tab_ingress)
                elif currentlogportinfo[p] != lastlogportinfo[p]:
                    currentegressacl, currentingressacl = currentlogportinfo[p]
                    lastegressacl, lastingressacl = lastlogportinfo[p]
                    if currentegressacl != lastegressacl:
                        for acl in lastegressacl:
                            if acl not in currentegressacl:
                                del_acl(cmds, [acl], tab_egress)
                        for acl in currentegressacl:
                            if acl not in lastegressacl:
                                add_acl(cmds, [acl], tab_egress)

                    if currentingressacl != lastingressacl:
                        for acl in lastingressacl:
                            if acl not in currentingressacl:
                                del_acl(cmds, [acl], tab_ingress)
                        for acl in currentingressacl:
                            if acl not in lastingressacl:
                                add_acl(cmds, [acl], tab_ingress)
            if cmds:
                await self.execute_commands(connection, cmds)
                del cmds[:]

            # subnet acl process
            for s in currentsubnetinfo:
                if s not in lastsubnetinfo:
                    acl = currentsubnetinfo[s]
                    add_acl(cmds, acl, tab_acl)
                elif currentsubnetinfo[s] != lastsubnetinfo[s]:
                    currentacl = currentsubnetinfo[s]
                    lastacl = lastsubnetinfo[s]
                    if currentacl != lastacl:
                        for acl in lastacl:
                            if acl not in currentacl:
                                del_acl(cmds, [acl], tab_acl)
                        for acl in currentacl:
                            if acl not in lastacl:
                                add_acl(cmds, [acl], tab_acl)
            if cmds:
                await self.execute_commands(connection, cmds)
                del cmds[:]

        except Exception:
            self._logger.warning("Unexpected exception in ACLUpdater. Will ignore and continue.", exc_info=True)


@defaultconfig
@depend(ofpportmanager.OpenflowPortManager, objectdb.ObjectDB)
class ACL(FlowBase):

    _tablerequest = (("ingress_acl", ('ingress',), ''),
                     ("l2input", ('ingress_acl',), ''),
                     ("acl", ('l2output',), ''),
                     ("egress_acl", ('acl',), ''),
                     ("egress", ("egress_acl",), ''))

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
        updater = ACLUpdater(conn, self)
        self._flowupdaters[conn] = updater
        updater.start()

    async def _remove_conn(self, conn):
        # Do not need to modify flows
        if conn in self._flowupdaters:
            self._flowupdaters.pop(conn).close()
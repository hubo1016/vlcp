'''
Created on 2016/4/13

:author: hubo
'''
from vlcp.service.sdn.flowbase import FlowBase
from vlcp.server.module import depend, ModuleNotification, callAPI
import vlcp.service.sdn.ofpportmanager as ofpportmanager
import vlcp.service.sdn.ovsdbportmanager as ovsdbportmanager
import vlcp.service.kvdb.objectdb as objectdb
from vlcp.event.event import Event, withIndices
from vlcp.event.runnable import RoutineContainer, RoutineException
from vlcp.config.config import defaultconfig
from vlcp.service.sdn.ofpmanager import FlowInitialize
from vlcp.utils.networkmodel import PhysicalPort, LogicalPort, PhysicalPortSet, LogicalPortSet, LogicalNetwork, \
    PhysicalNetwork,SubNet,RouterPort,VRouter, \
    PhysicalNetworkMap
from vlcp.utils.flowupdater import FlowUpdater

import itertools

@withIndices('datapathid', 'vhost', 'connection', 'logicalportchanged', 'physicalportchanged',
                                                    'logicalnetworkchanged', 'physicalnetworkchanged')
class DataObjectChanged(Event):
    pass

class IDAssigner(object):
    def __init__(self):
        self._indices = {}
        self._revindices = {}
        # Reserve 0 and 0xffff
        self._revindices[0] = '<reserve0>'
        self._revindices[0xffff] = '<reserve65535>'
        self._lastindex = 1
    def assign(self, key):
        if key in self._indices:
            return self._indices[key]
        else:
            ind = self._lastindex
            while ind in self._revindices:
                ind += 1
                ind &= 0xffff
            self._revindices[ind] = key
            self._indices[key] = ind
            self._lastindex = ind + 1
            return ind
    def unassign(self, keys):
        for k in keys:
            ind = self._indices.pop(k, None)
            if ind is not None:
                del self._revindices[ind]
    def frozen(self):
        return dict(self._indices)

def _to32bitport(portno):
    if portno >= 0xff00:
        portno = 0xffff0000 | portno
    return portno

class IOFlowUpdater(FlowUpdater):
    def __init__(self, connection, systemid, bridgename, parent):
        FlowUpdater.__init__(self, connection, (PhysicalPortSet.default_key(),),
                                            ('ioprocessing', connection),
                                            parent._logger)
        self._walkerdict = {PhysicalPortSet.default_key(): self._physicalport_walker}
        self._systemid = systemid
        self._bridgename = bridgename
        self._portnames = {}
        self._portids = {}
        self._currentportids = {}
        self._currentportnames = {}
        self._lastportids = {}
        self._lastportnames = {}
        self._lastnetworkids = {}
        self._networkids = IDAssigner()
        self._phynetworkids = IDAssigner()
        self._physicalnetworkids = {}
        self._logicalportkeys = set()
        self._physicalportkeys = set()
        self._logicalnetworkkeys = set()
        self._physicalnetworkkeys = set()
        self._original_initialkeys = []
        self._append_initialkeys = []
        self._parent = parent
    def update_ports(self, ports, ovsdb_ports):
        self._portnames.clear()
        self._portnames.update((p['name'], _to32bitport(p['ofport'])) for p in ovsdb_ports)
        self._portids.clear()
        self._portids.update((p['id'], _to32bitport(p['ofport'])) for p in ovsdb_ports if p['id'])

        logicalportkeys = [LogicalPort.default_key(id) for id in self._portids]

        self._original_initialkeys = logicalportkeys + [PhysicalPortSet.default_key()]
        self._initialkeys = tuple(itertools.chain(self._original_initialkeys, self._append_initialkeys))
        self._walkerdict = dict(itertools.chain(
            ((PhysicalPortSet.default_key(),self._physicalport_walker),),
            ((lgportkey,self._logicalport_walker) for lgportkey in logicalportkeys)
        ))

        for m in self.restart_walk():
            yield m

    def _logicalport_walker(self, key, value, walk, save):
        _, (id,) = LogicalPort._getIndices(key)
        if id not in self._portids:
            return
        save(key)
        if value is None:
            return
        try:
            lognet = walk(value.network.getkey())
        except KeyError:
            pass
        else:
            save(lognet.getkey())
            try:
                phynet = walk(lognet.physicalnetwork.getkey())
            except KeyError:
                pass
            else:
                save(phynet.getkey())
        if hasattr(value,"subnet"):
            try:
                subnet = walk(value.subnet.getkey())
            except KeyError:
                pass
            else:
                save(subnet.getkey())
                if hasattr(subnet,"router"):
                    try:
                        routerport = walk(subnet.router.getkey())
                    except KeyError:
                        pass
                    else:
                        save(routerport.getkey())
                        if hasattr(routerport,"router"):
                            try:
                                router = walk(routerport.router.getkey())
                            except KeyError:
                                pass
                            else:
                                save(router.getkey())
                                if router.interfaces.dataset():
                                    for weakobj in router.interfaces.dataset():
                                        try:
                                            weakrouterport = walk(weakobj.getkey())
                                        except KeyError:
                                            pass
                                        else:
                                            save(weakrouterport.getkey())
                                            try:
                                                s = walk(weakrouterport.subnet.getkey())
                                            except KeyError:
                                                pass
                                            else:
                                                save(s.getkey())
                                                try:
                                                    lgnet = walk(s.network.getkey())
                                                except KeyError:
                                                    pass
                                                else:
                                                    save(lgnet.getkey())
    def _physicalport_walker(self, key, value, walk, save):
        save(key)
        if value is None:
            return
        physet = value.set
        for name in self._portnames:
            phyports = physet.find(PhysicalPort, self._connection.protocol.vhost, self._systemid, self._bridgename, name)
            # There might be more than one match physical port rule for one port, pick the most specified one
            namedict = {}
            for p in phyports:
                _, inds = PhysicalPort._getIndices(p.getkey())
                name = inds[-1]
                ind_key = [i != '%' for i in inds]
                if name != '%':
                    if name in namedict:
                        if namedict[name][0] < ind_key:
                            namedict[name] = (ind_key, p)
                    else:
                        namedict[name] = (ind_key, p)
            phyports = [v[1] for v in namedict.values()]
            for p in phyports:
                try:
                    phyp = walk(p.getkey())
                except KeyError:
                    pass
                else:
                    save(phyp.getkey())
                    try:
                        phynet = walk(phyp.physicalnetwork.getkey())
                    except KeyError:
                        pass
                    else:
                        save(phynet.getkey())
                        if self._parent.enable_router_forward:
                            try:
                                phynetmap = walk(PhysicalNetworkMap.default_key(phynet.id))
                            except KeyError:
                                pass
                            else:
                                save(phynetmap.getkey())
                                for weak_lgnet in  phynetmap.logicnetworks.dataset():
                                    try:
                                        lgnet = walk(weak_lgnet.getkey())
                                    except KeyError:
                                        pass
                                    else:
                                        save(lgnet.getkey())

    def reset_initialkeys(self,keys,values):

        subnetkeys = [k for k,v in zip(keys,values) if v is not None and not v.isdeleted() and
                      v.isinstance(SubNet)]
        routerportkeys = [k for k,v in zip(keys,values) if v is not None and not v.isdeleted() and
                          v.isinstance(RouterPort)]
        portkeys = [k for k,v in zip(keys,values) if v is not None and not v.isdeleted() and
                    v.isinstance(VRouter)]
        self._append_initialkeys = subnetkeys + routerportkeys + portkeys
        self._initialkeys = tuple(itertools.chain(self._original_initialkeys, self._append_initialkeys))

    def walkcomplete(self, keys, values):
        conn = self._connection
        dpid = conn.openflow_datapathid
        vhost = conn.protocol.vhost
        _currentportids = dict(self._portids)
        _currentportnames = dict(self._portnames)
        updated_data = {}
        current_data = {}
        for cls, name, idg, assigner in ((LogicalPort, '_logicalportkeys', lambda x: _currentportids.get(x.id), None),
                                 (PhysicalPort, '_physicalportkeys', lambda x: _currentportnames.get(x.name), None),
                                 (LogicalNetwork, '_logicalnetworkkeys', lambda x: self._networkids.assign(x.getkey()), self._networkids),
                                 (PhysicalNetwork, '_physicalnetworkkeys', lambda x: self._phynetworkids.assign(x.getkey()), self._phynetworkids),
                                 ):
            objs = [v for v in values if v is not None and not v.isdeleted() and v.isinstance(cls)]
            cv = [(o, oid) for o,oid in ((o, idg(o)) for o in objs) if oid is not None]
            objkeys = set([v.getkey() for v,_ in cv])
            oldkeys = getattr(self, name)
            current_data[cls] = cv
            if objkeys != oldkeys:
                if assigner is not None:
                    assigner.unassign(oldkeys.difference(objkeys))
                setattr(self, name, objkeys)
                updated_data[cls] = True
        if updated_data:
            for m in self.waitForSend(DataObjectChanged(dpid, vhost, conn, LogicalPort in updated_data,
                                                                            PhysicalPort in updated_data,
                                                                            LogicalNetwork in updated_data,
                                                                            PhysicalNetwork in updated_data,
                                                                            current = (current_data.get(LogicalPort),
                                                                                       current_data.get(PhysicalPort),
                                                                                       current_data.get(LogicalNetwork),
                                                                                       current_data.get(PhysicalNetwork)))):
                yield m
        self._currentportids = _currentportids
        self._currentportnames = _currentportnames
    def updateflow(self, connection, addvalues, removevalues, updatedvalues):
        # We must do these in order, each with a batch:
        # 1. Remove flows
        # 2. Remove groups
        # 3. Add groups, modify groups
        # 4. Add flows, modify flows
        try:
            cmds = []
            ofdef = connection.openflowdef
            vhost = connection.protocol.vhost
            input_table = self._parent._gettableindex('ingress', vhost)
            input_next = self._parent._getnexttable('', 'ingress', vhost = vhost)
            output_table = self._parent._gettableindex('egress', vhost)
            # Cache all IDs, save them into last. We will need them for remove.
            _lastportids = self._lastportids
            _lastportnames = self._lastportnames
            _lastnetworkids = self._lastnetworkids
            _portids = dict(self._currentportids)
            _portnames = dict(self._currentportnames)
            _networkids = self._networkids.frozen()
            exist_objs = dict((obj.getkey(), obj) for obj in self._savedresult if obj is not None and not obj.isdeleted())
            # We must generate actions from network driver
            phyportset = [obj for obj in self._savedresult if obj is not None and not obj.isdeleted() and obj.isinstance(PhysicalPort)]
            phynetset = [obj for obj in self._savedresult if obj is not None and not obj.isdeleted() and obj.isinstance(PhysicalNetwork)]
            lognetset = [obj for obj in self._savedresult if obj is not None and not obj.isdeleted() and obj.isinstance(LogicalNetwork)]
            logportset = [obj for obj in self._savedresult if obj is not None and not obj.isdeleted() and obj.isinstance(LogicalPort)]
            # If a port is both a logical port and a physical port, flows may conflict.
            # Remove the port from dictionary if it is duplicated.
            logportofps = set(_portids[lp.id] for lp in logportset if lp.id in _portids)
            _portnames = dict((n,v) for n,v in _portnames.items() if v not in logportofps)
            self._lastportids = _portids
            self._lastportnames = _portnames
            self._lastnetworkids = _networkids
            # Group current ports by network for further use
            phyportdict = {}
            for p in phyportset:
                phyportdict.setdefault(p.physicalnetwork, []).append(p)
            lognetdict = {}
            for n in lognetset:
                lognetdict.setdefault(n.physicalnetwork, []).append(n)
            logportdict = {}
            for p in logportset:
                logportdict.setdefault(p.network, []).append(p)
            allapis = []
            # Updated networks when:
            # 1. Network is updated
            # 2. Physical network of this logical network is updated
            # 3. Logical port is added or removed from the network
            # 4. Physical port is added or removed from the physical network
            group_updates = set([obj for obj in updatedvalues if obj.isinstance(LogicalNetwork)])
            group_updates.update(obj.network for obj in addvalues if obj.isinstance(LogicalPort))
            #group_updates.update(obj.network for obj in updatedvalues if obj.isinstance(LogicalPort))
            group_updates.update(exist_objs[obj.network.getkey()] for obj in removevalues if obj.isinstance(LogicalPort) and obj.network.getkey() in exist_objs)
            updated_physicalnetworks = set(obj for obj in updatedvalues if obj.isinstance(PhysicalNetwork))
            updated_physicalnetworks.update(p.physicalnetwork for p in addvalues if p.isinstance(PhysicalPort))
            updated_physicalnetworks.update(exist_objs[p.physicalnetwork.getkey()] for p in removevalues if p.isinstance(PhysicalPort) and p.physicalnetwork.getkey() in exist_objs)
            updated_physicalnetworks.update(p.physicalnetwork for p in updatedvalues if p.isinstance(PhysicalPort))
            group_updates.update(lnet for pnet in updated_physicalnetworks
                                 if pnet in lognetdict
                                 for lnet in lognetdict[pnet])
            for pnet in phynetset:
                if pnet in lognetdict and pnet in phyportdict:
                    for lognet in lognetdict[pnet]:
                        netid = _networkids.get(lognet.getkey())
                        if netid is not None:
                            for p in phyportdict[pnet]:
                                if lognet in addvalues or lognet in group_updates or p in addvalues or p in updatedvalues:
                                    pid = _portnames.get(p.name)
                                    if pid is not None:
                                        def subr(lognet, p, netid, pid):
                                            try:
                                                for m in callAPI(self, 'public', 'createioflowparts', {'connection': connection,
                                                                                                       'logicalnetwork': lognet,
                                                                                                       'physicalport': p,
                                                                                                       'logicalnetworkid': netid,
                                                                                                       'physicalportid': pid}):
                                                    yield m
                                            except Exception:
                                                self._parent._logger.warning("Create flow parts failed for %r and %r", lognet, p, exc_info = True)
                                                self.retvalue = None
                                            else:
                                                self.retvalue = ((lognet, p), self.retvalue)
                                        allapis.append(subr(lognet, p, netid, pid))
            for m in self.executeAll(allapis):
                yield m
            flowparts = dict(r[0] for r in self.retvalue if r[0] is not None)
            if connection.protocol.disablenxext:
                # Nicira extension is disabled, use metadata instead
                # 64-bit metadata is used as:
                # | 16-bit input network | 16-bit output network | 16-bit reserved | 16-bit output port |
                # When first initialized, input network = output network = Logical Network no.
                # output port = OFPP_ANY, reserved bits are 0x0000
                # Currently used reserved bits:
                # left-most (offset = 15, mask = 0x8000): allow output to IN_PORT
                # offset = 14, mask = 0x4000: 1 if is IN_PORT is a logical port, 0 else
                # right-most (offset = 0, mask = 0x0001): VXLAN learned
                def create_input_instructions(lognetid, extra_actions, is_logport):
                    lognetid = (lognetid & 0xffff)
                    instructions = [ofdef.ofp_instruction_write_metadata(
                                        metadata = (lognetid << 48) | (lognetid << 32) | ((0x4000 if is_logport else 0) << 16) | (ofdef.OFPP_ANY & 0xffff),
                                        metadata_mask = 0xffffffffffffffff
                                    ),
                                    ofdef.ofp_instruction_goto_table(table_id = input_next)
                                    ]
                    if extra_actions:
                        instructions.insert(0, ofdef.ofp_instruction_actions(actions = list(extra_actions)))
                    return instructions
                def create_output_oxm(lognetid, portid, in_port = False):
                    r = [ofdef.create_oxm(ofdef.OXM_OF_METADATA_W, (portid & 0xFFFF) | (0x80000000 if in_port else 0) | ((lognetid & 0xFFFF) << 32), 0x0000FFFF8000FFFF)]
                    if in_port:
                        r.append(ofdef.create_oxm(ofdef.OXM_OF_IN_PORT, portid))
                    return r
            else:
                # With nicira extension, we store input network, output network and output port in REG4, REG5 and REG6
                # REG7 is used as the reserved bits
                def create_input_instructions(lognetid, extra_actions, is_logport):
                    lognetid = (lognetid & 0xffff)
                    return [ofdef.ofp_instruction_actions(actions = [
                                    ofdef.ofp_action_set_field(
                                            field = ofdef.create_oxm(ofdef.NXM_NX_REG4, lognetid)
                                            ),
                                    ofdef.ofp_action_set_field(
                                            field = ofdef.create_oxm(ofdef.NXM_NX_REG5, lognetid)
                                            ),
                                    ofdef.ofp_action_set_field(
                                            field = ofdef.create_oxm(ofdef.NXM_NX_REG6, ofdef.OFPP_ANY)
                                            ),
                                    ofdef.ofp_action_set_field(
                                            field = ofdef.create_oxm(ofdef.NXM_NX_REG7, (0x4000 if is_logport else 0))
                                            )
                                ] + list(extra_actions)),
                            ofdef.ofp_instruction_goto_table(table_id = input_next)
                            ]
                def create_output_oxm(lognetid, portid, in_port = False):
                    r = [ofdef.create_oxm(ofdef.NXM_NX_REG5, lognetid),
                            ofdef.create_oxm(ofdef.NXM_NX_REG6, portid),
                            ofdef.create_oxm(ofdef.NXM_NX_REG7_W, 0x8000 if in_port else 0, 0x8000)]
                    if in_port:
                        r.append(ofdef.create_oxm(ofdef.OXM_OF_IN_PORT, portid))
                    return r
            for obj in removevalues:
                if obj.isinstance(LogicalPort):
                    ofport = _lastportids.get(obj.id)
                    if ofport is not None:
                        cmds.append(ofdef.ofp_flow_mod(table_id = input_table,
                                                       command = ofdef.OFPFC_DELETE,
                                                       priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                       buffer_id = ofdef.OFP_NO_BUFFER,
                                                       out_port = ofdef.OFPP_ANY,
                                                       out_group = ofdef.OFPG_ANY,
                                                       match = ofdef.ofp_match_oxm(oxm_fields = [
                                                                ofdef.create_oxm(ofdef.OXM_OF_IN_PORT,
                                                                                 ofport
                                                                                 )])
                                                       ))
                        cmds.append(ofdef.ofp_flow_mod(table_id = output_table,
                                                       command = ofdef.OFPFC_DELETE,
                                                       priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                       buffer_id = ofdef.OFP_NO_BUFFER,
                                                       out_port = ofport,
                                                       out_group = ofdef.OFPG_ANY,
                                                       match = ofdef.ofp_match_oxm()))
                        cmds.append(ofdef.ofp_flow_mod(table_id = output_table,
                                                       command = ofdef.OFPFC_DELETE,
                                                       priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                       buffer_id = ofdef.OFP_NO_BUFFER,
                                                       out_port = ofdef.OFPP_IN_PORT,
                                                       out_group = ofdef.OFPG_ANY,
                                                       match = ofdef.ofp_match_oxm(oxm_fields = [
                                                                    ofdef.create_oxm(ofdef.OXM_OF_IN_PORT, ofport)])))
                elif obj.isinstance(PhysicalPort):
                    ofport = _lastportnames.get(obj.name)
                    if ofport is not None:
                        cmds.append(ofdef.ofp_flow_mod(table_id = input_table,
                                                       command = ofdef.OFPFC_DELETE,
                                                       priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                       buffer_id = ofdef.OFP_NO_BUFFER,
                                                       out_port = ofdef.OFPP_ANY,
                                                       out_group = ofdef.OFPG_ANY,
                                                       match = ofdef.ofp_match_oxm(oxm_fields = [
                                                                ofdef.create_oxm(ofdef.OXM_OF_IN_PORT,
                                                                                 ofport
                                                                                 )])
                                                       ))
                        cmds.append(ofdef.ofp_flow_mod(table_id = output_table,
                                                       cookie = 0x0001000000000000 | ((ofport & 0xffff) << 16),
                                                       cookie_mask = 0xffffffffffff0000,
                                                       command = ofdef.OFPFC_DELETE,
                                                       priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                       buffer_id = ofdef.OFP_NO_BUFFER,
                                                       out_port = ofdef.OFPP_ANY,
                                                       out_group = ofdef.OFPG_ANY,
                                                       match = ofdef.ofp_match_oxm()))
                elif obj.isinstance(LogicalNetwork):
                    groupid = _lastnetworkids.get(obj.getkey())
                    if groupid is not None:
                        cmds.append(ofdef.ofp_flow_mod(table_id = input_table,
                                                       cookie = 0x0001000000000000 | groupid,
                                                       cookie_mask = 0xffffffffffffffff,
                                                       command = ofdef.OFPFC_DELETE,
                                                       priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                       buffer_id = ofdef.OFP_NO_BUFFER,
                                                       out_port = ofdef.OFPP_ANY,
                                                       out_group = ofdef.OFPG_ANY,
                                                       match = ofdef.ofp_match_oxm()
                                                       ))
                        cmds.append(ofdef.ofp_flow_mod(table_id = output_table,
                                                       cookie = 0x0001000000000000 | groupid,
                                                       cookie_mask = 0xffff00000000ffff,
                                                       command = ofdef.OFPFC_DELETE,
                                                       priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                       buffer_id = ofdef.OFP_NO_BUFFER,
                                                       out_port = ofdef.OFPP_ANY,
                                                       out_group = ofdef.OFPG_ANY,
                                                       match = ofdef.ofp_match_oxm()
                                                       ))
                        cmds.append(ofdef.ofp_flow_mod(table_id = output_table,
                                                       command = ofdef.OFPFC_DELETE,
                                                       priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                       buffer_id = ofdef.OFP_NO_BUFFER,
                                                       out_port = ofdef.OFPP_ANY,
                                                       out_group = ofdef.OFPG_ANY,
                                                       match = ofdef.ofp_match_oxm(oxm_fields = create_output_oxm(groupid, ofdef.OFPP_ANY))
                                                       ))
            # Never use flow mod to update an input flow of physical port, because the input_oxm may change.
            for obj in updatedvalues:
                if obj.isinstance(PhysicalPort):
                    ofport = _portnames.get(obj.name)
                    if ofport is not None:
                        cmds.append(ofdef.ofp_flow_mod(table_id = input_table,
                                                       command = ofdef.OFPFC_DELETE,
                                                       priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                       buffer_id = ofdef.OFP_NO_BUFFER,
                                                       out_port = ofdef.OFPP_ANY,
                                                       out_group = ofdef.OFPG_ANY,
                                                       match = ofdef.ofp_match_oxm(oxm_fields = [
                                                                ofdef.create_oxm(ofdef.OXM_OF_IN_PORT,
                                                                                 ofport
                                                                                 )])
                                                       ))
                elif obj.isinstance(LogicalNetwork):
                    groupid = _networkids.get(obj.getkey())
                    if groupid is not None:
                        cmds.append(ofdef.ofp_flow_mod(table_id = input_table,
                                                       cookie = 0x0001000000000000 | groupid,
                                                       cookie_mask = 0xffffffffffffffff,
                                                       command = ofdef.OFPFC_DELETE,
                                                       priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                       buffer_id = ofdef.OFP_NO_BUFFER,
                                                       out_port = ofdef.OFPP_ANY,
                                                       out_group = ofdef.OFPG_ANY,
                                                       match = ofdef.ofp_match_oxm()
                                                       ))
                elif obj.isinstance(PhysicalNetwork):
                    if obj in phyportdict:
                        for p in phyportdict[obj]:
                            ofport = _portnames.get(p.name)
                            if ofport is not None and p not in addvalues and p not in updatedvalues:
                                cmds.append(ofdef.ofp_flow_mod(table_id = input_table,
                                                               command = ofdef.OFPFC_DELETE,
                                                               priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                               buffer_id = ofdef.OFP_NO_BUFFER,
                                                               out_port = ofdef.OFPP_ANY,
                                                               out_group = ofdef.OFPG_ANY,
                                                               match = ofdef.ofp_match_oxm(oxm_fields = [
                                                                        ofdef.create_oxm(ofdef.OXM_OF_IN_PORT,
                                                                                         ofport
                                                                                         )])
                                                               ))
            for m in self.execute_commands(connection, cmds):
                yield m
            del cmds[:]
            for obj in removevalues:
                if obj.isinstance(LogicalNetwork):
                    groupid = _lastnetworkids.get(obj.getkey())
                    if groupid is not None:
                        cmds.append(ofdef.ofp_group_mod(command = ofdef.OFPGC_DELETE,
                                                        type = ofdef.OFPGT_ALL,
                                                        group_id = groupid
                                                        ))
            for m in self.execute_commands(connection, cmds):
                yield m
            del cmds[:]
            disablechaining = connection.protocol.disablechaining
            created_groups = {}
            def create_buckets(obj, groupid):
                # Generate buckets
                buckets = [ofdef.ofp_bucket(actions=[ofdef.ofp_action_output(port = _portids[p.id])])
                           for p in logportdict[obj]
                           if p.id in _portids] if obj in logportdict else []
                allactions = [ofdef.ofp_action_output(port = _portids[p.id])
                              for p in logportdict[obj]
                              if p.id in _portids] if obj in logportdict else []
                disablegroup = False
                if obj.physicalnetwork in phyportdict:
                    for p in phyportdict[obj.physicalnetwork]:
                        if (obj, p) in flowparts:
                            fp = flowparts[(obj,p)]
                            allactions.extend(fp[3])
                            if disablechaining and not disablegroup and any(a.type == ofdef.OFPAT_GROUP for a in fp[3]):
                                # We cannot use chaining. We use a long action list instead, and hope there is no conflicts
                                disablegroup = True
                            else:
                                buckets.append(ofdef.ofp_bucket(actions=list(fp[3])))
                if disablegroup:
                    created_groups[groupid] = allactions
                else:
                    created_groups[groupid] = [ofdef.ofp_action_group(group_id = groupid)]
                return buckets
            for obj in addvalues:
                if obj.isinstance(LogicalNetwork):
                    groupid = _networkids.get(obj.getkey())
                    if groupid is not None:
                        cmds.append(ofdef.ofp_group_mod(command = ofdef.OFPGC_ADD,
                                                        type = ofdef.OFPGT_ALL,
                                                        group_id = groupid,
                                                        buckets = create_buckets(obj, groupid)
                                                        ))
            for obj in group_updates:
                groupid = _networkids.get(obj.getkey())
                if groupid is not None:
                    cmds.append(ofdef.ofp_group_mod(command = ofdef.OFPGC_MODIFY,
                                                    type = ofdef.OFPGT_ALL,
                                                    group_id = groupid,
                                                    buckets = create_buckets(obj, groupid)
                                                    ))
            for m in self.execute_commands(connection, cmds):
                yield m
            del cmds[:]
            # There are 5 kinds of flows:
            # 1. in_port = (Logical Port)
            # 2. in_port = (Physical_Port), network = (Logical_Network)
            # 3. out_port = (Logical Port)
            # 4. out_port = (Physical_Port), network = (Logical_Network)
            # 5. out_port = OFPP_ANY, network = (Logical_Network)
            for obj in addvalues:
                if obj.isinstance(LogicalPort):
                    ofport = _portids.get(obj.id)
                    lognetid = _networkids.get(obj.network.getkey())
                    if ofport is not None and lognetid is not None:
                        cmds.append(ofdef.ofp_flow_mod(table_id = input_table,
                                                       command = ofdef.OFPFC_ADD,
                                                       priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                       buffer_id = ofdef.OFP_NO_BUFFER,
                                                       out_port = ofdef.OFPP_ANY,
                                                       out_group = ofdef.OFPG_ANY,
                                                       match = ofdef.ofp_match_oxm(oxm_fields = [
                                                                ofdef.create_oxm(ofdef.OXM_OF_IN_PORT,
                                                                                 ofport
                                                                                 )]),
                                                       instructions = create_input_instructions(lognetid, [], True)
                                                       ))
                        cmds.append(ofdef.ofp_flow_mod(table_id = output_table,
                                                       command = ofdef.OFPFC_ADD,
                                                       priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                       buffer_id = ofdef.OFP_NO_BUFFER,
                                                       out_port = ofdef.OFPP_ANY,
                                                       out_group = ofdef.OFPG_ANY,
                                                       match = ofdef.ofp_match_oxm(oxm_fields = create_output_oxm(lognetid, ofport)),
                                                       instructions = [ofdef.ofp_instruction_actions(actions = [
                                                                    ofdef.ofp_action_output(port = ofport)
                                                                    ])]
                                                       ))
                        cmds.append(ofdef.ofp_flow_mod(table_id = output_table,
                                                       command = ofdef.OFPFC_ADD,
                                                       priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                       buffer_id = ofdef.OFP_NO_BUFFER,
                                                       out_port = ofdef.OFPP_ANY,
                                                       out_group = ofdef.OFPG_ANY,
                                                       match = ofdef.ofp_match_oxm(oxm_fields = create_output_oxm(lognetid, ofport, True)),
                                                       instructions = [ofdef.ofp_instruction_actions(actions = [
                                                                    ofdef.ofp_action_output(port = ofdef.OFPP_IN_PORT)
                                                                    ])]
                                                       ))
            # Ignore update of logical port
            # Physical port:
            for obj in addvalues:
                if obj.isinstance(PhysicalPort):
                    ofport = _portnames.get(obj.name)
                    if ofport is not None and obj.physicalnetwork in lognetdict:
                        for lognet in lognetdict[obj.physicalnetwork]:
                            lognetid = _networkids.get(lognet.getkey())
                            if lognetid is not None and (lognet, obj) in flowparts:
                                input_oxm, input_actions, output_actions, _, output_actions2 = flowparts[(lognet, obj)]
                                cmds.append(ofdef.ofp_flow_mod(table_id = input_table,
                                                               cookie = 0x0001000000000000 | lognetid,
                                                               cookie_mask = 0xffffffffffffffff,
                                                               command = ofdef.OFPFC_ADD,
                                                               priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                               buffer_id = ofdef.OFP_NO_BUFFER,
                                                               out_port = ofdef.OFPP_ANY,
                                                               out_group = ofdef.OFPG_ANY,
                                                               match = ofdef.ofp_match_oxm(oxm_fields = [
                                                                        ofdef.create_oxm(ofdef.OXM_OF_IN_PORT,
                                                                                         ofport
                                                                                         )] + list(input_oxm)),
                                                               instructions = create_input_instructions(lognetid, input_actions, False)
                                                               ))
                                cmds.append(ofdef.ofp_flow_mod(table_id = output_table,
                                                               cookie = 0x0001000000000000 | lognetid | ((ofport & 0xffff) << 16),
                                                               cookie_mask = 0xffffffffffffffff,
                                                               command = ofdef.OFPFC_ADD,
                                                               priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                               buffer_id = ofdef.OFP_NO_BUFFER,
                                                               out_port = ofdef.OFPP_ANY,
                                                               out_group = ofdef.OFPG_ANY,
                                                               match = ofdef.ofp_match_oxm(oxm_fields = create_output_oxm(lognetid, ofport, False)),
                                                               instructions = [ofdef.ofp_instruction_actions(actions = 
                                                                            list(output_actions))]
                                                               ))
                                cmds.append(ofdef.ofp_flow_mod(table_id = output_table,
                                                               cookie = 0x0001000000000000 | lognetid | ((ofport & 0xffff) << 16),
                                                               cookie_mask = 0xffffffffffffffff,
                                                               command = ofdef.OFPFC_ADD,
                                                               priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                               buffer_id = ofdef.OFP_NO_BUFFER,
                                                               out_port = ofdef.OFPP_ANY,
                                                               out_group = ofdef.OFPG_ANY,
                                                               match = ofdef.ofp_match_oxm(oxm_fields = create_output_oxm(lognetid, ofport, True)),
                                                               instructions = [ofdef.ofp_instruction_actions(actions = 
                                                                            list(output_actions2))]
                                                               ))
            for lognet in addvalues:
                if lognet.isinstance(LogicalNetwork):
                    lognetid = _networkids.get(lognet.getkey())
                    if lognetid is not None and lognet.physicalnetwork in phyportdict:
                        for obj in phyportdict[lognet.physicalnetwork]:
                            ofport = _portnames.get(obj.name)
                            if ofport is not None and (lognet, obj) in flowparts and obj not in addvalues:
                                input_oxm, input_actions, output_actions, _, output_actions2 = flowparts[(lognet, obj)]
                                cmds.append(ofdef.ofp_flow_mod(table_id = input_table,
                                                               cookie = 0x0001000000000000 | lognetid,
                                                               cookie_mask = 0xffffffffffffffff,
                                                               command = ofdef.OFPFC_ADD,
                                                               priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                               buffer_id = ofdef.OFP_NO_BUFFER,
                                                               out_port = ofdef.OFPP_ANY,
                                                               out_group = ofdef.OFPG_ANY,
                                                               match = ofdef.ofp_match_oxm(oxm_fields = [
                                                                        ofdef.create_oxm(ofdef.OXM_OF_IN_PORT,
                                                                                         ofport
                                                                                         )] + input_oxm),
                                                               instructions = create_input_instructions(lognetid, input_actions, False)
                                                               ))
                                cmds.append(ofdef.ofp_flow_mod(table_id = output_table,
                                                               cookie = 0x0001000000000000 | lognetid | ((ofport & 0xffff) << 16),
                                                               cookie_mask = 0xffffffffffffffff,
                                                               command = ofdef.OFPFC_ADD,
                                                               priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                               buffer_id = ofdef.OFP_NO_BUFFER,
                                                               out_port = ofdef.OFPP_ANY,
                                                               out_group = ofdef.OFPG_ANY,
                                                               match = ofdef.ofp_match_oxm(oxm_fields = create_output_oxm(lognetid, ofport, False)),
                                                               instructions = [ofdef.ofp_instruction_actions(actions = 
                                                                            list(output_actions))]
                                                               ))
                                cmds.append(ofdef.ofp_flow_mod(table_id = output_table,
                                                               cookie = 0x0001000000000000 | lognetid | ((ofport & 0xffff) << 16),
                                                               cookie_mask = 0xffffffffffffffff,
                                                               command = ofdef.OFPFC_ADD,
                                                               priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                               buffer_id = ofdef.OFP_NO_BUFFER,
                                                               out_port = ofdef.OFPP_ANY,
                                                               out_group = ofdef.OFPG_ANY,
                                                               match = ofdef.ofp_match_oxm(oxm_fields = create_output_oxm(lognetid, ofport, True)),
                                                               instructions = [ofdef.ofp_instruction_actions(actions = 
                                                                            list(output_actions2))]
                                                               ))
            for obj in updatedvalues:
                if obj.isinstance(PhysicalPort):
                    ofport = _portnames.get(obj.name)
                    if ofport is not None and obj.physicalnetwork in lognetdict:
                        for lognet in lognetdict[obj.physicalnetwork]:
                            lognetid = _networkids.get(lognet.getkey())
                            if lognetid is not None and (lognet, obj) in flowparts and not lognet in addvalues:
                                input_oxm, input_actions, output_actions, _, output_actions2 = flowparts[(lognet, obj)]
                                cmds.append(ofdef.ofp_flow_mod(table_id = input_table,
                                                               cookie = 0x0001000000000000 | lognetid,
                                                               cookie_mask = 0xffffffffffffffff,
                                                               command = ofdef.OFPFC_ADD,
                                                               priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                               buffer_id = ofdef.OFP_NO_BUFFER,
                                                               out_port = ofdef.OFPP_ANY,
                                                               out_group = ofdef.OFPG_ANY,
                                                               match = ofdef.ofp_match_oxm(oxm_fields = [
                                                                        ofdef.create_oxm(ofdef.OXM_OF_IN_PORT,
                                                                                         ofport
                                                                                         )] + input_oxm),
                                                               instructions = create_input_instructions(lognetid, input_actions, False)
                                                               ))
                                cmds.append(ofdef.ofp_flow_mod(table_id = output_table,
                                                               cookie = 0x0001000000000000 | lognetid | ((ofport & 0xffff) << 16),
                                                               cookie_mask = 0xffffffffffffffff,
                                                               command = ofdef.OFPFC_MODIFY,
                                                               priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                               buffer_id = ofdef.OFP_NO_BUFFER,
                                                               out_port = ofdef.OFPP_ANY,
                                                               out_group = ofdef.OFPG_ANY,
                                                               match = ofdef.ofp_match_oxm(oxm_fields = create_output_oxm(lognetid, ofport, False)),
                                                               instructions = [ofdef.ofp_instruction_actions(actions = 
                                                                            list(output_actions))]
                                                               ))
                                cmds.append(ofdef.ofp_flow_mod(table_id = output_table,
                                                               cookie = 0x0001000000000000 | lognetid | ((ofport & 0xffff) << 16),
                                                               cookie_mask = 0xffffffffffffffff,
                                                               command = ofdef.OFPFC_MODIFY,
                                                               priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                               buffer_id = ofdef.OFP_NO_BUFFER,
                                                               out_port = ofdef.OFPP_ANY,
                                                               out_group = ofdef.OFPG_ANY,
                                                               match = ofdef.ofp_match_oxm(oxm_fields = create_output_oxm(lognetid, ofport, True)),
                                                               instructions = [ofdef.ofp_instruction_actions(actions = 
                                                                            list(output_actions2))]
                                                               ))
            for lognet in updatedvalues:
                if lognet.isinstance(LogicalNetwork):
                    lognetid = _networkids.get(lognet.getkey())
                    if lognetid is not None and lognet.physicalnetwork in phyportdict:
                        for obj in phyportdict[lognet.physicalnetwork]:
                            ofport = _portnames.get(obj.name)
                            if ofport is not None and (lognet, obj) in flowparts and obj not in addvalues and obj not in updatedvalues:
                                input_oxm, input_actions, output_actions, _, output_actions2 = flowparts[(lognet, obj)]
                                cmds.append(ofdef.ofp_flow_mod(table_id = input_table,
                                                               cookie = 0x0001000000000000 | lognetid,
                                                               cookie_mask = 0xffffffffffffffff,
                                                               command = ofdef.OFPFC_ADD,
                                                               priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                               buffer_id = ofdef.OFP_NO_BUFFER,
                                                               out_port = ofdef.OFPP_ANY,
                                                               out_group = ofdef.OFPG_ANY,
                                                               match = ofdef.ofp_match_oxm(oxm_fields = [
                                                                        ofdef.create_oxm(ofdef.OXM_OF_IN_PORT,
                                                                                         ofport
                                                                                         )] + input_oxm),
                                                               instructions = create_input_instructions(lognetid, input_actions, False)
                                                               ))
                                cmds.append(ofdef.ofp_flow_mod(table_id = output_table,
                                                               cookie = 0x0001000000000000 | lognetid | ((ofport & 0xffff) << 16),
                                                               cookie_mask = 0xffffffffffffffff,
                                                               command = ofdef.OFPFC_MODIFY,
                                                               priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                               buffer_id = ofdef.OFP_NO_BUFFER,
                                                               out_port = ofdef.OFPP_ANY,
                                                               out_group = ofdef.OFPG_ANY,
                                                               match = ofdef.ofp_match_oxm(oxm_fields = create_output_oxm(lognetid, ofport)),
                                                               instructions = [ofdef.ofp_instruction_actions(actions = 
                                                                            list(output_actions))]
                                                               ))
                                cmds.append(ofdef.ofp_flow_mod(table_id = output_table,
                                                               cookie = 0x0001000000000000 | lognetid | ((ofport & 0xffff) << 16),
                                                               cookie_mask = 0xffffffffffffffff,
                                                               command = ofdef.OFPFC_MODIFY,
                                                               priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                               buffer_id = ofdef.OFP_NO_BUFFER,
                                                               out_port = ofdef.OFPP_ANY,
                                                               out_group = ofdef.OFPG_ANY,
                                                               match = ofdef.ofp_match_oxm(oxm_fields = create_output_oxm(lognetid, ofport, True)),
                                                               instructions = [ofdef.ofp_instruction_actions(actions = 
                                                                            list(output_actions2))]
                                                               ))
            # Physical network is updated
            for pnet in updatedvalues:
                if pnet.isinstance(PhysicalNetwork) and pnet in lognetdict:
                    for lognet in lognetdict[pnet]:
                        if lognet.isinstance(LogicalNetwork):
                            lognetid = _networkids.get(lognet.getkey())
                            if lognetid is not None and lognet not in updatedvalues and lognet not in addvalues and lognet.physicalnetwork in phyportdict:
                                for obj in phyportdict[lognet.physicalnetwork]:
                                    ofport = _portnames.get(obj.name)
                                    if ofport is not None and (lognet, obj) in flowparts and obj not in addvalues and obj not in updatedvalues:
                                        input_oxm, input_actions, output_actions, _, output_actions2 = flowparts[(lognet, obj)]
                                        cmds.append(ofdef.ofp_flow_mod(table_id = input_table,
                                                                       cookie = 0x0001000000000000 | lognetid,
                                                                       cookie_mask = 0xffffffffffffffff,
                                                                       command = ofdef.OFPFC_ADD,
                                                                       priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                                       buffer_id = ofdef.OFP_NO_BUFFER,
                                                                       out_port = ofdef.OFPP_ANY,
                                                                       out_group = ofdef.OFPG_ANY,
                                                                       match = ofdef.ofp_match_oxm(oxm_fields = [
                                                                                ofdef.create_oxm(ofdef.OXM_OF_IN_PORT,
                                                                                                 ofport
                                                                                                 )] + input_oxm),
                                                                       instructions = create_input_instructions(lognetid, input_actions, False)
                                                                       ))
                                        cmds.append(ofdef.ofp_flow_mod(table_id = output_table,
                                                                       cookie = 0x0001000000000000 | lognetid | ((ofport & 0xffff) << 16),
                                                                       cookie_mask = 0xffffffffffffffff,
                                                                       command = ofdef.OFPFC_MODIFY,
                                                                       priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                                       buffer_id = ofdef.OFP_NO_BUFFER,
                                                                       out_port = ofdef.OFPP_ANY,
                                                                       out_group = ofdef.OFPG_ANY,
                                                                       match = ofdef.ofp_match_oxm(oxm_fields = create_output_oxm(lognetid, ofport)),
                                                                       instructions = [ofdef.ofp_instruction_actions(actions = 
                                                                                    list(output_actions))]
                                                                       ))
                                        cmds.append(ofdef.ofp_flow_mod(table_id = output_table,
                                                                       cookie = 0x0001000000000000 | lognetid | ((ofport & 0xffff) << 16),
                                                                       cookie_mask = 0xffffffffffffffff,
                                                                       command = ofdef.OFPFC_MODIFY,
                                                                       priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                                       buffer_id = ofdef.OFP_NO_BUFFER,
                                                                       out_port = ofdef.OFPP_ANY,
                                                                       out_group = ofdef.OFPG_ANY,
                                                                       match = ofdef.ofp_match_oxm(oxm_fields = create_output_oxm(lognetid, ofport, True)),
                                                                       instructions = [ofdef.ofp_instruction_actions(actions = 
                                                                                    list(output_actions2))]
                                                                       ))
            # Logical network broadcast
            for lognet in addvalues:
                if lognet.isinstance(LogicalNetwork):
                    lognetid = _networkids.get(lognet.getkey())
                    if lognetid is not None and lognetid in created_groups:
                        cmds.append(ofdef.ofp_flow_mod(table_id = output_table,
                                                       command = ofdef.OFPFC_ADD,
                                                       priority = ofdef.OFP_DEFAULT_PRIORITY,
                                                       buffer_id = ofdef.OFP_NO_BUFFER,
                                                       out_port = ofdef.OFPP_ANY,
                                                       out_group = ofdef.OFPG_ANY,
                                                       match = ofdef.ofp_match_oxm(oxm_fields = create_output_oxm(lognetid, ofdef.OFPP_ANY)),
                                                       instructions = [ofdef.ofp_instruction_actions(actions =
                                                                            created_groups.pop(lognetid))]
                                                       ))
            for lognetid, actions in created_groups.items():
                cmds.append(ofdef.ofp_flow_mod(table_id = output_table,
                                               command = ofdef.OFPFC_ADD,
                                               priority = ofdef.OFP_DEFAULT_PRIORITY,
                                               buffer_id = ofdef.OFP_NO_BUFFER,
                                               out_port = ofdef.OFPP_ANY,
                                               out_group = ofdef.OFPG_ANY,
                                               match = ofdef.ofp_match_oxm(oxm_fields = create_output_oxm(lognetid, ofdef.OFPP_ANY)),
                                               instructions = [ofdef.ofp_instruction_actions(actions = actions)]
                                               ))                
            # Ignore logical network update
            for m in self.execute_commands(connection, cmds):
                yield m
        except Exception:
            self._parent._logger.warning("Update flow for connection %r failed with exception", connection, exc_info = True)
            # We don't want the whole flow update stops, so ignore the exception and continue
    
@defaultconfig
@depend(ofpportmanager.OpenflowPortManager, ovsdbportmanager.OVSDBPortManager, objectdb.ObjectDB)
class IOProcessing(FlowBase):
    "Ingress and Egress processing"
    _tablerequest = (("ingress", (), ''),
                     ("egress", ("ingress",),''))
    # vHost map from OpenFlow vHost to OVSDB vHost. If the OpenFlow vHost is not found in this map,
    # it will map to the default OVSDB vHost ('')
    _default_vhostmap = {}
    # Enable forwarding in this server, so it becomes a forwarding node (also known as a N/S gateway)
    _default_enable_router_forward = False

    def __init__(self, server):
        FlowBase.__init__(self, server)
        self.apiroutine = RoutineContainer(self.scheduler)
        self.apiroutine.main = self._main
        self.routines.append(self.apiroutine)
        self._flowupdaters = {}
        self._portchanging = set()
        self._portchanged = set()
    def _main(self):
        flow_init = FlowInitialize.createMatcher(_ismatch = lambda x: self.vhostbind is None or x.vhost in self.vhostbind)
        port_change = ModuleNotification.createMatcher("openflowportmanager", "update", _ismatch = lambda x: self.vhostbind is None or x.vhost in self.vhostbind)
        while True:
            yield (flow_init, port_change)
            e = self.apiroutine.event
            c = e.connection
            if self.apiroutine.matcher is flow_init:
                self.apiroutine.subroutine(self._init_conn(self.apiroutine.event.connection))
            else:
                if self.apiroutine.event.reason == 'disconnected':
                    self.apiroutine.subroutine(self._remove_conn(c))
                else:
                    self.apiroutine.subroutine(self._portchange(c))
    def _init_conn(self, conn):
        # Default drop
        for m in conn.protocol.batch((conn.openflowdef.ofp_flow_mod(table_id = self._gettableindex("ingress", conn.protocol.vhost),
                                                           command = conn.openflowdef.OFPFC_ADD,
                                                           priority = 0,
                                                           buffer_id = conn.openflowdef.OFP_NO_BUFFER,
                                                           match = conn.openflowdef.ofp_match_oxm(),
                                                           instructions = [conn.openflowdef.ofp_instruction_actions(
                                                                            type = conn.openflowdef.OFPIT_CLEAR_ACTIONS
                                                                            )]
                                                           ),
                                      conn.openflowdef.ofp_flow_mod(table_id = self._gettableindex("egress", conn.protocol.vhost),
                                                           command = conn.openflowdef.OFPFC_ADD,
                                                           priority = 0,
                                                           buffer_id = conn.openflowdef.OFP_NO_BUFFER,
                                                           match = conn.openflowdef.ofp_match_oxm(),
                                                           instructions = [conn.openflowdef.ofp_instruction_actions(
                                                                            type = conn.openflowdef.OFPIT_CLEAR_ACTIONS
                                                                            )]
                                                           )), conn, self.apiroutine):
            yield m
        if conn in self._flowupdaters:
            self._flowupdaters[conn].close()
        datapath_id = conn.openflow_datapathid
        ovsdb_vhost = self.vhostmap.get(conn.protocol.vhost, "")
        for m in callAPI(self.apiroutine, 'ovsdbmanager', 'waitbridgeinfo', {'datapathid': datapath_id,
                                                                            'vhost': ovsdb_vhost}):
            yield m
        bridgename, systemid, _ = self.apiroutine.retvalue            
        new_updater = IOFlowUpdater(conn, systemid, bridgename, self)
        self._flowupdaters[conn] = new_updater
        new_updater.start()
        for m in self._portchange(conn):
            yield m
    def _remove_conn(self, conn):
        # Do not need to modify flows
        if conn in self._flowupdaters:
            self._flowupdaters[conn].close()
            del self._flowupdaters[conn]
        if False:
            yield
    def _portchange(self, conn):
        # Do not re-enter
        if conn in self._portchanging:
            self._portchanged.add(conn)
            return
        self._portchanging.add(conn)
        try:
            while True:
                self._portchanged.discard(conn)
                flow_updater = self._flowupdaters.get(conn)
                if flow_updater is None:
                    break
                datapath_id = conn.openflow_datapathid
                ovsdb_vhost = self.vhostmap.get(conn.protocol.vhost, "")
                for m in callAPI(self.apiroutine, 'openflowportmanager', 'getports', {'datapathid': datapath_id,
                                                                                      'vhost': conn.protocol.vhost}):
                    yield m
                ports = self.apiroutine.retvalue
                if conn in self._portchanged:
                    continue
                if not conn.connected:
                    self._portchanged.discard(conn)
                    return
                def ovsdb_info():
                    resync = 0
                    while True:
                        try:
                            if conn in self._portchanged:
                                self.apiroutine.retvalue = None
                                return
                            for m in self.apiroutine.executeAll([callAPI(self.apiroutine, 'ovsdbportmanager', 'waitportbyno', {'datapathid': datapath_id,
                                                                                                   'vhost': ovsdb_vhost,
                                                                                                   'portno': p.port_no,
                                                                                                   'timeout': 5 + 5 * min(resync, 5)
                                                                                                   })
                                                                 for p in ports]):
                                yield m
                        except StopIteration:
                            break
                        except Exception:
                            self._logger.warning('Cannot retrieve port info from OVSDB for datapathid %016x, vhost = %r', datapath_id, ovsdb_vhost, exc_info = True)
                            for m in callAPI(self.apiroutine, 'ovsdbmanager', 'getconnection', {'datapathid': datapath_id,
                                                                                                'vhost': ovsdb_vhost}):
                                yield m
                            if self.apiroutine.retvalue is None:
                                self._logger.warning("OVSDB connection may not be ready for datapathid %016x, vhost = %r", datapath_id, ovsdb_vhost)
                                trytimes = 0
                                while True:
                                    try:
                                        for m in callAPI(self.apiroutine, 'ovsdbmanager', 'waitconnection', {'datapathid': datapath_id,
                                                                                                             'vhost': ovsdb_vhost}):
                                            yield m
                                    except Exception:
                                        trytimes += 1
                                        if trytimes > 10:
                                            self._logger.warning("OVSDB connection is still not ready after a long time for %016x, vhost = %r. Keep waiting...", datapath_id, ovsdb_vhost)
                                            trytimes = 0
                                    else:
                                        break
                            else:
                                self._logger.warning('OpenFlow ports may not be synchronized. Try resync...')
                                # Connection is up but ports are not synchronized, try resync
                                for m in self.apiroutine.executeAll([callAPI(self.apiroutine, 'openflowportmanager', 'resync',
                                                                             {'datapathid': datapath_id,
                                                                              'vhost': conn.protocol.vhost}),
                                                                     callAPI(self.apiroutine, 'ovsdbportmanager', 'resync',
                                                                             {'datapathid': datapath_id,
                                                                              'vhost': ovsdb_vhost})]):
                                    yield m
                                # Do not resync too often
                                for m in self.apiroutine.waitWithTimeout(0.1):
                                    yield m
                                resync += 1
                        else:
                            break
                    self.apiroutine.retvalue = [r[0] for r in self.apiroutine.retvalue]
                conn_down = conn.protocol.statematcher(conn)
                try:
                    for m in self.apiroutine.withException(ovsdb_info(), conn_down):
                        yield m
                except RoutineException:
                    self._portchanged.discard(conn)
                    return
                if conn in self._portchanged:
                    continue
                ovsdb_ports = self.apiroutine.retvalue
                flow_updater = self._flowupdaters.get(conn)
                if flow_updater is None:
                    break
                for m in flow_updater.update_ports(ports, ovsdb_ports):
                    yield m
                if conn not in self._portchanged:
                    break
        finally:
            self._portchanging.remove(conn)

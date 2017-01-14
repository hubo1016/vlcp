from vlcp.config import defaultconfig
from vlcp.event import Event
from vlcp.event import RoutineContainer
from vlcp.event import withIndices
from vlcp.protocol.openflow import OpenflowConnectionStateEvent
from vlcp.server.module import callAPI, publicapi, api
from vlcp.service.sdn import ioprocessing
from vlcp.service.sdn import ovsdbportmanager
from vlcp.service.sdn.flowbase import FlowBase
from vlcp.service.sdn.ofpmanager import FlowInitialize
from vlcp.service.utils.remoteapi import remoteAPI
from vlcp.utils.ethernet import ETHERTYPE_8021Q


def _is_vxlan(obj):
    try:
        return obj.physicalnetwork.type == 'vxlan'
    except AttributeError:
        return False


@withIndices("connection","logicalnetworkid","type")
class VXLANMapChanged(Event):
    UPDATED = 'updated'
    DELETED = 'deleted'

@withIndices("connection","physname","phyiname","type")
class VtepControllerCall(Event):
    BIND = 'bind'
    UNBIND = "unbind"
    UNBINDALL = "unbindall"

class VXLANHandler(RoutineContainer):
    def __init__(self,connection,parent):
        super(VXLANHandler,self).__init__(connection.scheduler)
        self._conn = connection
        self._parent = parent

        self.vxlan_vlan_map_info = {}
        self.vlanid_pool = parent.vlanid_pool
        self._last_lognets_info = {}
        self._last_phyport_info = {}

        self._store_event = dict()
        self._store_bind_event = []
        self._store_unbind_event = []
        self._store_unbindall_event = []

        if self._parent.remote_api:
            self.api = remoteAPI
        else:
            self.api = callAPI

    def main(self):

        try:
            data_change_event = ioprocessing.DataObjectChanged.createMatcher(None, None, self._conn)

            # routine sync call to vtep controller (updatelogicalswitch,unbindlogicalswitch,unbindphysicalport)
            self.subroutine(self.action_handler(),True,"_action_handler")

            _last_change_event = [None]
            while True:
                yield (data_change_event,)
                _last_change_event[0] = self.event
                def _update_callback(event, matcher):
                    _last_change_event[0] = event
                def _update_loop():
                    while _last_change_event[0] is not None:
                        try:
                            last_lognets_info = self._last_lognets_info
                            last_phyport_info = self._last_phyport_info
        
                            current_logports,current_phyports,current_lognets,_ = _last_change_event[0].current
                            _last_change_event[0] = None
        
                            lognet_to_logports = {}
                            for p, _ in current_logports:
                                lognet_to_logports.setdefault(p.network, []).append(p.id)
        
                            datapath_id = self._conn.openflow_datapathid
                            vhost = self._conn.protocol.vhost
        
                            def get_phyport_info(portid):
                                try:
                                    for m in callAPI(self, "ovsdbportmanager", "waitportbyno",
                                                 {"datapathid": datapath_id,
                                                  "vhost": vhost,
                                                  "timeout": 1,
                                                  "portno": portid}):
                                        yield m
                                except ovsdbportmanager.OVSDBPortNotAppearException:
                                    self.retvalue = None
                            phy_ports = [p for p in current_phyports if _is_vxlan(p[0])]
                            for m in self.executeAll([get_phyport_info(pid) for p,pid in phy_ports]):
                                yield m
        
                            phy_port_info  = dict((k[0], (v[0]['external_ids']['vtep-physname'],
                                                                v[0]['external_ids']['vtep-phyiname']))
                                                  for k,v in zip(phy_ports,self.retvalue)
                                                  if v[0] is not None and \
                                                    'vtep-physname' in v[0]['external_ids'] and \
                                                    'vtep-phyiname' in v[0]['external_ids'])
                            unique_phyports = dict((p.physicalnetwork, p)
                                                   for p,_ in sorted(phy_ports, key = lambda x: x[1])
                                                   if p in phy_port_info)
        
                            lognet_to_phyport =  dict((n,unique_phyports[n.physicalnetwork])
                                                      for n,_ in current_lognets
                                                      if n.physicalnetwork in unique_phyports)
        
                            current_lognets_info = dict((n,(phy_port_info[v], lognet_to_logports[n]))
                                                        for n,v in lognet_to_phyport.items()
                                                        if n in lognet_to_logports)
        
                            self._last_lognets_info = current_lognets_info
                            self._last_phyport_info = phy_port_info
        
                            # add or update new phyport, unbind all physical interface bind
                            # must do it as init first ....
                            for p in phy_port_info:
                                if p not in last_phyport_info or\
                                        (p in last_phyport_info and last_phyport_info[p] != phy_port_info[p]):
        
                                    physname = phy_port_info[p][0]
                                    phyiname = phy_port_info[p][1]
        
                                    event = VtepControllerCall(connection=self._conn,type=VtepControllerCall.UNBINDALL,
                                                               physname=physname,phyiname=phyiname)
        
                                    for m in self.waitForSend(event):
                                        yield m
        
        
                            # remove phyport , unbind all physical interface bind also
                            for p in last_phyport_info:
                                if p not in phy_port_info or\
                                        (p in phy_port_info and last_phyport_info[p] != phy_port_info[p]):
                                    physname = last_phyport_info[p][0]
                                    phyiname = last_phyport_info[p][1]
        
                                    event = VtepControllerCall(connection=self._conn,type=VtepControllerCall.UNBINDALL,
                                                               physname=physname,phyiname=phyiname)
        
                                    for m in self.waitForSend(event):
                                        yield m
        
                            for n in last_lognets_info:
                                if n not in current_lognets_info:
                                    # means lognet removed
                                    # release vlan id
                                    # unbind physwitch
        
                                    # if last cause exception , n.id mybe not in vxlan_vlan_map_info
                                    if n.id in self.vxlan_vlan_map_info:
                                        vlan_id = self.vxlan_vlan_map_info[n.id]
                                        physname = last_lognets_info[n][0][0]
                                        phyiname = last_lognets_info[n][0][1]
        
                                        del self.vxlan_vlan_map_info[n.id]
        
                                        event = VXLANMapChanged(self._conn,n.id,VXLANMapChanged.DELETED)
        
                                        for m in self.waitForSend(event):
                                            yield m
        
                                        event = VtepControllerCall(connection=self._conn,logicalnetworkid=n.id,
                                                                   physname=physname,phyiname=phyiname,vlanid=vlan_id,
                                                                   type=VtepControllerCall.UNBIND)
        
                                        for m in self.waitForSend(event):
                                            yield m
                                
                                else:
                                    last_info = last_lognets_info[n]
                                    current_info = current_lognets_info[n]
        
                                    if last_info[0] != current_info[0]:
                                        # physwitch info have changed
                                        # unbind it
        
                                        if n.id in self.vxlan_vlan_map_info:
                                            vid = self.vxlan_vlan_map_info[n.id]
                                            physname = last_lognets_info[n][0][0]
                                            phyiname = last_lognets_info[n][0][1]
        
                                            del self.vxlan_vlan_map_info[n.id]
        
                                            event = VtepControllerCall(connection=self._conn, logicalnetworkid=n.id,
                                                                       physname=physname, phyiname=phyiname, vlanid=vid,
                                                                       type=VtepControllerCall.UNBIND)
        
                                            for m in self.waitForSend(event):
                                                yield m
                                           
                                    else:
                                        if last_info[1] != current_info[1]:
                                            add_ports = list(set(current_info[1]).difference(set(last_info[1])))
        
                                            if add_ports:
                                                # add new add_ports
                                                # update bind logical ports
        
                                                if n.id in self.vxlan_vlan_map_info:
                                                    vid = self.vxlan_vlan_map_info[n.id]
                                                    physname = last_lognets_info[n][0][0]
                                                    phyiname = last_lognets_info[n][0][1]
        
                                                    event = VtepControllerCall(connection=self._conn, logicalnetworkid=n.id,
                                                                               vni = n.vni,logicalports=add_ports,
                                                                               physname=physname, phyiname=phyiname, vlanid=vid,
                                                                               type=VtepControllerCall.BIND)
        
                                                    for m in self.waitForSend(event):
                                                        yield m
        
        
                            for n in current_lognets_info:
                                if n not in last_lognets_info:
                                    # means add lognet
                                    # find avaliable vlan id , send event
                                    # bind physwitch
                                    find = False
                                    _current_vlan_ids = set(self.vxlan_vlan_map_info.values())
                                    for start,end in self.vlanid_pool:
                                        for vid in range(start,end + 1):
                                            if vid not in _current_vlan_ids:
                                                find = True
                                                break
                                        if find:
                                            break
        
                                    if find:
                                        self.vxlan_vlan_map_info[n.id] = vid
        
                                        event = VXLANMapChanged(self._conn,n.id,VXLANMapChanged.UPDATED,vlan_id = vid)
                                        for m in self.waitForSend(event):
                                            yield m
        
                                        physname = current_lognets_info[n][0][0]
                                        phyiname = current_lognets_info[n][0][1]
                                        ports = current_lognets_info[n][1]
        
                                        event = VtepControllerCall(connection=self._conn, logicalnetworkid=n.id,
                                                                   vni=n.vni, logicalports=ports,
                                                                   physname=physname, phyiname=phyiname, vlanid=vid,
                                                                   type=VtepControllerCall.BIND)
        
                                        for m in self.waitForSend(event):
                                            yield m
        
                                    else:
                                        self._parent._logger.warning('Not enough vlan_id for logical network %r', n.id)
                                        event = VXLANMapChanged(self._conn,n.id,VXLANMapChanged.UPDATED,vlan_id = None)
                                        for m in self.waitForSend(event):
                                            yield m
                                else:
        
                                    last_info = last_lognets_info[n]
                                    current_info = current_lognets_info[n]
        
                                    if last_info[0] != current_info[0]:
                                        # physwitch info have changed
                                        # rebind it
                                        if n.id in self.vxlan_vlan_map_info:
                                            vid = self.vxlan_vlan_map_info[n.id]
                                            physname = current_lognets_info[n][0][0]
                                            phyiname = current_lognets_info[n][0][1]
                                            ports = current_lognets_info[n][1]
        
                                            event = VtepControllerCall(connection=self._conn, logicalnetworkid=n.id,
                                                                       vni=n.vni, logicalports=ports,
                                                                       physname=physname, phyiname=phyiname, vlanid=vid,
                                                                       type=VtepControllerCall.BIND)
        
                                            for m in self.waitForSend(event):
                                                yield m
        
        
                        except Exception:
                            self._parent._logger.info(" vxlan vtep handler exception , continue", exc_info = True)
                for m in self.withCallback(_update_loop(), _update_callback, data_change_event):
                    yield m
        finally:

            if hasattr(self,"_action_handler"):
                self._action_handler.close()


# ===============================================================================
#     def time_cycle(self):
#
#         while True:
#             for m in self.waitWithTimeout(self._parent.refreshinterval):
#                 yield m
#
#             for n,v in self._last_lognets_info.items():
#
#                 if n.id in self.vxlan_vlan_map_info:
#                     vid = self.vxlan_vlan_map_info[n.id]
#                     physname = v[0][0]
#                     phyiname = v[0][1]
#                     ports = v[1]
#
#                     event = VtepControllerCall(connection=self._conn, logicalnetworkid=n.id,
#                                                                vni=n.vni, logicalports=ports,
#                                                                physname=physname, phyiname=phyiname, vlanid=vid,
#                                                                type=VtepControllerCall.BIND)
#                     self.scheduler.emergesend(event)
# ===============================================================================

    def action_handler(self):

        bind_event = VtepControllerCall.createMatcher(self._conn)
        event_queue = []
        timeout_flag = [False]

        def handle_action():
            while event_queue or timeout_flag[0]:
                events = event_queue[:]
                del event_queue[:]

                for e in events:
                    # every event must have physname , phyiname
                    physname = e.physname
                    phyiname = e.phyiname
                    if e.type == VtepControllerCall.UNBINDALL:
                        # clear all other event info
                        self._store_event[(physname,phyiname)] = {"all":e}
                    elif e.type == VtepControllerCall.BIND:
                        # bind will combine bind event before
                        vlanid = e.vlanid
                        if (physname,phyiname) in self._store_event:
                            v = self._store_event[(physname,phyiname)]

                            if vlanid in v:
                                x = v[vlanid]
                                logicalports = e.logicalports

                                if x[0] == VtepControllerCall.BIND and x[1] == e.logicalnetworkid:
                                    new_set = set(e.logicalports)
                                    old_set = set(x[3])
                                    new_set.update(old_set)
                                    logicalports = list(new_set)

                                v.update({vlanid:(e.type,e.logicalnetworkid,e.vni,logicalports)})
                                self._store_event[(physname,phyiname)] = v
                            else:
                                # new bind info , no combind event
                                v.update({vlanid:(e.type,e.logicalnetworkid,e.vni,e.logicalports)})
                                self._store_event[(physname,phyiname)] = v
                        else:
                            self._store_event[(physname,phyiname)] = {vlanid:(e.type,e.logicalnetworkid,
                                                                              e.vni,e.logicalports)}

                    elif e.type == VtepControllerCall.UNBIND:

                        vlanid = e.vlanid

                        if (physname,phyiname) in self._store_event:
                            v = self._store_event[(physname,phyiname)]
                            v.update({vlanid:(e.type,e.logicalnetworkid)})
                            self._store_event[(physname,phyiname)] = v
                        else:
                            self._store_event[(physname,phyiname)] = {vlanid:(e.type,e.logicalnetworkid)}

                    else:
                        self._parent._logger.warning("catch error type event %r , ignore it", e)
                        continue

                call = []
                target_name = "vtepcontroller"
                for k,v in self._store_event.items():
                    if "all" in v:
                        # send unbindall
                        call.append(self.api(self,target_name,"unbindphysicalport",
                                             {"physicalswitch": k[0], "physicalport": k[1]},
                                             timeout=10))
                        # unbindall , del it whatever
                        del v["all"]

                try:
                    for m in self.executeAll(call):
                        yield m
                except Exception:
                    self._parent._logger.warning("unbindall remove call failed")

                for k,v in self._store_event.items():
                    for vlanid , e in dict(v).items():
                        if vlanid != "all":
                            if e[0] == VtepControllerCall.BIND:

                                params = {"physicalswitch": k[0],
                                            "physicalport": k[1],
                                            "vlanid": vlanid,
                                            "logicalnetwork": e[1],
                                            "vni":e[2],
                                            "logicalports": e[3]}

                                try:
                                    for m in self.api(self,target_name,"updatelogicalswitch",
                                                  params,timeout=10):
                                        yield m
                                except Exception:
                                    self._parent._logger.warning("update logical switch error,try next %r",params)
                                else:
                                    del self._store_event[k][vlanid]

                            if e[0] == VtepControllerCall.UNBIND:

                                params = {"logicalnetwork":e[1],
                                                "physicalswitch":k[0],
                                                "physicalport":k[1],
                                                  "vlanid":vlanid}

                                try:
                                    for m in self.api(self,target_name,"unbindlogicalswitch",
                                                      params,timeout=10):
                                        yield m
                                except Exception:
                                    self._parent._logger.warning("unbind logical switch error,try next %r",params)
                                else:
                                    del self._store_event[k][vlanid]

                self._store_event = dict((k,v) for k,v in self._store_event.items() if v )

                if timeout_flag[0]:
                    timeout_flag[0] = False

        def append_event(event,matcher):
            event_queue.append(event)

        while True:

            for m in self.waitWithTimeout(10,bind_event):
                yield m

            if not self.timeout:
                event_queue.append(self.event)
            else:
                timeout_flag[0] = True

            for m in self.withCallback(handle_action(),append_event,bind_event):
                yield m


    def wait_vxlan_map_info(self,container,logicalnetworkid,timeout = 4):

        if logicalnetworkid in self.vxlan_vlan_map_info:
            container.retvalue = self.vxlan_vlan_map_info[logicalnetworkid]
        else:
            event = VXLANMapChanged.createMatcher(self._conn,logicalnetworkid,VXLANMapChanged.UPDATED)

            for m in container.waitWithTimeout(timeout,event):
                yield m

            if container.timeout:
                raise ValueError(" cannot find vxlan vlan map info ")
            else:
                container.retvalue = container.event.vlan_id

@defaultconfig
class VXLANVtep(FlowBase):
    """
    Use hardware_vtep instead of software VXLAN
    """
    # Use these VLANs for vtep configuration. Must not be conflicted with VLAN networks.
    _default_vlanid_pool = ((3000, 4000),)

    # remote_api means call remote api to vtep controller
    _default_remote_api = True

    # interval time to retry failed actions
    _default_retryactioninterval = 60

    def __init__(self,server):
        super(VXLANVtep,self).__init__(server)
        self.app_routine = RoutineContainer(self.scheduler)
        self.app_routine.main = self._main
        self.routines.append(self.app_routine)

        self.conns = {}
        self.vxlan_vlan_map_info = {}

        self.createAPI(publicapi(self.createioflowparts, self.app_routine,
                                 lambda connection,logicalnetwork,**kwargs:
                                        _is_vxlan(logicalnetwork)),
                       api(self.get_vxlan_bind_info,self.app_routine))

    def _main(self):

        # check vlan pool
        lastend = 0
        for start,end in self.vlanid_pool:
            if start > end or start < lastend:
                raise ValueError(" vlan sequences overlapped or disorder ")
            if end > 4095:
                raise ValueError(" vlan out of range (0 -- 4095) ")
            lastend = end

        flow_init = FlowInitialize.createMatcher(_ismatch= lambda x : self.vhostbind is None
                                                 or x.vhost in self.vhostbind)
        conn_down = OpenflowConnectionStateEvent.createMatcher(state = OpenflowConnectionStateEvent.CONNECTION_DOWN,
                                                               _ismatch= lambda x:self.vhostbind is None
                                                               or x.createby.vhost in self.vhostbind)

        while True:
            yield (flow_init,conn_down)

            if self.app_routine.matcher is flow_init:

                conn = self.app_routine.event.connection

                self.app_routine.subroutine(self._init_conn(conn))

            if self.app_routine.matcher is conn_down:
                conn = self.app_routine.event.connection

                self.app_routine.subroutine(self._uninit_conn(conn))

    def _init_conn(self,conn):
        if conn in self.conns:
            handler = self.conns.pop(conn)
            handler.close()

        handler = VXLANHandler(conn,self)
        handler.start()
        self.conns[conn] = handler

        if None:
            yield

    def _uninit_conn(self,conn):
        if conn in self.conns:
            handler = self.conns.pop(conn)
            handler.close()

        if None:
            yield

    def get_vxlan_bind_info(self,systemid=None):
        """get vxlan -> vlan , bind info"""

        ret = []
        for conn in self.conns:
            datapath_id = conn.openflow_datapathid
            vhost = conn.protocol.vhost

            for m in callAPI(self.app_routine,"ovsdbmanager","getbridgeinfo",{"datapathid":datapath_id,
                                                                              "vhost":vhost}):
                yield m

            if self.app_routine.retvalue:
                _,system_id,_ = self.app_routine.retvalue

                if systemid:
                    if systemid == system_id:
                        handler = self.conns[conn]
                        ret.append({system_id: handler.vxlan_vlan_map_info})
                        break
                else:
                    handler = self.conns[conn]
                    ret.append({system_id:handler.vxlan_vlan_map_info})


        self.app_routine.retvalue = ret

    def createioflowparts(self,connection,logicalnetwork,physicalport,logicalnetworkid,physicalportid):

        self._logger.debug(" create flow parts connection = %r",connection)
        self._logger.debug(" create flow parts logicalnetworkid = %r", logicalnetworkid)
        self._logger.debug(" create flow parts physicalportid = %r", physicalportid)

        find = False
        vid = None
        if connection in self.conns:

            handler = self.conns[connection]

            try:
                for m in handler.wait_vxlan_map_info(self.app_routine,logicalnetwork.id):
                    yield m
            except Exception:
                find = False
                self._logger.warning(" get vxlan vlan map info error", exc_info = True)
            else:
                find = True
                vid = self.app_routine.retvalue
                if vid is None:
                    self._logger.warning('Not enough vlan ID, io flow parts not created')
                    find = False

        if find:
            self.app_routine.retvalue = self.createflowparts(connection, vid, physicalportid)
        else:
            self.app_routine.retvalue = ([],[],[],[],[])

    def createflowparts(self,connection,vid,physicalportid):
        ofdef = connection.openflowdef
        in_flow_match_part = [
            ofdef.create_oxm(ofdef.OXM_OF_VLAN_VID, vid | ofdef.OFPVID_PRESENT)
        ]

        in_flow_action_part = [
            ofdef.ofp_action(type=ofdef.OFPAT_POP_VLAN)
        ]

        out_flow_action_part = [
            ofdef.ofp_action_push(ethertype=ETHERTYPE_8021Q),
            ofdef.ofp_action_set_field(
                field=ofdef.create_oxm(
                    ofdef.OXM_OF_VLAN_VID,
                    vid | ofdef.OFPVID_PRESENT
                )
            ),
            ofdef.ofp_action_output(
                port=physicalportid
            )
        ]

        out_flow_action_part2 = [
            ofdef.ofp_action_push(ethertype=ETHERTYPE_8021Q),
            ofdef.ofp_action_set_field(
                field=ofdef.create_oxm(
                    ofdef.OXM_OF_VLAN_VID,
                    vid | ofdef.OFPVID_PRESENT
                )
            ),
            ofdef.ofp_action_output(
                port=ofdef.OFPP_IN_PORT
            )
        ]

        group_buckets = [
            ofdef.ofp_action_push(ethertype=ETHERTYPE_8021Q),
            ofdef.ofp_action_set_field(
                field=ofdef.create_oxm(
                    ofdef.OXM_OF_VLAN_VID,
                    vid | ofdef.OFPVID_PRESENT
                )
            ),
            ofdef.ofp_action_output(
                port=physicalportid
            )
        ]

        return in_flow_match_part, in_flow_action_part,out_flow_action_part, group_buckets,out_flow_action_part2

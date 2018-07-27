from vlcp.config import defaultconfig
from vlcp.event import Event
from vlcp.event import RoutineContainer
from vlcp.event import withIndices
from vlcp.protocol.openflow import OpenflowConnectionStateEvent
from vlcp.server.module import call_api, publicapi, api
from vlcp.service.sdn import ioprocessing
from vlcp.service.sdn import ovsdbportmanager
from vlcp.service.sdn.flowbase import FlowBase
from vlcp.service.sdn.ofpmanager import FlowInitialize
from vlcp.service.utils.remoteapi import remoteAPI
from vlcp.utils.ethernet import ETHERTYPE_8021Q
from contextlib import closing
from vlcp.event.event import M_


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
            self.api = call_api

    async def main(self):

        try:
            data_change_event = ioprocessing.DataObjectChanged.createMatcher(None, None, self._conn)

            # routine sync call to vtep controller (updatelogicalswitch,unbindlogicalswitch,unbindphysicalport)
            self.subroutine(self.action_handler(),True,"_action_handler")

            _last_change_event = None
            while True:
                ev = await data_change_event
                _last_change_event = ev
                def _update_callback(event, matcher):
                    nonlocal _last_change_event
                    _last_change_event = event
                async def _update_loop():
                    nonlocal _last_change_event
                    while _last_change_event is not None:
                        try:
                            last_lognets_info = self._last_lognets_info
                            last_phyport_info = self._last_phyport_info
        
                            current_logports,current_phyports,current_lognets,_ = _last_change_event.current
                            _last_change_event = None
        
                            lognet_to_logports = {}
                            for p, _ in current_logports:
                                lognet_to_logports.setdefault(p.network, []).append(p.id)
        
                            datapath_id = self._conn.openflow_datapathid
                            vhost = self._conn.protocol.vhost
        
                            async def get_phyport_info(portid):
                                try:
                                    return await call_api(self, "ovsdbportmanager", "waitportbyno",
                                                 {"datapathid": datapath_id,
                                                  "vhost": vhost,
                                                  "timeout": 1,
                                                  "portno": portid})
                                except ovsdbportmanager.OVSDBPortNotAppearException:
                                    return None
                            phy_ports = [p for p in current_phyports if _is_vxlan(p[0])]
                            port_result = await self.execute_all([get_phyport_info(pid) for p,pid in phy_ports])
        
                            phy_port_info  = dict((k[0], (v['external_ids']['vtep-physname'],
                                                                v['external_ids']['vtep-phyiname']))
                                                  for k,v in zip(phy_ports, port_result)
                                                  if v is not None and \
                                                    'vtep-physname' in v['external_ids'] and \
                                                    'vtep-phyiname' in v['external_ids'])
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
        
                                    await self.wait_for_send(event)
        
        
                            # remove phyport , unbind all physical interface bind also
                            for p in last_phyport_info:
                                if p not in phy_port_info or\
                                        (p in phy_port_info and last_phyport_info[p] != phy_port_info[p]):
                                    physname = last_phyport_info[p][0]
                                    phyiname = last_phyport_info[p][1]
        
                                    event = VtepControllerCall(connection=self._conn,type=VtepControllerCall.UNBINDALL,
                                                               physname=physname,phyiname=phyiname)
        
                                    await self.wait_for_send(event)
        
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
        
                                        await self.wait_for_send(event)
        
                                        event = VtepControllerCall(connection=self._conn,logicalnetworkid=n.id,
                                                                   physname=physname,phyiname=phyiname,vlanid=vlan_id,
                                                                   type=VtepControllerCall.UNBIND)
        
                                        await self.wait_for_send(event)
                                
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
        
                                            await self.wait_for_send(event)
                                           
                                    else:
                                        if last_info[1] != current_info[1]:        
                                            if set(current_info[1]) != set(last_info[1]):
                                                # update bind logical ports
        
                                                if n.id in self.vxlan_vlan_map_info:
                                                    vid = self.vxlan_vlan_map_info[n.id]
                                                    physname = last_lognets_info[n][0][0]
                                                    phyiname = last_lognets_info[n][0][1]
        
                                                    event = VtepControllerCall(connection=self._conn, logicalnetworkid=n.id,
                                                                               vni = n.vni,logicalports=current_info[1],
                                                                               physname=physname, phyiname=phyiname, vlanid=vid,
                                                                               type=VtepControllerCall.BIND)
        
                                                    await self.wait_for_send(event)
        
        
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
                                        await self.wait_for_send(event)
        
                                        physname = current_lognets_info[n][0][0]
                                        phyiname = current_lognets_info[n][0][1]
                                        ports = current_lognets_info[n][1]
        
                                        event = VtepControllerCall(connection=self._conn, logicalnetworkid=n.id,
                                                                   vni=n.vni, logicalports=ports,
                                                                   physname=physname, phyiname=phyiname, vlanid=vid,
                                                                   type=VtepControllerCall.BIND)
        
                                        await self.wait_for_send(event)
        
                                    else:
                                        self._parent._logger.warning('Not enough vlan_id for logical network %r', n.id)
                                        event = VXLANMapChanged(self._conn,n.id,VXLANMapChanged.UPDATED,vlan_id = None)
                                        await self.wait_for_send(event)
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
        
                                            await self.wait_for_send(event)
        
        
                        except Exception:
                            self._parent._logger.info(" vxlan vtep handler exception , continue", exc_info = True)
                await self.with_callback(_update_loop(), _update_callback, data_change_event)
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

    async def action_handler(self):
        """
        Call vtep controller in sequence, merge mutiple calls if possible
        
        When a bind relationship is updated, we always send all logical ports to a logicalswitch,
        to make sure it recovers from some failed updates (so called idempotency). When multiple
        calls are pending, we only need to send the last of them.
        """
        bind_event = VtepControllerCall.createMatcher(self._conn)
        event_queue = []
        timeout_flag = [False]

        async def handle_action():
            while event_queue or timeout_flag[0]:
                events = event_queue[:]
                del event_queue[:]

                for e in events:
                    # every event must have physname , phyiname
                    # physname: physical switch name - must be same with OVSDB-VTEP switch
                    # phyiname: physical port name - must be same with the corresponding port
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
                                logicalports = e.logicalports
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
                        self._parent._logger.warning("catch error type event %r , ignore it", exc_info=True)
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
                    await self.execute_all(call)
                except Exception:
                    self._parent._logger.warning("unbindall remove call failed", exc_info=True)

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
                                    await self.api(self,target_name,"updatelogicalswitch",
                                                  params,timeout=10)
                                except Exception:
                                    self._parent._logger.warning("update logical switch error,try next %r",params, exc_info=True)
                                else:
                                    del self._store_event[k][vlanid]

                            elif e[0] == VtepControllerCall.UNBIND:

                                params = {"logicalnetwork":e[1],
                                                "physicalswitch":k[0],
                                                "physicalport":k[1],
                                                  "vlanid":vlanid}

                                try:
                                    await self.api(self,target_name,"unbindlogicalswitch",
                                                      params,timeout=10)
                                except Exception:
                                    self._parent._logger.warning("unbind logical switch error,try next %r",params, exc_info=True)
                                else:
                                    del self._store_event[k][vlanid]

                self._store_event = dict((k,v) for k,v in self._store_event.items() if v)

                if timeout_flag[0]:
                    timeout_flag[0] = False

        def append_event(event, matcher):
            event_queue.append(event)

        while True:
            timeout, ev, m = await self.wait_with_timeout(10, bind_event)

            if not timeout:
                event_queue.append(ev)
            else:
                timeout_flag[0] = True

            await self.with_callback(handle_action(), append_event, bind_event)


    async def wait_vxlan_map_info(self,container,logicalnetworkid,timeout = 4):

        if logicalnetworkid in self.vxlan_vlan_map_info:
            return self.vxlan_vlan_map_info[logicalnetworkid]
        else:
            matcher = VXLANMapChanged.createMatcher(self._conn,logicalnetworkid,VXLANMapChanged.UPDATED)
            timeout_, ev, m = await container.wait_with_timeout(timeout, matcher)

            if timeout_:
                raise ValueError(" cannot find vxlan vlan map info ")
            else:
                return ev.vlan_id


def _check_vlanrange(vlanrange):
    lastend = 0
    for start,end in vlanrange:
        if start <= 0 or end > 4095:
            raise ValueError('VLAN ID out of range (1 - 4095)')
        if start > end or start <= lastend:
            raise ValueError('VLAN sequences overlapped or disordered: [%r, %r]' % (start, end))
        lastend = end


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

    async def _main(self):

        # check vlan pool
        lastend = 0
        _check_vlanrange(self.vlanid_pool)

        flow_init = FlowInitialize.createMatcher(_ismatch= lambda x : self.vhostbind is None
                                                 or x.vhost in self.vhostbind)
        conn_down = OpenflowConnectionStateEvent.createMatcher(state = OpenflowConnectionStateEvent.CONNECTION_DOWN,
                                                               _ismatch= lambda x:self.vhostbind is None
                                                               or x.createby.vhost in self.vhostbind)

        while True:
            ev, m = await M_(flow_init,conn_down)

            if m is flow_init:

                conn = ev.connection

                self.app_routine.subroutine(self._init_conn(conn))

            elif m is conn_down:
                conn = ev.connection

                self.app_routine.subroutine(self._uninit_conn(conn))

    async def _init_conn(self,conn):
        if conn in self.conns:
            handler = self.conns.pop(conn)
            handler.close()

        handler = VXLANHandler(conn,self)
        handler.start()
        self.conns[conn] = handler

    async def _uninit_conn(self,conn):
        if conn in self.conns:
            handler = self.conns.pop(conn)
            handler.close()

    async def get_vxlan_bind_info(self,systemid=None):
        """get vxlan -> vlan , bind info"""

        ret = []
        for conn in self.conns:
            datapath_id = conn.openflow_datapathid
            vhost = conn.protocol.vhost

            r = await call_api(self.app_routine,"ovsdbmanager","getbridgeinfo",{"datapathid":datapath_id,
                                                                              "vhost":vhost})

            if r:
                _,system_id,_ = r

                if systemid:
                    if systemid == system_id:
                        handler = self.conns[conn]
                        ret.append({system_id: handler.vxlan_vlan_map_info})
                        break
                else:
                    handler = self.conns[conn]
                    ret.append({system_id:handler.vxlan_vlan_map_info})


        return ret

    async def createioflowparts(self,connection,logicalnetwork,physicalport,logicalnetworkid,physicalportid):

        self._logger.debug(" create flow parts connection = %r",connection)
        self._logger.debug(" create flow parts logicalnetworkid = %r", logicalnetworkid)
        self._logger.debug(" create flow parts physicalportid = %r", physicalportid)

        find = False
        vid = None
        if connection in self.conns:

            handler = self.conns[connection]

            try:
                vid = await handler.wait_vxlan_map_info(self.app_routine,logicalnetwork.id)
            except Exception:
                find = False
                self._logger.warning(" get vxlan vlan map info error", exc_info = True)
            else:
                find = True
                if vid is None:
                    self._logger.warning('Not enough vlan ID, io flow parts not created')
                    find = False
        if find:
            return self.createflowparts(connection, vid, physicalportid)
        else:
            return ([],[],[],[],[])

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

import json

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
from vlcp.utils.webclient import WebClient


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

        self._store_event = []
        self._store_bind_event = []
        self._store_unbind_event = []
        self._store_unbindall_event = []

        self.wc = WebClient()

        if self._parent.remote_api:
            self.api = remoteAPI
        else:
            self.api = callAPI
    def main(self):

        try:
            data_change_event = ioprocessing.DataObjectChanged.createMatcher(None, None, self._conn)

            self.subroutine(self.time_cycle(),True,"_time_cycle")

            # routine sync call to vtep controller (updatelogicalswitch,unbindlogicalswitch,unbindphysicalport)
            self.subroutine(self.action_handler(),True,"_action_handler")

            # routine handle action event which send failed , in cycle
            self.subroutine(self.handle_failed_action(),True,"_handle_failed_action")
            while True:
                yield (data_change_event,)

                try:
                    last_lognets_info = self._last_lognets_info
                    last_phyport_info = self._last_phyport_info

                    current_logports,current_phyports,current_lognets,_ = self.event.current

                    lognet_to_logports = dict((n,[p.id for p,_ in current_logports if p.network == n])
                                              for n,_ in current_lognets)

                    phy_port_info = {}


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

                    temp_phy_port_info  = dict(zip(phy_ports,self.retvalue))

                    for k,v in temp_phy_port_info.items():
                        if v and v[0]['external_ids']['vtep-physname'] and v[0]['external_ids']['vtep-phyiname']:
                            phy_port_info[k[0]] = [v[0]['external_ids']['vtep-physname'],
                                                v[0]['external_ids']['vtep-phyiname']]

                    lognet_to_phyport =  dict((n,[p for p,_ in current_phyports if _is_vxlan(p)
                                            and p.physicalnetwork == n.physicalnetwork]) for n,_ in current_lognets)

                    current_lognets_info = dict((n,((phy_port_info[lognet_to_phyport[n][0]][0],
                                                     phy_port_info[lognet_to_phyport[n][0]][1]),lognet_to_logports[n]))
                                                for n,nid in current_lognets if _is_vxlan(n)
                                                and n in lognet_to_phyport and lognet_to_phyport[n][0] in phy_port_info
                                                and n in lognet_to_logports)

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
                        if p not in phy_port_info:
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
                                vid = self.vxlan_vlan_map_info[n.id]
                                physname = last_lognets_info[n][0][0]
                                phyiname = last_lognets_info[n][0][1]

                                del self.vxlan_vlan_map_info[n.id]

                                event = VXLANMapChanged(self._conn,n.id,VXLANMapChanged.DELETED)

                                for m in self.waitForSend(event):
                                    yield m

                                event = VtepControllerCall(connection=self._conn,logicalnetworkid=n.id,
                                                           physname=physname,phyiname=phyiname,vlanid=vid,
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
                            for start,end in self.vlanid_pool:
                                for vid in range(start,end + 1):
                                    if str(vid) not in self.vxlan_vlan_map_info.values():
                                        find = True
                                        break

                            if find:
                                self.vxlan_vlan_map_info[n.id] = str(vid)

                                event = VXLANMapChanged(self._conn,n.id,VXLANMapChanged.UPDATED,vlan_id = str(vid))
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
                                raise ValueError(" find avaliable vlan id error")
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

        finally:

            if hasattr(self,"_time_cycle"):
                self._time_cycle.close()

            if hasattr(self,"_action_handler"):
                self._action_handler.close()

            if hasattr(self,"_handle_failed_action"):
                self._handle_failed_action.close()

    def time_cycle(self):

        while True:
            for m in self.waitWithTimeout(self._parent.refreshinterval):
                yield m

            for n,v in self._last_lognets_info.items():

                if n.id in self.vxlan_vlan_map_info:
                    vid = self.vxlan_vlan_map_info[n.id]
                    physname = v[0][0]
                    phyiname = v[0][1]
                    ports = v[1]
                    
                    event = VtepControllerCall(connection=self._conn, logicalnetworkid=n.id,
                                                               vni=n.vni, logicalports=ports,
                                                               physname=physname, phyiname=phyiname, vlanid=vid,
                                                               type=VtepControllerCall.BIND)

                    for m in self.waitForSend(event):
                        yield m

    def handle_failed_action(self):

        while True:
            for m in self.waitWithTimeout(self._parent.retryactioninterval):
                yield m

            self._store_event = [ x for x in self._store_event
                                  if x[1] in self._store_bind_event or x[1] in self._store_unbind_event]

            handle_events = self._store_event[:]
            del self._store_event[:]

            for x in handle_events:
                if x[0] == "BIND":
                    logicalnetworkid = x[1][0]
                    vni = x[1][1]
                    physname = x[1][2]
                    phyiname = x[1][3]
                    vlanid = x[1][4]
                    ports = x[1][5]
                    e = VtepControllerCall(connection=self._conn,logicalnetworkid=logicalnetworkid,vni=vni,
                                           physname=physname,phyiname=phyiname,vlanid=vlanid,logicalports=ports,
                                           type=VtepControllerCall.BIND)

                    for m in self.waitForSend(e):
                        yield m

                elif x[0] == "UNBIND":
                    logicalnetworkid = x[1][0]
                    physname = x[1][1]
                    phyiname = x[1][2]
                    vlanid = x[1][3]

                    e = VtepControllerCall(connection=self._conn,logicalnetworkid=logicalnetworkid,
                                           physname=physname,phyiname=phyiname,vlanid=vlanid,
                                           type=VtepControllerCall.UNBIND)

                    for m in self.waitForSend(e):
                        yield m
                else:
                    self._parent._logger.warning(" handle failed event , invaild type %r ignore ..",x[0])
                    continue

    def action_handler(self):

        bind_event = VtepControllerCall.createMatcher(self._conn,type=VtepControllerCall.BIND)
        unbind_event = VtepControllerCall.createMatcher(self._conn,type=VtepControllerCall.UNBIND)
        unbindall_event = VtepControllerCall.createMatcher(self._conn,type=VtepControllerCall.UNBINDALL)

        bind_event = VtepControllerCall.createMatcher(self._conn)
        event_queue = []

        def handle_action():
            while event_queue:
                events = event_queue[:]
                del event_queue[:]

                bind_events = [e for e in events if e.type == VtepControllerCall.BIND]
                unbind_events = [e for e in events if e.type == VtepControllerCall.UNBIND]
                unbindall_events = [e for e in events if e.type == VtepControllerCall.UNBINDALL]

                target_name = "vtepcontroller"
                for e in events:
                    if e.type == VtepControllerCall.BIND:
                        method = "updatelogicalswitch"
                        lgnetid=e.logicalnetworkid
                        vni = e.vni
                        physname = e.physname
                        phyiname = e.phyiname
                        vlanid = e.vlanid
                        ports = e.logicalports

                        params = {"physicalswitch": physname,
                                   "physicalport": phyiname,
                                   "vlanid": vlanid,
                                   "logicalnetwork": lgnetid,
                                   "vni":vni,
                                   "logicalports": ports}

                        bind_info = (e.logicalnetworkid,e.vni,e.physname,e.phyiname,e.vlanid,e.logicalports)
                        if bind_info in self._store_bind_event:
                            # means this bind event faild in last ,  delete it to never retry
                            self._store_bind_event = [x for x in self._store_bind_event if x != bind_info]

                        try:
                            for m in self.api(self,target_name,method,params):
                                yield m
                        except Exception:
                            # error , store this event
                            self._parent._logger.warning(" bind event handle error %r , store it",e,exec_info=True)
                            self._store_bind_event.append(bind_info)
                            self._store_event.append(("BIND",bind_info))

                    elif e.type == VtepControllerCall.UNBIND:
                        method = "unbindlogicalswitch"
                        lgnetid=e.logicalnetworkid
                        physname = e.physname
                        phyiname = e.phyiname
                        vlanid = e.vlanid

                        params = {"logicalnetwork":e.logicalnetworkid,
                                  "physicalswitch":e.physname,
                                  "physicalport":e.phyiname,
                                  "vlanid":e.vlanid}

                        unbind_info = (e.logicalnetworkid,e.physname,e.phyiname,e.vlanid)
                        if unbind_info in self._store_unbind_event:
                            self._store_unbind_event = [ x for x in self._store_unbind_event if x != unbind_info]

                        # unbind event, should remove last bind failed , no need to retry it
                        self._store_bind_event = [x for x in self._store_bind_event
                                                  if unbind_info != (x[0],x[2],x[3],x[4])]
                        try:
                            for m in self.api(self,target_name,method,params):
                                yield m
                        except Exception:
                            self._parent._logger.warning(" unbind event handle error %r, store it",e,exec_info=True)
                            self._store_unbind_event.append(unbind_info)
                            self._store_event.append(("UNBIND",unbind_info))

                    elif e.type == VtepControllerCall.UNBINDALL:
                        method = "unbindphysicalport"

                        physname = e.physname
                        phyiname = e.phyiname
                        params = {"physicalswitch":e.physname,"physicalport":e.phyiname}

                        unbindall_info = (e.physname,e.phyiname)

                        # unbindall ,  some bind , unbind event action last failed , no need do it !
                        self._store_bind_event = [x for x in self._store_bind_event
                                                  if unbindall_info != (x[2],x[3])]

                        self._store_unbind_event = [ x for x in self._store_unbind_event
                                                     if unbindall_info != (x[1],x[2])]

                        try:
                            for m in self.api(self,target_name,method,params):
                                yield m
                        except Exception:
                            self._parent._logger.warning(" unbindall event handle error %r , store it",e,exec_info=True)
                            # don't store unbindall event , because if unbindall failed
                            # (1) init unbindall failed , after event will work ...
                            # (2) clean unbindall failed, nothing to do for me ...
                            # else store it ,retry it , mybe drop some vaild actions

                            # self._store_unbindall_event.append(e)
                    else:
                        self._parent._logger.warning("catch error type event %r , ignore it",e)
                        continue


        def append_event(event,matcher):
            event_queue.append(event)

        while True:
            yield (bind_event,)

            event_queue.append(self.event)

            for m in self.withCallback(handle_action(),append_event,bind_event):
                yield m


    def wait_vxlan_map_info(self,contianer,logicalnetworkid,timeout = 4):

        if logicalnetworkid in self.vxlan_vlan_map_info:
            contianer.retvalue = int(self.vxlan_vlan_map_info[logicalnetworkid])
        else:
            event = VXLANMapChanged.createMatcher(self._conn,logicalnetworkid,VXLANMapChanged.UPDATED)

            for m in contianer.waitWithTimeout(timeout,event):
                yield m

            if contianer.timeout:
                raise ValueError(" can find vxlan vlan map info ")
            else:
                contianer.retvalue = int(contianer.event.vlan_id)

@defaultconfig
class VXLANVtep(FlowBase):
    _default_vlanid_pool = ((3000, 4000),)
    _default_refreshinterval = 3600

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

        self._logger.info(" create flow parts connection = %r",connection)
        self._logger.info(" create flow parts logicalnetworkid = %r", logicalnetworkid)
        self._logger.info(" create flow parts physicalportid = %r", physicalportid)

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

import json

from vlcp.config import defaultconfig

from vlcp.utils.webclient import WebClient, WebException
from vlcp.event import Event
from vlcp.event import withIndices
from vlcp.server.module import callAPI, publicapi, api
from vlcp.protocol.openflow import OpenflowConnectionStateEvent
from vlcp.service.sdn import ioprocessing
from vlcp.service.sdn import ovsdbportmanager
from vlcp.service.sdn.ofpmanager import FlowInitialize
from vlcp.event import RoutineContainer
from vlcp.service.sdn.flowbase import FlowBase
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

class VXLANHandler(RoutineContainer):
    def __init__(self,connection,parent):
        super(VXLANHandler,self).__init__(connection.scheduler)
        self._conn = connection
        self._parent = parent

        self.vxlan_vlan_map_info = {}
        self.vlanid_pool = parent.vlanid_pool
        self._last_lognets_info = {}

        self.wc = WebClient()

    def main(self):

        try:
            data_change_event = ioprocessing.DataObjectChanged.createMatcher(None, None, self._conn)

            self.subroutine(self.time_cycle(),True,"_time_cycle")

            while True:
                yield (data_change_event,)

                try:
                    last_lognets_info = self._last_lognets_info

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

                                self.subroutine(self._unbind_lgnet(n.id,physname,phyiname,vid))
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
                                    self.subroutine(self._unbind_lgnet(n.id, physname, phyiname, vid))
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
                                            self.subroutine(self._bind_lgnet(n.id,physname,phyiname,vid,add_ports))

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
                                self.subroutine(self._bind_lgnet(n.id,physname,phyiname,vid,ports))
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

                                    self.subroutine(self._bind_lgnet(n.id, physname, phyiname, vid, ports))

                except Exception:
                    self._parent._logger.info(" vxlan vtep handler exception , continue", exc_info = True)

        finally:

            if hasattr(self,"_time_cycle"):
                self._time_cycle.close()

    def time_cycle(self):

        while True:
            for m in self.waitWithTimeout(self._parent.refreshinterval):
                yield m

            self._parent._logger.info(" ------- time cycle ---")
            for n,v in self._last_lognets_info.items():

                if n.id in self.vxlan_vlan_map_info:
                    vid = self.vxlan_vlan_map_info[n.id]
                    physname = v[0][0]
                    phyiname = v[0][1]
                    ports = v[1]
                    self.subroutine(self._bind_lgnet(n.id, physname, phyiname, vid, ports))


    def _bind_lgnet(self,logicalnetworkid,physname,phyiname,vlanid,logicalports):
        self._parent._logger.debug(" bind logicalnetwork id = %r",logicalnetworkid)
        self._parent._logger.debug(" bind physname %r,phyiname %r",physname,phyiname)
        self._parent._logger.debug(" bind vlanid %r",vlanid)
        self._parent._logger.debug(" bind logicalports %r",logicalports)

        url = self._parent.vtepcontroller_url + 'vtepcontroller/updatelogicalswitch'

        request = {"physicalswitch":physname,
                   "physicalport":phyiname,
                   "vlanid":vlanid,
                   "logicalnetwork":logicalnetworkid,
                   "logicalports":logicalports}

        request_data = json.dumps(request).encode("utf-8")

        again = 3
        while True:
            try:
                for m in self.wc.urlgetcontent(self,url,request_data,
                                      b'POST',{"Content-Type":"application/json"}):
                    yield m
                resp = json.loads(self.retvalue)
            except Exception:
                again -= 1
                if again > 0:
                    self._parent._logger.warning("url open error %r , try again (%r)",url,again)
                    continue
                else:
                    self._parent._logger.warning("url open error %r final", url)
                    break
            else:
                self._parent._logger.info(" url open success %r",url)
                break

    def _unbind_lgnet(self,logicalnetworkid,physname,phyiname,vlanid):
        self._parent._logger.debug(" unbind logicalnetwork id = %r",logicalnetworkid)
        self._parent._logger.debug(" unbind physname %r,phyiname %r",physname,phyiname)
        self._parent._logger.debug(" unbind vlanid %r",vlanid)

        url = self._parent.vtepcontroller_url + 'vtepcontroller/unbindlogicalswitch'

        request = {"physicalswitch":physname,
                   "physicalport":phyiname,
                   "vlanid":vlanid,
                   "logicalnetwork":logicalnetworkid}

        request_data = json.dumps(request).encode("utf-8")

        again = 3
        while True:
            try:
                for m in self.wc.urlgetcontent(self,url,request_data,
                                      b'POST',{"Content-Type":"application/json"}):
                    yield m
                resp = json.loads(self.retvalue)
            except Exception:
                again -= again
                if again > 0:
                    self._parent._logger.warning("url open error %r , try again (%r)",url,again)
                    continue
                else:
                    self._parent._logger.warning("url open error %r final", url)
                    break
            else:
                self._parent._logger.info(" url open success %r",url)
                break

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

    _default_vtepcontroller_url = 'http://127.0.0.1:8081/'

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
                       api(self.get_vxlan_bind_info))

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

    def get_vxlan_bind_info(self,logicalnetwork):
        """get vxlan -> vlan , bind info"""

        ret = []
        for conn in self.conns:
            handler = self.conns[conn]
            ret.append(handler.vxlan_vlan_map_info)

        return ret

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

'''
Created on 2015/7/30

:author: hubo
'''
from __future__ import print_function
# from pprint import pprint
import json
def pprint(v):
    print(json.dumps(v, indent=2))
from vlcp.server import Server
from vlcp.event import Client, RoutineContainer
from vlcp.protocol.openflow import Openflow, OpenflowConnectionStateEvent, Openflow
from vlcp.protocol.openflow import common
import sys
import logging

of_proto = Openflow((common.OFP13_VERSION, common.OFP14_VERSION))

class MainRoutine(RoutineContainer):
    async def main(self):
        connected = OpenflowConnectionStateEvent.createMatcher()
        ev = await connected
        pprint(common.dump(ev.connection.openflow_featuresreply, tostr=True))
        connection = ev.connection
        currdef = connection.openflowdef
        openflow_reply = await of_proto.querymultipart(
                                            currdef.ofp_multipart_request.new(
                                                type = currdef.OFPMP_DESC
                                            ), connection, self)
        for msg in openflow_reply:
            pprint(common.dump(msg, tostr=True))
        openflow_reply = await of_proto.querymultipart(
                                            currdef.ofp_multipart_request.new(
                                                type = currdef.OFPMP_PORT_DESC
                                            ), connection, self)
        for msg in openflow_reply:
            pprint(common.dump(msg, tostr=True))
        req = currdef.ofp_flow_stats_request.new(
                table_id = currdef.OFPTT_ALL,
                out_port = currdef.OFPP_ANY,
                out_group = currdef.OFPG_ANY,
                match = currdef.ofp_match_oxm.new())
        openflow_reply = await of_proto.querymultipart(req, connection, self)
        for msg in openflow_reply:
            pprint(common.dump(msg, dumpextra = True, typeinfo = common.DUMPTYPE_FLAT, tostr=True))
        req = currdef.ofp_msg.new()
        req.header.type = currdef.OFPT_GET_CONFIG_REQUEST
        openflow_reply = await of_proto.querywithreply(req, connection, self)
        pprint(common.dump(openflow_reply, tostr=True))
        req = currdef.ofp_role_request.new(role = currdef.OFPCR_ROLE_NOCHANGE)
        openflow_reply = await of_proto.querywithreply(req, connection, self)
        pprint(common.dump(openflow_reply, tostr=True))
        req = currdef.ofp_msg.new()
        req.header.type = currdef.OFPT_GET_ASYNC_REQUEST
        openflow_reply = await of_proto.querywithreply(req, connection, self)
        pprint(common.dump(openflow_reply, tostr=True))
        await mgt_conn.shutdown(False)

if __name__ == '__main__':
    logging.basicConfig()
    s = Server()
    #s.scheduler.logger.setLevel(logging.DEBUG)
    #of_proto._logger.setLevel(logging.DEBUG)
    #of_proto.debugging = True
    #NamedStruct._logger.setLevel(logging.DEBUG)
    bridge = sys.argv[1]
    routine = MainRoutine(s.scheduler)
    routine.start()
    mgt_conn = Client('unix:/var/run/openvswitch/' + bridge + '.mgmt', of_proto, s.scheduler)
    mgt_conn.start()
    s.serve()
    

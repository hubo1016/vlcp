'''
Created on 2015/7/30

@author: hubo
'''
from pprint import pprint
from server import Server
from event import Client, RoutineContainer
from protocol.openflow import Openflow, OpenflowConnectionStateEvent, Openflow
from protocol.openflow import common
import sys
import logging
from utils.namedstruct import NamedStruct

of_proto = Openflow((common.OFP13_VERSION,))

class MainRoutine(RoutineContainer):
    def main(self):
        connected = OpenflowConnectionStateEvent.createMatcher()
        yield (connected,)
        pprint(common.dump(self.event.connection.openflow_featuresreply))
        connection = self.event.connection
        currdef = connection.openflowdef
        for m in of_proto.querymultipart(currdef.ofp_multipart_request.new(
                type = currdef.OFPMP_DESC), connection, self):
            yield m
        for msg in self.openflow_reply:
            pprint(common.dump(msg))
        for m in of_proto.querymultipart(currdef.ofp_multipart_request.new(
                type = currdef.OFPMP_PORT_DESC), connection, self):
            yield m
        for msg in self.openflow_reply:
            pprint(common.dump(msg))
        req = currdef.ofp_flow_stats_request.new(
                table_id = currdef.OFPTT_ALL,
                out_port = currdef.OFPP_ANY,
                out_group = currdef.OFPG_ANY,
                match = currdef.ofp_match_oxm.new())
        for m in of_proto.querymultipart(req, connection, self):
            yield m
        for msg in self.openflow_reply:
            pprint(common.dump(msg, dumpextra = True, typeinfo = common.DUMPTYPE_FLAT))
        req = currdef.ofp_msg.new()
        req.header.type = currdef.OFPT_GET_CONFIG_REQUEST
        for m in of_proto.querywithreply(req, connection, self):
            yield m
        pprint(common.dump(self.openflow_reply))
        req = currdef.ofp_role_request.new(role = currdef.OFPCR_ROLE_NOCHANGE)
        for m in of_proto.querywithreply(req, connection, self):
            yield m
        pprint(common.dump(self.openflow_reply))
        req = currdef.ofp_msg.new()
        req.header.type = currdef.OFPT_GET_ASYNC_REQUEST
        for m in of_proto.querywithreply(req, connection, self):
            yield m
        pprint(common.dump(self.openflow_reply))        
        for m in mgt_conn.shutdown(False):
            yield m

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
    

'''
Created on 2015/7/30

:author: hubo
'''
from pprint import pprint
from vlcp.server import Server
from vlcp.event import Client, RoutineContainer
from vlcp.protocol.jsonrpc import JsonRPC, JsonRPCConnectionStateEvent
import logging

jsonrpc = JsonRPC()

class MainRoutine(RoutineContainer):
    def main(self):
        connected = JsonRPCConnectionStateEvent.createMatcher(state = JsonRPCConnectionStateEvent.CONNECTION_UP)
        yield (connected,)
        connection = self.event.connection
        for m in jsonrpc.querywithreply('list_dbs', [], connection, self):
            yield m
        if self.jsonrpc_error:
            pprint(self.jsonrpc_error)
        else:
            pprint(self.jsonrpc_result)
        dbname = self.jsonrpc_result[0]
        for m in jsonrpc.querywithreply('get_schema', [dbname], connection, self):
            yield m
        if self.jsonrpc_error:
            pprint(self.jsonrpc_error)
        else:
            pprint(self.jsonrpc_result)
        for m in connection.shutdown():
            yield m

if __name__ == '__main__':
    logging.basicConfig()
    s = Server()
    #s.scheduler.logger.setLevel(logging.DEBUG)
    #JsonRPC.debugging = True
    #JsonRPC._logger.setLevel(logging.DEBUG)
    routine = MainRoutine(s.scheduler)
    routine.start()
    mgt_conn = Client('unix:/var/run/openvswitch/db.sock', jsonrpc, s.scheduler)
    mgt_conn.start()
    s.serve()
    

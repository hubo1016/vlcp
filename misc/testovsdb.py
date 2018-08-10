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
    async def main(self):
        connected = JsonRPCConnectionStateEvent.createMatcher(state = JsonRPCConnectionStateEvent.CONNECTION_UP)
        ev = await connected
        connection = ev.connection
        result, error = await jsonrpc.querywithreply('list_dbs', [], connection, self)
        if error:
            pprint(error)
        else:
            pprint(result)
        dbname = result[0]
        result, error = await jsonrpc.querywithreply('get_schema', [dbname], connection, self)
        if error:
            pprint(error)
        else:
            pprint(result)
        await connection.shutdown()


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
    

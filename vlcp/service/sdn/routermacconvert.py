import itertools

from vlcp.event import RoutineContainer
from vlcp.protocol.openflow import OpenflowConnectionStateEvent
from vlcp.server.module import callAPI
from vlcp.service.sdn.flowbase import FlowBase
from vlcp.service.sdn.ofpmanager import FlowInitialize
from vlcp.utils.flowupdater import FlowUpdater
import vlcp.service.sdn.ioprocessing as iop

class RouterMACConvertUpdater(FlowUpdater):
    def __init__(self, connection, parent):
        super(RouterMACConvertUpdater, self).__init__(connection, (),
                                                      ("RouterMACConvertUpdater", connection), parent._logger)
        self._parent = parent
        self._lastphysicalport = {}

    def main(self):
        try:
            self.subroutine(self._update_handler(), True, "updater_handler")

            for m in FlowUpdater.main(self):
                yield m
        finally:

            if hasattr(self, "updater_handler"):
                self.updater_handler.close()

    def _update_handler(self):

        dataobjectchange = iop.DataObjectChanged.createMatcher(None, None, self._connection)

        while True:
            yield (dataobjectchange,)

            _, self._lastphysicalport, _, _ = self.event.current


    def _update_walk(self):
        physicalportkeys = [p.getkey() for p, _ in self._lastphysicalport]

        self._initialkeys = physicalportkeys
        #self._original_keys = logicalportkeys + logicalnetkeys

        self._walkerdict = dict(itertools.chain(((p, self._walk_phyport) for p in physicalportkeys)))

        self.subroutine(self.restart_walk(), False)

    def _walk_phyport(self, key, value, walk, save):
        if value is None:
            return
        save(key)

    def updateflow(self, connection, addvalues, removevalues, updatedvalues):

        allobjects = set(o for o in self._savedresult if o is not None and not o.isdeleted())
        datapath_id = connection.openflow_datapathid
        vhost = connection.protocol.vhost

        for m in self.executeAll([callAPI(self,"ovsdbportmanager","waitportbyname",
                                    {"datapathid":datapath_id,"vhost":vhost,"name":name})]
                                    for _,name in self._lastphysicalport):
            yield

        physicalportdesc = self.retvalue



class RouterMACConvert(FlowBase):
    _tablerequest = (
        ("inmacconvert", ("ingress",), ""),
        ("l2input","inmacconvert",""),
        ("outmacconvert", ("l2output",), ""),
        ("egress", ("outmacconvert",), "")
    )

    _default_inroutermac = '1a:23:67:59:63:33'
    _default_outroutermacmask = '0a:00:00:00:00:00'

    def __init__(self, server):
        super(RouterMACConvert, self).__init__(server)
        self.app_routine = RoutineContainer(self.scheduler)
        self.app_routine.main = self._main
        self.routines.append(self.app_routine)
        self._flowupdater = dict()

    def _main(self):
        flowinit = FlowInitialize.createMatcher(_ismatch=lambda x: self.vhostbind is None or
                                                                   x.vhost in self.vhostbind)

        conndown = OpenflowConnectionStateEvent.createMatcher(state=OpenflowConnectionStateEvent.CONNECTION_DOWN,
                                                              _ismatch=lambda x: self.vhostbind is None
                                                                                 or x.createby.vhost in self.vhostbind)

        while True:
            yield (flowinit, conndown)

            if self.app_routine.matcher is flowinit:
                c = self.app_routine.event.connection
                self.app_routine.subroutine(self._init_conn(c))
            if self.app_routine.matcher is conndown:
                c = self.app_routine.event.connection
                self.app_routine.subroutine(self._uninit_conn(c))

    def _init_conn(self, conn):
        if conn in self._flowupdater:
            updater = self._flowupdater.pop(conn)
            updater.close()

        updater = RouterMACConvertUpdater(conn, self)

        self._flowupdater[conn] = updater
        updater.start()

    def _uninit_conn(self, conn):
        if conn in self._flowupdater:
            updater = self._flowupdater.pop(conn)
            updater.close()

        if False:
            yield
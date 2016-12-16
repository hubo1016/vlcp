from vlcp.config import defaultconfig

from vlcp.event import RoutineContainer

from vlcp.utils.webclient import WebClient

from vlcp.server.module import Module,callAPI, api

@defaultconfig
class RemoteCall(Module):
    _default_target_url_map = {}

    def __init__(self,server):
        super(RemoteCall,self).__init__(server)
        self.app_routine = RoutineContainer(self.scheduler)
        # there is no need to run this container main, so don't append it
        # self.routines.append(self.app_routine)
        self.wc = WebClient()
        self.createAPI(api(self.call,self.app_routine))

    def call(self,remote_module,method,**kwargs):
        self._logger.info("remote call remote_module %r",remote_module)
        self._logger.info("remote call method %r", method)
        self._logger.info("remote call kwargs %r", kwargs)

        if None:
            yield

        self.app_routine.retvalue = []


def remoteAPI(container,targetname,name,params={}):
    args = {"remote_module":targetname,"method":name}
    args.update(params)
    for m in callAPI(container,"remotecall","call",params=args,timeout=20):
        yield m
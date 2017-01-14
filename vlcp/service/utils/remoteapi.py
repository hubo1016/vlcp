import json

from vlcp.config import defaultconfig

from vlcp.event import RoutineContainer

from vlcp.utils.webclient import WebClient, WebException

from vlcp.server.module import Module,callAPI, api

@defaultconfig
class RemoteCall(Module):
    """
    Route local API calls to remote management API.
    """
    # URL list for every target, should be ``{target: [url, url, ...]}``
    _default_target_url_map = {}

    def __init__(self,server):
        super(RemoteCall,self).__init__(server)
        self.app_routine = RoutineContainer(self.scheduler)
        # there is no need to run this container main, so don't append it
        # self.routines.append(self.app_routine)
        self.wc = WebClient()
        self.app_routine.main = self._main
        self.routines.append(self.app_routine)
        self.createAPI(api(self.call,self.app_routine))
    
    def _main(self):
        try:
            self.wc.cleanup_task(self.app_routine, 120)
            yield ()
        finally:
            self.wc.endtask()

    def call(self,remote_module,method,timeout,params):
        """
        Call remote API
        
        :param remote_module: target name for the remote module
        
        :param method: method name of the API
        
        :param timeout: timeout for the call
        
        :param params: A dictionary contains all the parameters need for the call
        
        :return: Return result from the remote call
        """
        self._logger.info("remote call remote_module %r",remote_module)
        self._logger.info("remote call method %r", method)
        self._logger.info("remote call kwargs %r", params)
        success = False

        if remote_module and remote_module in self.target_url_map:
            endpoints = self.target_url_map[remote_module]
            params = json.dumps(params).encode("utf-8")
            for endpoint in endpoints:
                url = endpoint + "/" + remote_module + "/" + method

                try:
                    for m in self.wc.urlgetcontent(self.app_routine,url,params,b'POST',
                                                   {"Content-Type":"application/json"},timeout=timeout):
                        yield m
                except WebException as e:
                    # this endpoint connection error , try next url
                    self._logger.warning(" url (%r) post error %r , break ..",url,e)
                    success = False
                    raise
                except Exception:
                    self._logger.warning(" url (%r) connection error , try next ..", url)
                    continue
                else:
                    success = True
                    break

        else:
            self._logger.warning(" target (%r) url not existed, ignore it ",remote_module)

        if not success:
            raise IOError("remote call connection error !")

        self.app_routine.retvalue = []

def remoteAPI(container,targetname,name,params={},timeout=60.0):
    args = {"remote_module":targetname,"method":name,"timeout":timeout,"params":params}
    for m in callAPI(container,"remotecall","call",params=args,timeout=timeout + 20):
        yield m
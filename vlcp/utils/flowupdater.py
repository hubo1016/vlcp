'''
Created on 2016/4/11

:author: hubo
'''
from vlcp.event.runnable import RoutineContainer, GeneratorExit_
from vlcp.server.module import call_api, send_api
from uuid import uuid1
from vlcp.event.event import Event, withIndices, M_
from vlcp.utils.dataobject import multiwaitif
from vlcp.protocol.openflow.openflow import OpenflowErrorResultException
from namedstruct.namedstruct import dump
import json
import logging
from contextlib import closing

@withIndices('updater', 'type')
class FlowUpdaterNotification(Event):
    """
    These notifications are edge triggered - they work with identifiers
    and set
    """
    STARTWALK = 'startwalk'
    DATAUPDATED = 'dataupdated'
    FLOWUPDATE = 'flowupdate'

class FlowUpdater(RoutineContainer):
    def __init__(self, connection, initialkeys = (), requestid = None, logger = None):
        """
        Retrieve data objects from ObjectDB and use them to generate flows
        
        The module starts ObjectDB.walk from initial keys and walkers. After the
        walk completes, the retrieved data objects are used by `updateflow()` to
        generate flows and send them to the OpenFlow connection. When the retrieved
        objects are updated, FlowUpdater either restart the walk process (re-walk)
        or directly call another `updateflow()`, according to the objects that are updated.
        
        A subclass should re-initialize `self._initialkeys` and `self._walkerdict`
        before `main()` coroutine starts to customize the process.
        
        `updateflow()` is guaranteed for no re-entrance i.e. will not be called until
        the last call returns. Multiple changes may be merged into the same call.
        
        :param connection: OpenFlow connection
        
        :param initialkeys: DEPRECATED The key list that triggers a re-walk
        
        :param requestid: request id to retrieve data objects from ObjectDB
        
        :param logger: inherit a logger from a module
        """
        RoutineContainer.__init__(self, connection.scheduler)
        # When keys in this sequence are updated, start re-walk
        self._initialkeys = initialkeys
        self._connection = connection
        # Walker dict, will be re-initialized by a subclass
        self._walkerdict = {}
        # Walk result
        self._savedkeys = ()
        self._savedresult = []
        # Update notification (with a cache for synchronize)
        self._updatedset = set()
        self._updatedset2 = set()
        if not logger:
            self._logger = logging.getLogger(__name__ + '.FlowUpdater')
        else:
            self._logger = logger
        if requestid is None:
            self._requstid = str(uuid1())
        else:
            self._requstid = requestid
        self._requestindex = 0
        # Detect data object updates
        self._dataupdateroutine = None
        # Calling updateflow() in the same routine to prevent a re-entrance
        self._flowupdateroutine = None

    def reset_initialkeys(self, keys, values):
        """
        Callback after walk complete, can be used to update `self._initialkeys`.
        """
        pass

    async def walkcomplete(self, keys, values):
        """
        Async callback after walk complete, before flow update
        """
        pass

    async def updateflow(self, connection, addvalues, removevalues, updatedvalues):
        """
        Update flow callback. When data objects are updated (either by a re-walk
        or by a direct update), this method is called with the modification, after
        the last `updateflow()` call ends.
        """
        pass

    def shouldupdate(self, newvalues, updatedvalues):
        """
        Callback when upate. Rewrite this method to ignore some updates.
        
        If this callback returns False, the update is ignored.
        """
        return True

    async def restart_walk(self):
        """
        Force a re-walk
        """
        if not self._restartwalk:
            self._restartwalk = True
            await self.wait_for_send(FlowUpdaterNotification(self, FlowUpdaterNotification.STARTWALK))

    async def _dataobject_update_detect(self, _initialkeys, _savedresult):
        """
        Coroutine that wait for retrieved value update notification
        """
        def expr(newvalues, updatedvalues):
            if any(v.getkey() in _initialkeys for v in updatedvalues if v is not None):
                return True
            else:
                return self.shouldupdate(newvalues, updatedvalues)
        while True:
            updatedvalues, _ = await multiwaitif(_savedresult, self, expr, True)
            if not self._updatedset:
                self.scheduler.emergesend(FlowUpdaterNotification(self, FlowUpdaterNotification.DATAUPDATED))
            self._updatedset.update(updatedvalues)

    def updateobjects(self, updatedvalues):
        """
        Force a update notification on specified objects, even if they are not actually updated
        in ObjectDB
        """
        if not self._updatedset:
            self.scheduler.emergesend(FlowUpdaterNotification(self, FlowUpdaterNotification.DATAUPDATED))
        self._updatedset.update(set(updatedvalues).intersection(self._savedresult))

    async def _flowupdater(self):
        """
        Coroutine calling `updateflow()`
        """
        lastresult = set(v for v in self._savedresult if v is not None and not v.isdeleted())
        flowupdate = FlowUpdaterNotification.createMatcher(self, FlowUpdaterNotification.FLOWUPDATE)
        while True:
            currentresult = [v for v in self._savedresult if v is not None and not v.isdeleted()]
            # Calculating differences
            additems = []
            updateditems = []
            updatedset2 = self._updatedset2
            for v in currentresult:
                if v not in lastresult:
                    additems.append(v)
                else:
                    lastresult.remove(v)
                    if v in updatedset2:
                        # Updated
                        updateditems.append(v)
            removeitems = lastresult
            self._updatedset2.clear()
            # Save current result for next difference
            lastresult = set(currentresult)
            if not additems and not removeitems and not updateditems:
                await flowupdate
                continue
            await self.updateflow(self._connection, set(additems), removeitems, set(updateditems))

    async def main(self):
        """
        Main coroutine
        """
        try:
            lastkeys = set()
            dataupdate = FlowUpdaterNotification.createMatcher(self, FlowUpdaterNotification.DATAUPDATED)
            startwalk = FlowUpdaterNotification.createMatcher(self, FlowUpdaterNotification.STARTWALK)
            self.subroutine(self._flowupdater(), False, '_flowupdateroutine')
            # Cache updated objects
            presave_update = set()
            while True:
                self._restartwalk = False
                presave_update.update(self._updatedset)
                self._updatedset.clear()
                _initialkeys = set(self._initialkeys)
                try:
                    walk_result = await call_api(self, 'objectdb', 'walk',
                                                        {'keys': self._initialkeys, 'walkerdict': self._walkerdict,
                                                         'requestid': (self._requstid, self._requestindex)})
                except Exception:
                    self._logger.warning("Flow updater %r walk step failed, conn = %r", self, self._connection,
                                         exc_info=True)
                    # Cleanup
                    await call_api(self, 'objectdb', 'unwatchall',
                                         {'requestid': (self._requstid, self._requestindex)})
                    await self.wait_with_timeout(2)
                    self._requestindex += 1
                if self._restartwalk:
                    continue
                if self._updatedset:
                    if any(v.getkey() in _initialkeys for v in self._updatedset):
                        # During walk, there are other initial keys that are updated
                        # To make sure we get the latest result, restart the walk
                        continue
                lastkeys = set(self._savedkeys)
                _savedkeys, _savedresult = walk_result
                removekeys = tuple(lastkeys.difference(_savedkeys))
                self.reset_initialkeys(_savedkeys, _savedresult)
                _initialkeys = set(self._initialkeys)
                if self._dataupdateroutine:
                    self.terminate(self._dataupdateroutine)
                # Start detecting updates
                self.subroutine(self._dataobject_update_detect(_initialkeys, _savedresult), False, "_dataupdateroutine")
                # Set the updates back (potentially merged with newly updated objects)
                self._updatedset.update(v for v in presave_update)
                presave_update.clear()
                await self.walkcomplete(_savedkeys, _savedresult)
                if removekeys:
                    await call_api(self, 'objectdb', 'munwatch', {'keys': removekeys,
                                                                  'requestid': (self._requstid, self._requestindex)})
                # Transfer updated objects to updatedset2 before a flow update notification
                # This helps to make `walkcomplete` executes before `updateflow`
                #
                # But notice that since there is only a single data object copy in all the program,
                # it is impossible to hide the change completely during `updateflow`
                self._updatedset2.update(self._updatedset)
                self._updatedset.clear()
                self._savedkeys = _savedkeys
                self._savedresult = _savedresult
                await self.wait_for_send(FlowUpdaterNotification(self, FlowUpdaterNotification.FLOWUPDATE))
                while not self._restartwalk:
                    if self._updatedset:
                        if any(v.getkey() in _initialkeys for v in self._updatedset):
                            break
                        else:
                            self._updatedset2.update(self._updatedset)
                            self._updatedset.clear()
                            self.scheduler.emergesend(FlowUpdaterNotification(self, FlowUpdaterNotification.FLOWUPDATE))
                    await M_(dataupdate, startwalk)
        except Exception:
            self._logger.exception("Flow updater %r stops update by an exception, conn = %r", self, self._connection)
            raise
        finally:
            self.subroutine(send_api(self, 'objectdb', 'unwatchall', {'requestid': (self._requstid, self._requestindex)}),
                            False)
            if self._flowupdateroutine:
                self.terminate(self._flowupdateroutine)
                self._flowupdateroutine = None
            if self._dataupdateroutine:
                self.terminate(self._dataupdateroutine)
                self._dataupdateroutine = None

    async def execute_commands(self, conn, cmds):
        if cmds:
            try:
                _, openflow_replydict = await conn.protocol.batch(cmds, conn, self)
            except OpenflowErrorResultException as exc:
                self._logger.warning("Some Openflow commands return error result on connection %r, will ignore and continue.\n"
                                             "Details:\n%s", conn,
                                             "\n".join("REQUEST = \n%s\nERRORS = \n%s\n" % (json.dumps(dump(k, tostr=True), indent=2),
                                                                                            json.dumps(dump(v, tostr=True), indent=2))
                                                       for k,v in exc.result[1].items()))

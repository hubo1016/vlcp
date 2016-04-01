'''
Created on 2016/3/24

:author: hubo
'''
from vlcp.config.config import defaultconfig
import vlcp.service.kvdb.storage as storage
import vlcp.service.kvdb.redisnotifier as redisnotifier
from vlcp.server.module import depend, Module, callAPI, api
import vlcp.utils.dataobject as dataobj
from vlcp.event.runnable import RoutineContainer
from vlcp.event.event import Event, withIndices
from time import time
from copy import deepcopy
from vlcp.event.core import QuitException

@withIndices()
class RetrieveRequestSend(Event):
    pass

@withIndices('id')
class RetrieveReply(Event):
    pass

@defaultconfig
@depend(storage.KVStorage, redisnotifier.UpdateNotifier)
class ObjectDB(Module):
    service = True
    def __init__(self, server):
        Module.__init__(self, server)
        self._managed_objs = {}
        self._watches = {}
        self._watchedkeys = set()
        self._requests = []
        self._transactno = 0
        self._stale = False
        self.apiroutine = RoutineContainer(self.scheduler)
        self.apiroutine.main = self._update
        self.routines.append(self.apiroutine)
        self.createAPI(api(self.mget, self.apiroutine),
                       api(self.get, self.apiroutine),
                       api(self.mgetonce, self.apiroutine),
                       api(self.getonce, self.apiroutine),
                       api(self.mwatch, self.apiroutine),
                       api(self.watch, self.apiroutine),
                       api(self.munwatch, self.apiroutine),
                       api(self.unwatch, self.apiroutine),
                       api(self.transact, self.apiroutine),
                       api(self.watchlist, self.apiroutine)
                       )
    def load(self, container):
        for m in callAPI(container, 'updatenotifier', 'createnotifier'):
            yield m
        self._notifier = container.retvalue
        for m in Module.load(self, container):
            yield m
        self.routines.append(self._notifier)
    def _update(self):
        update_keys = set()
        timestamp = '%012x' % (int(time() * 1000),) + '-'
        notification_matcher = self._notifier.notification_matcher()
        def updateinner():
            processing_requests = []
            loopCount = 0
            # New managed keys
            retrieve_list = set()
            retrieveonce_list = set()
            update_result = {}
            while (retrieve_list or update_keys or self._requests):
                watch_keys = set()
                # Updated keys
                update_list = set()
                if loopCount >= 10 and not retrieve_list:
                    if not update_keys:
                        break
                    elif loopCount >= 100:
                        # Too many updates, we must stop to respond
                        self._logger.warning("There are still database updates after 100 loops of mget, respond with potential inconsistent values")
                        break
                if update_keys:
                    update_list.update(update_keys)
                    update_keys.clear()
                if self._requests:
                    # Processing requests
                    for r in self._requests:
                        if r[2] == 'unwatch':
                            for k in r[0]:
                                s = self._watches.get(k)
                                if s:
                                    s.discard(r[3])
                                    if not s:
                                        del self._watches[k]
                            # Do not need to wait
                            for m in self.apiroutine.waitForSend(RetrieveReply(r[1], result = None)):
                                yield m
                        elif r[2] == 'watch':
                            retrieve_list.update(r[0])
                            for k in r[0]:
                                self._watches.setdefault(k, set()).add(r[3])
                            processing_requests.append(r)
                        elif r[2] == 'get':
                            retrieve_list.update(r[0])
                            processing_requests.append(r)
                        else:
                            retrieveonce_list.update(r[0])
                            processing_requests.append(r)
                    del self._requests[:]
                if retrieve_list:
                    watch_keys.update(retrieve_list)
                # Add watch_keys to notification
                watch_keys.difference_update(self._watchedkeys)
                if watch_keys:
                    for m in self._notifier.add_listen(*tuple(watch_keys.difference(self._watchedkeys))):
                        yield m
                    self._watchedkeys.update(watch_keys)
                get_list = list(update_list.union(retrieve_list.union(retrieveonce_list).difference(self._managed_objs.keys()).difference(update_result.keys())))
                if get_list:
                    try:
                        for m in callAPI(self.apiroutine, 'kvstorage', 'mget', {'keys': get_list}):
                            yield m
                    except QuitException:
                        raise
                    except Exception:
                        # Serve with cache
                        self._logger.warning('KVStorage retrieve failed, serve with cache', exc_info = True)
                        self._stale = True
                        # Discard all retrieved results
                        update_result.clear()
                        # Retry update later
                        update_keys.update(update_list)
                        break
                    else:
                        result = self.apiroutine.retvalue
                        self._stale = False
                    update_result.update(zip(get_list, result))
                    new_retreive_list = set()
                    for k,v in zip(get_list, result):
                        if v is not None:
                            if hasattr(v, 'setkey'):
                                v.setkey(k)
                            if k in update_list or k in retrieve_list:
                                if hasattr(v, 'kvdb_retrievelist'):
                                    newrt = set(v.kvdb_retrievelist())
                                    if newrt.difference(self._watchedkeys):
                                        # There are some new keys to retrieve. The value itself may be updated
                                        # before the next retrieve returns, we will always assume that there
                                        # may be an update
                                        update_keys.add(k)
                                    new_retreive_list.update(newrt)
                else:
                    new_retreive_list = set()
                retrieve_list.clear()
                retrieve_list.update(new_retreive_list)
                loopCount += 1
            # Update result
            send_events = []
            self._transactno += 1
            transactid = '%s%016x' % (timestamp, self._transactno)
            update_objs = []
            for k,v in update_result.items():
                if k in self._watchedkeys:
                    if v is None:
                        oldv = self._managed_objs.get(k)
                        if oldv is not None:
                            if hasattr(oldv, 'kvdb_detach'):
                                oldv.kvdb_detach()
                                update_objs.append((k, oldv, dataobj.DataObjectUpdateEvent.DELETED))
                            else:
                                update_objs.append((k, None, dataobj.DataObjectUpdateEvent.DELETED))
                            del self._managed_objs[k]
                    else:
                        oldv = self._managed_objs.get(k)
                        if oldv is not None:
                            if oldv != v:
                                if oldv and hasattr(oldv, 'kvdb_update'):
                                    oldv.kvdb_update(v)
                                    update_objs.append((k, oldv, dataobj.DataObjectUpdateEvent.UPDATED))
                                else:
                                    if hasattr(oldv, 'kvdb_detach'):
                                        oldv.kvdb_detach()
                                    self._managed_objs[k] = v
                                    update_objs.append((k, v, dataobj.DataObjectUpdateEvent.UPDATED))
                        else:
                            self._managed_objs[k] = v
                            update_objs.append((k, v, dataobj.DataObjectUpdateEvent.UPDATED))
            for k in update_result.keys():
                v = self._managed_objs.get(k)
                if v is not None and hasattr(v, 'kvdb_retrievefinished'):
                    v.kvdb_retrievefinished(self._managed_objs)
            allkeys = tuple(k for k,_,_ in update_objs)
            send_events.extend((dataobj.DataObjectUpdateEvent(k, transactid, t, object = v, allkeys = allkeys) for k,v,t in update_objs))
            # Process requests
            for r in processing_requests:
                if r[2] == 'get':
                    objs = [self._managed_objs.get(k) for k in r[0]]
                    for k,v in zip(r[0], objs):
                        if v is not None:
                            self._watches.setdefault(k, set()).add(r[3])
                    result = [o.create_reference() if o is not None and hasattr(o, 'create_reference') else o
                              for o in objs]
                elif r[2] == 'watch':
                    result = [(v.create_reference() if hasattr(v, 'create_reference') else v)
                              if v is not None else dataobj.ReferenceObject(k)
                              for k,v in ((k,self._managed_objs.get(k)) for k in r[0])]
                else:
                    result = [deepcopy(update_result.get(k, self._managed_objs.get(k))) for k in r[0]]
                send_events.append(RetrieveReply(r[1], result = result))
            # Use DFS to remove unwatched objects
            mark_set = set()
            def dfs(k):
                if k in mark_set:
                    return
                mark_set.add(k)
                v = self._managed_objs.get(k)
                if v is not None and hasattr(v, 'kvdb_internalref'):
                    for k2 in v.kvdb_internalref():
                        dfs(k2)
            for k in self._watches.keys():
                dfs(k)
            remove_keys = self._watchedkeys.difference(mark_set)
            if remove_keys:
                self._watchedkeys.difference_update(remove_keys)
                for m in self._notifier.remove_listen(*tuple(remove_keys)):
                    yield m
                for k in remove_keys:
                    if k in self._managed_objs:
                        del self._managed_objs[k]
            for e in send_events:
                for m in self.apiroutine.waitForSend(e):
                    yield m
        request_matcher = RetrieveRequestSend.createMatcher()
        def onupdate(event, matcher):
            update_keys.update(self._watchedkeys.intersection(event.keys))
        while True:
            if not update_keys and not self._requests:
                yield (notification_matcher, request_matcher)
                if self.apiroutine.matcher is notification_matcher:
                    onupdate(self.apiroutine.event)
                for m in self.apiroutine.withCallback(updateinner(), onupdate, notification_matcher):
                    yield m
    def mget(self, keys, requestid):
        "Get multiple objects and manage them. Return references to the objects."
        notify = not self._requests
        rid = object()
        self._requests.append((tuple(keys), rid, 'get', requestid))
        if notify:
            for m in self.apiroutine.waitForSend(RetrieveRequestSend()):
                yield m
        yield (RetrieveReply.createMatcher(rid),)
        self.apiroutine.retvalue = self.apiroutine.event.result
    def get(self, key, requestid):
        "Get an object from specified key, and manage the object. Return a reference to the object or None if not exists."
        for m in self.mget([key], requestid):
            yield m
        self.apiroutine.retvalue = self.apiroutine.retvalue[0]
    def mgetonce(self, keys):
        "Get multiple objects, return copies of them. Referenced objects are not retrieved."
        notify = not self._requests
        rid = object()
        self._requests.append((tuple(keys), rid, 'getonce'))
        if notify:
            for m in self.apiroutine.waitForSend(RetrieveRequestSend()):
                yield m
        yield (RetrieveReply.createMatcher(rid),)
        self.apiroutine.retvalue = self.apiroutine.event.result
    def getonce(self, key):
        "Get a object without manage it. Return a copy of the object, or None if not exists. Referenced objects are not retrieved."
        for m in self.mgetonce([key]):
            yield m
        self.apiroutine.retvalue = self.apiroutine.retvalue[0]
    def watch(self, key, requestid):
        "Try to find an object and return a reference. Use reference.isdeleted() to test whether the object exists. "\
        "Use reference.wait(container) to wait for the object to be existed. Use reference.release() to cancel the watch."
        for m in self.mwatch([key], requestid):
            yield m
        self.apiroutine.retvalue = self.apiroutine.retvalue[0]
    def mwatch(self, keys, requestid):
        "Try to return all the references, see watch()"
        notify = not self._requests
        rid = object()
        self._requests.append((tuple(keys), rid, 'watch', requestid))
        if notify:
            for m in self.apiroutine.waitForSend(RetrieveRequestSend()):
                yield m
        yield (RetrieveReply.createMatcher(rid),)
        self.apiroutine.retvalue = self.apiroutine.event.result
    def unwatch(self, key, requestid):
        "Cancel management of a key"
        for m in self.munwatch([key], requestid):
            yield m
        self.apiroutine.retvalue = None
    def munwatch(self, keys, requestid):
        "Cancel management of keys"
        notify = not self._requests
        rid = object()
        self._requests.append((tuple(keys), rid, 'unwatch', requestid))
        if notify:
            for m in self.apiroutine.waitForSend(RetrieveRequestSend()):
                yield m
        yield (RetrieveReply.createMatcher(rid),)
        self.apiroutine.retvalue = None
    def transact(self, keys, updater):
        "Try to update keys in a transact, with an updater(keys, values), which returns (updated_keys, updated_values). "\
        "The updater may be called more than once."
        updated_keys_ref = [None]
        def object_updater(keys, values):
            for k, v in zip(keys, values):
                if v is not None and hasattr(v, 'setkey'):
                    v.setkey(k)
            updated_keys, updated_values = updater(keys, values)
            updated_keys_ref[0] = tuple(updated_keys)
            return (updated_keys, updated_values)
        for m in callAPI(self.apiroutine, 'kvstorage', 'updateall', {'keys': keys, 'updater': object_updater}):
            yield m
        for m in self._notifier.publish(*updated_keys_ref[0]):
            yield m
    def watchlist(self, requestid = None):
        "Return a dictionary whose keys are database keys, and values are lists of request ids. Optionally filtered by request id"
        return dict((k,list(v)) for k,v in self._watches.items() if requestid is None or requestid in v)

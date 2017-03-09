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
from vlcp.event.core import QuitException, syscall_removequeue
import itertools
from vlcp.utils.dataobject import AlreadyExistsException, UniqueKeyReference,\
    MultiKeyReference, DataObjectSet, UniqueKeySet, WeakReferenceObject,\
    MultiKeySet, ReferenceObject

@withIndices()
class RetrieveRequestSend(Event):
    pass

@withIndices('id')
class RetrieveReply(Event):
    pass

def _str(b):
    if isinstance(b, str):
        return b
    elif isinstance(b, bytes):
        return b.decode('utf-8')
    else:
        return str(b)

def _str2(b):
    if isinstance(b, str):
        return b
    elif isinstance(b, bytes):
        return b.decode('utf-8')
    elif hasattr(b, 'getkey'):
        return b.getkey()
    else:
        return str(b)


class StaleResultException(Exception):
    def __init__(self, result, desc = "Result is stale"):
        Exception.__init__(desc)
        self.result = result

class _NeedMoreKeysException(Exception):
    pass

@defaultconfig
@depend(storage.KVStorage, redisnotifier.UpdateNotifier)
class ObjectDB(Module):
    """
    Abstract transaction layer for KVDB
    """
    service = True
    # Priority for object update event
    _default_objectupdatepriority = 450
    # Enable debugging mode for updater: all updaters will be called for an extra time
    # to make sure it does not crash with multiple calls
    _default_debuggingupdater = False
    def __init__(self, server):
        Module.__init__(self, server)
        self._managed_objs = {}
        self._watches = {}
        self._watchedkeys = set()
        self._requests = []
        self._transactno = 0
        self._stale = False
        self._updatekeys = set()
        self._update_version = {}
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
                       api(self.unwatchall, self.apiroutine),
                       api(self.transact, self.apiroutine),
                       api(self.watchlist),
                       api(self.walk, self.apiroutine)
                       )
    def load(self, container):
        self.scheduler.queue.addSubQueue(\
                self.objectupdatepriority, dataobj.DataObjectUpdateEvent.createMatcher(), 'dataobjectupdate')
        for m in callAPI(container, 'updatenotifier', 'createnotifier'):
            yield m
        self._notifier = container.retvalue
        for m in Module.load(self, container):
            yield m
        self.routines.append(self._notifier)
    def unload(self, container, force=False):
        for m in container.syscall(syscall_removequeue(self.scheduler.queue, 'dataobjectupdate')):
            yield m
        for m in Module.unload(self, container, force=force):
            yield m
    def _update(self):
        timestamp = '%012x' % (int(time() * 1000),) + '-'
        notification_matcher = self._notifier.notification_matcher(False)
        def copywithkey(obj, key):
            newobj = deepcopy(obj)
            if hasattr(newobj, 'setkey'):
                newobj.setkey(key)
            return newobj
        def getversion(obj):
            if obj is None:
                return (0, -1)
            else:
                return (getattr(obj, 'kvdb_createtime', 0), getattr(obj, 'kvdb_updateversion', 0))
        def isnewer(obj, version):
            if obj is None:
                return version[1] != -1
            else:
                return getversion(obj) > version
        request_matcher = RetrieveRequestSend.createMatcher()
        def onupdate(event, matcher):
            update_keys = self._watchedkeys.intersection([_str(k) for k in event.keys])
            self._updatekeys.update(update_keys)
            if event.extrainfo:
                for k,v in zip(event.keys, event.extrainfo):
                    k = _str(k)
                    if k in update_keys:
                        v = tuple(v)
                        oldv = self._update_version.get(k, (0, -1))
                        if oldv < v:
                            self._update_version[k] = v
            else:
                for k in event.keys:
                    try:
                        del self._update_version[_str(k)]
                    except KeyError:
                        pass
        def updateinner():
            processing_requests = []
            # New managed keys
            retrieve_list = set()
            orig_retrieve_list = set()
            retrieveonce_list = set()
            orig_retrieveonce_list = set()
            # Retrieved values are stored in update_result before merging into current storage
            update_result = {}
            # key => [(walker_func, original_keys, rid), ...]
            walkers = {}
            self._loopCount = 0
            # A request-id -> retrieve set dictionary to store the saved keys
            savelist = {}
            def updateloop():
                while (retrieve_list or self._updatekeys or self._requests):
                    watch_keys = set()
                    # Updated keys
                    update_list = set()
                    if self._loopCount >= 10 and not retrieve_list:
                        if not self._updatekeys:
                            break
                        elif self._loopCount >= 100:
                            # Too many updates, we must stop to respond
                            self._logger.warning("There are still database updates after 100 loops of mget, respond with potential inconsistent values")
                            break
                    if self._updatekeys:
                        update_list.update(self._updatekeys)
                        self._updatekeys.clear()
                    if self._requests:
                        # Processing requests
                        for r in self._requests:
                            if r[2] == 'unwatch':
                                try:
                                    for k in r[0]:
                                        s = self._watches.get(k)
                                        if s:
                                            s.discard(r[3])
                                            if not s:
                                                del self._watches[k]
                                    # Do not need to wait
                                except Exception as exc:
                                    for m in self.apiroutine.waitForSend(RetrieveReply(r[1], exception = exc)):
                                        yield m                                    
                                else:
                                    for m in self.apiroutine.waitForSend(RetrieveReply(r[1], result = None)):
                                        yield m
                            elif r[2] == 'watch':
                                retrieve_list.update(r[0])
                                orig_retrieve_list.update(r[0])
                                for k in r[0]:
                                    self._watches.setdefault(k, set()).add(r[3])
                                processing_requests.append(r)
                            elif r[2] == 'get':
                                retrieve_list.update(r[0])
                                orig_retrieve_list.update(r[0])
                                processing_requests.append(r)
                            elif r[2] == 'walk':
                                retrieve_list.update(r[0])
                                processing_requests.append(r)
                                for k,v in r[3].items():
                                    walkers.setdefault(k, []).append((v, (r[0], r[1])))
                            else:
                                retrieveonce_list.update(r[0])
                                orig_retrieveonce_list.update(r[0])
                                processing_requests.append(r)
                        del self._requests[:]
                    if retrieve_list:
                        watch_keys.update(retrieve_list)
                    # Add watch_keys to notification
                    watch_keys.difference_update(self._watchedkeys)
                    if watch_keys:
                        for k in watch_keys:
                            if k in update_result:
                                self._update_version[k] = getversion(update_result[k])
                        for m in self._notifier.add_listen(*tuple(watch_keys.difference(self._watchedkeys))):
                            yield m
                        self._watchedkeys.update(watch_keys)
                    get_list_set = update_list.union(retrieve_list.union(retrieveonce_list).difference(self._managed_objs.keys()).difference(update_result.keys()))
                    get_list = list(get_list_set)
                    if get_list:
                        try:
                            for m in callAPI(self.apiroutine, 'kvstorage', 'mget', {'keys': get_list}):
                                yield m
                        except QuitException:
                            raise
                        except Exception:
                            # Serve with cache
                            if not self._stale:
                                self._logger.warning('KVStorage retrieve failed, serve with cache', exc_info = True)
                            self._stale = True
                            # Discard all retrieved results
                            update_result.clear()
                            # Retry update later
                            self._updatekeys.update(update_list)
                            #break
                            changed_set = set()
                        else:
                            result = self.apiroutine.retvalue
                            self._stale = False
                            for k,v in zip(get_list, result):
                                if v is not None and hasattr(v, 'setkey'):
                                    v.setkey(k)
                                if k in self._watchedkeys and k not in self._update_version:
                                    self._update_version[k] = getversion(v)
                            changed_set = set(k for k,v in zip(get_list, result) if k not in update_result or getversion(v) != getversion(update_result[k]))
                            update_result.update(zip(get_list, result))
                    else:
                        changed_set = set()
                    # All keys which should be retrieved in next loop
                    new_retrieve_list = set()
                    # Keys which should be retrieved in next loop for a single walk
                    new_retrieve_keys = set()
                    # Keys that are used in current walk will be retrieved again in next loop
                    used_keys = set()
                    # We separate the original data and new retrieved data space, and do not allow
                    # cross usage, to prevent discontinue results 
                    def walk_original(key):
                        if hasattr(key, 'getkey'):
                            key = key.getkey()
                        key = _str(key)
                        if key not in self._watchedkeys:
                            # This key is not retrieved, raise a KeyError, and record this key
                            new_retrieve_keys.add(key)
                            raise KeyError('Not retrieved')
                        elif self._stale:
                            if key not in self._managed_objs:
                                new_retrieve_keys.add(key)
                            else:
                                used_keys.add(key)
                            return self._managed_objs.get(key)
                        elif key in changed_set:
                            # We are retrieving from the old result, do not allow to use new data
                            used_keys.add(key)
                            new_retrieve_keys.add(key)
                            raise KeyError('Not retrieved')
                        elif key in update_result:
                            used_keys.add(key)
                            return update_result[key]
                        elif key in self._managed_objs:
                            used_keys.add(key)
                            return self._managed_objs[key]
                        else:
                            # This key is not retrieved, raise a KeyError, and record this key
                            new_retrieve_keys.add(key)
                            raise KeyError('Not retrieved')
                    def walk_new(key):
                        if hasattr(key, 'getkey'):
                            key = key.getkey()
                        key = _str(key)
                        if key not in self._watchedkeys:
                            # This key is not retrieved, raise a KeyError, and record this key
                            new_retrieve_keys.add(key)
                            raise KeyError('Not retrieved')
                        elif key in get_list_set:
                            # We are retrieving from the new data
                            used_keys.add(key)
                            return update_result[key]
                        elif key in self._managed_objs or key in update_result:
                            # Do not allow the old data
                            used_keys.add(key)
                            new_retrieve_keys.add(key)
                            raise KeyError('Not retrieved')
                        else:
                            # This key is not retrieved, raise a KeyError, and record this key
                            new_retrieve_keys.add(key)
                            raise KeyError('Not retrieved')
                    def create_walker(orig_key):
                        if self._stale:
                            return walk_original
                        elif orig_key in changed_set:
                            return walk_new
                        else:
                            return walk_original
                    walker_set = set()
                    def default_walker(key, obj, walk):
                        if key in walker_set:
                            return
                        else:
                            walker_set.add(key)
                        if hasattr(obj, 'kvdb_retrievelist'):
                            rl = obj.kvdb_retrievelist()
                            for k in rl:
                                try:
                                    newobj = walk(k)
                                except KeyError:
                                    pass
                                else:
                                    if newobj is not None:
                                        default_walker(k, newobj, walk)
                    for k in orig_retrieve_list:
                        v = update_result.get(k)
                        if v is not None:
                            new_retrieve_keys.clear()
                            used_keys.clear()
                            default_walker(k, v, create_walker(k))
                            if new_retrieve_keys:
                                new_retrieve_list.update(new_retrieve_keys)
                                self._updatekeys.update(used_keys)
                                self._updatekeys.add(k)
                    savelist.clear()
                    for k,ws in walkers.items():
                        # k: the walker key
                        # ws: list of [walker_func, (request_original_keys, rid)]
                        # Retry every walker, starts with k, with the value of v
                        if k in update_result:
                            # The value is newly retrieved
                            v = update_result.get(k)
                        else:
                            # Use the stored value
                            v = self._managed_objs.get(k)
                        if ws:
                            for w,r in list(ws):
                                # w: walker_func
                                # r: (request_original_keys, rid)
                                # Custom walker
                                def save(key):
                                    if hasattr(key, 'getkey'):
                                        key = key.getkey()
                                    key = _str(key)
                                    if key != k and key not in used_keys:
                                        raise ValueError('Cannot save a key without walk')
                                    savelist.setdefault(r[1], set()).add(key)
                                try:
                                    new_retrieve_keys.clear()
                                    used_keys.clear()
                                    w(k, v, create_walker(k), save)
                                except Exception as exc:
                                    # if one walker failed, the whole request is failed, remove all walkers
                                    for orig_k in r[0]:
                                        if orig_k in walkers:
                                            walkers[orig_k][:] = [(w0, r0) for w0,r0 in walkers[orig_k] if r0[1] != r[1]]
                                    processing_requests[:] = [r0 for r0 in processing_requests if r0[1] != r[1]]
                                    for m in self.apiroutine.waitForSend(RetrieveReply(r[1], exception = exc)):
                                        yield m
                                else:
                                    if new_retrieve_keys:
                                        new_retrieve_list.update(new_retrieve_keys)
                                        self._updatekeys.update(used_keys)
                                        self._updatekeys.add(k)
                    for save in savelist.values():
                        for k in save:
                            v = update_result.get(k)
                            if v is not None:
                                # If we retrieved a new value, we should also retrieved the references
                                # from this value
                                new_retrieve_keys.clear()
                                used_keys.clear()
                                default_walker(k, v, create_walker(k))
                                if new_retrieve_keys:
                                    new_retrieve_list.update(new_retrieve_keys)
                                    self._updatekeys.update(used_keys)
                                    self._updatekeys.add(k)                            
                    retrieve_list.clear()
                    retrieveonce_list.clear()
                    retrieve_list.update(new_retrieve_list)
                    self._loopCount += 1
                    if self._stale:
                        watch_keys = set(retrieve_list)
                        watch_keys.difference_update(self._watchedkeys)
                        if watch_keys:
                            for m in self._notifier.add_listen(*tuple(watch_keys)):
                                yield m
                            self._watchedkeys.update(watch_keys)
                        break
            while True:
                for m in self.apiroutine.withCallback(updateloop(), onupdate, notification_matcher):
                    yield m
                if self._loopCount >= 100 or self._stale:
                    break
                # If some updated result is newer than the notification version, we should wait for the notification
                should_wait = False
                for k,v in update_result.items():
                    if k in self._watchedkeys:
                        oldv = self._update_version.get(k)
                        if oldv is not None and isnewer(v, oldv):
                            should_wait = True
                            break
                if should_wait:
                    for m in self.apiroutine.waitWithTimeout(0.2, notification_matcher):
                        yield m
                    if self.apiroutine.timeout:
                        break
                    else:
                        onupdate(self.apiroutine.event, self.apiroutine.matcher)
                else:
                    break
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
                elif r[2] == 'walk':
                    saved_keys = list(savelist.get(r[1], []))
                    for k in saved_keys:
                        self._watches.setdefault(k, set()).add(r[4])
                    objs = [self._managed_objs.get(k) for k in saved_keys]
                    result = (saved_keys,
                              [o.create_reference() if hasattr(o, 'create_reference') else o
                               if o is not None else dataobj.ReferenceObject(k)
                               for k,o in zip(saved_keys, objs)])
                else:
                    result = [copywithkey(update_result.get(k, self._managed_objs.get(k)), k) for k in r[0]]
                send_events.append(RetrieveReply(r[1], result = result, stale = self._stale))
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
            def output_result():
                remove_keys = self._watchedkeys.difference(mark_set)
                if remove_keys:
                    self._watchedkeys.difference_update(remove_keys)
                    for m in self._notifier.remove_listen(*tuple(remove_keys)):
                        yield m
                    for k in remove_keys:
                        if k in self._managed_objs:
                            del self._managed_objs[k]
                        if k in self._update_version:
                            del self._update_version[k]
                for e in send_events:
                    for m in self.apiroutine.waitForSend(e):
                        yield m
            for m in self.apiroutine.withCallback(output_result(), onupdate):
                yield m
        while True:
            if not self._updatekeys and not self._requests:
                yield (notification_matcher, request_matcher)
                if self.apiroutine.matcher is notification_matcher:
                    onupdate(self.apiroutine.event, self.apiroutine.matcher)
            for m in updateinner():
                yield m
    def mget(self, keys, requestid, nostale = False):
        "Get multiple objects and manage them. Return references to the objects."
        keys = tuple(_str2(k) for k in keys)
        notify = not self._requests
        rid = object()
        self._requests.append((keys, rid, 'get', requestid))
        if notify:
            for m in self.apiroutine.waitForSend(RetrieveRequestSend()):
                yield m
        yield (RetrieveReply.createMatcher(rid),)
        if hasattr(self.apiroutine.event, 'exception'):
            raise self.apiroutine.event.exception
        if nostale and self.apiroutine.event.stale:
            raise StaleResultException(self.apiroutine.event.result)
        self.apiroutine.retvalue = self.apiroutine.event.result
    def get(self, key, requestid, nostale = False):
        """
        Get an object from specified key, and manage the object.
        Return a reference to the object or None if not exists.
        """
        for m in self.mget([key], requestid, nostale):
            yield m
        self.apiroutine.retvalue = self.apiroutine.retvalue[0]
    def mgetonce(self, keys, nostale = False):
        "Get multiple objects, return copies of them. Referenced objects are not retrieved."
        keys = tuple(_str2(k) for k in keys)
        notify = not self._requests
        rid = object()
        self._requests.append((keys, rid, 'getonce'))
        if notify:
            for m in self.apiroutine.waitForSend(RetrieveRequestSend()):
                yield m
        yield (RetrieveReply.createMatcher(rid),)
        if hasattr(self.apiroutine.event, 'exception'):
            raise self.apiroutine.event.exception
        if nostale and self.apiroutine.event.stale:
            raise StaleResultException(self.apiroutine.event.result)
        self.apiroutine.retvalue = self.apiroutine.event.result
    def getonce(self, key, nostale = False):
        "Get a object without manage it. Return a copy of the object, or None if not exists. Referenced objects are not retrieved."
        for m in self.mgetonce([key], nostale):
            yield m
        self.apiroutine.retvalue = self.apiroutine.retvalue[0]
    def watch(self, key, requestid, nostale = False):
        """
        Try to find an object and return a reference. Use ``reference.isdeleted()`` to test
        whether the object exists.
        Use ``reference.wait(container)`` to wait for the object to be existed.
        """
        for m in self.mwatch([key], requestid, nostale):
            yield m
        self.apiroutine.retvalue = self.apiroutine.retvalue[0]
    def mwatch(self, keys, requestid, nostale = False):
        "Try to return all the references, see ``watch()``"
        keys = tuple(_str2(k) for k in keys)
        notify = not self._requests
        rid = object()
        self._requests.append(keys, rid, 'watch', requestid)
        if notify:
            for m in self.apiroutine.waitForSend(RetrieveRequestSend()):
                yield m
        yield (RetrieveReply.createMatcher(rid),)
        if hasattr(self.apiroutine.event, 'exception'):
            raise self.apiroutine.event.exception
        if nostale and self.apiroutine.event.stale:
            raise StaleResultException(self.apiroutine.event.result)
        self.apiroutine.retvalue = self.apiroutine.event.result
    def unwatch(self, key, requestid):
        "Cancel management of a key"
        for m in self.munwatch([key], requestid):
            yield m
        self.apiroutine.retvalue = None
    def unwatchall(self, requestid):
        "Cancel management for all keys that are managed by requestid"
        keys = [k for k,v in self._watches.items() if requestid in v]
        for m in self.munwatch(keys, requestid):
            yield m
    def munwatch(self, keys, requestid):
        "Cancel management of keys"
        keys = tuple(_str2(k) for k in keys)
        notify = not self._requests
        rid = object()
        self._requests.append((keys, rid, 'unwatch', requestid))
        if notify:
            for m in self.apiroutine.waitForSend(RetrieveRequestSend()):
                yield m
        yield (RetrieveReply.createMatcher(rid),)
        if hasattr(self.apiroutine.event, 'exception'):
            raise self.apiroutine.event.exception
        self.apiroutine.retvalue = None
    def transact(self, keys, updater, withtime = False):
        """
        Try to update keys in a transact, with an ``updater(keys, values)``,
        which returns ``(updated_keys, updated_values)``.
        
        The updater may be called more than once. If ``withtime = True``,
        the updater should take three parameters:
        ``(keys, values, timestamp)`` with timestamp as the server time
        """
        keys = tuple(_str2(k) for k in keys)
        updated_ref = [None, None]
        extra_keys = []
        extra_key_set = []
        auto_remove_keys = set()
        orig_len = len(keys)
        def updater_with_key(keys, values, timestamp):
            # Automatically manage extra keys
            remove_uniquekeys = []
            remove_multikeys = []
            update_uniquekeys = []
            update_multikeys = []
            keystart = orig_len + len(auto_remove_keys)
            for v in values[:keystart]:
                if v is not None:
                    if hasattr(v, 'kvdb_uniquekeys'):
                        remove_uniquekeys.extend((k,v.create_weakreference()) for k in v.kvdb_uniquekeys())
                    if hasattr(v, 'kvdb_multikeys'):
                        remove_multikeys.extend((k,v.create_weakreference()) for k in v.kvdb_multikeys())
            if self.debuggingupdater:
                # Updater may be called more than once, ensure that this updater does not crash
                # on multiple calls
                kc = keys[:orig_len]
                vc = [v.clone_instance() if v is not None and hasattr(v, 'clone_instance') else deepcopy(v) for v in values[:orig_len]]
                if withtime:
                    updated_keys, updated_values = updater(kc, vc, timestamp)
                else:
                    updated_keys, updated_values = updater(kc, vc)
            if withtime:
                updated_keys, updated_values = updater(keys[:orig_len], values[:orig_len], timestamp)
            else:
                updated_keys, updated_values = updater(keys[:orig_len], values[:orig_len])
            for v in updated_values:
                if v is not None:
                    if hasattr(v, 'kvdb_uniquekeys'):
                        update_uniquekeys.extend((k,v.create_weakreference()) for k in v.kvdb_uniquekeys())
                    if hasattr(v, 'kvdb_multikeys'):
                        update_multikeys.extend((k,v.create_weakreference()) for k in v.kvdb_multikeys())
            extrakeysdict = dict(zip(keys[keystart:keystart + len(extra_keys)], values[keystart:keystart + len(extra_keys)]))
            extrakeysetdict = dict(zip(keys[keystart + len(extra_keys):keystart + len(extra_keys) + len(extra_key_set)],
                                       values[keystart + len(extra_keys):keystart + len(extra_keys) + len(extra_key_set)]))
            tempdict = {}
            old_values = dict(zip(keys, values))
            updated_keyset = set(updated_keys)
            try:
                append_remove = set()
                autoremove_keys = set()
                # Use DFS to find auto remove keys
                def dfs(k):
                    if k in autoremove_keys:
                        return
                    autoremove_keys.add(k)
                    if k not in old_values:
                        append_remove.add(k)
                    else:
                        oldv = old_values[k]
                        if oldv is not None and hasattr(oldv, 'kvdb_autoremove'):
                            for k2 in oldv.kvdb_autoremove():
                                dfs(k2)
                for k,v in zip(updated_keys, updated_values):
                    if v is None:
                        dfs(k)
                if append_remove:
                    raise _NeedMoreKeysException()
                for k,v in remove_uniquekeys:
                    if v.getkey() not in updated_keyset and v.getkey() not in auto_remove_keys:
                        # This key is not updated, keep the indices untouched
                        continue
                    if k not in extrakeysdict:
                        raise _NeedMoreKeysException()
                    elif extrakeysdict[k] is not None and extrakeysdict[k].ref.getkey() == v.getkey():
                        # If the unique key does not reference to the correct object
                        # there may be an error, but we ignore this.
                        # Save in a temporary dictionary. We may restore it later.
                        tempdict[k] = extrakeysdict[k]
                        extrakeysdict[k] = None
                        setkey = UniqueKeyReference.get_keyset_from_key(k)
                        if setkey not in extrakeysetdict:
                            raise _NeedMoreKeysException()
                        else:
                            ks = extrakeysetdict[setkey]
                            if ks is None:
                                ks = UniqueKeySet.create_from_key(setkey)
                                extrakeysetdict[setkey] = ks
                            ks.set.dataset().discard(WeakReferenceObject(k))
                for k,v in remove_multikeys:
                    if v.getkey() not in updated_keyset and v.getkey() not in auto_remove_keys:
                        # This key is not updated, keep the indices untouched
                        continue
                    if k not in extrakeysdict:
                        raise _NeedMoreKeysException()
                    else:
                        mk = extrakeysdict[k]
                        if mk is not None:
                            mk.set.dataset().discard(v)
                            if not mk.set.dataset():
                                tempdict[k] = extrakeysdict[k]
                                extrakeysdict[k] = None
                                setkey = MultiKeyReference.get_keyset_from_key(k)
                                if setkey not in extrakeysetdict:
                                    raise _NeedMoreKeysException()
                                else:
                                    ks = extrakeysetdict[setkey]
                                    if ks is None:
                                        ks = MultiKeySet.create_from_key(setkey)
                                        extrakeysetdict[setkey] = ks
                                    ks.set.dataset().discard(WeakReferenceObject(k))
                for k,v in update_uniquekeys:
                    if k not in extrakeysdict:
                        raise _NeedMoreKeysException()
                    elif extrakeysdict[k] is not None and extrakeysdict[k].ref.getkey() != v.getkey():
                        raise AlreadyExistsException('Unique key conflict for %r and %r, with key %r' % \
                                                     (extrakeysdict[k].ref.getkey(), v.getkey(), k))
                    elif extrakeysdict[k] is None:
                        lv = tempdict.get(k, None)
                        if lv is not None and lv.ref.getkey() == v.getkey():
                            # Restore this value
                            nv = lv
                        else:
                            nv = UniqueKeyReference.create_from_key(k)
                            nv.ref = ReferenceObject(v.getkey())
                        extrakeysdict[k] = nv
                        setkey = UniqueKeyReference.get_keyset_from_key(k)
                        if setkey not in extrakeysetdict:
                            raise _NeedMoreKeysException()
                        else:
                            ks = extrakeysetdict[setkey]
                            if ks is None:
                                ks = UniqueKeySet.create_from_key(setkey)
                                extrakeysetdict[setkey] = ks
                            ks.set.dataset().add(nv.create_weakreference())
                for k,v in update_multikeys:
                    if k not in extrakeysdict:
                        raise _NeedMoreKeysException()
                    else:
                        mk = extrakeysdict[k]
                        if mk is None:
                            mk = tempdict.get(k, None)
                            if mk is None:
                                mk = MultiKeyReference.create_from_key(k)
                                mk.set = DataObjectSet()
                            setkey = MultiKeyReference.get_keyset_from_key(k)
                            if setkey not in extrakeysetdict:
                                raise _NeedMoreKeysException()
                            else:
                                ks = extrakeysetdict[setkey]
                                if ks is None:
                                    ks = MultiKeySet.create_from_key(setkey)
                                    extrakeysetdict[setkey] = ks
                                ks.set.dataset().add(mk.create_weakreference())
                        mk.set.dataset().add(v)
                        extrakeysdict[k] = mk
            except _NeedMoreKeysException:
                # Prepare the keys
                extra_keys[:] = list(set(itertools.chain((k for k,v in remove_uniquekeys if v.getkey() in updated_keyset or v.getkey() in autoremove_keys),
                                                         (k for k,v in remove_multikeys if v.getkey() in updated_keyset or v.getkey() in autoremove_keys),
                                                         (k for k,_ in update_uniquekeys),
                                                         (k for k,_ in update_multikeys))))
                extra_key_set[:] = list(set(itertools.chain((UniqueKeyReference.get_keyset_from_key(k) for k,v in remove_uniquekeys if v.getkey() in updated_keyset or v.getkey() in autoremove_keys),
                                                         (MultiKeyReference.get_keyset_from_key(k) for k,v in remove_multikeys if v.getkey() in updated_keyset or v.getkey() in autoremove_keys),
                                                         (UniqueKeyReference.get_keyset_from_key(k) for k,_ in update_uniquekeys),
                                                         (MultiKeyReference.get_keyset_from_key(k) for k,_ in update_multikeys))))
                auto_remove_keys.clear()
                auto_remove_keys.update(autoremove_keys.difference(keys[:orig_len])
                                                          .difference(extra_keys)
                                                          .difference(extra_key_set))
                raise
            else:
                extrakeys_list = list(extrakeysdict.items())
                extrakeyset_list = list(extrakeysetdict.items())
                autoremove_list = list(autoremove_keys.difference(updated_keys)
                                                      .difference(extrakeysdict.keys())
                                                      .difference(extrakeysetdict.keys()))
                return (tuple(itertools.chain(updated_keys,
                                              (k for k,_ in extrakeys_list),
                                              (k for k,_ in extrakeyset_list),
                                              autoremove_list)),
                        tuple(itertools.chain(updated_values,
                                               (v for _,v in extrakeys_list),
                                               (v for _,v in extrakeyset_list),
                                               [None] * len(autoremove_list))))
                        
        def object_updater(keys, values, timestamp):
            old_version = {}
            for k, v in zip(keys, values):
                if v is not None and hasattr(v, 'setkey'):
                    v.setkey(k)
                if v is not None and hasattr(v, 'kvdb_createtime'):
                    old_version[k] = (getattr(v, 'kvdb_createtime'), getattr(v, 'kvdb_updateversion', 1))
            updated_keys, updated_values = updater_with_key(keys, values, timestamp)
            updated_ref[0] = tuple(updated_keys)
            new_version = []
            for k,v in zip(updated_keys, updated_values):
                if v is None:
                    new_version.append((timestamp, -1))
                elif k in old_version:
                    ov = old_version[k]
                    setattr(v, 'kvdb_createtime', ov[0])
                    setattr(v, 'kvdb_updateversion', ov[1] + 1)
                    new_version.append((ov[0], ov[1] + 1))
                else:
                    setattr(v, 'kvdb_createtime', timestamp)
                    setattr(v, 'kvdb_updateversion', 1)
                    new_version.append((timestamp, 1))
            updated_ref[1] = new_version
            return (updated_keys, updated_values)
        while True:
            try:
                for m in callAPI(self.apiroutine, 'kvstorage', 'updateallwithtime',
                                 {'keys': keys + tuple(auto_remove_keys) + \
                                         tuple(extra_keys) + tuple(extra_key_set),
                                         'updater': object_updater}):
                    yield m
            except _NeedMoreKeysException:
                pass
            else:
                break
        # Short cut update notification
        update_keys = self._watchedkeys.intersection(updated_ref[0])
        self._updatekeys.update(update_keys)
        for k,v in zip(updated_ref[0], updated_ref[1]):
            k = _str(k)
            if k in update_keys:
                v = tuple(v)
                oldv = self._update_version.get(k, (0, -1))
                if oldv < v:
                    self._update_version[k] = v
        for m in self.apiroutine.waitForSend(RetrieveRequestSend()):
            yield m
        for m in self._notifier.publish(updated_ref[0], updated_ref[1]):
            yield m
    def watchlist(self, requestid = None):
        """
        Return a dictionary whose keys are database keys, and values are lists of request ids.
        Optionally filtered by request id
        """
        return dict((k,list(v)) for k,v in self._watches.items() if requestid is None or requestid in v)
    def walk(self, keys, walkerdict, requestid, nostale = False):
        """
        Recursively retrieve keys with customized functions.
        walkerdict is a dictionary ``key->walker(key, obj, walk, save)``.
        """
        keys = tuple(_str2(k) for k in keys)
        notify = not self._requests
        rid = object()
        self._requests.append((keys, rid, 'walk', dict(walkerdict), requestid))
        if notify:
            for m in self.apiroutine.waitForSend(RetrieveRequestSend()):
                yield m
        yield (RetrieveReply.createMatcher(rid),)
        if hasattr(self.apiroutine.event, 'exception'):
            raise self.apiroutine.event.exception
        if nostale and self.apiroutine.event.stale:
            raise StaleResultException(self.apiroutine.event.result)
        self.apiroutine.retvalue = self.apiroutine.event.result
        
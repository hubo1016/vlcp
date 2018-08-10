'''
Created on 2016/3/24

:author: hubo
'''
from vlcp.config.config import defaultconfig
import vlcp.service.kvdb.storage as storage
import vlcp.service.kvdb.redisnotifier as redisnotifier
from vlcp.server.module import depend, Module, call_api, api
import vlcp.utils.dataobject as dataobj
from vlcp.event.runnable import RoutineContainer
from vlcp.event.event import Event, withIndices, M_
from time import time
from copy import deepcopy
from vlcp.event.core import QuitException, syscall_removequeue
import itertools
from vlcp.utils.dataobject import AlreadyExistsException, UniqueKeyReference,\
    MultiKeyReference, DataObjectSet, UniqueKeySet, WeakReferenceObject,\
    MultiKeySet, ReferenceObject, request_context
from contextlib import closing
import functools
import copy
from vlcp.utils.exceptions import AsyncTransactionLockException, StaleResultException,\
    TransactionRetryExceededException, TransactionTimeoutException, WalkKeyNotRetrieved

try:
    from itertools import izip
except ImportError:
    izip = zip

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
        self._requestids = {}
        self._watchedkeys = set()
        self._requests = []
        self._transactno = 0
        self._stale = False
        self._updatekeys = set()
        self._update_version = {}
        self._cache = None
        self._pending_gc = 0
        self.apiroutine = RoutineContainer(self.scheduler)
        self.apiroutine.main = self._update
        self.routines.append(self.apiroutine)
        self.create_api(api(self.mget, self.apiroutine),
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
                       api(self.walk, self.apiroutine),
                       api(self.gettimestamp, self.apiroutine),
                       api(self.asynctransact, self.apiroutine),
                       api(self.writewalk, self.apiroutine),
                       api(self.asyncwritewalk, self.apiroutine)
                       )

    def _set_watch(self, key, requestid):
        self._watches.setdefault(key, set()).add(requestid)
        self._requestids.setdefault(requestid, set()).add(key)

    def _remove_watch(self, key, requestid):
        s = self._watches.get(key)
        if s:
            s.discard(requestid)
            if not s:
                del self._watches[key]
        s = self._requestids.get(requestid)
        if s:
            s.discard(key)
            if not s:
                del self._requestids[requestid]
    
    def _remove_all_watches(self, requestid):
        s = self._requestids.get(requestid)
        if s is not None:
            for k in s:
                s2 = self._watches.get(k)
                if s2:
                    s2.discard(requestid)
                    if not s2:
                        del self._watches[k]
            del self._requestids[requestid]

    async def load(self, container):
        self.scheduler.queue.addSubQueue(\
                self.objectupdatepriority, dataobj.DataObjectUpdateEvent.createMatcher(), 'dataobjectupdate')
        self._notifier = await call_api(container, 'updatenotifier', 'createnotifier')
        await Module.load(self, container)
        self.routines.append(self._notifier)

    async def unload(self, container, force=False):
        await container.syscall(syscall_removequeue(self.scheduler.queue, 'dataobjectupdate'))
        await Module.unload(self, container, force=force)

    async def _update(self):
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
        async def updateinner():
            processing_requests = []
            # New managed keys
            retrieve_list = set()
            orig_retrieve_list = set()
            retrieveonce_list = set()
            orig_retrieveonce_list = set()
            processing_request_ids = set()
            # Retrieved values are stored in update_result before merging into current storage
            update_result = {}
            # key => [(walker_func, (original_keys, rid)), ...]
            walkers = {}
            # Use the loop count as a revision identifier, then the valid revisions of the value
            # in update_result is a range, from the last loop count the value changed
            # (or -1 if not changed), to the last loop count the value is retrieved
            #
            # each walker can only walk on keys that shares at least one revision to ensure the
            # values are consistent. If no revision could be shared, all the keys must be retrieved
            # again to get a consistent view
            revision_min = {}
            revision_max = {}
            self._loopCount = 0
            # A request-id -> retrieve set dictionary to store the saved keys
            savelist = {}
            
            # (start_key, walker_func, rid) => set(used_keys)
            walker_used_keys = {}
            
            # used_key => [(start_key, walker_func, (original_keys, rid)), ...]
            used_key_ref = {}
            
            def _update_walker_ref(start_key, walker, original_keys, rid, used_keys):
                old_used_keys = walker_used_keys.get((start_key, walker, rid), ())
                for k in old_used_keys:
                    if k not in used_keys:
                        old_list = used_key_ref[k]
                        for i, v in enumerate(old_list):
                            if v[0] == start_key and v[1] == walker and v[2][1] == rid:
                                break
                        else:
                            continue
                        old_list[i:] = old_list[i+1:]
                for k in used_keys:
                    if k not in old_used_keys:
                        used_key_ref.setdefault(k, []).append((start_key, walker, (original_keys, rid)))
                walker_used_keys[(start_key, walker, rid)] = set(used_keys)
            
            # (start_key, walker, rid) => cached_result
            finished_walkers = {}
            
            def _dirty_walkers(new_values):
                for k in new_values:
                    if k in used_key_ref:
                        for start_key, walker, (_, rid) in used_key_ref[k]:
                            finished_walkers.pop((start_key, walker, rid), None)
            
            async def updateloop():
                while (retrieve_list or self._updatekeys or self._requests):
                    # default walker, default walker cached, customized walker, customized walker cached
                    _performance_counters = [0, 0, 0, 0]
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
                                        self._remove_watch(k, r[3])
                                    # Do not need to wait
                                except Exception as exc:
                                    await self.apiroutine.wait_for_send(RetrieveReply(r[1], exception = exc))
                                else:
                                    await self.apiroutine.wait_for_send(RetrieveReply(r[1], result = None))
                            elif r[2] == 'unwatchall':
                                if r[3] in processing_request_ids:
                                    # unwatch a processing request
                                    # pend this request until all requests are processed
                                    processing_requests.append(r)
                                else:
                                    try:
                                        self._remove_all_watches(r[3])
                                    except Exception as exc:
                                        await self.apiroutine.wait_for_send(RetrieveReply(r[1], exception = exc))
                                    else:
                                        await self.apiroutine.wait_for_send(RetrieveReply(r[1], result = None))
                            elif r[2] == 'watch':
                                retrieve_list.update(r[0])
                                orig_retrieve_list.update(r[0])
                                for k in r[0]:
                                    self._set_watch(k, r[3])
                                processing_requests.append(r)
                                processing_request_ids.add(r[3])
                            elif r[2] == 'get':
                                retrieve_list.update(r[0])
                                orig_retrieve_list.update(r[0])
                                processing_requests.append(r)
                                processing_request_ids.add(r[3])
                            elif r[2] == 'walk':
                                retrieve_list.update(r[0])
                                processing_requests.append(r)
                                for k,v in r[3].items():
                                    walkers.setdefault(k, []).append((v, (r[0], r[1])))
                                processing_request_ids.add(r[4])
                            else:
                                retrieveonce_list.update(r[0])
                                orig_retrieveonce_list.update(r[0])
                                processing_requests.append(r)
                        del self._requests[:]
                    if retrieve_list:
                        watch_keys = tuple(k for k in retrieve_list if k not in self._watchedkeys)
                        # Add watch_keys to notification
                        if watch_keys:
                            for k in watch_keys:
                                if k in update_result:
                                    self._update_version[k] = getversion(update_result[k])
                            await self._notifier.add_listen(*watch_keys)
                            self._watchedkeys.update(watch_keys)
                    get_list_set = update_list.union(itertools.chain((k for k in retrieve_list
                                                     if k not in update_result and k not in self._managed_objs),
                                                     (k for k in retrieveonce_list
                                                     if k not in update_result and k not in self._managed_objs)))
                    get_list = list(get_list_set)
                    new_values = set()
                    if get_list:
                        try:
                            result, self._cache = await call_api(
                                                            self.apiroutine,
                                                            'kvstorage',
                                                            'mgetwithcache',
                                                            {'keys': get_list, 'cache': self._cache}
                                                        )
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
                            revision_min.clear()
                            revision_max.clear()
                        else:
                            self._stale = False
                            for k,v in izip(get_list, result):
                                # Update revision information
                                revision_max[k] = self._loopCount
                                if k not in update_result:
                                    if k not in self._managed_objs:
                                        # A newly retrieved key
                                        revision_min[k] = self._loopCount
                                        old_value = None
                                    else:
                                        old_value = self._managed_objs[k]
                                else:
                                    old_value = update_result[k]
                                # Check if the value is changed
                                if old_value is not v and getversion(old_value) != getversion(v):
                                    revision_min[k] = self._loopCount
                                    new_values.add(k)
                                else:
                                    if k not in revision_min:
                                        revision_min[k] = -1
                                if old_value is not v:
                                    if v is not None and hasattr(v, 'setkey'):
                                        v.setkey(k)
                                if k in self._watchedkeys and k not in self._update_version:
                                    self._update_version[k] = getversion(v)

                            update_result.update(zip(get_list, result))
                    # Disable cache for walkers with updated keys
                    _dirty_walkers(new_values)
                    
                    # All keys which should be retrieved in next loop
                    new_retrieve_list = set()
                    # Keys which should be retrieved in next loop for a single walk
                    new_retrieve_keys = set()
                    # Keys that are used in current walk will be retrieved again in next loop
                    used_keys = set()
                    # We separate the data with revisions to prevent inconsistent result
                    def create_walker(orig_key, strict=True):
                        revision_range = [revision_min.get(orig_key, -1), revision_max.get(orig_key, -1)]
                        def _walk_with_revision(key):
                            if hasattr(key, 'getkey'):
                                key = key.getkey()
                            key = _str(key)
                            if key not in self._watchedkeys:
                                # This key is not retrieved, raise a KeyError, and record this key
                                new_retrieve_keys.add(key)
                                raise WalkKeyNotRetrieved(key)
                            elif self._stale:
                                if key not in self._managed_objs:
                                    new_retrieve_keys.add(key)
                                used_keys.add(key)
                                return self._managed_objs.get(key)
                            elif key not in update_result and key not in self._managed_objs:
                                # This key is not retrieved, raise a KeyError, and record this key
                                new_retrieve_keys.add(key)
                                raise WalkKeyNotRetrieved(key)
                            # Check revision
                            current_revision = (
                                max(revision_min.get(key, -1), revision_range[0]),
                                min(revision_max.get(key, -1), revision_range[1])
                            )
                            if current_revision[1] < current_revision[0]:
                                # revisions cannot match
                                new_retrieve_keys.add(key)
                                if strict:
                                    used_keys.add(key)
                                    raise WalkKeyNotRetrieved(key)
                            else:
                                # update revision range
                                revision_range[:] = current_revision
                            if key in update_result:
                                used_keys.add(key)
                                return update_result[key]
                            else:
                                used_keys.add(key)
                                return self._managed_objs[key]
                        return _walk_with_revision
                    _default_walker_dup_check = set()
                    def default_walker(key, obj, walk, _circle_detect = None):
                        if _circle_detect is None:
                            _circle_detect = set()
                        if key in _circle_detect:
                            return
                        else:
                            _circle_detect.add(key)
                        if hasattr(obj, 'kvdb_internalref'):
                            rl = obj.kvdb_internalref()
                            for k in rl:
                                try:
                                    newobj = walk(k)
                                except KeyError:
                                    pass
                                else:
                                    if newobj is not None:
                                        default_walker(k, newobj, walk, _circle_detect)
                    def _do_default_walker(k):
                        if k not in _default_walker_dup_check:
                            _default_walker_dup_check.add(k)
                            _performance_counters[0] += 1
                            if (k, None, None) not in finished_walkers:
                                v = update_result.get(k)
                                if v is not None:
                                    new_retrieve_keys.clear()
                                    used_keys.clear()
                                    default_walker(k, v, create_walker(k, False))
                                    if new_retrieve_keys:
                                        new_retrieve_list.update(new_retrieve_keys)
                                        self._updatekeys.update(used_keys)
                                        self._updatekeys.add(k)
                                    else:
                                        _all_used_keys = used_keys.union([k])
                                        _update_walker_ref(k, None, None, None, _all_used_keys)
                                        finished_walkers[(k, None, None)] = None
                                else:
                                    _update_walker_ref(k, None, None, None, [k])
                                    finished_walkers[(k, None, None)] = None
                            else:
                                _performance_counters[1] += 1
                    for k in orig_retrieve_list:
                        _do_default_walker(k)
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
                                _performance_counters[2] += 1
                                _cache_key = (k, w, r[1])
                                if _cache_key in finished_walkers:
                                    _performance_counters[3] += 1
                                    savelist.setdefault(r[1], set()).update(finished_walkers[_cache_key])
                                else:
                                    _local_save_list = set()
                                    def save(key):
                                        if hasattr(key, 'getkey'):
                                            key = key.getkey()
                                        key = _str(key)
                                        if key != k and key not in used_keys:
                                            raise ValueError('Cannot save a key without walk')
                                        _local_save_list.add(key)
                                    try:
                                        new_retrieve_keys.clear()
                                        used_keys.clear()
                                        w(k, v, create_walker(k), save)
                                    except Exception as exc:
                                        # if one walker failed, the whole request is failed, remove all walkers
                                        self._logger.warning("A walker raises an exception which rolls back the whole walk process. "
                                                             "walker = %r, start key = %r, new_retrieve_keys = %r, used_keys = %r",
                                                             w, k, new_retrieve_keys, used_keys, exc_info=True)
                                        for orig_k in r[0]:
                                            if orig_k in walkers:
                                                walkers[orig_k][:] = [(w0, r0) for w0,r0 in walkers[orig_k] if r0[1] != r[1]]
                                        processing_requests[:] = [r0 for r0 in processing_requests if r0[1] != r[1]]
                                        savelist.pop(r[1])
                                        await self.apiroutine.wait_for_send(RetrieveReply(r[1], exception = exc))
                                    else:
                                        savelist.setdefault(r[1], set()).update(_local_save_list)
                                        if new_retrieve_keys:
                                            new_retrieve_list.update(new_retrieve_keys)
                                            self._updatekeys.update(used_keys)
                                            self._updatekeys.add(k)
                                        else:
                                            _all_used_keys = used_keys.union([k])
                                            _update_walker_ref(k, w, r[0], r[1], _all_used_keys)
                                            finished_walkers[_cache_key] = _local_save_list
                    for save in savelist.values():
                        for k in save:
                            _do_default_walker(k)
                    retrieve_list.clear()
                    retrieveonce_list.clear()
                    retrieve_list.update(new_retrieve_list)
                    self._logger.debug("Loop %d: %d default walker (%d cached), %d customized walker (%d cached)",
                                       self._loopCount,
                                       *_performance_counters)
                    self._loopCount += 1
                    if self._stale:
                        watch_keys = tuple(k for k in retrieve_list if k not in self._watchedkeys)
                        if watch_keys:
                            await self._notifier.add_listen(*watch_keys)
                            self._watchedkeys.update(watch_keys)
                        break
            while True:
                await self.apiroutine.with_callback(updateloop(), onupdate, notification_matcher)
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
                    timeout, ev, m = await self.apiroutine.wait_with_timeout(0.2, notification_matcher)
                    if timeout:
                        break
                    else:
                        onupdate(ev, m)
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
            unwatchall = []
            for r in processing_requests:
                if r[2] == 'get':
                    objs = [self._managed_objs.get(k) for k in r[0]]
                    for k,v in zip(r[0], objs):
                        if v is not None:
                            self._set_watch(k, r[3])
                    result = [o.create_reference() if o is not None and hasattr(o, 'create_reference') else o
                              for o in objs]
                elif r[2] == 'watch':
                    result = [(v.create_reference() if hasattr(v, 'create_reference') else v)
                              if v is not None else dataobj.ReferenceObject(k)
                              for k,v in ((k,self._managed_objs.get(k)) for k in r[0])]
                elif r[2] == 'walk':
                    saved_keys = list(savelist.get(r[1], []))
                    for k in saved_keys:
                        self._set_watch(k, r[4])
                    objs = [self._managed_objs.get(k) for k in saved_keys]
                    result = (saved_keys,
                              [o.create_reference() if hasattr(o, 'create_reference') else o
                               if o is not None else dataobj.ReferenceObject(k)
                               for k,o in zip(saved_keys, objs)])
                elif r[2] == 'unwatchall':
                    # Remove watches after all results are processed
                    unwatchall.append(r[3])
                    result = None
                else:
                    result = [copywithkey(update_result.get(k, self._managed_objs.get(k)), k) for k in r[0]]
                send_events.append(RetrieveReply(r[1], result = result, stale = self._stale))
            for requestid in unwatchall:
                self._remove_all_watches(requestid)
            async def output_result():
                for e in send_events:
                    await self.apiroutine.wait_for_send(e)
            await self.apiroutine.with_callback(output_result(), onupdate, notification_matcher)
            self._pending_gc += 1
        async def _gc():
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
                await self._notifier.remove_listen(*tuple(remove_keys))
                for k in remove_keys:
                    if k in self._managed_objs:
                        del self._managed_objs[k]
                    if k in self._update_version:
                        del self._update_version[k]
            if self._cache is not None:
                self._cache.gc(self._managed_objs)
            self._pending_gc = 0
        while True:
            if not self._updatekeys and not self._requests:
                if self._pending_gc >= 10:
                    await self.apiroutine.with_callback(_gc(), onupdate, notification_matcher)
                    continue
                elif self._pending_gc:
                    timeout, ev, m = await self.apiroutine.wait_with_timeout(1, notification_matcher, request_matcher)
                    if timeout:
                        await self.apiroutine.with_callback(_gc(), onupdate, notification_matcher)
                        continue
                else:
                    ev, m = await M_(notification_matcher, request_matcher)
                if m is notification_matcher:
                    onupdate(ev, m)
            await updateinner()

    async def mget(self, keys, requestid, nostale = False):
        "Get multiple objects and manage them. Return references to the objects."
        keys = tuple(_str2(k) for k in keys)
        notify = not self._requests
        rid = object()
        self._requests.append((keys, rid, 'get', requestid))
        if notify:
            await self.apiroutine.wait_for_send(RetrieveRequestSend())
        ev = await RetrieveReply.createMatcher(rid)
        if hasattr(ev, 'exception'):
            raise ev.exception
        if nostale and ev.stale:
            raise StaleResultException(ev.result)
        return ev.result

    async def get(self, key, requestid, nostale = False):
        """
        Get an object from specified key, and manage the object.
        Return a reference to the object or None if not exists.
        """
        r = await self.mget([key], requestid, nostale)
        return r[0]

    async def mgetonce(self, keys, nostale = False):
        "Get multiple objects, return copies of them. Referenced objects are not retrieved."
        keys = tuple(_str2(k) for k in keys)
        notify = not self._requests
        rid = object()
        self._requests.append((keys, rid, 'getonce'))
        if notify:
            await self.apiroutine.wait_for_send(RetrieveRequestSend())
        ev = await RetrieveReply.createMatcher(rid)
        if hasattr(ev, 'exception'):
            raise ev.exception
        if nostale and ev.stale:
            raise StaleResultException(ev.result)
        return ev.result

    async def getonce(self, key, nostale = False):
        "Get a object without manage it. Return a copy of the object, or None if not exists. Referenced objects are not retrieved."
        r = await self.mgetonce([key], nostale)
        return r[0]

    async def watch(self, key, requestid, nostale = False):
        """
        Try to find an object and return a reference. Use ``reference.isdeleted()`` to test
        whether the object exists.
        Use ``reference.wait(container)`` to wait for the object to be existed.
        """
        r = await self.mwatch([key], requestid, nostale)
        return r[0]

    async def mwatch(self, keys, requestid, nostale = False):
        "Try to return all the references, see ``watch()``"
        keys = tuple(_str2(k) for k in keys)
        notify = not self._requests
        rid = object()
        self._requests.append((keys, rid, 'watch', requestid))
        if notify:
            await self.apiroutine.wait_for_send(RetrieveRequestSend())
        ev = await RetrieveReply.createMatcher(rid)
        if hasattr(ev, 'exception'):
            raise ev.exception
        if nostale and ev.stale:
            raise StaleResultException(ev.result)
        return ev.result

    async def unwatch(self, key, requestid):
        "Cancel management of a key"
        await self.munwatch([key], requestid)

    async def unwatchall(self, requestid):
        "Cancel management for all keys that are managed by requestid"
        notify = not self._requests
        rid = object()
        self._requests.append(((), rid, 'unwatchall', requestid))
        if notify:
            await self.apiroutine.wait_for_send(RetrieveRequestSend())
        ev = await RetrieveReply.createMatcher(rid)
        if hasattr(ev, 'exception'):
            raise ev.exception

    async def munwatch(self, keys, requestid):
        "Cancel management of keys"
        keys = tuple(_str2(k) for k in keys)
        notify = not self._requests
        rid = object()
        self._requests.append((keys, rid, 'unwatch', requestid))
        if notify:
            await self.apiroutine.wait_for_send(RetrieveRequestSend())
        ev = await RetrieveReply.createMatcher(rid)
        if hasattr(ev, 'exception'):
            raise ev.exception

    async def transact(self, keys, updater, withtime = False, maxtime = 60):
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
        start_time = self.apiroutine.scheduler.current_time
        retry_times = 1
        while True:
            try:
                await call_api(self.apiroutine, 'kvstorage', 'updateallwithtime',
                                 {'keys': keys + tuple(auto_remove_keys) + \
                                         tuple(extra_keys) + tuple(extra_key_set),
                                         'updater': object_updater})
            except _NeedMoreKeysException:
                if maxtime is not None and\
                    self.apiroutine.scheduler.current_time - start_time > maxtime:
                    raise TransactionTimeoutException
                retry_times += 1
            except Exception:
                self._logger.debug("Transaction %r interrupted in %r retries", updater, retry_times)
                raise
            else:
                self._logger.debug("Transaction %r done in %r retries", updater, retry_times)                
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
        if not self._requests:
            # Fake notification
            await self.apiroutine.wait_for_send(RetrieveRequestSend())
        await self._notifier.publish(updated_ref[0], updated_ref[1])
    
    async def gettimestamp(self):
        """
        Get a timestamp from database server
        """
        _timestamp = None
        def _updater(keys, values, timestamp):
            nonlocal _timestamp
            _timestamp = timestamp
            return ((), ())
        await call_api(self.apiroutine, 'kvstorage', 'updateallwithtime',
                                 {'keys': (),
                                  'updater': _updater})
        return _timestamp

    def watchlist(self, requestid = None):
        """
        Return a dictionary whose keys are database keys, and values are lists of request ids.
        Optionally filtered by request id
        """
        return dict((k,list(v)) for k,v in self._watches.items() if requestid is None or requestid in v)

    async def walk(self, keys, walkerdict, requestid, nostale = False):
        """
        Recursively retrieve keys with customized functions.
        walkerdict is a dictionary ``key->walker(key, obj, walk, save)``.
        """
        keys = tuple(_str2(k) for k in keys)
        notify = not self._requests
        rid = object()
        self._requests.append((keys, rid, 'walk', dict(walkerdict), requestid))
        if notify:
            await self.apiroutine.wait_for_send(RetrieveRequestSend())
        ev = await RetrieveReply.createMatcher(rid)
        if hasattr(ev, 'exception'):
            raise ev.exception
        if nostale and ev.stale:
            raise StaleResultException(ev.result)
        return ev.result
    
    async def asynctransact(self, asyncupdater, withtime = False,
                            maxretry = None, maxtime=60):
        """
        Read-Write transaction with asynchronous operations.
        
        First, the `asyncupdater` is called with `asyncupdater(last_info, container)`.
        `last_info` is the info from last `AsyncTransactionLockException`.
        When `asyncupdater` is called for the first time, last_info = None.
        
        The async updater should be an async function, and return
        `(updater, keys)`. The `updater` should
        be a valid updater function used in `transaction` API. `keys` will
        be the keys used in the transaction.
        
        The async updater can return None to terminate the transaction
        without exception.
        
        After the call, a transaction is automatically started with the
        return values of `asyncupdater`.
        
        `updater` can raise `AsyncTransactionLockException` to restart
        the transaction from `asyncupdater`.
        
        :param asyncupdater: An async updater `asyncupdater(last_info, container)`
                             which returns `(updater, keys)`
        
        :param withtime: Whether the returned updater need a timestamp
        
        :param maxretry: Limit the max retried times
        
        :param maxtime: Limit the execution time. The transaction is abandoned
                        if still not completed after `maxtime` seconds. 
        """
        start_time = self.apiroutine.scheduler.current_time
        def timeleft():
            if maxtime is None:
                return None
            else:
                time_left = maxtime + start_time - \
                                self.apiroutine.scheduler.current_time
                if time_left <= 0:
                    raise TransactionTimeoutException
                else:
                    return time_left
        retry_times = 0
        last_info = None
        while True:
            timeout, r = \
                    await self.apiroutine.execute_with_timeout(
                            timeleft(),
                            asyncupdater(last_info, self.apiroutine)
                        )
            if timeout:
                raise TransactionTimeoutException
            if r is None:
                return
            updater, keys = r
            try:
                await self.transact(keys, updater, withtime, timeleft())
            except AsyncTransactionLockException as e:
                retry_times += 1
                if maxretry is not None and retry_times > maxretry:
                    raise TransactionRetryExceededException
                # Check time left
                timeleft()
                last_info = e.info
            except Exception:
                self._logger.debug("Async transaction %r interrupted in %r retries", asyncupdater, retry_times + 1)
                raise
            else:
                self._logger.debug("Async transaction %r done in %r retries", asyncupdater, retry_times + 1)
                break
    
    async def writewalk(self, keys, walker, withtime = False, maxtime = 60):
        """
        A read-write transaction with walkers
        
        :param keys: initial keys used in walk. Provide keys already known to
                     be necessary to optimize the transaction.

        :param walker: A walker should be `walker(walk, write)`,
                           where `walk` is a function `walk(key)->value`
                           to get a value from the database, and
                           `write` is a function `write(key, value)`
                           to save value to the database.
                           
                           A value can be write to a database any times.
                           A `walk` called after `write` is guaranteed
                           to retrieve the previously written value.

        :param withtime: if withtime=True, an extra timestamp parameter is given to
                         walkers, so walker should be
                         `walker(walk, write, timestamp)`
        
        :param maxtime: max execution time of this transaction
        """
        @functools.wraps(walker)
        async def _asyncwalker(last_info, container):
            return (keys, walker)
        return await self.asyncwritewalk(_asyncwalker, withtime, maxtime)

    async def asyncwritewalk(self, asyncwalker, withtime = False, maxtime = 60):
        """
        A read-write transaction with walker factory
        
        :param asyncwalker: an async function called as `asyncwalker(last_info, container)`
                            and returns (keys, walker), which
                            are the same as parameters of `writewalk`
        
                            :param keys: initial keys used in walk
                            
                            :param walker: A walker should be `walker(walk, write)`,
                                               where `walk` is a function `walk(key)->value`
                                               to get a value from the database, and
                                               `write` is a function `write(key, value)`
                                               to save value to the database.
                                               
                                               A value can be write to a database any times.
                                               A `walk` called after `write` is guaranteed
                                               to retrieve the previously written value.
                                               
                                               raise AsyncTransactionLockException in walkers
                                               to restart the transaction
        
        :param withtime: if withtime=True, an extra timestamp parameter is given to
                         walkers, so walkers should be
                         `walker(key, value, walk, write, timestamp)`
        
        :param maxtime: max execution time of this transaction
        """
        @functools.wraps(asyncwalker)
        async def _asyncupdater(last_info, container):
            if last_info is not None:
                from_walker, real_info = last_info
                if not from_walker:
                    keys, orig_keys, walker = real_info
                else:
                    r = await asyncwalker(real_info, container)
                    if r is None:
                        return None
                    keys, walker = r
                    orig_keys = keys
            else:
                r = await asyncwalker(None, container)
                if r is None:
                    return None
                keys, walker = r
                orig_keys = keys
            @functools.wraps(walker)
            def _updater(keys, values, timestamp):
                _stored_objs = dict(zip(keys, values))
                if self.debuggingupdater:
                    _stored_old_values = {k: v.jsonencode()
                                          for k,v in zip(keys, values)
                                          if hasattr(v, 'jsonencode')}
                # Keys written by walkers
                _walker_write_dict = {}
                _lost_keys = set()
                _used_keys = set()
                def _walk(key):
                    if key not in _stored_objs:
                        _lost_keys.add(key)
                        raise WalkKeyNotRetrieved(key)
                    else:
                        if key not in _walker_write_dict:
                            _used_keys.add(key)
                        return _stored_objs[key]
                def _write(key, value):
                    _walker_write_dict[key] = value
                    _stored_objs[key] = value
                try:
                    if withtime:
                        walker(_walk, _write, timestamp)
                    else:
                        walker(_walk, _write)
                except AsyncTransactionLockException as e:
                    raise AsyncTransactionLockException((True, e.info))
                if _lost_keys:
                    _lost_keys.update(_used_keys)
                    _lost_keys.update(orig_keys)
                    raise AsyncTransactionLockException((False, (_lost_keys, orig_keys, walker)))
                if self.debuggingupdater:
                    # Check if there are changes not written
                    for k, v in _stored_old_values.items():
                        if k not in _walker_write_dict:
                            v2 = _stored_objs[k]
                            assert hasattr(v2, 'jsonencode') and v2.jsonencode() == v
                if _walker_write_dict:
                    return tuple(zip(*_walker_write_dict.items()))
                else:
                    return (), ()
            return (_updater, keys)
        return await self.asynctransact(_asyncupdater, True, maxtime=maxtime)

'''
Created on 2016/3/21

:author: hubo
'''

from vlcp.server.module import Module, callAPI, depend, ModuleNotification,\
    ModuleLoadStateChanged, api, proxy
from vlcp.config import defaultconfig
import vlcp.service.connection.redisdb as redisdb
from vlcp.event.event import withIndices, Event
from vlcp.event.runnable import RoutineContainer
from vlcp.event.connection import ConnectionResetException
from vlcp.event.core import syscall_removequeue, QuitException
import json
from time import time
import functools
import uuid
import logging

@withIndices('notifier', 'transactid', 'keys', 'reason', 'fromself')
class UpdateNotification(Event):
    UPDATED = 'updated'
    RESTORED = 'restored'

@withIndices('notifier', 'stage')
class ModifyListen(Event):
    SUBSCRIBE = 'subscribe'
    LISTEN = 'listen'

def _delegate(func):
    @functools.wraps(func)
    def f(self, *args, **kwargs):
        for m in self.delegate(func(self, *args, **kwargs)):
            yield m
    return f

def _bytes(s):
    if isinstance(s, bytes):
        return s
    else:
        return s.encode('utf-8')

class _Notifier(RoutineContainer):
    _logger = logging.getLogger(__name__ + '.Notifier')
    def __init__(self, vhostbind, prefix, scheduler=None, daemon=False):
        RoutineContainer.__init__(self, scheduler=scheduler, daemon=daemon)
        self.vhostbind = vhostbind
        self.prefix = _bytes(prefix)
        self._matchers = {}
        self._publishkey = uuid.uuid1().hex
        self._publishno = 1
        self._publish_wait = set()
        self._matchadd_wait = set()
        self._matchremove_wait = set()
    def main(self):
        try:
            timestamp = '%012x' % (int(time() * 1000),) + '-'
            transactno = 1
            for m in callAPI(self, 'redisdb', 'getclient', {'vhost':self.vhostbind}):
                yield m
            client, encoder, decoder = self.retvalue
            self.subroutine(self._modifier(client), True, "modifierroutine")
            listen_modify = ModifyListen.createMatcher(self, ModifyListen.LISTEN)
            connection_down = client.subscribe_state_matcher(self, False)
            connection_up = client.subscribe_state_matcher(self, True)
            module_loaded = ModuleLoadStateChanged.createMatcher(state = ModuleLoadStateChanged.LOADED,
                                                 _ismatch = lambda x: x._instance.getServiceName() == 'redisdb')
            matchers = tuple(self._matchers.values()) + (listen_modify, connection_down)
            last_transact = None
            while True:
                yield matchers
                if self.matcher is listen_modify:
                    matchers = tuple(self._matchers.values()) + (listen_modify, connection_down)
                elif self.matcher is connection_down:
                    # Connection is down, wait for restore
                    # The module may be reloaded
                    recreate_matchers = False
                    last_transact = None
                    while True:
                        yield (connection_up, module_loaded)
                        if self.matcher is module_loaded:
                            self.terminate(self.modifierroutine)
                            for m in callAPI(self, 'redisdb', 'getclient', {'vhost':self.vhostbind}):
                                yield m
                            client, encoder, decoder = self.retvalue
                            # Recreate listeners
                            connection_down = client.subscribe_state_matcher(self, False)
                            connection_up = client.subscribe_state_matcher(self, True)
                            try:
                                for m in client.subscribe(self, *tuple(self._matchers.keys())):
                                    yield m
                            except Exception:
                                recreate_matchers = True
                                continue
                            else:
                                self._matchers = dict(zip(self._matchers.keys(), self.retvalue))
                                self.subroutine(self._modifier(client), True, "modifierroutine")
                                matchers = tuple(self._matchers.values()) + (listen_modify, connection_down)
                                break
                        else:
                            if recreate_matchers:
                                try:
                                    for m in client.subscribe(self, *[self.prefix + k for k in self._matchers.keys()]):
                                        yield m
                                except Exception:
                                    recreate_matchers = True
                                    continue
                                else:
                                    self._matchers = dict(zip(self._matchers.keys(), self.retvalue))
                                    self.subroutine(self._modifier(client), True, "modifierroutine")
                                    matchers = tuple(self._matchers.values()) + (listen_modify, connection_down)
                                    break
                            else:
                                matchers = tuple(self._matchers.values()) + (listen_modify, connection_down)
                                break
                    if self._publish_wait:
                        self.subroutine(self.publish())
                    transactid = '%s%016x' % (timestamp, transactno)
                    transactno += 1
                    self.subroutine(self.waitForSend(
                                UpdateNotification(self, transactid, tuple(self._matchers.keys()), UpdateNotification.RESTORED, False)
                                                                           ), False)
                else:
                    transact = decoder(self.event.message)
                    if transact['id'] == last_transact:
                        # Ignore duplicated updates
                        continue
                    last_transact = transact['id']
                    pubkey, sep, pubno = last_transact.partition('-')
                    fromself = (sep and pubkey == self._publishkey)
                    transactid = '%s%016x' % (timestamp, transactno)
                    transactno += 1
                    self.subroutine(self.waitForSend(
                                UpdateNotification(self, transactid, tuple(transact['keys']), UpdateNotification.UPDATED, fromself)
                                                                           ), False)
        finally:
            self.terminate(self.modifierroutine)
    def _modifier(self, client):
        try:
            modify_matcher = ModifyListen.createMatcher(self, ModifyListen.SUBSCRIBE)
            while True:
                try:
                    while self._matchadd_wait or self._matchremove_wait:
                        if self._matchadd_wait:
                            # Subscribe new keys
                            current_add = set(self._matchadd_wait)
                            self._matchadd_wait.clear()
                            add_keys = list(current_add.difference(self._matchers.keys()))
                            try:
                                for m in client.subscribe(self, *[self.prefix + k for k in add_keys]):
                                    yield m
                            except:
                                # Return to matchadd
                                self._matchadd_wait.update(current_add.difference(self._matchremove_wait))
                                raise
                            else:
                                self._matchers.update(zip(add_keys, self.retvalue))
                                for m in self.waitForSend(ModifyListen(self, ModifyListen.LISTEN)):
                                    yield m
                        if self._matchremove_wait:
                            # Unsubscribe keys
                            current_remove = set(self._matchremove_wait)
                            self._matchremove_wait.clear()
                            del_keys = list(current_remove.intersection(self._matchers.keys()))
                            try:
                                for m in client.unsubscribe(self, *[self.prefix + k for k in del_keys]):
                                    yield m
                            except:
                                # Return to matchremove
                                self._matchremove_wait.update(current_remove.difference(self._matchadd_wait))
                                raise
                            else:
                                for k in del_keys:
                                    del self._matchers[k]
                                for m in self.waitForSend(ModifyListen(self, ModifyListen.LISTEN)):
                                    yield m
                    yield (modify_matcher,)
                except (IOError, ConnectionResetException):
                    # Wait for connection resume
                    connection_up = client.subscribe_state_matcher(self)
                    yield (connection_up,)
        except QuitException:
            pass
        except:
            self.subroutine(self._clearup(client, list(self._matchers.keys())))
            raise
        else:
            self.subroutine(self._clearup(client, list(self._matchers.keys())))            
    def _clearup(self, client, keys):
        try:
            for m in client.unsubscribe(self, *[self.prefix + k for k in keys]):
                yield m
        except Exception:
            pass
    def add_listen(self, *keys):
        keys = [_bytes(k) for k in keys]
        self._matchremove_wait.difference_update(keys)
        self._matchadd_wait.update(keys)
        for m in self.waitForSend(ModifyListen(self, ModifyListen.SUBSCRIBE)):
            yield m
    def remove_listen(self, *keys):        
        keys = [_bytes(k) for k in keys]
        self._matchadd_wait.difference_update(keys)
        self._matchremove_wait.update(keys)
        for m in self.waitForSend(ModifyListen(self, ModifyListen.SUBSCRIBE)):
            yield m
    @_delegate
    def publish(self, *keys):
        keys = [_bytes(k) for k in keys]
        if self._publish_wait:
            merged_keys = list(self._publish_wait.union(keys))
            self._publish_wait.clear()
        else:
            merged_keys = list(keys)
        if not merged_keys:
            raise StopIteration
        for m in callAPI(self, 'redisdb', 'getclient', {'vhost':self.vhostbind}):
            yield m
        client, encoder, _ = self.retvalue
        transactid = '%s-%016x' % (self._publishkey, self._publishno)
        self._publishno += 1
        msg = encoder({'id':transactid, 'keys':merged_keys})
        try:
            for m in client.batch_execute(self, *((('MULTI',),) +
                                                tuple(('PUBLISH', self.prefix + k, msg) for k in merged_keys) +
                                                (('EXEC',),))):
                yield m
        except (IOError, ConnectionResetException):
            self._logger.warning('Following keys are not published because exception occurred, delay to next publish: %r', merged_keys, exc_info = True)
            self._publish_wait.update(merged_keys)
        else:
            self._publish_wait.clear()
    def notification_matcher(self, fromself = None):
        if fromself is None:
            return UpdateNotification.createMatcher(self)
        else:
            return UpdateNotification.createMatcher(notifier = self, fromself = fromself)

@defaultconfig
@depend(redisdb.RedisDB)
class RedisNotifier(Module):
    """
    Update notification with Redis Pub/Sub
    """
    _default_vhostbind = ''
    _default_prefix = 'vlcp.updatenotifier.'
    def __init__(self, server):
        Module.__init__(self, server)
        self.createAPI(api(self.createnotifier))
    def load(self, container):
        self.scheduler.queue.addSubQueue(999, ModifyListen.createMatcher(), "redisnotifier_modifylisten")
        for m in Module.load(self, container):
            yield m
    def unload(self, container, force=False):
        for m in Module.unload(self, container, force=force):
            yield m
        for m in container.syscall_noreturn(syscall_removequeue(self.scheduler.queue, "redisnotifier_modifylisten")):
            yield m
    def createnotifier(self):
        "Create a new notifier object"
        n = _Notifier(self.vhostbind, self.prefix, self.scheduler)
        n.start()
        return n

UpdateNotifier = proxy('UpdateNotifier', RedisNotifier)

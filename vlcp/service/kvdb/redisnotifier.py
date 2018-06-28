'''
Created on 2016/3/21

:author: hubo
'''

from vlcp.server.module import Module, call_api, depend, ModuleNotification,\
    ModuleLoadStateChanged, api, proxy
from vlcp.config import defaultconfig
import vlcp.service.connection.redisdb as redisdb
from vlcp.event.event import withIndices, Event, M_
from vlcp.event.runnable import RoutineContainer
from vlcp.event.connection import ConnectionResetException
from vlcp.event.core import syscall_removequeue, QuitException
import json
from time import time
from zlib import compress, decompress, error as zlib_error
import functools
import uuid
import logging
from contextlib import closing


@withIndices('notifier', 'transactid', 'keys', 'reason', 'fromself')
class UpdateNotification(Event):
    UPDATED = 'updated'
    RESTORED = 'restored'


@withIndices('notifier', 'stage')
class ModifyListen(Event):
    SUBSCRIBE = 'subscribe'
    LISTEN = 'listen'


def _bytes(s):
    if isinstance(s, bytes):
        return s
    else:
        return s.encode('utf-8')


def _str(s):
    if isinstance(s, str):
        return s
    else:
        return s.decode('utf-8')


class _Notifier(RoutineContainer):
    _logger = logging.getLogger(__name__ + '.Notifier')

    def __init__(self, vhostbind, prefix, scheduler=None, singlecastlimit = 256, deflate = False):
        RoutineContainer.__init__(self, scheduler=scheduler, daemon=False)
        self.vhostbind = vhostbind
        self.prefix = _bytes(prefix)
        self._matchers = {}
        self._publishkey = uuid.uuid1().hex
        self._publishno = 1
        self._publish_wait = set()
        self._matchadd_wait = set()
        self._matchremove_wait = set()
        self._singlecastlimit = singlecastlimit
        self._deflate = deflate

    async def main(self):
        try:
            timestamp = '%012x' % (int(time() * 1000),) + '-'
            transactno = 1
            client, encoder, decoder = await call_api(self, 'redisdb', 'getclient', {'vhost':self.vhostbind})
            try:
                s_matchers = await client.subscribe(self, self.prefix)
            except Exception:
                _no_connection_start = True
            else:
                _no_connection_start = False
            self._matchers[b''] = s_matchers[0]
            if self._deflate:
                oldencoder = encoder
                olddecoder = decoder
                def encoder(x):
                    return compress(oldencoder(x))
                def decoder(x):
                    try:
                        return olddecoder(decompress(x))
                    except zlib_error:
                        return olddecoder(x)
            if not _no_connection_start:
                self.subroutine(self._modifier(client), True, "modifierroutine")
            listen_modify = ModifyListen.createMatcher(self, ModifyListen.LISTEN)
            connection_down = client.subscribe_state_matcher(self, False)
            connection_up = client.subscribe_state_matcher(self, True)
            module_loaded = ModuleLoadStateChanged.createMatcher(state = ModuleLoadStateChanged.LOADED,
                                                 _ismatch = lambda x: x._instance.getServiceName() == 'redisdb')
            matchers = tuple(self._matchers.values()) + (listen_modify, connection_down)
            last_transact = None
            while True:
                if not _no_connection_start:
                    ev, m = await M_(*matchers)
                if not _no_connection_start and m is listen_modify:
                    matchers = tuple(self._matchers.values()) + (listen_modify, connection_down)
                elif _no_connection_start or m is connection_down:
                    # Connection is down, wait for restore
                    # The module may be reloaded
                    if _no_connection_start:
                        recreate_matchers = True
                    else:
                        recreate_matchers = False
                    last_transact = None
                    while True:
                        _, m = await M_(connection_up, module_loaded)
                        if m is module_loaded:
                            self.terminate(self.modifierroutine)
                            client, encoder, decoder = await call_api(self, 'redisdb', 'getclient', {'vhost':self.vhostbind})
                            if self._deflate:
                                oldencoder = encoder
                                olddecoder = decoder
                                def encoder(x):
                                    return compress(oldencoder(x))
                                def decoder(x):
                                    try:
                                        return olddecoder(decompress(x))
                                    except zlib_error:
                                        return olddecoder(x)
                            # Recreate listeners
                            connection_down = client.subscribe_state_matcher(self, False)
                            connection_up = client.subscribe_state_matcher(self, True)
                            try:
                                s_matchers = await client.subscribe(self, *tuple(self._matchers.keys()))
                            except Exception:
                                recreate_matchers = True
                                continue
                            else:
                                self._matchers = dict(zip(self._matchers.keys(), s_matchers))
                                self.subroutine(self._modifier(client), True, "modifierroutine")
                                matchers = tuple(self._matchers.values()) + (listen_modify, connection_down)
                                break
                        else:
                            if recreate_matchers:
                                try:
                                    s_matchers = await client.subscribe(self, *[self.prefix + k for k in self._matchers.keys()])
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
                    async def send_restore_notify(transactid):
                        if self._matchadd_wait or self._matchremove_wait:
                            # Wait for next subscribe success
                            await self.wait_with_timeout(1, ModifyListen.createMatcher(self, ModifyListen.LISTEN))
                        await self.wait_for_send(
                                UpdateNotification(
                                    self,
                                    transactid,
                                    tuple(self._matchers.keys()),
                                    UpdateNotification.RESTORED,
                                    False,
                                    extrainfo = None)
                                )
                    self.subroutine(send_restore_notify(transactid), False)
                else:
                    transact = decoder(ev.message)
                    if transact['id'] == last_transact:
                        # Ignore duplicated updates
                        continue
                    last_transact = transact['id']
                    pubkey, sep, pubno = last_transact.partition('-')
                    fromself = (sep and pubkey == self._publishkey)
                    transactid = '%s%016x' % (timestamp, transactno)
                    transactno += 1
                    self.subroutine(self.wait_for_send(
                                            UpdateNotification(
                                                self,
                                                transactid,
                                                tuple(_bytes(k) for k in transact['keys']),
                                                UpdateNotification.UPDATED,
                                                fromself,
                                                extrainfo = transact.get('extrainfo'))
                                        ),
                                    False)
        finally:
            if hasattr(self ,'modifierroutine') and self.modifierroutine:
                self.terminate(self.modifierroutine)

    async def _modifier(self, client):
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
                                s_matchers = await client.subscribe(self, *[self.prefix + k for k in add_keys])
                            except:
                                # Return to matchadd
                                self._matchadd_wait.update(current_add.difference(self._matchremove_wait))
                                raise
                            else:
                                self._matchers.update(zip(add_keys, s_matchers))
                                await self.wait_for_send(ModifyListen(self, ModifyListen.LISTEN))
                        if self._matchremove_wait:
                            # Unsubscribe keys
                            current_remove = set(self._matchremove_wait)
                            self._matchremove_wait.clear()
                            del_keys = list(current_remove.intersection(self._matchers.keys()))
                            try:
                                await client.unsubscribe(self, *[self.prefix + k for k in del_keys])
                            except:
                                # Return to matchremove
                                self._matchremove_wait.update(current_remove.difference(self._matchadd_wait))
                                raise
                            else:
                                for k in del_keys:
                                    del self._matchers[k]
                                await self.wait_for_send(ModifyListen(self, ModifyListen.LISTEN))
                    await modify_matcher
                except (IOError, ConnectionResetException):
                    # Wait for connection resume
                    connection_up = client.subscribe_state_matcher(self)
                    await connection_up
        finally:
            self.subroutine(self._clearup(client, list(self._matchers.keys())))

    async def _clearup(self, client, keys):
        try:
            if not self.scheduler.quitting:
                await client.unsubscribe(self, *[self.prefix + k for k in keys])
        except Exception:
            pass

    async def add_listen(self, *keys):
        keys = [_bytes(k) for k in keys]
        self._matchremove_wait.difference_update(keys)
        self._matchadd_wait.update(keys)
        await self.wait_for_send(ModifyListen(self, ModifyListen.SUBSCRIBE))

    async def remove_listen(self, *keys):        
        keys = [_bytes(k) for k in keys]
        self._matchadd_wait.difference_update(keys)
        self._matchremove_wait.update(keys)
        await self.wait_for_send(ModifyListen(self, ModifyListen.SUBSCRIBE))

    async def publish(self, keys = (), extrainfo = None):
        keys = [_bytes(k) for k in keys]
        if self._publish_wait:
            merged_keys = list(self._publish_wait.union(keys))
            self._publish_wait.clear()
        else:
            merged_keys = list(keys)
        if not merged_keys:
            return
        client, encoder, _ = await call_api(self, 'redisdb', 'getclient', {'vhost':self.vhostbind})
        transactid = '%s-%016x' % (self._publishkey, self._publishno)
        self._publishno += 1
        msg = encoder({'id':transactid, 'keys':[_str(k) for k in merged_keys], 'extrainfo': extrainfo})
        try:
            if len(merged_keys) > self._singlecastlimit:
                await client.execute_command(self, 'PUBLISH', self.prefix, msg)
            else:
                await client.batch_execute(self, *((('MULTI',),) +
                                                    tuple(('PUBLISH', self.prefix + k, msg) for k in merged_keys) +
                                                    (('EXEC',),)))
        except (IOError, ConnectionResetException):
            self._logger.warning('Following keys are not published because exception occurred, delay to next publish: %r', merged_keys, exc_info = True)
            self._publish_wait.update(merged_keys)

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
    # Bind to RedisDB vHost
    _default_vhostbind = ''
    # Use this prefix for subscribe channels
    _default_prefix = 'vlcp.updatenotifier.'
    # If a notification contains more than .singlecastlimit keys, do not replicate the notification
    # to all channels; only broadcast it once in the public channel.
    _default_singlecastlimit = 256
    # Use an extra deflate compression on notification, should not be necessary if you already have
    # module.redisdb.deflate=True
    _default_deflate = False
    def __init__(self, server):
        Module.__init__(self, server)
        self.create_api(api(self.createnotifier))

    async def load(self, container):
        self.scheduler.queue.addSubQueue(999, ModifyListen.createMatcher(), "redisnotifier_modifylisten")
        await Module.load(self, container)

    async def unload(self, container, force=False):
        await Module.unload(self, container, force=force)
        await container.syscall_noreturn(syscall_removequeue(self.scheduler.queue, "redisnotifier_modifylisten"))

    def createnotifier(self):
        "Create a new notifier object"
        n = _Notifier(self.vhostbind, self.prefix, self.scheduler, self.singlecastlimit, self.deflate)
        n.start()
        return n


UpdateNotifier = proxy('UpdateNotifier', RedisNotifier)

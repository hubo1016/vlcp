'''
Created on 2016/10/9

:author: hubo
'''
from vlcp.config import defaultconfig
from vlcp.service.connection.tcpserver import TcpServerBase
from vlcp.server.module import api, ModuleLoadStateChanged, callAPI
from vlcp.event.lock import Lock
from zlib import compress, decompress, error as zlib_error
import pickle
from vlcp.utils.zkclient import ZooKeeperClient, ZooKeeperSessionUnavailable,\
        ZooKeeperSessionStateChanged
from vlcp.protocol.zookeeper import ZooKeeper
from namedstruct.namedstruct import dump
from random import randrange, shuffle, sample
import logging
from vlcp.event.runnable import RoutineContainer, MultipleException
from vlcp.event.event import withIndices, Event
import functools
from vlcp.event.core import QuitException
from contextlib import closing
try:
    import cPickle
except ImportError:
    pass
import json
from vlcp.utils.jsonencoder import encode_default, decode_object
import itertools
try:
    from itertools import izip
except ImportError:
    def izip(*args, **kwargs):
        return iter(zip(*args, **kwargs))

from time import time
try:
    from urllib.parse import urlsplit, urlunsplit
except Exception:
    from urlparse import urlsplit, urlunsplit
import vlcp.utils.zookeeper as zk
from uuid import uuid1
from vlcp.utils.indexedheap import IndexedHeap
from vlcp.event.lock import Lock

def _tobytes(k):
    if not isinstance(k, bytes):
        return k.encode('utf-8')
    else:
        return k

def _escape_path(key):
    '''
    Replace '/', '\\' in key
    '''
    return _tobytes(key).replace(b'$', b'$_').replace(b'/', b'$+').replace(b'\\', b'$$')

def _unescape_path(key):
    return _tobytes(key).replace(b'$$', b'\\').replace(b'$+', b'/').replace(b'$_', b'$')

class ZooKeeperResultException(Exception):
    pass


@withIndices('notifier', 'transactid', 'keys', 'reason', 'fromself')
class UpdateNotification(Event):
    UPDATED = 'updated'
    RESTORED = 'restored'

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

def _str(s):
    if isinstance(s, str):
        return s
    else:
        return s.decode('utf-8')

def _discard_watchers(watchers):
    for w in watchers:
        if w is not None:
            w.close()

class _Notifier(RoutineContainer):
    _logger = logging.getLogger(__name__ + '.Notifier')
    def __init__(self, vhostbind, scheduler=None, singlecastlimit = 32, deflate = False):
        RoutineContainer.__init__(self, scheduler=scheduler, daemon=False)
        self.vhostbind = vhostbind
        self._poll_routines = {}
        self._publishkey = uuid1().hex
        self._subscribekey = uuid1().hex
        self._publishno = 1
        self._subscribeno = 1
        self._publish_wait = set()
        self._singlecastlimit = singlecastlimit
        self._deflate = deflate
        self._heap = IndexedHeap()
        self._transacts = {}
        self._timestamp = ''
        self._transactno = 0
        self._client = None
        self._decoder = None
    def _check_completes(self, completes, err_allows = (), err_expects = ()):
        for result in completes:
            if result.err != zk.ZOO_ERR_OK and result.err not in err_allows:
                if result.err not in err_expects:
                    self._logger.warning('Unexpected err result received: %r', dump(result))
                raise ZooKeeperResultException('Error result received: ' + str(result.err))
    def _insert_barrier(self, key, zxid):
        self._heap.push((False, key, zxid), (zxid, 0))
    def _remove_barrier(self, key, zxid, *new_transacts):
        self._heap.remove((False, key, zxid))
        # Insert new transactions
        for trans, zxid in new_transacts:
            trans_id = trans['id']
            if trans_id not in self._heap:
                self._transacts[trans_id] = trans
                self._heap.push((True, trans_id), (zxid, 1))
        while self._heap and self._heap.top()[0]:
            _, trans_id = self._heap.pop()
            transact = self._transacts.pop(trans_id)
            pubkey, sep, _ = trans_id.partition('-')
            local_transid = '%s%016x' % (self._timestamp, self._transactno)
            self._transactno += 1
            self.scheduler.emergesend(
                UpdateNotification(self, local_transid, tuple(_bytes(k) for k in transact['keys']),
                                   UpdateNotification.UPDATED,
                                   (sep and pubkey == self._publishkey),
                                   extrainfo = transact.get('extrainfo')))
    def _poller(self, client, key, decoder):
        if not client:
            return
        last_children = None
        last_zxid = 0
        try:
            sub_id = '%s-%016x' % (self._subscribekey, self._subscribeno)
            self._subscribeno += 1
            while True:
                for m in client.requests([zk.setdata(key, b''),
                                          zk.create(key, b'')],
                                         self):
                    yield m
                completes, lost, retries, _ = self.retvalue
                if lost or retries:
                    continue
                self._check_completes(completes, (zk.ZOO_ERR_NODEEXISTS, zk.ZOO_ERR_NONODE))
                break
            while True:
                try:
                    for m in client.requests([zk.getchildren(key, True)],
                                             self):
                        yield m
                    completes, lost, retries, watchers = self.retvalue
                    try:
                        if lost or retries:
                            continue
                        if completes[0].err == zk.ZOO_ERR_NONODE:
                            last_zxid = completes[0].zxid
                            # Insert a barrier in case there are updates
                            self._insert_barrier(key, last_zxid)
                            for m in client.requests([zk.setdata(key, b''),
                                                      zk.create(key, b'')],
                                                     self):
                                yield m
                            completes, lost, retries, _ = self.retvalue
                            if lost or retries:
                                continue
                            self._check_completes(completes, (zk.ZOO_ERR_NODEEXISTS, zk.ZOO_ERR_NONODE))
                            last_children = set()
                            continue
                        self._check_completes(completes)
                        current_children = set(completes[0].children)
                        if last_children is not None:
                            new_children = current_children.difference(last_children)
                        else:
                            new_children = set()
                        last_children = current_children
                        def _watcher_update(watcher):
                            try:
                                while True:
                                    with closing(self.executeWithTimeout(60, watcher.wait(self))) as g:
                                        for m in g:
                                            yield m
                                    if self.timeout:
                                        for i in range(0, 3):
                                            # Update the watcher key to prevent it from being recycled
                                            for m in client.requests([zk.setdata(key, b'')],
                                                                     self):
                                                yield m
                                            completes, lost, retries, _ = self.retvalue
                                            if lost or retries:
                                                continue
                                            else:
                                                self._check_completes(completes, (zk.ZOO_ERR_NONODE,))
                                                if completes[0].err == zk.ZOO_ERR_NONODE:
                                                    raise ZooKeeperSessionUnavailable('expired')
                                                break
                                        else:
                                            raise ZooKeeperSessionUnavailable('expired')
                                        continue
                                    else:
                                        break
                            except ZooKeeperSessionUnavailable:
                                self.retvalue = -1
                            else:
                                watcher_event = self.retvalue
                                # Insert a barrier
                                self._insert_barrier(key, watcher_event.zxid)
                                self.retvalue = watcher_event.zxid
                            finally:
                                watcher.close()
                        def _get_transacts(last_zxid, new_children):
                            try:
                                new_children = sorted(c for c in new_children if c.startswith(b'notify'))
                                reqs = [zk.getdata(key + b'/' + c)
                                                          for c in new_children]
                                completes = []
                                while reqs:
                                    try:
                                        for m in client.requests(reqs, self):
                                            yield m
                                    except ZooKeeperSessionUnavailable:
                                        break
                                    completes2, lost, retries, _ = self.retvalue
                                    completes.extend(completes2[:len(reqs) - len(lost) - len(retries)])
                                    reqs = lost + retries
                                completes = [c for c in completes if c.err != zk.ZOO_ERR_NONODE]
                                self._check_completes(completes)
                                result = []
                                for c in completes:
                                    try:
                                        result.append((decoder(c.data), c.stat.mzxid))
                                    except Exception:
                                        self._logger.warning('Error while decoding data: %r, will ignore. key = %r',
                                                             c.data, key, exc_info = True)
                            except Exception:
                                self._remove_barrier(key, last_zxid)
                                raise
                            else:
                                self._remove_barrier(key, last_zxid, *result)
                                self.retvalue = None
                        with closing(self.executeAll([_watcher_update(watchers[0]),
                                                  _get_transacts(last_zxid, new_children)])) as g:
                            for m in g:
                                yield m
                        ((last_zxid,), _) = self.retvalue
                    finally:
                        _discard_watchers(watchers)
                except QuitException:
                    raise
                except Exception:
                    if client._shutdown:
                        break
                    else:
                        self._logger.warning('Unexpected exception occurs', exc_info = True)
                        for m in self.waitWithTimeout(1):
                            yield m
        finally:
            self._remove_barrier(key, last_zxid)
    def _create_poller(self, key):
        self._poll_routines[key] = self.subroutine(self._poller(self._client, b'/vlcp/notifier/bykey/' + _escape_path(key), self._decoder))
    def main(self):
        try:
            self._timestamp = '%012x' % (int(time() * 1000),) + '-'
            self._transactno = 1
            for m in callAPI(self, 'zookeeperdb', 'getclient', {'vhost':self.vhostbind}):
                yield m
            client, encoder, decoder = self.retvalue
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
            self._client = client
            self._decoder = decoder
            def _recreate_pollers():
                for k in self._poll_routines:
                    if k:
                        self._poll_routines[k].close()
                        self._create_poller(k)
                if b'' in self._poll_routines:
                    self._poll_routines[b''].close()
                self._poll_routines[b''] = self.subroutine(self._poller(client, b'/vlcp/notifier/all', decoder))
            _recreate_pollers()
            connection_down = ZooKeeperSessionStateChanged.createMatcher(ZooKeeperSessionStateChanged.EXPIRED,
                                                                         client)
            connection_up = ZooKeeperSessionStateChanged.createMatcher(ZooKeeperSessionStateChanged.CREATED,
                                                                       client)
            module_loaded = ModuleLoadStateChanged.createMatcher(state = ModuleLoadStateChanged.LOADED,
                                                 _ismatch = lambda x: x._instance.getServiceName() == 'zookeeperdb')
            while True:
                yield (connection_down,)
                # Connection is down, wait for restore
                # The module may be reloaded
                while client.session_state == ZooKeeperSessionStateChanged.EXPIRED:
                    yield (connection_up, module_loaded)
                    if self.matcher is module_loaded:
                        # recreate pollers
                        for m in callAPI(self, 'zookeeperdb', 'getclient', {'vhost':self.vhostbind}):
                            yield m
                        client, encoder, decoder = self.retvalue
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
                        self._client = client
                        self._decoder = decoder
                        connection_down = ZooKeeperSessionStateChanged.createMatcher(ZooKeeperSessionStateChanged.EXPIRED,
                                                                                     client)
                        connection_up = ZooKeeperSessionStateChanged.createMatcher(ZooKeeperSessionStateChanged.CREATED,
                                                                                   client)
                        _recreate_pollers()
                if self._publish_wait:
                    self.subroutine(self.publish())
                local_transid = '%s%016x' % (self._timestamp, self._transactno)
                self._transactno += 1
                self.scheduler.emergesend(
                    UpdateNotification(self, local_transid, tuple(_bytes(k) for k in self._poll_routines if k),
                                       UpdateNotification.RESTORED,
                                       False,
                                       extrainfo = None))
        finally:
            for k in self._poll_routines:
                self._poll_routines[k].close()
    def add_listen(self, *keys):
        keys = [_bytes(k) for k in keys]
        for k in keys:
            if k not in self._poll_routines:
                self._create_poller(k)
        if False:
            yield
    def remove_listen(self, *keys):        
        keys = [_bytes(k) for k in keys]
        for k in keys:
            if k in self._poll_routines:
                self._poll_routines.pop(k).close()
        if False:
            yield
    @_delegate
    def publish(self, keys = (), extrainfo = None):
        keys = [_bytes(k) for k in keys]
        if self._publish_wait:
            merged_keys = list(self._publish_wait.union(keys))
            self._publish_wait.clear()
        else:
            merged_keys = list(keys)
        if not merged_keys:
            return
        merged_keys = [_escape_path(k) for k in merged_keys]
        for m in callAPI(self, 'zookeeperdb', 'getclient', {'vhost':self.vhostbind}):
            yield m
        client, encoder, _ = self.retvalue
        transactid = '%s-%016x' % (self._publishkey, self._publishno)
        self._publishno += 1
        msg = encoder({'id':transactid, 'keys':[_str(k) for k in merged_keys], 'extrainfo': extrainfo})
        transactid = _tobytes(transactid)
        if len(merged_keys) > self._singlecastlimit:
            reqs = [zk.create(b'/vlcp/notifier/all/notify-' + transactid + b'-', msg, False, True)]
            detect_path = b'/vlcp/notifier/all'
            recycle_path = [b'/vlcp/notifier/all']
        else:
            reqs = [zk.setdata(b'/vlcp/notifier/bykey/' + k, b'')
                    for k in merged_keys] + \
                   [zk.create(b'/vlcp/notifier/bykey/' + k, b'')
                    for k in merged_keys] + \
                    [zk.multi(
                        *[zk.multi_create(b'/vlcp/notifier/bykey/' + k + b'/notify-' + transactid + b'-',
                             msg, False, True)
                        for k in merged_keys])]
            detect_path = b'/vlcp/notifier/bykey/' + merged_keys[0]
            recycle_path = [b'/vlcp/notifier/bykey/' + k for k in merged_keys]
        while True:
            try:
                for m in client.requests(reqs, self, 30, priority = 1):
                    yield m
            except ZooKeeperSessionUnavailable:
                self._logger.warning('Following keys are not published because exception occurred, delay to next publish: %r', merged_keys, exc_info = True)
                self._publish_wait.update(merged_keys)
                break
            else:
                completes, lost, retries, _ = self.retvalue
                if retries:
                    continue
                elif lost:
                    # At least the last request is lost, we must detect whether it is completed
                    try:
                        for m in client.requests([zk.getchildren(detect_path)],
                                                 self, 30, priority = 2):
                            yield m
                    except ZooKeeperSessionUnavailable:
                        self._logger.warning('Following keys are not published because exception occurred, delay to next publish: %r', merged_keys, exc_info = True)
                        self._publish_wait.update(merged_keys)
                        break
                    else:
                        completes, lost, retries, _ = self.retvalue
                        self._check_completes(completes)
                        if lost or retries:
                            self._logger.warning('Following keys are not published because exception occurred, delay to next publish: %r', merged_keys, exc_info = True)
                            self._publish_wait.update(merged_keys)
                        else:
                            trans_prefix = b'notify-' + transactid + b'-'
                            if any((k.startswith(trans_prefix) for k in completes[0].children)):
                                # Succeeded
                                break
                            else:
                                # Failed
                                continue
                else:
                    self._check_completes(completes[-1:])
                    break
        for m in callAPI(self, 'zookeeperdb', 'recycle', {'vhost':self.vhostbind,
                                                          'keys': recycle_path}):
            yield m
                            
    def notification_matcher(self, fromself = None):
        if fromself is None:
            return UpdateNotification.createMatcher(self)
        else:
            return UpdateNotification.createMatcher(notifier = self, fromself = fromself)


@defaultconfig
class ZooKeeperDB(TcpServerBase):
    '''
    Create zookeeper clients to connect to redis server
    '''
    # This URL is special comparing to other connection URLs, it should be like::
    # 
    #     zk://server1[:port1],server2[:port2],server3[:port3]/[chroot]
    # 
    # Firstly there may be multiple hosts (and ports) in the connection URL, they
    # form a ZooKeeper cluster; secondly, there can be a "chroot" path in the URL
    # path. If "chroot" path apears, ZooKeeperDB uses a child node as the root node,
    # makes it easy to host multiple services in the same ZooKeeper cluster.
    _default_url = 'tcp://localhost/'
    # Default serialization protocol, "json" or "pickle"
    _default_serialize = 'json'
    # Use deflate to compress the serialized data
    _default_deflate = True
    # Pickle version, either a number or "default"/"highest"
    _default_pickleversion = 'default'
    # Use cPickle if possible
    _default_cpickle = True
    # Disable autosuccess; this module is loaded successfully after it initialized the ZooKeeper node.
    _default_autosuccess = False
    # Only enable ZooKeeper KVDB service on specified vHosts, this service uses ZooKeeper in a special way.
    # Other vHosts may be used as normal ZooKeeper clients without extra assumption.
    _default_kvdbvhosts = None
    # Notifier service vHost binding
    _default_notifierbind = ''
    # If a notification contains more than .singlecastlimit keys, do not replicate the notification
    # to all channels; only broadcast it once in the public channel.
    _default_singlecastlimit = 32
    # Use an extra deflate compression on notification, should not be necessary if you already have
    # .deflate=True
    _default_notifierdeflate = False
    client = True
    def __init__(self, server):
        self._zookeeper_clients = {}
        TcpServerBase.__init__(self, server, ZooKeeper)
        if self.serialize == 'pickle' or self.serialize == 'cpickle' or self.serialize == 'cPickle':
            if self.serialize == 'pickle':
                if not self.cpickle:
                    p = pickle
                else:
                    p = cPickle
            else:
                p = cPickle
            if self.pickleversion is None or self.pickleversion == 'default':
                pickleversion = None
            elif self.pickleversion == 'highest':
                pickleversion = p.HIGHEST_PROTOCOL
            else:
                pickleversion = self.pickleversion
            if self.deflate:
                def _encode(obj):
                    return compress(p.dumps(obj, pickleversion), 1)
            else:
                def _encode(obj):
                    return p.dumps(obj, pickleversion)
            self._encode = _encode
            if self.deflate:
                def _decode(data):
                    if data is None:
                        return None
                    else:
                        try:
                            return p.loads(decompress(data))
                        except zlib_error:
                            return p.loads(data)
            else:
                def _decode(data):
                    if data is None:
                        return None
                    else:
                        return p.loads(data)
            self._decode = _decode
        else:
            if self.deflate:
                def _encode(obj):
                    return compress(json.dumps(obj, default=encode_default).encode('utf-8'), 1)
                self._encode = _encode
                def _decode(data):
                    if data is None:
                        return None
                    else:
                        try:
                            data = decompress(data)
                        except zlib_error:
                            pass
                        if not isinstance(data, str) and isinstance(data, bytes):
                            data = data.decode('utf-8')
                        return json.loads(data, object_hook=decode_object)
                self._decode = _decode
            else:
                def _encode(obj):
                    return json.dumps(obj, default=encode_default).encode('utf-8')
                self._encode = _encode
                def _decode(data):
                    if data is None:
                        return None
                    elif not isinstance(data, str) and isinstance(data, bytes):
                        data = data.decode('utf-8')
                    return json.loads(data, object_hook=decode_object)
                self._decode = _decode
        self.appendAPI(api(self.getclient),
                       api(self.get, self.apiroutine),
                       api(self.set, self.apiroutine),
                       api(self.delete, self.apiroutine),
                       api(self.mget, self.apiroutine),
                       api(self.mset, self.apiroutine),
                       api(self.update, self.apiroutine),
                       api(self.mupdate, self.apiroutine),
                       api(self.updateall, self.apiroutine),
                       api(self.updateallwithtime, self.apiroutine),
                       api(self.recycle),
                       api(self.createnotifier),
                       api(self.listallkeys, self.apiroutine))
        self.apiroutine.main = self._main
        self.routines.append(self.apiroutine)
        self._recycle_list = {}
        self._identifier_uuid = uuid1().hex
        self._identifier_counter = 0

    def _check_completes(self, completes, err_allows = (), err_expects = ()):
        for result in completes:
            if result.err != zk.ZOO_ERR_OK and result.err not in err_allows:
                if result.err not in err_expects:
                    self._logger.warning('Unexpected err result received: %r', dump(result))
                raise ZooKeeperResultException('Error result received: ' + str(result.err))
    
    def _main(self):
        # Initialize structures
        def initialize_zookeeper(client, vhost):
            # Create following nodes:
            # /kvdb, /notifier, /notifier/all, /notifier/bykey
            self._recycle_list[vhost] = set()
            while True:
                for m in client.requests([zk.create(b'/vlcp', b''),
                                          zk.create(b'/vlcp/kvdb', b''),
                                          zk.create(b'/vlcp/notifier', b''),
                                          zk.create(b'/vlcp/notifier/all', b''),
                                          zk.create(b'/vlcp/notifier/bykey', b''),
                                          zk.create(b'/vlcp/tmp', b''),
                                          zk.create(b'/vlcp/tmp/timer', b'')],
                                         self.apiroutine):
                    yield m
                result = self.apiroutine.retvalue
                if not result[1] and not result[2]:
                    break
        allow_vhosts = self.kvdbvhosts
        if allow_vhosts is None:
            routines = [initialize_zookeeper(client, vh) for vh, client in self._zookeeper_clients.items()]
        else:
            routines = [initialize_zookeeper(client, vh) for vh, client in self._zookeeper_clients.items() if vh in allow_vhosts]
        try:
            with closing(self.apiroutine.executeAll(routines, retnames = ())) as g:
                for m in g:
                    yield m
        except Exception as exc:
            for m in self.changestate(ModuleLoadStateChanged.FAILED, self.apiroutine):
                yield m
            raise exc
        else:
            for m in self.changestate(ModuleLoadStateChanged.SUCCEEDED, self.apiroutine):
                yield m
            if allow_vhosts is None:
                recycle_routines = [self._recycle_routine(client, vh) for vh, client in self._zookeeper_clients.items()]
            else:
                recycle_routines = [self._recycle_routine(client, vh) for vh, client in self._zookeeper_clients.items() if vh in allow_vhosts]
            with closing(self.apiroutine.executeAll(recycle_routines, retnames = ())) as g:
                for m in g:
                    yield m
    def _client_class(self, config, protocol, vhost):
        def _create_client(url, protocol, scheduler = None, key = None, certificate = None, ca_certs = None, bindaddress = None):
            # URL should like: zk://<server1>[:port1],<server2>[:port2],.../chrootpath
            r = urlsplit(url, 'tcp')
            server_list = [urlunsplit((r.scheme, l.strip(), '/', '', '')) for l in r.netloc.split(',') if l.strip()]
            client = ZooKeeperClient(self.apiroutine, server_list, r.path)
            if key:
                client.key = key
            if certificate:
                client.certificate = certificate
            if ca_certs:
                client.ca_certs = ca_certs
            self._zookeeper_clients[vhost] = client
            return client
        return _create_client
    def getclient(self, vhost = ''):
        "Return a tuple of ``(zookeeperclient, encoder, decoder)`` for specified vhost"
        return (self._zookeeper_clients.get(vhost), self._encode, self._decode)
    def get(self, key, timeout = None, vhost = ''):
        "Get value from key"
        for m in self.mget((key,), vhost = vhost):
            yield m
        self.apiroutine.retvalue = self.apiroutine.retvalue[0]
    def set(self, key, value, timeout = None, vhost = ''):
        "Set value to key, with an optional timeout"
        # These interfaces are just for compatibility. Inefficiency is OK.
        for m in self.mset(((key, value),), timeout, vhost):
            yield m
        self.apiroutine.retvalue = None
    def delete(self, key, vhost = ''):
        "Delete a key from the storage"
        def updater(keys, values, server_time):
            return ((key,), (None,))
        for m in self.updateallwithtime((), updater, None, vhost):
            yield m
        self.apiroutine.retvalue = None
    def mget(self, keys, vhost = ''):
        "Get multiple values from multiple keys"
        if not keys:
            self.apiroutine.retvalue = []
            return
        client = self._zookeeper_clients.get(vhost)
        if client is None:
            raise ValueError('vhost ' + repr(vhost) + ' is not defined')
        escaped_keys = [_escape_path(k) for k in keys]
        for m in client.requests([zk.getchildren2(b'/vlcp/kvdb/' + k) for k in escaped_keys], self.apiroutine, 60):
            yield m
        completes, losts, retries, _ = self.apiroutine.retvalue
        if losts or retries:
            raise ZooKeeperSessionUnavailable(ZooKeeperSessionStateChanged.DISCONNECTED)
        zxid_limit = completes[0].zxid
        def retrieve_version(ls2_result, rootdir, zxid_limit):
            if ls2_result.err != zk.ZOO_ERR_OK:
                if ls2_result.err != zk.ZOO_ERR_NONODE:
                    self._logger.warning('Unexpected error code is received for %r: %r', rootdir, dump(ls2_result))
                    raise ZooKeeperResultException('Unexpected error code is received: ' + str(ls2_result.err))
                self.apiroutine.retvalue = None
                return
            children = [(name.rpartition(b'-'), name) for name in ls2_result.children if name.startswith(b'data-')]
            children.sort(reverse = True)
            for _, name in children:
                for m in client.requests([zk.getdata(rootdir + name)], self.apiroutine, 60):
                    yield m
                completes, losts, retries, _ = self.apiroutine.retvalue
                if losts or retries:
                    # Should not happen but in case...
                    raise ZooKeeperSessionUnavailable(ZooKeeperSessionStateChanged.DISCONNECTED)
                if completes[0].err == zk.ZOO_ERR_NONODE:
                    # Though not quite possible, in extreme situations the key might be lost
                    # Return None should be the correct result for the most time
                    self.apiroutine.retvalue = None
                    return
                self._check_completes(completes)
                if completes[0].stat.mzxid <= zxid_limit:
                    if completes[0].data:
                        self.apiroutine.retvalue = completes[0].data
                    else:
                        self.apiroutine.retvalue = None
                    return
            self.apiroutine.retvalue = None
        with closing(self.apiroutine.executeAll([retrieve_version(r, b'/vlcp/kvdb/' + k + b'/', zxid_limit)
                                             for r,k in izip(completes, escaped_keys)])) as g:
            for m in g:
                yield m
        self.apiroutine.retvalue = [self._decode(r[0]) for r in self.apiroutine.retvalue]
    def mset(self, kvpairs, timeout = None, vhost = ''):
        "Set multiple values on multiple keys"
        if not kvpairs:
            return
        d = kvpairs
        if hasattr(d, 'items'):
            d = d.items()
        keys = [kv[0] for kv in d]
        values = [kv[1] for kv in d]
        def updater(k, v, server_time):
            return (keys, values)
        for m in self.updateallwithtime((), updater, timeout, vhost):
            yield m
        self.apiroutine.retvalue = None
    def update(self, key, updater, timeout = None, vhost = ''):
        '''
        Update in-place with a custom function
        
        :param key: key to update
        
        :param updater: ``func(k,v)``, should return a new value to update, or return None to delete. The function
                        may be call more than once.
        
        :param timeout: new timeout
        
        :returns: the updated value, or None if deleted
        '''
        for m in self.mupdate((key,), updater, timeout, vhost):
            yield m
        (self.apiroutine.retvalue,) = self.apiroutine.retvalue
    def mupdate(self, keys, updater, timeout = None, vhost = ''):
        """
        Update multiple keys in-place with a custom function, see update.
        
        Either all success, or all fail.
        """
        def new_updater(keys, values, server_time):
            return (keys, [updater(k,v) for k,v in izip(keys, values)])
        for m in self.updateallwithtime(keys, new_updater, timeout, vhost):
            yield m
        _, self.apiroutine.retvalue = self.apiroutine.retvalue
    def updateall(self, keys, updater, timeout = None, vhost = ''):
        """
        Update multiple keys in-place, with a function ``updater(keys, values)``
        which returns ``(updated_keys, updated_values)``.
        
        Either all success or all fail
        """
        def new_updater(keys, values, server_time):
            return updater(keys, values)
        for m in self.updateallwithtime(keys, new_updater, timeout, vhost):
            yield m
    def updateallwithtime(self, keys, updater, timeout = None, vhost = ''):
        """
        Update multiple keys in-place, with a function ``updater(keys, values, timestamp)``
        which returns ``(updated_keys, updated_values)``.
        
        Either all success or all fail.
        
        Timestamp is a integer standing for current time in microseconds.
        """
        client = self._zookeeper_clients.get(vhost)
        if client is None:
            raise ValueError('vhost ' + repr(vhost) + ' is not defined')
        sorted_keys = sorted(keys)
        # Current implementation cannot accept duplicated keys
        for i in range(0, len(sorted_keys) -1):
            if sorted_keys[i] == sorted_keys[i+1]:
                self._logger.warning('Duplicated keys are provided: %r. They should not appear normally. This is a BUG.', repr(sorted_keys[i]))
                self._logger.warning('All keys: %r', keys)
                # Create a wrapper
                unique_keys = list(set(keys))
                def _unique_keys_updater(uniq_keys, values, timestamp):
                    value_dict = dict(zip(uniq_keys, values))
                    return updater(keys, [value_dict[k] for k in keys], timestamp)
                for m in self.updateallwithtime(unique_keys, _unique_keys_updater, timeout, vhost):
                    yield m
                return
        locks = [Lock((k, 'zookeeperdb_writelock'), self.apiroutine.scheduler) for k in sorted_keys]
        try:
            for l in locks:
                for m in l.lock(self.apiroutine):
                    yield m
            barrier_list = []
            def _pre_create_keys(escaped_keys, session_lock = None):
                if escaped_keys:
                    # Even if the node already exists, we modify it to increase the version,
                    # So recycling would not remove it
                    create_requests = [request
                                        for k in escaped_keys
                                           for request in (
                                                zk.setdata(b'/vlcp/kvdb/' + k, b''),
                                                zk.create(b'/vlcp/kvdb/' + k, b'')
                                            )
                                        ]
                    while True:
                        for m in client.requests(create_requests, self.apiroutine, 60, session_lock,
                                                 priority = 1):
                            yield m
                        completes, losts, retries, _ = self.apiroutine.retvalue
                        self._check_completes(completes[:len(create_requests) - len(losts) - len(retries)], (zk.ZOO_ERR_NONODE, zk.ZOO_ERR_NODEEXISTS))
                        if losts or retries:
                            create_requests = losts + retries
                        else:
                            break
            try:
                if keys:
                    # Create barriers
                    counter = self._identifier_counter
                    self._identifier_counter = counter + 1
                    # We must use an unique name to identify the created node, since the response might be lost
                    # due to connection lost
                    identifier = _tobytes('barrier%s%010d-' % (self._identifier_uuid, counter))
                    escaped_keys = [_escape_path(k) for k in keys]
                    # The keys may not exists, create the parent nodes; if they already exists, the creation fails
                    # It is not necessary to use a transact, it is OK as long as the nodes are finally created
                    for m in _pre_create_keys(escaped_keys):
                        yield m
                    # Register these keys for a future recycling
                    self._recycle_list[vhost].update((b'/vlcp/kvdb/' + k for k in escaped_keys))
                    sync_limit = 4
                    while True:
                        for m in client.requests([zk.multi(
                                                *[zk.multi_create(b'/vlcp/kvdb/' + k + b'/' + identifier,
                                                                  b'', True, True)
                                                  for k in escaped_keys]
                                            )], self.apiroutine, 60, priority = 1):
                            yield m
                        completes, losts, retries, _ = self.apiroutine.retvalue
                        if not losts and not retries:
                            self._check_completes(completes)
                            multi_resp = completes[0].responses[:len(escaped_keys)]
                            if any(r.err == zk.ZOO_ERR_NONODE for r in multi_resp):
                                # The atomic broadcast only reach half of the servers,
                                # so if we create the nodes and immediately lost connection
                                # and reconnect to another server, the nodes may not be ready.
                                # Send a sync to solve this problem.
                                # But isn't it a ZooKeeper bug? And when sync is not solving the
                                # problem, tries pre-create again.
                                if sync_limit <= 0:
                                    self._logger.warning('Inconsist result detected, first 10 keys = %r', escaped_keys[:10])
                                    first_err = next(num for num,r in enumerate(multi_resp) if r.err != zk.ZOO_ERR_OK)
                                    self._logger.warning('The first non-OK result is: %r (No. %d)', escaped_keys[first_err], first_err)
                                    if first_err == 0:
                                        self._logger.warning('Not retrying to prevent a dead loop')
                                        raise ZooKeeperSessionUnavailable(ZooKeeperSessionStateChanged.DISCONNECTED)
                                    else:
                                        self._logger.warning('Retry create the keys')
                                    for m in _pre_create_keys(escaped_keys[first_err:]):
                                        yield m
                                    sync_limit = 4
                                sync_limit -= 1
                                for m in client.requests([zk.sync(b'/vlcp/kvdb')],
                                                         self.apiroutine, 60):
                                    yield m
                                continue
                            self._check_completes(multi_resp)
                            barrier_list = [r.path for r in multi_resp]
                            session_lock = client.session_id
                            break
                        if losts:
                            self._logger.warning('Barrier create result lost, check the result, identifier = %r', identifier)
                            # Response is lost, we must check the result
                            for m in client.requests([zk.sync(b'/vlcp/kvdb/' + escaped_keys[0]),
                                                 zk.getchildren(b'/vlcp/kvdb/' + escaped_keys[0])],
                                                self.apiroutine, 60, priority = 2):
                                yield m
                            completes, losts, retries, _ = self.apiroutine.retvalue
                            if losts or retries:
                                raise ZooKeeperSessionUnavailable(ZooKeeperSessionStateChanged.DISCONNECTED)
                            self._check_completes(completes)
                            created_identifier = [child for child in
                                                        sorted(completes[1].children,
                                                               key = lambda x: x.rpartition(b'-')[2], reverse = True)
                                                 if child.startswith(identifier)]
                            if len(created_identifier) > 1:
                                self._logger.warning('Replicated barriers are created')
                            if created_identifier:
                                # Succeeded, retrieve other created names
                                barrier_list = [b'/vlcp/kvdb/' + escaped_keys[0] + b'/' + created_identifier[0]]
                                session_lock = client.session_id
                                if len(escaped_keys) > 1:
                                    for m in client.requests([zk.getchildren(b'/vlcp/kvdb/' + k)
                                                         for k in escaped_keys[1:]],
                                                        self.apiroutine, 60, session_lock, priority = 2):
                                        yield m
                                    completes, losts, retries, _ = self.apiroutine.retvalue
                                    if losts or retries:
                                        raise ZooKeeperSessionUnavailable(ZooKeeperSessionStateChanged.DISCONNECTED)
                                    self._check_completes(completes)
                                    rollback = False
                                    for k,r in izip(escaped_keys[1:], completes):
                                        created_identifier = [child for child in sorted(completes[1].children,
                                                               key = lambda x: x.rpartition(b'-')[2], reverse = True) if child.startswith(identifier)]
                                        if not created_identifier:
                                            # Should not happen, but if that happens, it will cause a dead lock
                                            self._logger.warning('Created barriers are missing, roll back other barriers. key = %r', k)
                                            rollback = True
                                        else:
                                            barrier_list.append(b'/vlcp/kvdb/' + k + b'/' + created_identifier[0])
                                    if rollback:
                                        raise ZooKeeperResultException('Barriers are lost')
                                break
                    # Use mtime for created barrier for timestamp
                    for m in client.requests([zk.exists(barrier_list[0])],
                                        self.apiroutine, 60, session_lock, priority = 1):
                        yield m
                    completes, losts, retries, _ = self.apiroutine.retvalue
                    if losts or retries:
                        raise ZooKeeperSessionUnavailable(ZooKeeperSessionStateChanged.DISCONNECTED)                
                    self._check_completes(completes)
                    server_time = completes[0].stat.mtime * 1000
                    # Retrieve values
                    for m in client.requests([zk.getchildren(b'/vlcp/kvdb/' + k)
                                         for k in escaped_keys],
                                        self.apiroutine, 60, session_lock, priority = 1):
                        yield m
                    completes, losts, retries, _ = self.apiroutine.retvalue
                    if losts or retries:
                        raise ZooKeeperSessionUnavailable(ZooKeeperSessionStateChanged.DISCONNECTED)
                    self._check_completes(completes)
                    value_path = []
                    for b,r,k in izip(barrier_list, completes, escaped_keys):
                        barrier_seq = b.rpartition(b'-')[2]
                        try:
                            maxitem = max((item for item in
                                            ((name.rpartition(b'-')[2], name) for name in r.children)
                                            if item[0] < barrier_seq))
                        except Exception:
                            # no valid value
                            value_path.append(None)
                        else:
                            name = maxitem[1]
                            if name.startswith(b'barrier'):
                                # There is a pending operation, it may be done, or cancelled
                                # We must wait for it
                                value_path.append((True, k, sorted((item for item in
                                            ((name.rpartition(b'-')[2], name) for name in r.children)
                                            if item[0] < barrier_seq), reverse = True)))
                            else:
                                value_path.append((False, b'/vlcp/kvdb/' + k + b'/' + name))
                    def _normal_get_value():
                        for m in client.requests([zk.getdata(p[1])
                                             for p in value_path if p is not None and not p[0]],
                                            self.apiroutine, 60, session_lock, priority = 1):
                            yield m
                        completes, losts, retries, _ = self.apiroutine.retvalue
                        if losts or retries:
                            raise ZooKeeperSessionUnavailable(ZooKeeperSessionStateChanged.DISCONNECTED)
                        self._check_completes(completes)
                        self.apiroutine.retvalue = [r.data if r.data else None for r in completes]
                    def _wait_value(key, children):
                        dup_names = [name for _,name in children if name.startswith(identifier)]
                        if dup_names:
                            self._logger.warning('There are duplicated barriers. Removing...')
                            delete_requests = [zk.delete(b'/vlcp/kvdb/' + key + b'/' + name)
                                                     for name in dup_names]
                            while delete_requests:
                                for m in client.requests(delete_requests,
                                                         self.apiroutine, 60, session_lock, priority = 1):
                                    yield m
                                completes, lost, retries, _ = self.apiroutine.retvalue
                                self._check_completes(completes[:len(completes) - len(lost) - len(retries)],
                                                      (zk.ZOO_ERR_NONODE,))
                                if lost or retries:
                                    delete_requests = lost + retries
                            children = [child for child in children if not child[1].startswith(identifier)]
                        for seq, name in children:
                            if name.startswith(b'barrier'):
                                while True:
                                    for m in client.requests([zk.getdata(b'/vlcp/kvdb/' + key + b'/' + name, True)],
                                                        self.apiroutine, 60, session_lock, priority = 1):
                                        yield m
                                    completes, losts, retries, waiters = self.apiroutine.retvalue
                                    try:
                                        if losts or retries:
                                            raise ZooKeeperSessionUnavailable(ZooKeeperSessionStateChanged.DISCONNECTED)
                                        self._check_completes(completes, (zk.ZOO_ERR_NONODE,))
                                        if completes[0].err == zk.ZOO_ERR_OK:
                                            # wait for the barrier to be removed
                                            for m in self.apiroutine.executeWithTimeout(90, waiters[0].wait(self.apiroutine)):
                                                yield m
                                            if self.apiroutine.timeout:
                                                self._logger.warning('Wait too long on a barrier, possible deadlock, key = %r, name = %r', key, name)
                                                self._logger.warning('Try to remove it to solve the problem. THIS IS NOT NORMAL.')
                                                for m in client.requests([zk.delete(b'/vlcp/kvdb/' + key + b'/' + name)],
                                                                         self.apiroutine, 60, session_lock, priority = 2):
                                                    yield m
                                                completes, lost, retries, _ = self.apiroutine.retvalue
                                                self._check_completes(completes, (zk.ZOO_ERR_NONODE,))
                                            else:
                                                if self.apiroutine.retvalue.type != zk.DELETED_EVENT_DEF:
                                                    # Should not happen
                                                    continue
                                                else:
                                                    break
                                        else:
                                            break
                                    finally:
                                        _discard_watchers(waiters)
                                # Done or roll back, let's check
                                for m in client.requests([zk.getdata(b'/vlcp/kvdb/' + key + b'/data-' + seq)],
                                                    self.apiroutine, 60, session_lock, priority = 1):
                                    yield m
                                completes, losts, retries, _ = self.apiroutine.retvalue
                                if losts or retries:
                                    raise ZooKeeperSessionUnavailable(ZooKeeperSessionStateChanged.DISCONNECTED)
                                self._check_completes(completes, (zk.ZOO_ERR_NONODE,))
                                if completes[0].err == zk.ZOO_ERR_OK:
                                    # Done
                                    self.apiroutine.retvalue = completes[0].data if completes[0].data else None
                                    return
                            else:
                                # Normal get
                                for m in client.requests([zk.getdata(b'/vlcp/kvdb/' + key + b'/' + name)],
                                                    self.apiroutine, 60, session_lock, priority = 1):
                                    yield m
                                completes, losts, retries, _ = self.apiroutine.retvalue
                                if losts or retries:
                                    raise ZooKeeperSessionUnavailable(ZooKeeperSessionStateChanged.DISCONNECTED)
                                self._check_completes(completes)
                                self.apiroutine.retvalue = completes[0].data if completes[0].data else None
                                return
                        self.apiroutine.retvalue = None
                    try:
                        with closing(self.apiroutine.executeAll([_normal_get_value()] + \
                                        [_wait_value(vp[1], vp[2])
                                         for vp in value_path
                                         if vp and vp[0]])) as g:
                            for m in g:
                                yield m
                    except MultipleException as exc:
                        raise exc.args[0]
                    results = self.apiroutine.retvalue
                    values = []
                    normal_result_iterator = iter(results[0][0])
                    wait_result_iterator = (r[0] for r in results[1:])
                    for vp in value_path:
                        if vp is None:
                            values.append(None)
                        elif vp[0]:
                            values.append(next(wait_result_iterator))
                        else:
                            values.append(next(normal_result_iterator))
                else:
                    barrier_list = []
                    escaped_keys = []
                    # We still need a timestamp, use /vlcp/tmp/timer
                    while True:
                        for m in client.requests([zk.setdata(b'/vlcp/tmp/timer', b''),
                                             zk.exists(b'/vlcp/tmp/timer')],
                                            self.apiroutine, 60, priority = 1):
                            yield m
                        completes, losts, retries, _ = self.apiroutine.retvalue
                        if not losts and not retries:
                            break
                    self._check_completes(completes)
                    server_time = completes[1].stat.mtime * 1000
                    session_lock = client.session_id
                    values = []
                try:
                    new_keys, new_values = updater(keys, [self._decode(v) for v in values], server_time)
                    keys_deleted = []
                    values_encoded = []
                    key_dup_test_set = set()
                    kv_set_list = list(izip(new_keys, new_values))
                    kv_set_list.reverse()
                    for k,v in kv_set_list:
                        if k in key_dup_test_set:
                            self._logger.warning('Updater returns duplicated keys %r, this is a BUG.', k)
                            continue
                        key_dup_test_set.add(k)
                        if v is None:
                            keys_deleted.append(k)
                        else:
                            values_encoded.append((k, self._encode(v)))
                except Exception:
                    raise
                # Write the result
                # If the keys are in the old keys, replace the barrier to the new data.
                # If the keys are not in the old keys, create a new node
                old_key_set = set(keys)
                delete_other_keys = [_escape_path(k) for k in keys_deleted if k not in old_key_set]
                delete_barrier_keys = [_escape_path(k) for k in keys_deleted if k in old_key_set]
                set_other_keys = [(_escape_path(k),v) for k,v in values_encoded if k not in old_key_set]
                set_barrier_keys = [(_escape_path(k),v) for k,v in values_encoded if k in old_key_set]
                
                barrier_dict = dict(zip(escaped_keys, barrier_list))
                # pre-create the new keys parent node
                for m in _pre_create_keys(delete_other_keys + [k for k,_ in set_other_keys], session_lock):
                    yield m
                # register them to be recycled later
                self._recycle_list[vhost].update((b'/vlcp/kvdb/' + k for k in (delete_other_keys + [k for k,_ in set_other_keys])))
                # We must create/delete all the keys in a single transaction
                # Should understand that this means the total write data must be limited to less than 4MB
                # Compress may help, but do not expect too much
                multi_op = [zk.multi_create(b'/vlcp/kvdb/' + k + b'/data-', b'', False, True)
                             for k in delete_other_keys]
                multi_op.extend((zk.multi_create(b'/vlcp/kvdb/' + k + b'/data-', data, False, True)
                             for k,data in set_other_keys))
                multi_op.extend((zk.multi_create(b'/vlcp/kvdb/' + k + b'/data-' + barrier_dict[k].rpartition(b'-')[2], b'')
                             for k in delete_barrier_keys))
                multi_op.extend((zk.multi_create(b'/vlcp/kvdb/' + k + b'/data-' + barrier_dict[k].rpartition(b'-')[2], data)
                             for k,data in set_barrier_keys))
                multi_op.extend((zk.multi_delete(k) for k in barrier_list))
                if multi_op:
                    while True:
                        for m in client.requests([zk.multi(*multi_op)],
                                            self.apiroutine, 60, session_lock, priority = 2):
                            yield m                    
                        completes, losts, retries, _ = self.apiroutine.retvalue
                        if retries:
                            raise ZooKeeperSessionUnavailable(ZooKeeperSessionStateChanged.DISCONNECTED)
                        elif losts:
                            if barrier_list:
                                self._logger.warning('Write response is lost, check the result')
                                # Check whether the transaction is succeeded
                                for m in client.requests([zk.sync(barrier_list[0]),
                                                     zk.exists(barrier_list[0])],
                                                    self.apiroutine, 60, session_lock, priority = 2):
                                    yield m
                                completes, losts, retries, _ = self.apiroutine.retvalue
                                if losts or retries:
                                    raise ZooKeeperSessionUnavailable(ZooKeeperSessionStateChanged.DISCONNECTED)
                                else:
                                    self._check_completes(completes, (zk.ZOO_ERR_NONODE,),)
                                    if completes[1].err == zk.ZOO_ERR_NONODE:
                                        # Already deleted, so the transaction is succeeded
                                        break
                                    # Retry the transaction else wise
                            else:
                                # If there is not a barrier, we cannot determine whether the transaction is successful
                                # But it also will not cause any deadlock, so we simply ignore it and assume it fails
                                # A write operation without barriers usually means it can be retried safely, no transaction
                                # is needed
                                raise ZooKeeperSessionUnavailable(ZooKeeperSessionStateChanged.DISCONNECTED)
                        else:
                            self._check_completes(completes)
                            self._check_completes(completes[0].responses[:len(multi_op)])
                            break
                self.apiroutine.retvalue = (new_keys, new_values)
            except Exception:
                if barrier_list:
                    def clearup():
                        # if the session expires, the barrier should automatically be removed
                        try:
                            for m in client.requests([zk.delete(p) for p in barrier_list],
                                                self.apiroutine, 60, client.session_id, priority = 2):
                                yield m
                        except ZooKeeperSessionUnavailable:
                            pass
                    self.apiroutine.subroutine(clearup())
                raise
        finally:
            for l in locks:
                l.unlock()
    
    def _recycle_routine(self, client, vhost):
        # Sleep for a random interval
        for m in self.apiroutine.waitWithTimeout(randrange(0, 60)):
            yield m
        _recycle_list = set(self._recycle_list[vhost])
        self._recycle_list[vhost].clear()
        def _recycle_key(recycle_key):
            is_notifier = recycle_key.startswith(b'/vlcp/notifier/bykey/')
            # Retrieve server time
            for m in client.requests([zk.setdata(b'/vlcp/tmp/timer', b''),
                                 zk.exists(b'/vlcp/tmp/timer')],
                                self.apiroutine, 60):
                yield m
            completes, losts, retries, _ = self.apiroutine.retvalue
            if losts or retries:
                raise ZooKeeperSessionUnavailable(ZooKeeperSessionStateChanged.DISCONNECTED)
            self._check_completes(completes)
            # time limit is 2 minutes ago
            time_limit = completes[1].stat.mtime - 120000
            # Get the children list
            for m in client.requests([zk.getchildren2(recycle_key)],
                                     self.apiroutine, 60):
                yield m
            completes, losts, retries, _ = self.apiroutine.retvalue
            if losts or retries:
                raise ZooKeeperSessionUnavailable(ZooKeeperSessionStateChanged.DISCONNECTED)
            self._check_completes(completes, (zk.ZOO_ERR_NONODE,))
            if completes[0].err == zk.ZOO_ERR_NONODE:
                self.apiroutine.retvalue = False
                return
            can_recycle_parent = (completes[0].stat.mtime < time_limit)
            recycle_parent_version = completes[0].stat.version
            children = [name for name in completes[0].children if name.startswith(b'data') or name.startswith(b'notify')]
            other_children = completes[0].stat.numChildren - len(children)
            children.sort(key = lambda x: (x.rpartition(b'-')[2], x))
            # Use a binary search to find the boundary for deletion
            # We recycle a version if:
            # 1. It has been created for more than 2 minutes; and
            # 2. It is not the latest version in all versions that matches (1), unless it is empty
            # We leave the latest value because mget() from other clients to other ZooKeeper servers
            # might need the old version of data to keep mget() to get the same version for all keys
            begin = 0
            end = len(children)
            is_empty = True
            while begin < end:
                middle = (begin + end) // 2
                for m in client.requests([zk.exists(recycle_key + b'/' + children[middle])],
                                         self.apiroutine, 60):
                    yield m
                completes, losts, retries, _ = self.apiroutine.retvalue
                if losts or retries:
                    raise ZooKeeperSessionUnavailable(ZooKeeperSessionStateChanged.DISCONNECTED)
                self._check_completes(completes, (zk.ZOO_ERR_NONODE,))
                if completes[0].err == zk.ZOO_ERR_NONODE:
                    # Might already be recycled
                    recycle_key = None
                    break
                if completes[0].stat.ctime < time_limit:
                    is_empty = (completes[0].stat.dataLength <= 0)
                    begin = middle + 1
                else:
                    end = middle
            if not recycle_key:
                self.apiroutine.retvalue = False
                return
            if not is_notifier and not is_empty and begin >= 0:
                # Leave an extra node
                begin -= 1
            operations = [zk.delete(recycle_key + b'/' + name)
                            for name in children[:begin]]
            if begin == len(children) and not other_children and can_recycle_parent:
                # Try to remove the whole key. We check the version, so that
                # if recycling happens before updateallwithtime() precreating the key,
                # the precreating procedure will create it again;
                # if recycling happens after updateallwithtime() precreating, the
                # version should already be changed so the delete fails
                operations.append(zk.delete(recycle_key, recycle_parent_version))
            if operations:
                while operations:
                    for m in client.requests(operations, self.apiroutine, 60):
                        yield m
                    completes, losts, retries, _ = self.apiroutine.retvalue
                    operations = losts + retries
                    # It is not necessary to check the return value; any result is acceptable.
                self.apiroutine.retvalue = True
            else:
                self.apiroutine.retvalue = False
        recycle_all_freq = 10
        recycle_all_counter = 8
        while True:
            if not _recycle_list:
                if len(self._recycle_list[vhost]) < 800:
                    self._logger.info('Recycling pended to wait for keys expiration, recycle_all_counter = %d, vhost = %r', recycle_all_counter, vhost)
                    _recycle_list.update(self._recycle_list[vhost])
                    self._recycle_list[vhost].clear()
                    for m in self.apiroutine.waitWithTimeout(180 - len(self._recycle_list[vhost]) / 5.0):
                        yield m
                    recycle_all_counter += 1
                else:
                    _recycle_list.update(self._recycle_list[vhost])
                    self._recycle_list[vhost].clear()
                self._logger.info('Recycling %d keys, vhost = %r', len(_recycle_list), vhost)
                if recycle_all_counter >= recycle_all_freq:
                    recycle_all_counter = 0
                    self._logger.info('Starting full recycling, vhost = %r', vhost)
                    # Do a full recycling to all keys
                    # We want to keep the unrecycled nodes under 10%
                    # which means when we random select 10N keys,
                    # the expectation of recycled keys are less than N
                    try:
                        for m in client.requests([zk.getchildren(b'/vlcp/kvdb'),
                                                  zk.getchildren(b'/vlcp/notifier/bykey')],
                                                 self.apiroutine, 60):
                            yield m
                        completes, losts, retries, _ = self.apiroutine.retvalue
                        if losts or retries:
                            raise ZooKeeperSessionUnavailable(ZooKeeperSessionStateChanged.DISCONNECTED)
                        all_path = [b'/vlcp/kvdb/' + p for p in completes[0].children] + \
                                    [b'/vlcp/notifier/bykey/' + p for p in completes[1].children] + \
                                    [b'/vlcp/notifier/all']
                        if len(all_path) > 2000:
                            all_path = sample(all_path, 2000)
                        else:
                            shuffle(all_path)
                        self._logger.info('Full recycling started on %d keys, vhost = %r', len(all_path), vhost)
                        _total_try = 0
                        _total_succ = 0
                        for i in range(0, len(all_path), 50):
                            step_succ = 0
                            for p in all_path[i:i+50]:
                                for m in _recycle_key(p):
                                    yield m
                                _total_try += 1
                                if self.apiroutine.retvalue:
                                    step_succ += 1
                                    _total_succ += 1
                            if step_succ < 3:
                                break
                        self._logger.info('Stop full recycling, total tried %d keys, recycled %d keys, vhost = %r', _total_try, _total_succ, vhost)
                        last_req = recycle_all_freq
                        if len(all_path) <= 100:
                            recycle_all_freq = 40
                        else:
                            if _total_try < 20:
                                recycle_all_freq = 40
                            else:
                                estimate_p = float(_total_succ) / float(_total_try)
                                if estimate_p < 0.01:
                                    estimate_p = 0.01
                                recycle_all_freq = int(recycle_all_freq * 0.1 / estimate_p)
                                if recycle_all_freq <= 1:
                                    recycle_all_freq = 1
                                elif recycle_all_freq >= 50:
                                    recycle_all_freq = 50
                        self._logger.info('Next full recycling starts in %d turns, vhost = %r', recycle_all_freq, vhost)
                        if last_req * 2 <= recycle_all_freq:
                            # Redistribute the recycling time
                            recycle_all_counter = randrange(0, recycle_all_freq)
                            self._logger.info('Redistribute recycling counter to %d', recycle_all_counter)
                    except ZooKeeperSessionUnavailable:
                        self._logger.info('Full recycling cancelled because we are disconnected from ZooKeeper server')
                        for m in self.apiroutine.waitWithTimeout(randrange(0, 60)):
                            yield m
                    except Exception:
                        self._logger.warning('Full recycle exception occurs, vhost = %r', vhost,
                                             exc_info = True)
                        for m in self.apiroutine.waitWithTimeout(randrange(0, 60)):
                            yield m
                continue
            recycle_key = _recycle_list.pop()
            try:
                for m in _recycle_key(recycle_key):
                    yield m
            except ZooKeeperSessionUnavailable:
                self._recycle_list[vhost].add(recycle_key)
                for m in self.apiroutine.waitWithTimeout(randrange(0, 60)):
                    yield m
            except Exception:
                self._logger.warning('recycle routine exception occurs, vhost = %r, current_key = %r', vhost, recycle_key,
                                     exc_info = True)
                for m in self.apiroutine.waitWithTimeout(randrange(0, 60)):
                    yield m
    def recycle(self, keys, vhost = ''):
        '''
        Recycle extra versions from the specified keys.
        '''
        self._recycle_list[vhost].update(_tobytes(k) for k in keys)
    def createnotifier(self, vhost = None):
        "Create a new notifier object"
        if vhost is None:
            vhost = self.notifierbind
        n = _Notifier(vhost, self.scheduler, self.singlecastlimit, not self.deflate and self.notifierdeflate)
        n.start()
        return n
    def listallkeys(self, vhost = ''):
        '''
        Return all keys in the KVDB. For management purpose.
        '''
        client = self._zookeeper_clients.get(vhost)
        if client is None:
            raise ValueError('vhost ' + repr(vhost) + ' is not defined')
        for m in client.requests([zk.getchildren(b'/vlcp/kvdb')],
                                 self.apiroutine, 60):
            yield m
        completes, lost, retries, _ = self.apiroutine.retvalue
        if lost or retries:
            raise ZooKeeperSessionUnavailable(ZooKeeperSessionStateChanged.DISCONNECTED)
        self._check_completes(completes)
        self.apiroutine.retvalue = [_str(k) for k in completes[0].children]

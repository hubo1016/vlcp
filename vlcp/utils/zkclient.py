'''
Created on 2016/9/22

:author: hubo
'''

from vlcp.config import Configurable, config
from vlcp.protocol.zookeeper import ZooKeeper, ZooKeeperRetryException,\
    ZooKeeperConnectionStateEvent, ZooKeeperResponseEvent, ZooKeeperWatcherEvent,\
    ZooKeeperSessionExpiredException
from random import shuffle, random, randrange
from vlcp.event.connection import Client
from vlcp.event.event import Event, withIndices
import logging
import vlcp.utils.zookeeper as zk
try:
    from itertools import izip_longest
except Exception:
    izip_longest = zip
from time import time
from vlcp.event.future import RoutineFuture
from contextlib import closing

@withIndices('state', 'client', 'sessionid')
class ZooKeeperSessionStateChanged(Event):
    CREATED = 'created'
    DISCONNECTED = 'disconnected'
    RECONNECTED = 'reconnected'
    AUTHFAILED = 'authfailed'
    EXPIRED = 'expired'


@withIndices('client', 'sessionid', 'restore')
class ZooKeeperRestoreWatches(Event):
    pass


class ZooKeeperSessionUnavailable(Exception):
    def __init__(self, state):
        Exception.__init__(self, "ZooKeeper state is '%r'" % (state,))
        self.state = state


class ZooKeeperIllegalPathException(ValueError):
    pass

_MAX_SETWATCHES_SIZE = 128 * 1024

_should_add_watch = set(((zk.CREATED_EVENT_DEF, zk.ZOO_ERR_NONODE),))

@config('zookeeperclient')
class ZooKeeperClient(Configurable):
    """
    ZooKeeper client to send requests to a cluster
    """
    # Default ZooKeeper server list, should be a list contains connection URLs
    _default_serverlist = []
    # Chroot to a child node instead of the root node. All paths used in the program
    # will be mapped to *chroot_path*/*path*
    _default_chroot = '/'
    # Extra authentications, should be list of tuples [(scheme1, auth1), (scheme2, auth2), ...]
    _default_auth = []
    # Zookeeper session timeout
    _default_sessiontimeout = 20
    # If not None, ZooKeeperClient will disconnect from the server and reconnect to a random server
    # to make sure the connections to ZooKeeper servers are balanced. It sometimes causes problems,
    # so it is disabled by default.
    _default_rebalancetime = None
    _logger = logging.getLogger(__name__ + '.ZooKeeperClient')
    def __init__(self, container, serverlist = None, chroot = None, protocol = None, readonly = False,
                 restart_session = True):
        if serverlist is not None:
            self.serverlist = list(serverlist)
        else:
            self.serverlist = list(self.serverlist)
        shuffle(self.serverlist)
        self.nextptr = 0
        self.current_connection = None
        if protocol is None:
            self.protocol = ZooKeeper()
        else:
            self.protocol = protocol
        self.protocol.persist = False
        if chroot:
            self.chroot = chroot
        else:
            self.chroot = self.chroot
        if not isinstance(self.chroot, bytes):
            self.chroot = self.chroot.encode('utf-8')
        if self.chroot is None or self.chroot == b'/':
            self.chroot = b''
        self.chroot = self.chroot.rstrip(b'/')
        self._container = container
        self.readonly = readonly
        self.auth_set = set(self.auth)
        self.restart_session = restart_session
        self.session_id = 0
        self.session_state = ZooKeeperSessionStateChanged.EXPIRED
        self._shutdown = False
        self.key = None
        self.certificate = None
        self.ca_certs = None
        self._last_zxid = 0
    def start(self, asyncstart = False):
        self._connmanage_routine = self._container.subroutine(self._connection_manage(), asyncstart)
    def reset(self):
        '''
        Discard current session and start a new one
        '''
        self._connmanage_routine.close()
        self._shutdown = False
        self.start()
    def shutdown(self):
        self._connmanage_routine.close()
        if False:
            yield
    def _connection_manage(self):
        try:
            failed = 0
            self._last_zxid = last_zxid = 0
            session_id = 0
            passwd = b'\x00' * 16
            last_conn_time = None
            while True:
                self.currentserver = self.serverlist[self.nextptr]
                np = self.nextptr + 1
                if np >= len(self.serverlist):
                    np = 0
                self.nextptr = np
                conn = Client(self.currentserver, self.protocol, self._container.scheduler,
                              self.key, self.certificate, self.ca_certs)
                self.current_connection = conn
                conn_up = ZooKeeperConnectionStateEvent.createMatcher(ZooKeeperConnectionStateEvent.UP,
                                                                      conn)
                conn_nc = ZooKeeperConnectionStateEvent.createMatcher(ZooKeeperConnectionStateEvent.NOTCONNECTED,
                                                                      conn)
                conn.start()
                try:
                    yield (conn_up, conn_nc)
                    if self._container.matcher is conn_nc:
                        self._logger.warning('Connect to %r failed, try next server', self.currentserver)
                        if failed > 5:
                            # Wait for a small amount of time to prevent a busy loop
                            # Socket may be rejected, it may fail very quick
                            for m in self._container.waitWithTimeout(min((failed - 5) * 0.1, 1.0)):
                                yield m
                        failed += 1
                        continue
                    try:
                        # Handshake
                        set_watches = []
                        if self.session_state == ZooKeeperSessionStateChanged.DISCONNECTED:
                            for m in self._container.waitForSend(ZooKeeperRestoreWatches(self,
                                                                                         self.session_id,
                                                                                         True,
                                                                                         restore_watches = (set(), set(), set()))):
                                yield m
                            yield (ZooKeeperRestoreWatches.createMatcher(self),)
                            data_watches, exists_watches, child_watches = \
                                    self._container.event.restore_watches
                            if data_watches or exists_watches or child_watches:
                                current_set_watches = zk.SetWatches(relativeZxid = last_zxid)
                                current_length = 0
                                for d, e, c in izip_longest(data_watches, exists_watches, child_watches):
                                    if d is not None:
                                        current_set_watches.dataWatches.append(d)
                                        current_length += 4 + len(d)
                                    if e is not None:
                                        current_set_watches.existWatches.append(e)
                                        current_length += 4 + len(e)
                                    if c is not None:
                                        current_set_watches.childWatches.append(c)
                                        current_length += 4 + len(c)
                                    if current_length > _MAX_SETWATCHES_SIZE:
                                        # Split set_watches
                                        set_watches.append(current_set_watches)
                                        current_set_watches = zk.SetWatches(relativeZxid = last_zxid)
                                if current_set_watches.dataWatches or current_set_watches.existWatches \
                                        or current_set_watches.childWatches:
                                    set_watches.append(current_set_watches)
                        auth_list = list(self.auth_set)
                        with closing(self._container.executeWithTimeout(10,
                                    self.protocol.handshake(conn,
                                        zk.ConnectRequest(lastZxidSeen = last_zxid,
                                                          timeOut = int(self.sessiontimeout * 1000.0),
                                                          sessionId = session_id,
                                                          passwd = passwd,
                                                          readOnly = self.readonly),
                                         self._container,
                                        [zk.AuthPacket(scheme = a[0], auth = a[1]) for a in auth_list] +
                                        set_watches))) as g:
                            for m in g:
                                yield m
                        if self._container.timeout:
                            raise IOError
                    except ZooKeeperSessionExpiredException:
                        self._logger.warning('Session expired.')
                        # Session expired
                        self.session_state = ZooKeeperSessionStateChanged.EXPIRED
                        for m in self._container.waitForSend(ZooKeeperSessionStateChanged(
                                    ZooKeeperSessionStateChanged.EXPIRED,
                                    self,
                                    session_id)):
                            yield m
                        if self.restart_session:
                            failed = 0
                            last_zxid = 0
                            session_id = 0
                            passwd = b'\x00' * 16
                            last_conn_time = None
                            continue
                        else:
                            break                        
                    except Exception:
                        self._logger.warning('Handshake failed to %r, try next server', self.currentserver)
                        if failed > 5:
                            # There is a bug ZOOKEEPER-1159 that ZooKeeper server does not respond
                            # for session expiration, but directly close the connection.
                            # This is a workaround: we store the time that we disconnected from the server,
                            # if we have exceeded the session expiration time, we declare the session is expired
                            if last_conn_time is not None and last_conn_time + self.sessiontimeout * 2 < time():
                                self._logger.warning('Session expired detected from client time.')
                                # Session expired
                                self.session_state = ZooKeeperSessionStateChanged.EXPIRED
                                for m in self._container.waitForSend(ZooKeeperSessionStateChanged(
                                            ZooKeeperSessionStateChanged.EXPIRED,
                                            self,
                                            session_id)):
                                    yield m
                                if self.restart_session:
                                    failed = 0
                                    last_zxid = 0
                                    session_id = 0
                                    passwd = b'\x00' * 16
                                    last_conn_time = None
                                    continue
                                else:
                                    break                        
                            else:
                                # Wait for a small amount of time to prevent a busy loop
                                for m in self._container.waitWithTimeout(min((failed - 5) * 0.1, 1.0)):
                                    yield m
                        failed += 1
                    else:
                        failed = 0
                        conn_resp, auth_resp = self._container.retvalue
                        if conn_resp.timeOut <= 0:
                            # Session expired
                            # Currently should not happen because handshake() should raise an exception
                            self._logger.warning('Session expired detected from handshake packet')
                            self.session_state = ZooKeeperSessionStateChanged.EXPIRED
                            for m in self._container.waitForSend(ZooKeeperSessionStateChanged(
                                        ZooKeeperSessionStateChanged.EXPIRED,
                                        self,
                                        session_id)):
                                yield m
                            if self.restart_session:
                                failed = 0
                                last_zxid = 0
                                last_conn_time = None
                                session_id = 0
                                passwd = b'\x00' * 16
                                continue
                            else:
                                break
                        else:
                            session_id = conn_resp.sessionId
                            passwd = conn_resp.passwd
                            # Authentication result check
                            auth_failed = any(a.err == zk.ZOO_ERR_AUTHFAILED for a in auth_resp)
                            if auth_failed:
                                self._logger.warning('ZooKeeper authentication failed for following auth: %r',
                                                     [a for a,r in zip(auth_list, auth_resp) if r.err == zk.ZOO_ERR_AUTHFAILED])
                                self.session_state = ZooKeeperSessionStateChanged.AUTHFAILED
                                for m in self._container.waitForSend(ZooKeeperSessionStateChanged(
                                                ZooKeeperSessionStateChanged.AUTHFAILED,
                                                self,
                                                session_id
                                            )):
                                    yield m
                                # Not retrying
                                break
                            else:
                                self.session_readonly = getattr(conn_resp, 'readOnly', False)
                                self.session_id = session_id
                                if self.session_state == ZooKeeperSessionStateChanged.EXPIRED:
                                    for m in self._container.waitForSend(ZooKeeperSessionStateChanged(
                                                ZooKeeperSessionStateChanged.CREATED,
                                                self,
                                                session_id
                                            )):
                                        yield m
                                else:
                                    for m in self._container.waitForSend(ZooKeeperSessionStateChanged(
                                                ZooKeeperSessionStateChanged.RECONNECTED,
                                                self,
                                                session_id
                                            )):
                                        yield m
                                self.session_state = ZooKeeperSessionStateChanged.CREATED
                        if conn.connected:
                            conn_down = ZooKeeperConnectionStateEvent.createMatcher(ZooKeeperConnectionStateEvent.DOWN,
                                                                                    conn,
                                                                                    conn.connmark
                                                                                    )
                            auth_failed = ZooKeeperResponseEvent.createMatcher(zk.AUTH_XID,
                                                                               conn,
                                                                               conn.connmark,
                                                                               _ismatch = lambda x: x.message.err == ZOO_ERR_AUTHFAILED)
                            while True:
                                rebalancetime = self.rebalancetime
                                if rebalancetime is not None:
                                    rebalancetime += random() * 60
                                for m in self._container.waitWithTimeout(rebalancetime, conn_down, auth_failed):
                                    yield m
                                if self._container.timeout:
                                    # Rebalance
                                    if conn.zookeeper_requests:
                                        # There are still requests not processed, wait longer
                                        for _ in range(0, 3):
                                            longer_time = random() * 10
                                            for m in self._container.waitWithTimeout(longer_time, conn_down, auth_failed):
                                                yield m
                                            if not self._container.timeout:
                                                # Connection is down, or auth failed
                                                break
                                            if not conn.zookeeper_requests:
                                                break
                                        else:
                                            # There is still requests, skip for this time
                                            continue
                                    # Rebalance to a random server
                                    if self._container.timeout:
                                        self.nextptr = randrange(len(self.serverlist))
                                break
                            if self._container.matcher is auth_failed:
                                self._logger.warning('ZooKeeper authentication failed, shutdown the connection')
                                self.session_state = ZooKeeperSessionStateChanged.AUTHFAILED
                                for m in self._container.waitForSend(ZooKeeperSessionStateChanged(
                                                ZooKeeperSessionStateChanged.AUTHFAILED,
                                                self,
                                                session_id
                                            )):
                                    yield m
                                # Not retrying
                                break
                            else:
                                # Connection is down, try other servers
                                if not self._container.timeout:
                                    self._logger.warning('Connection lost to %r, try next server', self.currentserver)
                                else:
                                    self._logger.info('Rebalance to next server')
                                self._last_zxid = last_zxid = conn.zookeeper_lastzxid
                                last_conn_time = time()
                                self.session_state = ZooKeeperSessionStateChanged.DISCONNECTED
                                for m in self._container.waitForSend(ZooKeeperSessionStateChanged(
                                                ZooKeeperSessionStateChanged.DISCONNECTED,
                                                self,
                                                session_id
                                            )):
                                    yield m                                    
                finally:
                    conn.subroutine(conn.shutdown(True), False)
                    self.current_connection = None
        finally:
            self._shutdown = True
            if self.session_state != ZooKeeperSessionStateChanged.EXPIRED and self.session_state != ZooKeeperSessionStateChanged.AUTHFAILED:
                self.session_state = ZooKeeperSessionStateChanged.EXPIRED
                self._container.scheduler.emergesend(ZooKeeperSessionStateChanged(
                                                ZooKeeperSessionStateChanged.EXPIRED,
                                                self,
                                                session_id
                                            ))
    def chroot_path(self, path):
        return self.chroot + path
    def unchroot_path(self, path):
        return path[len(self.chroot):]
    def _analyze(self, request):
        if request.type in (zk.ZOO_EXISTS_OP, zk.ZOO_GETDATA_OP, zk.ZOO_GETACL_OP, zk.ZOO_GETCHILDREN_OP,
                            zk.ZOO_SYNC_OP, zk.ZOO_PING_OP, zk.ZOO_GETCHILDREN2_OP, zk.ZOO_SETAUTH_OP):
            # These requests can be retried even if they are already sent 
            can_retry = True
        else:
            can_retry = False
        watch_type = None
        if request.type == zk.ZOO_MULTI_OP:
            # chroot sub ops
            for op in request.requests:
                if hasattr(op, 'path'):
                    op.path = self.chroot_path(op.path)
        else:
            if hasattr(request, 'path'):
                request.path = self.chroot_path(request.path)
            if getattr(request, 'watch', False):
                if request.type == zk.ZOO_GETDATA_OP:
                    watch_type = zk.CHANGED_EVENT_DEF
                elif request.type == zk.ZOO_EXISTS_OP:
                    watch_type = zk.CREATED_EVENT_DEF
                elif request.type == zk.ZOO_GETCHILDREN_OP or request.type == zk.ZOO_GETCHILDREN2_OP:
                    watch_type = zk.CHILD_EVENT_DEF
        return (request, can_retry, watch_type)
    def watch_path(self, path, watch_type, container):
        '''
        Watch the specified path as specified type
        '''
        if watch_type == zk.CHANGED_EVENT_DEF:
            watch_matchers = (ZooKeeperWatcherEvent.createMatcher(None, None, self.protocol, zk.CHANGED_EVENT_DEF, None, path),
                              ZooKeeperWatcherEvent.createMatcher(None, None, self.protocol, zk.DELETED_EVENT_DEF, None, path))
        else:
            watch_matchers = (ZooKeeperWatcherEvent.createMatcher(None, None, self.protocol, watch_type, None, path),)
        # If the session expires, raise exception and exit
        session_state = ZooKeeperSessionStateChanged.createMatcher(ZooKeeperSessionStateChanged.EXPIRED,
                                                                   self,
                                                                   self.session_id)
        auth_failed = ZooKeeperSessionStateChanged.createMatcher(ZooKeeperSessionStateChanged.AUTHFAILED,
                                                                   self,
                                                                   self.session_id)
        # If the watchers are restored, restore the matchers
        restore_matcher = ZooKeeperRestoreWatches.createMatcher(self, self.session_id, True)
        while True:
            yield (session_state, auth_failed, restore_matcher) + watch_matchers
            if container.matcher is session_state or container.matcher is auth_failed:
                raise ZooKeeperSessionUnavailable(container.event.state)
            elif container.matcher is restore_matcher:
                ev = container.event
                ev.restore_watches[{zk.CHANGED_EVENT_DEF : 0,
                                   zk.CREATED_EVENT_DEF : 1,
                                   zk.CHILD_EVENT_DEF : 2}[watch_type]].add(path)
            else:
                watcher_event = container.event.message
                if watcher_event.path:
                    watcher_event.path = self.unchroot_path(watcher_event.path)
                container.retvalue = watcher_event
                break
    def requests(self, requests, container, timeout = None, session_lock = None, callback = None, priority = 0):
        '''
        similar to vlcp.protocol.zookeeper.ZooKeeper.requests, but:
           1. Returns an extra item *watchers*, which is a list of objects corresponding to each request.
              if the request has watch=True, the corresponding object is a RoutineFuture object;
              if the request has watch=False or does not support watch, the corresponding object is None.
              Use watcher.wait() to get the watch event. Use watcher.close() to discard the watcher.
          
           2. If the connection is lost during requests, this method waits for reconnecting until timeout, 
              session expires or the response of a request which is not read-only is lost.
        
        :param requests: sequence of request
        
        :param container: container of current routine
        
        :param timeout: if not None, wait only for specified time. Notice that it is not an exact limit,
                it won't stop the execution unless the connection is lost
        
        :param session_lock: if not None, only execute if the session_id == session_lock
        
        :param callback: if not None, callback(request, response) is called immediately after any response is received
        
        :return: (result, lost_responses, retry_requests, watchers) tuple, the first three are the
                same as ZooKeeper.requests, the last item *watchers* is a list of RoutineFuture objects
        '''
        if self._shutdown:
            raise ZooKeeperSessionUnavailable(self.session_state)
        if session_lock is not None and self.session_id != session_lock:
            raise ZooKeeperSessionUnavailable(ZooKeeperSessionStateChanged.EXPIRED)
        start_time = time()
        if timeout is not None:
            end_time = start_time + timeout
        def left_time():
            if timeout is None:
                return None
            else:
                return max(end_time - time(), 0)
        def has_time_left():
            t = left_time()
            return t is None or t > 0
        result = {}
        lost_responses = []
        analysis = dict((v[0], (v[1], v[2])) for v in (self._analyze(r) for r in requests))
        retry_requests = list(requests)
        watchers = {}
        def requests_callback(request, response):
            watch_type = analysis[request][1]
            if watch_type is not None and (response.err == zk.ZOO_ERR_OK or \
                                           (watch_type, response.err) in _should_add_watch):
                watchers[request] = RoutineFuture(self.watch_path(request.path, watch_type, container), container)
            if callback is not None:
                callback(request, response)
        def unchroot_response(resp):
            if resp.zookeeper_request_type == zk.ZOO_MULTI_OP:
                for r in resp.responses:
                    if hasattr(r, 'path'):
                        r.path = self.unchroot_path(r.path)
            elif hasattr(resp, 'path'):
                resp.path = self.unchroot_path(resp.path)
            return resp
        while has_time_left() and not lost_responses and retry_requests:
            if self.session_state != ZooKeeperSessionStateChanged.CREATED:
                def wait_for_connect():
                    state_change = ZooKeeperSessionStateChanged.createMatcher(None, self)
                    while True:
                        yield (state_change,)
                        if container.event.state in (ZooKeeperSessionStateChanged.CREATED, ZooKeeperSessionStateChanged.RECONNECTED):
                            break
                        elif self._shutdown:
                            raise ZooKeeperSessionUnavailable(self.session_state)
                        elif session_lock is not None and (container.event.sessionid != session_lock or \
                                                           container.event.state == ZooKeeperSessionStateChanged.EXPIRED):
                            raise ZooKeeperSessionUnavailable(ZooKeeperSessionStateChanged.EXPIRED)
                try:
                    for m in container.executeWithTimeout(left_time(), wait_for_connect()):
                        yield m
                except ZooKeeperSessionUnavailable:
                    if len(retry_requests) == len(requests):
                        raise
                    else:
                        break
                if container.timeout:
                    if len(retry_requests) == len(requests):
                        raise ZooKeeperSessionUnavailable(ZooKeeperSessionStateChanged.DISCONNECTED)
                    else:
                        break
            # retry all the requests
            for m in self.protocol.requests(self.current_connection, retry_requests, container,
                                            requests_callback, priority=priority):
                yield m
            new_result, new_lost, new_retry = container.retvalue
            # Save the results
            result.update((k,unchroot_response(v)) for k,v in zip(retry_requests, new_result) if v is not None)
            if new_lost:
                # Some responses are lost
                for i in range(len(new_lost) - 1, -1, -1):
                    if not analysis[new_lost[i]][0]:
                        # This request can not be retried
                        break
                else:
                    i = -1
                new_retry = new_lost[i+1:] + new_retry
                new_lost = new_lost[:i+1]
                if new_lost:
                    # Some requests can not be retried, this is as far as we go
                    lost_responses = new_lost
                    retry_requests = new_retry
                    break
            retry_requests = new_retry
        container.retvalue = ([result.get(r, None) for r in requests],
                              lost_responses,
                              retry_requests,
                              [watchers.get(r, None) for r in requests])
    def get_last_zxid(self):
        '''
        Return the latest zxid seen from servers
        '''
        if not self.current_connection:
            return self._last_zxid
        else:
            return getattr(self.current_connection, 'zookeeper_lastzxid', self._last_zxid)

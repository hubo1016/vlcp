'''
Created on 2015/12/29

:author: hubo
'''
from __future__ import print_function
from vlcp.utils.connector import async_processor, async_to_async, Connector,\
    generator_to_async
from vlcp.event.event import withIndices, Event
from vlcp.config import defaultconfig
from vlcp.server.module import Module, api, callAPI
import functools
import threading
import signal
from vlcp.event.runnable import RoutineContainer
from vlcp.event.runnable import RoutineException
import pdb
import code
from vlcp.config.config import manager
from vlcp.protocol.protocol import Protocol
from vlcp.event.connection import Client
import os
import socket
import re
from vlcp.event.core import InterruptedBySignalException
try:
    from Queue import Queue, PriorityQueue
except:
    from queue import Queue, PriorityQueue
import traceback
import sys
try:
    import thread
except:
    import _thread as thread

def console_help():
    print(Console._full_help)

def restore_console():
    if not hasattr(Console, '_instance') or not Console._instance:
        raise ValueError('Console is not loaded')
    Console._instance.restore_console()

@withIndices('type')
class ConsoleEvent(Event):
    canignore = False

@withIndices()
class ConsoleServiceCall(Event):
    pass

@withIndices('waiter')
class ConsoleServiceCancel(Event):
    pass

@withIndices('socket')
class SocketInjectDone(Event):
    pass

@withIndices()
class InterruptPoller(Event):
    pass

class Waiter(object):
    def __init__(self):
        self.event = threading.Event()
        self.event.clear()
        self.exception = None
        self.result = None
    def wait(self, timeout = None):
        self.event.wait(timeout)
        if self.exception:
            raise self.exception
        else:
            return self.result
    def raise_exception(self, exc):
        self.exception = exc
        self.event.set()
    def send_result(self, val):
        self.result = val
        self.event.set()

@defaultconfig
class Console(Module):
    '''
    VLCP debugging console.
    
    Besides the normal functions of Python interactive console,
    Following variables are provided for debugging purpose:
    
       server, manager, container
        
    Following functions can be used to control VLCP running:
    
       callapi, capture, sendevent, subroutine, execute, breakpoint, syscall,
       resume, debug, restore_console, console_help
       
    For details call console_help()
    '''
    _full_help = '''
VLCP debugging console.
Besides the normal functions of python interactive console,
following variables are provided for debugging purpose:
server - current running VLCP server
manager - current configuration manager
container - internal used routine container

Following functions can be used to control VLCP running:

callapi(modulename, functionname, **kwargs)
 - Call module API modulename/functionname with kwargs, return result

capture(matchers, blocking = False, breakpoint = False, captureonce = False, callback = None)
 - Capture events matched with specified matchers and print the event. Other parameters:
   - blocking: if True, wait until the events are captured
   - breakpoint: if True, suspend the event loop and wait for resume()
   - captureonce: if True, remove the matchers on first capture
   - callback: func(event, matcher) called on every capture if specified
   
sendevent(event, emerge = False)
 - Send specified event to scheduler. if merge = True, send immediately without block

subroutine(routine)
 - create a new routine in container.

execute(routine)
 - execute the routine in container, and return container.retvalue
 
breakpoint()
 - stop running and wait for resume().
 
syscall(syscall_func)
 - execute syscall_func in syscall context
 
resume()
 - resume from breakpoint
 
debug()
 - resume from breakpoint with pdb.set_trace() to enter pdb debugging. Suspend the interactive console
   to work with pdb.

restore_console()
 - Prepare to continue in pdb and resume the console. Type in pdb: 
     clear
     import vlcp.service.debugging.console
     vlcp.service.debugging.console.restore_console()
     continue

console_help()
 - show this help
'''
    service = False
    # Directly start VLCP in the console mode. By default, the console module creates a
    # telnet server and wait for a connection. The console can be used in the telnet session.
    # With startinconsole = True, the module uses stdin/stdout to create the console.
    _default_startinconsole = False
    # Default telnet connection URL, this is a passive connection on port 9923, so use::
    # 
    #     telnet localhost 9923
    # 
    # to connect to the console.
    _default_telnetconsole = 'ptcp://localhost:9923/'
    # If SSL is configured (with pssl://...), specify the private key file
    _default_key = None
    # If SSL is configured, specify the certificate file
    _default_certificate = None
    # If SSL is configured, specify the CA file
    _default_ca_certs = None
    def _service_routine(self):
        self.apiroutine.subroutine(self._intercept_main())
        csc = ConsoleServiceCall.createMatcher()
        while True:
            yield (csc,)
            self.apiroutine.subroutine(self.apiroutine.event.routine, True)
    def _service_call_routine(self, waiter, call):
        try:
            for m in self.apiroutine.withException(call, ConsoleServiceCancel.createMatcher(waiter)):
                yield  m
        except RoutineException:
            pass
        except Exception as exc:
            waiter.raise_exception(exc)
        else:
            waiter.send_result(self.apiroutine.retvalue)
    def _intercept_main(self):
        cr = self.apiroutine.currentroutine
        self.sendEventQueue = Queue()
        _console_connect_event = threading.Event()
        _console_connect_event.clear()
        for m in self.apiroutine.waitForSend(ConsoleEvent('initproxy')):
            yield m
        if not self.startinconsole:
            p = Protocol()
            p.persist = True
            p.createqueue = False
            def init(connection):
                sock = connection.socket
                self.telnet_socket = sock
                self.scheduler.unregisterPolling(connection.socket)
                connection.socket = None
                connection.connected = False
                _console_connect_event.set()
                yield (SocketInjectDone.createMatcher(sock),)
            p.init = init
            p.reconnect_init = init
            Client(self.telnetconsole, p, self.scheduler, self.key, self.certificate, self.ca_certs).start()
        def syscall_threaded_main(scheduler, processor):
            # Detach self
            scheduler.unregisterall(cr)
            self._threaded_main_quit = False
            def threaded_main():
                try:
                    scheduler.main(False, False)
                finally:
                    self._threaded_main_quit = True
                    _console_connect_event.set()
            t = threading.Thread(target=threaded_main)
            t.daemon = True
            t.start()
            try:
                if self.startinconsole:
                    self._interactive()
                else:
                    while not self._threaded_main_quit:
                        try:
                            while not _console_connect_event.is_set():
                                # There is a bug in Python 2.x that wait without timeout cannot be
                                # interrupted by signal
                                _console_connect_event.wait(3600)
                            if self._threaded_main_quit:
                                break
                        except InterruptedBySignalException:
                            # This signal should interrupt the poller, but poller is not in the main thread
                            # Send an event through the proxy will do the trick
                            self.sendEventQueue.put((InterruptPoller(),))
                            continue
                        pstdin_r, pstdin_w = os.pipe()
                        pstdout_r, pstdout_w = os.pipe()
                        orig_stdin = sys.stdin
                        orig_stdout = sys.stdout
                        orig_stderr = sys.stderr
                        try:
                            pstdin = os.fdopen(pstdin_r, 'rU', 0)
                            pstdout = os.fdopen(pstdout_w, 'w', 0)
                            sys.stdin = pstdin
                            sys.stdout = pstdout
                            sys.stderr = pstdout
                            sock = self.telnet_socket
                            sock.setblocking(True)
                            self.telnet_socket = None
                            _console_connect_event.clear()
                            t = threading.Thread(target=self._telnet_server, args=(pstdin_w, pstdout_r, sock, orig_stdout))
                            t.daemon = True
                            t.start()
                            try:
                                self._interactive()
                            except SystemExit:
                                pass
                            if not t.is_alive():
                                break
                            self.sendEventQueue.put((SocketInjectDone(sock),))
                        finally:
                            try:
                                sock.shutdown(socket.SHUT_RDWR)
                            except:
                                pass
                            try:
                                pstdin.close()
                            except:
                                pass
                            try:
                                pstdout.close()
                            except:
                                pass
                            sys.stdin = orig_stdin
                            sys.stdout = orig_stdout
                            sys.stderr = orig_stderr
            except SystemExit:
                pass
            finally:
                self.sendEventQueue.put(None)
                scheduler.quit()
                if self.startinconsole:
                    print('Wait for scheduler end, this may take some time...')
                t.join()
        for m in self.apiroutine.syscall(syscall_threaded_main, True):
            yield m
    def _telnet_server_writer(self, queue, sock):
        lastseq = -1
        while True:
            t, seq, val = queue.get()
            if t < 0:
                break
            if t != 2 or seq >= lastseq:
                try:
                    sock.sendall(val)
                except:
                    break
            if t == 0:
                lastseq = seq
    def _telnet_server_writer2(self, pstdout_r, queue, lock, orig_stdout):
        while True:
            data = os.read(pstdout_r, 1024)
            if data == '':
                os.close(pstdout_r)
                break
            data, _ = re.subn(br'\r?\n', b'\r\n', data)
            lock.acquire()
            try:
                self._telnet_seq += 1
                seq = self._telnet_seq
            finally:
                lock.release()
            queue.put((2, seq, data))
    def _telnet_server(self, pstdin_w, pstdout_r, sock, orig_stdout):
        queue = PriorityQueue()
        inputbuffer = b''
        self._telnet_seq = 0
        try:
            t = threading.Thread(target=self._telnet_server_writer, args=(queue, sock))
            t.daemon = True
            t.start()
            lock = threading.Lock()
            def writeall(data):
                start = 0
                while start < len(data):
                    size = os.write(pstdin_w, data[start:])
                    start += size
            def sendcontrol(t, data):
                lock.acquire()
                try:
                    self._telnet_seq += 1
                    seq = self._telnet_seq
                finally:
                    lock.release()
                queue.put((t, seq, data))
            t2 = threading.Thread(target=self._telnet_server_writer2, args=(pstdout_r, queue, lock, orig_stdout))
            t2.daemon = True
            t2.start()
            escaping = False
            option = None
            while True:
                newdata = sock.recv(1024)
                if newdata == b'':
                    break
                for i in range(0, len(newdata)):
                    c = newdata[i:i+1]
                    if escaping:
                        if option == b'\xfd' and c == b'\x06':
                            sendcontrol(1, b'\xff\xfb\x06')
                            option = None
                            escaping = False
                        elif option == b'\xfd' or option == b'\xfe':
                            sendcontrol(1, b'\xff\xfc' + c)
                            option = None
                            escaping = False
                        elif option == b'\xfb' or option == b'\xfc':
                            sendcontrol(1, b'\xff\xfe' + c)
                            option = None
                            escaping = False
                        elif c in (b'\xfb', b'\xfc', b'\xfd', b'\xfe'):
                            option = c
                        else:
                            option = None
                            if c == b'\xf3' or c == b'\xf4':
                                thread.interrupt_main()
                            escaping = False
                    else:
                        if c == b'\x03':
                            thread.interrupt_main()
                        elif c == b'\x08':
                            inputbuffer = inputbuffer[:-1]
                        elif c == b'\x00':
                            inputbuffer += b'\n'
                            writeall(inputbuffer)
                            inputbuffer = b''
                        elif c == b'\r' or c == b'\n':
                            inputbuffer += c
                            writeall(inputbuffer)
                            inputbuffer = b''
                        elif c == b'\xff':
                            escaping = True
                        else:
                            inputbuffer += c
        except OSError:
            pass
        except IOError:
            pass
        finally:
            try:
                os.close(pstdin_w)
            except:
                pass
            queue.put((-1, -1, -1))
    def _interactive(self):
        lsignal = signal.signal(signal.SIGINT, signal.default_int_handler)
        try:
            _breakpoint_event = threading.Event()
            _current_thread = threading.current_thread().ident
            _enter_pdb = [False]
            def _async_run(call):
                self.sendEventQueue.put((ConsoleServiceCall(routine = call),))
            def _async(func):
                @functools.wraps(func)
                def f(*args, **kwargs):
                    _async_run(func(*args, **kwargs))
                return f
            def _service_call_customized(factory):
                waiter = Waiter()
                self.sendEventQueue.put((ConsoleServiceCall(routine=factory(waiter)),))
                try:
                    return waiter.wait()
                except:
                    self.sendEventQueue.put((ConsoleServiceCancel(waiter),))
                    raise
            def execute(call):
                return _service_call_customized(lambda waiter: self._service_call_routine(waiter, call))
            def _service(func):
                @functools.wraps(func)
                def f(*args, **kwargs):
                    return execute(func(*args, **kwargs))
                return f
            @_service
            def callapi(modulename, functionname, **kwargs):
                return callAPI(self.apiroutine, modulename, functionname, kwargs)
            @_service
            def sendevent(event, emerge = False):
                if emerge:
                    self.apiroutine.scheduler.emergesend(event)
                else:
                    for m in self.apiroutine.waitForSend(event):
                        yield m
                self.apiroutine.retvalue = None
            @_service
            def subroutine(routine):
                self.apiroutine.retvalue = self.apiroutine.subroutine(routine)
                if False:
                    yield
            @_service
            def syscall(syscall_func):
                for m in self.apiroutine.syscall(syscall_func):
                    yield m
            def breakpoint():
                in_thread = threading.current_thread().ident
                if in_thread == _current_thread:
                    _breakpoint()
                else:
                    print('Enter VLCP debugging breakpoint:')
                    traceback.print_stack()
                    print('Call resume() to continue the event loop, or debug() to enter pdb')
                    _breakpoint_event.clear()
                    _breakpoint_event.wait()
                    if _enter_pdb[0]:
                        pdb.set_trace()
                    else:
                        print('Resume from breakpoint.')
            @_async
            def _breakpoint():
                breakpoint()
                if False:
                    yield
            def resume():
                _enter_pdb[0] = False
                _breakpoint_event.set()
            @_async
            def restore_console():
                self._restore_console_event.set()
                if False:
                    yield
            self.restore_console = restore_console
            def debug():
                _enter_pdb[0] = True
                self._restore_console_event.clear()
                _breakpoint_event.set()
                # Switch to event loop thread, suspend the main thread, wait for restore_console
                self._restore_console_event.wait()
            _capture_breakpoint = breakpoint
            def capture(matchers, blocking = False, breakpoint = False, captureonce = False, callback = None):
                def _capture_service(waiter):
                    if blocking:
                        csm = ConsoleServiceCancel.createMatcher(waiter)
                    else:
                        waiter.send_result(self.apiroutine.currentroutine)
                    firsttime = True
                    while firsttime or not captureonce:
                        if blocking:
                            yield tuple(matchers) + (csm,)
                        else:
                            yield matchers
                        if blocking and self.apiroutine.matcher is csm:
                            # Cancelled
                            return
                        print('Event Captured: Capture %r with %r' % (self.apiroutine.event, self.apiroutine.matcher))
                        if firsttime and blocking:
                            waiter.send_result((self.apiroutine.event, self.apiroutine.matcher, self.apiroutine.currentroutine))
                        firsttime = False
                        if callback:
                            try:
                                callback(self.apiroutine.event, self.apiroutine.matcher)
                            except:
                                print('Exception while running callback:')
                                traceback.print_exc()
                        if breakpoint:
                            _capture_breakpoint()
                return _service_call_customized(_capture_service)
            code.interact(self.__doc__ + '\n' + 'Python ' + str(sys.version) + ' on ' + str(sys.platform),
                          None,
                          {'server':self.server,'manager':manager, 'container':self.apiroutine,
                                     'callapi':callapi, 'capture':capture, 'sendevent':sendevent,
                                     'subroutine':subroutine, 'breakpoint':breakpoint, 'syscall':syscall,
                                     'resume':resume, 'debug':debug, 'restore_console':restore_console,
                                     'console_help':console_help,'execute':execute})
        finally:
            signal.signal(signal.SIGINT, lsignal)
    def __init__(self, server):
        '''
        Constructor
        '''
        Module.__init__(self, server)
        self._ce_matcher = ConsoleEvent.createMatcher()
        self.apiroutine = RoutineContainer(self.scheduler)
        self.apiroutine.main = self._service_routine
        self._restore_console_event = threading.Event()
        @generator_to_async(True, False)
        def proxy(event, matcher):
            while True:
                events = self.sendEventQueue.get()
                if events is None:
                    break
                yield events
        @async_to_async(True, False)
        @async_processor
        def processor(event, matcher, queueout):
            if event.type == 'initproxy':
                proxy(event, matcher, queueout)
        self.connector = Connector(processor, (self._ce_matcher,), self.scheduler, False)
        self.routines.append(self.apiroutine)
        self.routines.append(self.connector)

if __name__ == '__main__':
    from vlcp.server import main
    import sys
    manager['module.console.startinconsole'] = True
    modules = list(sys.argv[1:]) + ['__main__.Console']
    main(None, modules)

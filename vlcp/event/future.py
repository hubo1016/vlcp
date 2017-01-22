'''
Created on 2016/9/28

:author: hubo

Future is a helper class to simplify the process of retrieving a result from other routines.
The implementation is straight-forward: first check the return value, if not set, wait for
the event. Multiple routines can wait for the same Future object.

The interface is similar to asyncio, but:

- Cancel is not supported - you should terminate the sender routine instead. But RoutineFuture supports
  close() (and cancel() which is the same)

- Callback is not supported - start a subroutine to wait for the result instead.

- result() returns None if the result is not ready; exception() is not supported.

- New wait() generator function: get the result, or wait for the result until available. It is
  always the recommended way to use a future; result() is not recommended.

- ensure_result() which returns a context manager: this should be used in the sender routine,
  to ensure that a result is always set after exit the with scope. If the result is not set,
  it is set to None; if an exception is raised, it is set with set_exception.

'''
from vlcp.event.event import withIndices, Event
from contextlib import contextmanager

@withIndices('futureobj')
class FutureEvent(Event):
    pass


class Future(object):
    """
    Basic future object
    """
    def __init__(self, scheduler):
        self._scheduler = scheduler
    def done(self):
        '''
        :return: True if the result is available; False otherwise.
        '''
        return hasattr(self, '_result')
    def result(self):
        '''
        :return: None if the result is not ready, the result from set_result, or raise the exception
                 from set_exception. If the result can be None, it is not possible to tell if the result is
                 available; use done() to determine that.
        '''
        try:
            r = getattr(self, '_result')
        except AttributeError:
            return None
        else:
            if hasattr(self, '_exception'):
                raise self._exception
            else:
                return r
    def wait(self, container):
        '''
        :param container: container of current routine
        
        :return: The result, or raise the exception from set_exception. The result is returned to container.retvalue.
        '''
        if hasattr(self, '_result'):
            if hasattr(self, '_exception'):
                raise self._exception
            else:
                container.retvalue = self._result
        else:
            yield (FutureEvent.createMatcher(self),)
            if hasattr(container.event, 'exception'):
                raise container.event.exception
            else:
                container.retvalue = container.event.result
    def set_result(self, result):
        '''
        Set the result to Future object, wake up all the waiters
        
        :param result: result to set
        '''
        if hasattr(self, '_result'):
            raise ValueError('Cannot set the result twice')
        self._result = result
        self._scheduler.emergesend(FutureEvent(self, result = result))
    def set_exception(self, exception):
        '''
        Set an exception to Future object, wake up all the waiters
        
        :param exception: exception to set
        '''
        if hasattr(self, '_result'):
            raise ValueError('Cannot set the result twice')
        self._result = None
        self._exception = exception
        self._scheduler.emergesend(FutureEvent(self, exception = exception))
    @contextmanager
    def ensure_result(self, supress_exception = False, defaultresult = None):
        '''
        Context manager to ensure returning the result
        '''
        try:
            yield self
        except Exception as exc:
            self.set_exception(exc)
            if not supress_exception:
                raise
        else:
            if not self.done():
                self.set_result(defaultresult)


class FutureCancelledException(Exception):
    pass


class RoutineFuture(Future):
    '''
    Quick wrapper to create a subroutine and return the result to a Future object
    '''
    def __init__(self, subprocess, container):
        '''
        Start the subprocess
        
        :param subprocess: a generator process, which returns the result to container.retvalue on exit
        
        :param container: the routine container to run the subprocess with
        '''
        Future.__init__(self, container.scheduler)
        def _subroutine():
            with self.ensure_result(True):
                try:
                    for m in subprocess:
                        yield m
                except GeneratorExit:
                    raise FutureCancelledException('close is called before result returns')
                else:
                    self.set_result(container.retvalue)
        self._routine = container.subroutine(_subroutine())
    def close(self):
        '''
        Terminate the subprocess
        '''
        if not self.done():
            self._routine.close()
    def cancel(self):
        '''
        Same as close()
        '''
        self.close()

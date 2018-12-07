'''
Created on 2016/9/28

:author: hubo

Future is a helper class to simplify the process of retrieving a result from other routines.
The implementation is straight-forward: first check the return value, if not set, wait for
the event. Multiple routines can wait for the same Future object.

The interface is similar to asyncio, but:

- Cancel is not supported - you should terminate the sender routine instead. But `RoutineFuture` supports
  `close()` (and `cancel()` which is the same)

- Callback is not supported - start a subroutine to wait for the result instead.

- `result()` returns None if the result is not ready; `exception()` is not supported - use full_result() instead

- New `wait()` async function: get the result, or wait for the result until available. It is
  always the recommended way to use a future; `result()` is not recommended.
  
  `wait()` will NOT cancel the `Future` (or `RoutineFuture`) when the waiting coroutine is
  closed. This is different from `asyncio.Future`. To ensure that the future closes after awaited,
  use `wait_and_close()` of `RoutineFuture`.

- `ensure_result()` returns a context manager: this should be used in the sender routine,
  to ensure that a result is always set after exit the with scope. If the result is not set,
  it is set to None; if an exception is raised, it is set with set_exception.

Since v2.0, you can directly use `await future` to wait for the result

'''
from vlcp.event.event import withIndices, Event, M_
from contextlib import contextmanager
from vlcp.event.runnable import GeneratorExit_, RoutineContainer,\
    RoutineException

@withIndices('futureobj')
class FutureEvent(Event):
    pass


class FutureCancelledException(Exception):
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

    def full_result(self):
        """
        Return (is_done, result, exception) tuple without raising the exception.
        """
        if hasattr(self, '_result'):
            return (True, self._result, None)
        elif hasattr(self, '_exception'):
            return (True, None, self._exception)
        else:
            return (False, None, None)

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
    async def wait(self, container = None):
        '''
        :param container: DEPRECATED container of current routine
        
        :return: The result, or raise the exception from set_exception.
        '''
        if hasattr(self, '_result'):
            if hasattr(self, '_exception'):
                raise self._exception
            else:
                return self._result
        else:
            ev = await FutureEvent.createMatcher(self)
            if hasattr(ev, 'exception'):
                raise ev.exception
            else:
                return ev.result

    def get_matcher(self):
        """
        Return None if already done, or return a matcher to wait.
        This helps waiting for multiple futures and/or with other event matchers.
        
        :return: None if already done, and a matcher if not 
        """
        if hasattr(self, '_result'):
            return None
        else:
            return FutureEvent.createMatcher(self)

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
            if not self.done():
                self.set_exception(exc)
            if not supress_exception:
                raise
        except:
            if not self.done():
                self.set_exception(FutureCancelledException('cancelled'))
            raise
        else:
            if not self.done():
                self.set_result(defaultresult)
    
    def __await__(self):
        return self.wait().__await__()
    
    def close(self):
        return
    
    def cancel(self):
        return


class RoutineFuture(Future):
    '''
    Quick wrapper to create a subroutine and return the result to a Future object
    '''
    def __init__(self, subprocess, container):
        '''
        Start the subprocess
        
        :param subprocess: a generator process, which returns the result to future on exit
        
        :param container: the routine container to run the subprocess with
        '''
        Future.__init__(self, container.scheduler)
        async def _subroutine():
            with self.ensure_result(True):
                try:
                    r = await subprocess
                except GeneratorExit_:
                    raise FutureCancelledException('close is called before result returns')
                else:
                    self.set_result(r)
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
    
    async def wait_and_close(self):
        """
        wait for result; always close no matter success or failed
        """
        try:
            return await self.wait()
        finally:
            self.close()

'''
Created on 2018/6/4

:author: hubo
'''
from __future__ import print_function
from vlcp.config import defaultconfig
from vlcp.server import main
from vlcp.server.module import Module
from vlcp.event.runnable import RoutineContainer
from contextlib import closing
from random import random, randrange

@defaultconfig
class TestModule(Module):
    def __init__(self, server):
        Module.__init__(self, server)
        self.apiroutine = RoutineContainer(self.scheduler)
        self.apiroutine.main = self.main
        self.routines.append(self.apiroutine)
    
    async def create_timers(self, wide=5, deep=5):
        w = random() * 10.0
        if not deep:
            if randrange(100) == 0:
                await self.apiroutine.wait_with_timeout(10)
            return
        async def _subroutine():
            await self.apiroutine.execute_with_timeout(w, self.create_timers(wide, deep-1))
        s = self.apiroutine.subroutine(_subroutine(), False)
        await self.apiroutine.execute_with_timeout(
                        w,
                        self.apiroutine.execute_all([self.create_timers(wide, deep-1)
                                                    for _ in range(wide-1)])
                    )
        s.close()
    
    async def main(self):
        count = 0
        while True:
            await self.apiroutine.wait_with_timeout(0.1)
            self.apiroutine.subroutine(self.create_timers())
            count += 1
            if count % 100 == 0:
                print(count)


if __name__ == '__main__':
    main()

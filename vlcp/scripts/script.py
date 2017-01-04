'''
Created on 2016/10/20

:author: hubo
'''
from __future__ import print_function
from vlcp.server.module import Module
from vlcp.server import main as server_main
from vlcp.event.runnable import RoutineContainer
import sys
import getopt

class ScriptModule(Module):
    '''
    Base script module
    '''
    configkey = 'main'
    # This is not meant to be configured in a configuration file; this is done by the command line executor
    _default_args = ()
    # This is not meant to be configured in a configuration file; this is done by the command line executor
    _default_kwargs = {}
    options = ()
    def __init__(self, server):
        Module.__init__(self, server)
        self.apiroutine = RoutineContainer(self.scheduler)
        def _main():
            try:
                for m in self.run(*self.args, **self.kwargs):
                    yield m
            finally:
                self.scheduler.quit()
        self.apiroutine.main = _main
        self.routines.append(self.apiroutine)
    def run(self, *argv, **kwargs):
        print('Running script...')
        if False:
            yield
    @classmethod
    def main(cls):
        short_opts = 'f:h?'
        long_opts = ['help']
        short_dict = {}
        for opt in cls.options:
            hasarg = len(opt) < 3 or opt[2]
            if hasarg:
                if len(opt) > 1 and opt[1]:
                    short_opts += opt[1] + ':'
                    short_dict[opt[1]] = opt[0]
                long_opts.append(opt[0] + '=')
            else:
                if len(opt) > 1 and opt[1]:
                    short_opts += opt[1]
                    short_dict[opt[1]] = opt[0]
                long_opts.append(opt[0])
        try:
            options, args = getopt.gnu_getopt(sys.argv[1:], short_opts, long_opts)
        except Exception as exc:
            print(str(exc))
            print()
            print(cls.__doc__)
            sys.exit(2)
        else:
            opt_dict = {}
            configfile = None
            for k,v in options:
                if k == '--help' or k == '-?' or k == '-h':
                    print(cls.__doc__)
                    sys.exit(0)
                elif k == '-f':
                    configfile = v
                else:
                    if k.startswith('--'):
                        opt_dict[k[2:]] = v
                    else:
                        opt_dict[short_dict[k[1:]]] = v
            from vlcp.config import manager
            manager['main.args'] = args
            manager['main.kwargs'] = opt_dict
            server_main(configfile, ('__main__.' + cls.__name__,))

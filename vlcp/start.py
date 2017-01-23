#env python
'''
Created on 2015/10/19

:author: hubo

Command-line entry 
'''

from __future__ import print_function
from vlcp.server import main

import sys
import os.path
# No argparse
import getopt

# Document
doc = '''Run VLCP server from command line
[python|pypy] vlcp.py [-f <configfile>] [-d] [-p <pidfile>] [-F <fork>] [startmodule] ...
[python|pypy] vlcp.py --help

Available options:
  -f            Configuration file position (default: /etc/vlcp.conf)
  -d            Start as a daemon (Need python-daemon support)
  -p            When start as a daemon, specify a pid file (default: /var/run/vlcp.pid, or configured in
                configuration file)
  -F            Fork to <fork> sub processes, may be used with protocol.default.reuseport option
  startmodule   Specify modules to be started, replace server.startup in configuration file
  -h,-?,--help  Show this help
'''
def usage():
    print(doc)
    sys.exit(2)
def parsearg():
    try:
        options, args = getopt.gnu_getopt(sys.argv[1:], 'f:p:F:?hd', 'help')
        configfile = None
        pidfile = '/var/run/vlcp.pid'
        daemon = False
        fork = None
        for k,v in options:
            if k == '--help' or k == '-?' or k == '-h':
                usage()
            elif k == '-f':
                configfile = v
            elif k == '-p':
                pidfile = v
            elif k == '-d':
                daemon = True
            elif k == '-F':
                fork = int(v)
        startup = None
        if args:
            startup = args
        return (configfile, daemon, pidfile, startup, fork)
    except getopt.GetoptError as exc:
        print(exc)
        usage()

def default_start():
    """
    Use `sys.argv` for starting parameters. This is the entry-point of `vlcp-start`
    """
    (config, daemon, pidfile, startup, fork) = parsearg()
    if config is None:
        if os.path.isfile('/etc/vlcp.conf'):
            config = '/etc/vlcp.conf'
        else:
            print('/etc/vlcp.conf is not found; start without configurations.')
    elif not config:
        config = None
    main(config, startup, daemon, pidfile, fork)


if __name__ == '__main__':
    default_start()

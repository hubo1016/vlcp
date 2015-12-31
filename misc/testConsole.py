'''
Created on 2015/12/30

@author: hubo
'''
from vlcp.server import main
from vlcp.config.config import manager

if __name__ == '__main__':
    #manager['module.console.startinconsole'] = True
    main(None, ('vlcp.service.debugging.console.Console',))

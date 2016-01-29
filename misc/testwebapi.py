'''
Created on 2016/1/29

:author: hubo
'''

from vlcp.server import main
from vlcp.config.config import manager

if __name__ == '__main__':
    manager['module.httpserver.url'] = ''
    manager['module.httpserver.vhost.api.url'] = 'ltcp://localhost:8081/'
    main(None, ('vlcp.service.manage.webapi.WebAPI', 'vlcp.service.manage.modulemanager.Manager',
                'vlcp.service.utils.knowledge.Knowledge'))

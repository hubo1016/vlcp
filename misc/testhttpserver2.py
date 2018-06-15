'''
Created on 2015/8/27

:author: hubo
'''
from vlcp.server import main
from vlcp.server.module import Module, depend
import vlcp.service.utils.session
import vlcp.service.connection.httpserver
import vlcp.service.web.static
from vlcp.utils.http import HttpHandler
from vlcp.config.config import manager


@depend(vlcp.service.connection.httpserver.HttpServer, vlcp.service.utils.session.Session,
        vlcp.service.web.static.Static)
class TestHttpServer(Module):
    def __init__(self, server):
        Module.__init__(self, server)
        self.routines.append(MainHandler(self.scheduler))

class MainHandler(HttpHandler):
    document = '''
<!DOCTYPE html >
<html>
<head>
<title>Test Server Page</title>
<link rel="stylesheet" type="text/css" href="/static/test.css"/>
</head>
<body>
OK!<br/>
Host = %s<br/>
Path = %s<br/>
Headers = %s<br/>
RealPath = %s<br/>
OriginalPath = %s<br/>
Cookies = %s<br/>
Args = %s<br/>
Form = %s<br/>
</body>
</html>
'''
    rewrites = ((br'/test1', br'/?test1=true'),
                (br'/test/(.*)', br'/?test=\1'),
                (br'/test3/(.*)', br'../?test=\1'),
                (br'/testcss', br'/static/test.css'))
    redirects = ((br'/test2', br'/?test2=true'),
                 (br'/redirect/(.*)', br'/\1'),
                 (br'/redirect2/(.*)', br'../test/\1'))
    def formatstr(self, tmpl, params):
        if not isinstance(tmpl, bytes):
            # Python 3
            return (tmpl % tuple(v if not hasattr(v, 'decode') else v.decode('utf-8') for v in params))
        else:
            return (tmpl % params)
    @HttpHandler.route(b'/', method = [b'GET', b'HEAD', b'POST'])
    async def default(self, env):
        await env.parseform()
        #await self.wait_with_timeout(10)
        if 'auth' in env.args:
            username, password = env.basicauth()
            if username != b'test' or password != b'testpassword':
                env.basicauthfail()
        elif 'exception' in env.args:
            raise Exception('Test Exception')
        elif 'rewrite' in env.args:
            await env.rewrite(b'/?test=a&test2=b')
            return
        elif 'redirect' in env.args:
            await env.redirect(b'/?test3=b&test4=c')
            return
        elif 'redirect2' in env.args:
            await env.redirect(b'http://www.baidu.com/')
            return
        if 'download' in env.args:
            env.header('Content-Disposition', 'attachment; filename="a:b.txt"')
        await env.write(self.formatstr(self.document,
                                      (env.escape(env.host),
                                       env.escape(env.fullpath),
                                       env.escape(repr(env.headers)),
                                       env.escape(env.path),
                                       env.escape(env.originalpath),
                                       env.escape(repr(env.cookies)),
                                       env.escape(repr(env.args)),
                                       env.escape(repr(env.form))
                                       )))

if __name__ == '__main__':
    #s.scheduler.debugging = True
    #s.scheduler.logger.setLevel(logging.DEBUG)
    #Http.debugging = True
    #Http._logger.setLevel(logging.DEBUG)
    #manager['server.debugging'] = True
    manager['module.httpserver.url'] = None
    manager['module.httpserver.urls'] = ['ltcp://0.0.0.0:8080']
    manager['protocol.http.showerrorinfo'] = True
    manager['module.console.startinconsole'] = False
    main(None, ('__main__.TestHttpServer',))
    

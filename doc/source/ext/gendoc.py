'''
Created on 2017/1/13

:author: hubo
'''

from configlist import list_config
from listmodules import list_modules, list_proxy
import jinja2
import os
import os.path

def _merge_all(func):
    def _func():
        result = func('vlcp')
        result.update(func('vlcp_docker'))
        return result
    return _func

generate_list = [(_merge_all(list_config),
                  'allconfigurations.rst.tmpl',
                  'allconfigurations.inc',
                  ['configurations']),
                 (_merge_all(list_proxy),
                  'allproxyconfigs.rst.tmpl',
                  'allproxyconfigs.inc',
                  ['configurations']),
                (_merge_all(list_modules),
                 'allmodulelist.rst.tmpl',
                 'allmodulelist.inc',
                 ['modulelist'])
                 ]

def generate_doc(app, env, added, changed, removed):
    updated = set()
    if not os.path.isdir(os.path.join(env.srcdir, 'gensrc')):
        os.makedirs(os.path.join(env.srcdir, 'gensrc'))
    for func, source, target, update_list in generate_list:
        app.info('Generating %r ...' % (target,))
        config_dict = func()
        with open(os.path.join(env.srcdir, source), 'rb') as f:
            text = f.read().decode('utf-8')
        template = jinja2.Template(text)
        result = template.render(data_input=config_dict)
        with open(os.path.join(env.srcdir,'gensrc',target), 'w') as f:
            f.write(result)
        updated.update(update_list)
    return list(updated)

def setup(app):
    app.connect('env-get-outdated', generate_doc)
    return {'version': '0.1'}

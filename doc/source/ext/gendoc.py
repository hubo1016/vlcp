'''
Created on 2017/1/13

:author: hubo
'''

from configlist import list_config
from listmodules import list_modules, list_proxy
import jinja2
import os
import os.path
import shutil
from pkgutil import walk_packages
from vlcp.event import Event

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
                 ['modulelist']),
                (_merge_all(list_proxy),
                 'allproxymodules.rst.tmpl',
                 'allproxymodules.inc',
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

exclude_list = ['vlcp.protocol.openflow.defs']

def generate_references(app, env, added, changed, removed):
    branch = 'master'
    if 'READTHEDOCS_VERSION' in os.environ:
        branch = os.environ['READTHEDOCS_VERSION']
        if branch == 'latest':
            branch = 'master'
    
    with open(os.path.join(env.srcdir, 'ref_package.rst.tmpl'), 'rb') as f:
        text = f.read().decode('utf-8')
    package_template = jinja2.Template(text)

    with open(os.path.join(env.srcdir, 'ref_module.rst.tmpl'), 'rb') as f:
        text = f.read().decode('utf-8')
    module_template = jinja2.Template(text)
    
    shutil.rmtree(os.path.join(env.srcdir,'gensrc/ref'), True)
    
    def _build_package(root_package, githubproj):
        pkg = __import__(root_package, fromlist=['_'])
        
        for _, module, is_pkg in walk_packages(pkg.__path__, root_package + '.'):
            if any(module.startswith(e) for e in exclude_list):
                continue
            app.info('Generating reference for ' + module + '...')
            if is_pkg:
                package_path = 'gensrc/ref/' + module.replace('.', '/')
                module_path = os.path.join(package_path, '__init__')
                result = package_template.render(package_name = module,
                                                 package_path = '/' + package_path,
                                                 githubproject = githubproj,
                                                 branch = branch)
            else:
                module_path = 'gensrc/ref/' + module.replace('.', '/')
                package_path = os.path.dirname(module_path)
                result = module_template.render(module_name = module,
                                                githubproject = githubproj,
                                                branch = branch)
            if not os.path.isdir(os.path.join(env.srcdir, package_path)):
                os.makedirs(os.path.join(env.srcdir, package_path))
            with open(os.path.join(env.srcdir, module_path + '.rst'), 'w') as f:
                f.write(result)
            yield '/' + module_path
    return [reference] + list(_build_package('vlcp', 'hubo1016/vlcp')) + list(_build_package('vlcp_docker', 'hubo1016/vlcp-docker-plugin'))

def skip_members(app, what, name, obj, skip, options):
    if not skip and name == '__weakref__':
        return True
    elif what == 'module' and isinstance(obj, type) and issubclass(obj, Event):
        return False
    else:
        return skip

def setup(app):
    app.connect('env-get-outdated', generate_doc)
    app.connect('env-get-outdated', generate_references)
    app.connect('autodoc-skip-member', skip_members)
    return {'version': '0.1'}

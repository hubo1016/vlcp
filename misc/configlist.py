'''
Created on 2017/1/3

Automatically acquire all the configurations and their default values

:author: hubo
'''
from __future__ import print_function
from pkgutil import walk_packages
from vlcp.config import Configurable
from inspect import getsourcelines, cleandoc
import argparse

def list_config(root_package = 'vlcp'):
    '''
    Walk through all the sub modules, find subclasses of vlcp.config.Configurable,
    list their available configurations through _default_ prefix
    '''
    pkg = __import__(root_package, fromlist=['_'])
    return_dict = {}
    for imp, module, _ in walk_packages(pkg.__path__, root_package + '.'):
        m = __import__(module, fromlist = ['_'])
        for name, v in vars(m).items():
            if v is not None and isinstance(v, type) and issubclass(v, Configurable) \
                    and v is not Configurable \
                    and hasattr(v, '__dict__') and 'configkey' in v.__dict__:
                configkey = v.__dict__['configkey']
                if configkey not in return_dict:
                    configs = {}
                    v2 = v
                    parents = [v2]
                    while True:
                        parent = None
                        for c in v2.__bases__:
                            if issubclass(c, Configurable):
                                parent = c
                        if parent is None or parent is Configurable:
                            break
                        if hasattr(parent, '__dict__') and 'configkey' not in parent.__dict__:
                            parents.append(parent)
                            v2 = parent
                        else:
                            break
                    for v2 in reversed(parents):
                        for k, default_value in v2.__dict__.items():
                            if k.startswith('_default_'):
                                configname = configkey + '.' + k[len('_default_'):]
                                configs.setdefault(configname, {})['default'] = repr(default_value)
                        # Inspect the source lines to find remarks for these configurations
                        lines, _ = getsourcelines(v2)
                        last_remark = []
                        for l in lines:
                            l = l.strip()
                            if not l:
                                continue
                            if l.startswith('#'):
                                last_remark.append(l[1:])
                            else:
                                if l.startswith('_default_'):
                                    key, sep, _ = l.partition('=')
                                    if sep and key.startswith('_default_'):
                                        configname = configkey + '.' +  key[len('_default_'):].strip()
                                        if configname in configs and last_remark:
                                            configs[configname]['description'] = cleandoc('\n' + '\n'.join(last_remark))
                                del last_remark[:]
                    if configs:
                        return_dict[configkey] = {'class': v.__module__ + '.' + name , 'configs': configs}
    return return_dict

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Automatically acquire all the configurations and their default values')
    parser.add_argument('-r', '--root', help='Root package', default = 'vlcp')
    parser.add_argument('-t', '--template', help='Generate result with a jinja2 template file', default = '')
    parser.add_argument('-e', '--encode', help='Template file encoding', default = 'utf-8')
    args = parser.parse_args()
    config_dict = list_config(args.root)
    if args.template:
        import jinja2
        with open(args.template, 'rb') as f:
            text = f.read().decode(args.encode)
        template = jinja2.Template(text)
        print(template.render(config=config_dict))
    else:
        import json
        print(json.dumps(config_dict, sort_keys=True, indent=2))

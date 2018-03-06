import importlib


def _try_import_json(modules=('ujson', 'simplejson', 'json')):
    exception = ImportError

    for module_name in modules:
        try:
            return importlib.import_module(module_name)
        except ImportError as exc:
            exception = exc

    raise exception

json = _try_import_json()

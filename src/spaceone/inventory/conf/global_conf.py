DATABASES = {
    'default': {
        'db': 'inventory',
        'host': 'localhost',
        'port': 27017,
        'username': '',
        'password': ''
    }
}

CACHES = {
    'default': {},
    'local': {
        'backend': 'spaceone.core.cache.local_cache.LocalCache',
        'max_size': 128,
        'ttl': 86400
    }
}

HANDLERS = {
}

CONNECTORS = {
    'IdentityConnector': {
    },
    'SecretConnector': {
    },
    'PluginConnector': {
    },
    'CollectorPluginConnector': {
    },

}

ENDPOINTS = {}
LOG = {}
QUEUES = {}
SCHEDULERS = {}
WORKERS = {}
TOKEN = ""
TOKEN_INFO = {}
collect_queue = ""      # Queue name for asynchronous collect

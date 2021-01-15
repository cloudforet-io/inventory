DATABASE_AUTO_CREATE_INDEX = True
DATABASE_SUPPORT_AWS_DOCUMENT_DB = False
DATABASES = {
    'default': {
        'db': 'inventory',
        'host': 'localhost',
        'port': 27017,
        'username': '',
        'password': '',
        'read_preference': 'SECONDARY_PREFERRED'
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

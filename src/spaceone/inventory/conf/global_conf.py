DATABASE_AUTO_CREATE_INDEX = True
DATABASE_CASE_INSENSITIVE_INDEX = False
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
        'ttl': 300
    }
}

HANDLERS = {
}

CONNECTORS = {
    'CollectorPluginConnector': {
    },
    'SpaceConnector': {
        'backend': 'spaceone.core.connector.space_connector.SpaceConnector',
        'endpoints': {
            'identity': 'grpc://identity:50051',
            'plugin': 'grpc://plugin:50051',
            'repository': 'grpc://repository:50051',
            'secret': 'grpc://secret:50051',
            'config': 'grpc://config:50051',
        }
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

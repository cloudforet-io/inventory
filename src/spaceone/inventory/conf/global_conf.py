DATABASE_AUTO_CREATE_INDEX = True
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

# Garbage Collection Policies
JOB_TIMEOUT = 2  # 2 Hours
JOB_TERMINATION_TIME = 2 * 30  # 2 Months
RESOURCE_TERMINATION_TIME = 6 * 30  # 6 Months
DEFAULT_DELETE_POLICIES = {
    'inventory.CloudService': 48,  # 48 Hours
    'inventory.CloudServiceType': 48,  # 48 Hours
    'inventory.Region': 48,  # 48 Hours
}
DEFAULT_DISCONNECTED_STATE_DELETE_POLICY = 3  # 3 Count
DELETE_EXCLUDE_DOMAINS = []

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
collect_queue = ""  # Queue name for asynchronous collect

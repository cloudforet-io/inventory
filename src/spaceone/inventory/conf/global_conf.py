# Database Settings
DATABASE_AUTO_CREATE_INDEX = True
DATABASES = {
    "default": {
        "db": "inventory",
        "host": "localhost",
        "port": 27017,
        "username": "",
        "password": "",
        "read_preference": "SECONDARY_PREFERRED",
    }
}

# Cache Settings
CACHES = {
    "default": {},
    "local": {
        "backend": "spaceone.core.cache.local_cache.LocalCache",
        "max_size": 128,
        "ttl": 300,
    },
}

# Garbage Collection Policies
JOB_TIMEOUT = 2  # 2 Hours
JOB_TERMINATION_TIME = 2 * 30  # 2 Months
RESOURCE_TERMINATION_TIME = 3 * 30  # 3 Months
DEFAULT_DELETE_POLICIES = {
    "inventory.CloudService": 48,  # 48 Hours
    "inventory.CloudServiceType": 3 * 30 * 24,  # 3 Months
    "inventory.Region": 48,  # 48 Hours
}
DEFAULT_DISCONNECTED_STATE_DELETE_POLICY = 3  # 3 Count
DELETE_EXCLUDE_DOMAINS = []

# Cloud Service Stats Schedule Settings
STATS_SCHEDULE_HOUR = 15  # Hour (UTC)

# Handler Settings
HANDLERS = {
    # "authentication": [{
    #     "backend": "spaceone.core.handler.authentication_handler:SpaceONEAuthenticationHandler"
    # }],
    # "authorization": [{
    #     "backend": "spaceone.core.handler.authorization_handler:SpaceONEAuthorizationHandler"
    # }],
    # "mutation": [{
    #     "backend": "spaceone.core.handler.mutation_handler:SpaceONEMutationHandler"
    # }],
    # "event": []
}

CONNECTORS = {
    "AWSS3UploadConnector": {},
    "SMTPConnector": {
        "host": "smtp.mail.com",
        "port": "1234",
        "user": "cloudforet",
        "password": "1234",
        "from_email": "support@cloudforet.com",
    },
    "SpaceConnector": {
        "backend": "spaceone.core.connector.space_connector:SpaceConnector",
        "endpoints": {
            "identity": "grpc://identity:50051",
            "plugin": "grpc://plugin:50051",
            "repository": "grpc://repository:50051",
            "secret": "grpc://secret:50051",
            "config": "grpc://config:50051",
            "file_manager": "grpc://file-manager: 50051",
        },
    },
}

# Log Settings
LOG = {}

# Queue Settings
QUEUES = {}
SCHEDULERS = {}
WORKERS = {}

# System Token Settings
TOKEN = ""

# Collector Settings
collect_queue = ""  # Queue name for asynchronous collect

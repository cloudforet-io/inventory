######################################################################
#    ************ Very Important ************
#
# This is resource map for collector
# If you add new service and manager for specific RESOURCE_TYPE,
# add here for collector
######################################################################

RESOURCE_MAP = {
    'inventory.Server': 'ServerManager',
    'inventory.FilterCache': 'FilterManager',
    'inventory.CloudService': 'CloudServiceManager',
    'inventory.CloudServiceType': 'CloudServiceTypeManager',
    'inventory.Region': 'RegionManager',
    'inventory.ErrorResource': 'CollectingManager',
}

SERVICE_MAP = {
    'inventory.Server': 'ServerService',
    'inventory.FilterCache': 'CollectorService',
    'inventory.CloudService': 'CloudServiceService',
    'inventory.CloudServiceType': 'CloudServiceTypeService',
    'inventory.Region': 'RegionService',
    'inventory.ErrorResource': 'CollectorService',
}

DB_QUEUE_NAME = 'db_q'
NOT_COUNT = 0
CREATED = 1
UPDATED = 2
ERROR = 3
JOB_TASK_STAT_EXPIRE_TIME = 3600  # 1 hour
WATCHDOG_WAITING_TIME = 30  # wait 30 seconds, before watchdog works

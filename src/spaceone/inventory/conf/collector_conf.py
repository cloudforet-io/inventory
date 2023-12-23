######################################################################
#    ************ Very Important ************
#
# This is resource map for collector
# If you add new service and manager for specific RESOURCE_TYPE,
# add here for collector
######################################################################

RESOURCE_MAP = {
    "inventory.CloudService": ("CloudServiceService", "CloudServiceManager"),
    "inventory.CloudServiceType": (
        "CloudServiceTypeService",
        "CloudServiceTypeManager",
    ),
    "inventory.Region": ("RegionService", "RegionManager"),
    "inventory.ErrorResource": ("CollectorService", "CollectingManager"),
}


OP_MAP = {"=": "eq", ">=": "gte", "<=": "lte", ">": "gt", "<": "lt", "!=": "not"}

DB_QUEUE_NAME = "db_q"

NOT_COUNT = 0
CREATED = 1
UPDATED = 2
ERROR = 3

JOB_TASK_STAT_EXPIRE_TIME = 3600  # 1 hour
WATCHDOG_WAITING_TIME = 30  # wait 30 seconds, before watchdog works

MAX_MESSAGE_LENGTH = 2000

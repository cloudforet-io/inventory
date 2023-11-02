from spaceone.core.pygrpc.server import GRPCServer
from .change_history import ChangeHistory
from .cloud_service import CloudService
from .cloud_service_query_set import CloudServiceQuerySet
from .cloud_service_report import CloudServiceReport
from .cloud_service_stats import CloudServiceStats
from .cloud_service_type import CloudServiceType
from .collector import Collector
from .collector_rule import CollectorRule
from .job import Job
from .job_task import JobTask
from .note import Note
from .region import Region
from .resource_group import ResourceGroup

_all_ = ['app']

app = GRPCServer()
app.add_service(ChangeHistory)
app.add_service(CloudService)
app.add_service(CloudServiceQuerySet)
app.add_service(CloudServiceReport)
app.add_service(CloudServiceStats)
app.add_service(CloudServiceType)
app.add_service(Collector)
app.add_service(CollectorRule)
app.add_service(Job)
app.add_service(JobTask)
app.add_service(Note)
app.add_service(Region)
app.add_service(ResourceGroup)

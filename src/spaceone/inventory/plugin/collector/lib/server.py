from spaceone.core.pygrpc.server import GRPCServer
from spaceone.core.plugin.server import PluginServer
from spaceone.inventory.plugin.collector.interface.grpc import app
from spaceone.inventory.plugin.collector.service.collector_service import CollectorService
from spaceone.inventory.plugin.collector.service.job_service import JobService

__all__ = ['CollectorPluginServer']


class CollectorPluginServer(PluginServer):
    _grpc_app: GRPCServer = app
    _global_conf_path: str = 'spaceone.inventory.plugin.collector.conf.global_conf:global_conf'
    _plugin_methods = {
        'Collector': {
            'service': CollectorService,
            'methods': ['init', 'verify', 'collect']
        },
        'Job': {
            'service': JobService,
            'methods': ['get_tasks']
        }
    }

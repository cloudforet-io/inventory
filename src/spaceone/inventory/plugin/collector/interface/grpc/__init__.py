from spaceone.core.pygrpc.server import GRPCServer
from spaceone.inventory.plugin.collector.interface.grpc.collector import Collector
from spaceone.inventory.plugin.collector.interface.grpc.job import Job


_all_ = ['app']

app = GRPCServer()
app.add_service(Collector)
app.add_service(Job)

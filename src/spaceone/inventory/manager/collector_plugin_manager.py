import logging
from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector

__ALL__ = ['CollectorPluginManager']

_LOGGER = logging.getLogger(__name__)


class CollectorPluginManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def init_plugin(self, endpoint, options):
        plugin_connector: SpaceConnector = self.locator.get_connector('SpaceConnector', endpoint=endpoint)
        return plugin_connector.dispatch('Collector.init', {'options': options})

    def verify_plugin(self, endpoint, options, secret_data):
        plugin_connector: SpaceConnector = self.locator.get_connector('SpaceConnector', endpoint=endpoint)
        params = {'options': options, 'secret_data': secret_data}
        plugin_connector.dispatch('Collector.verify', params)

    def collect(self, endpoint, options, secret_data, collector_filter, task_options):
        plugin_connector: SpaceConnector = self.locator.get_connector('SpaceConnector', endpoint=endpoint)

        params = {
            'options': options,
            'secret_data': secret_data,
            'filter': collector_filter
        }

        if task_options:
            params['task_options'] = task_options

        return plugin_connector.dispatch('Collector.collect', params)

    def get_tasks(self, endpoint, secret_data, options):
        plugin_connector: SpaceConnector = self.locator.get_connector('SpaceConnector', endpoint=endpoint)

        params = {'options': options, 'secret_data': secret_data}
        return plugin_connector.dispatch('Job.get_tasks', params)

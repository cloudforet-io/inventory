import functools
import logging
from spaceone.api.inventory.v1 import resource_group_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.inventory.model.resource_group_model import ResourceGroup, Resource

__all__ = ['ResourceGroupInfo', 'ResourceGroupsInfo']

_LOGGER = logging.getLogger(__name__)


def ResourceInfo(resource: Resource):
    info = {
        'resource_type': resource.resource_type,
        'filter': change_list_value_type(resource.filter),
        'keyword': resource.keyword
    }
    return resource_group_pb2.Resource(**info)


def ResourceGroupInfo(rg_vo: ResourceGroup, minimal=False):
    info = {
        'resource_group_id': rg_vo.resource_group_id,
        'name': rg_vo.name,
        'project_id': rg_vo.project_id
    }

    if not minimal:
        info.update({
            'resources': list(map(ResourceInfo, rg_vo.resources)),
            'options': change_struct_type(rg_vo.options),
            'tags': change_struct_type(rg_vo.tags),
            'domain_id': rg_vo.domain_id,
            'created_at': utils.datetime_to_iso8601(rg_vo.created_at),
        })

    return resource_group_pb2.ResourceGroupInfo(**info)


def ResourceGroupsInfo(rg_vos, total_count, **kwargs):
    return resource_group_pb2.ResourceGroupsInfo(results=list(map(functools.partial(ResourceGroupInfo, **kwargs),
                                                                  rg_vos)),
                                                 total_count=total_count)

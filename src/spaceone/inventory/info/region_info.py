import functools
import logging
from spaceone.api.inventory.v1 import region_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.inventory.model.region_model import Region

__all__ = ["RegionInfo", "RegionsInfo"]

_LOGGER = logging.getLogger(__name__)


def RegionInfo(region_vo: Region, minimal=False):
    info = {
        "region_id": region_vo.region_id,
        "name": region_vo.name,
        "region_code": region_vo.region_code,
        "provider": region_vo.provider,
    }

    if not minimal:
        info.update(
            {
                "region_key": region_vo.region_key,
                "tags": change_struct_type(region_vo.tags),
                "workspace_id": region_vo.workspace_id,
                "domain_id": region_vo.domain_id,
                "created_at": utils.datetime_to_iso8601(region_vo.created_at),
                "updated_at": utils.datetime_to_iso8601(region_vo.updated_at),
            }
        )

    return region_pb2.RegionInfo(**info)


def RegionsInfo(region_vos, total_count, **kwargs):
    return region_pb2.RegionsInfo(
        results=list(map(functools.partial(RegionInfo, **kwargs), region_vos)),
        total_count=total_count,
    )

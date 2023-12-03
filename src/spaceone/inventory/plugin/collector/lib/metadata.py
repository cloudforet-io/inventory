import os
import inspect
import logging
from spaceone.core.error import *
from spaceone.core import config
from spaceone.core.utils import load_yaml_from_file
from spaceone.inventory.plugin.collector.lib.metadata_generator import MetadataGenerator

__all__ = ["convert_cloud_service_meta", "convert_cloud_service_type_meta"]

_LOGGER = logging.getLogger(__name__)


def convert_cloud_service_meta(
    provider, cloud_service_group, cloud_service_type
) -> dict:
    return {
        "view": {
            "sub_data": {
                "reference": {
                    "resource_type": "inventory.CloudServiceType",
                    "options": {
                        "provider": provider,
                        "cloud_service_group": cloud_service_group,
                        "cloud_service_type": cloud_service_type,
                    },
                }
            }
        }
    }


def convert_cloud_service_type_meta(metadata_path: str) -> dict:
    yaml_path = _get_yaml_path(metadata_path)
    old_metadata = load_yaml_from_file(yaml_path)
    return MetadataGenerator(old_metadata).generate_metadata()


def _get_yaml_path(metadata_path: str) -> str:
    root_path = __import__(config.get_package()).__path__[0]

    try:
        yaml_path = os.path.join(root_path, metadata_path)
    except Exception as e:
        raise ERROR_INVALID_PARAMETER(
            key="metadata_path", reason=f"Invalid metadata_path: {metadata_path}"
        )
    return yaml_path

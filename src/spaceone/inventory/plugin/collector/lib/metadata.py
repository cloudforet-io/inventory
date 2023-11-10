from spaceone.core.utils import load_yaml_from_file


def convert_cloud_service_meta(provider, cloud_service_group, cloud_service_type) -> dict:
    return {
        'view': {
            'sub_data': {
                'reference': {
                    'resource_type': 'inventory.CloudServiceType',
                    'options': {
                        'provider': provider,
                        'cloud_service_group': cloud_service_group,
                        'cloud_service_type': cloud_service_type,
                    }
                }
            }
        }
    }


def convert_cloud_service_type_meta(metadata_path: str) -> dict:
    # Not Implemented
    return {}

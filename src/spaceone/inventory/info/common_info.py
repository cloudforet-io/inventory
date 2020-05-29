from google.protobuf.empty_pb2 import Empty
from spaceone.core.pygrpc.message_type import *

__all__ = ['EmptyInfo', 'StatisticsInfo']


def EmptyInfo():
    return Empty()


def StatisticsInfo(values):
    return change_struct_type({'results': values})

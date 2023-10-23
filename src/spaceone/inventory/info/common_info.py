from google.protobuf.empty_pb2 import Empty
from spaceone.core.pygrpc.message_type import *

__all__ = ['EmptyInfo', 'StatisticsInfo', 'AnalyzeInfo', 'ExportInfo']


def EmptyInfo():
    return Empty()


def StatisticsInfo(result):
    return change_struct_type(result)


def AnalyzeInfo(result):
    return change_struct_type(result)


def ExportInfo(result):
    return change_struct_type(result)

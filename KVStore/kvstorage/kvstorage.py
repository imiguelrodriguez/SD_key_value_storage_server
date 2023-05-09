import time
import random
from typing import Dict, Union, List
import logging
import grpc
from KVStore.protos.kv_store_pb2 import *
from KVStore.protos.kv_store_pb2_grpc import KVStoreServicer, KVStoreStub

from KVStore.protos.kv_store_shardmaster_pb2 import Role

EVENTUAL_CONSISTENCY_INTERVAL: int = 2

logger = logging.getLogger("KVStore")


class KVStorageService:

    def __init__(self):
        pass

    def get(self, key: int) -> str:
        pass

    def l_pop(self, key: int) -> str:
        pass

    def r_pop(self, key: int) -> str:
        pass

    def put(self, key: int, value: str):
        pass

    def append(self, key: int, value: str):
        pass

    def redistribute(self, destination_server: str, lower_val: int, upper_val: int):
        pass

    def transfer(self, keys_values: list):
        pass

    def add_replica(self, server: str):
        pass

    def remove_replica(self, server: str):
        pass


class KVStorageSimpleService(KVStorageService):

    def __init__(self):
        super().__init__()
        self._dictionary = dict()

    def get(self, key: int) -> Union[str, None]:
        try:
            return self._dictionary[key]
        except KeyError:
            return None

    def l_pop(self, key: int) -> Union[str, None]:
        try:
            if len(self._dictionary[key]) >= 1:
                char = self._dictionary[key][0]
                self._dictionary[key] = self._dictionary[key][1:]
                return char
            else:
                self._dictionary[key] = ""
                return ""
        except KeyError:
            return None

    def r_pop(self, key: int) -> Union[str, None]:
        try:
            if len(self._dictionary[key]) >= 1:
                char = self._dictionary[key][-1]
                self._dictionary[key] = self._dictionary[key][:-1]
                return char
            else:
                self._dictionary[key] = ""
                return ""
        except KeyError:
            return None

    def put(self, key: int, value: str):
        self._dictionary[key] = value

    def append(self, key: int, value: str):
        if key in self._dictionary.keys():
            self._dictionary[key] = self._dictionary[key] + value # casting?
        else:
            self._dictionary[key] = value

    def redistribute(self, destination_server: str, lower_val: int, upper_val: int):
        """
        To fill with your code
        """

    def transfer(self, keys_values: List[KeyValue]):
        """
        To fill with your code
        """


class KVStorageReplicasService(KVStorageSimpleService):
    role: Role

    def __init__(self, consistency_level: int):
        super().__init__()
        self.consistency_level = consistency_level
        """
        To fill with your code
        """

    def l_pop(self, key: int) -> str:
        """
        To fill with your code
        """

    def r_pop(self, key: int) -> str:
        """
        To fill with your code
        """

    def put(self, key: int, value: str):
        """
        To fill with your code
        """

    def append(self, key: int, value: str):
        """
        To fill with your code
        """

    def add_replica(self, server: str):
        """
        To fill with your code
        """

    def remove_replica(self, server: str):
        """
        To fill with your code
        """

    def set_role(self, role: Role):
        logger.info(f"Got role {role}")
        self.role = role


class KVStorageServicer(KVStoreServicer):

    def __init__(self, service: KVStorageService):
        self.storage_service = service
        """
        To fill with your code
        """

    def Get(self, request: GetRequest, context) -> GetResponse:
        key = request.key
        response = self.storage_service.get(key)
        get_response = GetResponse()
        if response is not None:
            get_response.value = response
        return get_response

    def LPop(self, request: GetRequest, context) -> GetResponse:
        key = request.key
        response = self.storage_service.l_pop(key)
        get_response = GetResponse()
        if response is not None:
            get_response.value = response
        return get_response

    def RPop(self, request: GetRequest, context) -> GetResponse:
        key = request.key
        response = self.storage_service.r_pop(key)
        get_response = GetResponse()
        if response is not None:
            get_response.value = response
        return get_response

    def Put(self, request: PutRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        key = request.key
        value = request.value
        self.storage_service.put(key, value)
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def Append(self, request: AppendRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        key = request.key
        value = request.value
        self.storage_service.append(key, value)
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def Redistribute(self, request: RedistributeRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        """
        To fill with your code
        """

    def Transfer(self, request: TransferRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        """
        To fill with your code
        """

    def AddReplica(self, request: ServerRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        """
        To fill with your code
        """

    def RemoveReplica(self, request: ServerRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        """
        To fill with your code
        """

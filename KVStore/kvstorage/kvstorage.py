import threading
import time
from typing import Union, List
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

    def lock_replica(self):
        pass

    def release_replica(self):
        pass


class KVStorageSimpleService(KVStorageService):

    def __init__(self):
        super().__init__()
        self._dictionary = dict()
        self._brothers = dict()

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
            self._dictionary[key] = self._dictionary[key] + value  # casting?
        else:
            self._dictionary[key] = value

    def redistribute(self, destination_server: str, lower_val: int, upper_val: int):
        keys_transfer = []
        logger.info(f"Lower val {lower_val}, upper val {upper_val}")
        logger.info(self._dictionary)
        req = TransferRequest()

        for i in range(lower_val, upper_val + 1):
            if i in self._dictionary.keys():
                popped = self._dictionary.pop(i)
                req.keys_values.append(KeyValue(key=i, value=popped))
                keys_transfer.append(KeyValue(key=i, value=popped))
        if destination_server not in self._brothers.keys():
            channel = grpc.insecure_channel(destination_server)
            self._brothers[destination_server] = KVStoreStub(channel)
        if len(keys_transfer) != 0:
            self._brothers[destination_server].Transfer(req)

    def transfer(self, keys_values: List[KeyValue]):
        for key_value in keys_values:
            self._dictionary[key_value.key] = key_value.value


class KVStorageReplicasService(KVStorageSimpleService):
    role: Role

    def __init__(self, consistency_level: int):
        super().__init__()
        self.consistency_level = consistency_level
        self._replicas = dict()
        self._updates = set()
        self._updates_proc = threading.Thread(target=self._update)
        self._lock = threading.Lock()

    def _update(self):
        while True:
            time.sleep(EVENTUAL_CONSISTENCY_INTERVAL)
            if self.consistency_level < len(self._replicas):
                replicas_list = list(self._replicas.keys())
                for key in self._updates:
                    for replica in range(self.consistency_level, len(self._replicas)):
                        self._replicas[replicas_list[replica]].Put(PutRequest(key=key, value=super()._dictionary[key]))

    def get(self, key: int) -> Union[str, None]:
        return super().get(key)

    def l_pop(self, key: int) -> str:
        self._lock_replicas()
        val = super().l_pop(key)
        if self.role == Role.Value("MASTER"):
            keys = list(self._replicas)
            consistency = self.consistency_level if self.consistency_level <= len(self._replicas) else len(
                self._replicas)
            for i in range(consistency):
                self._replicas[keys[i]].LPop(GetRequest(key))
                self._replicas[keys[i]].ReleaseReplica(ReleaseRequest())
        return val

    def r_pop(self, key: int) -> str:
        self._lock_replicas()
        val = super().l_pop(key)
        if self.role == Role.Value("MASTER"):
            keys = list(self._replicas)
            consistency = self.consistency_level if self.consistency_level <= len(self._replicas) else len(
                self._replicas)
            for i in range(consistency):
                self._replicas[keys[i]].RPop(GetRequest(key))
                self._replicas[keys[i]].ReleaseReplica(ReleaseRequest())
        return val

    def put(self, key: int, value: str):
        self._lock_replicas()
        super().put(key, value)
        if self.role == Role.Value("MASTER"):
            keys = list(self._replicas)
            consistency = self.consistency_level if self.consistency_level <= len(self._replicas) else len(
                self._replicas)
            for i in range(consistency):
                self._replicas[keys[i]].Put(PutRequest(key=key, value=value))
                self._replicas[keys[i]].ReleaseReplica(ReleaseRequest())

    def append(self, key: int, value: str):
        self._lock_replicas()
        super().append(key, value)
        if self.role == Role.Value("MASTER"):
            keys = list(self._replicas)
            consistency = self.consistency_level if self.consistency_level <= len(self._replicas) else len(
                self._replicas)
            for i in range(consistency):
                self._replicas[keys[i]].Append(AppendRequest(key, value))
                self._replicas[keys[i]].ReleaseReplica(ReleaseRequest())

    def add_replica(self, server: str):
        channel = grpc.insecure_channel(server)
        stub = KVStoreStub(channel)
        self._replicas[server] = stub
        req = TransferRequest()
        for i in self._dictionary.keys():
            req.keys_values.append(KeyValue(key=i, value=self._dictionary[i]))
        stub.Transfer(req)

    def remove_replica(self, server: str):
        self._replicas.pop(server)

    def transfer(self, keys_values: List[KeyValue]):
        super().transfer(keys_values)
        for key_value in keys_values:
            self._dictionary[key_value.key] = key_value.value
        req = TransferRequest()
        for i in self._dictionary.keys():
            req.keys_values.append(KeyValue(key=i, value=self._dictionary[i]))
        for replica in self._replicas.keys():
            self._replicas[replica].Transfer(req)

    def set_role(self, role: Role):
        logger.info(f"Got role {role}")
        self.role = role
        if role == 0:
            self._updates_proc.start()

    def lock_replica(self):
        self._lock.acquire()

    def release_replica(self):
        self._lock.release()

    def _lock_replicas(self):
        if self.role == 0:  # that is, I'm a master
            keys = list(self._replicas)
            consistency = self.consistency_level if self.consistency_level <= len(self._replicas) else len(self._replicas)
            for i in range(consistency):
                self._replicas[keys[i]].LockReplica(LockRequest())


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
        destination_server = request.destination_server
        lower_val = request.lower_val
        upper_val = request.upper_val
        self.storage_service.redistribute(destination_server, lower_val, upper_val)
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def Transfer(self, request: TransferRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        keys_values = request.keys_values
        self.storage_service.transfer(list(keys_values))
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def AddReplica(self, request: ServerRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        server = request.server
        self.storage_service.add_replica(server)
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def RemoveReplica(self, request: ServerRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        server = request.server
        self.storage_service.remove_replica(server)
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def LockReplica(self, request: LockRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        self.storage_service.lock_replica()
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def ReleaseReplica(self, request: ReleaseRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        self.storage_service.release_replica()
        return google_dot_protobuf_dot_empty__pb2.Empty()

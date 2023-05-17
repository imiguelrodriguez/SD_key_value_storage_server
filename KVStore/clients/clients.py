from typing import Union, Dict
import grpc
import logging
from KVStore.protos.kv_store_pb2 import GetRequest, PutRequest, GetResponse, AppendRequest
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub
from KVStore.protos.kv_store_shardmaster_pb2 import QueryRequest, QueryResponse, QueryReplicaRequest, Operation
from KVStore.protos.kv_store_shardmaster_pb2_grpc import ShardMasterStub

logger = logging.getLogger(__name__)


def _get_return(ret: GetResponse) -> Union[str, None]:
    if ret.HasField("value"):
        return ret.value
    else:
        return None


class SimpleClient:
    def __init__(self, kvstore_address: str):
        self.channel = grpc.insecure_channel(kvstore_address)
        self.stub = KVStoreStub(self.channel)

    def get(self, key: int) -> Union[str, None]:
        get_request = GetRequest(key=key)
        return _get_return(self.stub.Get(get_request))

    def l_pop(self, key: int) -> Union[str, None]:
        get_request = GetRequest(key=key)
        return _get_return(self.stub.LPop(get_request))

    def r_pop(self, key: int) -> Union[str, None]:
        get_request = GetRequest(key=key)
        return _get_return(self.stub.RPop(get_request))

    def put(self, key: int, value: str):
        put_request = PutRequest(key=key, value=value)
        self.stub.Put(put_request)

    def append(self, key: int, value: str):
        append_request = AppendRequest(key=key, value=value)
        self.stub.Append(append_request)

    def stop(self):
        self.channel.close()


class ShardClient(SimpleClient):
    def __init__(self, shard_master_address: str, kvstore_address: str):
        super().__init__(kvstore_address)
        self.channel = grpc.insecure_channel(shard_master_address)
        self.stub = ShardMasterStub(self.channel)
        self._servers = dict()  # dictionary that will store port:stub, so that overhead is reduced

    def _query_server(self, key: int, value: Union[str, None], type: str):
        req = QueryRequest(key=key)
        response = self.stub.Query(req)
        if key not in self._servers.keys():
            channel = grpc.insecure_channel(response)
            self._servers[key] = KVStoreStub(channel)
        if type.upper() == "GET":
            get_request = GetRequest(key=key)
            return get_request
        elif type.upper() == "PUT":
            put_request = PutRequest(key=key, value=value)
            return put_request
        else:
            append_request = AppendRequest(key=key, value=value)
            return append_request

    def get(self, key: int) -> Union[str, None]:
        return _get_return(self._servers[key].Get(self._query_server(key, None, "get")))

    def l_pop(self, key: int) -> Union[str, None]:
        return _get_return(self._servers[key].LPop(self._query_server(key, None, "get")))

    def r_pop(self, key: int) -> Union[str, None]:
        return _get_return(self._servers[key].RPop(self._query_server(key, None, "get")))

    def put(self, key: int, value: str):
        self._servers[key].Put(self._query_server(key, None, "put"))

    def append(self, key: int, value: str):
        self._servers[key].Append(self._query_server(key, None, "append"))


class ShardReplicaClient(ShardClient):

    def get(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """

    def l_pop(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """

    def r_pop(self, key: int) -> Union[str, None]:
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

from typing import Union, Dict, Type
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
    def __init__(self, shard_master_address: str):
        self.channel = grpc.insecure_channel(shard_master_address)
        self.stub = ShardMasterStub(self.channel)
        self._servers = dict()  # dictionary that will store port:stub, so that overhead is reduced

    def _query_server(self, key: int, value: Union[str, None], typee: Union[Type[GetRequest], Type[PutRequest], Type[AppendRequest]]):
        req = QueryRequest(key=key)
        response = self.stub.Query(req)
        if response.server not in self._servers.keys():
            channel = grpc.insecure_channel(response.server)
            self._servers[response.server] = KVStoreStub(channel)
        if typee is GetRequest:
            return typee(key=key), response.server
        else:
            return typee(key=key, value=value), response.server

    def get(self, key: int) -> Union[str, None]:
        query, server = self._query_server(key, None, GetRequest)
        val = _get_return(self._servers[server].Get(query))
        return val

    def l_pop(self, key: int) -> Union[str, None]:
        query, server = self._query_server(key, None, GetRequest)
        val = _get_return(self._servers[server].LPop(query))
        return val

    def r_pop(self, key: int) -> Union[str, None]:
        query, server = self._query_server(key, None, GetRequest)
        val = _get_return(self._servers[server].RPop(query))
        return val

    def put(self, key: int, value: str):
        query, server = self._query_server(key, value, PutRequest)
        self._servers[server].Put(query)

    def append(self, key: int, value: str):
        query, server = self._query_server(key, value, AppendRequest)
        self._servers[server].Append(query)


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

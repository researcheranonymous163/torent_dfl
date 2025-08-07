import asyncio
import json
import sys
from typing import override, Awaitable, Callable, Any

import grpc.aio
from google.protobuf.empty_pb2 import Empty as GrpcEmpty
from grpc.aio import server as GrpcServer, Channel as GrpcChannel, ServicerContext, insecure_channel as grpc_insecure_channel

from dfl.communication import Communicator, Meta, LocalPublishingCommunicator
from dfl.communication.encoders import Encoder, PickleBytesEncoder, ChainEncoder, ForceTypeEncoder
from dfl.extensions.asyncio import shield_exception
from .communication_pb2 import Message as GrpcMessage, Header as GrpcHeader, Subscribe as GrpcSubscribe, Receive as GrpcReceive
from .communication_pb2_grpc import CommunicatorServicer as GrpcCommunicatorServicer, CommunicatorStub as GrpcCommunicatorStub
from .communication_pb2_grpc import add_CommunicatorServicer_to_server


class GrpcCommunicator(Communicator, GrpcCommunicatorServicer):
	_default_grpc_options = {
		'grpc.max_receive_message_length': -1,
		'grpc.max_send_message_length': -1
	}
	
	@override
	def __init__(
			self,
			name: str,
			receive_handler: Callable[[Meta | None], Awaitable[tuple[Meta | None, Any]]] | None = None,
			handler: Callable[[Meta | None, Any], Awaitable[None]] | None = None,
			listen_address: str | None = None,  # TODO: default to an address reaching the listen_address.
			encoder: Encoder | None = None,
			grpc_options: dict[str, Any] | None = None,
			init_server: bool | grpc.aio.Server = True,
	) -> None:
		super().__init__(name)
		
		self._handler = handler
		self._receive_handler = receive_handler
		
		if listen_address is None:
			listen_address = f"[::]:50051"
		self._listen_address = listen_address
		
		if encoder is None:
			encoder = PickleBytesEncoder()
		self._encoder = ChainEncoder((encoder, ForceTypeEncoder(bytes)))
		
		if grpc_options is None:
			grpc_options = {}
		self._grpc_options = GrpcCommunicator._default_grpc_options | grpc_options
		
		self._init_server = init_server
		
		self._clients: dict[str, tuple[GrpcCommunicatorStub, GrpcChannel]] = dict()
		self._clients_lock = asyncio.Lock()
	
	@override
	async def __ainit__(self) -> None:
		await super().__ainit__()
		
		self._pub = await LocalPublishingCommunicator()
		
		# noinspection PySimplifyBooleanCheck
		if self._init_server != False:
			# noinspection PySimplifyBooleanCheck
			if self._init_server == True:
				self._server = GrpcServer(options=list(self._grpc_options.items()))
				self._server.add_insecure_port(self._listen_address)
			elif isinstance(self._init_server, grpc.aio.Server):
				self._server = self._init_server
			else:
				raise ValueError(f"Unexpected `init_server` value: {self._init_server}.")
			
			add_CommunicatorServicer_to_server(self, self._server)
			if self._init_server == True:
				await self._server.start()
		
		del self._init_server
		del self._listen_address
	
	# FIXME: the default value for `pre_encode` should probably be `True`.
	@override
	async def publish(self, meta: Meta, data, meta_id: Meta | None = None, pre_encode=False, **kwargs) -> None:
		if meta_id is None:
			meta_id = meta
		if 'from' not in meta_id:
			meta_id = meta_id | {'from': self.name}
		
		if pre_encode:  # FIXME: the pre-encoded status should be saved beside the data to avoid re-encoding in `HandleSubscribe`.
			meta, data = await self._encoder.encode(meta, data)
		
		await self._pub.publish(meta, data, meta_id)
	
	@override
	async def subscribe(self, meta: Meta, meta_only=False, wait_publish=True, **kwargs) -> (Meta, Any):
		from_client = await self._get_client(meta['from'])
		
		meta, data = GrpcCommunicator._from_grpc_message(
			await from_client.HandleSubscribe(GrpcSubscribe(
				header=GrpcCommunicator._to_grpc_header(meta),
				wait_publish=wait_publish,
				meta_only=meta_only
			))
		)
		
		meta, data = await self._encoder.decode(meta, data)
		return meta, data
	
	@override
	async def unpublish(self, meta: Meta, **kwargs) -> None:
		await self._pub.unpublish(meta)
	
	@override
	async def send(self, to: str, meta: Meta | None = None, data=None, **kwargs) -> None:
		meta, data = await self._encoder.encode(meta, data)
		
		to_client = await self._get_client(to)
		await to_client.Handle(GrpcCommunicator._to_grpc_message(meta, data))
	
	@override
	async def receive(self, from_, meta: Meta | None = None, meta_only=False, **kwargs) -> (Meta | None, Any):
		from_client = await self._get_client(from_)
		
		meta, data = GrpcCommunicator._from_grpc_message(
			await from_client.HandleReceive(GrpcReceive(
				header=GrpcCommunicator._to_grpc_header(meta),
				meta_only=meta_only
			))
		)
		
		meta, data = await self._encoder.decode(meta, data)
		return meta, data
	
	@override
	async def Handle(self, request: GrpcMessage, context: ServicerContext[GrpcMessage, GrpcEmpty]) -> GrpcEmpty:
		if self._handler is not None:
			meta, data = GrpcCommunicator._from_grpc_message(request)
			meta, data = await self._encoder.decode(meta, data)
			await self._handler(meta, data)
			return GrpcEmpty()
		else:
			raise NotImplementedError
	
	@override
	async def HandleSubscribe(self, request: GrpcSubscribe, context: ServicerContext[GrpcSubscribe, GrpcMessage]) -> GrpcMessage:
		meta = GrpcCommunicator._from_grpc_header(request.header)
		assert meta is not None
		meta, data = await self._pub.subscribe(meta, request.meta_only, request.wait_publish)
		
		if request.meta_only:
			data = None
		else:
			meta, data = await self._encoder.encode(meta, data)
		
		return GrpcCommunicator._to_grpc_message(meta, data)
	
	@override
	async def HandleReceive(self, request: GrpcReceive, context: ServicerContext[GrpcReceive, GrpcMessage]) -> GrpcMessage:
		meta = GrpcCommunicator._from_grpc_header(request.header)
		
		if self._receive_handler is not None:
			meta, data = await self._receive_handler(meta)
		else:
			raise NotImplementedError
		
		if request.meta_only:
			data = None
		else:
			meta, data = await self._encoder.encode(meta, data)
		
		return GrpcCommunicator._to_grpc_message(meta, data)
	
	async def _get_client(self, name: str) -> GrpcCommunicatorStub:
		name = GrpcCommunicator._normalize_name(name)
		
		client, channel = self._clients.get(name, (None, None))
		if client is not None:
			return client
		
		async with self._clients_lock:
			client, channel = self._clients.get(name, (None, None))
			if client is not None:
				return client
			else:
				channel = grpc_insecure_channel(target=name, options=list(self._grpc_options.items()))
				
				client = GrpcCommunicatorStub(channel)
				self._clients[name] = (client, channel)
				return client
	
	async def _release_client(self, name: str) -> None:
		name = GrpcCommunicator._normalize_name(name)
		
		_, channel = self._clients[name]
		await channel.close()
		del self._clients[name]
	
	async def close(self) -> None:
		await super().close()
		
		async def _release_all_clients():
			async with self._clients_lock:
				async with asyncio.TaskGroup() as tg:
					for name in self._clients.keys():
						tg.create_task(shield_exception(self._release_client(name)))
		
		async with asyncio.TaskGroup() as tg:
			tg.create_task(shield_exception(_release_all_clients()))
			tg.create_task(shield_exception(self._server.stop(grace=sys.float_info.max)))
	
	@staticmethod
	def _normalize_name(name: str) -> str:
		if ':' not in name:
			return f'{name}:50051'
		else:
			return name
	
	@staticmethod
	def _to_grpc_message(meta: Meta | None, data) -> GrpcMessage:
		return GrpcMessage(header=GrpcCommunicator._to_grpc_header(meta), body=data)
	
	@staticmethod
	def _from_grpc_message(grpc_message: GrpcMessage) -> (Meta | None, Any):
		return GrpcCommunicator._from_grpc_header(grpc_message.header), grpc_message.body
	
	@staticmethod
	def _from_grpc_header(header: GrpcHeader | None) -> Meta | None:
		if header is None:
			return None
		else:
			return json.loads(header.json_string)
	
	@staticmethod
	def _to_grpc_header(meta: Meta | None) -> GrpcHeader | None:
		if meta is None:
			return None
		else:
			return GrpcHeader(json_string=json.dumps(meta))

import asyncio
import functools
import json
from collections import defaultdict
from typing import Any, Callable, Awaitable, override

Meta = dict[str, Any]


class Communicator:
	def __init__(self, name) -> None:
		self.name = name
	
	async def __ainit__(self) -> None:
		pass
	
	def __await__(self):
		async def _coro():
			await self.__ainit__()
			return self
		
		return _coro().__await__()
	
	async def publish(self, meta: Meta, data, meta_id: Meta | None = None, **kwargs) -> None:
		"""
		`meta_id` should be a sub-set of `meta`; it defaults to `meta`.
		"""
		raise NotImplementedError
	
	async def subscribe(self, meta: Meta, meta_only=False, wait_publish=True, **kwargs) -> (Meta, Any):
		raise NotImplementedError
	
	async def unpublish(self, meta: Meta, **kwargs) -> None:
		raise NotImplementedError
	
	async def send(self, to, meta: Meta | None = None, data=None, **kwargs) -> None:
		raise NotImplementedError
	
	async def receive(self, from_, meta: Meta | None = None, meta_only=False, **kwargs) -> (Meta | None, Any):
		raise NotImplementedError
	
	async def close(self) -> None:
		pass


class LocalPublishingCommunicator(Communicator):
	@override
	def __init__(self, meta_hasher: Callable[[Meta], str] = functools.partial(json.dumps, sort_keys=True)):
		super().__init__(None)
		
		self._meta_hasher = meta_hasher
		
		self._d_lock = asyncio.Lock()
		self._d: dict[str, asyncio.Event | (Meta, Any)] = dict()
	
	@override
	async def publish(self, meta: Meta, data, meta_id: Meta | None = None, **kwargs) -> None:
		if isinstance(data, asyncio.Event):
			raise ValueError("Storing the special `asyncio.Event` and `None` data types are prohibited (not yet supported).")
		
		if meta_id is None:
			meta_id = meta
		
		meta_hash = self._meta_hasher(meta_id)
		
		# Fast path: if the meta entry already exists.
		_, d_data = self._d.get(meta_hash, (None, None))
		if d_data is not None:
			if not isinstance(d_data, asyncio.Event):
				raise ValueError(f"Duplicate meta.")
			
			self._d[meta_hash] = (meta, data)
			
			d_data.set()
		else:  # Slow path
			async with self._d_lock:
				_, d_data = self._d.get(meta_hash, (None, None))
				if d_data is None:
					self._d[meta_hash] = (meta, data)
				if d_data is not None:
					if not isinstance(d_data, asyncio.Event):
						raise ValueError(f"Duplicate meta.")
					
					self._d[meta_hash] = (meta, data)
					
					d_data.set()
	
	@override
	async def subscribe(self, meta: Meta, meta_only=False, wait_publish=True, **kwargs) -> (Meta, Any):
		meta_hash = self._meta_hasher(meta)
		
		if not wait_publish:
			d_meta, d_data = self._d.get(meta_hash, (None, None))
			if d_data is None or isinstance(d_data, asyncio.Event):
				raise FileNotFoundError
			else:
				return d_meta, d_data
		else:
			# Fast path: if the meta entry already exists.
			d_meta, d_data = self._d.get(meta_hash, (None, None))
			if d_data is not None:
				if isinstance(d_data, asyncio.Event):
					await d_data.wait()
					return await self.subscribe(meta=meta, meta_only=meta_only, wait_publish=wait_publish)
				else:
					return d_meta, d_data
			else:  # Slow path
				async with self._d_lock:
					d_meta, d_data = self._d.get(meta_hash, (None, None))
					if d_data is None:
						event = asyncio.Event()
						self._d[meta_hash] = (None, event)
					else:
						if isinstance(d_data, asyncio.Event):
							event = d_data
						else:
							return d_meta, d_data
				
				await event.wait()
				return await self.subscribe(meta=meta, meta_only=meta_only, wait_publish=wait_publish)
	
	@override
	async def unpublish(self, meta: Meta, **kwargs) -> None:
		meta_hash = self._meta_hasher(meta)
		
		async with self._d_lock:
			_ = self._d.pop(meta_hash, None)


class LocalCommunicator(Communicator):
	_default_pool = dict()
	
	_pools_locks = defaultdict(asyncio.Lock)
	_pools_pubs = defaultdict(LocalPublishingCommunicator)
	
	@override
	def __init__(
			self,
			name,
			receive_handler: Callable[[Meta | None], Awaitable[tuple[Meta | None, Any]]] | None = None,
			handler: Callable[[Meta | None, Any], Awaitable[None]] | None = None,
			pool: dict[Any, 'LocalCommunicator'] | None = None,
	) -> None:
		super().__init__(name)
		
		if pool is None:
			pool = LocalCommunicator._default_pool
		
		self._pool = pool
		self._pool_lock = LocalCommunicator._pools_locks[id(self._pool)]
		
		self._receive_handler = receive_handler
		self._handler = handler
	
	@override
	async def __ainit__(self) -> None:
		await super().__ainit__()
		
		self._pub = await LocalCommunicator._pools_pubs[id(self._pool)]
		
		async with self._pool_lock:
			if self.name in self._pool:
				raise ValueError(f"Duplicate local node: {self.name}.")
			self._pool[self.name] = self
	
	@override
	async def publish(self, meta: Meta, data, meta_id: Meta | None = None, **kwargs) -> None:
		await self._pub.publish(meta, data, meta_id)
	
	async def subscribe(self, meta: Meta, meta_only=False, wait_publish=True, **kwargs) -> (Meta, Any):
		return await self._pub.subscribe(meta, meta_only, wait_publish)
	
	@override
	async def unpublish(self, meta: Meta, **kwargs) -> None:
		await self._pub.unpublish(meta)
	
	@override
	async def send(self, to, meta: Meta | None = None, data=None, **kwargs) -> None:
		to_communicator = self._pool.get(to, None)
		if to_communicator is None:
			raise ValueError(f"No local communicator named \"{to}\".")
		
		if to_communicator._handler is not None:
			await to_communicator._handler(meta, data)
		else:
			raise NotImplementedError
	
	@override
	async def receive(self, from_, meta: Meta | None = None, meta_only=False, **kwargs) -> (Meta | None, Any):
		from_communicator = self._pool.get(from_, None)
		if from_communicator is None:
			raise ValueError(f"No local communicator named \"{from_}\".")
		
		if from_communicator._receive_handler is not None:
			return await from_communicator._receive_handler(meta)
		else:
			raise NotImplementedError
	
	@override
	async def close(self):
		await super().close()
		
		async with self._pool_lock:
			del self._pool[self.name]


def __getattr__(name):
	match name:
		case 'NatsCommunicator':
			from .nats import NatsCommunicator
			globals()[name] = NatsCommunicator
			return NatsCommunicator
		case 'QbittorrentCommunicator':
			from .qbittorrent import QbittorrentCommunicator
			globals()[name] = QbittorrentCommunicator
			return QbittorrentCommunicator
		case 'GrpcCommunicator':
			from .grpc import GrpcCommunicator
			globals()[name] = GrpcCommunicator
			return GrpcCommunicator
		case _:
			raise AttributeError(f"Module {__name__!r} has no attribute {name!r}.")

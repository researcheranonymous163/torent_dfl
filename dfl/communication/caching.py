import asyncio
from typing import Any, override

from frozendict import deepfreeze, frozendict

from . import Communicator, Meta


class StrongSubscribeCachingCommunicatorDecorator(Communicator):
	# noinspection PyMissingConstructor
	def __init__(self, communicator: Communicator) -> None:
		self._communicator = communicator
		
		self._cache = dict[frozendict, asyncio.Task | tuple[Meta, Any]]()
		self._cache_lock = asyncio.Lock()
	
	@property
	def name(self):
		return self._communicator.name
	
	@name.setter
	def name(self, name):
		self._communicator.name = name
	
	@override
	async def subscribe(self, meta: Meta, meta_only=False, wait_publish=True, **kwargs) -> (Meta, Any):
		frozen_meta = deepfreeze(meta)
		
		set_res = False
		res = self._cache.get(frozen_meta, None)
		if res is None:
			if not meta_only:
				async with self._cache_lock:
					res = self._cache.get(frozen_meta, None)
					if res is None:
						res = asyncio.create_task(self._communicator.subscribe(meta, meta_only, wait_publish, **kwargs))
						self._cache[frozen_meta] = res
						set_res = True
			else:
				res = await self._communicator.subscribe(meta, meta_only, wait_publish, **kwargs)
		
		if isinstance(res, asyncio.Task):
			res = await res
			if set_res:
				self._cache[frozen_meta] = res
		
		return res
	
	@override
	async def receive(self, from_, meta: Meta | None = None, meta_only=False, **kwargs) -> (Meta | None, Any):
		return await self._communicator.receive(from_, meta, meta_only, **kwargs)  # TODO: cache receive too, if requested.
	
	@override
	async def publish(self, meta: Meta, data, meta_id: Meta | None = None, **kwargs) -> None:
		return await self._communicator.publish(meta, data, meta_id, **kwargs)
	
	@override
	async def unpublish(self, meta: Meta, **kwargs) -> None:
		return await self._communicator.unpublish(meta, **kwargs)
	
	@override
	async def send(self, to, meta: Meta | None = None, data=None, **kwargs) -> None:
		return await self._communicator.send(to, meta, data, **kwargs)
	
	async def uncache(self, meta: Meta | None = None) -> None:
		if meta is None:
			return
		
		frozen_meta = deepfreeze(meta)
		
		async with self._cache_lock:
			del self._cache[frozen_meta]
	
	async def clear(self):
		async with self._cache_lock:
			self._cache.clear()
	
	async def close(self) -> None:
		async with asyncio.TaskGroup() as tg:
			tg.create_task(self.clear())
			tg.create_task(self._communicator.close())

# TODO: `WeakCachingCommunicatorDecorator`.

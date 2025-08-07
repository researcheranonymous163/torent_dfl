from __future__ import annotations

import asyncio
from typing import Callable, override, Any, TYPE_CHECKING

from dfl import Meta

if TYPE_CHECKING:
	import torch


class Aggregator:
	async def add(self, meta: Meta | None = None, data=None) -> None:
		raise NotImplementedError
	
	async def aggregate(self) -> Any:
		raise NotImplementedError


class SumTensorsAggregator(Aggregator):
	def __init__(self):
		super().__init__()
		
		import torch
		
		self._sum = torch.tensor(0, torch.float64)
		self._lock = asyncio.Lock()
	
	@override
	async def add(self, meta: Meta | None = None, data=None) -> None:
		async with self._lock:
			self._sum = await asyncio.to_thread(lambda: self._sum.add_(data))
	
	@override
	async def aggregate(self) -> torch.Tensor:
		async with self._lock:
			return self._sum


class AvgTensorsAggregator(Aggregator):
	def __init__(self):
		super().__init__()
		
		import torch
		
		self._sum = torch.tensor(0, dtype=torch.float64)
		self._lock = asyncio.Lock()
		self._n = 0
	
	@override
	async def add(self, meta: Meta | None = None, data=None) -> None:
		async with self._lock:
			self._n += 1
			self._sum = await asyncio.to_thread(lambda: self._sum.add(data))
	
	@override
	async def aggregate(self) -> torch.Tensor:
		async with self._lock:
			return await asyncio.to_thread(lambda: self._sum / self._n)


class WeightedAvgTensorsAggregator(Aggregator):
	def __init__(self, weight_meta_key: str = 'weight'):
		super().__init__()
		
		import torch
		
		self._weight_meta_key = weight_meta_key
		
		self._sum = torch.tensor(0, dtype=torch.float64)
		self._lock = asyncio.Lock()
		self._ws = 0
	
	@override
	async def add(self, meta: Meta | None = None, data=None) -> None:
		async with self._lock:
			w = meta[self._weight_meta_key]
			self._ws += w
			self._sum = await asyncio.to_thread(lambda: self._sum.add_(data, alpha=w))
	
	@override
	async def aggregate(self) -> torch.Tensor:
		async with self._lock:
			return await asyncio.to_thread(lambda: self._sum / self._ws)


class PartitioningAggregator(Aggregator):
	def __init__(self, partition_agg_factory: Callable[..., Aggregator], partition_meta_key: str = 'name'):
		super().__init__()
		self._partition_agg_factory = partition_agg_factory
		self._partition_meta_key = partition_meta_key
		self._partitions_aggs = dict()
		self._lock = asyncio.Lock()  # TODO: use a read/write lock.
	
	@override
	async def add(self, meta: Meta | None = None, data=None) -> None:
		partition = meta.get(self._partition_meta_key)
		if partition is None:
			raise ValueError(f"Data `{(meta, data)}` has no `{self._partition_meta_key}` metadata.")
		
		async with self._lock:
			agg = self._partitions_aggs.get(partition, None)
			if agg is None:
				agg = self._partition_agg_factory(**{self._partition_meta_key: partition})
				self._partitions_aggs[partition] = agg
		
		await agg.add(meta, data)
	
	@override
	async def aggregate(self) -> dict[Any, Any]:
		async with self._lock:
			return dict([(partition, await agg.aggregate()) for (partition, agg) in self._partitions_aggs.items()])


class StateDictAggregator(PartitioningAggregator):
	def __init__(self, param_agg_factory: Callable[..., Aggregator] | None = None, param_meta_key: str = 'name'):
		if param_agg_factory is None:
			param_agg_factory = AvgTensorsAggregator
		
		super().__init__(partition_agg_factory=param_agg_factory, partition_meta_key=param_meta_key)
	
	@override
	async def add(self, meta: Meta | None = None, data=None) -> None:
		if not isinstance(data, dict):
			raise ValueError(f"Unexpected data type `{type(data)}`.")
		
		for param_name, v in data.items():
			param_meta = meta.copy() | {self._partition_meta_key: param_name}
			await super().add(param_meta, v)


class TensorToDeviceDecoratingAggregator(Aggregator):
	def __init__(self, device, agg: Aggregator):
		super().__init__()
		self._device = device
		self._agg = agg
	
	@override
	async def add(self, meta: Meta | None = None, data=None) -> None:
		data = await asyncio.to_thread(data.to, self._device)
		await self._agg.add(meta, data)
	
	@override
	async def aggregate(self) -> Any:
		return await self._agg.aggregate()

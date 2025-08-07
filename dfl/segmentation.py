from __future__ import annotations

from typing import Any, override, OrderedDict, TYPE_CHECKING

if TYPE_CHECKING:
	import torch


class Segmenter:
	async def segment(self, model: Any) -> dict[Any, Any] | list[Any]:
		raise NotImplementedError
	
	async def de_segment(self, segmented_model: dict[Any, Any] | list[Any]) -> Any:
		raise NotImplementedError


class TorchStateDictSegmenter(Segmenter):
	@override
	async def segment(self, model: dict[str, torch.Tensor]) -> dict[str, torch.Tensor]:
		return model
	
	@override
	async def de_segment(self, segmented_model: dict[str, torch.Tensor]) -> dict[str, torch.Tensor]:
		return segmented_model


class KEvenTorchStateDictSegmenter(Segmenter):
	def __init__(self, k: int, model_params_specs: OrderedDict[str, torch.Size] | None = None):
		self.k = k
		self.model_params_specs = model_params_specs
	
	@override
	async def segment(self, model: OrderedDict[str, torch.Tensor]) -> list[torch.Tensor]:
		import torch
		
		if self.model_params_specs is None:
			self.model_params_specs = OrderedDict[str, torch.Size]([(k, v.size()) for k, v in model.items()])
		
		model = torch.cat([p.flatten() for p in model.values()])
		
		total_size = model.numel()
		segment_size = total_size // self.k
		remainder_size = total_size % self.k
		
		segments = []
		start = 0
		for i in range(self.k):
			extra = 1 if i < remainder_size else 0
			end = start + segment_size + extra
			segments.append(model[start:end])
			start = end
		
		return segments
	
	@override
	async def de_segment(self, segmented_model: list[torch.Tensor]) -> dict[str, torch.Tensor]:
		import torch
		
		segmented_model = torch.cat(segmented_model)
		
		offset = 0
		model = {}
		for param_name, param_size in self.model_params_specs.items():
			numel = param_size.numel()
			model[param_name] = segmented_model[offset:offset + numel].view(param_size)
			offset += numel
		
		return model


class AlwaysDictSegmenterDecorator(Segmenter):
	def __init__(self, segmenter: Segmenter, expects_list_only=None, always_str_key=False, str_keys: dict[str, Any] | None = None):
		self._segmenter = segmenter
		self._expects_list_only = expects_list_only
		self._always_str_key = always_str_key
		self._str_keys = str_keys
	
	@override
	async def segment(self, model: Any) -> dict[Any, Any]:
		segments = await self._segmenter.segment(model)
		if isinstance(segments, list):
			if self._expects_list_only is None:
				self._expects_list_only = True
			
			segments = OrderedDict[int, Any](sorted({k: v for k, v in enumerate(segments)}.items()))
		
		if self._always_str_key:
			if self._str_keys is None:
				self._str_keys = {str(k): k for k, v in segments.items()}
			segments = {str(k): v for k, v in segments.items()}
		
		return segments
	
	@override
	async def de_segment(self, segmented_model: dict[Any, Any]) -> Any:
		if self._always_str_key:
			segmented_model = {self._str_keys[k]: v for k, v in segmented_model.items()}
		
		if self._expects_list_only:
			segmented_model = sorted(segmented_model.items())
			segmented_model = [(int(k), v)[1] for k, v in segmented_model]
		
		return await self._segmenter.de_segment(segmented_model)

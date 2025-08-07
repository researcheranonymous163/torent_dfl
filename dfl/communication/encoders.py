import asyncio
import io
import pickle
import tempfile
from pathlib import Path
from typing import Any, override, Iterable, Reversible
from zipfile import ZipFile

import aiofiles

from dfl.extensions.zipfile import zip_directory
from . import Meta


class Encoder:
	async def encode(self, meta: Meta | None, data, **kwargs) -> (Meta | None, Any):
		raise NotImplementedError
	
	async def decode(self, meta: Meta | None, data, **kwargs) -> (Meta | None, Any):
		raise NotImplementedError


class NopEncoder(Encoder):
	async def encode(self, meta: Meta | None, data, **kwargs) -> (Meta | None, Any):
		return meta, data
	
	async def decode(self, meta: Meta | None, data, **kwargs) -> (Meta | None, Any):
		return meta, data


class ForceTypeEncoder(Encoder):
	@override
	def __init__(self, t1, t2=None) -> None:
		self._t1 = t1
		if t2 is None:
			t2 = t1
		self._t2 = t2
	
	@override
	async def encode(self, meta: Meta | None, data, **kwargs) -> (Meta | None, Any):
		if not isinstance(data, self._t1):
			raise TypeError(f"Unknown data type `{type(data)}`; expected `{self._t1}`.")
		return meta, data
	
	@override
	async def decode(self, meta: Meta | None, data, **kwargs) -> (Meta | None, Any):
		if self._t2 is not None and not isinstance(data, self._t2):
			raise TypeError(f"Unknown data type `{type(data)}`; expected `{self._t2}`.")
		return meta, data


class PathBytesEncoder(Encoder):
	@override
	def __init__(self, default_temp_dir_path: Path = None) -> None:
		self._default_temp_dir_path = default_temp_dir_path
	
	@override
	async def encode(self, meta: Meta | None, data, **kwargs) -> (Meta | None, Any):
		if isinstance(data, Path):
			if meta is None:
				meta = dict()
			else:
				meta = meta.copy()
			
			if 'content-transfer-encoding' not in meta:
				meta['content-transfer-encoding'] = []
			ctes = meta['content-transfer-encoding']
			
			if await asyncio.to_thread(data.is_file):
				ctes.append('file-as-bytes')
				async with aiofiles.open(data, mode='rb') as f:
					data = await f.read()
			elif await asyncio.to_thread(data.is_dir):
				ctes.append('directory-as-bytes')
				buff = io.BytesIO()
				with ZipFile(buff, mode='w') as z:
					await asyncio.to_thread(zip_directory, data, z)
				data = buff.getvalue()
			else:
				raise ValueError(f"Neither file nor directory path: {data}.")
		
		return meta, data
	
	@override
	async def decode(self, meta: Meta | None, data, path: Path | None = None, **kwargs) -> (Meta | None, Any):
		if data is not None and meta is not None:
			ctes: list | None = meta.get('content-transfer-encoding', None)
			if ctes is not None and len(ctes) > 0:
				cte = ctes[-1]
				
				if cte == 'file-as-bytes':
					ctes.pop()
					
					if path is not None:
						async with aiofiles.open(path, mode='wb') as f:
							await f.write(data)
						data = path
					else:
						async with aiofiles.tempfile.NamedTemporaryFile(delete=False, dir=self._default_temp_dir_path) as f:
							await f.write(data)
							data = Path(f.name)
				elif cte == 'directory-as-bytes':
					ctes.pop()
					
					if path is None:
						with await asyncio.to_thread(tempfile.TemporaryDirectory, delete=False, dir=self._default_temp_dir_path) as d:
							path = Path(d)
					
					with ZipFile(io.BytesIO(data)) as z:
						await asyncio.to_thread(z.extractall, path)
					
					data = path
		
		return meta, data


class PickleBytesEncoder(Encoder):
	@override
	async def encode(self, meta: Meta | None, data, **kwargs) -> (Meta | None, Any):
		if data is not None and not isinstance(data, bytes):
			if meta is None:
				meta = dict()
			else:
				meta = meta.copy()
			
			if 'content-transfer-encoding' not in meta:
				meta['content-transfer-encoding'] = []
			
			ctes = meta['content-transfer-encoding']
			ctes.append('pickle')
			
			data = pickle.dumps(data)
		
		return meta, data
	
	@override
	async def decode(self, meta: Meta | None, data, **kwargs) -> (Meta | None, Any):
		if data is not None and meta is not None:
			ctes: list | None = meta.get('content-transfer-encoding', None)
			if ctes is not None and len(ctes) > 0 and ctes[-1] == 'pickle':
				ctes.pop()
				data = pickle.loads(data)
		
		return meta, data


class ChainEncoder(Encoder):
	def __init__(self, encoders_chain: Iterable[Encoder]) -> None:
		self._encoders_chain = encoders_chain
	
	@override
	async def encode(self, meta: Meta | None, data, **kwargs) -> (Meta | None, Any):
		for encoder in self._encoders_chain:
			meta, data = await encoder.encode(meta, data, **kwargs)
		return meta, data
	
	@override
	async def decode(self, meta: Meta | None, data, **kwargs) -> (Meta | None, Any):
		if isinstance(self._encoders_chain, Reversible):
			reversed_encoders_chain = reversed(self._encoders_chain)
		else:
			reversed_encoders_chain = reversed(list(self._encoders_chain))
		
		for encoder in reversed_encoders_chain:
			meta, data = await encoder.decode(meta, data, **kwargs)
		return meta, data


class BytesPathEncoder(Encoder):
	@override
	def __init__(self, default_temp_dir_path: Path = None) -> None:
		self._default_temp_dir_path = default_temp_dir_path
	
	@override
	async def encode(self, meta: Meta | None, data, path: Path | None = None, **kwargs) -> (Meta | None, Any):
		if isinstance(data, bytes):
			if meta is None:
				meta = dict()
			else:
				meta = meta.copy()
			
			if 'content-transfer-encoding' not in meta:
				meta['content-transfer-encoding'] = []
			
			ctes = meta['content-transfer-encoding']
			ctes.append('bytes-as-file')
			
			if path is not None:
				async with aiofiles.open(path, mode='wb') as f:
					await f.write(data)
				data = path
			else:
				async with aiofiles.tempfile.NamedTemporaryFile(delete=False, dir=self._default_temp_dir_path) as f:
					await f.write(data)
					data = Path(f.name)
		
		return meta, data
	
	@override
	async def decode(self, meta: Meta | None, data, **kwargs) -> (Meta | None, Any):
		if data is not None and meta is not None:
			ctes: list | None = meta.get('content-transfer-encoding', None)
			if ctes is not None and len(ctes) > 0:
				cte = ctes[-1]
				if cte == 'bytes-as-file':
					ctes.pop()
					async with aiofiles.open(data, mode='rb') as f:
						data = await f.read()
		
		return meta, data


class PathEncoder(BytesPathEncoder):
	@override
	def __init__(self, default_temp_dir_path: Path = None, bytes_encoder: Encoder | None = None) -> None:
		super().__init__(default_temp_dir_path)
		
		if bytes_encoder is None:
			bytes_encoder = PickleBytesEncoder()
		self._bytes_encoder = bytes_encoder
	
	@override
	async def encode(self, meta: Meta | None, data, path: Path | None = None, **kwargs) -> (Meta | None, Any):
		if not isinstance(data, Path):
			meta, data = await self._bytes_encoder.encode(meta, data)
			meta, data = await super().encode(meta, data)
		
		return meta, data
	
	@override
	async def decode(self, meta: Meta | None, data, **kwargs) -> (Meta | None, Any):
		meta, data = await super().decode(meta, data)
		meta, data = await self._bytes_encoder.decode(meta, data)
		
		return meta, data


class PathOrBytesEncoder(Encoder):
	def __init__(self, bytes_encoder: Encoder | None = None) -> None:
		self._bytes_encoder = bytes_encoder
	
	@override
	async def encode(self, meta: Meta | None, data, path: Path | None = None, **kwargs) -> (Meta | None, Any):
		if not isinstance(data, Path):
			meta, data = await self._bytes_encoder.encode(meta, data)
		
		return meta, data
	
	@override
	async def decode(self, meta: Meta | None, data, **kwargs) -> (Meta | None, Any):
		meta, data = await self._bytes_encoder.decode(meta, data)
		
		return meta, data

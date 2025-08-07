import asyncio
import io
import shutil
import tempfile
from collections import defaultdict
from datetime import datetime, timedelta
from os import environ
from pathlib import Path
from typing import override, Any, Literal, Iterable

import aiofiles
import aiofiles.os as aioos
import pyben
from frozendict import frozendict
from qbittorrentapi import Client as QbtClient

import dfl.extensions.os
import dfl.extensions.qbittorrentapi as qbt_api_utils
from dfl.extensions.torrentfile import TorrentFileV2
from . import Communicator
from . import Meta
from .encoders import Encoder, PickleBytesEncoder, ChainEncoder, ForceTypeEncoder, PathOrBytesEncoder, BytesPathEncoder


# TODO: allow multiple subscribe call per torrent (per qbt-hash).
class QbittorrentCommunicator(Communicator):
	@override
	def __init__(
			self,
			name,
			meta_communicator: Communicator,
			client: QbtClient | None = None,
			downloads_dir_path: Path | None = None,
			encoder: Encoder | None = None,
			unsubscribe_strategy: Literal['auto', 'manual', 'immediate'] = 'auto',
			auto_unsubscribe_ttl: timedelta = timedelta(seconds=30),
	) -> None:
		super().__init__(name)
		
		self._meta_communicator = meta_communicator
		self._client = client
		self._downloads_dir_path = downloads_dir_path
		
		self._qbt_encoder = _QbittorrentEncoder(downloads_dir_path, downloads_dir_path)
		if encoder is None:
			encoder = PathOrBytesEncoder(PickleBytesEncoder())
		self._encoder = ChainEncoder((encoder, ForceTypeEncoder(bytes | Path), self._qbt_encoder, ForceTypeEncoder(Path)))
		
		self._unsubscribe_strategy = unsubscribe_strategy
		self._auto_unsubscribe_ttl = auto_unsubscribe_ttl
		
		self._metas_qbt_hashes = dict[frozendict, str]()
	
	@override
	async def __ainit__(self) -> None:
		if self._client is None:
			if 'QBITTORRENTAPI_HOST' in environ or 'PYTHON_QBITTORRENTAPI_HOST' in environ:
				self._client = QbtClient()
			else:  # Enforce defaults. TODO: contribute this to the upstream.
				self._client = QbtClient(host='localhost', port=8080)
			
			await asyncio.to_thread(self._client.auth.log_in)
		
		if self._downloads_dir_path is None:
			with await asyncio.to_thread(tempfile.TemporaryDirectory, delete=False) as d:
				self._downloads_dir_path = Path(d)
		
		if self._unsubscribe_strategy == 'auto':
			self._torrents_last_activities = defaultdict[str, datetime | None](lambda: None)
			self._pool_torrents_last_activities_task = asyncio.create_task(self._pool_torrents_last_activities())
			self._torrents_auto_unsubscribe_tasks: list[asyncio.Task] = []
	
	async def _pool_torrents_last_activities(self) -> None:
		while True:
			data = await asyncio.to_thread(self._client.sync.maindata.delta)
			if 'torrents' in data:
				for qbt_hash, torrent_data in data.torrents.items():
					if 'last_activity' in torrent_data:
						self._torrents_last_activities[qbt_hash] = datetime.fromtimestamp(torrent_data.last_activity)
			
			await asyncio.sleep(1)
	
	async def _torrent_auto_unsubscribe(self, qbt_hash: str) -> None:
		try:
			while True:
				last_activity = self._torrents_last_activities[qbt_hash]
				if last_activity is not None:
					last_activity_delta = datetime.now() - last_activity
					if last_activity_delta > self._auto_unsubscribe_ttl:
						await self._unsubscribe(qbt_hash)
						del self._torrents_last_activities[qbt_hash]
						break
				else:
					last_activity_delta = timedelta()
				
				await asyncio.sleep((self._auto_unsubscribe_ttl - last_activity_delta).total_seconds())
		except asyncio.CancelledError as e:
			await asyncio.shield(self._unsubscribe(qbt_hash))
			del self._torrents_last_activities[qbt_hash]
			
			raise e
	
	@override
	async def publish(self, meta: Meta, data, meta_id: Meta | None = None, **kwargs) -> None:
		if meta_id is None:
			meta_id = meta.copy()
		
		meta, data = await self._encoder.encode(meta, data)
		
		# noinspection PyTypeChecker
		tfv2 = TorrentFileV2(
			path=data,
			align=True,
			progress=0
		)
		torrent_buff = io.BytesIO()
		_, torrent = tfv2.write(outfile=torrent_buff)
		torrent_bytes = torrent_buff.getvalue()
		
		qbt_hash = qbt_api_utils.qbt_hash(torrent)
		self._metas_qbt_hashes[frozendict(meta_id)] = qbt_hash
		
		meta, data = await self._qbt_encoder.encode(meta, data, qbt_hash=qbt_hash, move=True)
		
		added_newly = await asyncio.to_thread(qbt_api_utils.torrents_mash_add, qbt_client=self._client, qbt_hash=qbt_hash, torrent_files=torrent_bytes, save_path=data.absolute(), content_layout='NoSubfolder')
		assert added_newly
		
		await qbt_api_utils.torrents_wait_add(self._client, qbt_hash)
		await qbt_api_utils.torrents_wait_finish(self._client, qbt_hash)
		
		await self._meta_communicator.publish(meta, torrent_bytes, meta_id)
	
	@override
	async def subscribe(self, meta: Meta, meta_only=False, wait_publish=True, files: Iterable[Path | str] | None = None, **kwargs) -> (Meta, Any):
		frozen_meta = frozendict(meta)
		
		meta, torrent_bytes = await self._meta_communicator.subscribe(meta, meta_only, wait_publish)
		if meta_only:
			return meta, None
		
		torrent = pyben.loads(torrent_bytes)
		qbt_hash = qbt_api_utils.qbt_hash(torrent)
		self._metas_qbt_hashes[frozen_meta] = qbt_hash
		
		torrent_dir = self._downloads_dir_path / qbt_hash
		await asyncio.to_thread(torrent_dir.mkdir)
		
		torrent_file_tree = torrent['info']['file tree']
		
		if files is None:
			# TODO: `NoSubfolder` is not defined in the library's type-hint; instead, the wrong `NoSubFolder` is defined! Open-source contribute...
			added_newly = await asyncio.to_thread(qbt_api_utils.torrents_mash_add, qbt_client=self._client, qbt_hash=qbt_hash, torrent_files=torrent_bytes, save_path=torrent_dir.absolute(), content_layout='NoSubfolder')
			assert added_newly
			
			await qbt_api_utils.torrents_wait_add(self._client, qbt_hash)
			await qbt_api_utils.torrents_wait_finish(self._client, qbt_hash)
		else:
			files = {Path(f) if not isinstance(f, Path) else f for f in files}
			available_files = qbt_api_utils.flatten_file_tree(torrent_file_tree)
			
			selected_files_entries: set[tuple[int, Path]] = set()
			for file_id, file in enumerate(available_files):
				if file in files:
					selected_files_entries.add((file_id, file))
			selected_files = {file for _, file in selected_files_entries}
			
			missing_requested_files = files - selected_files
			if len(missing_requested_files) > 0:
				raise ValueError(f"Missing requested files: {', '.join([str(file) for file in missing_requested_files])}.")
			
			selected_files_ids = {file_id for file_id, _ in selected_files_entries}
			
			added_newly = await asyncio.to_thread(qbt_api_utils.torrents_mash_add, qbt_client=self._client, qbt_hash=qbt_hash, torrent_files=torrent_bytes, save_path=torrent_dir.absolute(), content_layout='NoSubfolder', is_stopped=True)
			assert added_newly
			
			await qbt_api_utils.torrents_wait_add(self._client, qbt_hash)
			await asyncio.to_thread(self._client.torrents.file_priority, qbt_hash, list(range(len(available_files))), priority=0)
			await asyncio.to_thread(self._client.torrents.file_priority, qbt_hash, selected_files_ids, priority=1)
			
			await qbt_api_utils.torrents_mash_start(self._client, qbt_hash)
			
			await qbt_api_utils.torrents_wait_finish(self._client, qbt_hash)
		
		meta, data = await self._encoder.decode(meta, torrent_dir)
		
		match self._unsubscribe_strategy:
			case 'immediate':
				await self._unsubscribe(qbt_hash)
			case 'auto':
				self._torrents_auto_unsubscribe_tasks.append(asyncio.create_task(self._torrent_auto_unsubscribe(qbt_hash)))
			case 'manual':
				pass  # NOP
			case _:
				raise ValueError(f"Invalid unsubscribe strategy: {self._unsubscribe_strategy}.")
		
		return meta, data
	
	async def unsubscribe(self, meta: Meta) -> None:
		assert self._unsubscribe_strategy == 'manual'
		
		frozen_meta = frozendict(meta)
		
		qbt_hash = self._metas_qbt_hashes.get(frozen_meta, None)
		if qbt_hash is None:
			_, torrent_bytes = await self._meta_communicator.subscribe(meta, wait_publish=False)
			torrent = pyben.loads(torrent_bytes)
			qbt_hash = qbt_api_utils.qbt_hash(torrent)
		
		await self._unsubscribe(qbt_hash)
	
	async def _unsubscribe(self, qbt_hash: str) -> None:
		await asyncio.to_thread(self._client.torrents.delete, torrent_hashes=qbt_hash, delete_files=False)
		await asyncio.to_thread(shutil.rmtree, self._downloads_dir_path / qbt_hash)
	
	@override
	async def unpublish(self, meta: Meta, **kwargs) -> None:
		frozen_meta = frozendict(meta)
		qbt_hash = self._metas_qbt_hashes.get(frozen_meta, None)
		if qbt_hash is None:
			_, torrent_bytes = await self._meta_communicator.subscribe(meta, wait_publish=False)
			torrent = pyben.loads(torrent_bytes)
			qbt_hash = qbt_api_utils.qbt_hash(torrent)
		
		await self._meta_communicator.unpublish(meta)
		
		await self._unsubscribe(qbt_hash)
	
	@override
	async def close(self) -> None:
		if self._unsubscribe_strategy == 'auto':
			self._pool_torrents_last_activities_task.cancel()
			_ = await asyncio.gather(self._pool_torrents_last_activities_task, return_exceptions=True)
			
			for t in self._torrents_auto_unsubscribe_tasks:
				t.cancel()
			_ = await asyncio.gather(*self._torrents_auto_unsubscribe_tasks, return_exceptions=True)
		
		async with asyncio.TaskGroup() as tg:
			tg.create_task(asyncio.to_thread(self._client.auth.log_out))
			tg.create_task(self._meta_communicator.close())


class _QbittorrentEncoder(Encoder):
	def __init__(self, downloads_dir_path: Path, default_temp_dir_path: Path | None = None) -> None:
		self._downloads_dir_path = downloads_dir_path
		self._default_temp_dir_path = default_temp_dir_path
		
		self._mock_bytes_path_encoder = BytesPathEncoder()
	
	@override
	async def encode(self, meta: Meta | None, data: Path | bytes, qbt_hash: str | None = None, move=False, **kwargs) -> (Meta | None, Path):
		if qbt_hash is not None:
			torrent_dir = self._downloads_dir_path / qbt_hash
			await asyncio.to_thread(torrent_dir.mkdir)
		else:
			with await asyncio.to_thread(tempfile.TemporaryDirectory, delete=False, dir=self._default_temp_dir_path) as d:
				torrent_dir = Path(d)
		
		if isinstance(data, Path) and await asyncio.to_thread(data.is_dir):
			if not move:
				await asyncio.to_thread(dfl.extensions.os.link_recursive, data, torrent_dir, exist_ok=True)
			else:
				if await asyncio.to_thread(torrent_dir.exists):
					for item in await asyncio.to_thread(data.iterdir):
						await asyncio.to_thread(shutil.move, item, torrent_dir / item.name)
					await aioos.rmdir(data)
				else:
					await asyncio.to_thread(shutil.move, data, torrent_dir)
		else:
			if isinstance(data, bytes):
				dest = torrent_dir / 'data.bin'
				meta, _ = await self._mock_bytes_path_encoder.encode(meta, data, path=dest)
			elif isinstance(data, Path):
				dest = torrent_dir / data.name
				if not move:
					await aioos.link(data, dest)
				else:
					await asyncio.to_thread(shutil.move, data, dest)
			else:
				raise TypeError
			
			if meta is None:
				meta = dict()
			else:
				meta = meta.copy()
			
			if 'content-transfer-encoding' not in meta:
				meta['content-transfer-encoding'] = []
			ctes = meta['content-transfer-encoding']
			ctes.append('single-file-in-directory')
		
		data = torrent_dir
		return meta, data
	
	@override
	async def decode(self, meta: Meta | None, data: Path, path: Path | None = None, **kwargs) -> (Meta | None, Path):
		ctes: list | None = meta.get('content-transfer-encoding', None)
		if ctes is not None and len(ctes) > 0 and ctes[-1] == 'single-file-in-directory':
			ctes.pop()
			
			single_file = next(iter(await aioos.listdir(data)))
			data = data / single_file
		
		meta, data = await self._mock_bytes_path_encoder.decode(meta, data)
		if isinstance(data, bytes):
			pass  # NOP
		elif isinstance(data, Path):
			if await asyncio.to_thread(data.is_dir):
				if path is None:
					with await asyncio.to_thread(tempfile.TemporaryDirectory, delete=False, dir=self._default_temp_dir_path) as d:
						path = Path(d)
				
				await asyncio.to_thread(dfl.extensions.os.link_recursive, data, path, exist_ok=True)
				data = path
			else:
				if path is None:
					async with aiofiles.tempfile.NamedTemporaryFile(delete=False, dir=self._default_temp_dir_path) as f:
						path = Path(f.name)
						await aioos.remove(path)
				
				await aioos.link(data, path)
				data = path
		else:
			raise TypeError
		
		return meta, data

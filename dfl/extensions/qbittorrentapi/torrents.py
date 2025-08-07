import asyncio
import logging
from typing import Iterable

import qbittorrentapi as qbt_api

from .exceptions import TorrentsAddGenericNotOkError

_logger = logging.getLogger('dfl.extensions.qbittorrentapi')


def mash_add(qbt_client: qbt_api.Client, qbt_hash: str | None = None, **kwargs) -> bool:
	if qbt_hash is None:
		# TODO
		raise NotImplementedError("Mash-adding without externally providing the qBittorrent hash is not yet supported.")
	
	try:
		qbt_client.torrents.add(**kwargs)
		return True
	except TorrentsAddGenericNotOkError:
		if exists(qbt_client, qbt_hash):
			return False
		else:
			raise


def exists(qbt_client: qbt_api.Client, qbt_hash: str) -> bool:
	try:
		_ = qbt_client.torrents.properties(torrent_hash=qbt_hash)
		return True
	except qbt_api.NotFound404Error:
		return False


async def mash_start(qbt_client: qbt_api.Client, qbt_hash: str):
	while True:
		await asyncio.to_thread(qbt_client.torrents.start, qbt_hash)
		
		info = (await asyncio.to_thread(qbt_client.torrents.info.all, torrent_hashes=qbt_hash))[0]
		
		state = qbt_api.TorrentState(info['state'])
		if state == qbt_api.TorrentState.CHECKING_RESUME_DATA:
			pass
		elif state.is_stopped:
			_logger.warning(f"The just-resumed torrent is still paused! Will retry resuming.")
		else:
			break
		
		# TODO: any way to go around this?
		await asyncio.sleep(1)


async def wait_finish(qbt_client: qbt_api.Client, qbt_hash: str, files_ids: int | Iterable[int] | None = None):
	if files_ids is not None and isinstance(files_ids, int):
		files_ids = (files_ids,)
	
	# Polling-wait for the download to finish.
	while True:
		if files_ids is None:
			info = (await asyncio.to_thread(qbt_client.torrents.info.all, torrent_hashes=qbt_hash))[0]
			progress = info['progress']
		else:
			# TODO: this method also accepts an `indexes` argument, but it's not in the library; open-source contribute...
			files = await asyncio.to_thread(qbt_client.torrents.files, qbt_hash)
			progress_size = 0
			total_size = 0
			for file in files:
				if file['index'] not in files_ids:
					continue
				total_size += file['size']
				progress_size += file['progress'] * file['size']
			
			files_indices = [file['index'] for file in files]
			assert not any([id_ not in files_indices for id_ in files_ids])
			
			progress = progress_size / total_size
		
		if progress == 1:
			break
		
		await asyncio.sleep(1)


# TODO: migrate to a reactive/proactive solution instead of polling.
async def wait_add(qbt_client: qbt_api.Client, qbt_hash: str):
	while True:
		if await asyncio.to_thread(exists, qbt_client, qbt_hash):
			break
		
		await asyncio.sleep(1)

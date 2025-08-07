import qbittorrentapi as qbt_api

import dfl.extensions.qbittorrentapi.exceptions
from .file_tree import flatten_file_tree
from .hash import qbt_hash
from .torrents import exists as torrents_exists, mash_add as torrents_mash_add, mash_start as torrents_mash_start, wait_add as torrents_wait_add, wait_finish as torrents_wait_finish

_org_torrents_add = qbt_api.Client.torrents_add


def _patch_torrents_add(self, **kwargs) -> None:
	res = _org_torrents_add(self, **kwargs)
	if res != 'Ok.':
		raise exceptions.TorrentsAddGenericNotOkError(f"Failed to add torrent: \"{res}\".")


qbt_api.Client.torrents_add = _patch_torrents_add

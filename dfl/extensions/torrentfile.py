import torrentfile.torrent
from torrentfile.torrent import TorrentFileV2

_org_init = TorrentFileV2


# TODO: contribute this fix to the up-stream.
def _patch_init(**kwargs):
	v = _org_init(**kwargs)
	if 'length' in v.meta['info']:
		del v.meta['info']['length']
	return v


TorrentFileV2 = _patch_init
torrentfile.torrent.TorrentFileV2 = _patch_init

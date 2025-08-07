import hashlib

import pyben


def qbt_hash(meta: dict) -> str:
	bencoded_meta_info = pyben.dumps(meta['info'])
	# TODO: also support v1.
	v2_info_hash = hashlib.sha256(bencoded_meta_info).digest()
	
	return v2_info_hash.hex()[:40]

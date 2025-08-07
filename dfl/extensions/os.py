import os
from pathlib import Path


def link_recursive(src, dst, exist_ok=False):
	Path(dst).mkdir(exist_ok=exist_ok)
	
	for root, dirs, files in os.walk(src):
		rel_path = os.path.relpath(root, src)
		dst_path = os.path.join(dst, rel_path)
		
		if os.path.normpath(dst_path) != os.path.normpath(dst):
			os.makedirs(dst_path, exist_ok=False)
		
		for file in files:
			src_file = os.path.join(root, file)
			dst_file = os.path.join(dst_path, file)
			os.link(src_file, dst_file)

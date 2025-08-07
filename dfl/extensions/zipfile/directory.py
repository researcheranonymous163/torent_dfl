import os
import zipfile
from pathlib import Path


def zip_directory(path: Path, zip_file: zipfile.ZipFile, include_self=False):
	for root, _, files in path.walk():
		for file in files:
			file = os.path.join(root, file)
			if include_self:
				zip_file.write(file, os.path.relpath(file, os.path.join(path, '..')))
			else:
				zip_file.write(file, os.path.relpath(file, path))

from pathlib import Path


def flatten_file_tree(file_tree: dict) -> list[Path]:
	if len(file_tree) == 1 and list(file_tree.keys())[0] == '':
		return []
	
	contents = []
	for file, file_file_tree in file_tree.items():
		file = Path(file)
		
		contents.append(file)
		
		file_file_tree_contents = flatten_file_tree(file_file_tree)
		contents += [file / file_file for file_file in file_file_tree_contents]
	
	return contents

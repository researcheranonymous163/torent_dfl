def pretty_format(fs):
	items = (f"{repr(it)}" for it in fs)
	return "{" + ", ".join(items) + "}"

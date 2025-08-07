from frozendict import frozendict


def new_repr(self):
	items = (f"{repr(k)}: {repr(v)}" for k, v in self.items())
	return "{" + ", ".join(items) + "}"


frozendict.__repr__ = new_repr

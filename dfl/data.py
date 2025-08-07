import random
from random import Random
from typing import Any

from sklearn.model_selection import train_test_split


def stratified_split(*arrays, labels, n_splits: int, rnd: Random | None = None) -> tuple[Any, ...]:
	if n_splits == 1:
		return arrays
	else:
		splits = train_test_split(
			*arrays, labels,
			train_size=1 / n_splits,
			# Found the following range from its respective error message if passed a random float.
			random_state=rnd.randint(0, 4294967295) if rnd is not None else random.randint(0, 4294967295),
			stratify=labels
		)
		
		labels_split, labels_rest = splits[-2:]
		arrays_splits = splits[:-2]
		
		return *arrays_splits[::2], *stratified_split(*arrays_splits[1::2], labels=labels_rest, n_splits=n_splits - 1, rnd=rnd)

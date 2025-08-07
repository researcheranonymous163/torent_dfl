import asyncio
import logging
import traceback
from typing import Iterator, AsyncIterator


def _next_anext(it: Iterator):
	try:
		return next(it)
	except StopIteration:
		raise StopAsyncIteration


async def iter_to_aiter(it: Iterator) -> AsyncIterator:
	try:
		while True:
			item = await asyncio.to_thread(_next_anext, it)
			yield item
	except StopAsyncIteration:
		return


async def shield_exception(coro, logger: logging.Logger | None = None):
	try:
		await coro
	except Exception as e:
		if logger is not None:
			logger.exception(e)
		else:
			traceback.print_exception(e)

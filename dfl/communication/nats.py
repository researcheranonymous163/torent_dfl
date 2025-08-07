import asyncio
import hashlib
import json
from typing import override, Callable, Any

import nats
import nats.js
import nats.js.errors
from nats.js.api import StreamConfig, ConsumerConfig

from dfl.communication.encoders import Encoder, PickleBytesEncoder, ChainEncoder, ForceTypeEncoder
from . import Communicator, Meta


# TODO: support uni-cast by listening on bare `dfl.nodes.<name>`.
class NatsCommunicator(Communicator):
	@override
	def __init__(
			self,
			name: str,
			client: nats.NATS | None = None,
			js_client: nats.js.JetStreamContext | None = None,
			meta_hasher: Callable[[Meta], str] | None = None,
			encoder: Encoder | None = None
	) -> None:
		super().__init__(name)
		
		if meta_hasher is None:
			meta_hasher = NatsCommunicator._hash
		
		self._client = client
		self._js_client = js_client
		self._meta_hasher = meta_hasher
		
		if encoder is None:
			encoder = PickleBytesEncoder()
		self._encoder = ChainEncoder((encoder, ForceTypeEncoder(bytes)))
	
	@override
	async def __ainit__(self) -> None:
		await super().__ainit__()
		
		if self._client is None:
			self._client = nats.connect()
		
		if self._js_client is None:
			self._js_client = self._client.jetstream(timeout=None)
		
		stream_name = 'dfl-publications'
		stream_config = StreamConfig(
			name=stream_name,
			subjects=['dfl.publications.*'],
			max_msgs_per_subject=1
		)
		try:
			await self._js_client.stream_info(stream_name)
			await self._js_client.update_stream(stream_config)
		except nats.js.errors.NotFoundError:
			await self._js_client.add_stream(stream_config)
	
	@override
	async def publish(self, meta: Meta, data, meta_id: Meta | None = None, **kwargs) -> None:
		if meta_id is None:
			meta_id = meta.copy()
		meta, data = await self._encoder.encode(meta, data)
		
		meta_hash = self._meta_hasher(meta_id)
		
		# TODO: why not raising an error in case of duplicate (the message would drop silently)?
		await self._client.publish(
			subject=f'dfl.publications.{meta_hash}',
			payload=data,
			headers={'X_Dfl_Meta_Json': json.dumps(meta)}
		)
	
	@override
	async def subscribe(self, meta: Meta, meta_only=False, wait_publish=True, **kwargs) -> (Meta, Any):
		stream_name = 'dfl-publications'
		
		meta_hash = self._meta_hasher(meta)
		subject = f'dfl.publications.{meta_hash}'
		
		if wait_publish:
			sub = None
			try:
				# TODO: manual delete the ephemeral consumer.
				sub = await self._js_client.pull_subscribe(subject=subject, config=ConsumerConfig(headers_only=meta_only))
				msg = (await sub.fetch(timeout=None))[0]
			finally:
				if sub is not None:
					await sub.unsubscribe()
		else:
			# TODO: might not need to pull the data if the `meta_only` argument is set.
			msg = await self._js_client.get_last_msg(stream_name=stream_name, subject=subject)
		
		meta, data = json.loads(msg.header['X_Dfl_Meta_Json']), msg.data
		
		if meta_only:
			data = None
		else:
			meta, data = await self._encoder.decode(meta, data)
		
		return meta, data
	
	@override
	async def unpublish(self, meta: Meta, **kwargs) -> None:
		stream_name = 'dfl-publications'
		meta_hash = self._meta_hasher(meta)
		subject = f'dfl.publications.{meta_hash}'
		
		# TODO: only need the sequence number of the message; no need to pull the data.
		msg = await self._js_client.get_last_msg(stream_name=stream_name, subject=subject)
		deleted = await self._js_client.delete_msg(stream_name, msg.seq)
		if not deleted:
			raise FileNotFoundError
	
	@override
	async def close(self) -> None:
		await super().close()
		
		# noinspection PyProtectedMember
		_orig_closed_cb = self._client._closed_cb
		
		close_event = asyncio.Event()
		
		async def _closed_cb():
			close_event.set()
		
		try:
			self._client._closed_cb = _closed_cb
			await self._client.drain()
			await close_event.wait()
		finally:
			self._client._closed_cb = _orig_closed_cb
	
	@staticmethod
	def _hash(meta: Meta) -> str:
		return hashlib.sha256(json.dumps(meta, sort_keys=True).encode('UTF-8')).hexdigest()

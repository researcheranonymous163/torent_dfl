from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Message(_message.Message):
    __slots__ = ("header", "body")
    HEADER_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    header: Header
    body: bytes
    def __init__(self, header: _Optional[_Union[Header, _Mapping]] = ..., body: _Optional[bytes] = ...) -> None: ...

class Header(_message.Message):
    __slots__ = ("json_string",)
    JSON_STRING_FIELD_NUMBER: _ClassVar[int]
    json_string: str
    def __init__(self, json_string: _Optional[str] = ...) -> None: ...

class Subscribe(_message.Message):
    __slots__ = ("header", "wait_publish", "meta_only")
    HEADER_FIELD_NUMBER: _ClassVar[int]
    WAIT_PUBLISH_FIELD_NUMBER: _ClassVar[int]
    META_ONLY_FIELD_NUMBER: _ClassVar[int]
    header: Header
    wait_publish: bool
    meta_only: bool
    def __init__(self, header: _Optional[_Union[Header, _Mapping]] = ..., wait_publish: bool = ..., meta_only: bool = ...) -> None: ...

class Receive(_message.Message):
    __slots__ = ("header", "meta_only")
    HEADER_FIELD_NUMBER: _ClassVar[int]
    META_ONLY_FIELD_NUMBER: _ClassVar[int]
    header: Header
    meta_only: bool
    def __init__(self, header: _Optional[_Union[Header, _Mapping]] = ..., meta_only: bool = ...) -> None: ...

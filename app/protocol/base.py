from dataclasses import dataclass
from typing import Optional


@dataclass
class Header:
    _bytes: bytes

    def to_bytes(self) -> bytes:
        return self._bytes

    def __repr__(self):
        return self._bytes.hex()


@dataclass
class Content:
    _bytes: bytes

    def to_bytes(self) -> bytes:
        return self._bytes

    def __repr__(self):
        return self._bytes.hex()


@dataclass
class Message:
    # Ref.: https://kafka.apache.org/protocol.html#protocol_common
    # RequestOrResponse => Size (RequestMessage | ResponseMessage)
    #   Size => int32
    size: Optional[int]
    header: Header
    content: Content

    def __post_init__(self):
        if self.size is None:
            self.size = len(self.header.to_bytes() + self.content.to_bytes())

    def to_bytes(self) -> bytes:
        return self.size.to_bytes(4, byteorder="big") + self.header.to_bytes() + self.content.to_bytes()

    def __repr__(self):
        return (f"Message: {self.to_bytes().hex()}\n"
                f"\tSize: {self.size}\n"
                f"\tHeader: {self.header}\n"
                f"\tContent: {self.content}")

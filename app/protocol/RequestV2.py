from dataclasses import dataclass
from .base import Message, Content
from .headers.RequestHeaderV2 import RequestHeaderV2


@dataclass
class RequestV2(Message):
    _bytes: bytes
    header: RequestHeaderV2

    def __init__(self, _bytes):
        self._bytes = _bytes
        size = int.from_bytes(self._bytes[:4], byteorder='big')
        client_id_length = RequestHeaderV2.get_client_id_length(self._bytes[4:])
        header_end_index = 4 + RequestHeaderV2.client_id_length_index + 2 + client_id_length + 1  # No TAG: +1 for num=0
        super().__init__(
            size,
            RequestHeaderV2(self._bytes[4:header_end_index]),
            Content(self._bytes[header_end_index:])
        )

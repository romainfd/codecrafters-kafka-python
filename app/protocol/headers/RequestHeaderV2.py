from dataclasses import dataclass
from ..base import Header


@dataclass
class RequestHeaderV2(Header):
    # Ref.: https://kafka.apache.org/protocol.html#protocol_messages
    # Request Header v2 => request_api_key request_api_version correlation_id client_id TAG_BUFFER
    #   request_api_key => INT16
    #   request_api_version => INT16
    #   correlation_id => INT32
    #   client_id => NULLABLE_STRING
    client_id_length_index = 2 + 2 + 4

    def __post_init__(self):
        self.request_api_key = int.from_bytes(self._bytes[:2], byteorder="big")
        self.request_api_version = int.from_bytes(self._bytes[2:4], byteorder="big")
        self.correlation_id = int.from_bytes(self._bytes[4:8], byteorder="big")
        self.client_id_length = self.get_client_id_length(self._bytes)
        self.client_id = int.from_bytes(self._bytes[10:10 + self.client_id_length], byteorder="big")
        self.tagged_fields = self._bytes[10 + self.client_id_length:]

    @staticmethod
    def get_client_id_length(header_bytes):
        # NULLABLE_STRING: Represents a sequence of characters or null.
        # For non-null strings, first the length N is given as an INT16. Then N bytes follow = UTF-8 character sequence.
        # A null value is encoded with length of -1 and there are no following bytes.
        client_id_length = int.from_bytes(
            header_bytes[RequestHeaderV2.client_id_length_index:RequestHeaderV2.client_id_length_index + 2])
        return 0 if client_id_length == -1 else client_id_length

    def __repr__(self):
        return (f"{self.request_api_key} - {self.request_api_version} - {self.correlation_id} - "
                f"{self.client_id} (len = {self.client_id_length}) - {self.tagged_fields.hex()}")
from dataclasses import dataclass
from .base import Content
from .commons import get_size_representation, TAG_BUFFER


@dataclass
class APIKeysV3:
    # Ref.: https://kafka.apache.org/protocol.html#The_Messages_ApiVersions
    #   api_keys => api_key min_version max_version TAG_BUFFER
    #     api_key => INT16
    #     min_version => INT16
    #     max_version => INT16
    api_key: int
    min_version: int
    max_version: int

    def to_bytes(self):
        return self.api_key.to_bytes(2, byteorder="big") + \
            self.min_version.to_bytes(2, byteorder="big") + \
            self.max_version.to_bytes(2, byteorder="big") + \
            TAG_BUFFER

    def __repr__(self):
        return f"{self.api_key} [{self.min_version} => {self.max_version}]"


@dataclass
class APIVersionsResponseV3(Content):
    # Ref.: https://kafka.apache.org/protocol.html#The_Messages_ApiVersions
    # ApiVersions Response (Version: 3) => error_code [api_keys] throttle_time_ms TAG_BUFFER
    #   error_code => INT16
    #   api_keys => api_key min_version max_version TAG_BUFFER
    #   throttle_time_ms => INT32
    error_code: int
    api_keys: list[APIKeysV3]
    throttle_time_ms: int

    def __post_init__(self):
        self._bytes = self.to_bytes()

    def to_bytes(self):
        # num_api_keys => empirically VARINT of N + 1 for COMPACT_ARRAY
        # 255 / 11111111 => 126
        # 127 / 01111111 => 126
        #  63 / 00111111 =>  62
        return self.error_code.to_bytes(2, byteorder="big") + \
            get_size_representation(self.api_keys).to_bytes(1, byteorder="big") + \
            b''.join(map(lambda k: k.to_bytes(), self.api_keys)) + \
            self.throttle_time_ms.to_bytes(4, byteorder="big") + \
            TAG_BUFFER

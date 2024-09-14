from socket import create_server
from dataclasses import dataclass
from typing import Optional


@dataclass
class Header:
    _bytes: bytes

    def to_bytes(self) -> bytes:
        return self._bytes


@dataclass
class Content:
    _bytes: bytes

    def to_bytes(self) -> bytes:
        return self._bytes


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
        return (f"Message: {self.to_bytes()}\n"
                f"\tSize: {self.size}\n"
                f"\tHeader: {self.header}\n"
                f"\tContent: {self.content}")


@dataclass
class RequestHeaderV2(Header):
    # Ref.: https://kafka.apache.org/protocol.html#protocol_messages
    # Request Header v2 => request_api_key request_api_version correlation_id client_id TAG_BUFFER
    #   request_api_key => INT16
    #   request_api_version => INT16
    #   correlation_id => INT32
    #   client_id => NULLABLE_STRING

    def __post_init__(self):
        self.request_api_key = int.from_bytes(self._bytes[:2], byteorder="big")
        self.request_api_version = int.from_bytes(self._bytes[2:4], byteorder="big")
        self.correlation_id = int.from_bytes(self._bytes[4:8], byteorder="big")
        self.client_id = int.from_bytes(self._bytes[8:], byteorder="big")
        # No tagged fields for this challenge | Ref.: https://app.codecrafters.io/courses/kafka/stages/wa6
        self.tagged_fields = b''

    def __repr__(self):
        return f"{self.request_api_key} - {self.request_api_version} - {self.correlation_id} - {self.client_id}"


@dataclass
class ResponseHeaderV0(Header):
    # Ref.: https://kafka.apache.org/protocol.html#protocol_messages
    # Response Header v0 => correlation_id
    #   correlation_id => INT32
    correlation_id: int

    def __init__(self, correlation_id):
        self.correlation_id = correlation_id
        super().__init__(self.to_bytes())

    def to_bytes(self):
        return self.correlation_id.to_bytes(4, byteorder="big")

    def __repr__(self):
        return f"{self.correlation_id}"


@dataclass
class ResponseHeaderV1(Header):
    # Ref.: https://kafka.apache.org/protocol.html#protocol_messages
    # Response Header v0 => correlation_id TAG_BUFFER
    #   correlation_id => INT32
    correlation_id: int
    # ToDo: handle TAG_BUFFER

    def __init__(self, correlation_id):
        self.correlation_id = correlation_id
        super().__init__(self.to_bytes())

    def to_bytes(self):
        return self.correlation_id.to_bytes(4, byteorder="big")

    def __repr__(self):
        return f"{self.correlation_id}"


@dataclass
class RequestV2(Message):
    _bytes: bytes
    header: RequestHeaderV2

    def __init__(self, _bytes):
        self._bytes = _bytes
        size = int.from_bytes(self._bytes[:4], byteorder='big')
        super().__init__(
            size,
            RequestHeaderV2(self._bytes[4:]),
            Content(b'')
        )


TAG_BUFFER = int(0).to_bytes(1, byteorder="big")  # Empirically...


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
    # no TAG_BUFFER in this challenge

    def to_bytes(self):
        return self.api_key.to_bytes(2, byteorder="big") + \
            self.min_version.to_bytes(2, byteorder="big") + \
            self.max_version.to_bytes(2, byteorder="big") + \
            TAG_BUFFER

    def __repr__(self):
        return f"API key = {self.api_key} [{self.min_version} => {self.max_version}]"


@dataclass
class APIVersionsResponseV3(Content):
    # Ref.: https://kafka.apache.org/protocol.html#The_Messages_ApiVersions
    # ApiVersions Response (Version: 3) => error_code [api_keys] throttle_time_ms TAG_BUFFER
    #   error_code => INT16
    #   throttle_time_ms => INT32
    error_code: int
    api_keys: list[APIKeysV3]
    throttle_time_ms: int

    def __post_init__(self):
        self.api_keys_num = len(self.api_keys)
        self._bytes = self.to_bytes()

    def to_bytes(self):
        # num_api_keys => empirically VARINT of N + 1 for COMPACT_ARRAY
        # 255 / 11111111 => 126
        # 127 / 01111111 => 126
        #  63 / 00111111 =>  62
        # TAG_BUFFER -> empirically INT16
        return self.error_code.to_bytes(2, byteorder="big") + \
            (self.api_keys_num + 1).to_bytes(1, byteorder="big") + \
            b''.join(map(lambda k: k.to_bytes(), self.api_keys)) + \
            self.throttle_time_ms.to_bytes(4, byteorder="big") + \
            TAG_BUFFER


def main():
    server = create_server(("localhost", 9092), reuse_port=True)
    socket, address = server.accept()  # wait for client

    print(f"Client connected: {address}")
    # Receive data from the client
    data = socket.recv(1024)
    print(f"Received data: {data}")
    request = RequestV2(data)
    print(f"Received request: {request}")
    # Error codes | Ref.: https://kafka.apache.org/protocol.html#protocol_error_codes
    error_code = 0 if request.header.request_api_version in [0, 1, 2, 3, 4] else 35
    message = Message(
        None,
        ResponseHeaderV0(request.header.correlation_id),
        APIVersionsResponseV3(
            b'',
            error_code,
            [
                APIKeysV3(18, 4, 4),  # APIVersions
                APIKeysV3(1, 16, 16),  # Fetch
            ],
            0
        )
    )
    print(f"Sending message: {message} as {message.to_bytes().hex()}")
    socket.sendall(message.to_bytes())


if __name__ == "__main__":
    main()

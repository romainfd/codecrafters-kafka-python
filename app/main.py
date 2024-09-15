from socket import create_server
from dataclasses import dataclass
from typing import Optional
from uuid import UUID


# API Key => [supported versions] mapping for our Kafka implementation
# Ref.: https://kafka.apache.org/protocol.html#protocol_api_keys
SUPPORTED_API_KEYS = {
    1: [16],  # FetchRequestV16
    # We don't parse the APIVersionsRequest Content => should only support v0 -> v2
    # Ref.: https://kafka.apache.org/protocol.html#The_Messages_ApiVersions
    # v4 doesn't exist but is the one the challenge is testing for support... => add a fake v4 support for the challenge
    # v4 was soon to be released and included thanks to that
    # Ref.: https://forum.codecrafters.io/t/question-about-handle-apiversions-requests-stage/1743
    # Support is still exaggerated as we don't implement the Request parameters handling
    # (client_software_name and client_software_version)
    18: [0, 1, 2, 4]
}


# No tagged fields for this challenge | Ref.: https://app.codecrafters.io/courses/kafka/stages/wa6
# => Just COMPACT_ARRAY with no elements => 0 on 1 byte (derived from empirical observations)
TAG_BUFFER = int(0).to_bytes(1, byteorder="big")


def get_size_representation(arr):
    # stored as 0 for 0 or N + 1 else
    return len(arr) + (len(arr) > 0)


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
    # Response Header v1 => correlation_id TAG_BUFFER
    #   correlation_id => INT32
    correlation_id: int

    def __init__(self, correlation_id):
        self.correlation_id = correlation_id
        super().__init__(self.to_bytes())

    def to_bytes(self):
        return self.correlation_id.to_bytes(4, byteorder="big") + TAG_BUFFER

    def __repr__(self):
        return f"{self.correlation_id}"


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


@dataclass
class Topic:
    # Ref.: https://kafka.apache.org/protocol.html#The_Messages_Fetch
    # topics = > topic_id [partitions] TAG_BUFFER
    #     topic_id => UUID
    #     partitions => partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes TAG_BUFFER
    #       partition => INT32
    #       current_leader_epoch => INT32
    #       fetch_offset => INT64
    #       last_fetched_epoch => INT32
    #       log_start_offset => INT64
    #       partition_max_bytes => INT32
    topic_id: UUID

    def __init__(self, _bytes):
        # UUID is 16 bytes
        self.topic_id = UUID(bytes=_bytes[:16])


@dataclass
class FetchRequestV16(RequestV2):
    # ToDo: should be moved to Content !
    topics: list[Topic]

    def __init__(self, _bytes):
        super().__init__(
            _bytes
        )
        # Specify Content in more details
        # Ref.: https://kafka.apache.org/protocol.html#The_Messages_Fetch
        # Fetch Request (Version: 16) => max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] rack_id TAG_BUFFER
        #   max_wait_ms => INT32
        #   min_bytes => INT32
        #   max_bytes => INT32
        #   isolation_level => INT8
        #   session_id => INT32
        #   session_epoch => INT32
        #   topics => topic_id [partitions] TAG_BUFFER
        #   forgotten_topics_data => topic_id [partitions] TAG_BUFFER
        #     topic_id => UUID
        #     partitions => INT32
        #   rack_id => COMPACT_STRING

        # ToDo: Add full request implementation
        self.topics = []
        # => skip all initial fields directly to topics_num length info
        offset = 4 + 4 + 4 + 1 + 4 + 4
        topics_num = int.from_bytes(self.content.to_bytes()[offset:offset + 1])
        for i in range(topics_num - 1):
            # ToDo: correctly handle offset for more than 1 topic
            self.topics.append(Topic(self.content.to_bytes()[offset + 1:]))


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


@dataclass
class PartitionsV16:
    # ToDo: implement all fields
    # Ref.: https://kafka.apache.org/protocol.html#The_Messages_Fetch
    #     partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records TAG_BUFFER
    #       partition_index => INT32
    #       error_code => INT16
    #       high_watermark => INT64
    #       last_stable_offset => INT64
    #       log_start_offset => INT64
    #       aborted_transactions => producer_id first_offset TAG_BUFFER
    #         producer_id => INT64
    #         first_offset => INT64
    #       preferred_read_replica => INT32
    #       records => COMPACT_RECORDS
    partition_index: int
    error_code: int

    def to_bytes(self):
        return self.partition_index.to_bytes(4) + \
            self.error_code.to_bytes(2) + \
            int(0).to_bytes(8 + 8 + 8 + 1 + 4 + 1 + 1)


@dataclass
class TopicResponsesV16:
    # Ref.: https://kafka.apache.org/protocol.html#The_Messages_Fetch
    #   responses => topic_id [partitions] TAG_BUFFER
    #     topic_id => UUID
    #     partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records TAG_BUFFER
    topic_id: UUID
    partitions: list[PartitionsV16]

    def to_bytes(self):
        return self.topic_id.bytes + \
            get_size_representation(self.partitions).to_bytes(1, byteorder="big") + \
            b''.join(map(lambda k: k.to_bytes(), self.partitions)) + \
            TAG_BUFFER


@dataclass
class FetchResponseV16(Content):
    # Ref.: https://kafka.apache.org/protocol.html#The_Messages_Fetch
    # Fetch Response (Version: 16) => throttle_time_ms error_code session_id [responses] TAG_BUFFER
    #   throttle_time_ms => INT32
    #   error_code => INT16
    #   session_id => INT32
    #   responses => topic_id [partitions] TAG_BUFFER
    throttle_time_ms: int
    error_code: int
    session_id: int
    responses: list[TopicResponsesV16]

    def __post_init__(self):
        self._bytes = self.to_bytes()

    def to_bytes(self):
        return self.throttle_time_ms.to_bytes(4, byteorder="big") + \
            self.error_code.to_bytes(2, byteorder="big") + \
            self.session_id.to_bytes(4, byteorder="big") + \
            get_size_representation(self.responses).to_bytes(1, byteorder="big") + \
            b''.join(map(lambda k: k.to_bytes(), self.responses)) + \
            TAG_BUFFER


def main():
    server = create_server(("localhost", 9092), reuse_port=True)
    socket, address = server.accept()  # wait for client

    print(f"Client connected: {address}")
    # Receive data from the client
    data = socket.recv(1024)
    request = RequestV2(data)
    print(f"Received request: {request}")
    # Error codes | Ref.: https://kafka.apache.org/protocol.html#protocol_error_codes
    # Request API key | Ref.: https://kafka.apache.org/protocol.html#protocol_api_keys
    if request.header.request_api_key == 18:  # API Versions
        # v0, v1, v2 and v4
        error_code = 0 if request.header.request_api_version in SUPPORTED_API_KEYS[18] else 35
        # APIVersionsResponseV3 uses ResponseHeaderV0 even though it is a flexible version for back-compatibility
        # Ref.: https://github.com/apache/kafka/blob/3.8.0/clients/src/main/resources/common/message/ApiVersionsResponse.json#L24-L26
        header = ResponseHeaderV0(request.header.correlation_id)
        content = APIVersionsResponseV3(
            b'',
            error_code,
            [
                APIKeysV3(18, 4, 4),  # API Versions
                APIKeysV3(1, 16, 16),  # Fetch
            ],
            0
        )
    elif request.header.request_api_key == 1:  # Fetch
        # v16 only
        error_code = 0 if request.header.request_api_version in SUPPORTED_API_KEYS[1] else 35
        # FetchResponseV16 is a flexible version => ResponseHeaderV1
        # Ref.: https://github.com/apache/kafka/blob/3.8.0/clients/src/main/resources/common/message/FetchResponse.json#L51
        header = ResponseHeaderV1(request.header.correlation_id)
        # Parse Request Content
        request = FetchRequestV16(data)
        print(f"Parsed request: {request}")
        if len(request.topics) == 0:
            content = FetchResponseV16(
                b'',
                1,
                error_code,
                0,
                []
            )
        else:
            # Stage "Fetch with an unknown topic"
            content = FetchResponseV16(
                b'',
                1,
                error_code,
                0,
                [
                    TopicResponsesV16(
                        request.topics[0].topic_id,
                        [PartitionsV16(0, 100)]
                    )
                ]
            )
    else:
        raise NotImplementedError
    response = Message(
        None,
        header,
        content
    )
    print(f"Sending message: {response} as {response.to_bytes().hex()}")
    socket.sendall(response.to_bytes())


if __name__ == "__main__":
    main()

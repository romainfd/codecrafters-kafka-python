from dataclasses import dataclass
from uuid import UUID
from .base import Content
from .commons import get_size_representation, TAG_BUFFER


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

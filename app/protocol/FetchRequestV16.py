from dataclasses import dataclass
from .RequestV2 import RequestV2
from uuid import UUID


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



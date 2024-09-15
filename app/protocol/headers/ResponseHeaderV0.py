from dataclasses import dataclass
from ..base import Header


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

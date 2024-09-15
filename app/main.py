from socket import create_server
from .protocol.base import Message
from .protocol.RequestV2 import RequestV2
from .protocol.headers.ResponseHeaderV0 import ResponseHeaderV0
from .protocol.headers.ResponseHeaderV1 import ResponseHeaderV1
from .protocol.ApiVersionsResponseV3 import APIVersionsResponseV3, APIKeysV3
from .protocol.FetchRequestV16 import FetchRequestV16
from .protocol.FetchResponseV16 import FetchResponseV16, TopicResponsesV16, PartitionsV16


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


def main():
    server = create_server(("localhost", 9092), reuse_port=True)
    socket, address = server.accept()  # wait for client

    print(f"Client connected: {address}")
    while True:
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

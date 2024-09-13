from socket import create_server


class Message:
    header: bytes
    body: bytes

    def __init__(self, header: bytes, body: bytes):
        self.size = len(header + body)
        self.header = header
        self.request_api_key = int.from_bytes(header[:2], byteorder="big")
        self.request_api_version = int.from_bytes(header[2:4], byteorder="big")
        self.correlation_id = int.from_bytes(header[4:8], byteorder="big")
        self.client_id = int.from_bytes(header[8:], byteorder="big")
        self.tagged_fields = ''  # No tagged fields for us
        self.body = body

    def to_bytes(self):
        return self.size.to_bytes(4, byteorder="big") + self.header + self.body

    def __repr__(self):
        return (f"{self.size} | "
                f"{self.request_api_key} - {self.request_api_version} - {self.correlation_id} - {self.client_id} | "
                f"{self.body}")


def main():
    server = create_server(("localhost", 9092), reuse_port=True)
    socket, address = server.accept()  # wait for client

    print(f"Client connected: {address}")
    # Receive data from the client
    data = socket.recv(1024)
    print(f"Received data: {data}")
    request = Message(data[4:], b'')
    print(f"Received request: {request}")
    error_code = 0 if request.request_api_version in [0, 1, 2, 3, 4] else 35
    message = Message(
        request.correlation_id.to_bytes(4, byteorder="big"),
        # API Version Response | Ref.: https://kafka.apache.org/protocol.html#The_Messages_ApiVersions
        # error_code INT16
        error_code.to_bytes(2, byteorder="big") +
        # api_key INT16
        int(18).to_bytes(2, byteorder="big") +
        # min_version INT16
        int(4).to_bytes(2, byteorder="big") +
        # max_version INT16
        int(4).to_bytes(2, byteorder="big")
    )
    print(f"Sending message: {message.to_bytes().hex()}")
    socket.sendall(message.to_bytes())


if __name__ == "__main__":
    main()

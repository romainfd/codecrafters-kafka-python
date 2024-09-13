from socket import create_server


class Message:
    header: bytes
    body: bytes

    def __init__(self, header: bytes, body: bytes):
        self.header = header
        self.request_api_key = header[:2]
        self.request_api_version = header[2:4]
        self.correlation_id = header[4:8]
        self.client_id = header[8:]
        self.tagged_fields = ''  # No tagged fields for us
        self.body = body

    def to_bytes(self):
        return len(self.header + self.body).to_bytes(4, byteorder="big") + self.header + self.body


def main():
    server = create_server(("localhost", 9092), reuse_port=True)
    socket, address = server.accept()  # wait for client

    print(f"Client connected: {address}")
    # Receive data from the client
    data = socket.recv(1024)
    print(f"Received data: {data}")
    request = Message(data[4:], b'')
    message = Message(request.correlation_id, b'')
    print(f"Sending message: {message.to_bytes().hex()}")
    socket.sendall(message.to_bytes())


if __name__ == "__main__":
    main()

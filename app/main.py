from socket import create_server


class Message:
    header: bytes
    body: bytes

    def __init__(self, header: bytes, body: bytes):
        self.header = header
        self.request_api_key = int.from_bytes(header[:2], byteorder="big")
        self.request_api_version = int.from_bytes(header[2:4], byteorder="big")
        self.correlation_id = int.from_bytes(header[4:8], byteorder="big")
        self.client_id = int.from_bytes(header[8:], byteorder="big")
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
    message = Message(request.correlation_id.to_bytes(4, byteorder="big"), int(35).to_bytes(2, byteorder="2"))
    print(f"Sending message: {message.to_bytes().hex()}")
    socket.sendall(message.to_bytes())


if __name__ == "__main__":
    main()

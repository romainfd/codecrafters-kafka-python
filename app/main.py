from socket import create_server


class Message:
    body: bytes

    def __init__(self, body: bytes):
        self.body = body

    def to_bytes(self):
        return len(self.body).to_bytes(4, byteorder="big") + self.body


def main():
    server = create_server(("localhost", 9092), reuse_port=True)
    socket, address = server.accept()  # wait for client

    print(f"Client connected: {address}")
    # Receive data from the client
    data = socket.recv(1024)
    print(f"Received data: {data}")
    correlation_id = 7  # hardcoded for now
    message = Message(correlation_id.to_bytes(4, byteorder="big"))
    print(f"Sending message: {message.to_bytes().hex()}")
    socket.sendall(message.to_bytes())


if __name__ == "__main__":
    main()

import socket
import struct


class conn:
    HANDSHAKE_HEADER = b"P2PFILESHARINGPROJ"
    HANDSHAKE_ZEROS = b"\x00" * 10
    HANDSHAKE_LEN = 32

    def __init__(self, peer_id, host=None, port=None, client_socket=None):
        self.id = int(peer_id)
        self.remote_peer_id = None

        if client_socket is None:
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.client_socket.connect((host, int(port)))
        else:
            self.client_socket = client_socket

    def _recv_exact(self, size):
        data = b""

        while len(data) < size:
            chunk = self.client_socket.recv(size - len(data))
            if not chunk:
                raise ConnectionError("Socket closed while receiving data.")
            data += chunk

        return data

    def send_handshake(self):
        message = (
            self.HANDSHAKE_HEADER
            + self.HANDSHAKE_ZEROS
            + struct.pack("!I", self.id)
        )
        self.client_socket.sendall(message)

    def receive_handshake(self, expected_peer_id=None):
        data = self._recv_exact(self.HANDSHAKE_LEN)
        header = data[:18]
        zeros = data[18:28]
        remote_peer_id = struct.unpack("!I", data[28:32])[0]

        if header != self.HANDSHAKE_HEADER:
            raise ValueError("Invalid handshake header.")

        if zeros != self.HANDSHAKE_ZEROS:
            raise ValueError("Invalid handshake zero bits.")

        if expected_peer_id is not None and remote_peer_id != int(expected_peer_id):
            raise ValueError(
                f"Expected peer ID {expected_peer_id}, received {remote_peer_id}."
            )

        self.remote_peer_id = remote_peer_id
        return remote_peer_id

    def createMsg(self, msg_type, msg_data=b""):
        if msg_data is None:
            msg_data = b""

        if not isinstance(msg_data, bytes):
            raise TypeError("msg_data must be bytes.")

        message_length = 1 + len(msg_data)
        return (
            struct.pack("!I", message_length)
            + struct.pack("!B", int(msg_type))
            + msg_data
        )

    def sendMsg(self, msg_type, msg_data=b""):
        try:
            self.client_socket.sendall(self.createMsg(msg_type, msg_data))
        except KeyboardInterrupt:
            raise
        except Exception:
            return False

        return True

    def receive(self):
        try:
            length_bytes = self._recv_exact(4)
            message_length = struct.unpack("!I", length_bytes)[0]

            if message_length < 1:
                return (None, None)

            body = self._recv_exact(message_length)
        except KeyboardInterrupt:
            raise
        except Exception:
            return (None, None)

        return (body[0], body[1:])

    def close(self):
        if self.client_socket is not None:
            try:
                self.client_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.client_socket.close()
            self.client_socket = None

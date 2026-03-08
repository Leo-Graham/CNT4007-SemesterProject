# helper class to store information about an established TCP connection
# between two peers

import socket
import struct

class conn:
    HANDSHAKE_HEADER = b"P2PFILESHARINGPROJ"   # 18 bytes
    HANDSHAKE_ZEROS = b"\x00" * 10             # 10 bytes
    HANDSHAKE_LEN = 32

    # initializes a connection (or stores an existing connection) between
    # one peer and another
    def __init__(self, peer_id, host=None, port=None, client_socket=None):
        self.id = peer_id

        if client_socket is None:
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.client_socket.connect((host, int(port)))
        else:
            self.client_socket = client_socket

    # helper function to receive exactly n bytes
    def _recv_exact(self, n):
        data = b""
        while len(data) < n:
            chunk = self.client_socket.recv(n - len(data))
            if not chunk:
                raise ConnectionError("Socket closed while receiving data.")
            data += chunk
        return data

    # ---------------- HANDSHAKE ----------------
    # handshake header (18-byte string) | zero bits (10 bytes) | peer ID (4 bytes)

    def send_handshake(self):
        msg = (self.HANDSHAKE_HEADER + self.HANDSHAKE_ZEROS+ struct.pack("!I", self.id))
        self.client_socket.sendall(msg)

    def receive_handshake(self):
        data = self._recv_exact(self.HANDSHAKE_LEN)
        header = data[:18]
        zeros = data[18:28]

        peer_id_bytes = data[28:32]
        remote_peer_id = struct.unpack("!I", peer_id_bytes)[0]

        return remote_peer_id

    # ---------------- MESSAGES ----------------
    # message length (4 bytes) | message type (1 byte) | message payload (variable)

    # helper function to create a message based on the message type and data provided
    def createMsg(self, msg_type, msg_data=b""):
        if msg_data is None:
            msg_data = b""

        if not isinstance(msg_data, bytes):
            raise TypeError("msg_data must be bytes.")

        msg_length = 1 + len(msg_data)  # type byte + payload
        msg = struct.pack("!I", msg_length)
        msg += struct.pack("!B", msg_type)
        msg += msg_data
        return msg

    # sends a message through this connection
    def sendMsg(self, msg_type, msg_data=b""):
        try:
            msg = self.createMsg(msg_type, msg_data)
            self.client_socket.sendall(msg)

        except KeyboardInterrupt:
            raise

        except Exception:
            return False

        return True

    # receives a message - returns (msg_type, msg_data)
    def receive(self):
        try:
            length_bytes = self._recv_exact(4)
            msg_length = struct.unpack("!I", length_bytes)[0]

            if msg_length < 1:
                return (None, None)

            msg_body = self._recv_exact(msg_length)
            msg_type = msg_body[0]
            msg_data = msg_body[1:]

        except KeyboardInterrupt:
            raise

        except Exception:
            return (None, None)

        return (msg_type, msg_data)

    # closes the connection
    def close(self):
        if self.client_socket is not None:
            self.client_socket.close()
            self.client_socket = None
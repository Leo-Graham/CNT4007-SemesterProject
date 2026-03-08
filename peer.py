# Bare Bones code - referenced this website a lot !! very helpful for some basic p2p functionality
# https://cs.berry.edu/~nhamid/p2p/framework-python.html

import random
import socket
import struct
import threading

from conn import conn


class peer:
    # message types
    CHOKE = 0
    UNCHOKE = 1
    INTERESTED = 2
    NOT_INTERESTED = 3
    HAVE = 4
    BITFIELD = 5
    REQUEST = 6
    PIECE = 7

    def __init__(self, server_port, id, num_pieces=8):
        # initialize the peer node
        self.server_port = int(server_port)
        self.id = int(id)

        # shuts down the peer node - false by default
        self.shutdown = False

        # simple piece state placeholders
        self.num_pieces = num_pieces
        self.have = [False] * self.num_pieces
        self.pieces = [None] * self.num_pieces

        # active neighbor connections
        self.neighbors = []
        self.lock = threading.Lock()

    # create a server socket for this peer object
    def createserver_socket(self, port):
        # uses IPv4 and TCP
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(("", int(port)))

        # decides how many connections in queue
        server_socket.listen(5)
        return server_socket

    # main loop that runs while the peer is alive
    def runPeer(self):
        # instantiates a socket to begin the run loop
        server_socket = self.createserver_socket(self.server_port)
        server_socket.settimeout(2)

        print(f"Server for peer {self.id} is running on port {self.server_port}")

        # the run loop - should go until keyboard interrupt
        while not self.shutdown:
            try:
                client_socket, clientAddr = server_socket.accept()
                client_socket.settimeout(None)

                t = threading.Thread(
                    target=self.handlePeer,
                    args=(client_socket, clientAddr),
                    daemon=True
                )
                t.start()

            except KeyboardInterrupt:
                self.shutdown = True
                continue
            except socket.timeout:
                continue
            except Exception as e:
                print("Accept error:", e)
                continue

        print(f"Peer {self.id} shutting down.")
        server_socket.close()

    # ---------------- BITFIELD HELPERS ----------------

    # packs self.have into bitfield bytes
    def pack_bitfield(self):
        out = bytearray((self.num_pieces + 7) // 8)
        for i, has_piece in enumerate(self.have):
            if has_piece:
                byte_index = i // 8
                bit_index = 7 - (i % 8)   # high bit first
                out[byte_index] |= (1 << bit_index)
        return bytes(out)

    # unpacks remote bitfield bytes into a list of booleans
    def unpack_bitfield(self, data):
        result = [False] * self.num_pieces
        for i in range(self.num_pieces):
            byte_index = i // 8
            if byte_index >= len(data):
                break
            bit_index = 7 - (i % 8)
            result[i] = bool((data[byte_index] >> bit_index) & 1)
        return result

    # ---------------- MESSAGE HELPERS ----------------

    def update_interest(self, connection, remote_bitfield):
        interested = False

        for i in range(self.num_pieces):
            if remote_bitfield[i] and not self.have[i]:
                interested = True
                break

        if interested:
            connection.sendMsg(self.INTERESTED)
        else:
            connection.sendMsg(self.NOT_INTERESTED)

    def request_piece(self, connection, remote_bitfield):
        if remote_bitfield is None:
            return

        candidates = [
            i for i in range(self.num_pieces)
            if remote_bitfield[i] and not self.have[i]
        ]

        if candidates:
            piece_index = random.choice(candidates)
            connection.sendMsg(self.REQUEST, struct.pack("!I", piece_index))

    def broadcast_have(self, piece_index):
        payload = struct.pack("!I", piece_index)

        with self.lock:
            dead_neighbors = []

            for neighbor in self.neighbors:
                ok = neighbor.sendMsg(self.HAVE, payload)
                if not ok:
                    dead_neighbors.append(neighbor)

            for neighbor in dead_neighbors:
                try:
                    neighbor.close()
                except Exception:
                    pass
                if neighbor in self.neighbors:
                    self.neighbors.remove(neighbor)

    # ---------------- PEER HANDLER ----------------
    # handshake message is structured as follows
    # handshake header (18-byte string) | zero bits (10 bytes) | peer ID (4 bytes)

    # messages are structured as follows
    # message length (4 bytes) | message type (1 byte) | message payload (variable)

    # type          | value | payload
    # choke         | 0     |
    # unchoke       | 1     |
    # interested    | 2     |
    # not interested| 3     |
    # have          | 4     | 4-byte piece index
    # bitfield      | 5     | bitfield bytes
    # request       | 6     | 4-byte piece index
    # piece         | 7     | 4-byte piece index + piece bytes

    def handlePeer(self, client_socket, client_address):
        host, port = client_address
        connection = conn(self.id, client_socket=client_socket)

        with self.lock:
            self.neighbors.append(connection)

        try:
            # receive handshake, then send handshake back
            remote_peer_id = connection.receive_handshake()
            connection.send_handshake()

            remote_bitfield = None
            choked_by_remote = True

            # send my bitfield after handshake if I have at least one piece
            my_bitfield = self.pack_bitfield()
            if any(b != 0 for b in my_bitfield):
                connection.sendMsg(self.BITFIELD, my_bitfield)

            while True:
                msg_type, msg_data = connection.receive()

                if msg_type is None:
                    break

                if msg_type == self.CHOKE:
                    choked_by_remote = True

                elif msg_type == self.UNCHOKE:
                    choked_by_remote = False
                    self.request_piece(connection, remote_bitfield)

                elif msg_type == self.BITFIELD:
                    remote_bitfield = self.unpack_bitfield(msg_data)
                    self.update_interest(connection, remote_bitfield)

                elif msg_type == self.HAVE:
                    if len(msg_data) != 4:
                        continue

                    piece_index = struct.unpack("!I", msg_data)[0]

                    if remote_bitfield is None:
                        remote_bitfield = [False] * self.num_pieces

                    if 0 <= piece_index < self.num_pieces:
                        remote_bitfield[piece_index] = True
                        self.update_interest(connection, remote_bitfield)

                elif msg_type == self.REQUEST:
                    if len(msg_data) != 4:
                        continue

                    piece_index = struct.unpack("!I", msg_data)[0]

                    if 0 <= piece_index < self.num_pieces and self.have[piece_index]:
                        piece_data = self.pieces[piece_index]
                        if piece_data is not None:
                            payload = struct.pack("!I", piece_index) + piece_data
                            connection.sendMsg(self.PIECE, payload)

                elif msg_type == self.PIECE:
                    if len(msg_data) < 4:
                        continue

                    piece_index = struct.unpack("!I", msg_data[:4])[0]
                    piece_data = msg_data[4:]

                    if 0 <= piece_index < self.num_pieces and not self.have[piece_index]:
                        self.have[piece_index] = True
                        self.pieces[piece_index] = piece_data
                        self.broadcast_have(piece_index)

                        if not choked_by_remote:
                            self.request_piece(connection, remote_bitfield)

        finally:
            with self.lock:
                if connection in self.neighbors:
                    self.neighbors.remove(connection)
            connection.close()
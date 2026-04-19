import argparse
import math
import random
import shutil
import socket
import struct
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from conn import conn

PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_DIR = PROJECT_DIR / "Configs"
DEFAULT_COMMON_CFG = DEFAULT_CONFIG_DIR / "Common.cfg"
DEFAULT_PEERINFO_CFG = DEFAULT_CONFIG_DIR / "PeerInfo.cfg"
LEGACY_COMMON_CFG = PROJECT_DIR / "Common.cfg"
LEGACY_PEERINFO_CFG = PROJECT_DIR / "PeerInfo.cfg"


@dataclass
class NeighborState:
    connection: conn
    bitfield: list[bool] | None = None
    interested_in_me: bool = False
    i_am_interested: bool = False
    am_choking_peer: bool = True
    peer_choking_me: bool = True
    downloaded_bytes: int = 0
    outstanding_request: int | None = None


class peer:
    CHOKE = 0
    UNCHOKE = 1
    INTERESTED = 2
    NOT_INTERESTED = 3
    HAVE = 4
    BITFIELD = 5
    REQUEST = 6
    PIECE = 7

    def __init__(self, peer_id, common_config, peer_infos, base_dir, log_dir=None):
        self.id = int(peer_id)
        self.common = dict(common_config)
        self.peer_infos = dict(peer_infos)
        self.peer_order = list(self.peer_infos.keys())

        if self.id not in self.peer_infos:
            raise ValueError(f"Peer {self.id} is not defined in PeerInfo.cfg.")

        self.self_info = self.peer_infos[self.id]
        self.server_port = int(self.self_info["port"])
        self.k = int(self.common["NumberOfPreferredNeighbors"])
        self.unchoking_interval = int(self.common["UnchokingInterval"])
        self.optimistic_unchoking_interval = int(
            self.common["OptimisticUnchokingInterval"]
        )
        self.file_name = str(self.common["FileName"])
        self.file_size = int(self.common["FileSize"])
        self.piece_size = int(self.common["PieceSize"])

        self.base_dir = Path(base_dir).resolve()
        self.peer_dir = self._resolve_peer_dir()
        self.peer_dir.mkdir(parents=True, exist_ok=True)
        self.num_pieces = math.ceil(self.file_size / self.piece_size)

        self.log_dir = PROJECT_DIR if log_dir is None else Path(log_dir).resolve()
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.log_path = self.log_dir / f"log_peer_{self.id}.log"
        self.log_path.write_text("", encoding="utf-8")

        self.lock = threading.RLock()
        self.log_lock = threading.Lock()
        self.shutdown_event = threading.Event()
        self.server_socket = None
        self.threads = []

        self.have = [False] * self.num_pieces
        self.pieces = [None] * self.num_pieces
        self.neighbor_states = {}
        self.requested_pieces = set()
        self.preferred_neighbors = set()
        self.optimistic_neighbor = None
        self.completed_peers = {
            peer_id
            for peer_id, info in self.peer_infos.items()
            if info["has_file"]
        }
        self.completion_logged = self.self_info["has_file"]

        self._load_initial_file_if_present()

    @staticmethod
    def parse_common(file_path):
        config = {}
        with open(file_path, "r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                key, value = line.split()
                if key in {
                    "NumberOfPreferredNeighbors",
                    "UnchokingInterval",
                    "OptimisticUnchokingInterval",
                    "FileSize",
                    "PieceSize",
                }:
                    config[key] = int(value)
                else:
                    config[key] = value
        return config

    @staticmethod
    def parse_peer(file_path):
        peers = {}
        with open(file_path, "r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                peer_id, host, port, has_file = line.split()
                peers[int(peer_id)] = {
                    "host": host,
                    "port": int(port),
                    "has_file": has_file == "1",
                }
        return peers

    @classmethod
    def from_config(
        cls,
        peer_id,
        common_path=None,
        peer_info_path=None,
        base_dir=None,
        log_dir=None,
    ):
        if common_path is None:
            common_path = DEFAULT_COMMON_CFG if DEFAULT_COMMON_CFG.exists() else LEGACY_COMMON_CFG
        if peer_info_path is None:
            peer_info_path = (
                DEFAULT_PEERINFO_CFG if DEFAULT_PEERINFO_CFG.exists() else LEGACY_PEERINFO_CFG
            )

        common_path = Path(common_path)
        peer_info_path = Path(peer_info_path)

        if not common_path.is_absolute():
            common_path = (PROJECT_DIR / common_path).resolve()
        else:
            common_path = common_path.resolve()

        if not peer_info_path.is_absolute():
            peer_info_path = (PROJECT_DIR / peer_info_path).resolve()
        else:
            peer_info_path = peer_info_path.resolve()

        if base_dir is None:
            base_dir = PROJECT_DIR
        else:
            base_dir = Path(base_dir).resolve()

        return cls(
            peer_id=peer_id,
            common_config=cls.parse_common(common_path),
            peer_infos=cls.parse_peer(peer_info_path),
            base_dir=base_dir,
            log_dir=log_dir,
        )

    def _track_thread(self, thread):
        self.threads.append(thread)
        thread.start()

    def _resolve_peer_dir(self):
        preferred_dir = self.base_dir / f"peer_{self.id}"
        alternate_dirs = [
            self.base_dir / str(self.id),
            self.base_dir / "Peers" / f"peer_{self.id}",
            self.base_dir / "Peers" / str(self.id),
        ]

        if preferred_dir.exists():
            return preferred_dir

        for candidate in alternate_dirs:
            if candidate.exists():
                preferred_dir.mkdir(parents=True, exist_ok=True)
                for item in candidate.iterdir():
                    target = preferred_dir / item.name
                    if item.is_dir():
                        shutil.copytree(item, target, dirs_exist_ok=True)
                    else:
                        shutil.copy2(item, target)
                return preferred_dir

        return preferred_dir

    def _piece_bounds(self, piece_index):
        start = piece_index * self.piece_size
        end = min(start + self.piece_size, self.file_size)
        return (start, end)

    def _expected_piece_size(self, piece_index):
        start, end = self._piece_bounds(piece_index)
        return end - start

    def _load_initial_file_if_present(self):
        if not self.self_info["has_file"]:
            return

        file_path = self.peer_dir / self.file_name
        if not file_path.exists():
            raise FileNotFoundError(
                f"Peer {self.id} is marked as having the file, but "
                f"{file_path} does not exist."
            )

        payload = file_path.read_bytes()
        if len(payload) != self.file_size:
            raise ValueError(
                f"Expected {self.file_size} bytes in {file_path}, "
                f"found {len(payload)}."
            )

        for piece_index in range(self.num_pieces):
            start, end = self._piece_bounds(piece_index)
            self.have[piece_index] = True
            self.pieces[piece_index] = payload[start:end]

        self.completed_peers.add(self.id)

    def has_complete_file(self):
        with self.lock:
            return all(self.have)

    def count_owned_pieces(self):
        with self.lock:
            return sum(1 for has_piece in self.have if has_piece)

    def pack_bitfield(self):
        with self.lock:
            local_have = list(self.have)

        output = bytearray((self.num_pieces + 7) // 8)
        for piece_index, has_piece in enumerate(local_have):
            if has_piece:
                byte_index = piece_index // 8
                bit_index = 7 - (piece_index % 8)
                output[byte_index] |= 1 << bit_index
        return bytes(output)

    def unpack_bitfield(self, payload):
        result = [False] * self.num_pieces
        for piece_index in range(self.num_pieces):
            byte_index = piece_index // 8
            if byte_index >= len(payload):
                break
            bit_index = 7 - (piece_index % 8)
            result[piece_index] = bool((payload[byte_index] >> bit_index) & 1)
        return result

    def log(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{timestamp}]: {message}\n"
        with self.log_lock:
            with open(self.log_path, "a", encoding="utf-8") as handle:
                handle.write(line)

    def _send_initial_bitfield(self, remote_peer_id):
        payload = self.pack_bitfield()
        if any(payload):
            self._send_message(remote_peer_id, self.BITFIELD, payload)

    def _send_message(self, remote_peer_id, msg_type, payload=b""):
        with self.lock:
            state = self.neighbor_states.get(remote_peer_id)
            connection = state.connection if state is not None else None

        if connection is None:
            return False

        if connection.sendMsg(msg_type, payload):
            return True

        self._drop_connection(remote_peer_id)
        return False

    def _remote_has_interesting_piece(self, remote_peer_id):
        with self.lock:
            state = self.neighbor_states.get(remote_peer_id)
            if state is None or state.bitfield is None:
                return False
            local_have = list(self.have)
            remote_have = list(state.bitfield)

        return any(remote_piece and not local_piece for remote_piece, local_piece in zip(remote_have, local_have))

    def _update_interest(self, remote_peer_id):
        interested = self._remote_has_interesting_piece(remote_peer_id)

        with self.lock:
            state = self.neighbor_states.get(remote_peer_id)
            if state is None or state.i_am_interested == interested:
                return
            state.i_am_interested = interested

        msg_type = self.INTERESTED if interested else self.NOT_INTERESTED
        self._send_message(remote_peer_id, msg_type)

    def _refresh_interests(self):
        with self.lock:
            peer_ids = list(self.neighbor_states.keys())

        for remote_peer_id in peer_ids:
            self._update_interest(remote_peer_id)

    def _write_complete_file(self):
        with self.lock:
            if not all(self.have):
                return
            payload = b"".join(self.pieces)

        file_path = self.peer_dir / self.file_name
        file_path.write_bytes(payload)

    def _mark_remote_complete_locked(self, remote_peer_id):
        state = self.neighbor_states.get(remote_peer_id)
        if state is not None and state.bitfield is not None and all(state.bitfield):
            self.completed_peers.add(remote_peer_id)

    def _handle_local_completion(self):
        should_log = False

        with self.lock:
            if all(self.have):
                self.completed_peers.add(self.id)
                if not self.completion_logged:
                    self.completion_logged = True
                    should_log = True

        self._write_complete_file()

        if should_log:
            self.log(f"Peer {self.id} has downloaded the complete file.")

    def _broadcast_have(self, piece_index):
        payload = struct.pack("!I", piece_index)

        with self.lock:
            peer_ids = list(self.neighbor_states.keys())

        for remote_peer_id in peer_ids:
            self._send_message(remote_peer_id, self.HAVE, payload)

    def _store_piece(self, remote_peer_id, piece_index, piece_data):
        expected_size = self._expected_piece_size(piece_index)
        if len(piece_data) != expected_size:
            return False

        with self.lock:
            if piece_index < 0 or piece_index >= self.num_pieces:
                return False

            state = self.neighbor_states.get(remote_peer_id)
            if state is None:
                return False

            if state.outstanding_request == piece_index:
                state.outstanding_request = None

            self.requested_pieces.discard(piece_index)

            if self.have[piece_index]:
                return False

            self.have[piece_index] = True
            self.pieces[piece_index] = piece_data
            state.downloaded_bytes += len(piece_data)

        piece_count = self.count_owned_pieces()
        self.log(
            f"Peer {self.id} has downloaded the piece {piece_index} "
            f"from {remote_peer_id}. Now the number of pieces it has is {piece_count}."
        )
        self._broadcast_have(piece_index)
        self._refresh_interests()
        self._handle_local_completion()
        return True

    def _request_piece(self, remote_peer_id):
        with self.lock:
            state = self.neighbor_states.get(remote_peer_id)
            if (
                state is None
                or state.peer_choking_me
                or state.outstanding_request is not None
                or state.bitfield is None
            ):
                return

            candidates = [
                piece_index
                for piece_index in range(self.num_pieces)
                if state.bitfield[piece_index]
                and not self.have[piece_index]
                and piece_index not in self.requested_pieces
            ]

            if not candidates:
                return

            piece_index = random.choice(candidates)
            state.outstanding_request = piece_index
            self.requested_pieces.add(piece_index)

        if not self._send_message(
            remote_peer_id,
            self.REQUEST,
            struct.pack("!I", piece_index),
        ):
            with self.lock:
                state = self.neighbor_states.get(remote_peer_id)
                if state is not None and state.outstanding_request == piece_index:
                    state.outstanding_request = None
                self.requested_pieces.discard(piece_index)
            return

    def _send_piece(self, remote_peer_id, piece_index):
        with self.lock:
            state = self.neighbor_states.get(remote_peer_id)
            if state is None or state.am_choking_peer:
                return

            if piece_index < 0 or piece_index >= self.num_pieces or not self.have[piece_index]:
                return

            piece_data = self.pieces[piece_index]
            if piece_data is None:
                return

        payload = struct.pack("!I", piece_index) + piece_data
        self._send_message(remote_peer_id, self.PIECE, payload)

    def _drop_connection(self, remote_peer_id):
        state = None
        with self.lock:
            state = self.neighbor_states.pop(remote_peer_id, None)
            self.preferred_neighbors.discard(remote_peer_id)
            if self.optimistic_neighbor == remote_peer_id:
                self.optimistic_neighbor = None
            if state is not None and state.outstanding_request is not None:
                self.requested_pieces.discard(state.outstanding_request)

        if state is not None:
            state.connection.close()

    def _register_connection(self, remote_peer_id, connection, initiated_by_me):
        with self.lock:
            if remote_peer_id in self.neighbor_states:
                return False
            self.neighbor_states[remote_peer_id] = NeighborState(connection=connection)

        if initiated_by_me:
            self.log(f"Peer {self.id} makes a connection to Peer {remote_peer_id}.")
        else:
            self.log(f"Peer {self.id} is connected from Peer {remote_peer_id}.")

        self._send_initial_bitfield(remote_peer_id)

        thread = threading.Thread(
            target=self._message_loop,
            args=(remote_peer_id,),
            daemon=True,
        )
        self._track_thread(thread)
        return True

    def _handle_choke(self, remote_peer_id):
        with self.lock:
            state = self.neighbor_states.get(remote_peer_id)
            if state is None:
                return
            state.peer_choking_me = True
            if state.outstanding_request is not None:
                self.requested_pieces.discard(state.outstanding_request)
                state.outstanding_request = None

        self.log(f"Peer {self.id} is choked by {remote_peer_id}.")

    def _handle_unchoke(self, remote_peer_id):
        with self.lock:
            state = self.neighbor_states.get(remote_peer_id)
            if state is None:
                return
            state.peer_choking_me = False

        self.log(f"Peer {self.id} is unchoked by {remote_peer_id}.")
        self._request_piece(remote_peer_id)

    def _handle_interested(self, remote_peer_id):
        with self.lock:
            state = self.neighbor_states.get(remote_peer_id)
            if state is None:
                return
            state.interested_in_me = True

        self.log(f"Peer {self.id} received the 'interested' message from {remote_peer_id}.")

    def _handle_not_interested(self, remote_peer_id):
        with self.lock:
            state = self.neighbor_states.get(remote_peer_id)
            if state is None:
                return
            state.interested_in_me = False

        self.log(
            f"Peer {self.id} received the 'not interested' message from {remote_peer_id}."
        )

    def _handle_bitfield(self, remote_peer_id, payload):
        remote_bitfield = self.unpack_bitfield(payload)
        with self.lock:
            state = self.neighbor_states.get(remote_peer_id)
            if state is None:
                return
            state.bitfield = remote_bitfield
            self._mark_remote_complete_locked(remote_peer_id)

        self._update_interest(remote_peer_id)

    def _handle_have(self, remote_peer_id, payload):
        if len(payload) != 4:
            return

        piece_index = struct.unpack("!I", payload)[0]
        if piece_index < 0 or piece_index >= self.num_pieces:
            return

        with self.lock:
            state = self.neighbor_states.get(remote_peer_id)
            if state is None:
                return
            if state.bitfield is None:
                state.bitfield = [False] * self.num_pieces
            state.bitfield[piece_index] = True
            self._mark_remote_complete_locked(remote_peer_id)

        self.log(
            f"Peer {self.id} received the 'have' message from {remote_peer_id} "
            f"for the piece {piece_index}."
        )
        self._update_interest(remote_peer_id)

    def _handle_request(self, remote_peer_id, payload):
        if len(payload) != 4:
            return

        piece_index = struct.unpack("!I", payload)[0]
        self._send_piece(remote_peer_id, piece_index)

    def _handle_piece(self, remote_peer_id, payload):
        if len(payload) < 4:
            return

        piece_index = struct.unpack("!I", payload[:4])[0]
        piece_data = payload[4:]

        if self._store_piece(remote_peer_id, piece_index, piece_data):
            self._request_piece(remote_peer_id)

    def _message_loop(self, remote_peer_id):
        try:
            while not self.shutdown_event.is_set():
                with self.lock:
                    state = self.neighbor_states.get(remote_peer_id)
                    connection = state.connection if state is not None else None

                if connection is None:
                    break

                msg_type, payload = connection.receive()
                if msg_type is None:
                    break

                if msg_type == self.CHOKE:
                    self._handle_choke(remote_peer_id)
                elif msg_type == self.UNCHOKE:
                    self._handle_unchoke(remote_peer_id)
                elif msg_type == self.INTERESTED:
                    self._handle_interested(remote_peer_id)
                elif msg_type == self.NOT_INTERESTED:
                    self._handle_not_interested(remote_peer_id)
                elif msg_type == self.BITFIELD:
                    self._handle_bitfield(remote_peer_id, payload)
                elif msg_type == self.HAVE:
                    self._handle_have(remote_peer_id, payload)
                elif msg_type == self.REQUEST:
                    self._handle_request(remote_peer_id, payload)
                elif msg_type == self.PIECE:
                    self._handle_piece(remote_peer_id, payload)
        finally:
            self._drop_connection(remote_peer_id)

    def _accept_loop(self):
        while not self.shutdown_event.is_set():
            try:
                client_socket, _ = self.server_socket.accept()
                client_socket.settimeout(None)
            except socket.timeout:
                continue
            except OSError:
                break

            connection = conn(self.id, client_socket=client_socket)

            try:
                remote_peer_id = connection.receive_handshake()
                if remote_peer_id not in self.peer_infos or remote_peer_id == self.id:
                    raise ValueError("Received handshake from unexpected peer.")
                connection.send_handshake()
            except Exception:
                connection.close()
                continue

            if not self._register_connection(remote_peer_id, connection, initiated_by_me=False):
                connection.close()

    def _connect_to_previous_peer(self, remote_peer_id):
        remote_info = self.peer_infos[remote_peer_id]

        while not self.shutdown_event.is_set():
            with self.lock:
                if remote_peer_id in self.neighbor_states:
                    return

            connection = None
            try:
                connection = conn(
                    self.id,
                    host=remote_info["host"],
                    port=remote_info["port"],
                )
                connection.send_handshake()
                connection.receive_handshake(expected_peer_id=remote_peer_id)
                if self._register_connection(remote_peer_id, connection, initiated_by_me=True):
                    return
                connection.close()
            except Exception:
                if connection is not None:
                    connection.close()

            self.shutdown_event.wait(1.0)

    def _set_neighbor_choke_state(self, remote_peer_id, should_choke):
        with self.lock:
            state = self.neighbor_states.get(remote_peer_id)
            if state is None or state.am_choking_peer == should_choke:
                return
            state.am_choking_peer = should_choke

        msg_type = self.CHOKE if should_choke else self.UNCHOKE
        self._send_message(remote_peer_id, msg_type)

    def _preferred_neighbor_loop(self):
        while not self.shutdown_event.wait(self.unchoking_interval):
            with self.lock:
                interested = [
                    remote_peer_id
                    for remote_peer_id, state in self.neighbor_states.items()
                    if state.interested_in_me
                ]

                if self.has_complete_file():
                    random.shuffle(interested)
                    chosen_list = interested[: self.k]
                else:
                    random.shuffle(interested)
                    interested.sort(
                        key=lambda remote_peer_id: self.neighbor_states[remote_peer_id].downloaded_bytes,
                        reverse=True,
                    )
                    chosen_list = interested[: self.k]

                chosen = set(chosen_list)
                changed = chosen != self.preferred_neighbors
                self.preferred_neighbors = chosen

                for state in self.neighbor_states.values():
                    state.downloaded_bytes = 0

                optimistic_neighbor = self.optimistic_neighbor
                peer_ids = list(self.neighbor_states.keys())

            if changed:
                preferred_text = ", ".join(str(peer_id) for peer_id in chosen_list) or "None"
                self.log(f"Peer {self.id} has the preferred neighbors {preferred_text}.")

            for remote_peer_id in peer_ids:
                should_choke = (
                    remote_peer_id not in chosen
                    and remote_peer_id != optimistic_neighbor
                )
                self._set_neighbor_choke_state(remote_peer_id, should_choke)

    def _optimistic_neighbor_loop(self):
        while not self.shutdown_event.wait(self.optimistic_unchoking_interval):
            with self.lock:
                candidates = [
                    remote_peer_id
                    for remote_peer_id, state in self.neighbor_states.items()
                    if state.interested_in_me
                    and remote_peer_id not in self.preferred_neighbors
                    and state.am_choking_peer
                ]

                old_neighbor = self.optimistic_neighbor
                new_neighbor = random.choice(candidates) if candidates else None
                self.optimistic_neighbor = new_neighbor

            if new_neighbor != old_neighbor and new_neighbor is not None:
                self.log(
                    f"Peer {self.id} has the optimistically unchoked neighbor {new_neighbor}."
                )

            if old_neighbor is not None and old_neighbor != new_neighbor:
                with self.lock:
                    old_is_preferred = old_neighbor in self.preferred_neighbors
                if not old_is_preferred:
                    self._set_neighbor_choke_state(old_neighbor, True)

            if new_neighbor is not None:
                self._set_neighbor_choke_state(new_neighbor, False)

    def _completion_loop(self):
        while not self.shutdown_event.wait(1.0):
            with self.lock:
                all_complete = (
                    self.id in self.completed_peers
                    and len(self.completed_peers) == len(self.peer_infos)
                )

            if all_complete:
                self.shutdown_event.set()
                break

    def run(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(("", self.server_port))
        self.server_socket.listen(len(self.peer_infos))
        self.server_socket.settimeout(1.0)

        accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._track_thread(accept_thread)

        preferred_thread = threading.Thread(
            target=self._preferred_neighbor_loop,
            daemon=True,
        )
        self._track_thread(preferred_thread)

        optimistic_thread = threading.Thread(
            target=self._optimistic_neighbor_loop,
            daemon=True,
        )
        self._track_thread(optimistic_thread)

        completion_thread = threading.Thread(
            target=self._completion_loop,
            daemon=True,
        )
        self._track_thread(completion_thread)

        my_position = self.peer_order.index(self.id)
        for remote_peer_id in self.peer_order[:my_position]:
            connector_thread = threading.Thread(
                target=self._connect_to_previous_peer,
                args=(remote_peer_id,),
                daemon=True,
            )
            self._track_thread(connector_thread)

        try:
            while not self.shutdown_event.is_set():
                self.shutdown_event.wait(0.5)
        except KeyboardInterrupt:
            self.shutdown_event.set()
        finally:
            if self.server_socket is not None:
                try:
                    self.server_socket.close()
                except OSError:
                    pass

            with self.lock:
                peer_ids = list(self.neighbor_states.keys())

            for remote_peer_id in peer_ids:
                self._drop_connection(remote_peer_id)

            current_thread = threading.current_thread()
            for thread in self.threads:
                if thread is current_thread:
                    continue
                thread.join(timeout=2.0)


PeerProcess = peer


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run a CNT4007 peer process.")
    parser.add_argument("peer_id", type=int, help="Peer ID from PeerInfo.cfg")
    args = parser.parse_args(argv)

    process = peer.from_config(peer_id=args.peer_id)
    process.run()


if __name__ == "__main__":
    main()

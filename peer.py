import shutil
import socket
import struct
import argparse
import math
import random
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

        self.peer_dir = self.base_dir / f"peer_{self.id}"
        check = [
            self.base_dir / str(self.id),
            self.base_dir / "Peers" / f"peer_{self.id}",
            self.base_dir / "Peers" / str(self.id),
        ]
        found = next((c for c in check if c.exists()), None)
        if found and not self.peer_dir.exists():
            shutil.copytree(found, self.peer_dir, dirs_exist_ok=True)

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

        if self.self_info["has_file"]:
            getpath = self.peer_dir / self.file_name
            if not getpath.exists():
                raise FileNotFoundError(
                    f"Peer {self.id} is marked as having the file, but "
                    f"{getpath} does not exist."
                )
            if getpath.stat().st_size != self.file_size:
                raise ValueError(
                    f"Expected {self.file_size} bytes in {getpath}, "
                    f"found {getpath.stat().st_size}."
                )
            with getpath.open("rb") as f:
                for i in range(self.num_pieces):
                    start = i * self.piece_size
                    size = min(start + self.piece_size, self.file_size) - start
                    self.pieces[i] = f.read(size)
                    self.have[i] = True
            self.completed_peers.add(self.id)

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
                check = raw_line.strip()
                if not check:
                    continue
                getid, host, port, getnum = check.split()
                peers[int(getid)] = {
                    "host": host,
                    "port": int(port),
                    "has_file": getnum == "1",
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

    def trackcurrentthread(self, thread):
        self.threads.append(thread)
        thread.start()

    def getsize(self, indpiece):
        start = indpiece * self.piece_size
        return min(start + self.piece_size, self.file_size) - start

    def ifcompletefile(self):
        with self.lock:
            return all(self.have)

    def numowned(self):
        with self.lock:
            return sum(1 for has_piece in self.have if has_piece)

    def pack_bitfield(self):
        with self.lock:
            local_have = list(self.have)

        ret = bytearray((self.num_pieces + 7) // 8)
        for indpiece, has_piece in enumerate(local_have):
            if has_piece:
                byteind = indpiece // 8
                bitind = 7 - (indpiece % 8)
                ret[byteind] |= 1 << bitind
        return bytes(ret)

    def unpack_bitfield(self, msgbytes):
        result = [False] * self.num_pieces
        for indpiece in range(self.num_pieces):
            byte_index = indpiece // 8
            if byte_index >= len(msgbytes):
                break
            bit_index = 7 - (indpiece % 8)
            result[indpiece] = bool((msgbytes[byte_index] >> bit_index) & 1)
        return result

    def log(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{timestamp}]: {message}\n"
        with self.log_lock:
            with open(self.log_path, "a", encoding="utf-8") as handle:
                handle.write(line)

    def sendinitbits(self, remoteid):
        ret = self.pack_bitfield()
        if any(ret):
            with self.lock:
                state = self.neighbor_states.get(remoteid)
                check = state.connection if state is not None else None
            if check is not None:
                if not check.sendMsg(self.BITFIELD, ret):
                    with self.lock:
                        state = self.neighbor_states.pop(remoteid, None)
                        self.preferred_neighbors.discard(remoteid)
                        if self.optimistic_neighbor == remoteid:
                            self.optimistic_neighbor = None
                        if state is not None and state.outstanding_request is not None:
                            self.requested_pieces.discard(state.outstanding_request)
                    if state is not None:
                        state.connection.close()

    def checkneigborpiece(self, remoteid):
        with self.lock:
            state = self.neighbor_states.get(remoteid)
            if state is None or state.bitfield is None:
                return False
            local_have = list(self.have)
            remote_have = list(state.bitfield)

        return any(remote_piece and not local_piece for remote_piece, local_piece in zip(remote_have, local_have))

    def _update_interest(self, remoteid):
        interested = self.checkneigborpiece(remoteid)

        with self.lock:
            state = self.neighbor_states.get(remoteid)
            if state is None or state.i_am_interested == interested:
                return
            state.i_am_interested = interested

        msg_type = self.INTERESTED if interested else self.NOT_INTERESTED
        with self.lock:
            state = self.neighbor_states.get(remoteid)
            check = state.connection if state is not None else None
        if check is not None:
            if not check.sendMsg(msg_type):
                with self.lock:
                    state = self.neighbor_states.pop(remoteid, None)
                    self.preferred_neighbors.discard(remoteid)
                    if self.optimistic_neighbor == remoteid:
                        self.optimistic_neighbor = None
                    if state is not None and state.outstanding_request is not None:
                        self.requested_pieces.discard(state.outstanding_request)
                if state is not None:
                    state.connection.close()

    def updateinterest(self):
        with self.lock:
            getlist = list(self.neighbor_states.keys())

        for id in getlist:
            self._update_interest(id)

    def markcomplete(self, remoteid):
        state = self.neighbor_states.get(remoteid)
        if state is not None and state.bitfield is not None and all(state.bitfield):
            self.completed_peers.add(remoteid)

    def localcomplete(self):
        check = False

        with self.lock:
            if all(self.have):
                self.completed_peers.add(self.id)
                if not self.completion_logged:
                    self.completion_logged = True
                    check = True
        with self.lock:
            if all(self.have):
                write = b"".join(self.pieces)
                (self.peer_dir / self.file_name).write_bytes(write)

        if check:
            self.log(f"Peer {self.id} has downloaded the complete file.")

    def piececheck(self, remoteid, indpiece, piece_data):
        if len(piece_data) != self.getsize(indpiece):
            return False

        with self.lock:
            state = self.neighbor_states.get(remoteid)

            if indpiece not in range(self.num_pieces) or state is None or self.have[indpiece]:
                return False

            self.have[indpiece] = True
            self.pieces[indpiece] = piece_data
            state.downloaded_bytes += len(piece_data)

            if state.outstanding_request == indpiece:
                state.outstanding_request = None

            self.requested_pieces.discard(indpiece)
            piece_count = sum(1 for h in self.have if h)

        self.log(
            f"Peer {self.id} has downloaded the piece {indpiece} "
            f"from {remoteid}. Now the number of pieces it has is {piece_count}."
        )

        ret = struct.pack("!I", indpiece)
        with self.lock:
            getlist = list(self.neighbor_states.keys())
        for id in getlist:
            with self.lock:
                state = self.neighbor_states.get(id)
                check = state.connection if state is not None else None
            if check is not None:
                if not check.sendMsg(self.HAVE, ret):
                    with self.lock:
                        state = self.neighbor_states.pop(id, None)
                        self.preferred_neighbors.discard(id)
                        if self.optimistic_neighbor == id:
                            self.optimistic_neighbor = None
                        if state is not None and state.outstanding_request is not None:
                            self.requested_pieces.discard(state.outstanding_request)
                    if state is not None:
                        state.connection.close()

        self.updateinterest()
        self.localcomplete()
        return True

    def reqpiece(self, remoteid):
        with self.lock:
            state = self.neighbor_states.get(remoteid)
            if not state or state.peer_choking_me or state.outstanding_request is not None or state.bitfield is None:
                return

            candidates = [
                i for i in range(self.num_pieces)
                if state.bitfield[i] and not self.have[i] and i not in self.requested_pieces
            ]

            if not (indpiece := random.choice(candidates) if candidates else None):
                return

            state.outstanding_request = indpiece
            self.requested_pieces.add(indpiece)

        with self.lock:
            state = self.neighbor_states.get(remoteid)
            check = state.connection if state is not None else None
        if check is None or not check.sendMsg(self.REQUEST, struct.pack("!I", indpiece)):
            with self.lock:
                if (state := self.neighbor_states.get(remoteid)) and state.outstanding_request == indpiece:
                    state.outstanding_request = None
                self.requested_pieces.discard(indpiece)
            if check is None:
                with self.lock:
                    state = self.neighbor_states.pop(remoteid, None)
                    self.preferred_neighbors.discard(remoteid)
                    if self.optimistic_neighbor == remoteid:
                        self.optimistic_neighbor = None
                    if state is not None and state.outstanding_request is not None:
                        self.requested_pieces.discard(state.outstanding_request)
                if state is not None:
                    state.connection.close()

    def sendpiece(self, remoteid, indpiece):
        with self.lock:
            state = self.neighbor_states.get(remoteid)
            piece_data = self.pieces[indpiece] if indpiece in range(self.num_pieces) else None

            if not state or state.am_choking_peer or piece_data is None or not self.have[indpiece]:
                return

            check = state.connection

        if not check.sendMsg(self.PIECE, struct.pack("!I", indpiece) + piece_data):
            with self.lock:
                state = self.neighbor_states.pop(remoteid, None)
                self.preferred_neighbors.discard(remoteid)
                if self.optimistic_neighbor == remoteid:
                    self.optimistic_neighbor = None
                if state is not None and state.outstanding_request is not None:
                    self.requested_pieces.discard(state.outstanding_request)
            if state is not None:
                state.connection.close()

    def regconn(self, remoteid, connection, initiated_by_me):
        with self.lock:
            if remoteid in self.neighbor_states:
                return False
            self.neighbor_states[remoteid] = NeighborState(connection=connection)

        if initiated_by_me:
            self.log(f"Peer {self.id} makes a connection to Peer {remoteid}.")
        else:
            self.log(f"Peer {self.id} is connected from Peer {remoteid}.")

        self.sendinitbits(remoteid)

        thread = threading.Thread(
            target=self.loopformsg,
            args=(remoteid,),
            daemon=True,
        )
        self.trackcurrentthread(thread)
        return True

    def controlchoke(self, remoteid):
        with self.lock:
            state = self.neighbor_states.get(remoteid)
            if state is None:
                return
            state.peer_choking_me = True
            if state.outstanding_request is not None:
                self.requested_pieces.discard(state.outstanding_request)
                state.outstanding_request = None

        self.log(f"Peer {self.id} is choked by {remoteid}.")

    def controlunchoke(self, remoteid):
        with self.lock:
            state = self.neighbor_states.get(remoteid)
            if state is None:
                return
            state.peer_choking_me = False

        self.log(f"Peer {self.id} is unchoked by {remoteid}.")
        self.reqpiece(remoteid)

    def controlinterested(self, remoteid):
        with self.lock:
            state = self.neighbor_states.get(remoteid)
            if state is None:
                return
            state.interested_in_me = True

        self.log(f"Peer {self.id} received the 'interested' message from {remoteid}.")

    def controluninterested(self, remoteid):
        with self.lock:
            state = self.neighbor_states.get(remoteid)
            if state is None:
                return
            state.interested_in_me = False

        self.log(
            f"Peer {self.id} received the 'not interested' message from {remoteid}."
        )

    def controlbitfield(self, remoteid, msgbytes):
        remote_bitfield = self.unpack_bitfield(msgbytes)
        with self.lock:
            state = self.neighbor_states.get(remoteid)
            if state is None:
                return
            state.bitfield = remote_bitfield
            self.markcomplete(remoteid)

        self._update_interest(remoteid)

    def controlhave(self, remoteid, msgbytes):
        if len(msgbytes) != 4:
            return

        indpiece = struct.unpack("!I", msgbytes)[0]
        if indpiece < 0 or indpiece >= self.num_pieces:
            return

        with self.lock:
            state = self.neighbor_states.get(remoteid)
            if state is None:
                return
            if state.bitfield is None:
                state.bitfield = [False] * self.num_pieces
            state.bitfield[indpiece] = True
            self.markcomplete(remoteid)

        self.log(
            f"Peer {self.id} received the 'have' message from {remoteid} "
            f"for the piece {indpiece}."
        )
        self._update_interest(remoteid)

    def controlreq(self, remoteid, msgbytes):
        if len(msgbytes) != 4:
            return

        indpiece = struct.unpack("!I", msgbytes)[0]
        self.sendpiece(remoteid, indpiece)

    def controlpiece(self, remoteid, msgbytes):
        if len(msgbytes) < 4:
            return

        indpiece = struct.unpack("!I", msgbytes[:4])[0]
        piece_data = msgbytes[4:]

        if self.piececheck(remoteid, indpiece, piece_data):
            self.reqpiece(remoteid)

    def loopformsg(self, remoteid):
        try:
            while not self.shutdown_event.is_set():
                with self.lock:
                    state = self.neighbor_states.get(remoteid)
                    connection = state.connection if state is not None else None

                if connection is None:
                    break

                msg_type, msgbytes = connection.receive()
                if msg_type is None:
                    break

                if msg_type == self.CHOKE:
                    self.controlchoke(remoteid)
                elif msg_type == self.UNCHOKE:
                    self.controlunchoke(remoteid)
                elif msg_type == self.INTERESTED:
                    self.controlinterested(remoteid)
                elif msg_type == self.NOT_INTERESTED:
                    self.controluninterested(remoteid)
                elif msg_type == self.BITFIELD:
                    self.controlbitfield(remoteid, msgbytes)
                elif msg_type == self.HAVE:
                    self.controlhave(remoteid, msgbytes)
                elif msg_type == self.REQUEST:
                    self.controlreq(remoteid, msgbytes)
                elif msg_type == self.PIECE:
                    self.controlpiece(remoteid, msgbytes)
        finally:
            with self.lock:
                state = self.neighbor_states.pop(remoteid, None)
                self.preferred_neighbors.discard(remoteid)
                if self.optimistic_neighbor == remoteid:
                    self.optimistic_neighbor = None
                if state is not None and state.outstanding_request is not None:
                    self.requested_pieces.discard(state.outstanding_request)
            if state is not None:
                state.connection.close()

    def _connect_to_previous_peer(self, remoteid):
        peerinfo = self.peer_infos[remoteid]

        while not self.shutdown_event.wait(1.0):
            with self.lock:
                if remoteid in self.neighbor_states:
                    return

            connection = None
            try:
                connection = conn(self.id, host=peerinfo["host"], port=peerinfo["port"])
                connection.send_handshake()
                connection.receive_handshake(expected_peer_id=remoteid)
                if self.regconn(remoteid, connection, initiated_by_me=True):
                    return
                connection.close()
            except Exception:
                if connection:
                    connection.close()

    def setneighborstate(self, remoteid, should_choke):
        with self.lock:
            state = self.neighbor_states.get(remoteid)
            if state is None or state.am_choking_peer == should_choke:
                return
            state.am_choking_peer = should_choke

        msg_type = self.CHOKE if should_choke else self.UNCHOKE
        with self.lock:
            state = self.neighbor_states.get(remoteid)
            check = state.connection if state is not None else None
        if check is not None:
            if not check.sendMsg(msg_type):
                with self.lock:
                    state = self.neighbor_states.pop(remoteid, None)
                    self.preferred_neighbors.discard(remoteid)
                    if self.optimistic_neighbor == remoteid:
                        self.optimistic_neighbor = None
                    if state is not None and state.outstanding_request is not None:
                        self.requested_pieces.discard(state.outstanding_request)
                if state is not None:
                    state.connection.close()

    def prefneighborloop(self):
        while not self.shutdown_event.wait(self.unchoking_interval):
            with self.lock:
                interested = [
                    remoteid
                    for remoteid, state in self.neighbor_states.items()
                    if state.interested_in_me
                ]

                if self.ifcompletefile():
                    random.shuffle(interested)
                    chosen_list = interested[: self.k]
                else:
                    random.shuffle(interested)
                    interested.sort(
                        key=lambda remoteid: self.neighbor_states[remoteid].downloaded_bytes,
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

            for remoteid in peer_ids:
                should_choke = (
                    remoteid not in chosen
                    and remoteid != optimistic_neighbor
                )
                self.setneighborstate(remoteid, should_choke)

    def optimisticneighborloop(self):
        while not self.shutdown_event.wait(self.optimistic_unchoking_interval):
            with self.lock:
                candidates = [
                    remoteid
                    for remoteid, state in self.neighbor_states.items()
                    if state.interested_in_me
                    and remoteid not in self.preferred_neighbors
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
                    self.setneighborstate(old_neighbor, True)

            if new_neighbor is not None:
                self.setneighborstate(new_neighbor, False)

    def endloop(self):
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

        def accloop():
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
                    remoteid = connection.receive_handshake()
                    if remoteid not in self.peer_infos or remoteid == self.id:
                        raise ValueError("Received handshake from unexpected peer.")
                    connection.send_handshake()
                except Exception:
                    connection.close()
                    continue

                if not self.regconn(remoteid, connection, initiated_by_me=False):
                    connection.close()

        self.trackcurrentthread(threading.Thread(target=accloop, daemon=True))
        self.trackcurrentthread(threading.Thread(target=self.prefneighborloop, daemon=True))
        self.trackcurrentthread(threading.Thread(target=self.optimisticneighborloop, daemon=True))
        self.trackcurrentthread(threading.Thread(target=self.endloop, daemon=True))

        my_position = self.peer_order.index(self.id)
        for remoteid in self.peer_order[:my_position]:
            self.trackcurrentthread(threading.Thread(
                target=self._connect_to_previous_peer,
                args=(remoteid,),
                daemon=True,
            ))

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

            for remoteid in peer_ids:
                with self.lock:
                    state = self.neighbor_states.pop(remoteid, None)
                    self.preferred_neighbors.discard(remoteid)
                    if self.optimistic_neighbor == remoteid:
                        self.optimistic_neighbor = None
                    if state is not None and state.outstanding_request is not None:
                        self.requested_pieces.discard(state.outstanding_request)
                if state is not None:
                    state.connection.close()

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
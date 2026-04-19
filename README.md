# CNT4007 Semester Project
Leo Graham
Justin Ming
Julia Bush
Raymon Martinez

This project implements a BitTorrent-style peer-to-peer file sharing system for CNT4007.

## Files You Need

- `peerProcess.py` - launcher for a peer process
- `peer.py` - main peer logic
- `conn.py` - TCP connection and message framing helpers
- `Configs/Common.cfg` - shared file-transfer settings
- `Configs/PeerInfo.cfg` - peer list, ports, and seed status

## Default Behavior

When you run:

```bash
python peerProcess.py <peerID>
```

the program automatically:

- reads `Configs/Common.cfg`
- reads `Configs/PeerInfo.cfg`
- creates or uses `peer_<peerID>` in the project root
- writes logs to `log_peer_<peerID>.log` in the project root

## Current Config

The current config is a 3-peer localhost setup:

```txt
1001 localhost 7101 1
1002 localhost 7102 0
1003 localhost 7103 0
```

That means:

- peer `1001` starts with the full file
- peers `1002` and `1003` start empty
- all 3 peers can be run on one machine in separate terminals

## Before You Run

Make sure these folders exist:

- `peer_1001`
- `peer_1002`
- `peer_1003`

Make sure the seed file exists here before starting:

```txt
peer_1001/tree.jpg
```

That filename should match `FileName` in `Configs/Common.cfg`.

Do not place the file in `peer_1002` or `peer_1003` before the run.

## Run Locally

Open 3 terminals in the project root and run:

```bash
python peerProcess.py 1001
```

```bash
python peerProcess.py 1002
```

```bash
python peerProcess.py 1003
```

When the run finishes:

- the file should appear in `peer_1002`
- the file should appear in `peer_1003`
- `log_peer_1001.log`, `log_peer_1002.log`, and `log_peer_1003.log` should exist


## Multi-Machine Use

To run on multiple machines:

1. Put the same project code on each machine.
2. Keep the same `Configs/Common.cfg` on each machine.
3. Update `Configs/PeerInfo.cfg` so each peer uses a real reachable hostname or IP instead of `localhost`.
4. Put the seed file only in the folder for peers marked with `1`.
5. Run one peer process per listed peer.

Example 3-machine config:

```txt
1001 192.168.1.10 7101 1
1002 192.168.1.11 7102 0
1003 192.168.1.12 7103 0
```

Then run:

- machine 1: `python peerProcess.py 1001`
- machine 2: `python peerProcess.py 1002`
- machine 3: `python peerProcess.py 1003`

## Quick Sanity Check

Before running, you can verify the code compiles:

```bash
python -m py_compile conn.py peer.py peerProcess.py
```

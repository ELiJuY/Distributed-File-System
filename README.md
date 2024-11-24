# Distributed File System with Replication

This project implements a distributed file system with support for replication and fault tolerance. The system consists of three main components:
- **Master Server:** Handles metadata and file management.
- **Storage Servers:** Store file replicas and ensure redundancy.
- **Client Library:** Provides APIs to interact with the distributed file system.

## Features
- File creation, reading, and writing.
- Server replication for fault tolerance.
- Dynamic assignment of servers to files and blocks.
- Simple command-line interface for testing.

## Components
1. **Master Server (`master.c`)**
   - Manages file metadata and server assignments.
   - Ensures proper replication across storage servers.

2. **Storage Server (`server.c`)**
   - Stores file blocks and handles replication.
   - Communicates with the master server and clients.

3. **Client Library (`mgfs.c`)**
   - Provides functions for connecting to the master server, managing files, and writing data.

4. **Testing Application (`test.c`)**
   - A CLI-based application to test the distributed file system functionalities.

## How to Run
### Prerequisites
- A C compiler (e.g., GCC).
- Linux-based environment (recommended for socket operations).
- Networking configured for communication between servers and clients.

### Steps
1. **Compile the Components:**
   Use the makefile in each in the directories:
	- /master_node
	- /server_node
	- /mgfs_lib
	- /clients

2. Start the Master Server: Run the master server on a designated port.

./master <master_port>

3. Start Storage Servers: Each server needs the master server's IP and port for registration.

./server <storage_directory> <master_ip> <master_port>

4.Run the Client Application: Interact with the distributed file system using the CLI.

./clients/test
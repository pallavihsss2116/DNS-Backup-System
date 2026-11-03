**SDN-Aware Resilient Distributed Backup System**
**Overview**
A distributed backup application that transfers files in chunks over TCP to a Primary Backup Server (h3) and a Standby Backup Server (h2) running on a Mininet network controlled by a POX SDN Controller.

If the primary server or a network path fails:

Client automatically switches to the standby server
SDN controller dynamically reroutes traffic
Backup continues without restarting
Every chunk is verified using SHA-256 integrity checking
A Tkinter-based dashboard visualizes:

Chunk-by-chunk transfer progress
Server status
Network topology
Throughput statistics
Live event logs


**Problem Statement**
Traditional backup systems often fail when servers or network paths become unavailable during file transfer.

This project implements a Distributed Backup System that:

Transfers files using TCP sockets
Splits files into chunks
Stores backups on multiple servers
Uses SDN for intelligent path selection
Automatically recovers from server and link failures
Ensures data integrity through SHA-256 verification


**System Architecture**
Network Topology
              s2
             /  \
   h1 --- s1      s4 --- h3 (Primary Server)
             \  /
              s3
              |
              h2 (Standby Server)
**Hosts**
Host	Role	                 IP Address
h1	  Client	               10.0.0.1
h2	  Standby Backup Server	 10.0.0.2
h3	  Primary Backup Server	 10.0.0.3

**Paths**
Path A: s1 → s2 → s4 (5 ms links)
Path B: s1 → s3 → s4 (15 ms links)
All inter-switch links are limited to 10 Mbps.

**Objectives and Implementation**

**Requirement	              Implementation**
TCP File Transfer	        Client and servers communicate using TCP sockets with a length-prefixed JSON protocol
File Chunking	            Files divided into configurable chunks (128 KB - 1 MB)
Data Integrity	          SHA-256 verification for every chunk
Resume Support	          Already uploaded chunks are skipped during retransmission
Multiple Backup Servers 	Primary and standby backup servers
SDN-Based Routing       	POX controller calculates paths using Dijkstra algorithm
Failure Recovery	        Automatic rerouting and failover
Performance Monitoring  	Throughput and recovery time measurements

**Key Features**

1. Automatic Failover
Heartbeat monitoring
Connection error detection
Controller reachability checks
Automatic switch from h3 to h2

2. Failure Scenarios Supported
Primary server crash
Server freeze/hang
Cable disconnection
Single link failure
Multiple path failures
Interrupted transfer

3. SDN Integration
Client communicates with the controller through a UNIX socket to obtain:
Active paths
Server reachability
Routing updates

4. Interrupt & Resume
Transfers can resume without retransmitting successfully stored chunks.

5. Integrity Verification
Per-chunk SHA-256 checks
Whole-file verification during restore

6. Dashboard
   
Tkinter dashboard with:

Chunk visualization
Server monitoring
Live topology view
Throughput graph
Event log
Failure injection controls
CSV export

7. CSV Export
Export:
Transfer results
Event logs
Chunk maps

**Technologies Used**
Python 3
TCP Sockets
Threading
Hashlib (SHA-256)
Tkinter
Mininet
Open vSwitch
POX Controller
OpenFlow 1.0
JSON Event Logging

**Project Structure**
sdn-backup/
│
├── backup/
│   ├── client.py
│   ├── server.py
│   └── common.py
│
├── controller/
│   └── multipath_controller.py
│
├── topology/
│   └── topology.py
│
├── dashboard/
│   ├── app.py
│   └── model.py
│
├── tests/
│   ├── test_core.py
│   ├── test_controller_logic.py
│   └── test_model.py
│
├── docs/
│   ├── ARCHITECTURE.md
│   ├── RESULTS.md
│   ├── DEMO_SCRIPT.md
│   └── screenshots/
│
├── local_demo.py
├── run_all.sh
└── CONTRIBUTIONS.md

**Installation**
Ubuntu Setup
sudo apt update

sudo apt install -y \
mininet \
openvswitch-switch \
python3-tk \
ffmpeg \
git

**Install POX**
git clone https://github.com/noxrepo/pox ~/pox

**Clone Project**
git clone <repository-url> ~/sdn-backup

**Create Shared Log Directory**
sudo chmod -R 777 /tmp/sdnbackup 2>/dev/null || true

**Running the Project**

Terminal 1 - Start POX Controller
cp ~/sdn-backup/controller/multipath_controller.py ~/pox/ext/
cd ~/pox
./pox.py \
log.level --INFO \
openflow.discovery \
openflow.spanning_tree \
multipath_controller

Terminal 2 - Start Mininet
sudo mn -c

sudo python3 \
~/sdn-backup/topology/topology.py \
--delay 0.02

Terminal 3 - Start Dashboard

python3 ~/sdn-backup/dashboard/app.py --replay

One Command Startup
./run_all.sh

**Sample Transfer Output**

==================================================================
                     TRANSFER COMPLETE
==================================================================

File:        clip.mp4
Size:        6.00 MB

Chunks:      48

Integrity:   Checksum Verified

Time Taken:  0.78 seconds

Throughput:  7.67 MB/s (64.4 Mbps)

Stored on:
h2: 48 chunks
h3: 48 chunks

Failover:    none needed

Resumed:
22 chunks already present
26 chunks transmitted

==================================================================


**Demo Scenarios**
Scenario	Expected Result
Normal Backup	Successful backup with integrity verification
Kill h3	Automatic failover to h2
h3 Crash	Automatic recovery
h3 Freeze	Heartbeat timeout and failover
Cable Cut	Switch to standby
Link Failure	Controller reroutes traffic
Resume Transfer	Missing chunks only are sent
Restore	Original file recovered successfully

**Testing**
Run unit tests:

python3 tests/test_core.py

python3 tests/test_controller_logic.py

python3 tests/test_model.py

**Limitations**
POX supports only OpenFlow 1.0.
Failover is client-controlled.
Link delay is not considered in routing cost.
Chunk map stored locally in JSON format.
No encryption.
No chunk deduplication.

**Future Enhancements**

Virtual IP based transparent failover
QoS-aware routing
End-to-end encryption
Deduplication
Delay-aware routing metrics
Cloud storage integration

**Team Members**
Pallavi H S
Narun Patil
Prakruthi R

**License**
Academic Course Project - Computer Networks (SDN)

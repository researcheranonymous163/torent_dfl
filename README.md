# torent_dfl

this project developed for provide communicatoin Bittorent in Desentralized Federated Learning Environment . in Deeplopito project we imported this project to using Bittorent Communicator . 
for communictation between nodes provided funcition like push , pull and broadcast .

## Communicator: Handles message passing between nodes using different backends:

* LocalCommunicator: In-memory communication

* GrpcCommunicator: gRPC-based communication

* NatsCommunicator: NATS message broker

* QbittorrentCommunicator: Uses qBittorrent for large data transfers


## Aggregator: Combines model updates from nodes:

* Basic operations (sum, average)

* Weighted averaging

* Partitioned aggregation

* State dictionary handling (PyTorch compatible)

## Key Features
* Asynchronous Operations: Built on asyncio for high concurrency

* Multiple Protocols: Supports gRPC, NATS, and torrent-based communication

* Tensor Aggregation: Specialized handling of PyTorch tensors

* Caching: Implements subscription caching for efficiency

* Data Handling: Includes utilities for data splitting and encoding
 

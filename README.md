# Paxos

_An implementation of the Paxos Consensus Protocol_

Paxos is a protocol for solving consensus through state machine replication in
an asynchronous environment with unreliable processes. This consensus protocol
is then extended to a replication protocol commonly referred to as Multi-Paxos
to assign global, persistent, total order to a sequence of client updates. The
protocol works by having multiple replicas work in parallel to maintain the same
state. This state is updated on each request from a client by each replica,
allowing it to be automatically replicated and preserved even in the case of
failures. The basic algorithm was famously described by Leslie Lamport in his
1998 paper, [The Part-Time
Parliament](https://www.microsoft.com/en-us/research/publication/part-time-parliament/).
It was later clarified in his follow-up paper from 2001, [Paxos Made
Simple](https://www.microsoft.com/en-us/research/publication/paxos-made-simple/?from=https%3A%2F%2Fresearch.microsoft.com%2Fen-us%2Fum%2Fpeople%2Flamport%2Fpubs%2Fpaxos-simple.pdf).

This library is implemented with a minimalistic philosophy. The main `paxos`
package implements only the core Paxos algorithm, with storage handling, network
transport, and physical clocks left to the clients of the library. This
minimalism buys flexibility, determinism, and performance. The design was
heavily inspired by [CoreOS's raft
library](https://github.com/coreos/etcd/tree/master/raft).

The library was also heavily influenced by the work of Jonathan Kirsch and Yair Amir
in their paper [Paxos for System Builders](https://www.cs.jhu.edu/~jak/docs/paxos_for_system_builders.pdf).


## Features

The paxos implementation is a full implementation of the Paxos replication
protocol. Features include:

- Leader election
- Update replication


## Building

Run `make` to build the binaries `client` and `server`

Run `make clean` to clean all build artifacts

Run `make check` to perform linting and static analysis


## Running

The project comes with two sample binaries, `client` and `server`. These
binaries exemplify the proper use of the library.

To run a server process, a command like the following can be used:

```
./server -p 54321 -h hostfile
```

All server processes are identical; there is no designated leader process.
Instead, the Paxos protocol will perform leader election periodically itself to
establish a stable leader. There is also no order in which processes need to be
brought up, although they will exit after 15 seconds if a connection cannot be
established any of their peers.

To run a client process, a command like the following can be used:

```
./client -p 54321 -h hostfile
```

The client will connect to all available hosts in the hostfile and then prompt
the user for an update string. Whenever an update is entered, it will send the
update to a random available server, which attempts to globally order the update.

### Verbose Mode (server only)

Adding the `-v` (`--verbose`) flag will turn on verbose mode, which will
print logging information to standard error. This information includes details
about all messages sent and received, as well as round timeout information.

### Command Line Arguments

A full list of command line arguments for the two binaries can be seen by
passing the `--help` flag.


## Testing

The project comes with an automated test suite which contains both direct unit
tests to test pieces of functionality within the Paxos state machine, and larger
network tests that test a network of Paxos nodes. The unit tests are scattered
throughout the `paxos/*_test.go` files, while the network tests are located in
the `paxos/paxos_test.go` file.

To run all tests, run the command `make test`


## System Architecture

The library is designed around the the `paxos` type, which is a single-threaded
state machine implementing the Paxos consensus protocol. The state machine can
be interacted with only through a `Node` instance, which is a thread-safe handle
to a `paxos` state machine.

Because the library pushes tasks like storage handling and network transport up
to the users of the library, these users have a few responsibilities. In a loop,
the user should read from the `Node.Ready` channel and process the updates it
contains. These `Ready` struct will contain any updates to the persistent state
of the node that should be synced to disk, and messages that need to be
delivered to other nodes, and any updates that have been successfully ordered.
The user should also periodically call `Node.Tick` in regular interval (probably
via a `time.Ticker`).

Together, the state machine handling loop will look something like:

```
for {
    select {
    case <-ticker.C:
        node.Tick()
    case rd := <-node.Ready():
        if rd.PersistentState != nil {
            saveToStorage(rd.PersistentState)
        }
        for _, msg := range rd.Messages {
            send(msg)
        }
        for _, update := range rd.OrderedUpdates {
            applyUpdate(update)
        }
    case <-ctx.Done():
        return
    }
}
```

To propose a change to the state machine, serialize the update into a byte slice
within a `pb.ClientUpdate` and call:

```
node.Propose(ctx, update)
```

If committed, the data will eventually appear in the `rd.OrderedUpdates` slice.

### Example Transport

While the paxos library itself does not restrict users to a specific network
transport approach, the project includes an example using the
[gRPC](http://www.grpc.io/) framework. This decision means that the layer can
hook directly into the `paxospb` protocol buffer definitions defined in the
`paxos` library (see [Protobuf Message
Serialization](#protobuf-message-serialization)). It also allowed the example
implementation to utilize client-side message streaming between Paxos nodes to
achieve improved performance.

The example transport layer can be seen in the `transport` package.


## Design Decisions

### Injected Time

Instead of including timeouts within the `paxos` library bound to physical time,
the library instead exposes the notion of a "tick". Ticks are injected by
clients of the library using the `Node.Tick` method. These ticks are then
transferred to various `tickingTimer` instances that handle heartbeats and
election timeouts. By avoiding a dependence on physical time, the package has
remained fully deterministic and is much easier to test.

### Protobuf Message Serialization

All messages that the `paxos` library interacts with are implemented as [Protocol
Buffers](https://developers.google.com/protocol-buffers/). This decision gives a
couple of huge benefits. Primarily, it means that they come with a mechanism for
trivial serialization, which is essential for sending the messages over a
network of persisting them to disk. It also means that the future modifications
to the message format will remain backwards compatible. Third, it means that the
message formats are language-agnostic. Finally, it means that if a user of the
library chooses to use gRPC as a network transport like we have, the messages
will play nicely with their service definitions.


## Implementation Issues

### Deterministic Failures Testing

One of the implementation issues faced while developing the algorithm was its
difficulty to test because the amount of required machinery was so large and
necessitated having multiple processes run on different hosts. To get around
this restriction and make testing easier, we isolated the core state machine
from the network transport layer. This meant that we could mock out the network
interactions of nodes and perform comprehensive testing on the underlying
algorithm. The result of this can be seen in the `paxos/paxos_test.go` network
tests. Here, we can choose to interfere with network traffic, isolate paxos
nodes, or crash paxos instances completely. All of this is deterministic and
easy to test.

### Multiple Processes on the Same Host

Another implementation issue faced while developing the sample applications was
their difficulty to run because the suggested template "assumes that each host
is running only one instance of the process." This meant that even during
development, to test a _m_ process instance of the algorithm, _m_ hosts needed
to coordinate and be kept in sync with code changes. To address this, the
single-process-per-host restriction was lifted early in the development cycle.
This was accomplished by allowing an optional port specification in the hostfile
for a given process using a `<hostname>:<port>` notation. Once individual
processes could specify unique ports, an optional `-i` (`--id`) flag was used to
distinguish the current process in a hostfile where multiple processes were
running on the same host. This way, the algorithm could be run on a single host
with a hostfile like:

```
<hostname>:1234
<hostname>:1235
<hostname>:1236
<hostname>:1237
```

And commands like:

```
./server -h hostfile -i=0
```


## Future Development

There are a few things missing from the library at the moment that will be
addressed in future improvements.

### Recovery

At the moment, a crashed node has no way to join back into the paxos group that
it was previously a member of. We should introduce a mechanism for node
recovery.

### Reconciliation

Related to recovery, the Paxos algorithm does not specify a reconciliation
protocol to bring servers back up to date. This means that the algorithm can
allow servers to order updates quickly without allowing them to actually execute
the updates due to gaps in the global sequence. A reconciliation strategy
should be introduced to address this issue.

### Membership Changes

At the moment, the library requires that a set of Paxos instances be specified
before starting the protocol. This set must stay the same, and no new nodes are
allowed to join. In the future support for changing membership of the paxos
group should be added.

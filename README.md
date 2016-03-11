# Copycat

[![Build Status](https://travis-ci.org/atomix/copycat.svg)](https://travis-ci.org/atomix/copycat)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.atomix.copycat/copycat-server/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.atomix.copycat%22) 
[![Gitter](https://img.shields.io/badge/GITTER-join%20chat-green.svg)](https://gitter.im/atomix/atomix)

#### [Website][Website] • [Javadoc][Javadoc] • [Atomix][Atomix] • [Jepsen Tests](https://github.com/atomix/atomix-jepsen) • [Google Group][Google group]

Copycat is a feature complete, fully asynchronous implementation of the [Raft consensus algorithm][Raft] in Java 8
built for [Atomix][Atomix] and designed for use in any project. The implementation provides a fully featured [client][clients] and
[server][servers] to operate on [replicated state machines][state machines]. The implementation includes:
* [Pre-vote election protocol](http://atomix.io/copycat/docs/internals/#preventing-disruptions-due-to-leader-changes) (§[4.2.3][dissertation])
* [Session-based linearizable writes](http://atomix.io/copycat/docs/internals/#client-commands) (§[6.3][dissertation])
* [Lease-based fast linearizable reads from leaders](http://atomix.io/copycat/docs/internals/#client-queries) (§[6.4.1][dissertation])
* [Fast sequential reads from followers](http://atomix.io/copycat/docs/internals/#processing-queries-on-followers) (§[6.4.1][dissertation])
* [FIFO consistency for concurrent/asynchronous operations](http://atomix.io/copycat/docs/internals/#preserving-program-order) (§[11.1.2][dissertation])
* [Session-based sequential/linearizable state machine events](http://atomix.io/copycat/docs/internals/#session-events) (§[6.3][dissertation])
* [Membership changes](http://atomix.io/copycat/docs/internals/#membership-changes) (§[4.3][dissertation])
* [Asynchronous gossip protocol](http://atomix.io/copycat/docs/internals/#passive-members)
* [Incremental log compaction](http://atomix.io/copycat/docs/internals/#log-compaction-algorithm) (§[5.3][dissertation])
* [Snapshots](http://atomix.io/copycat/docs/internals/#snapshots-via-log-compaction) (§[5.1][dissertation])

Additionally, this implementation has undergone extensive [Jepsen testing](http://github.com/atomix/atomix-jepsen)
to verify it maintains linearizability in a number of different failure scenarios.

*For more information on the Raft implementation itself, see [Copycat internals](http://atomix.io/copycat/docs/internals/)*

#### [Website][Website] • [Javadoc][Javadoc] • [Atomix][Atomix] • [Jepsen Tests](https://github.com/atomix/atomix-jepsen) • [Google Group][Google group]

[Raft]: https://raft.github.io/
[dissertation]: https://ramcloud.stanford.edu/~ongaro/thesis.pdf
[Atomix]: http://github.com/atomix/atomix
[clients]: http://atomix.io/copycat/docs/client/
[servers]: http://atomix.io/copycat/docs/server/
[state machines]: http://atomix.io/copycat/docs/state-machine/
[Website]: http://atomix.io/copycat/
[Google group]: https://groups.google.com/forum/#!forum/copycat
[Javadoc]: http://atomix.io/copycat/api/latest/

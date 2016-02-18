# Copycat

[![Build Status](https://travis-ci.org/atomix/copycat.svg)](https://travis-ci.org/atomix/copycat)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.atomix.copycat/copycat-server/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.atomix.copycat%22) 
[![Gitter](https://img.shields.io/badge/GITTER-join%20chat-green.svg)](https://gitter.im/atomix/atomix)

#### [Website][Website] • [Javadoc][Javadoc] • [Atomix][Atomix] • [Jepsen Tests](https://github.com/atomix/atomix-jepsen) • [Google Group][Google group]

Copycat is a feature complete, fully asynchronous implementation of the [Raft consensus algorithm][Raft] in Java 8
built for [Atomix][Atomix] and designed for use in any project. The implementation provides a fully featured [client][clients] and
[server][servers] to operate on [replicated state machines][state machines]. The implementation includes:
* Pre-vote election protocol (§[4.2.3][dissertation])
* Session-based linearizable writes (§[6.3][dissertation])
* Lease-based fast linearizable reads from leaders (§[6.4.1][dissertation])
* Fast sequential reads from followers (§[6.4.1][dissertation])
* FIFO consistency for concurrent/asynchronous operations
* Session-based sequential/linearizable state machine events (§[6.3][dissertation])
* Membership changes (§[4.3][dissertation])
* Snapshots (§[5.1][dissertation])
* Log cleaning (§[5.3][dissertation])

Additionally, this implementation has undergone extensive [Jepsen testing](http://github.com/jhalterman/copycat-jepsen)
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

# Copycat

[![Build Status](https://travis-ci.org/atomix/copycat.png)](https://travis-ci.org/atomix/copycat)

#### [Website][Website] • [Javadoc][Javadoc] • [Atomix][Atomix] • [Jepsen Tests](https://github.com/atomix/atomix-jepsen) • [Google Group][Google group]

Copycat is a feature complete, fully asynchronous implementation of the [Raft consensus algorithm][Raft] in Java 8
designed for use in [Atomix][Atomix]. The implementation provides a fully featured [client][clients] and [server][servers]
and includes:
* Pre-vote election protocol (§[4.2.3][dissertation])
* Session-based linearizable writes (§[6.3][dissertation])
* Lease-based fast linearizable reads from leaders (§[6.4.1][dissertation])
* Fast sequential reads from followers (§[6.4.1][dissertation])
* Sequential consistency for concurrent/asynchronous operations from a single client
* Session-based state machine events (§[6.3][dissertation])
* Membership changes (§[4.3][dissertation])
* Log compaction via cleaning (§[5.3][dissertation])

Additionally, this implementation has undergone extensive [Jepsen testing](http://github.com/jhalterman/copycat-jepsen)
to verify it maintains linearizability in a number of different failure scenarios.

*For more information on the Raft implementation itself, see [Raft internals](http://atomix.github.io/copycat/user-manual/internals/)*

### Project status: beta

Copycat is a fault-tolerant framework that provides strong consistency guarantees, and as such we take the responsibility
to test these claims and document the implementation very seriously. Copycat's implementation of the
[Raft consensus algorithm](https://raft.github.io/) is well tested, well documented, and [verified by
Jepsen](https://github.com/atomix/atomix-jepsen). But the *beta* label indicates that the implementation
may still have some bugs or other issues that make it not suitable for production. Once we've reached consensus
on the lack of significant bugs in the beta release(s), a release candidate will be pushed. Once we've reached
consensus on the stability of the release candidate(s) and Copycat's production readiness, a full release will be pushed.
It's all about that **consensus**!

Documentation for most of Copycat's implementation of the Raft algorithm is
[available on the Copycat website](http://atomix.github.io/copycat/user-manual/internals/), and users are encouraged
to [explore the Javadoc][Javadoc] which is also heavily documented. All documentation remains under continued
development, and websites for both Copycat and [Atomix][Atomix] will continue to be updated until and after a release.

## Examples

The [Atomix][Atomix] project is a collection of standalone Copycat `StateMachine`s and proxies that can be
multiplexed on a single Raft replicated log.
* [Distributed collections](https://github.com/atomix/atomix/blob/master/collections/src/main/java/io/atomix/collections/state/MapState.java)
* [Leader elections](https://github.com/atomix/atomix/blob/master/coordination/src/main/java/io/atomix/coordination/state/LeaderElectionState.java)
* [Locks](https://github.com/atomix/atomix/blob/master/coordination/src/main/java/io/atomix/coordination/state/LockState.java)
* [Group membership](https://github.com/atomix/atomix/blob/master/coordination/src/main/java/io/atomix/coordination/state/MembershipGroupState.java)

#### [Website][Website] • [Javadoc][Javadoc] • [Atomix][Atomix] • [Jepsen Tests](https://github.com/atomix/atomix-jepsen) • [Google Group][Google group]

[Raft]: https://raft.github.io/
[dissertation]: https://ramcloud.stanford.edu/~ongaro/thesis.pdf
[Atomix]: http://github.com/atomix/atomix
[clients]: http://atomix.io/copycat/user-manual/client
[servers]: http://atomix.io/copycat/user-manual/server
[Website]: http://atomix.io/copycat/
[Google group]: https://groups.google.com/forum/#!forum/copycat
[Javadoc]: http://atomix.io/copycat/api/latest/

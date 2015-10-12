# Copycat

[![Build Status](https://travis-ci.org/atomix/copycat.png)](https://travis-ci.org/atomix/copycat)

### [Website][Website] • [Google Group][Google group] • [Javadoc][Javadoc] • [Atomix][Atomix]

Copycat is a feature complete, fully asynchronous implementation of the [Raft consensus algorithm][Raft] in Java 8
designed for use in [Atomix][Atomix]. The implementation provides a fully featured [client][clients] and [server][servers]
and includes:
* Pre-vote election protocol (§[4.2.3][dissertation])
* Session-based linearizable writes (§[6.3][dissertation])
* Lease-based reads from leaders (§[6.4.1][dissertation])
* Serializable reads from followers (§[6.4.1][dissertation])
* Session-based state machine events (§[6.3][dissertation])
* Membership changes (§[4.3][dissertation])
* Log compaction via cleaning (§[5.3][dissertation])

Additionally, this implementation has undergone [Jepsen testing](http://github.com/jhalterman/copycat-jepsen)
to verify it maintains linearizability in a number of different failure scenarios.

*For more information on the Raft implementation itself, see [Raft internals](http://atomix.github.io/copycat/user-manual/internals/)*

## Examples

Additionally, the [Atomix][Atomix] project contains a multitude of examples of Copycat state machines, including
[collections](https://github.com/atomix/atomix/blob/master/collections/src/main/java/io/atomix/collections/state/MapState.java),
[leader elections](https://github.com/atomix/atomix/blob/master/coordination/src/main/java/io/atomix/coordination/state/LeaderElectionState.java),
[locks](https://github.com/atomix/atomix/blob/master/coordination/src/main/java/io/atomix/coordination/state/LockState.java),
[group membership](https://github.com/atomix/atomix/blob/master/coordination/src/main/java/io/atomix/coordination/state/MembershipGroupState.java), and more.

## Usage

Copycat consists of two separate projects: the Copycat client and server. The server is a standalone Raft server
implementation through which users can define and manage arbitrary replicated state machines, and the client is a
Raft client designed specifically to interact with the Copycat server to maintain strong consistency constraints.

A snapshot of Copycat is deployed on every push to the `master` branch. There is no official release of Copycat in
Maven Central yet, but there will in the coming weeks. In the meantime:

To add the Copycat server to your project, add a dependency on the `copycat-server` project:

```
<dependency>
  <groupId>io.atomix.copycat</groupId>
  <artifactId>copycat-server</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

Similarly, to add the Copycat client to your project, add a dependency on the `copycat-client` project:

```
<dependency>
  <groupId>io.atomix.copycat</groupId>
  <artifactId>copycat-client</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

Alternatively, you can build the Jars using Maven:

```
git clone --branch master git@github.com:atomix/copycat.git
cd copycat
mvn install
```

For documentation on how to use the Raft client and server, please visit the [website][Website].

[Raft]: https://raft.github.io/
[dissertation]: https://ramcloud.stanford.edu/~ongaro/thesis.pdf
[Atomix]: http://github.com/atomix/atomix
[clients]: http://atomix.io/copycat/user-manual/client
[servers]: http://atomix.io/copycat/user-manual/server
[Website]: http://atomix.io/copycat/
[Google group]: https://groups.google.com/forum/#!forum/copycat
[Javadoc]: http://atomix.io/copycat/api/latest/

# Catalogue

[![Build Status](https://travis-ci.org/atomix/copycat.png)](https://travis-ci.org/atomix/copycat)

### [Website][Website] • [Google Group][Google group] • [Javadoc][Javadoc]

Catalogue is a feature complete, fully asynchronous implementation of the [Raft consensus algorithm][Raft] in Java 8
designed for use in [Copycat][Copycat]. The implementation provides a fully featured [client][clients] and [server][servers]
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

*For more information on the Raft implementation itself, see [Raft internals](http://atomix.github.io/copycat/user-manual/raft-internals/)*

## Usage

Catalogue consists of two separate projects: the Catalogue client and server. The server is a standalone Raft server
implementation through which users can define and manage arbitrary replicated state machines, and the client is a
Raft client designed specifically to interact with the Catalogue server to maintain strong consistency constraints.

A snapshot of Catalogue is deployed on every push to the `master` branch. There is no official release of Catalogue in
Maven Central yet, but there will in the coming weeks. In the meantime:

To add the Catalogue server to your project, add a dependency on the `copycat-server` project:

```
<dependency>
  <groupId>io.atomix.copycat</groupId>
  <artifactId>copycat-server</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

Similarly, to add the Catalogue client to your project, add a dependency on the `copycat-client` project:

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
[Copycat]: http://github.com/atomix/copycat
[clients]: http://atomix.io/user-manual/copycat/raft-internals/#clients
[servers]: http://atomix.io/user-manual/copycat/raft-internals/#servers
[Website]: http://atomix.io/user-manual/copycat/raft-framework/
[Google group]: https://groups.google.com/forum/#!forum/copycat
[Javadoc]: http://atomix.io/copycat/api/latest/

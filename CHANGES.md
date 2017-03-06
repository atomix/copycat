# Change Log
All notable changes to this project will be documented in this file.

## Unreleased

## 1.2.3 - 2017-03-03

### Bug Fixes
* [#293](https://github.com/atomix/copycat/pull/293) - Relax consistency constraints for client sequencer to ensure missing events cannot prevent the client from progressing

## 1.2.2 - 2017-02-27

### CRITICAL
* [#287](https://github.com/atomix/copycat/pull/287) - Fix misaligned lock metadata in segment file headers

### Bug Fixes
* [#291](https://github.com/atomix/copycat/pull/291) - Ensure server is shut down if cluster `leave()` fails on `leave()`
* [#290](https://github.com/atomix/copycat/pull/290) - Allow compactor to interrupt compaction threads on shutdown
* [#289](https://github.com/atomix/copycat/pull/289) - Complete session unregister if session is already closed
* [#288](https://github.com/atomix/copycat/pull/288) - Ensure snapshot is taken in the state machine thread
* [#285](https://github.com/atomix/copycat/pull/285) - Too many connection attempts to unavailable followers
* [#284](https://github.com/atomix/copycat/pull/284) - Local address is returned as leader by followers in `ConnectResponse`

## 1.2.1 - 2017-02-23

### CRITICAL
* [#283](https://github.com/atomix/copycat/pull/283) - Ensure leader check for linearizable queries occurs after query is applied

### Bug Fixes
* [#282](https://github.com/atomix/copycat/pull/282) - Reset session command sequence number with keep alive requests
* [#281](https://github.com/atomix/copycat/pull/281) - `Transport` is not properly closed on server `leave()`
* [#277](https://github.com/atomix/copycat/pull/277) - Expire client sessions when command/query fail with unknown session
* [#276](https://github.com/atomix/copycat/pull/276) - Simplify management of client connections in servers
* [#271](https://github.com/atomix/copycat/pull/271) - Ensure `ServerSelectionStrategies.FOLLOWERS` can connect to single node

## 1.2.0 - 2017-01-12

### CRITICAL
* [#270](https://github.com/atomix/copycat/pull/270) - Remove `assert` statement with side effects preventing `InitializeEntry` from being appended
* [#266](https://github.com/atomix/copycat/pull/266) and [#269](https://github.com/atomix/copycat/pull/269) - Prevent `commitIndex` from being incremented beyond the last index in an `AppendRequest`

### Bug Fixes
* [#267](https://github.com/atomix/copycat/pull/267) - Limit request queue size to prevent stack overflow when queue is drained
* [#258](https://github.com/atomix/copycat/pull/258) - Fix improper session removal in certain cases after client recovers a session
* [#249](https://github.com/atomix/copycat/pull/249) - Fix incorrect exception arguments in state machine exceptions
* [#247](https://github.com/atomix/copycat/pull/247) - Fix 100% CPU usage due to compaction thread count
* [#244](https://github.com/atomix/copycat/pull/244) - Ensure interval is properly used in state machine scheduler
* [#240](https://github.com/atomix/copycat/pull/240) - Fix client state change/recovery bugs
* [#238](https://github.com/atomix/copycat/pull/238) - Ensure client can be closed when a majority of the cluster is down

### Features
* [#263](https://github.com/atomix/copycat/pull/263) - Support client-provided per-session timeouts
* [#262](https://github.com/atomix/copycat/pull/262) - Support user-provided client IDs

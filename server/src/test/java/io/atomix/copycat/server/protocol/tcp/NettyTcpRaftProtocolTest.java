/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */
package io.atomix.copycat.server.protocol.tcp;

import io.atomix.copycat.ConsistencyLevel;
import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.protocol.request.*;
import io.atomix.copycat.protocol.response.*;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.protocol.RaftProtocolClient;
import io.atomix.copycat.server.protocol.RaftProtocolClientConnection;
import io.atomix.copycat.server.protocol.RaftProtocolServer;
import io.atomix.copycat.server.protocol.request.*;
import io.atomix.copycat.server.protocol.response.*;
import io.atomix.copycat.util.concurrent.Listener;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Netty TCP based Raft protocol test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class NettyTcpRaftProtocolTest extends ConcurrentTestCase {

  /**
   * Tests sending and receiving requests.
   */
  public void testSendReceive() throws Throwable {
    NettyTcpRaftProtocol protocol = new NettyTcpRaftProtocol();
    RaftProtocolClient client = protocol.createClient();
    RaftProtocolServer server = protocol.createServer();

    server.listen(new Address("localhost", 5000), connection -> {
      connection.onConnect(request -> {
        threadAssertNotNull(request.client());
        resume();
        return CompletableFuture.completedFuture(ConnectResponse.builder()
          .withStatus(ProtocolResponse.Status.OK)
          .withLeader(new Address("localhost", 1234))
          .withMembers(Collections.emptyList())
          .build());
      });

      connection.onRegister(request -> {
        threadAssertNotNull(request.client());
        threadAssertEquals(1000L, request.timeout());
        resume();
        return CompletableFuture.completedFuture(RegisterResponse.builder()
          .withStatus(ProtocolResponse.Status.OK)
          .withLeader(new Address("localhost", 1234))
          .withSession(1)
          .withMembers(Arrays.asList(
            new Address("localhost", 1234),
            new Address("localhost", 2345),
            new Address("localhost", 3456)
          ))
          .withTimeout(1000)
          .build());
      });

      connection.onKeepAlive(request -> {
        threadAssertEquals(1L, request.session());
        threadAssertEquals(2L, request.commandSequence());
        threadAssertEquals(3L, request.eventIndex());
        resume();
        return CompletableFuture.completedFuture(KeepAliveResponse.builder()
          .withStatus(ProtocolResponse.Status.OK)
          .withLeader(new Address("localhost", 1234))
          .withMembers(Arrays.asList(
            new Address("localhost", 1234),
            new Address("localhost", 2345),
            new Address("localhost", 3456)
          ))
          .build());
      });

      connection.onUnregister(request -> {
        threadAssertEquals(1L, request.session());
        resume();
        return CompletableFuture.completedFuture(UnregisterResponse.builder()
          .withStatus(ProtocolResponse.Status.OK)
          .build());
      });

      connection.onCommand(request -> {
        threadAssertEquals(1L, request.session());
        threadAssertEquals(2L, request.sequence());
        resume();
        return CompletableFuture.completedFuture(CommandResponse.builder()
          .withStatus(ProtocolResponse.Status.OK)
          .withIndex(1)
          .withEventIndex(2)
          .withResult(new byte[0])
          .build());
      });

      connection.onQuery(request -> {
        threadAssertEquals(1L, request.session());
        threadAssertEquals(2L, request.sequence());
        resume();
        return CompletableFuture.completedFuture(QueryResponse.builder()
          .withStatus(ProtocolResponse.Status.OK)
          .withIndex(1)
          .withEventIndex(2)
          .withResult(new byte[0])
          .build());
      });

      connection.onAccept(request -> {
        threadAssertNotNull(request.client());
        threadAssertEquals("localhost", request.address().host());
        threadAssertEquals(1234, request.address().port());
        resume();
        return CompletableFuture.completedFuture(AcceptResponse.builder()
          .withStatus(ProtocolResponse.Status.OK)
          .build());
      });

      connection.onConfigure(request -> {
        threadAssertEquals(1, request.leader());
        threadAssertEquals(2L, request.term());
        threadAssertEquals(3L, request.index());
        threadAssertEquals(4L, request.timestamp());
        threadAssertNotNull(request.members());
        resume();
        return CompletableFuture.completedFuture(ConfigureResponse.builder()
          .withStatus(ProtocolResponse.Status.OK)
          .build());
      });

      connection.onReconfigure(request -> {
        threadAssertEquals(1L, request.index());
        threadAssertEquals(2L, request.term());
        threadAssertEquals(new Address("localhost", 1234).hashCode(), request.member().id());
        threadAssertEquals("localhost", request.member().address().host());
        threadAssertEquals(1234, request.member().address().port());
        resume();
        return CompletableFuture.completedFuture(ReconfigureResponse.builder()
          .withStatus(ProtocolResponse.Status.OK)
          .withIndex(1)
          .withTerm(2)
          .withTime(1000)
          .withMembers(Collections.emptyList())
          .build());
      });

      connection.onInstall(request -> {
        threadAssertEquals(1, request.leader());
        threadAssertEquals(2L, request.term());
        threadAssertEquals(3L, request.index());
        threadAssertEquals(4, request.offset());
        threadAssertNotNull(request.data());
        threadAssertTrue(request.complete());
        resume();
        return CompletableFuture.completedFuture(InstallResponse.builder()
          .withStatus(ProtocolResponse.Status.OK)
          .build());
      });

      connection.onJoin(request -> {
        threadAssertEquals(new Address("localhost", 1234).hashCode(), request.member().id());
        threadAssertEquals("localhost", request.member().address().host());
        threadAssertEquals(1234, request.member().address().port());
        resume();
        return CompletableFuture.completedFuture(JoinResponse.builder()
          .withStatus(ProtocolResponse.Status.OK)
          .withIndex(1)
          .withTerm(2)
          .withTime(1000)
          .withMembers(Collections.emptyList())
          .build());
      });

      connection.onLeave(request -> {
        threadAssertEquals(new Address("localhost", 1234).hashCode(), request.member().id());
        threadAssertEquals("localhost", request.member().address().host());
        threadAssertEquals(1234, request.member().address().port());
        resume();
        return CompletableFuture.completedFuture(LeaveResponse.builder()
          .withStatus(ProtocolResponse.Status.OK)
          .withIndex(1)
          .withTerm(2)
          .withTime(1000)
          .withMembers(Collections.emptyList())
          .build());
      });

      connection.onPoll(request -> {
        threadAssertEquals(1L, request.term());
        threadAssertEquals(2L, request.logIndex());
        threadAssertEquals(3L, request.logTerm());
        threadAssertEquals(4, request.candidate());
        resume();
        return CompletableFuture.completedFuture(PollResponse.builder()
          .withStatus(ProtocolResponse.Status.OK)
          .withTerm(1)
          .withAccepted(true)
          .build());
      });

      connection.onVote(request -> {
        threadAssertEquals(1L, request.term());
        threadAssertEquals(2L, request.logIndex());
        threadAssertEquals(3L, request.logTerm());
        threadAssertEquals(4, request.candidate());
        resume();
        return CompletableFuture.completedFuture(VoteResponse.builder()
          .withStatus(ProtocolResponse.Status.OK)
          .withTerm(1)
          .withVoted(true)
          .build());
      });

      connection.onAppend(request -> {
        threadAssertEquals(1, request.leader());
        threadAssertEquals(2L, request.term());
        threadAssertEquals(3L, request.logIndex());
        threadAssertEquals(4L, request.logTerm());
        threadAssertNotNull(request.entries());
        threadAssertEquals(5L, request.commitIndex());
        threadAssertEquals(6L, request.globalIndex());
        resume();
        return CompletableFuture.completedFuture(AppendResponse.builder()
          .withStatus(ProtocolResponse.Status.OK)
          .withTerm(1)
          .withLogIndex(2)
          .withSucceeded(true)
          .build());
      });
    }).thenRun(this::resume);
    await(5000);

    RaftProtocolClientConnection connection = client.connect(new Address("localhost", 5000)).join();

    connection.connect(ConnectRequest.builder()
      .withClient(UUID.randomUUID().toString())
      .build())
      .thenAccept(response -> {
        threadAssertEquals(ProtocolResponse.Status.OK, response.status());
        threadAssertEquals("localhost", response.leader().host());
        threadAssertEquals(1234, response.leader().port());
        threadAssertNotNull(response.members());
        resume();
      });

    connection.register(RegisterRequest.builder()
      .withClient(UUID.randomUUID().toString())
      .withTimeout(1000)
      .build())
      .thenAccept(response -> {
        threadAssertEquals(ProtocolResponse.Status.OK, response.status());
        threadAssertEquals("localhost", response.leader().host());
        threadAssertEquals(1234, response.leader().port());
        threadAssertEquals(1L, response.session());
        threadAssertEquals("localhost", response.members().iterator().next().host());
        threadAssertEquals(1234, response.members().iterator().next().port());
        threadAssertEquals(1000L, response.timeout());
        resume();
      });

    connection.keepAlive(KeepAliveRequest.builder()
      .withSession(1)
      .withCommandSequence(2)
      .withEventIndex(3)
      .build())
      .thenAccept(response -> {
        threadAssertEquals(ProtocolResponse.Status.OK, response.status());
        threadAssertEquals("localhost", response.leader().host());
        threadAssertEquals(1234, response.leader().port());
        threadAssertEquals("localhost", response.members().iterator().next().host());
        threadAssertEquals(1234, response.members().iterator().next().port());
        resume();
      });

    connection.unregister(UnregisterRequest.builder()
      .withSession(1)
      .build())
      .thenAccept(response -> {
        threadAssertEquals(ProtocolResponse.Status.OK, response.status());
        resume();
      });

    connection.command(CommandRequest.builder()
      .withSession(1)
      .withSequence(2)
      .withCommand(new byte[0])
      .build())
      .thenAccept(response -> {
        threadAssertEquals(ProtocolResponse.Status.OK, response.status());
        threadAssertEquals(1L, response.index());
        threadAssertEquals(2L, response.eventIndex());
        threadAssertNotNull(response.result());
        resume();
      });

    connection.query(QueryRequest.builder()
      .withSession(1)
      .withSequence(2)
      .withQuery(new byte[0])
      .withConsistency(ConsistencyLevel.LINEARIZABLE)
      .build())
      .thenAccept(response -> {
        threadAssertEquals(ProtocolResponse.Status.OK, response.status());
        threadAssertEquals(1L, response.index());
        threadAssertEquals(2L, response.eventIndex());
        threadAssertNotNull(response.result());
        resume();
      });

    connection.accept(AcceptRequest.builder()
      .withClient(UUID.randomUUID().toString())
      .withAddress(new Address("localhost", 1234))
      .build())
      .thenAccept(response -> {
        threadAssertEquals(ProtocolResponse.Status.OK, response.status());
        resume();
      });

    connection.configure(ConfigureRequest.builder()
      .withLeader(1)
      .withTerm(2)
      .withIndex(3)
      .withTime(4)
      .withMembers(Collections.emptyList())
      .build())
      .thenAccept(response -> {
        threadAssertEquals(ProtocolResponse.Status.OK, response.status());
        resume();
      });

    connection.reconfigure(ReconfigureRequest.builder()
      .withIndex(1)
      .withTerm(2)
      .withMember(new TestMember(new Address("localhost", 1234)))
      .build())
      .thenAccept(response -> {
        threadAssertEquals(ProtocolResponse.Status.OK, response.status());
        threadAssertEquals(1L, response.index());
        threadAssertEquals(2L, response.term());
        threadAssertEquals(1000L, response.timestamp());
        threadAssertNotNull(response.members());
        resume();
      });

    connection.install(InstallRequest.builder()
      .withLeader(1)
      .withTerm(2)
      .withIndex(3)
      .withOffset(4)
      .withData(new byte[0])
      .withComplete(true)
      .build())
      .thenAccept(response -> {
        threadAssertEquals(ProtocolResponse.Status.OK, response.status());
        resume();
      });

    connection.join(JoinRequest.builder()
      .withMember(new TestMember(new Address("localhost", 1234)))
      .build())
      .thenAccept(response -> {
        threadAssertEquals(ProtocolResponse.Status.OK, response.status());
        threadAssertEquals(1L, response.index());
        threadAssertEquals(2L, response.term());
        threadAssertEquals(1000L, response.timestamp());
        threadAssertNotNull(response.members());
        resume();
      });

    connection.leave(LeaveRequest.builder()
      .withMember(new TestMember(new Address("localhost", 1234)))
      .build())
      .thenAccept(response -> {
        threadAssertEquals(ProtocolResponse.Status.OK, response.status());
        threadAssertEquals(1L, response.index());
        threadAssertEquals(2L, response.term());
        threadAssertEquals(1000L, response.timestamp());
        threadAssertNotNull(response.members());
        resume();
      });

    connection.poll(PollRequest.builder()
      .withTerm(1)
      .withLogIndex(2)
      .withLogTerm(3)
      .withCandidate(4)
      .build())
      .thenAccept(response -> {
        threadAssertEquals(ProtocolResponse.Status.OK, response.status());
        threadAssertEquals(1L, response.term());
        threadAssertTrue(response.accepted());
        resume();
      });

    connection.vote(VoteRequest.builder()
      .withTerm(1)
      .withLogIndex(2)
      .withLogTerm(3)
      .withCandidate(4)
      .build())
      .thenAccept(response -> {
        threadAssertEquals(ProtocolResponse.Status.OK, response.status());
        threadAssertEquals(1L, response.term());
        threadAssertTrue(response.voted());
        resume();
      });

    connection.append(AppendRequest.builder()
      .withLeader(1)
      .withTerm(2)
      .withLogIndex(3)
      .withLogTerm(4)
      .withEntries(Collections.emptyList())
      .withCommitIndex(5)
      .withGlobalIndex(6)
      .build())
      .thenAccept(response -> {
        threadAssertEquals(ProtocolResponse.Status.OK, response.status());
        threadAssertEquals(1L, response.term());
        threadAssertEquals(2L, response.logIndex());
        threadAssertTrue(response.succeeded());
        resume();
      });

    await(5000, 30);
  }

  private static class TestMember implements Member {
    private final Address address;

    public TestMember(Address address) {
      this.address = address;
    }

    @Override
    public int id() {
      return address.hashCode();
    }

    @Override
    public Address address() {
      return address;
    }

    @Override
    public Address clientAddress() {
      return address;
    }

    @Override
    public Address serverAddress() {
      return address;
    }

    @Override
    public Type type() {
      return Type.ACTIVE;
    }

    @Override
    public Listener<Type> onTypeChange(Consumer<Type> callback) {
      return null;
    }

    @Override
    public Status status() {
      return Status.AVAILABLE;
    }

    @Override
    public Instant updated() {
      return Instant.now();
    }

    @Override
    public Listener<Status> onStatusChange(Consumer<Status> callback) {
      return null;
    }

    @Override
    public CompletableFuture<Void> promote() {
      return null;
    }

    @Override
    public CompletableFuture<Void> promote(Type type) {
      return null;
    }

    @Override
    public CompletableFuture<Void> demote() {
      return null;
    }

    @Override
    public CompletableFuture<Void> demote(Type type) {
      return null;
    }

    @Override
    public CompletableFuture<Void> remove() {
      return null;
    }
  }

}

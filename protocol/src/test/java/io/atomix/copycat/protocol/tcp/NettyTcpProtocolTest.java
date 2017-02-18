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
package io.atomix.copycat.protocol.tcp;

import io.atomix.copycat.ConsistencyLevel;
import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.protocol.ProtocolClient;
import io.atomix.copycat.protocol.ProtocolClientConnection;
import io.atomix.copycat.protocol.ProtocolServer;
import io.atomix.copycat.protocol.request.*;
import io.atomix.copycat.protocol.response.*;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Netty TCP based Raft protocol test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class NettyTcpProtocolTest extends ConcurrentTestCase {

  /**
   * Tests sending and receiving requests.
   */
  public void testSendReceive() throws Throwable {
    NettyTcpProtocol protocol = new NettyTcpProtocol();
    ProtocolClient client = protocol.createClient();
    ProtocolServer server = protocol.createServer();

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
    }).thenRun(this::resume);
    await(5000);

    ProtocolClientConnection connection = client.connect(new Address("localhost", 5000)).join();

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

    await(5000, 12);
  }

}

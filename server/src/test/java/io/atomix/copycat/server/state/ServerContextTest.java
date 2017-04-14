/*
 * Copyright 2015 the original author or authors.
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
 * limitations under the License.
 */
package io.atomix.copycat.server.state;

import io.atomix.catalyst.concurrent.SingleThreadContext;
import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.transport.Client;
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.transport.Server;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.transport.local.LocalServerRegistry;
import io.atomix.catalyst.transport.local.LocalTransport;
import io.atomix.copycat.protocol.Request;
import io.atomix.copycat.protocol.Response;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.function.Consumer;

/**
 * Server context test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class ServerContextTest extends AbstractStateTest<AbstractState> {
  private LocalServerRegistry registry;
  private Transport transport;
  private ThreadContext clientCtx;
  private Client client;
  private Server server;
  private Connection connection;

  /**
   * Sets up a server state.
   */
  @Override
  @BeforeMethod
  void beforeMethod() throws Throwable {
    super.beforeMethod();

    registry = new LocalServerRegistry();
    transport = new LocalTransport(registry);
    clientCtx = new SingleThreadContext("test-context", serializer.clone());

    server = transport.server();
    client = transport.client();

    serverCtx.execute(() -> {
      server.listen(members.get(0).clientAddress(), serverContext::connectClient).whenComplete((result, error) -> {
        threadAssertNull(error);
        resume();
      });
    });
    await();

    clientCtx.execute(() -> {
      client.connect(members.get(0).clientAddress()).whenComplete((result, error) -> {
        threadAssertNull(error);
        this.connection = result;
        resume();
      });
    });
    await();
  }

  /**
   * Clears test logs.
   */
  @Override
  @AfterMethod
  void afterMethod() throws Throwable {
    serverCtx.execute(() -> server.close().whenComplete((result, error) -> resume()));
    clientCtx.execute(() -> client.close().whenComplete((result, error) -> resume()));
    await(0, 2);
    clientCtx.close();

    super.afterMethod();
  }

  /**
   * Tests a server response.
   */
  private <T extends Request, U extends Response> void test(T request, Consumer<U> callback) throws Throwable {
    clientCtx.execute(() -> {
      connection.<T, U>sendAndReceive(request).whenComplete((response, error) -> {
        threadAssertNull(error);
        callback.accept(response);
        resume();
      });
    });
    await();
  }

}

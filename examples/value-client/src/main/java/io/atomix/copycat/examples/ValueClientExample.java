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
 * limitations under the License
 */
package io.atomix.copycat.examples;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.NettyTransport;
import io.atomix.copycat.client.CopycatClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Value client example.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class ValueClientExample {

  /**
   * Starts the client.
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 1)
      throw new IllegalArgumentException("must supply a path and set of host:port tuples");

    // Build a list of all member addresses to which to connect.
    List<Address> members = new ArrayList<>();
    for (int i = 0; i < args.length; i++) {
      String[] parts = args[i].split(":");
      members.add(new Address(parts[0], Integer.valueOf(parts[1])));
    }

    CopycatClient client = CopycatClient.builder(members)
      .withTransport(new NettyTransport())
      .build();

    client.connect().join();

    recursiveSet(client);

    while (client.state() != CopycatClient.State.CLOSED) {
      Thread.sleep(1000);
    }
  }

  /**
   * Recursively sets state machine values.
   */
  private static void recursiveSet(CopycatClient client) {
    client.submit(new SetCommand(UUID.randomUUID().toString())).thenRun(() -> {
      client.submit(new GetQuery()).thenAccept(value -> {
        client.context().schedule(Duration.ofMillis(10), () -> recursiveSet(client));
      });
    });
  }

}

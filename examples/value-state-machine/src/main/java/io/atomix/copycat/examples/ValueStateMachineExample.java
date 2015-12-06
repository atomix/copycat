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
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.storage.Storage;

import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Value state machine example.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class ValueStateMachineExample {

  /**
   * Starts the server.
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 2)
      throw new IllegalArgumentException("must supply a local port and at least one remote host:port tuple");

    int clientPort = Integer.valueOf(args[0]);
    int serverPort = Integer.valueOf(args[1]);

    Address clientAddress = new Address(InetAddress.getLocalHost().getHostName(), clientPort);
    Address serverAddress = new Address(InetAddress.getLocalHost().getHostName(), serverPort);

    List<Address> members = new ArrayList<>();
    for (int i = 1; i < args.length; i++) {
      String[] parts = args[i].split(":");
      members.add(new Address(parts[0], Integer.valueOf(parts[1])));
    }

    CopycatServer server = CopycatServer.builder(clientAddress, serverAddress, members)
      .withStateMachine(new ValueStateMachine())
      .withTransport(new NettyTransport())
      .withStorage(Storage.builder()
        .withDirectory(System.getProperty("user.dir") + "/logs/" + serverPort)
        // Limit the number of entries per segment and compaction intervals to demonstrate compaction.
        .withMaxEntriesPerSegment(1024)
        .withMinorCompactionInterval(Duration.ofSeconds(27))
        .withMajorCompactionInterval(Duration.ofSeconds(31))
        .build())
      .build();

    server.open().join();

    while (server.isOpen()) {
      Thread.sleep(1000);
    }
  }

}

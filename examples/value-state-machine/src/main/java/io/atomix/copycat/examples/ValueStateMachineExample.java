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
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.storage.Storage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Value state machine example. Expects at least 2 arguments:
 * 
 * <ul>
 * <li>path - the sub-directory to store log files in</li>
 * <li>host:port pairs - the host address and port of cluster members</li>
 * </ul>
 * <p>Example cluster arguments: <pre>logs 10.0.1.10:5000 10.0.1.11:5001 10.0.1.12:5002</pre>
 * <p>Example single node arguments: <pre>logs localhost:5000</pre>
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class ValueStateMachineExample {

  /**
   * Starts the server.
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 2)
      throw new IllegalArgumentException("must supply a path and set of host:port tuples");

    // Parse the address to which to bind the server.
    String[] mainParts = args[1].split(":");
    Address address = new Address(mainParts[0], Integer.valueOf(mainParts[1]));

    // Build a list of all member addresses to which to connect.
    List<Address> members = new ArrayList<>();
    for (int i = 1; i < args.length; i++) {
      String[] parts = args[i].split(":");
      members.add(new Address(parts[0], Integer.valueOf(parts[1])));
    }

    CopycatServer server = CopycatServer.builder(address)
      .withStateMachine(ValueStateMachine::new)
      .withTransport(new NettyTransport())
      .withStorage(Storage.builder()
        .withDirectory(args[0])
        .withMaxSegmentSize(1024 * 1024 * 32)
        .withMinorCompactionInterval(Duration.ofMinutes(1))
        .withMajorCompactionInterval(Duration.ofMinutes(15))
        .build())
      .build();

    server.serializer().register(SetCommand.class, 1);
    server.serializer().register(GetQuery.class, 2);
    server.serializer().register(DeleteCommand.class, 3);

    server.bootstrap(members).join();

    while (server.isRunning()) {
      Thread.sleep(1000);
    }
  }

}

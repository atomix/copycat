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
import io.atomix.copycat.client.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
    for (String arg : args) {
      String[] parts = arg.split(":");
      members.add(new Address(parts[0], Integer.valueOf(parts[1])));
    }

    CopycatClient client = CopycatClient.builder()
      .withTransport(new NettyTransport())
      .withConnectionStrategy(ConnectionStrategies.FIBONACCI_BACKOFF)
      .withRetryStrategy(RetryStrategies.FIBONACCI_BACKOFF)
      .withRecoveryStrategy(RecoveryStrategies.RECOVER)
      .withServerSelectionStrategy(ServerSelectionStrategies.LEADER)
      .build();

    client.serializer().register(SetCommand.class, 1);
    client.serializer().register(GetQuery.class, 2);
    client.serializer().register(DeleteCommand.class, 3);

    client.connect(members).join();

    AtomicInteger counter = new AtomicInteger();
    AtomicLong timer = new AtomicLong();
    client.context().schedule(Duration.ofSeconds(1), Duration.ofSeconds(1), () -> {
      long count = counter.get();
      long time = System.currentTimeMillis();
      long previousTime = timer.get();
      if (previousTime > 0) {
        System.out.println(String.format("Completed %d writes in %d milliseconds", count, time - previousTime));
      }
      counter.set(0);
      timer.set(time);
    });

    for (int i = 0; i < 10; i++) {
      recursiveSet(client, counter);
    }

    while (client.state() != CopycatClient.State.CLOSED) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        break;
      }
    }
  }

  /**
   * Recursively sets state machine values.
   */
  private static void recursiveSet(CopycatClient client, AtomicInteger counter) {
    client.submit(new SetCommand(UUID.randomUUID().toString())).thenRun(() -> {
      counter.incrementAndGet();
      recursiveSet(client, counter);
    });
  }

}

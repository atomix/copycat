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
package io.atomix.copycat.cli;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Copycat server runner.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class CopycatServerRunner implements Runnable {

  /**
   * Runs the Copycat server.
   *
   * @param args The command line arguments.
   */
  public static void main(String[] args) throws Exception {
    ArgumentParser parser = ArgumentParsers.newArgumentParser("Copycat")
      .defaultHelp(true)
      .description("Copycat server");
    parser.addArgument("state-machine")
      .required(true)
      .nargs(1)
      .help("The state machine class");
    parser.addArgument("--type", "-t")
      .choices(
        Member.Type.INACTIVE.name(),
        Member.Type.RESERVE.name(),
        Member.Type.PASSIVE.name(),
        Member.Type.ACTIVE.name())
      .help("The server type");
    parser.addArgument("--address", "-a")
      .required(true)
      .metavar("HOST:PORT")
      .help("The server address");
    parser.addArgument("--client-address", "-c")
      .required(true)
      .metavar("HOST:PORT")
      .help("The client address");
    parser.addArgument("--bootstrap", "-b")
      .nargs("*")
      .metavar("HOST:PORT")
      .help("Bootstraps a new cluster");
    parser.addArgument("--join", "-j")
      .nargs("+")
      .metavar("HOST:PORT")
      .help("Joins an existing cluster");
    parser.addArgument("--name", "-n")
      .metavar("STRING")
      .help("The server name")
      .setDefault("copycat");
    parser.addArgument("--log-dir", "-d")
      .metavar("FILE")
      .help("Server log directory")
      .setDefault(System.getProperty("user.dir"));
    parser.addArgument("--log-level", "-l")
      .metavar("LEVEL")
      .choices(
        StorageLevel.MEMORY.name(),
        StorageLevel.MAPPED.name(),
        StorageLevel.DISK.name())
      .help("The server storage level")
      .setDefault(StorageLevel.DISK.name());

    final Namespace namespace;
    try {
      namespace = parser.parseArgs(args);
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      System.exit(1);
      return;
    }

    new CopycatServerRunner(namespace).run();
  }

  private final Namespace namespace;

  private CopycatServerRunner(Namespace namespace) {
    this.namespace = namespace;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void run() {
    final Class<? extends StateMachine> stateMachineClass;
    try {
      stateMachineClass = (Class<? extends StateMachine>) Thread.currentThread()
        .getContextClassLoader().loadClass(namespace.getString("state-machine"));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    final String type = namespace.getString("type");
    final String address = namespace.getString("address");
    final String clientAddress = namespace.getString("client-address");
    final String name = namespace.getString("name");
    final String logDir = namespace.getString("log-dir");
    final String logLevel = namespace.getString("log-level");

    final CopycatServer.Builder builder;
    if (clientAddress != null) {
      builder = CopycatServer.builder(new Address(clientAddress), new Address(address))
        .withClientTransport(new NettyTransport());
    } else {
      builder = CopycatServer.builder(new Address(address));
    }

    builder.withName(name)
      .withType(Member.Type.valueOf(type))
      .withServerTransport(new NettyTransport())
      .withStateMachine(() -> {
        try {
          return stateMachineClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      })
      .withStorage(Storage.builder()
        .withStorageLevel(StorageLevel.valueOf(logLevel))
        .withDirectory(logDir)
        .build());

    CopycatServer server = builder.build();

    List<String> bootstrap = namespace.getList("bootstrap");
    if (bootstrap != null) {
      List<Address> cluster = bootstrap.stream().map(Address::new).collect(Collectors.toList());
      server.bootstrap(cluster).join();
    } else {
      List<String> join = namespace.getList("join");
      if (join != null) {
        List<Address> cluster = join.stream().map(Address::new).collect(Collectors.toList());
        server.join(cluster).join();
      } else {
        System.err.println("Must configure either -bootstrap or -join");
        System.exit(1);
      }
    }

    synchronized (CopycatServerRunner.class) {
      while (server.isRunning()) {
        try {
          CopycatServerRunner.class.wait();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}

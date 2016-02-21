/*
 * Copyright 2016 the original author or authors.
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
package io.atomix.copycat.server.session;

import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.session.ClosedSessionException;
import io.atomix.copycat.session.Session;

import java.util.concurrent.CompletableFuture;

/**
 * Provides an interface to communicating with a client via session events.
 * <p>
 * Each client that connects to a Raft cluster must open a {@link Session} in order to submit operations to the cluster.
 * When a client first connects to a server, it must register a new session. Once the session has been registered,
 * it can be used to submit {@link Command commands} and {@link Query queries}
 * or {@link #publish(String, Object) publish} session events.
 * <p>
 * Sessions represent a connection between a single client and all servers in a Raft cluster. Session information
 * is replicated via the Raft consensus algorithm, and clients can safely switch connections between servers without
 * losing their session. All consistency guarantees are provided within the context of a session. Once a session is
 * expired or closed, linearizability, sequential consistency, and other guarantees for events and operations are
 * effectively lost. Session implementations guarantee linearizability for session messages by coordinating between
 * the client and a single server at any given time. This means messages {@link #publish(String, Object) published}
 * via the {@link Session} are guaranteed to arrive on the other side of the connection exactly once and in the order
 * in which they are sent by replicated state machines. In the event of a server-to-client message being lost, the
 * message will be resent so long as at least one Raft server is able to communicate with the client and the client's
 * session does not expire while switching between servers.
 * <p>
 * Messages are sent to the other side of the session using the {@link #publish(String, Object)} method:
 * <pre>
 *   {@code
 *     session.publish("myEvent", "Hello world!");
 *   }
 * </pre>
 * When the message is published, it will be queued to be sent to the other side of the connection. Copycat guarantees
 * that the message will eventually be received by the client unless the session itself times out or is closed.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface ServerSession extends Session {

  /**
   * Publishes a {@code null} named event to the session.
   * <p>
   * When an event is published via the {@link Session}, it is sent to the other side of the session's
   * connection. Events can only be sent from a server-side replicated state machine to a client. Attempts
   * to send events from the client-side of the session will result in the event being handled by the client,
   * Sessions guarantee serializable consistency. If an event is sent from a Raft server to a client that is
   * disconnected or otherwise can't receive the event, the event will be resent once the client connects to
   * another server as long as its session has not expired.
   * <p>
   * Event messages must be serializable. For fast serialization, message types should implement
   * {@link io.atomix.catalyst.serializer.CatalystSerializable} or register a custom
   * {@link io.atomix.catalyst.serializer.TypeSerializer}. Normal Java {@link java.io.Serializable} and
   * {@link java.io.Externalizable} are supported but not recommended.
   * <p>
   * The returned {@link CompletableFuture} will be completed once the {@code event} has been sent
   * but not necessarily received by the other side of the connection. In the event of a network or other
   * failure, the message may be resent.
   *
   * @param event The event to publish.
   * @return A completable future to be called once the event has been published.
   * @throws NullPointerException If {@code event} is {@code null}
   * @throws ClosedSessionException If the session is closed
   * @throws io.atomix.catalyst.serializer.SerializationException If {@code message} cannot be serialized
   */
  Session publish(String event);

  /**
   * Publishes an event to the session.
   * <p>
   * When an event is published via the {@link Session}, it is sent to the other side of the session's
   * connection. Events can only be sent from a server-side replicated state machine to a client. Attempts
   * to send events from the client-side of the session will result in the event being handled by the client,
   * Sessions guarantee serializable consistency. If an event is sent from a Raft server to a client that is
   * disconnected or otherwise can't receive the event, the event will be resent once the client connects to
   * another server as long as its session has not expired.
   * <p>
   * Event messages must be serializable. For fast serialization, message types should implement
   * {@link io.atomix.catalyst.serializer.CatalystSerializable} or register a custom
   * {@link io.atomix.catalyst.serializer.TypeSerializer}. Normal Java {@link java.io.Serializable} and
   * {@link java.io.Externalizable} are supported but not recommended.
   * <p>
   * The returned {@link CompletableFuture} will be completed once the {@code event} has been sent
   * but not necessarily received by the other side of the connection. In the event of a network or other
   * failure, the message may be resent.
   *
   * @param event The event to publish.
   * @param message The event message. The message must be serializable either by implementing
   *               {@link io.atomix.catalyst.serializer.CatalystSerializable}, providing a
   *               {@link io.atomix.catalyst.serializer.TypeSerializer}, or implementing {@link java.io.Serializable}.
   * @return A completable future to be called once the event has been published.
   * @throws NullPointerException If {@code event} is {@code null}
   * @throws ClosedSessionException If the session is closed
   * @throws io.atomix.catalyst.serializer.SerializationException If {@code message} cannot be serialized
   */
  Session publish(String event, Object message);

}

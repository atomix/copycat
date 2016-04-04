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
package io.atomix.copycat;

/**
 * Wrapper for state machine events received by a client.
 * <p>
 * Events provide a mechanism for replicated state machines to communicate directly with clients via sessions.
 * Events are published to named listeners on the client. When an event is received by a client, all registered
 * event listeners are called, and the event listeners are responsible for {@link #complete() completing}
 * handling of the event.
 * <pre>
 *   {@code
 *   client.<ChangeEvent>onEvent("change", event -> {
 *     System.out.println(event.message().key() + " changed");
 *     event.complete();
 *   });
 *   }
 * </pre>
 * The {@link #complete()} method allows event listeners to complete the handling of events asynchronously.
 * It is critical that all events be completed. If an event is not eventually completed, a client will stop
 * receiving future events.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface Event<T> {

  /**
   * Returns the event name.
   *
   * @return The event name.
   */
  String name();

  /**
   * Returns the event message.
   *
   * @return The event message.
   */
  T message();

  /**
   * Acknowledges completion of the event.
   * <p>
   * Event listeners must complete all events.
   */
  void complete();

}

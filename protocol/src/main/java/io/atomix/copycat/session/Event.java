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
package io.atomix.copycat.session;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;

/**
 * Transports a named event from a server to a client {@link Session}.
 * <p>
 * Events are published by server state machines to client sessions as event objects. Each event sent to a session is
 * associated with a {@link String} event name and value.
 *
 * @see Session
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class Event<T> implements CatalystSerializable {
  private String event;
  private Object message;

  public Event() {
  }

  public Event(String event, Object message) {
    this.event = event;
    this.message = message;
  }

  /**
   * Returns the event name.
   *
   * @return The event name.
   */
  public String name() {
    return event;
  }

  /**
   * Returns the event message.
   *
   * @return The event message.
   */
  @SuppressWarnings("unchecked")
  public T message() {
    return (T) message;
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeUTF8(event);
    serializer.writeObject(message, buffer);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    event = buffer.readUTF8();
    message = serializer.readObject(buffer);
  }

  @Override
  public String toString() {
    return String.format("%s[event=%s, message=%s]", getClass().getSimpleName(), event, message);
  }

}

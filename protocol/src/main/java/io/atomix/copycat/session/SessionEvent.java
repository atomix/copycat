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
package io.atomix.copycat.session;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.copycat.Event;

import java.util.function.Consumer;

/**
 * Session event.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class SessionEvent<T> implements Event<T>, CatalystSerializable {
  private String event;
  private Object message;
  private transient long index;
  private transient Consumer<Long> acker;

  public SessionEvent() {
  }

  public SessionEvent(String event, Object message) {
    this.event = event;
    this.message = message;
  }

  @Override
  public String name() {
    return event;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T message() {
    return (T) message;
  }

  /**
   * Returns the event index.
   *
   * @return The event index.
   */
  public long index() {
    return index;
  }

  @Override
  public void complete() {
    acker.accept(index);
  }

  /**
   * Sets a callback to be called when the event is completed.
   *
   * @param callback A callback to be called when the event is completed.
   */
  public void onCompletion(long index, Consumer<Long> callback) {
    this.index = index;
    this.acker = callback;
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

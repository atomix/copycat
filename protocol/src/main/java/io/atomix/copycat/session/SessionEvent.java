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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Session event.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class SessionEvent<T> implements Event<T>, CatalystSerializable {
  private String event;
  private Object message;
  private final Runnable completer;
  private final AtomicBoolean complete;

  public SessionEvent() {
    this.completer = null;
    this.complete = null;
  }

  public SessionEvent(String event, Object message) {
    this.event = event;
    this.message = message;
    this.completer = null;
    this.complete = null;
  }

  public SessionEvent(String event, Object message, Runnable completer) {
    this.event = event;
    this.message = message;
    this.completer = completer;
    this.complete = new AtomicBoolean();
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

  @Override
  public void complete() {
    if (complete.compareAndSet(false, true)) {
      completer.run();
    }
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

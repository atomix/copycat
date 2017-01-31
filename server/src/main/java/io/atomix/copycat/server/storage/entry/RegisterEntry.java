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
 * limitations under the License.
 */
package io.atomix.copycat.server.storage.entry;

import io.atomix.copycat.util.Assert;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

/**
 * Stores a client register request.
 * <p>
 * The {@code RegisterEntry} is stored and replicated when a client submits a
 * {@link io.atomix.copycat.protocol.request.RegisterRequest} to the cluster to register a new session.
 * Session registrations are replicated and applied on all state machines to ensure each server state
 * machine has a consistent view of the sessions in the cluster, and registration entries are not
 * removed from the underlying log until the session itself has been expired or closed.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RegisterEntry extends TimestampedEntry<RegisterEntry> {
  private final String client;
  private final long timeout;

  public RegisterEntry(long timestamp, String client, long timeout) {
    super(timestamp);
    this.client = Assert.notNull(client, "client");
    this.timeout = Assert.argNot(timeout, timeout <= 0, "timeout must be positive");
  }

  @Override
  public Type<RegisterEntry> type() {
    return Type.REGISTER;
  }

  /**
   * Returns the entry client ID.
   *
   * @return The entry client ID.
   */
  public String client() {
    return client;
  }

  /**
   * Returns the session timeout.
   *
   * @return The session timeout.
   */
  public long timeout() {
    return timeout;
  }

  @Override
  public String toString() {
    return String.format("%s[client=%s, timeout=%d]", getClass().getSimpleName(), client(), timestamp());
  }

  /**
   * Register entry serializer.
   */
  public static class Serializer implements TimestampedEntry.Serializer<RegisterEntry> {
    @Override
    public void writeObject(BufferOutput output, RegisterEntry entry) {
      output.writeLong(entry.timestamp);
      output.writeString(entry.client);
      output.writeLong(entry.timeout);
    }

    @Override
    public RegisterEntry readObject(BufferInput input, Class<RegisterEntry> type) {
      return new RegisterEntry(input.readLong(), input.readString(), input.readLong());
    }
  }
}

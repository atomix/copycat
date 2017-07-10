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
package io.atomix.copycat.protocol;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;

import java.util.Objects;

/**
 * Connect client request.
 * <p>
 * Connect requests are sent by clients to specific servers when first establishing a connection.
 * Connections must be associated with a specific {@link #client() client ID} and must be established
 * each time the client switches servers. A client may only be connected to a single server at any
 * given time.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ConnectRequest extends AbstractRequest {
  public static final String NAME = "connect";

  /**
   * Returns a new connect client request builder.
   *
   * @return A new connect client request builder.
   */
  public static Builder builder() {
    return new Builder(new ConnectRequest());
  }

  /**
   * Returns a connect client request builder for an existing request.
   *
   * @param request The request to build.
   * @return The connect client request builder.
   * @throws NullPointerException if {@code request} is null
   */
  public static Builder builder(ConnectRequest request) {
    return new Builder(request);
  }

  private long session;
  private long connection;

  /**
   * Returns the connecting session ID.
   *
   * @return The connecting session ID.
   */
  public long session() {
    return session;
  }

  /**
   * Returns the connection ID.
   *
   * @return The connection ID.
   */
  public long connection() {
    return connection;
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeLong(session);
    buffer.writeLong(connection);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    session = buffer.readLong();
    connection = buffer.readLong();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session);
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof ConnectRequest && ((ConnectRequest) object).session == session;
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d, connection=%d]", getClass().getSimpleName(), session, connection);
  }

  /**
   * Register client request builder.
   */
  public static class Builder extends AbstractRequest.Builder<Builder, ConnectRequest> {
    protected Builder(ConnectRequest request) {
      super(request);
    }

    /**
     * Sets the connecting session ID.
     *
     * @param session The connecting session ID.
     * @return The connect request builder.
     */
    public Builder withSession(long session) {
      request.session = session;
      return this;
    }

    /**
     * Sets the connection ID.
     *
     * @param connection The connection ID.
     * @return The connect request builder.
     */
    public Builder withConnection(long connection) {
      request.connection = connection;
      return this;
    }
  }

}

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
package io.atomix.copycat.server.protocol;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.AbstractResponse;

import java.util.Objects;

/**
 * Configuration installation response.
 * <p>
 * Configuration installation responses are sent in response to configuration installation requests to
 * indicate the simple success of the installation of a configuration. If the response {@link #status()}
 * is {@link Status#OK} then the installation was successful.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ConfigureResponse extends AbstractResponse {

  /**
   * Returns a new configure response builder.
   *
   * @return A new configure response builder.
   */
  public static Builder builder() {
    return new Builder(new ConfigureResponse());
  }

  /**
   * Returns a configure response builder for an existing response.
   *
   * @param response The response to build.
   * @return The configure response builder.
   */
  public static Builder builder(ConfigureResponse response) {
    return new Builder(response);
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    status = Status.forId(buffer.readByte());
    if (status == Status.OK) {
      error = null;
    } else {
      error = CopycatError.forId(buffer.readByte());
    }
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    buffer.writeByte(status.id());
    if (status == Status.ERROR) {
      buffer.writeByte(error.id());
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof ConfigureResponse) {
      ConfigureResponse response = (ConfigureResponse) object;
      return response.status == status;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, error=%s]", getClass().getSimpleName(), status, error);
  }

  /**
   * Heartbeat response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, ConfigureResponse> {
    protected Builder(ConfigureResponse response) {
      super(response);
    }
  }

}

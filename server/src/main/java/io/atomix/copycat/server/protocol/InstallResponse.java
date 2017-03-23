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
 * Snapshot installation response.
 * <p>
 * Install responses are sent once a snapshot installation request has been received and processed.
 * Install responses provide no additional metadata aside from indicating whether or not the request
 * was successful.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class InstallResponse extends AbstractResponse {

  /**
   * Returns a new install response builder.
   *
   * @return A new install response builder.
   */
  public static Builder builder() {
    return new Builder(new InstallResponse());
  }

  /**
   * Returns a install response builder for an existing response.
   *
   * @param response The response to build.
   * @return The install response builder.
   */
  public static Builder builder(InstallResponse response) {
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
    if (object instanceof InstallResponse) {
      InstallResponse response = (InstallResponse) object;
      return response.status == status;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, error=%s]", getClass().getSimpleName(), status, error);
  }

  /**
   * Install response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, InstallResponse> {
    protected Builder(InstallResponse response) {
      super(response);
    }
  }

}

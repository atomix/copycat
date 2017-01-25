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
package io.atomix.copycat.server.protocol.response;

import io.atomix.copycat.protocol.response.AbstractResponse;
import io.atomix.copycat.protocol.response.ProtocolResponse;

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
  public InstallResponse(Status status, ProtocolResponse.Error error) {
    super(status, error);
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
    return String.format("%s[status=%s]", getClass().getSimpleName(), status);
  }

  /**
   * Install response builder.
   */
  public static class Builder extends AbstractResponse.Builder<InstallResponse.Builder, InstallResponse> {
    @Override
    public InstallResponse copy(InstallResponse response) {
      return new InstallResponse(response.status, response.error);
    }

    @Override
    public InstallResponse build() {
      return new InstallResponse(status, error);
    }
  }
}

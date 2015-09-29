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
package io.atomix.copycat.client.request;

import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.BuilderPool;
import io.atomix.catalyst.util.ReferenceManager;

import java.util.Objects;

/**
 * Protocol connect client request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=278)
public class ConnectRequest extends SessionRequest<ConnectRequest> {

  private static final BuilderPool<Builder, ConnectRequest> POOL = new BuilderPool<>(Builder::new);

  /**
   * Returns a new connect client request builder.
   *
   * @return A new connect client request builder.
   */
  public static Builder builder() {
    return POOL.acquire();
  }

  /**
   * Returns a connect client request builder for an existing request.
   *
   * @param request The request to build.
   * @return The connect client request builder.
   * @throws NullPointerException if {@code request} is null
   */
  public static Builder builder(ConnectRequest request) {
    return POOL.acquire(Assert.notNull(request, "request"));
  }

  /**
   * @throws NullPointerException if {@code referenceManager} is null
   */
  public ConnectRequest(ReferenceManager<ConnectRequest> referenceManager) {
    super(referenceManager);
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
    return String.format("%s", getClass().getSimpleName());
  }

  /**
   * Register client request builder.
   */
  public static class Builder extends SessionRequest.Builder<Builder, ConnectRequest> {

    /**
     * @throws NullPointerException if {@code pool} is null
     */
    protected Builder(BuilderPool<Builder, ConnectRequest> pool) {
      super(pool, ConnectRequest::new);
    }
  }

}

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
package io.atomix.copycat.server.protocol.request;

import io.atomix.copycat.util.Assert;
import io.atomix.copycat.server.cluster.Member;

import java.util.Objects;

/**
 * Member configuration change request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class ReconfigureRequest extends ConfigurationRequest {
  protected final long index;
  protected final long term;

  public ReconfigureRequest(Member member, long index, long term) {
    super(member);
    this.index = Assert.argNot(index, index < 0, "index must be positive");
    this.term = Assert.argNot(term, term < 0, "term must be positive");
  }

  /**
   * Returns the configuration index.
   *
   * @return The configuration index.
   */
  public long index() {
    return index;
  }

  /**
   * Returns the configuration term.
   *
   * @return The configuration term.
   */
  public long term() {
    return term;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), index, member);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof ReconfigureRequest) {
      ReconfigureRequest request = (ReconfigureRequest) object;
      return request.index == index && request.term == term && request.member.equals(member);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, term=%d, member=%s]", getClass().getSimpleName(), index, term, member);
  }

  /**
   * Reconfigure request builder.
   */
  public static class Builder extends ConfigurationRequest.Builder<ReconfigureRequest.Builder, ReconfigureRequest> {
    protected long index;
    protected long term;

    /**
     * Sets the request index.
     *
     * @param index The request index.
     * @return The request builder.
     */
    public Builder withIndex(long index) {
      this.index = Assert.argNot(index, index < 0, "index must be positive");
      return this;
    }

    /**
     * Sets the request term.
     *
     * @param term The request term.
     * @return The request builder.
     */
    public Builder withTerm(long term) {
      this.term = Assert.argNot(term, term < 0, "term must be positive");
      return this;
    }

    @Override
    public ReconfigureRequest copy(ReconfigureRequest request) {
      return new ReconfigureRequest(request.member, request.index, request.term);
    }

    @Override
    public ReconfigureRequest build() {
      return new ReconfigureRequest(member, index, term);
    }
  }
}

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
package io.atomix.copycat.server.protocol.local.request;

import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.protocol.request.ReconfigureRequest;

import java.util.Objects;

/**
 * Member configuration change request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class LocalReconfigureRequest extends LocalConfigurationRequest implements ReconfigureRequest {
  private final long index;
  private final long term;

  public LocalReconfigureRequest(Member member, long index, long term) {
    super(member);
    this.index = index;
    this.term = term;
  }

  @Override
  public long index() {
    return index;
  }

  @Override
  public long term() {
    return term;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), index, member);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof LocalReconfigureRequest) {
      LocalReconfigureRequest request = (LocalReconfigureRequest) object;
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
  public static class Builder extends LocalConfigurationRequest.Builder<ReconfigureRequest.Builder, ReconfigureRequest> implements ReconfigureRequest.Builder {
    private long index;
    private long term;

    @Override
    public Builder withIndex(long index) {
      this.index = Assert.argNot(index, index < 0, "index must be positive");
      return this;
    }

    @Override
    public Builder withTerm(long term) {
      this.term = Assert.argNot(term, term < 0, "term must be positive");
      return this;
    }

    @Override
    public ReconfigureRequest build() {
      return new LocalReconfigureRequest(member, index, term);
    }
  }
}

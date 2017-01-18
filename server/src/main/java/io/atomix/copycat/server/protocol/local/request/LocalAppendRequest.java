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
import io.atomix.copycat.protocol.local.request.AbstractLocalRequest;
import io.atomix.copycat.server.protocol.request.AppendRequest;
import io.atomix.copycat.server.storage.entry.Entry;

import java.util.List;
import java.util.Objects;

/**
 * Append entries request.
 * <p>
 * Append entries requests are at the core of the replication protocol. Leaders send append requests
 * to followers to replicate and commit log entries, and followers sent append requests to passive members
 * to replicate committed log entries.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalAppendRequest extends AbstractLocalRequest implements AppendRequest {
  private final long term;
  private final int leader;
  private final long logIndex;
  private final long logTerm;
  private final List<Entry> entries;
  private final long commitIndex;
  private final long globalIndex;

  public LocalAppendRequest(long term, int leader, long logIndex, long logTerm, List<Entry> entries, long commitIndex, long globalIndex) {
    this.term = term;
    this.leader = leader;
    this.logIndex = logIndex;
    this.logTerm = logTerm;
    this.entries = entries;
    this.commitIndex = commitIndex;
    this.globalIndex = globalIndex;
  }

  @Override
  public long term() {
    return term;
  }

  @Override
  public int leader() {
    return leader;
  }

  @Override
  public long logIndex() {
    return logIndex;
  }

  @Override
  public long logTerm() {
    return logTerm;
  }

  @Override
  public List<? extends Entry> entries() {
    return entries;
  }

  @Override
  public long commitIndex() {
    return commitIndex;
  }

  @Override
  public long globalIndex() {
    return globalIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), term, leader, logIndex, logTerm, entries, commitIndex, globalIndex);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof LocalAppendRequest) {
      LocalAppendRequest request = (LocalAppendRequest) object;
      return request.term == term
        && request.leader == leader
        && request.logIndex == logIndex
        && request.logTerm == logTerm
        && request.entries.equals(entries)
        && request.commitIndex == commitIndex
        && request.globalIndex == globalIndex;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[term=%d, leader=%s, logIndex=%d, logTerm=%d, entries=[%d], commitIndex=%d, globalIndex=%d]", getClass().getSimpleName(), term, leader, logIndex, logTerm, entries.size(), commitIndex, globalIndex);
  }

  /**
   * Append request builder.
   */
  public static class Builder extends AbstractLocalRequest.Builder<AppendRequest.Builder, AppendRequest> implements AppendRequest.Builder {
    private long term;
    private int leader;
    private long logIndex;
    private long logTerm;
    private List<Entry> entries;
    private long commitIndex = -1;
    private long globalIndex = -1;

    @Override
    public Builder withTerm(long term) {
      this.term = Assert.arg(term, term > 0, "term must be positive");
      return this;
    }

    @Override
    public Builder withLeader(int leader) {
      this.leader = leader;
      return this;
    }

    @Override
    public Builder withLogIndex(long index) {
      this.logIndex = Assert.argNot(index, index < 0, "log index must be not be negative");
      return this;
    }

    @Override
    public Builder withLogTerm(long term) {
      this.logTerm = Assert.argNot(term, term < 0, "term must be positive");
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Builder withEntries(List<? extends Entry> entries) {
      this.entries = (List<Entry>) Assert.notNull(entries, "entries");
      return this;
    }

    @Override
    public Builder addEntry(Entry entry) {
      this.entries.add(Assert.notNull(entry, "entry"));
      return this;
    }

    @Override
    public Builder withCommitIndex(long index) {
      this.commitIndex = Assert.argNot(index, index < 0, "commit index must not be negative");
      return this;
    }

    @Override
    public Builder withGlobalIndex(long index) {
      this.globalIndex = Assert.argNot(index, index < 0, "global index must not be negative");
      return this;
    }

    @Override
    public LocalAppendRequest build() {
      return new LocalAppendRequest(term, leader, logIndex, logTerm, entries, commitIndex, globalIndex);
    }
  }
}

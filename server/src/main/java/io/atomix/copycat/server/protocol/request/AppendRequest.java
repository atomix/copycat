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
import io.atomix.copycat.protocol.request.AbstractRequest;
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
public class AppendRequest extends AbstractRequest {
  protected final long term;
  protected final int leader;
  protected final long logIndex;
  protected final long logTerm;
  protected final List<Entry> entries;
  protected final long commitIndex;
  protected final long globalIndex;

  public AppendRequest(long term, int leader, long logIndex, long logTerm, List<Entry> entries, long commitIndex, long globalIndex) {
    this.term = Assert.arg(term, term > 0, "term must be positive");
    this.leader = leader;
    this.logIndex = Assert.argNot(logIndex, logIndex < 0, "logIndex must be not be negative");
    this.logTerm = Assert.argNot(logTerm, logTerm < 0, "logTerm must be positive");
    this.entries = Assert.notNull(entries, "entries");
    this.commitIndex = Assert.argNot(commitIndex, commitIndex < 0, "commitIndex must not be negative");
    this.globalIndex = Assert.argNot(globalIndex, globalIndex < 0, "globalIndex must not be negative");
  }

  /**
   * Returns the requesting node's current term.
   *
   * @return The requesting node's current term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns the requesting leader ID.
   *
   * @return The leader's ID.
   */
  public int leader() {
    return leader;
  }

  /**
   * Returns the index of the log entry preceding the new entry.
   *
   * @return The index of the log entry preceding the new entry.
   */
  public long logIndex() {
    return logIndex;
  }

  /**
   * Returns the term of the log entry preceding the new entry.
   *
   * @return The index of the term preceding the new entry.
   */
  public long logTerm() {
    return logTerm;
  }

  /**
   * Returns the log entries to append.
   *
   * @return A list of log entries.
   */
  public List<Entry> entries() {
    return entries;
  }

  /**
   * Returns the leader's commit index.
   *
   * @return The leader commit index.
   */
  public long commitIndex() {
    return commitIndex;
  }

  /**
   * Returns the leader's global index.
   *
   * @return The leader global index.
   */
  public long globalIndex() {
    return globalIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), term, leader, logIndex, logTerm, entries, commitIndex, globalIndex);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof AppendRequest) {
      AppendRequest request = (AppendRequest) object;
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
  public static class Builder extends AbstractRequest.Builder<AppendRequest.Builder, AppendRequest> {
    protected long term;
    protected int leader;
    protected long logIndex;
    protected long logTerm;
    protected List<Entry> entries;
    protected long commitIndex = -1;
    protected long globalIndex = -1;

    /**
     * Sets the request term.
     *
     * @param term The request term.
     * @return The append request builder.
     * @throws IllegalArgumentException if the {@code term} is not positive
     */
    public Builder withTerm(long term) {
      this.term = Assert.arg(term, term > 0, "term must be positive");
      return this;
    }

    /**
     * Sets the request leader.
     *
     * @param leader The request leader.
     * @return The append request builder.
     * @throws IllegalArgumentException if the {@code leader} is not positive
     */
    public Builder withLeader(int leader) {
      this.leader = leader;
      return this;
    }

    /**
     * Sets the request last log index.
     *
     * @param index The request last log index.
     * @return The append request builder.
     * @throws IllegalArgumentException if the {@code index} is not positive
     */
    public Builder withLogIndex(long index) {
      this.logIndex = Assert.argNot(index, index < 0, "log index must be not be negative");
      return this;
    }

    /**
     * Sets the request last log term.
     *
     * @param term The request last log term.
     * @return The append request builder.
     * @throws IllegalArgumentException if the {@code term} is not positive
     */
    public Builder withLogTerm(long term) {
      this.logTerm = Assert.argNot(term, term < 0, "term must be positive");
      return this;
    }

    /**
     * Sets the request entries.
     *
     * @param entries The request entries.
     * @return The append request builder.
     * @throws NullPointerException if {@code entries} is null
     */
    @SuppressWarnings("unchecked")
    public Builder withEntries(List<? extends Entry> entries) {
      this.entries = (List<Entry>) Assert.notNull(entries, "entries");
      return this;
    }

    /**
     * Sets the request commit index.
     *
     * @param index The request commit index.
     * @return The append request builder.
     * @throws IllegalArgumentException if index is not positive
     */
    public Builder withCommitIndex(long index) {
      this.commitIndex = Assert.argNot(index, index < 0, "commit index must not be negative");
      return this;
    }


    /**
     * Sets the request global index.
     *
     * @param index The request global index.
     * @return The append request builder.
     * @throws IllegalArgumentException if index is not positive
     */
    public Builder withGlobalIndex(long index) {
      this.globalIndex = Assert.argNot(index, index < 0, "global index must not be negative");
      return this;
    }

    @Override
    public AppendRequest copy(AppendRequest request) {
      return new AppendRequest(request.term, request.leader, request.logIndex, request.logTerm, request.entries, request.commitIndex, request.globalIndex);
    }

    @Override
    public AppendRequest build() {
      return new AppendRequest(term, leader, logIndex, logTerm, entries, commitIndex, globalIndex);
    }
  }
}

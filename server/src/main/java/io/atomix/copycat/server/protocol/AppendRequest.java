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
package io.atomix.copycat.server.protocol;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.protocol.AbstractRequest;
import io.atomix.copycat.server.storage.entry.Entry;

import java.util.ArrayList;
import java.util.Arrays;
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

  /**
   * Returns a new append request builder.
   *
   * @return A new append request builder.
   */
  public static Builder builder() {
    return new Builder(new AppendRequest());
  }

  /**
   * Returns an append request builder for an existing request.
   *
   * @param request The request to build.
   * @return The append request builder.
   */
  public static Builder builder(AppendRequest request) {
    return new Builder(request);
  }

  private long term;
  private int leader;
  private long logIndex;
  private long logTerm;
  private List<Entry> entries;
  private long commitIndex = -1;
  private long globalIndex = -1;

  /**
   * Returns the requesting node's current term.
   *
   * @return The requesting node's current term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns the requesting leader address.
   *
   * @return The leader's address.
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
  public List<? extends Entry> entries() {
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
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeLong(term)
      .writeInt(leader)
      .writeLong(logIndex)
      .writeLong(logTerm)
      .writeLong(commitIndex)
      .writeLong(globalIndex);

    buffer.writeInt(entries.size());
    for (Entry entry : entries) {
      buffer.writeLong(entry.getIndex()).writeLong(entry.getTerm());
      serializer.writeObject(entry, buffer);
    }
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    term = buffer.readLong();
    leader = buffer.readInt();
    logIndex = buffer.readLong();
    logTerm = buffer.readLong();
    commitIndex = buffer.readLong();
    globalIndex = buffer.readLong();

    int numEntries = buffer.readInt();
    entries = new ArrayList<>(numEntries);
    for (int i = 0; i < numEntries; i++) {
      long index = buffer.readLong();
      long term = buffer.readLong();
      Entry entry = serializer.readObject(buffer);
      entry.setIndex(index).setTerm(term);
      entries.add(entry);
    }
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
  public static class Builder extends AbstractRequest.Builder<Builder, AppendRequest> {
    protected Builder(AppendRequest request) {
      super(request);
    }

    /**
     * Sets the request term.
     *
     * @param term The request term.
     * @return The append request builder.
     * @throws IllegalArgumentException if the {@code term} is not positive
     */
    public Builder withTerm(long term) {
      request.term = Assert.arg(term, term > 0, "term must be positive");
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
      request.leader = leader;
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
      request.logIndex = Assert.argNot(index, index < 0, "log index must be not be negative");
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
      request.logTerm = Assert.argNot(term, term < 0, "term must be positive");
      return this;
    }

    /**
     * Sets the request entries.
     *
     * @param entries The request entries.
     * @return The append request builder.
     * @throws NullPointerException if {@code entries} is null
     */
    public Builder withEntries(Entry... entries) {
      return withEntries(Arrays.asList(Assert.notNull(entries, "entries")));
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
      request.entries = (List<Entry>) Assert.notNull(entries, "entries");
      return this;
    }

    /**
     * Adds an entry to the request.
     *
     * @param entry The entry to add.
     * @return The request builder.
     * @throws NullPointerException if {@code entry} is {@code null}
     */
    public Builder addEntry(Entry entry) {
      request.entries.add(Assert.notNull(entry, "entry"));
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
      request.commitIndex = Assert.argNot(index, index < 0, "commit index must not be negative");
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
      request.globalIndex = Assert.argNot(index, index < 0, "global index must not be negative");
      return this;
    }

    /**
     * @throws IllegalStateException if the term, log term, log index, commit index, or global index are not positive, or
     * if entries is null
     */
    @Override
    public AppendRequest build() {
      super.build();
      Assert.stateNot(request.term <= 0, "term must be positive");
      Assert.stateNot(request.logIndex < 0, "log index must not be negative");
      if (request.logIndex > 0) {
        Assert.stateNot(request.logTerm == 0, "log term must be specified");
      } else {
        Assert.stateNot(request.logTerm < 0, "log term must not be negative");
      }
      Assert.stateNot(request.entries == null, "entries cannot be null");
      Assert.stateNot(request.commitIndex < 0, "commit index must not be negative");
      Assert.stateNot(request.globalIndex < 0, "global index must not be negative");
      return request;
    }

    @Override
    public int hashCode() {
      return Objects.hash(request);
    }

    @Override
    public boolean equals(Object object) {
      return object instanceof Builder && ((Builder) object).request.equals(request);
    }

    @Override
    public String toString() {
      return String.format("%s[request=%s]", getClass().getCanonicalName(), request);
    }

  }

}

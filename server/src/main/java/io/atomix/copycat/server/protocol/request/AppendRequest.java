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

import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.server.storage.entry.Entry;

import java.util.Arrays;
import java.util.List;

/**
 * Append entries request.
 * <p>
 * Append entries requests are at the core of the replication protocol. Leaders send append requests
 * to followers to replicate and commit log entries, and followers sent append requests to passive members
 * to replicate committed log entries.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface AppendRequest extends RaftProtocolRequest {

  /**
   * Returns the requesting node's current term.
   *
   * @return The requesting node's current term.
   */
  long term();

  /**
   * Returns the requesting leader address.
   *
   * @return The leader's address.
   */
  int leader();

  /**
   * Returns the index of the log entry preceding the new entry.
   *
   * @return The index of the log entry preceding the new entry.
   */
  long logIndex();

  /**
   * Returns the term of the log entry preceding the new entry.
   *
   * @return The index of the term preceding the new entry.
   */
  long logTerm();

  /**
   * Returns the log entries to append.
   *
   * @return A list of log entries.
   */
  List<? extends Entry> entries();

  /**
   * Returns the leader's commit index.
   *
   * @return The leader commit index.
   */
  long commitIndex();

  /**
   * Returns the leader's global index.
   *
   * @return The leader global index.
   */
  long globalIndex();

  /**
   * Append request builder.
   */
  interface Builder extends RaftProtocolRequest.Builder<Builder, AppendRequest> {

    /**
     * Sets the request term.
     *
     * @param term The request term.
     * @return The append request builder.
     * @throws IllegalArgumentException if the {@code term} is not positive
     */
    Builder withTerm(long term);

    /**
     * Sets the request leader.
     *
     * @param leader The request leader.
     * @return The append request builder.
     * @throws IllegalArgumentException if the {@code leader} is not positive
     */
    Builder withLeader(int leader);

    /**
     * Sets the request last log index.
     *
     * @param index The request last log index.
     * @return The append request builder.
     * @throws IllegalArgumentException if the {@code index} is not positive
     */
    Builder withLogIndex(long index);

    /**
     * Sets the request last log term.
     *
     * @param term The request last log term.
     * @return The append request builder.
     * @throws IllegalArgumentException if the {@code term} is not positive
     */
    Builder withLogTerm(long term);

    /**
     * Sets the request entries.
     *
     * @param entries The request entries.
     * @return The append request builder.
     * @throws NullPointerException if {@code entries} is null
     */
    default Builder withEntries(Entry... entries) {
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
    Builder withEntries(List<? extends Entry> entries);

    /**
     * Adds an entry to the request.
     *
     * @param entry The entry to add.
     * @return The request builder.
     * @throws NullPointerException if {@code entry} is {@code null}
     */
    Builder addEntry(Entry entry);

    /**
     * Sets the request commit index.
     *
     * @param index The request commit index.
     * @return The append request builder.
     * @throws IllegalArgumentException if index is not positive
     */
    Builder withCommitIndex(long index);

    /**
     * Sets the request global index.
     *
     * @param index The request global index.
     * @return The append request builder.
     * @throws IllegalArgumentException if index is not positive
     */
    Builder withGlobalIndex(long index);
  }

}

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

/**
 * Server snapshot installation request.
 * <p>
 * Snapshot installation requests are sent by the leader to a follower when the follower indicates
 * that its log is further behind than the last snapshot taken by the leader. Snapshots are sent
 * in chunks, with each chunk being sent in a separate install request. As requests are received by
 * the follower, the snapshot is reconstructed based on the provided {@link #offset()} and other
 * metadata. The last install request will be sent with {@link #complete()} being {@code true} to
 * indicate that all chunks of the snapshot have been sent.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface InstallRequest extends RaftProtocolRequest {

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
   * Returns the snapshot index.
   *
   * @return The snapshot index.
   */
  long index();

  /**
   * Returns the offset of the snapshot chunk.
   *
   * @return The offset of the snapshot chunk.
   */
  int offset();

  /**
   * Returns the snapshot data.
   *
   * @return The snapshot data.
   */
  byte[] data();

  /**
   * Returns a boolean value indicating whether this is the last chunk of the snapshot.
   *
   * @return Indicates whether this request is the last chunk of the snapshot.
   */
  boolean complete();

  /**
   * Snapshot request builder.
   */
  interface Builder extends RaftProtocolRequest.Builder<Builder, InstallRequest> {

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
     * Sets the request index.
     *
     * @param index The request index.
     * @return The request builder.
     */
    Builder withIndex(long index);

    /**
     * Sets the request offset.
     *
     * @param offset The request offset.
     * @return The request builder.
     */
    Builder withOffset(int offset);

    /**
     * Sets the request snapshot bytes.
     *
     * @param snapshot The snapshot bytes.
     * @return The request builder.
     */
    Builder withData(byte[] snapshot);

    /**
     * Sets whether the request is complete.
     *
     * @param complete Whether the snapshot is complete.
     * @return The request builder.
     * @throws NullPointerException if {@code member} is null
     */
    Builder withComplete(boolean complete);
  }

}

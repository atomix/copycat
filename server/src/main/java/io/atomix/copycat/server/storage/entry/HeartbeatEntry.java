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
package io.atomix.copycat.server.storage.entry;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.ReferenceManager;

/**
 * Server heartbeat entry.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@SerializeWith(id=229)
public class HeartbeatEntry extends TimestampedEntry<HeartbeatEntry> {
  private int member;
  private long commitIndex;

  public HeartbeatEntry() {
  }

  public HeartbeatEntry(ReferenceManager<Entry<?>> referenceManager) {
    super(referenceManager);
  }

  /**
   * Sets the heartbeat member.
   *
   * @param member The heartbeat member.
   * @return The heartbeat entry.
   */
  public HeartbeatEntry setMember(int member) {
    this.member = member;
    return this;
  }

  /**
   * Returns the heartbeat member.
   *
   * @return The heartbeat member.
   */
  public int getMember() {
    return member;
  }

  /**
   * Sets the member commit index.
   *
   * @param commitIndex The member commit index.
   * @return The heartbeat entry.
   */
  public HeartbeatEntry setCommitIndex(long commitIndex) {
    this.commitIndex = Assert.argNot(commitIndex, commitIndex < 0, "commitIndex must be positive");
    return this;
  }

  /**
   * Returns the member commit index.
   *
   * @return The member commit index.
   */
  public long getCommitIndex() {
    return commitIndex;
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeInt(member);
    buffer.writeLong(commitIndex);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    member = buffer.readInt();
    commitIndex = buffer.readLong();
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, term=%d, member=%d, commitIndex=%d, timestamp=%d]", getClass().getSimpleName(), getIndex(), getTerm(), getMember(), getCommitIndex(), getTimestamp());
  }

}

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
import io.atomix.catalyst.util.ReferenceManager;

/**
 * Server heartbeat entry.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@SerializeWith(id=228)
public class HeartbeatEntry extends TimestampedEntry<HeartbeatEntry> {
  private int member;

  public HeartbeatEntry() {
  }

  public HeartbeatEntry(ReferenceManager<Entry<?>> referenceManager) {
    super(referenceManager);
  }

  public HeartbeatEntry setMember(int member) {
    this.member = member;
    return this;
  }

  public int getMember() {
    return member;
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeInt(member);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    member = buffer.readInt();
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, term=%d, member=%d, timestamp=%d]", getClass().getSimpleName(), getIndex(), getTerm(), getMember(), getTimestamp());
  }

}

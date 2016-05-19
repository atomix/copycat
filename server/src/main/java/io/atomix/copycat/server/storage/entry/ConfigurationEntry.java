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
package io.atomix.copycat.server.storage.entry;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.reference.ReferenceManager;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.storage.compaction.Compaction;

import java.util.Collection;

/**
 * Stores a cluster configuration.
 * <p>
 * The {@code ConfigurationEntry} stores information relevant to a single cluster configuration change.
 * Configuration change entries store a collection of {@link Member members} which each represent a
 * server in the cluster. Each time the set of members changes or a property of a single member changes,
 * a new {@code ConfigurationEntry} must be logged for the configuration change.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ConfigurationEntry extends TimestampedEntry<ConfigurationEntry> {
  private Collection<Member> members;

  public ConfigurationEntry() {
  }

  public ConfigurationEntry(ReferenceManager<Entry<?>> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Compaction.Mode getCompactionMode() {
    return Compaction.Mode.FULL;
  }

  /**
   * Returns the members.
   *
   * @return The members.
   */
  public Collection<Member> getMembers() {
    return members;
  }

  /**
   * Sets the members.
   *
   * @param members The members.
   * @return The configuration entry.
   * @throws NullPointerException if {@code members} is null
   */
  public ConfigurationEntry setMembers(Collection<Member> members) {
    this.members = Assert.notNull(members, "members");
    return this;
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    serializer.writeObject(members, buffer);
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    members = serializer.readObject(buffer);
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, term=%d, timestamp=%d, members=%s]", getClass().getSimpleName(), getIndex(), getTerm(), getTimestamp(), members);
  }

}

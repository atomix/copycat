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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.storage.compaction.Compaction;
import io.atomix.copycat.util.Assert;

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
  private final Collection<Member> members;

  public ConfigurationEntry(long timestamp, Collection<Member> members) {
    super(timestamp);
    this.members = Assert.notNull(members, "members");
  }

  @Override
  public Type<ConfigurationEntry> type() {
    return Type.CONFIGURATION;
  }

  @Override
  public Compaction.Mode compaction() {
    return Compaction.Mode.FULL;
  }

  /**
   * Returns the members.
   *
   * @return The members.
   */
  public Collection<Member> members() {
    return members;
  }

  @Override
  public String toString() {
    return String.format("%s[timestamp=%d, members=%s]", getClass().getSimpleName(), timestamp(), members);
  }

  /**
   * Configuration entry serializer.
   */
  public static class Serializer extends TimestampedEntry.Serializer<ConfigurationEntry> {
    @Override
    public void write(Kryo kryo, Output output, ConfigurationEntry entry) {
      output.writeLong(entry.timestamp);
      kryo.writeClassAndObject(output, entry.members);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ConfigurationEntry read(Kryo kryo, Input input, Class<ConfigurationEntry> type) {
      return new ConfigurationEntry(input.readLong(), (Collection<Member>) kryo.readClassAndObject(input));
    }
  }
}

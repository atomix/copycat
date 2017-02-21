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

import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.state.ServerMember;
import io.atomix.copycat.util.Assert;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
  public static class Serializer implements TimestampedEntry.Serializer<ConfigurationEntry> {
    @Override
    public void writeObject(BufferOutput output, ConfigurationEntry entry) {
      output.writeLong(entry.timestamp);
      output.writeInt(entry.members.size());
      for (Member member : entry.members) {
        output.writeByte(member.type().ordinal());
        output.writeByte(member.status().ordinal());
        output.writeString(member.serverAddress().host()).writeInt(member.serverAddress().port());
        if (member.clientAddress() != null) {
          output.writeBoolean(true)
            .writeString(member.clientAddress().host())
            .writeInt(member.clientAddress().port());
        } else {
          output.writeBoolean(false);
        }
        output.writeLong(member.updated().toEpochMilli());
      }
    }

    @Override
    public ConfigurationEntry readObject(BufferInput input, Class<ConfigurationEntry> type) {
      long timestamp = input.readLong();
      int size = input.readInt();
      List<Member> members = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        Member.Type memberType = Member.Type.values()[input.readByte()];
        Member.Status memberStatus = Member.Status.values()[input.readByte()];
        Address serverAddress = new Address(input.readString(), input.readInt());
        Address clientAddress = null;
        if (input.readBoolean()) {
          clientAddress = new Address(input.readString(), input.readInt());
        }
        Instant updated = Instant.ofEpochMilli(input.readLong());
        members.add(new ServerMember(memberType, memberStatus, serverAddress, clientAddress, updated));
      }
      return new ConfigurationEntry(timestamp, members);
    }
  }
}

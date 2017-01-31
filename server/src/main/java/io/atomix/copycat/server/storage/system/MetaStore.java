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
package io.atomix.copycat.server.storage.system;

import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.state.ServerMember;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.copycat.util.Assert;
import io.atomix.copycat.util.buffer.Buffer;
import io.atomix.copycat.util.buffer.FileBuffer;
import io.atomix.copycat.util.buffer.HeapBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Manages persistence of server configurations.
 * <p>
 * The server metastore is responsible for persisting server configurations according to the configured
 * {@link Storage#level() storage level}. Each server persists their current {@link #loadTerm() term}
 * and last {@link #loadVote() vote} as is dictated by the Raft consensus algorithm. Additionally, the
 * metastore is responsible for storing the last know server {@link Configuration}, including cluster
 * membership.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class MetaStore implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetaStore.class);
  private final Storage storage;
  private final Buffer buffer;

  public MetaStore(String name, Storage storage) {
    this.storage = Assert.notNull(storage, "storage");
    if (storage.level() == StorageLevel.MEMORY) {
      buffer = HeapBuffer.allocate(12);
    } else {
      storage.directory().mkdirs();
      File file = new File(storage.directory(), String.format("%s.meta", name));
      buffer = FileBuffer.allocate(file, 12);
    }
  }

  /**
   * Stores the current server term.
   *
   * @param term The current server term.
   * @return The metastore.
   */
  public synchronized MetaStore storeTerm(long term) {
    LOGGER.debug("Store term {}", term);
    buffer.writeLong(0, term).flush();
    return this;
  }

  /**
   * Loads the stored server term.
   *
   * @return The stored server term.
   */
  public synchronized long loadTerm() {
    return buffer.readLong(0);
  }

  /**
   * Stores the last voted server.
   *
   * @param vote The server vote.
   * @return The metastore.
   */
  public synchronized MetaStore storeVote(int vote) {
    LOGGER.debug("Store vote {}", vote);
    buffer.writeInt(8, vote).flush();
    return this;
  }

  /**
   * Loads the last vote for the server.
   *
   * @return The last vote for the server.
   */
  public synchronized int loadVote() {
    return buffer.readInt(8);
  }

  /**
   * Stores the current cluster configuration.
   *
   * @param configuration The current cluster configuration.
   * @return The metastore.
   */
  public synchronized MetaStore storeConfiguration(Configuration configuration) {
    LOGGER.debug("Store configuration {}", configuration);

    buffer.position(12)
      .writeByte(1)
      .writeLong(configuration.index())
      .writeLong(configuration.term())
      .writeLong(configuration.time());

    buffer.writeInt(configuration.members().size());
    for (Member member : configuration.members()) {
      buffer.writeByte(member.type().ordinal());
      buffer.writeString(member.serverAddress().host()).writeInt(member.serverAddress().port());
      if (member.clientAddress() != null) {
        buffer.writeBoolean(true)
          .writeString(member.clientAddress().host())
          .writeInt(member.clientAddress().port());
      } else {
        buffer.writeBoolean(false);
      }
      buffer.writeLong(member.updated().toEpochMilli());
    }

    buffer.flush();
    return this;
  }

  /**
   * Loads the current cluster configuration.
   *
   * @return The current cluster configuration.
   */
  @SuppressWarnings("unchecked")
  public synchronized Configuration loadConfiguration() {
    if (buffer.position(12).readByte() == 1) {
      long index = buffer.readLong();
      long term = buffer.readLong();
      long time = buffer.readLong();

      final int size = buffer.readInt();
      final List<Member> members = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        Member.Type memberType = Member.Type.values()[buffer.readByte()];
        Address serverAddress = new Address(buffer.readString(), buffer.readInt());
        Address clientAddress = null;
        if (buffer.readBoolean()) {
          clientAddress = new Address(buffer.readString(), buffer.readInt());
        }
        Instant updated = Instant.ofEpochMilli(buffer.readLong());
        members.add(new ServerMember(memberType, serverAddress, clientAddress, updated));
      }

      return new Configuration(index, term, time, members);
    }
    return null;
  }

  @Override
  public synchronized void close() {
    buffer.close();
  }

  @Override
  public String toString() {
    if (buffer instanceof FileBuffer) {
      return String.format("%s[%s]", getClass().getSimpleName(), ((FileBuffer) buffer).file());
    } else {
      return getClass().getSimpleName();
    }
  }

}

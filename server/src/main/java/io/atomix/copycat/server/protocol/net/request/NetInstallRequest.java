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
package io.atomix.copycat.server.protocol.net.request;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.protocol.net.request.AbstractNetRequest;
import io.atomix.copycat.server.protocol.request.InstallRequest;

import java.util.Arrays;
import java.util.Objects;

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
public class NetInstallRequest extends AbstractNetRequest implements InstallRequest, RaftNetRequest {
  private final long term;
  private final int leader;
  private final long index;
  private final int offset;
  private final byte[] data;
  private final boolean complete;

  public NetInstallRequest(long id, long term, int leader, long index, int offset, byte[] data, boolean complete) {
    super(id);
    this.term = term;
    this.leader = leader;
    this.index = index;
    this.offset = offset;
    this.data = data;
    this.complete = complete;
  }

  @Override
  public Type type() {
    return RaftNetRequest.Types.INSTALL_REQUEST;
  }

  @Override
  public long term() {
    return term;
  }

  @Override
  public int leader() {
    return leader;
  }

  @Override
  public long index() {
    return index;
  }

  @Override
  public int offset() {
    return offset;
  }

  @Override
  public byte[] data() {
    return data;
  }

  @Override
  public boolean complete() {
    return complete;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), term, leader, index, offset, complete, data);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof NetInstallRequest) {
      NetInstallRequest request = (NetInstallRequest) object;
      return request.term == term
        && request.leader == leader
        && request.index == index
        && request.offset == offset
        && request.complete == complete
        && Arrays.equals(request.data, data);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[term=%d, leader=%d, index=%d, offset=%d, data=%s, complete=%b]", getClass().getSimpleName(), term, leader, index, offset, data, complete);
  }

  /**
   * Snapshot request builder.
   */
  public static class Builder extends AbstractNetRequest.Builder<InstallRequest.Builder, InstallRequest> implements InstallRequest.Builder {
    private long term;
    private int leader;
    private long index;
    private int offset;
    private byte[] data;
    private boolean complete;

    public Builder(long id) {
      super(id);
    }

    @Override
    public Builder withTerm(long term) {
      this.term = Assert.arg(term, term > 0, "term must be positive");
      return this;
    }

    @Override
    public Builder withLeader(int leader) {
      this.leader = leader;
      return this;
    }

    @Override
    public Builder withIndex(long index) {
      this.index = Assert.argNot(index, index < 0, "index must be positive");
      return this;
    }

    @Override
    public Builder withOffset(int offset) {
      this.offset = Assert.argNot(offset, offset < 0, "offset must be positive");
      return this;
    }

    @Override
    public Builder withData(byte[] snapshot) {
      this.data = Assert.notNull(snapshot, "data");
      return this;
    }

    @Override
    public Builder withComplete(boolean complete) {
      this.complete = complete;
      return this;
    }

    @Override
    public NetInstallRequest build() {
      return new NetInstallRequest(id, term, leader, index, offset, data, complete);
    }
  }

  /**
   * Install request serializer.
   */
  public static class Serializer extends AbstractNetRequest.Serializer<NetInstallRequest> {
    @Override
    public void write(Kryo kryo, Output output, NetInstallRequest request) {
      output.writeLong(request.id);
      output.writeLong(request.term);
      output.writeInt(request.leader);
      output.writeLong(request.index);
      output.writeInt(request.offset);
      output.writeInt(request.data.length);
      output.write(request.data);
      output.writeBoolean(request.complete);
    }

    @Override
    public NetInstallRequest read(Kryo kryo, Input input, Class<NetInstallRequest> type) {
      return new NetInstallRequest(input.readLong(), input.readLong(), input.readInt(), input.readLong(), input.readInt(), input.readBytes(input.readInt()), input.readBoolean());
    }
  }
}

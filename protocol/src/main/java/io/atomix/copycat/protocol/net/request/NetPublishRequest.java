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
package io.atomix.copycat.protocol.net.request;

import io.atomix.copycat.protocol.request.PublishRequest;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

import java.util.ArrayList;
import java.util.List;

/**
 * TCP publish request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetPublishRequest extends PublishRequest implements NetRequest<NetPublishRequest> {
  private final long id;

  public NetPublishRequest(long id, long session, long eventIndex, long previousIndex, List<byte[]> events) {
    super(session, eventIndex, previousIndex, events);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Type.PUBLISH;
  }

  /**
   * TCP publish request builder.
   */
  public static class Builder extends PublishRequest.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public PublishRequest copy(PublishRequest request) {
      return new NetPublishRequest(id, request.session(), request.eventIndex(), request.previousIndex(), request.events());
    }

    @Override
    public PublishRequest build() {
      return new NetPublishRequest(id, session, eventIndex, previousIndex, events);
    }
  }

  /**
   * Publish request serializer.
   */
  public static class Serializer extends NetRequest.Serializer<NetPublishRequest> {
    @Override
    public void writeObject(BufferOutput output, NetPublishRequest request) {
      output.writeLong(request.id);
      output.writeLong(request.session);
      output.writeLong(request.eventIndex);
      output.writeLong(request.previousIndex);
      output.writeInt(request.events.size());
      for (byte[] event : request.events) {
        output.writeInt(event.length).write(event);
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public NetPublishRequest readObject(BufferInput input, Class<NetPublishRequest> type) {
      final long id = input.readLong();
      final long session = input.readLong();
      final long eventIndex = input.readLong();
      final long previousIndex = input.readLong();
      final int size = input.readInt();
      final List<byte[]> events = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        events.add(input.readBytes(input.readInt()));
      }
      return new NetPublishRequest(id, session, eventIndex, previousIndex, events);
    }
  }
}

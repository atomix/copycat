/*
 * Copyright 2017 the original author or authors.
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
package io.atomix.copycat.server.protocol.serializers;

import io.atomix.copycat.protocol.request.ProtocolRequest;
import io.atomix.copycat.protocol.serializers.ProtocolRequestSerializer;
import io.atomix.copycat.server.protocol.request.RaftProtocolRequest;

/**
 * Raft protocol request serializer.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class RaftProtocolRequestSerializer<T extends RaftProtocolRequest> extends ProtocolRequestSerializer<T> {

  public static ProtocolRequestSerializer<?> forType(ProtocolRequest.Type type) {
    switch (type.id()) {
      case 0x07:
        return new JoinRequestSerializer();
      case 0x08:
        return new LeaveRequestSerializer();
      case 0x09:
        return new InstallRequestSerializer();
      case 0x0a:
        return new ConfigureRequestSerializer();
      case 0x0b:
        return new ReconfigureRequestSerializer();
      case 0x0c:
        return new AcceptRequestSerializer();
      case 0x0d:
        return new PollRequestSerializer();
      case 0x0e:
        return new VoteRequestSerializer();
      case 0x0f:
        return new AppendRequestSerializer();
      default:
        throw new IllegalArgumentException("unknown request type: " + type);
    }
  }

}

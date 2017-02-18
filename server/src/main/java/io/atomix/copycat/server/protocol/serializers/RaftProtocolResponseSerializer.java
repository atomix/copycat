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

import io.atomix.copycat.protocol.response.ProtocolResponse;
import io.atomix.copycat.protocol.serializers.ProtocolResponseSerializer;
import io.atomix.copycat.server.protocol.response.RaftProtocolResponse;

/**
 * Raft protocol response serializer.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class RaftProtocolResponseSerializer<T extends RaftProtocolResponse> extends ProtocolResponseSerializer<T> {

  public static ProtocolResponseSerializer<?> forType(ProtocolResponse.Type type) {
    switch (type.id()) {
      case 0x17:
        return new JoinResponseSerializer();
      case 0x18:
        return new LeaveResponseSerializer();
      case 0x19:
        return new InstallResponseSerializer();
      case 0x1a:
        return new ConfigureResponseSerializer();
      case 0x1b:
        return new ReconfigureResponseSerializer();
      case 0x1c:
        return new AcceptResponseSerializer();
      case 0x1d:
        return new PollResponseSerializer();
      case 0x1e:
        return new VoteResponseSerializer();
      case 0x1f:
        return new AppendResponseSerializer();
      default:
        throw new IllegalArgumentException("unknown response type: " + type);
    }
  }

}

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
package io.atomix.copycat.protocol.serializers;

import io.atomix.copycat.protocol.request.ProtocolRequest;
import io.atomix.copycat.util.CopycatSerializer;

/**
 * Request serializer.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class ProtocolRequestSerializer<T extends ProtocolRequest> implements CopycatSerializer<T> {

  public static ProtocolRequestSerializer<?> forType(ProtocolRequest.Type type) {
    switch (type.id()) {
      case 0x00:
        return new ConnectRequestSerializer();
      case 0x01:
        return new RegisterRequestSerializer();
      case 0x02:
        return new KeepAliveRequestSerializer();
      case 0x03:
        return new UnregisterRequestSerializer();
      case 0x04:
        return new QueryRequestSerializer();
      case 0x05:
        return new CommandRequestSerializer();
      case 0x06:
        return new PublishRequestSerializer();
      default:
        throw new IllegalArgumentException("unknown request type: " + type);
    }
  }

}

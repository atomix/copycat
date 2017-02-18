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

import io.atomix.copycat.protocol.response.ProtocolResponse;
import io.atomix.copycat.util.CopycatSerializer;

/**
 * Response serializer.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class ProtocolResponseSerializer<T extends ProtocolResponse> implements CopycatSerializer<T> {

  public static ProtocolResponseSerializer<?> forType(ProtocolResponse.Type type) {
    switch (type.id()) {
      case 0x10:
        return new ConnectResponseSerializer();
      case 0x11:
        return new RegisterResponseSerializer();
      case 0x12:
        return new KeepAliveResponseSerializer();
      case 0x13:
        return new UnregisterResponseSerializer();
      case 0x14:
        return new QueryResponseSerializer();
      case 0x15:
        return new CommandResponseSerializer();
      case 0x16:
        return new PublishResponseSerializer();
      default:
        throw new IllegalArgumentException("unknown response type: " + type);
    }
  }

}

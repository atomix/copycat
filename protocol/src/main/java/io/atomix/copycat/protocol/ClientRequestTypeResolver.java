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
package io.atomix.copycat.protocol;

import io.atomix.catalyst.serializer.SerializableTypeResolver;
import io.atomix.catalyst.serializer.SerializerRegistry;

import java.util.HashMap;
import java.util.Map;

/**
 * Request serializable type resolver.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class ClientRequestTypeResolver implements SerializableTypeResolver {
  @SuppressWarnings("unchecked")
  private static final Map<Class<? extends Request>, Integer> TYPES = new HashMap() {{
    put(CommandRequest.class, -3);
    put(ConnectRequest.class, -4);
    put(KeepAliveRequest.class, -5);
    put(PublishRequest.class, -6);
    put(QueryRequest.class, -7);
    put(RegisterRequest.class, -8);
    put(UnregisterRequest.class, -9);
  }};

  @Override
  public void resolve(SerializerRegistry registry) {
    for (Map.Entry<Class<? extends Request>, Integer> entry : TYPES.entrySet()) {
      registry.register(entry.getKey(), entry.getValue());
    }
  }

}

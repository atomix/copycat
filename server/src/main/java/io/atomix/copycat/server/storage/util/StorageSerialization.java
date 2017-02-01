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
package io.atomix.copycat.server.storage.util;

import io.atomix.catalyst.serializer.SerializableTypeResolver;
import io.atomix.catalyst.serializer.SerializerRegistry;
import io.atomix.copycat.protocol.Request;
import io.atomix.copycat.server.storage.entry.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Log entry serializable type resolver.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class StorageSerialization implements SerializableTypeResolver {
  @SuppressWarnings("unchecked")
  private static final Map<Class<? extends Request>, Integer> TYPES = new HashMap() {{
    put(CommandEntry.class, -36);
    put(ConfigurationEntry.class, -37);
    put(KeepAliveEntry.class, -38);
    put(InitializeEntry.class, -39);
    put(QueryEntry.class, -40);
    put(RegisterEntry.class, -41);
    put(UnregisterEntry.class, -43);
  }};

  @Override
  public void resolve(SerializerRegistry registry) {
    for (Map.Entry<Class<? extends Request>, Integer> entry : TYPES.entrySet()) {
      registry.register(entry.getKey(), entry.getValue());
    }
  }

}
